import logging
import traceback

from core.exceptions import SyncError
from core.signals import pre_sync
from django.core.exceptions import ValidationError
from django.utils import timezone

from ..choices import ForwardExecutionBackendChoices
from ..choices import ForwardIngestionPhaseChoices
from ..choices import ForwardSourceStatusChoices
from ..choices import ForwardSyncStatusChoices
from ..models import ForwardIngestion
from ..models import ForwardIngestionIssue
from ..models import ForwardSource
from ..models import ForwardSync
from ..models import ForwardValidationRun
from ..utilities.logging import SyncLogging
from .api_usage import evaluate_forward_api_usage
from .runtime_guidance import log_worker_timeout_guidance
from .snapshot_freshness import latest_processed_catchup_decision
from .sync_state import mark_branch_run_failed

logger = logging.getLogger("forward_netbox.models")


def _prepare_forward_sync(sync, job=None):
    if job:
        sync.logger = SyncLogging(job=job.pk)
        user = job.user
    else:
        sync.logger = SyncLogging(job=sync.pk)
        user = sync.user

    pre_sync.send(sender=sync.__class__, instance=sync)

    sync.status = ForwardSyncStatusChoices.SYNCING
    ForwardSync.objects.filter(pk=sync.pk).update(status=sync.status)
    sync.source.status = ForwardSourceStatusChoices.SYNCING
    ForwardSource.objects.filter(pk=sync.source.pk).update(status=sync.source.status)
    return user


def _build_forward_ingestion(sync, job, executor):
    validation_run = getattr(executor, "last_validation_run", None)
    if not isinstance(validation_run, ForwardValidationRun):
        validation_run = None
    model_results = getattr(executor, "last_model_results", [])
    if not isinstance(model_results, list):
        model_results = []
    return ForwardIngestion.objects.create(
        sync=sync,
        job=job,
        validation_run=validation_run,
        model_results=model_results,
    )


def _record_forward_sync_failure(sync, job, executor, ingestion, exc):
    logger.exception("Forward sync failed")
    sync.status = ForwardSyncStatusChoices.FAILED
    if ingestion is None:
        ingestion = getattr(executor, "current_ingestion", None)
    if ingestion is None:
        ingestion = _build_forward_ingestion(sync, job, executor)
    else:
        validation_run = getattr(executor, "last_validation_run", None)
        if (
            isinstance(validation_run, ForwardValidationRun)
            and not ingestion.validation_run
        ):
            ingestion.validation_run = validation_run
            ingestion.save(update_fields=["validation_run"])
    mark_branch_run_failed(sync, f"Forward ingestion failed: {exc}")
    sync.logger.log_failure(f"Forward ingestion failed: {exc}", obj=ingestion)
    ForwardIngestionIssue.objects.create(
        ingestion=ingestion,
        phase=ForwardIngestionPhaseChoices.SYNC,
        message=str(exc),
        exception=exc.__class__.__name__,
        raw_data={"traceback": traceback.format_exc()},
    )
    return ingestion


def _finalize_forward_sync(sync, job):
    sync.last_synced = timezone.now()
    sync.source.last_synced = sync.last_synced
    if sync.status in (
        ForwardSyncStatusChoices.QUEUED,
        ForwardSyncStatusChoices.SYNCING,
    ):
        sync.source.status = ForwardSourceStatusChoices.SYNCING
    else:
        sync.source.status = (
            ForwardSourceStatusChoices.READY
            if sync.status
            in (
                ForwardSyncStatusChoices.READY_TO_MERGE,
                ForwardSyncStatusChoices.MERGING,
                ForwardSyncStatusChoices.COMPLETED,
            )
            else ForwardSourceStatusChoices.FAILED
        )
    ForwardSource.objects.filter(pk=sync.source.pk).update(
        last_synced=sync.source.last_synced,
        status=sync.source.status,
    )
    ForwardSync.objects.filter(pk=sync.pk).update(
        status=sync.status,
        last_synced=sync.last_synced,
    )
    if job:
        job.data = sync.logger.log_data
        job.save(update_fields=["data"])


def _record_forward_api_usage(sync, executor):
    client = getattr(executor, "client", None)
    summary_method = getattr(client, "api_usage_summary", None)
    if not callable(summary_method):
        return
    summary = summary_method()
    if not isinstance(summary, dict):
        return
    summary = dict(summary)
    budget = evaluate_forward_api_usage(
        summary,
        source_type=getattr(getattr(sync, "source", None), "type", None),
    )
    summary["budget"] = budget
    sync.logger.set_api_usage_summary(summary)
    sync.logger.log_info(
        "Forward API usage summary: "
        f"api_usage_status={budget.get('status')} "
        f"http_attempts={summary.get('http_attempts', 0)} "
        f"http_retries={summary.get('http_retries', 0)} "
        f"http_429_failures={summary.get('http_429_failures', 0)} "
        f"nqe_query_calls={summary.get('nqe_query_calls', 0)} "
        f"nqe_diff_calls={summary.get('nqe_diff_calls', 0)} "
        f"nqe_pages={summary.get('nqe_pages', 0)} "
        f"read_cache_hits={summary.get('read_cache_hits', 0)} "
        f"read_cache_hit_rate={summary.get('read_cache_hit_rate')} "
        f"observed_http_attempts_per_minute="
        f"{summary.get('observed_http_attempts_per_minute')} "
        f"throttle_sleep_seconds={summary.get('throttle_sleep_seconds', 0.0)}.",
        obj=sync,
    )


def run_forward_sync(sync, job=None, *, max_changes_per_branch=None):
    from .fast_bootstrap_executor import ForwardFastBootstrapExecutor
    from .multi_branch import ForwardMultiBranchExecutor

    sync.logger = SyncLogging(job=job.pk if job else sync.pk)
    try:
        sync.full_clean()
    except ValidationError as exc:
        sync.logger.log_failure(
            f"Forward sync configuration is invalid: {exc}",
            obj=sync,
        )
        raise

    if sync.is_waiting_for_branch_merge:
        sync.logger.log_warning(
            "Forward sync is waiting for the current shard branch to be merged.",
            obj=sync,
        )
        return

    if sync.status in (
        ForwardSyncStatusChoices.SYNCING,
        ForwardSyncStatusChoices.MERGING,
    ):
        raise SyncError(
            "Cannot initiate sync; a Forward ingestion is already in progress."
        )

    user = _prepare_forward_sync(sync, job=job)
    if max_changes_per_branch is None:
        max_changes_per_branch = sync.get_max_changes_per_branch()

    ingestion = None
    executor = None
    try:
        execution_backend = (sync.parameters or {}).get(
            "execution_backend",
            ForwardExecutionBackendChoices.BRANCHING,
        )
        log_worker_timeout_guidance(
            sync,
            sync.logger,
            execution_backend=execution_backend,
        )
        executor_class = (
            ForwardFastBootstrapExecutor
            if execution_backend == ForwardExecutionBackendChoices.FAST_BOOTSTRAP
            else ForwardMultiBranchExecutor
        )
        executor = executor_class(
            sync,
            sync.source.get_client(),
            sync.logger,
            user=user,
            job=job,
        )
        if execution_backend == ForwardExecutionBackendChoices.FAST_BOOTSTRAP:
            ingestions = executor.run()
        else:
            ingestions = executor.run(
                max_changes_per_branch=max_changes_per_branch,
            )
        if getattr(executor, "resumable_started", False) is True:
            sync.logger.log_success(
                "Forward Branching plan queued for resumable shard execution.",
                obj=sync,
            )
            return
        if not ingestions:
            sync.status = ForwardSyncStatusChoices.COMPLETED
            sync.logger.log_success("Forward ingestion completed.", obj=sync)
            return
        ingestion = ingestions[-1]
        if sync.status == ForwardSyncStatusChoices.READY_TO_MERGE:
            sync.logger.log_success(
                "Forward multi-branch shard staged for review.",
                obj=sync,
            )
            return
        sync.status = ForwardSyncStatusChoices.COMPLETED
        if execution_backend == ForwardExecutionBackendChoices.FAST_BOOTSTRAP:
            sync.logger.log_success(
                "Forward fast bootstrap ingestion completed.",
                obj=sync,
            )
        else:
            sync.logger.log_success(
                "Forward multi-branch ingestion completed.",
                obj=sync,
            )
        return
    except Exception as exc:
        ingestion = _record_forward_sync_failure(
            sync,
            job,
            executor,
            ingestion,
            exc,
        )
    finally:
        _record_forward_api_usage(sync, executor)
        _finalize_forward_sync(sync, job)
        if sync.status == ForwardSyncStatusChoices.COMPLETED:
            current_snapshot_id = ""
            if "ingestions" in locals() and ingestions:
                current_snapshot_id = str(
                    getattr(ingestions[-1], "snapshot_id", "") or ""
                ).strip()
            if not current_snapshot_id:
                current_ingestion = getattr(executor, "current_ingestion", None)
                current_snapshot_id = str(
                    getattr(current_ingestion, "snapshot_id", "") or ""
                ).strip()
            decision = latest_processed_catchup_decision(
                sync,
                current_snapshot_id=current_snapshot_id,
                client=getattr(executor, "client", None),
                current_job=job,
            )
            if decision["should_queue"]:
                selector = decision.get("snapshot_selector") or "latestProcessed"
                sync.logger.log_info(
                    f"Forward {selector} advanced from "
                    f"`{decision['current_snapshot_id']}` to "
                    f"`{decision['latest_processed_snapshot_id']}` during the run; "
                    "queuing a catch-up sync.",
                    obj=sync,
                )
                sync.enqueue_sync_job(adhoc=True, user=user)
