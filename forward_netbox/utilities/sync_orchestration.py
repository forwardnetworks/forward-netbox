import logging
import traceback

from core.exceptions import SyncError
from core.signals import pre_sync
from django.core.exceptions import ValidationError
from django.utils import timezone
from rq.timeouts import JobTimeoutException

from ..choices import ForwardIngestionPhaseChoices
from ..choices import ForwardSourceStatusChoices
from ..choices import ForwardSyncStatusChoices
from ..models import ForwardIngestion
from ..models import ForwardIngestionIssue
from ..models import ForwardSource
from ..models import ForwardSync
from ..models import ForwardValidationRun
from ..utilities.logging import SyncLogging
from .api_usage import record_forward_api_usage
from .runtime_guidance import log_worker_timeout_guidance

logger = logging.getLogger("forward_netbox.models")


def _prepare_forward_sync(sync, job=None):
    if job:
        sync.logger = SyncLogging(job=job.pk)
        user = job.user
    else:
        sync.logger = SyncLogging()
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


def should_skip_unchanged_snapshot(sync, *, force_unchanged=False, client=None):
    """Return the resolved snapshot id when a scheduled run can no-op.

    When the target equals the last eligible baseline, there is nothing new to
    fetch. An explicit force flag, used by manual UI/API runs, remains the
    repair path for out-of-band NetBox changes on the same snapshot. Scheduled,
    webhook, and catch-up runs never force unchanged work. Any resolution error
    falls through to a normal run.
    """
    if force_unchanged:
        return None
    baseline = sync.latest_baseline_ingestion()
    baseline_snapshot = str(getattr(baseline, "snapshot_id", "") or "").strip()
    if not baseline_snapshot:
        return None
    try:
        current_snapshot = str(sync.resolve_snapshot_id(client) or "").strip()
    except JobTimeoutException:
        raise
    except Exception:
        return None
    if current_snapshot and current_snapshot == baseline_snapshot:
        return current_snapshot
    return None


def _record_forward_api_usage(sync, executor):
    return record_forward_api_usage(sync, getattr(executor, "client", None))


def run_forward_sync(
    sync,
    job=None,
    *,
    max_changes_per_staging_item=None,
    force_unchanged=False,
):
    from .single_branch_executor import ForwardSingleBranchExecutor

    sync.logger = SyncLogging(job=job.pk if job else None)
    try:
        sync.full_clean()
    except ValidationError as exc:
        sync.logger.log_failure(
            f"Forward sync configuration is invalid: {exc}",
            obj=sync,
        )
        raise

    if sync.status == ForwardSyncStatusChoices.READY_TO_MERGE:
        sync.logger.log_warning(
            "Forward sync is waiting for its branch to be merged.",
            obj=sync,
        )
        return False

    if sync.status in (
        ForwardSyncStatusChoices.SYNCING,
        ForwardSyncStatusChoices.MERGING,
    ):
        raise SyncError(
            "Cannot initiate sync; a Forward ingestion is already in progress."
        )

    skip_snapshot = should_skip_unchanged_snapshot(
        sync,
        force_unchanged=force_unchanged,
    )
    if skip_snapshot:
        sync.status = ForwardSyncStatusChoices.COMPLETED
        sync.logger.log_success(
            f"Snapshot `{skip_snapshot}` is unchanged since the last baseline "
            "ingestion; skipping query execution (no-op). Run the sync manually "
            "to force a re-sync.",
            obj=sync,
        )
        _finalize_forward_sync(sync, job)
        return False

    user = _prepare_forward_sync(sync, job=job)

    ingestion = None
    executor = None
    try:
        # Single-branch is the only execution path (2.0): one provisioned branch
        # per sync, bulk staged and bulk merged.
        log_worker_timeout_guidance(sync, sync.logger)
        executor = ForwardSingleBranchExecutor(
            sync,
            sync.source.get_client(),
            sync.logger,
            user=user,
            job=job,
        )
        ingestions = executor.run()
        if not ingestions:
            sync.status = ForwardSyncStatusChoices.COMPLETED
            sync.logger.log_success("Forward ingestion completed.", obj=sync)
            return True
        ingestion = ingestions[-1]
        if sync.status == ForwardSyncStatusChoices.READY_TO_MERGE:
            sync.logger.log_success(
                "Forward single-branch sync staged for review.",
                obj=sync,
            )
            return True
        sync.status = ForwardSyncStatusChoices.COMPLETED
        sync.logger.log_success(
            "Forward single-branch ingestion completed.",
            obj=sync,
        )
        return True
    except JobTimeoutException:
        sync.status = ForwardSyncStatusChoices.TIMEOUT
        raise
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
