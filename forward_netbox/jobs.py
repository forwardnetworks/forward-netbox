import logging
import threading
import time
from datetime import datetime
from datetime import timedelta

from core.choices import JobStatusChoices
from core.exceptions import SyncError
from core.models import Job
from core.models import ObjectType
from django.contrib.auth import get_user_model
from netbox.context_managers import event_tracking
from netbox_branching.choices import BranchStatusChoices
from netbox_branching.models import Branch
from rq.timeouts import JobTimeoutException
from utilities.datetime import local_now
from utilities.request import NetBoxFakeRequest

from .choices import ForwardExecutionStepStatusChoices
from .choices import ForwardIngestionPhaseChoices
from .choices import ForwardSyncStatusChoices
from .exceptions import ForwardSyncError
from .models import ForwardIngestion
from .models import ForwardIngestionIssue
from .models import ForwardSync
from .utilities.execution_ledger import active_execution_run
from .utilities.execution_ledger import branch_run_state_from_execution_run
from .utilities.execution_ledger import claim_ingestion_merge_step
from .utilities.execution_ledger import claim_stage_step
from .utilities.execution_ledger import execution_step_for_ingestion
from .utilities.execution_ledger import latest_execution_run
from .utilities.execution_ledger import mark_ingestion_step_merged
from .utilities.execution_ledger import reconcile_execution_run
from .utilities.execution_ledger import touch_execution_step_progress
from .utilities.execution_ledger import update_run_from_branch_state
from .utilities.execution_ledger import upgrade_branch_run_state_to_execution_run
from .utilities.ingestion_merge import maybe_enqueue_next_branch_stage
from .utilities.json_safe import json_safe_value
from .utilities.logging import SyncLogging
from .utilities.resumable_branching import enqueue_branch_stage_job
from .utilities.resumable_branching import update_plan_item_state
from .utilities.sync_state import get_branch_run_display_state
from .utilities.sync_state import prune_stale_branch_run_state
from .utilities.validation import ForwardValidationRunner

logger = logging.getLogger(__name__)
STAGE_LIVENESS_HEARTBEAT_SECONDS = 30
STAGE_LIVENESS_LOG_SECONDS = 300


def _resolve_request_user(*, sync, job=None):
    if job is not None and getattr(job, "user", None) is not None:
        return job.user
    if getattr(sync, "user", None) is not None:
        return sync.user
    User = get_user_model()
    return User.objects.filter(is_active=True, is_superuser=True).order_by("pk").first()


def _normalize_job_log_level(level):
    return {
        "success": "info",
        "failure": "error",
    }.get(level, level)


def _build_job_log_entries(log_data):
    entries = []
    for entry in (log_data or {}).get("logs", []):
        if not isinstance(entry, (list, tuple)) or len(entry) < 5:
            continue
        timestamp = entry[0]
        if isinstance(timestamp, str):
            try:
                timestamp = datetime.fromisoformat(timestamp)
            except ValueError:
                timestamp = local_now()
        entries.append(
            {
                "timestamp": timestamp,
                "level": _normalize_job_log_level(entry[1]),
                "message": entry[4],
            }
        )
    return entries


def safe_save_job_data(job, obj_with_logger):
    try:
        if hasattr(obj_with_logger, "logger") and hasattr(
            obj_with_logger.logger, "log_data"
        ):
            log_data = json_safe_value(obj_with_logger.logger.log_data)
            update_fields = ["data"]
            job.data = log_data
            job.log_entries = _build_job_log_entries(log_data)
            update_fields.append("log_entries")
            job.save(update_fields=update_fields)
    except Exception as exc:
        logger.warning("Failed to save job data for job %s: %s", job.pk, exc)


def record_timeout_issue(ingestion, phase, message):
    if ingestion is None:
        return None
    existing = ForwardIngestionIssue.objects.filter(
        ingestion=ingestion,
        phase=phase,
        exception=JobTimeoutException.__name__,
    ).first()
    if existing:
        return existing
    return ForwardIngestionIssue.objects.create(
        ingestion=ingestion,
        phase=phase,
        message=message,
        exception=JobTimeoutException.__name__,
        raw_data={},
        coalesce_fields={},
        defaults={},
    )


def sync_forwardsync(job, *args, **kwargs):
    sync = ForwardSync.objects.get(pk=job.object_id)

    try:
        job.start()
        sync.sync(job=job, adhoc=bool(kwargs.get("adhoc")))
        safe_save_job_data(job, sync)
        job.terminate()
    except Exception as exc:
        safe_save_job_data(job, sync)
        timeout = isinstance(exc, JobTimeoutException)
        job.terminate(status=JobStatusChoices.STATUS_ERRORED)
        ForwardSync.objects.filter(pk=sync.pk).update(
            status=(
                ForwardSyncStatusChoices.TIMEOUT
                if timeout
                else ForwardSyncStatusChoices.FAILED
            )
        )
        sync.status = (
            ForwardSyncStatusChoices.TIMEOUT
            if timeout
            else ForwardSyncStatusChoices.FAILED
        )
        update_run_from_branch_state(sync)
        if timeout:
            ingestion = (
                ForwardIngestion.objects.filter(sync=sync).order_by("-pk").first()
            )
            message = "Forward sync job timed out. Increase RQ worker timeout and rerun the sync."
            record_timeout_issue(
                ingestion,
                ForwardIngestionPhaseChoices.SYNC,
                message,
            )
            sync.logger.log_failure(message, obj=sync)
        if isinstance(exc, (ForwardSyncError, SyncError, JobTimeoutException)):
            logger.error(exc)
        else:
            raise
    finally:
        if sync.interval and not kwargs.get("adhoc"):
            new_scheduled_time = local_now() + timedelta(minutes=sync.interval)
            sync.refresh_from_db()
            should_skip = not sync.scheduled or (
                sync.scheduled
                and sync.scheduled > job.started
                and sync.jobs.filter(
                    status__in=[
                        JobStatusChoices.STATUS_SCHEDULED,
                        JobStatusChoices.STATUS_PENDING,
                        JobStatusChoices.STATUS_RUNNING,
                    ]
                )
                .exclude(pk=job.pk)
                .exists()
            )
            if should_skip:
                logger.info(
                    "Not scheduling a new job for ForwardSync %s because scheduling changed while the current job was running.",
                    sync.pk,
                )
            if not should_skip:
                request = NetBoxFakeRequest(
                    {
                        "META": {},
                        "POST": sync.parameters,
                        "GET": {},
                        "FILES": {},
                        "user": _resolve_request_user(sync=sync, job=job),
                        "path": "",
                        "id": job.job_id,
                    }
                )

                with event_tracking(request):
                    sync.scheduled = new_scheduled_time
                    sync.full_clean()
                    sync.save()
                logger.info(
                    "Scheduled next sync for ForwardSync %s at %s.",
                    sync.pk,
                    new_scheduled_time,
                )


def validate_forwardsync(job, *args, **kwargs):
    sync = ForwardSync.objects.get(pk=job.object_id)

    try:
        job.start()
        sync.logger = SyncLogging(job=job.pk)
        validation_run = ForwardValidationRunner(
            sync,
            sync.source.get_client(),
            sync.logger,
            job=job,
        ).run_query_validation()
        safe_save_job_data(job, sync)
        job.object_type = ObjectType.objects.get_for_model(validation_run)
        job.object_id = validation_run.pk
        job.save(update_fields=["object_type", "object_id"])
        job.terminate()
    except Exception as exc:
        safe_save_job_data(job, sync)
        job.terminate(status=JobStatusChoices.STATUS_ERRORED)
        if type(exc) in (SyncError, JobTimeoutException):
            logger.error(exc)
        else:
            raise


def prune_forward_orphans(job, *args, **kwargs):
    """Background prune of out-of-scope NetBox devices for a sync.

    Run as a job because deleting many devices cascades to their interfaces and
    IP addresses (plus change-logging signals) and easily exceeds an HTTP gateway
    timeout on large fabrics.
    """
    from .utilities.scope_reconciliation import EmptyForwardScopeError
    from .utilities.scope_reconciliation import prune_orphan_devices

    sync = ForwardSync.objects.get(pk=job.object_id)
    try:
        job.start()
        result = prune_orphan_devices(sync)
        job.data = {
            "pruned_device_count": result.get("pruned_device_count", 0),
            "out_of_scope_sample": result.get("out_of_scope_sample", []),
        }
        job.save(update_fields=["data"])
        job.terminate()
    except EmptyForwardScopeError as exc:
        job.data = {"error": str(exc)}
        job.save(update_fields=["data"])
        job.terminate(status=JobStatusChoices.STATUS_ERRORED)
        logger.error(exc)
    except Exception as exc:
        job.terminate(status=JobStatusChoices.STATUS_ERRORED)
        if type(exc) in (SyncError, JobTimeoutException):
            logger.error(exc)
        else:
            raise


def create_forward_module_bays(job, *args, **kwargs):
    """Background creation of missing module bays for a sync (out-of-band ORM)."""
    from .utilities.module_readiness import compute_module_readiness_for_sync
    from .utilities.module_readiness import create_missing_module_bays

    sync = ForwardSync.objects.get(pk=job.object_id)
    try:
        job.start()
        report = compute_module_readiness_for_sync(sync)
        result = create_missing_module_bays(report)
        job.data = result
        job.save(update_fields=["data"])
        job.terminate()
    except Exception as exc:
        job.terminate(status=JobStatusChoices.STATUS_ERRORED)
        if type(exc) in (SyncError, JobTimeoutException):
            logger.error(exc)
        else:
            raise


def forward_dependency_preview(job, *args, **kwargs):
    """Background dependency dry-run preview for a sync.

    The dry-run builds a full multi-branch plan against live Forward data, which
    far exceeds an HTTP gateway timeout on large fabrics. Run it as a job and
    cache the JSON payload on ``job.data`` so the preview page can render it
    later without a Forward round-trip.
    """
    from .views import _dependency_dry_run_payload

    sync = ForwardSync.objects.get(pk=job.object_id)
    try:
        job.start()
        sync.logger = SyncLogging(job=job.pk)
        payload = _dependency_dry_run_payload(sync)
        job.data = json_safe_value(payload)
        job.save(update_fields=["data"])
        job.terminate()
    except Exception as exc:
        job.terminate(status=JobStatusChoices.STATUS_ERRORED)
        if type(exc) in (SyncError, JobTimeoutException):
            logger.error(exc)
        else:
            raise


def merge_forwardingestion(job, remove_branch=True, *args, **kwargs):
    ingestion = ForwardIngestion.objects.get(pk=job.object_id)
    try:
        run = active_execution_run(ingestion.sync) or latest_execution_run(
            ingestion.sync
        )
        if run is not None:
            reconcile_execution_run(run)
            run.refresh_from_db()
        request = NetBoxFakeRequest(
            {
                "META": {},
                "POST": ingestion.sync.parameters,
                "GET": {},
                "FILES": {},
                "user": _resolve_request_user(sync=ingestion.sync, job=job),
                "path": "",
                "id": job.job_id,
            }
        )

        job.start()
        if not ingestion.branch or getattr(ingestion.branch, "status", "") == "merged":
            ingestion.sync.logger = SyncLogging(job=job.pk)
            ingestion.sync.logger.log_info(
                "Forward ingestion branch is already merged or no longer present; "
                "skipping duplicate merge job.",
                obj=ingestion,
            )
            step = mark_ingestion_step_merged(
                ingestion,
                baseline_ready=bool(getattr(ingestion, "baseline_ready", False)),
                merge_job=job if isinstance(job, Job) else None,
            )
            if step is not None:
                ingestion.sync.logger.log_info(
                    (
                        "merge_queued -> merged "
                        f"(job={getattr(job, 'pk', 'n/a')}, step={step.index})"
                    ),
                    obj=ingestion,
                )
            next_stage_job = maybe_enqueue_next_branch_stage(ingestion, job.user)
            if next_stage_job is not None:
                ingestion.sync.logger.log_info(
                    f"Queued next stage step job {next_stage_job.pk} after merge completion.",
                    obj=ingestion,
                )
            safe_save_job_data(job, ingestion.sync)
            job.terminate()
            return
        if not claim_ingestion_merge_step(ingestion, job):
            ingestion.sync.logger = SyncLogging(job=job.pk)
            ingestion.sync.logger.log_info(
                "Forward ingestion merge is already claimed or completed; "
                "skipping duplicate merge job.",
                obj=ingestion,
            )
            if run is not None:
                reconcile_execution_run(run)
                run.refresh_from_db()
            next_stage_job = maybe_enqueue_next_branch_stage(ingestion, job.user)
            if next_stage_job is not None:
                ingestion.sync.logger.log_info(
                    f"Queued next stage step job {next_stage_job.pk} after merge reconciliation.",
                    obj=ingestion,
                )
            safe_save_job_data(job, ingestion.sync)
            job.terminate()
            return
        if isinstance(job, Job):
            ingestion.merge_job = job
        ingestion.save(update_fields=["merge_job"])
        ingestion.sync.logger = SyncLogging(job=job.pk)
        with event_tracking(request):
            ingestion.sync_merge(remove_branch=remove_branch)
        step = execution_step_for_ingestion(ingestion)
        if step is not None and step.status == ForwardExecutionStepStatusChoices.MERGED:
            ingestion.sync.logger.log_info(
                (
                    "merge_queued -> merged "
                    f"(job={getattr(job, 'pk', 'n/a')}, step={step.index})"
                ),
                obj=ingestion,
            )
        maybe_enqueue_next_branch_stage(ingestion, job.user)

        safe_save_job_data(job, ingestion.sync)
        job.terminate()
    except Exception as exc:
        logger.exception(
            "Error during merge for ForwardIngestion %s: %s", ingestion.pk, exc
        )
        timeout = isinstance(exc, JobTimeoutException)
        merge_not_ready_retryable = _is_merge_not_ready_retryable(exc)
        if timeout or merge_not_ready_retryable:
            message = (
                "Forward merge job timed out. Increase RQ worker timeout and rerun the merge."
                if timeout
                else str(exc)
            )
            if timeout:
                record_timeout_issue(
                    ingestion,
                    ForwardIngestionPhaseChoices.MERGE,
                    message,
                )
            state = get_branch_run_display_state(ingestion.sync)
            pending_index = int(state.get("pending_plan_index") or 0)
            if not pending_index:
                ledger_step = execution_step_for_ingestion(ingestion)
                pending_index = int(getattr(ledger_step, "index", 0) or 0)
            if pending_index:
                update_plan_item_state(
                    ingestion.sync,
                    pending_index,
                    status="merge_timeout",
                    last_error=message,
                    retry_count=_plan_item_retry_count(state, pending_index) + 1,
                )
            if timeout:
                ingestion.sync.logger.log_failure(message, obj=ingestion)
            else:
                if _reset_merge_not_ready_branch_state(ingestion):
                    ingestion.sync.logger.log_info(
                        "Reset transient Branching merge state to ready before automatic retry.",
                        obj=ingestion,
                    )
                ingestion.sync.logger.log_info(
                    (
                        "Merge job hit a transient Branching readiness guard; "
                        "attempting automatic requeue."
                    ),
                    obj=ingestion,
                )
            update_run_from_branch_state(ingestion.sync)
            auto_retry_job = maybe_enqueue_next_branch_stage(ingestion, job.user)
            if auto_retry_job is not None:
                ingestion.sync.logger.log_info(
                    (
                        "Queued automatic merge-timeout recovery job "
                        f"{auto_retry_job.pk}."
                    ),
                    obj=ingestion,
                )
                if type(exc) in (SyncError, JobTimeoutException):
                    logger.warning(exc)
                safe_save_job_data(job, ingestion.sync)
                job.terminate(status=JobStatusChoices.STATUS_ERRORED)
                return
        else:
            message = f"Forward merge job failed: {exc}"
            if getattr(ingestion.sync, "logger", None) is None:
                ingestion.sync.logger = SyncLogging(job=job.pk)
            ingestion.sync.logger.log_failure(message, obj=ingestion)
            _fail_nonretryable_merging_branch(ingestion, message)

        ForwardSync.objects.filter(pk=ingestion.sync.pk).update(
            status=(
                ForwardSyncStatusChoices.TIMEOUT
                if timeout
                else ForwardSyncStatusChoices.FAILED
            )
        )
        ingestion.sync.status = (
            ForwardSyncStatusChoices.TIMEOUT
            if timeout
            else ForwardSyncStatusChoices.FAILED
        )
        update_run_from_branch_state(ingestion.sync)
        safe_save_job_data(job, ingestion.sync)
        job.terminate(status=JobStatusChoices.STATUS_ERRORED)
        if type(exc) in (SyncError, JobTimeoutException):
            logger.error(exc)
        else:
            raise


def _plan_item_retry_count(state, index):
    for item in state.get("plan_items") or []:
        if int(item.get("index") or 0) == int(index):
            return int(item.get("retry_count") or 0)
    return 0


def _is_merge_not_ready_retryable(exc):
    if not isinstance(exc, SyncError):
        return False
    message = str(exc or "").lower()
    return "not ready to merge" in message and "branch" in message


def _reset_merge_not_ready_branch_state(ingestion):
    branch = getattr(ingestion, "branch", None)
    if branch is None:
        return False
    if str(getattr(branch, "status", "") or "") != BranchStatusChoices.MERGING:
        return False
    branch.status = BranchStatusChoices.READY
    branch.save(update_fields=["status", "last_updated"])
    return True


def _fail_nonretryable_merging_branch(ingestion, message):
    branch = getattr(ingestion, "branch", None)
    if branch is None:
        return False
    if str(getattr(branch, "status", "") or "") != BranchStatusChoices.MERGING:
        return False
    Branch.objects.filter(pk=branch.pk).update(status=BranchStatusChoices.FAILED)
    branch.status = BranchStatusChoices.FAILED
    ingestion.sync.logger.log_failure(
        (
            "Marked Branching branch failed after non-retryable merge error: "
            f"{message}"
        ),
        obj=ingestion,
    )
    return True


def _is_transient_db_connection_retryable(exc):
    message = str(exc or "").lower()
    return "too many clients already" in message or (
        "connection failed" in message and "port 5432" in message
    )


def _stage_liveness_monitor(*, sync, logger_, shard_index, model_string):
    if not shard_index or not model_string:
        return None, None

    stop_event = threading.Event()

    def _heartbeat_loop():
        last_log_at = 0.0
        while not stop_event.wait(STAGE_LIVENESS_HEARTBEAT_SECONDS):
            try:
                touched = touch_execution_step_progress(
                    sync,
                    model_string=model_string,
                    shard_index=shard_index,
                )
                if not touched:
                    break
                now = time.monotonic()
                if now - last_log_at >= STAGE_LIVENESS_LOG_SECONDS:
                    logger_.log_info(
                        (
                            f"Shard {int(shard_index)} for {model_string} is still "
                            "running; heartbeat refreshed."
                        ),
                        obj=sync,
                    )
                    last_log_at = now
            except Exception:
                logger.debug(
                    "Failed to refresh shard liveness heartbeat for sync %s shard %s.",
                    getattr(sync, "pk", "unknown"),
                    shard_index,
                    exc_info=True,
                )
                break

    thread = threading.Thread(
        target=_heartbeat_loop,
        name=f"forward-stage-heartbeat-{getattr(sync, 'pk', 'sync')}-{shard_index}",
        daemon=True,
    )
    thread.start()
    return stop_event, thread


def _stop_stage_liveness_monitor(stop_event, thread):
    if stop_event is None:
        return
    stop_event.set()
    if thread is not None and thread.is_alive():
        thread.join(timeout=1.0)


def _stage_job_failure_target(sync, *, claimed_index, job):
    state = get_branch_run_display_state(sync)
    fallback_index = int(
        state.get("pending_plan_index") or state.get("next_plan_index") or 1
    )
    if claimed_index is None:
        return fallback_index, True

    run = active_execution_run(sync)
    if run is None:
        return int(claimed_index), False

    step = (
        run.steps.filter(index=int(claimed_index), kind="stage")
        .order_by("index")
        .first()
    )
    if step is None:
        return int(claimed_index), False

    terminal_statuses = {
        ForwardExecutionStepStatusChoices.STAGED,
        ForwardExecutionStepStatusChoices.MERGE_QUEUED,
        ForwardExecutionStepStatusChoices.MERGED,
        ForwardExecutionStepStatusChoices.SKIPPED,
        ForwardExecutionStepStatusChoices.CANCELLED,
    }
    if step.status in terminal_statuses:
        return int(claimed_index), False

    incoming_job_id = getattr(job, "pk", None)
    if step.job_id and incoming_job_id and step.job_id != incoming_job_id:
        return int(claimed_index), False

    if int(claimed_index) < int(run.next_step_index or 1):
        return int(claimed_index), False

    return int(claimed_index), True


def _auto_enqueue_merge_for_staged_step(sync, *, user):
    run = active_execution_run(sync) or latest_execution_run(sync)
    if run is None or not bool(run.auto_merge):
        return None
    step = (
        run.steps.filter(
            kind="stage",
            index=int(run.next_step_index or 0),
            status=ForwardExecutionStepStatusChoices.STAGED,
        )
        .select_related("ingestion")
        .order_by("index")
        .first()
    )
    if step is None or step.ingestion is None:
        return None
    return maybe_enqueue_next_branch_stage(step.ingestion, user)


def stage_forward_branch_item(job, *args, **kwargs):
    sync = ForwardSync.objects.get(pk=job.object_id)
    from .utilities.multi_branch import ForwardMultiBranchExecutor

    claimed_index = None
    claimed_model_string = None
    liveness_stop_event = None
    liveness_thread = None
    try:
        job.start()
        sync.logger = SyncLogging(job=job.pk)
        sync.status = ForwardSyncStatusChoices.SYNCING
        sync.__class__.objects.filter(pk=sync.pk).update(status=sync.status)
        run = active_execution_run(sync)
        if run is None and not sync.execution_runs.exists():
            run = upgrade_branch_run_state_to_execution_run(sync)
        if run is not None:
            reconcile_execution_run(run)
            run.refresh_from_db()
            state = branch_run_state_from_execution_run(run)
        elif getattr(sync, "pk", None) and sync.execution_runs.exists():
            prune_stale_branch_run_state(sync)
            sync.logger.log_info(
                "No active Forward Branching execution run is claimable; skipping stage job.",
                obj=sync,
            )
            safe_save_job_data(job, sync)
            job.terminate()
            return
        else:
            state = sync.get_branch_run_state()
        if not state and run is not None:
            state = branch_run_state_from_execution_run(run)
        overlap_stage = bool(kwargs.get("overlap_stage", False))
        current_index = int(state.get("next_plan_index") or 1)
        if overlap_stage and run is not None:
            overlap_step = (
                run.steps.filter(
                    kind="stage",
                    job_id=getattr(job, "pk", None),
                    status__in=[
                        ForwardExecutionStepStatusChoices.QUEUED,
                        ForwardExecutionStepStatusChoices.RUNNING,
                    ],
                )
                .order_by("index")
                .first()
            )
            if overlap_step is not None:
                current_index = int(overlap_step.index)
        claimed_index = current_index
        if run is not None:
            claimed_step = claim_stage_step(sync, current_index, job)
            if claimed_step is None:
                # Reconcile once more and retry claim. This self-heals stale
                # queued/running step rows left by worker restarts or abandoned jobs.
                reconcile_execution_run(run)
                run.refresh_from_db()
                state = branch_run_state_from_execution_run(run)
                current_index = int(state.get("next_plan_index") or current_index or 1)
                if overlap_stage:
                    overlap_step = (
                        run.steps.filter(
                            kind="stage",
                            job_id=getattr(job, "pk", None),
                            status__in=[
                                ForwardExecutionStepStatusChoices.QUEUED,
                                ForwardExecutionStepStatusChoices.RUNNING,
                            ],
                        )
                        .order_by("index")
                        .first()
                    )
                    if overlap_step is not None:
                        current_index = int(overlap_step.index)
                claimed_step = claim_stage_step(sync, current_index, job)
                if claimed_step is None:
                    sync.logger.log_info(
                        "Forward Branching shard is already complete or no longer claimable.",
                        obj=sync,
                    )
                    safe_save_job_data(job, sync)
                    job.terminate()
                    return
            claimed_index = int(claimed_step.index)
            claimed_model_string = str(claimed_step.model_string or "")
        if run is not None and not claimed_model_string and claimed_index:
            claimed_step = (
                run.steps.filter(kind="stage", index=int(claimed_index))
                .order_by("index")
                .first()
            )
            if claimed_step is not None:
                claimed_model_string = str(claimed_step.model_string or "")
        liveness_stop_event, liveness_thread = _stage_liveness_monitor(
            sync=sync,
            logger_=sync.logger,
            shard_index=claimed_index,
            model_string=claimed_model_string,
        )
        executor = ForwardMultiBranchExecutor(
            sync,
            sync.source.get_client(),
            sync.logger,
            user=job.user,
            job=job,
        )
        executor.run_next_plan_item(
            max_changes_per_branch=sync.get_max_changes_per_branch(),
            expected_plan_index=claimed_index,
            claimed_step=claimed_step,
            overlap_stage=overlap_stage,
        )
        queued_merge_job = _auto_enqueue_merge_for_staged_step(sync, user=job.user)
        if queued_merge_job is not None:
            sync.logger.log_info(
                f"Queued merge job {queued_merge_job.pk} for staged shard progression.",
                obj=sync,
            )
        safe_save_job_data(job, sync)
        job.terminate()
    except Exception as exc:
        safe_save_job_data(job, sync)
        timeout = isinstance(exc, JobTimeoutException)
        db_retryable = _is_transient_db_connection_retryable(exc)
        current_index, should_fail_run = _stage_job_failure_target(
            sync,
            claimed_index=claimed_index,
            job=job,
        )
        state = get_branch_run_display_state(sync)
        if not should_fail_run:
            sync.logger.log_warning(
                (
                    "Forward Branching shard job failed after its claimed shard "
                    "was no longer active; preserving the current execution run."
                ),
                obj=sync,
            )
            safe_save_job_data(job, sync)
            job.terminate()
            if isinstance(exc, (ForwardSyncError, SyncError, JobTimeoutException)):
                logger.warning(exc)
                return
            raise

        if db_retryable:
            update_plan_item_state(
                sync,
                current_index,
                status="failed",
                last_error=str(exc),
                retry_count=_plan_item_retry_count(state, current_index) + 1,
            )
            job.terminate(status=JobStatusChoices.STATUS_ERRORED)
            sync.logger.log_warning(
                (
                    "Transient database connection pressure interrupted shard "
                    f"{current_index}; attempting automatic retry."
                ),
                obj=sync,
            )
            update_run_from_branch_state(sync)
            retry_job = enqueue_branch_stage_job(sync, user=job.user, adhoc=True)
            if retry_job is not None:
                sync.logger.log_info(
                    f"Queued automatic stage retry job {retry_job.pk}.",
                    obj=sync,
                )
                safe_save_job_data(job, sync)
                if isinstance(exc, (ForwardSyncError, SyncError, JobTimeoutException)):
                    logger.warning(exc)
                return

        update_plan_item_state(
            sync,
            current_index,
            status="timeout" if timeout else "failed",
            last_error=str(exc),
            retry_count=_plan_item_retry_count(state, current_index) + 1,
        )
        job.terminate(status=JobStatusChoices.STATUS_ERRORED)
        ForwardSync.objects.filter(pk=sync.pk).update(
            status=(
                ForwardSyncStatusChoices.TIMEOUT
                if timeout
                else ForwardSyncStatusChoices.FAILED
            )
        )
        sync.status = (
            ForwardSyncStatusChoices.TIMEOUT
            if timeout
            else ForwardSyncStatusChoices.FAILED
        )
        update_run_from_branch_state(sync)
        ingestion = ForwardIngestion.objects.filter(sync=sync).order_by("-pk").first()
        if timeout:
            message = (
                "Forward Branching shard job timed out. Resume the ingestion to retry "
                "the current shard instead of restarting the baseline."
            )
            record_timeout_issue(
                ingestion,
                ForwardIngestionPhaseChoices.SYNC,
                message,
            )
            sync.logger.log_failure(message, obj=sync)
        if isinstance(exc, (ForwardSyncError, SyncError, JobTimeoutException)):
            logger.error(exc)
        else:
            raise
    finally:
        _stop_stage_liveness_monitor(liveness_stop_event, liveness_thread)
