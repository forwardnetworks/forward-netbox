import logging
from datetime import datetime
from datetime import timedelta

from core.choices import JobStatusChoices
from core.exceptions import SyncError
from core.models import ObjectType
from netbox.context_managers import event_tracking
from rq.timeouts import JobTimeoutException
from utilities.datetime import local_now
from utilities.request import NetBoxFakeRequest

from .choices import ForwardIngestionPhaseChoices
from .choices import ForwardSyncStatusChoices
from .exceptions import ForwardSyncError
from .models import ForwardIngestion
from .models import ForwardIngestionIssue
from .models import ForwardSync
from .utilities.ingestion_merge import maybe_enqueue_next_branch_stage
from .utilities.json_safe import json_safe_value
from .utilities.logging import SyncLogging
from .utilities.resumable_branching import update_plan_item_state
from .utilities.validation import ForwardValidationRunner

logger = logging.getLogger(__name__)


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
        sync.sync(job=job)
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
                        "user": sync.user,
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


def merge_forwardingestion(job, remove_branch=True, *args, **kwargs):
    ingestion = ForwardIngestion.objects.get(pk=job.object_id)
    try:
        request = NetBoxFakeRequest(
            {
                "META": {},
                "POST": ingestion.sync.parameters,
                "GET": {},
                "FILES": {},
                "user": ingestion.sync.user,
                "path": "",
                "id": job.job_id,
            }
        )

        job.start()
        ingestion.merge_job = job
        ingestion.save(update_fields=["merge_job"])
        ingestion.sync.logger = SyncLogging(job=job.pk)
        with event_tracking(request):
            ingestion.sync_merge(remove_branch=remove_branch)
        maybe_enqueue_next_branch_stage(ingestion, job.user)

        safe_save_job_data(job, ingestion.sync)
        job.terminate()
    except Exception as exc:
        logger.exception(
            "Error during merge for ForwardIngestion %s: %s", ingestion.pk, exc
        )
        safe_save_job_data(job, ingestion.sync)
        timeout = isinstance(exc, JobTimeoutException)
        job.terminate(status=JobStatusChoices.STATUS_ERRORED)
        ForwardSync.objects.filter(pk=ingestion.sync.pk).update(
            status=(
                ForwardSyncStatusChoices.TIMEOUT
                if timeout
                else ForwardSyncStatusChoices.FAILED
            )
        )
        if timeout:
            message = "Forward merge job timed out. Increase RQ worker timeout and rerun the merge."
            record_timeout_issue(
                ingestion,
                ForwardIngestionPhaseChoices.MERGE,
                message,
            )
            state = ingestion.sync.get_branch_run_state()
            pending_index = int(state.get("pending_plan_index") or 0)
            if pending_index:
                update_plan_item_state(
                    ingestion.sync,
                    pending_index,
                    status="merge_timeout",
                    last_error=message,
                    retry_count=_plan_item_retry_count(state, pending_index) + 1,
                )
            ingestion.sync.logger.log_failure(message, obj=ingestion)
        if type(exc) in (SyncError, JobTimeoutException):
            logger.error(exc)
        else:
            raise


def _plan_item_retry_count(state, index):
    for item in state.get("plan_items") or []:
        if int(item.get("index") or 0) == int(index):
            return int(item.get("retry_count") or 0)
    return 0


def stage_forward_branch_item(job, *args, **kwargs):
    sync = ForwardSync.objects.get(pk=job.object_id)
    from .utilities.multi_branch import ForwardMultiBranchExecutor

    try:
        job.start()
        sync.logger = SyncLogging(job=job.pk)
        executor = ForwardMultiBranchExecutor(
            sync,
            sync.source.get_client(),
            sync.logger,
            user=job.user,
            job=job,
        )
        executor.run_next_plan_item(
            max_changes_per_branch=sync.get_max_changes_per_branch()
        )
        safe_save_job_data(job, sync)
        job.terminate()
    except Exception as exc:
        safe_save_job_data(job, sync)
        timeout = isinstance(exc, JobTimeoutException)
        state = sync.get_branch_run_state()
        current_index = int(
            state.get("pending_plan_index") or state.get("next_plan_index") or 1
        )
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
