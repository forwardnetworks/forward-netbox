import logging
from datetime import timedelta

from core.choices import JobStatusChoices
from core.exceptions import SyncError
from dcim.models import Site
from dcim.models import VirtualChassis
from dcim.signals import assign_virtualchassis_master
from django.db.models import signals
from netbox.context_managers import event_tracking
from rq.timeouts import JobTimeoutException
from utilities.datetime import local_now
from utilities.request import NetBoxFakeRequest

from .choices import ForwardIngestionPhaseChoices
from .choices import ForwardSyncStatusChoices
from .models import ForwardIngestion
from .models import ForwardIngestionIssue
from .models import ForwardSync
from .utilities.logging import SyncLogging

logger = logging.getLogger(__name__)

try:
    from dcim.signals import sync_cached_scope_fields
except ImportError:  # pragma: no cover - compatibility with older NetBox point releases
    sync_cached_scope_fields = None


def safe_save_job_data(job, obj_with_logger):
    try:
        if hasattr(obj_with_logger, "logger") and hasattr(
            obj_with_logger.logger, "log_data"
        ):
            job.data = obj_with_logger.logger.log_data
            job.save(update_fields=["data"])
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
        if type(exc) in (SyncError, JobTimeoutException):
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
                    sync.status = ForwardSyncStatusChoices.QUEUED
                    sync.full_clean()
                    sync.save()
                logger.info(
                    "Scheduled next sync for ForwardSync %s at %s.",
                    sync.pk,
                    new_scheduled_time,
                )


def merge_forwardingestion(job, remove_branch=False, *args, **kwargs):
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
            try:
                signals.post_save.disconnect(
                    assign_virtualchassis_master,
                    sender=VirtualChassis,
                )
                if sync_cached_scope_fields is not None:
                    signals.post_save.disconnect(sync_cached_scope_fields, sender=Site)
                ingestion.sync_merge()
            finally:
                signals.post_save.connect(
                    assign_virtualchassis_master,
                    sender=VirtualChassis,
                )
                if sync_cached_scope_fields is not None:
                    signals.post_save.connect(sync_cached_scope_fields, sender=Site)

        if (
            remove_branch
            and ingestion.sync.status != ForwardSyncStatusChoices.FAILED
            and ingestion.branch
        ):
            branching_branch = ingestion.branch
            ingestion.branch = None
            ingestion.save(update_fields=["branch"])
            branching_branch.delete()

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
            ingestion.sync.logger.log_failure(message, obj=ingestion)
        if type(exc) in (SyncError, JobTimeoutException):
            logger.error(exc)
        else:
            raise
