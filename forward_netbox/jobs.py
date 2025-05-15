import logging
from datetime import timedelta

from core.choices import DataSourceStatusChoices, JobStatusChoices
from core.exceptions import SyncError
from core.models import Job
from netbox.context_managers import event_tracking
from rq.timeouts import JobTimeoutException
from utilities.datetime import local_now
from utilities.request import NetBoxFakeRequest

from .models import ForwardSource, ForwardSync
from .utilities.logging import SyncLogging

logger = logging.getLogger(__name__)


def sync_forwardsource(job, adhoc=False):
    """Job to sync snapshots from Forward Enterprise into ForwardSnapshot model."""
    fwdsource = ForwardSource.objects.get(pk=job.object_id)

    try:
        job.start()
        fwdsource.sync(job=job)
        job.terminate()
    except Exception as e:
        job.terminate(status=JobStatusChoices.STATUS_ERRORED)
        ForwardSource.objects.filter(pk=fwdsource.pk).update(status=DataSourceStatusChoices.FAILED)
        if isinstance(e, (SyncError, JobTimeoutException)):
            logger.error(e)
        else:
            raise


def sync_forward(job, adhoc=False):
    """Job to ingest data from a ForwardSnapshot into NetBox using ForwardSync."""
    forwardsync = ForwardSync.objects.get(pk=job.object_id)
    logger_obj = SyncLogging(job=job.pk)

    try:
        job.start()
        forwardsync.logger = logger_obj
        forwardsync.run_ingestion(job=job)
        job.data = logger_obj.log_data
        job.terminate()
    except Exception as e:
        job.terminate(status=JobStatusChoices.STATUS_ERRORED)
        ForwardSync.objects.filter(pk=forwardsync.pk).update(status=DataSourceStatusChoices.FAILED)
        if isinstance(e, (SyncError, JobTimeoutException)):
            logger.error(e)
        else:
            raise
    finally:
        if forwardsync.interval and not adhoc:
            new_scheduled_time = local_now() + timedelta(minutes=forwardsync.interval)
            Job.enqueue(
                sync_forward,
                name=f"{forwardsync.name} - (scheduled)",
                instance=forwardsync,
                user=forwardsync.user,
                schedule_at=new_scheduled_time,
                interval=forwardsync.interval,
            )