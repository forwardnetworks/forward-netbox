import logging
from datetime import timedelta

from core.choices import DataSourceStatusChoices
from core.choices import JobStatusChoices
from core.exceptions import SyncError
from core.models import Job
from netbox.context_managers import event_tracking
from rq.timeouts import JobTimeoutException
from utilities.datetime import local_now
from utilities.request import NetBoxFakeRequest

from .models import IPFabricIngestion
from .models import IPFabricSource
from .models import IPFabricSync

logger = logging.getLogger(__name__)


def sync_ipfabricsource(job, *args, **kwargs):
    ipfsource = IPFabricSource.objects.get(pk=job.object_id)

    try:
        job.start()
        ipfsource.sync(job=job)
        job.terminate()
    except Exception as e:
        job.terminate(status=JobStatusChoices.STATUS_ERRORED)
        IPFabricSource.objects.filter(pk=ipfsource.pk).update(
            status=DataSourceStatusChoices.FAILED
        )
        if type(e) in (SyncError, JobTimeoutException):
            logging.error(e)
        else:
            raise e


def sync_ipfabric(job, *args, **kwargs):
    obj = IPFabricSync.objects.get(pk=job.object_id)

    try:
        job.start()
        obj.sync(job=job)
        job.terminate()
    except Exception as e:
        job.terminate(status=JobStatusChoices.STATUS_ERRORED)
        IPFabricSync.objects.filter(pk=obj.pk).update(
            status=DataSourceStatusChoices.FAILED
        )
        if type(e) in (SyncError, JobTimeoutException):
            logging.error(e)
        else:
            raise e
    finally:
        if obj.interval and not kwargs.get("adhoc"):
            new_scheduled_time = local_now() + timedelta(minutes=obj.interval)
            job = Job.enqueue(
                sync_ipfabric,
                name=f"{obj.name} - (scheduled)",
                instance=obj,
                user=obj.user,
                schedule_at=new_scheduled_time,
                interval=obj.interval,
            )


def merge_ipfabric_ingestion(job, remove_branch=False, *args, **kwargs):
    ingestion = IPFabricIngestion.objects.get(pk=job.object_id)
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
        with event_tracking(request):
            ingestion.sync_merge()
        if remove_branch:
            branching_branch = ingestion.branch
            ingestion.branch = None
            ingestion.save()
            branching_branch.delete()
        job.terminate()
    except Exception as e:
        print(e)
        job.terminate(status=JobStatusChoices.STATUS_ERRORED)
        IPFabricSync.objects.filter(pk=ingestion.sync.pk).update(
            status=DataSourceStatusChoices.FAILED
        )
        if type(e) in (SyncError, JobTimeoutException):
            logging.error(e)
        else:
            raise e
