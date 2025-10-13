import logging
from datetime import timedelta

from core.choices import DataSourceStatusChoices
from core.choices import JobStatusChoices
from core.exceptions import SyncError
from core.models import Job
from django.contrib.auth import get_user_model
from netbox.context import current_request
from netbox.context_managers import event_tracking
from rq.timeouts import JobTimeoutException
from utilities.datetime import local_now
from utilities.request import NetBoxFakeRequest

from .models import ForwardIngestion
from .models import ForwardSource
from .models import ForwardSync

logger = logging.getLogger(__name__)


def sync_forwardsource(job, *args, **kwargs):
    fwdsource = ForwardSource.objects.get(pk=job.object_id)

    request_token = None
    try:
        user = job.user
        if not user:
            User = get_user_model()
            user = (
                User.objects.filter(is_active=True, is_superuser=True)
                .order_by("pk")
                .first()
            )
            if not user:
                raise SyncError(
                    "Cannot sync snapshots: no user context available. Provide a user or create a superuser."
                )
            job.user = user

        request_token = current_request.set(
            NetBoxFakeRequest({"id": job.job_id, "user": user})
        )
        job.start()
        fwdsource.sync(job=job)
        job.terminate()
    except Exception as e:
        job.terminate(status=JobStatusChoices.STATUS_ERRORED)
        ForwardSource.objects.filter(pk=fwdsource.pk).update(
            status=DataSourceStatusChoices.FAILED
        )
        if type(e) in (SyncError, JobTimeoutException):
            logging.error(e)
        else:
            raise e
    finally:
        if request_token:
            current_request.reset(request_token)


def sync_forward(job, *args, **kwargs):
    obj = ForwardSync.objects.get(pk=job.object_id)

    try:
        job.start()
        obj.sync(job=job)
        job.terminate()
    except Exception as e:
        job.terminate(status=JobStatusChoices.STATUS_ERRORED)
        ForwardSync.objects.filter(pk=obj.pk).update(
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
                sync_forward,
                name=f"{obj.name} - (scheduled)",
                instance=obj,
                user=obj.user,
                schedule_at=new_scheduled_time,
                interval=obj.interval,
            )


def merge_forward_ingestion(job, remove_branch=False, *args, **kwargs):
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
        ForwardSync.objects.filter(pk=ingestion.sync.pk).update(
            status=DataSourceStatusChoices.FAILED
        )
        if type(e) in (SyncError, JobTimeoutException):
            logging.error(e)
        else:
            raise e
