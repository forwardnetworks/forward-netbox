import uuid
from functools import partial

import django_rq
from core.choices import JobNotificationChoices
from core.choices import JobStatusChoices
from core.models import Job
from core.models import ObjectType
from django.db import transaction
from django_pglocks import advisory_lock
from netbox.constants import ADVISORY_LOCK_KEYS
from utilities.rqworker import get_queue_for_model

from .runtime_guidance import effective_forward_job_timeout


def _dispatch_persisted_job(job_pk, func, kwargs, job_timeout=None):
    """Dispatch only while the persisted job and its bound object still exist."""
    with advisory_lock(ADVISORY_LOCK_KEYS["job-schedules"]):
        with transaction.atomic():
            job = Job.objects.select_for_update().filter(pk=job_pk).first()
            if job is None or job.status not in (
                JobStatusChoices.STATUS_PENDING,
                JobStatusChoices.STATUS_SCHEDULED,
            ):
                return None
            if job.object_type_id is not None and job.object is None:
                job.delete()
                return None

            queue = django_rq.get_queue(job.queue_name)
            enqueue_kwargs = {
                "job_id": str(job.job_id),
                "job": job,
                **kwargs,
                "job_timeout": max(
                    effective_forward_job_timeout(),
                    int(job_timeout or 0),
                ),
            }
            if job.scheduled is not None:
                return queue.enqueue_at(job.scheduled, func, **enqueue_kwargs)
            return queue.enqueue(func, **enqueue_kwargs)


def enqueue_forward_job(
    func,
    instance=None,
    name="",
    user=None,
    schedule_at=None,
    interval=None,
    immediate=False,
    queue_name=None,
    notifications=None,
    job_timeout=None,
    **kwargs,
):
    """Persist and dispatch a plugin job under one serialized lifecycle.

    NetBox defers the Redis write until the surrounding transaction commits.
    The commit callback must reacquire the same lock used by job start and
    ForwardSync deletion; otherwise a form transaction can enqueue work after
    its bound sync and Job row were concurrently deleted.
    """
    if schedule_at is not None and immediate:
        raise ValueError(
            "enqueue_forward_job() cannot combine schedule_at and immediate."
        )

    with advisory_lock(ADVISORY_LOCK_KEYS["job-schedules"]):
        if instance is not None:
            object_type = ObjectType.objects.get_for_model(
                instance,
                for_concrete_model=False,
            )
            object_id = instance.pk
        else:
            object_type = object_id = None
        rq_queue_name = queue_name or get_queue_for_model(
            object_type.model if object_type else None
        )
        django_rq.get_queue(rq_queue_name)
        job = Job(
            object_type=object_type,
            object_id=object_id,
            name=name,
            status=(
                JobStatusChoices.STATUS_SCHEDULED
                if schedule_at is not None
                else JobStatusChoices.STATUS_PENDING
            ),
            scheduled=schedule_at,
            interval=interval,
            user=user,
            job_id=uuid.uuid4(),
            queue_name=rq_queue_name,
            notifications=(
                notifications
                if notifications is not None
                else JobNotificationChoices.NOTIFICATION_ALWAYS
            ),
        )
        job.full_clean()
        job.save()

        if immediate:
            func(job_id=str(job.job_id), job=job, **kwargs)
        else:
            transaction.on_commit(
                partial(
                    _dispatch_persisted_job,
                    job.pk,
                    func,
                    dict(kwargs),
                    job_timeout,
                )
            )
        return job
