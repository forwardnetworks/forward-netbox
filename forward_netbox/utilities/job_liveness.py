from datetime import datetime
from datetime import timedelta

from core.choices import JobStatusChoices
from django.utils import timezone


STARTED_JOB_HEARTBEAT_STALE_SECONDS = 180


def job_has_live_execution(job) -> bool:
    """Return whether a NetBox core Job still has live queued/running work."""
    if job is None:
        return False
    if getattr(job, "completed", None) is not None:
        return False
    status = getattr(job, "status", "")
    if status in {
        JobStatusChoices.STATUS_COMPLETED,
        JobStatusChoices.STATUS_ERRORED,
        JobStatusChoices.STATUS_FAILED,
    }:
        return False

    rq_state = _rq_job_is_active(job)
    if rq_state is not None:
        return rq_state

    # If RQ cannot be inspected, preserve the historical NetBox-row behavior.
    return True


def _rq_job_is_active(job) -> bool | None:
    rq_job_id = str(getattr(job, "job_id", "") or "").strip()
    if not rq_job_id:
        return None
    queue_name = str(getattr(job, "queue_name", "") or "default").strip() or "default"
    try:
        import django_rq
        from rq.registry import DeferredJobRegistry
        from rq.registry import ScheduledJobRegistry
        from rq.registry import StartedJobRegistry
    except Exception:
        return None

    try:
        queue = django_rq.get_queue(queue_name)
        active_ids = set(_string_ids(getattr(queue, "job_ids", []) or []))
        started_ids = set()
        for registry_class in (
            StartedJobRegistry,
            DeferredJobRegistry,
            ScheduledJobRegistry,
        ):
            registry = registry_class(queue.name, connection=queue.connection)
            registry_ids = set(_string_ids(registry.get_job_ids()))
            active_ids.update(registry_ids)
            if registry_class is StartedJobRegistry:
                started_ids = registry_ids
        if rq_job_id not in active_ids:
            return False
        if rq_job_id in started_ids:
            stale = _rq_started_job_is_stale(queue, rq_job_id)
            if stale is True:
                return False
        return True
    except Exception:
        return None


def _string_ids(values):
    return [
        str(value.decode() if isinstance(value, bytes) else value) for value in values
    ]


def _rq_started_job_is_stale(queue, rq_job_id):
    try:
        from rq.job import Job as RQJob
    except Exception:
        return None
    try:
        rq_job = RQJob.fetch(rq_job_id, connection=queue.connection)
        status = str(rq_job.get_status(refresh=True) or "")
        return _started_job_heartbeat_stale(
            status=status,
            last_heartbeat=getattr(rq_job, "last_heartbeat", None),
            started_at=getattr(rq_job, "started_at", None),
        )
    except Exception:
        return None


def _started_job_heartbeat_stale(
    *,
    status,
    last_heartbeat,
    started_at=None,
    now=None,
    threshold_seconds=STARTED_JOB_HEARTBEAT_STALE_SECONDS,
):
    status_text = str(status or "").strip().lower()
    if "started" not in status_text:
        return False
    heartbeat_at = last_heartbeat or started_at
    if heartbeat_at is None:
        return None
    if isinstance(heartbeat_at, datetime) and timezone.is_naive(heartbeat_at):
        heartbeat_at = timezone.make_aware(
            heartbeat_at, timezone.get_current_timezone()
        )
    now = now or timezone.now()
    stale_after = timedelta(seconds=max(1, int(threshold_seconds or 1)))
    return (now - heartbeat_at) > stale_after
