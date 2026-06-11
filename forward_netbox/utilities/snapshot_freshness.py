from core.choices import JobStatusChoices

from ..choices import ForwardSyncStatusChoices
from ..exceptions import ForwardClientError
from .forward_api import LATEST_PROCESSED_SNAPSHOT


ACTIVE_JOB_STATUSES = {
    JobStatusChoices.STATUS_SCHEDULED,
    JobStatusChoices.STATUS_PENDING,
    JobStatusChoices.STATUS_RUNNING,
}


def latest_processed_catchup_decision(
    sync,
    *,
    current_snapshot_id=None,
    client=None,
    current_job=None,
):
    decision = {
        "should_queue": False,
        "reason": "",
        "current_snapshot_id": str(current_snapshot_id or "").strip(),
        "latest_processed_snapshot_id": "",
    }
    if sync.get_snapshot_id() != LATEST_PROCESSED_SNAPSHOT:
        decision["reason"] = "fixed_snapshot_selector"
        return decision
    if sync.status != ForwardSyncStatusChoices.COMPLETED:
        decision["reason"] = "sync_not_completed"
        return decision

    if not decision["current_snapshot_id"]:
        from ..models import ForwardIngestion

        latest_ingestion = (
            ForwardIngestion.objects.filter(sync=sync)
            .order_by("-pk")
            .only("snapshot_id")
            .first()
            if getattr(sync, "pk", None)
            else None
        )
        decision["current_snapshot_id"] = str(
            getattr(latest_ingestion, "snapshot_id", "") or ""
        ).strip()
    if not decision["current_snapshot_id"]:
        decision["reason"] = "missing_current_snapshot_id"
        return decision

    network_id = sync.get_network_id()
    if not network_id:
        decision["reason"] = "missing_network_id"
        return decision

    client = client or sync.source.get_client()
    try:
        latest_processed_snapshot_id = str(
            client.get_latest_processed_snapshot_id(network_id) or ""
        ).strip()
    except ForwardClientError:
        decision["reason"] = "latest_processed_lookup_failed"
        return decision
    decision["latest_processed_snapshot_id"] = latest_processed_snapshot_id
    if not latest_processed_snapshot_id:
        decision["reason"] = "missing_latest_processed_snapshot_id"
        return decision
    if latest_processed_snapshot_id == decision["current_snapshot_id"]:
        decision["reason"] = "already_current"
        return decision

    if getattr(sync, "pk", None):
        active_jobs = sync.jobs.filter(status__in=ACTIVE_JOB_STATUSES)
        if current_job is not None and getattr(current_job, "pk", None):
            active_jobs = active_jobs.exclude(pk=current_job.pk)
        if active_jobs.exists():
            decision["reason"] = "active_job_exists"
            return decision

    decision["should_queue"] = True
    decision["reason"] = "latest_processed_advanced"
    return decision
