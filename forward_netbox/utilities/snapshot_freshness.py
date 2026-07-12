from core.choices import JobStatusChoices

from ..choices import ForwardSyncStatusChoices
from ..exceptions import ForwardClientError
from ..exceptions import ForwardConnectivityError
from ..exceptions import ForwardQueryError
from .forward_api import LATEST_COLLECTED_SNAPSHOT
from .forward_api import LATEST_PROCESSED_SNAPSHOT
from .sync_facade import device_tag_scope
from .sync_facade import sync_run_job_names


ACTIVE_JOB_STATUSES = {
    JobStatusChoices.STATUS_SCHEDULED,
    JobStatusChoices.STATUS_PENDING,
    JobStatusChoices.STATUS_RUNNING,
}

DYNAMIC_SNAPSHOT_SELECTORS = {LATEST_PROCESSED_SNAPSHOT, LATEST_COLLECTED_SNAPSHOT}


def _resolve_latest_snapshot_id(sync, selector, network_id, client):
    """Resolve the catch-up target snapshot for a dynamic selector.

    latestProcessed -> newest processed snapshot. latestCollected -> newest
    snapshot with a freshly-collected in-scope device (scoped to the source
    device-tag filter, same as the sync's own resolution).
    """
    if selector == LATEST_COLLECTED_SNAPSHOT:
        include_tags, exclude_tags, include_match = device_tag_scope(sync)
        return str(
            client.get_latest_collected_snapshot_id(
                network_id,
                include_tags=include_tags,
                exclude_tags=exclude_tags,
                include_match=include_match,
            )
            or ""
        ).strip()
    return str(client.get_latest_processed_snapshot_id(network_id) or "").strip()


def latest_processed_catchup_decision(
    sync,
    *,
    current_snapshot_id=None,
    client=None,
    current_job=None,
):
    selector = sync.get_snapshot_id()
    decision = {
        "should_queue": False,
        "reason": "",
        "snapshot_selector": selector,
        "current_snapshot_id": str(current_snapshot_id or "").strip(),
        "latest_processed_snapshot_id": "",
    }
    if selector not in DYNAMIC_SNAPSHOT_SELECTORS:
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
        latest_snapshot_id = _resolve_latest_snapshot_id(
            sync, selector, network_id, client
        )
    except (ForwardClientError, ForwardConnectivityError, ForwardQueryError):
        # latestCollected raises when no recent snapshot has a collected in-scope
        # device; treat that like a failed lookup (no catch-up) rather than error.
        decision["reason"] = "latest_processed_lookup_failed"
        return decision
    decision["latest_processed_snapshot_id"] = latest_snapshot_id
    if not latest_snapshot_id:
        decision["reason"] = "missing_latest_processed_snapshot_id"
        return decision
    if latest_snapshot_id == decision["current_snapshot_id"]:
        decision["reason"] = "already_current"
        return decision

    if getattr(sync, "pk", None):
        # Only an actual sync RUN suppresses catch-up. The sync's job set also
        # holds standing-schedule rows (fixed names, permanently SCHEDULED) and
        # button jobs; matching on status alone would disable catch-up forever
        # once any standing schedule exists.
        active_jobs = sync.jobs.filter(
            status__in=ACTIVE_JOB_STATUSES,
            name__in=sync_run_job_names(sync),
        )
        if current_job is not None and getattr(current_job, "pk", None):
            active_jobs = active_jobs.exclude(pk=current_job.pk)
        if active_jobs.exists():
            decision["reason"] = "active_job_exists"
            return decision

    decision["should_queue"] = True
    decision["reason"] = "latest_processed_advanced"
    return decision
