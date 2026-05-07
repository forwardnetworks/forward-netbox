from core.exceptions import SyncError
from core.models import Job
from django.utils.module_loading import import_string

from ..choices import FORWARD_OPTIONAL_MODELS
from ..choices import ForwardSyncStatusChoices
from ..choices import forward_configured_models
from ..exceptions import ForwardSyncError
from .branch_budget import DEFAULT_MAX_CHANGES_PER_BRANCH
from .forward_api import LATEST_PROCESSED_SNAPSHOT
from .sync_state import get_max_changes_per_branch as get_state_max_changes_per_branch


def normalize_forward_sync(sync):
    parameters = dict(sync.parameters or {})
    parameters["multi_branch"] = True
    max_changes_per_branch = get_state_max_changes_per_branch(
        sync,
        DEFAULT_MAX_CHANGES_PER_BRANCH,
    )
    parameters["max_changes_per_branch"] = max(1, max_changes_per_branch)
    sync.auto_merge = bool(parameters.get("auto_merge", sync.auto_merge))
    sync.parameters = parameters


def resolve_snapshot_id(sync, client=None):
    snapshot_id = sync.get_snapshot_id()
    if snapshot_id != LATEST_PROCESSED_SNAPSHOT:
        return snapshot_id
    client = client or sync.source.get_client()
    network_id = sync.get_network_id()
    if not network_id:
        raise ForwardSyncError(
            "Forward sync requires a network on the source before resolving latestProcessed."
        )
    return client.get_latest_processed_snapshot_id(network_id)


def get_maps(sync):
    from ..models import ForwardNQEMap

    return list(
        ForwardNQEMap.objects.select_related("netbox_model")
        .filter(enabled=True)
        .order_by("weight", "pk")
    )


def get_query_parameters(sync):
    return {}


def uses_multi_branch(sync):
    return True


def is_model_enabled(sync, model_string):
    if model_string not in forward_configured_models():
        return False
    parameters = sync.parameters or {}
    return parameters.get(model_string, model_string not in FORWARD_OPTIONAL_MODELS)


def enabled_models(sync):
    return [
        model_string
        for model_string in forward_configured_models()
        if is_model_enabled(sync, model_string)
    ]


def get_model_strings(sync):
    return enabled_models(sync)


def enqueue_sync_job(sync, adhoc=False, user=None):
    if sync.is_waiting_for_branch_merge:
        raise SyncError(
            "Forward sync is waiting for the current shard branch to be merged."
        )
    if not user:
        user = sync.user
    if adhoc or sync.status == ForwardSyncStatusChoices.NEW:
        sync.status = ForwardSyncStatusChoices.QUEUED
        sync.__class__.objects.filter(pk=sync.pk).update(status=sync.status)
    return Job.enqueue(
        import_string("forward_netbox.jobs.sync_forwardsync"),
        instance=sync,
        user=user,
        name=f"{sync.name} - {'adhoc' if adhoc else 'scheduled'}",
        adhoc=adhoc,
        schedule_at=None if adhoc else sync.scheduled,
        interval=None if adhoc else sync.interval,
    )


def enqueue_validation_job(sync, adhoc=False, user=None):
    if not user:
        user = sync.user
    return Job.enqueue(
        import_string("forward_netbox.jobs.validate_forwardsync"),
        instance=sync,
        user=user,
        name=f"{sync.name} - validation",
        adhoc=adhoc,
        schedule_at=None,
        interval=None,
    )
