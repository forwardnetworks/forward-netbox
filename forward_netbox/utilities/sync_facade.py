from core.choices import JobStatusChoices
from core.exceptions import SyncError
from core.models import Job
from django.utils.module_loading import import_string

from ..choices import forward_configured_models
from ..choices import FORWARD_OPTIONAL_MODELS
from ..choices import ForwardDiffFallbackModeChoices
from ..choices import ForwardExecutionBackendChoices
from ..choices import ForwardSyncStatusChoices
from ..exceptions import ForwardSyncError
from .branch_budget import DEFAULT_MAX_CHANGES_PER_BRANCH
from .forward_api import LATEST_COLLECTED_SNAPSHOT
from .forward_api import LATEST_PROCESSED_SNAPSHOT
from .sync_state import get_max_changes_per_branch as get_state_max_changes_per_branch


DEFAULT_ENABLE_BULK_ORM_FOR_NEW_SYNCS = True


def normalize_forward_sync(sync):
    parameters = dict(sync.parameters or {})
    parameters["execution_backend"] = ForwardExecutionBackendChoices.SINGLE_BRANCH
    parameters["diff_fallback_mode"] = parameters.get(
        "diff_fallback_mode",
        ForwardDiffFallbackModeChoices.ALLOW_FALLBACK,
    )
    if "enable_bulk_orm" not in parameters:
        parameters["enable_bulk_orm"] = DEFAULT_ENABLE_BULK_ORM_FOR_NEW_SYNCS
    max_changes_per_branch = get_state_max_changes_per_branch(
        sync,
        DEFAULT_MAX_CHANGES_PER_BRANCH,
    )
    parameters["max_changes_per_branch"] = max(1, max_changes_per_branch)
    sync.auto_merge = bool(parameters.get("auto_merge", sync.auto_merge))
    sync.parameters = parameters


def device_tag_scope(sync):
    """Return (include_tags, exclude_tags, include_match) from the source params.

    Mirrors the normalization used by the live query fetch path so the
    latestCollected probe scopes to the same devices the sync would fetch.
    """
    source_parameters = dict(getattr(sync.source, "parameters", {}) or {})
    include_tags = source_parameters.get("device_tag_include_tags") or []
    exclude_tags = source_parameters.get("device_tag_exclude_tags") or []
    if not include_tags and source_parameters.get("device_tag_include"):
        include_tags = [source_parameters.get("device_tag_include")]
    if not exclude_tags and source_parameters.get("device_tag_exclude"):
        exclude_tags = [source_parameters.get("device_tag_exclude")]
    include_tags = [str(tag).strip() for tag in include_tags if str(tag).strip()]
    exclude_tags = [str(tag).strip() for tag in exclude_tags if str(tag).strip()]
    include_match = str(
        source_parameters.get("device_tag_include_match") or "any"
    ).strip()
    if include_match not in {"any", "all"}:
        include_match = "any"
    return include_tags, exclude_tags, include_match


def resolve_snapshot_id(sync, client=None):
    snapshot_id = sync.get_snapshot_id()
    if snapshot_id not in {LATEST_PROCESSED_SNAPSHOT, LATEST_COLLECTED_SNAPSHOT}:
        return snapshot_id
    client = client or sync.source.get_client()
    network_id = sync.get_network_id()
    if not network_id:
        raise ForwardSyncError(
            "Forward sync requires a network on the source before resolving "
            f"{snapshot_id}."
        )
    if snapshot_id == LATEST_COLLECTED_SNAPSHOT:
        include_tags, exclude_tags, include_match = device_tag_scope(sync)
        return client.get_latest_collected_snapshot_id(
            network_id,
            include_tags=include_tags,
            exclude_tags=exclude_tags,
            include_match=include_match,
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
    source_parameters = dict(
        getattr(getattr(sync, "source", None), "parameters", {}) or {}
    )
    filter_mode = str(
        source_parameters.get("device_tag_filter_mode") or "local"
    ).strip()
    if filter_mode != "query_parameters":
        return {}
    include_tags = source_parameters.get("device_tag_include_tags") or []
    exclude_tags = source_parameters.get("device_tag_exclude_tags") or []
    if not include_tags and source_parameters.get("device_tag_include"):
        include_tags = [source_parameters.get("device_tag_include")]
    if not exclude_tags and source_parameters.get("device_tag_exclude"):
        exclude_tags = [source_parameters.get("device_tag_exclude")]
    include_tags = [str(tag).strip() for tag in include_tags if str(tag).strip()]
    exclude_tags = [str(tag).strip() for tag in exclude_tags if str(tag).strip()]
    include_match = str(
        source_parameters.get("device_tag_include_match") or "any"
    ).strip()
    if include_match not in {"any", "all"}:
        include_match = "any"
    query_parameters = {}
    if len(include_tags) == 1:
        query_parameters["device_tag_include"] = include_tags[0]
    if len(exclude_tags) == 1:
        query_parameters["device_tag_exclude"] = exclude_tags[0]
    if include_tags:
        query_parameters["device_tag_include_tags"] = include_tags
        query_parameters["device_tag_include_match"] = include_match
    if exclude_tags:
        query_parameters["device_tag_exclude_tags"] = exclude_tags
    return query_parameters


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


def enqueue_validation_job(
    sync, adhoc=False, user=None, schedule_at=None, interval=None
):
    if not user:
        user = sync.user
    if schedule_at or interval:
        # Standing schedule: one per sync (enqueue_once dedup keys on the
        # ValidationJob fixed name + the sync instance); recurrence is handled
        # by JobRunner after each run completes. Cancel by deleting the
        # scheduled job from the Jobs list. Pass schedule_at through untouched:
        # core dedup keeps the existing row only when schedule_at is falsy or
        # matches, so defaulting it to now() here would delete + recreate the
        # schedule on every re-post instead of being idempotent.
        from ..jobs import ValidationJob

        return ValidationJob.enqueue_once(
            instance=sync,
            user=user,
            schedule_at=schedule_at,
            interval=interval,
        )
    return Job.enqueue(
        import_string("forward_netbox.jobs.validate_forwardsync"),
        instance=sync,
        user=user,
        name=f"{sync.name} - validation",
        adhoc=adhoc,
        schedule_at=None,
        interval=None,
    )


def enqueue_preview_schedule(sync, user=None, schedule_at=None, interval=None):
    """Standing dependency-preview schedule (immediate runs use
    enqueue_button_job, which keeps the legacy per-sync job name). schedule_at
    passes through untouched so enqueue_once re-posts stay idempotent (see
    enqueue_validation_job); interval-only means run now, then recur."""
    from ..jobs import DependencyPreviewJob

    if not user:
        user = sync.user
    return DependencyPreviewJob.enqueue_once(
        instance=sync,
        user=user,
        schedule_at=schedule_at,
        interval=interval,
    )


class JobAlreadyActive(Exception):
    """An equivalent job is already pending/running for this sync."""

    def __init__(self, job):
        self.job = job
        super().__init__(f"Job `{job.name}` is already {job.status} (job #{job.pk}).")


# The operator-facing background jobs ("button jobs") share one enqueue path so
# the HTML buttons and the REST API actions get identical job names (several
# lookups match on these strings - do NOT rename), permission expectations, and
# overlap behavior.
BUTTON_JOB_SPECS = {
    "dependency_preview": (
        "forward_netbox.jobs.forward_dependency_preview",
        "dependency preview",
        "forward_netbox.run_forwardsync",
    ),
    "prune_orphans": (
        "forward_netbox.jobs.prune_forward_orphans",
        "prune orphans",
        "dcim.delete_device",
    ),
    "tag_delete_eligible_ipam": (
        "forward_netbox.jobs.tag_forward_delete_eligible_ipam",
        "tag delete-eligible IPAM",
        "ipam.change_prefix",
    ),
    "create_module_bays": (
        "forward_netbox.jobs.create_forward_module_bays",
        "create module bays",
        "dcim.add_modulebay",
    ),
}

_ACTIVE_JOB_STATUSES = (
    JobStatusChoices.STATUS_PENDING,
    JobStatusChoices.STATUS_RUNNING,
)


def button_job_permission(kind):
    return BUTTON_JOB_SPECS[kind][2]


def enqueue_button_job(sync, kind, user, *, name_suffix_extra="", during_sync_ok=False):
    """Enqueue an operator button job with a shared overlap guard.

    Raises ``JobAlreadyActive`` instead of stacking a duplicate when an
    equivalent job is already pending/running. The guard is a PREFIX match on
    the job name so variants block each other (a manual "prune orphans" click
    refuses while "prune orphans (auto)" runs, and vice versa). Pruning also
    refuses while the sync itself is queued/running - deleting devices
    mid-ingest would race the apply. ``during_sync_ok`` skips only that
    sync-running check: the post-sync auto-prune hook enqueues from INSIDE the
    still-running sync job, which is safe by construction (the sync's apply
    work is already complete).
    """
    dotted_path, suffix, _permission = BUTTON_JOB_SPECS[kind]
    name = f"{sync.name} - {suffix}"
    active = (
        sync.jobs.filter(name__startswith=name, status__in=_ACTIVE_JOB_STATUSES)
        .order_by("pk")
        .first()
    )
    if active is not None:
        raise JobAlreadyActive(active)
    if kind == "prune_orphans" and not during_sync_ok:
        for run_suffix in ("adhoc", "scheduled"):
            running_sync = (
                sync.jobs.filter(
                    name=f"{sync.name} - {run_suffix}",
                    status__in=_ACTIVE_JOB_STATUSES,
                )
                .order_by("pk")
                .first()
            )
            if running_sync is not None:
                raise JobAlreadyActive(running_sync)
    return Job.enqueue(
        import_string(dotted_path),
        instance=sync,
        user=user,
        name=f"{name}{name_suffix_extra}",
    )
