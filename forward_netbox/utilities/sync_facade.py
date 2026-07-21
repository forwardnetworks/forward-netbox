from core.choices import JobStatusChoices
from core.exceptions import SyncError
from django.db import transaction as db_transaction
from django.db.models import Q
from django.utils.module_loading import import_string
from django_pglocks import advisory_lock
from netbox.constants import ADVISORY_LOCK_KEYS

from ..choices import forward_configured_models
from ..choices import FORWARD_OPTIONAL_MODELS
from ..choices import ForwardDiffFallbackModeChoices
from ..choices import ForwardSyncStatusChoices
from ..exceptions import ForwardSyncError
from .branch_budget import DEFAULT_MAX_CHANGES_PER_STAGING_ITEM
from .forward_api import LATEST_COLLECTED_SNAPSHOT
from .forward_api import LATEST_PROCESSED_SNAPSHOT
from .job_queue import enqueue_forward_job
from .sync_state import (
    get_max_changes_per_staging_item as get_state_max_changes_per_staging_item,
)


DEFAULT_ENABLE_BULK_ORM_FOR_NEW_SYNCS = True


def effective_scope_endpoints_by_include_tags(source_parameters):
    """Return endpoint include-scope behavior; missing state fails closed."""
    parameters = dict(source_parameters or {})
    return bool(parameters.get("scope_endpoints_by_include_tags", True))


def normalize_forward_sync(sync):
    parameters = dict(sync.parameters or {})
    parameters["diff_fallback_mode"] = parameters.get(
        "diff_fallback_mode",
        ForwardDiffFallbackModeChoices.ALLOW_FALLBACK,
    )
    if "enable_bulk_orm" not in parameters:
        parameters["enable_bulk_orm"] = DEFAULT_ENABLE_BULK_ORM_FOR_NEW_SYNCS
    parameters.setdefault("validation_schedule_interval", 0)
    parameters.setdefault("preview_schedule_interval", 0)
    max_changes_per_staging_item = get_state_max_changes_per_staging_item(
        sync,
        DEFAULT_MAX_CHANGES_PER_STAGING_ITEM,
    )
    parameters["max_changes_per_staging_item"] = max(1, max_changes_per_staging_item)
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
    include_tags = [str(tag).strip() for tag in include_tags if str(tag).strip()]
    exclude_tags = [str(tag).strip() for tag in exclude_tags if str(tag).strip()]
    include_match = str(
        source_parameters.get("device_tag_include_match") or "any"
    ).strip()
    if include_match not in {"any", "all"}:
        include_match = "any"
    query_parameters = {}
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


def sync_run_job_names(sync):
    """The two job names that represent an actual sync RUN for this sync.

    Gates that ask "is another sync run queued/running?" must match on these
    names only: since 2.5.6 a sync can also carry permanently-SCHEDULED
    standing-schedule rows (fixed JobRunner names "dependency preview" /
    "validation") plus per-sync button jobs, and a status-only filter would
    treat those as an active sync run forever."""
    return (f"{sync.name} - adhoc", f"{sync.name} - scheduled")


def _resolve_enqueue_user(sync, user=None):
    resolved = user or sync.user
    if resolved is None:
        raise SyncError(
            "Forward sync has no owner. Edit the sync as the intended owner "
            "before scheduling or running it."
        )
    if sync.user_id is None:
        sync.__class__.objects.filter(pk=sync.pk, user__isnull=True).update(
            user=resolved
        )
        sync.refresh_from_db(fields=["user"])
        resolved = sync.user
        if resolved is None:
            raise SyncError(
                "Forward sync owner adoption did not persist; retry the operation."
            )
    return resolved


def _enqueue_standing_job(
    job_class,
    *,
    sync,
    user,
    schedule_at,
    interval,
):
    """Update one standing chain without deleting its running occurrence."""
    with advisory_lock(ADVISORY_LOCK_KEYS["job-schedules"]):
        active_jobs = list(
            job_class.get_jobs(sync)
            .filter(status__in=JobStatusChoices.ENQUEUED_STATE_CHOICES)
            .order_by("pk")
        )
        running = next(
            (
                job
                for job in active_jobs
                if job.status == JobStatusChoices.STATUS_RUNNING
            ),
            None,
        )
        if running is not None:
            from ..jobs import terminate_job_once
            from .job_liveness import job_has_live_execution

            if not job_has_live_execution(running):
                terminate_job_once(
                    running,
                    status=JobStatusChoices.STATUS_ERRORED,
                    error=(
                        "Standing schedule occurrence has no live RQ execution; "
                        "reconciliation replaced the interrupted chain."
                    ),
                )
                active_jobs = [job for job in active_jobs if job.pk != running.pk]
                running = None
        if running is not None:
            for job in active_jobs:
                if job.pk != running.pk and job.status in (
                    JobStatusChoices.STATUS_PENDING,
                    JobStatusChoices.STATUS_SCHEDULED,
                ):
                    job.delete()
            return running

        existing = active_jobs[0] if active_jobs else None
        if existing is not None:
            if (not schedule_at or existing.scheduled == schedule_at) and (
                existing.interval == interval
            ):
                return existing
            existing.delete()
        return job_class.enqueue(
            instance=sync,
            user=user,
            schedule_at=schedule_at,
            interval=interval,
        )


def enqueue_sync_job(
    sync,
    adhoc=False,
    user=None,
    current_job=None,
    force_unchanged=False,
):
    user = _resolve_enqueue_user(sync, user)
    with advisory_lock(ADVISORY_LOCK_KEYS["job-schedules"]):
        sync.refresh_from_db(fields=["status", "scheduled", "interval", "user"])
        if sync.status == ForwardSyncStatusChoices.READY_TO_MERGE:
            raise SyncError("Forward sync is waiting for its branch to be merged.")
        active_names = (
            sync_run_job_names(sync) if adhoc else (f"{sync.name} - scheduled",)
        )
        active_statuses = [
            JobStatusChoices.STATUS_PENDING,
            JobStatusChoices.STATUS_RUNNING,
        ]
        if not adhoc:
            active_statuses.append(JobStatusChoices.STATUS_SCHEDULED)
        active_jobs = list(
            sync.jobs.filter(
                name__in=active_names,
                status__in=active_statuses,
            ).order_by("pk")
        )
        current_job_pk = getattr(current_job, "pk", None)
        running = next(
            (
                job
                for job in active_jobs
                if job.status == JobStatusChoices.STATUS_RUNNING
                and job.pk != current_job_pk
            ),
            None,
        )
        if running is not None:
            if force_unchanged:
                raise SyncError(
                    "Cannot force a same-snapshot re-sync while another sync "
                    "job is active; wait for the active job to finish and retry."
                )
            return running
        existing = next(
            (job for job in active_jobs if job.pk != current_job_pk),
            None,
        )
        if existing is not None:
            if force_unchanged:
                raise SyncError(
                    "Cannot force a same-snapshot re-sync while another sync "
                    "job is active; wait for the active job to finish and retry."
                )
            return existing
        if sync.status in (
            ForwardSyncStatusChoices.SYNCING,
            ForwardSyncStatusChoices.MERGING,
        ):
            raise SyncError(
                "Cannot queue another sync; a Forward ingestion is already in progress."
            )
        if adhoc or sync.status == ForwardSyncStatusChoices.NEW:
            sync.status = ForwardSyncStatusChoices.QUEUED
            sync.__class__.objects.filter(pk=sync.pk).update(status=sync.status)
        return enqueue_forward_job(
            import_string("forward_netbox.jobs.sync_forwardsync"),
            instance=sync,
            user=user,
            name=f"{sync.name} - {'adhoc' if adhoc else 'scheduled'}",
            adhoc=adhoc,
            force_unchanged=bool(force_unchanged),
            schedule_at=None if adhoc else sync.scheduled,
            interval=None if adhoc else sync.interval,
        )


def enqueue_validation_job(
    sync, adhoc=False, user=None, schedule_at=None, interval=None
):
    user = _resolve_enqueue_user(sync, user)
    if schedule_at or interval:
        # Standing schedule: one per sync, serialized by fixed JobRunner name
        # and sync instance; recurrence is handled by JobRunner after each run
        # completes. Pass schedule_at through untouched so defaulting it to
        # now() cannot churn an otherwise idempotent re-post.
        from ..jobs import ValidationJob

        with advisory_lock(ADVISORY_LOCK_KEYS["job-schedules"]):
            persist_standing_schedule_interval(sync, "validation", interval)
            return _enqueue_standing_job(
                ValidationJob,
                sync=sync,
                user=user,
                schedule_at=schedule_at,
                interval=interval,
            )
    with advisory_lock(ADVISORY_LOCK_KEYS["job-schedules"]):
        active = (
            sync.jobs.filter(
                Q(name__startswith=f"{sync.name} - validation") | Q(name="validation"),
                status__in=_ACTIVE_JOB_STATUSES,
            )
            .order_by("pk")
            .first()
        )
        if active is not None:
            raise JobAlreadyActive(active)
        from ..jobs import ValidationJob

        return ValidationJob.enqueue(
            instance=sync,
            user=user,
            name=f"{sync.name} - validation",
            adhoc=adhoc,
            schedule_at=None,
            interval=None,
        )


# Desired state for the two standing schedules lives in sync.parameters so it
# survives the Job rows themselves (a hard-killed worker mid-occurrence loses
# the recurrence chain; reconcile recreates it from the stored intent).
STANDING_SCHEDULE_PARAM_KEYS = {
    "validation": "validation_schedule_interval",
    "dependency_preview": "preview_schedule_interval",
}
STANDING_SCHEDULE_JOB_NAMES = {
    "validation": "validation",
    "dependency_preview": "dependency preview",
}


def standing_schedule_intent(parameters):
    """Comparable snapshot of canonical standing-schedule intervals."""
    parameters = parameters or {}
    return {
        key: int(parameters.get(key) or 0)
        for key in STANDING_SCHEDULE_PARAM_KEYS.values()
    }


def persist_standing_schedule_interval(sync, kind, interval):
    """Record the desired standing-schedule interval on the sync.

    0 is stored explicitly and means "operator cancelled".
    Transactional: locks the sync row so concurrent writers (validation vs
    preview persist, occurrence re-alignment, form save) cannot clobber
    each other's parameter keys."""
    key = STANDING_SCHEDULE_PARAM_KEYS[kind]
    with db_transaction.atomic():
        locked = sync.__class__.objects.select_for_update().filter(pk=sync.pk).first()
        if locked is None:
            # Sync deleted mid-flight (occurrence-guard race) — nothing to
            # persist against.
            return
        parameters = dict(locked.parameters or {})
        parameters[key] = int(interval or 0)
        sync.parameters = parameters
        sync.__class__.objects.filter(pk=sync.pk).update(parameters=parameters)


def cancel_standing_schedule(sync, kind):
    """Cancel the standing schedule: store intent 0 and delete its pending/
    scheduled rows (through Job.delete() so the RQ entry is cancelled).

    A RUNNING occurrence is deliberately left alone — deleting its row does
    not stop the worker, and core recurrence would re-INSERT and re-enqueue
    from in-memory state (schedule resurrection). Instead the occurrence's
    intent guard sees the stored 0 at its next firing and terminates the
    chain itself. Returns the number of rows removed."""
    with advisory_lock(ADVISORY_LOCK_KEYS["job-schedules"]):
        persist_standing_schedule_interval(sync, kind, 0)
        removed = 0
        for job in sync.jobs.filter(
            name=STANDING_SCHEDULE_JOB_NAMES[kind],
            status__in=[
                JobStatusChoices.STATUS_PENDING,
                JobStatusChoices.STATUS_SCHEDULED,
            ],
        ):
            job.delete()
            removed += 1
        return removed


def reconcile_standing_schedules(sync, user=None, schedule_at_by_kind=None):
    """Make the enqueued Job rows match the stored schedule intent.

    Called from form save and at the end of each sync run. Core JobRunner
    recurrence lives in handle()'s finally, so a hard-killed worker silently
    drops the chain; this is a no-op while the chain is healthy and recreates
    it when it vanished."""
    from ..jobs import DependencyPreviewJob
    from ..jobs import ValidationJob

    job_classes = {
        "validation": ValidationJob,
        "dependency_preview": DependencyPreviewJob,
    }
    schedule_at_by_kind = dict(schedule_at_by_kind or {})
    with advisory_lock(ADVISORY_LOCK_KEYS["job-schedules"]):
        try:
            sync.refresh_from_db(fields=["parameters", "user"])
        except sync.__class__.DoesNotExist:
            return
        parameters = sync.parameters or {}
        for kind, key in STANDING_SCHEDULE_PARAM_KEYS.items():
            name = STANDING_SCHEDULE_JOB_NAMES[kind]
            desired = int(parameters.get(key) or 0)
            if desired > 0:
                schedule_at = schedule_at_by_kind.get(kind)
                if (
                    schedule_at is not None
                    and sync.jobs.filter(
                        name=name,
                        status__in=[
                            JobStatusChoices.STATUS_PENDING,
                            JobStatusChoices.STATUS_SCHEDULED,
                        ],
                        interval=desired,
                    ).exists()
                ):
                    schedule_at = None
                kept = _enqueue_standing_job(
                    job_classes[kind],
                    sync=sync,
                    user=user or sync.user,
                    schedule_at=schedule_at,
                    interval=desired,
                )
                # Sweep surplus chains (e.g. an interval change that raced a
                # running occurrence left a second recurrence chain behind).
                for job in sync.jobs.filter(
                    name=name,
                    status__in=[
                        JobStatusChoices.STATUS_PENDING,
                        JobStatusChoices.STATUS_SCHEDULED,
                    ],
                ).exclude(pk=kept.pk):
                    job.delete()
            else:
                for job in sync.jobs.filter(
                    name=name,
                    status__in=[
                        JobStatusChoices.STATUS_PENDING,
                        JobStatusChoices.STATUS_SCHEDULED,
                    ],
                ):
                    job.delete()


def enqueue_preview_schedule(sync, user=None, schedule_at=None, interval=None):
    """Standing dependency-preview schedule (immediate runs use
    enqueue_button_job, which keeps the sync-qualified one-shot name). schedule_at
    passes through untouched so re-posts stay idempotent; interval-only means
    run now, then recur."""
    from ..jobs import DependencyPreviewJob

    user = _resolve_enqueue_user(sync, user)
    with advisory_lock(ADVISORY_LOCK_KEYS["job-schedules"]):
        persist_standing_schedule_interval(sync, "dependency_preview", interval)
        return _enqueue_standing_job(
            DependencyPreviewJob,
            sync=sync,
            user=user,
            schedule_at=schedule_at,
            interval=interval,
        )


class JobAlreadyActive(Exception):
    """An equivalent job is already pending/running for this sync."""

    def __init__(self, job):
        self.job = job
        super().__init__(f"Job `{job.name}` is already {job.status} (job #{job.pk}).")


class JobBlockedBySyncRun(JobAlreadyActive):
    """The requested job is refused while a sync run is active (prune). The
    requested work is NOT queued — distinct from JobAlreadyActive so the API
    does not imply the work is already happening."""


# The operator-facing background jobs ("button jobs") share one enqueue path so
# the HTML buttons and the REST API actions get identical job names (several
# lookups match on these strings - do NOT rename), permission expectations, and
# overlap behavior.
BUTTON_JOB_SPECS = {
    "dependency_preview": (
        "forward_netbox.jobs.DependencyPreviewJob",
        "dependency preview",
        "forward_netbox.run_forwardsync",
    ),
    "prune_orphans": (
        "forward_netbox.jobs.PruneOrphansJob",
        "prune orphans",
        "dcim.delete_device",
    ),
    "tag_delete_eligible_ipam": (
        "forward_netbox.jobs.TagDeleteEligibleIpamJob",
        "tag delete-eligible IPAM",
        "ipam.change_prefix",
    ),
}

_ACTIVE_JOB_STATUSES = (
    JobStatusChoices.STATUS_PENDING,
    JobStatusChoices.STATUS_RUNNING,
)


def button_job_permission(kind):
    return BUTTON_JOB_SPECS[kind][2]


def enqueue_button_job(
    sync,
    kind,
    user,
    *,
    job_kwargs=None,
):
    """Enqueue an operator button job with a shared overlap guard.

    Raises ``JobAlreadyActive`` instead of stacking a duplicate when an
    equivalent job is already pending/running. Pruning also
    refuses while the sync itself is queued/running - deleting devices
    mid-ingest would race the apply.
    """
    runner_path, suffix, _permission = BUTTON_JOB_SPECS[kind]
    name = f"{sync.name} - {suffix}"
    # Share the standing-schedule lock to close the check-then-enqueue race
    # between two concurrent POSTs and against a new standing occurrence.
    with advisory_lock(ADVISORY_LOCK_KEYS["job-schedules"]):
        # Two name shapes count as "the same job already active": the per-sync
        # immediate name and the fixed JobRunner name used by standing-schedule
        # occurrences (exact match; the permanently-SCHEDULED schedule row
        # itself must NOT block, so the status filter stays pending/running).
        active = (
            sync.jobs.filter(
                Q(name__startswith=name) | Q(name=suffix),
                status__in=_ACTIVE_JOB_STATUSES,
            )
            .order_by("pk")
            .first()
        )
        if active is not None:
            raise JobAlreadyActive(active)
        if kind == "prune_orphans":
            running_sync = (
                sync.jobs.filter(
                    name__in=sync_run_job_names(sync),
                    status__in=_ACTIVE_JOB_STATUSES,
                )
                .order_by("pk")
                .first()
            )
            if running_sync is not None:
                raise JobBlockedBySyncRun(running_sync)
        runner_class = import_string(runner_path)
        return runner_class.enqueue(
            instance=sync,
            user=user,
            name=name,
            **dict(job_kwargs or {}),
        )
