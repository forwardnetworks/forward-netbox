from contextlib import contextmanager

from core.exceptions import SyncError
from core.models import Job
from core.signals import pre_sync
from dcim.models import Site
from dcim.models import VirtualChassis
from django.db.models import signals
from django.utils import timezone
from django.utils.module_loading import import_string

from ..choices import ForwardSourceStatusChoices
from ..choices import ForwardSyncStatusChoices

try:
    from dcim.signals import sync_cached_scope_fields
except ImportError:  # pragma: no cover - compatibility with older NetBox point releases
    sync_cached_scope_fields = None


@contextmanager
def suppress_branch_merge_side_effect_signals():
    from dcim.signals import assign_virtualchassis_master

    signals.post_save.disconnect(
        assign_virtualchassis_master,
        sender=VirtualChassis,
    )
    if sync_cached_scope_fields is not None:
        signals.post_save.disconnect(sync_cached_scope_fields, sender=Site)
    try:
        yield
    finally:
        signals.post_save.connect(
            assign_virtualchassis_master,
            sender=VirtualChassis,
        )
        if sync_cached_scope_fields is not None:
            signals.post_save.connect(sync_cached_scope_fields, sender=Site)


def sync_merge_ingestion(ingestion, *, mark_baseline_ready=None, remove_branch=True):
    from .merge import merge_branch

    forwardsync = ingestion.sync
    if forwardsync.status == ForwardSyncStatusChoices.MERGING:
        raise SyncError("Cannot initiate merge; merge already in progress.")

    pre_sync.send(sender=ingestion.__class__, instance=ingestion)

    branch_run_state = forwardsync.get_branch_run_state()
    is_pending_branch_run = branch_run_state.get("pending_ingestion_id") == ingestion.pk and branch_run_state.get("awaiting_merge")
    if mark_baseline_ready is None:
        mark_baseline_ready = not is_pending_branch_run or bool(
            branch_run_state.get("pending_is_final")
        )

    forwardsync.status = ForwardSyncStatusChoices.MERGING
    ForwardSync = forwardsync.__class__
    ForwardSync.objects.filter(pk=forwardsync.pk).update(status=forwardsync.status)

    try:
        with suppress_branch_merge_side_effect_signals():
            merge_branch(ingestion=ingestion, sync_logger=forwardsync.logger)
        if mark_baseline_ready:
            ingestion.baseline_ready = True
            ingestion.__class__.objects.filter(pk=ingestion.pk).update(
                baseline_ready=True
            )
        if is_pending_branch_run:
            if branch_run_state.get("pending_is_final"):
                forwardsync.clear_branch_run_state()
            else:
                branch_run_state["awaiting_merge"] = False
                branch_run_state.pop("pending_ingestion_id", None)
                branch_run_state.pop("pending_plan_index", None)
                branch_run_state.pop("pending_is_final", None)
                forwardsync.set_branch_run_state(branch_run_state)
        if remove_branch:
            ingestion._cleanup_merged_branch()
        forwardsync.status = ForwardSyncStatusChoices.COMPLETED
    except Exception:
        forwardsync.status = ForwardSyncStatusChoices.FAILED
        ForwardSync.objects.filter(pk=forwardsync.pk).update(
            status=forwardsync.status,
        )
        forwardsync.source.status = ForwardSourceStatusChoices.FAILED
        forwardsync.source.__class__.objects.filter(pk=forwardsync.source.pk).update(
            status=forwardsync.source.status
        )
        raise

    forwardsync.last_synced = timezone.now()
    ForwardSync.objects.filter(pk=forwardsync.pk).update(
        status=forwardsync.status,
        last_synced=forwardsync.last_synced,
    )


def enqueue_merge_job(ingestion, user, remove_branch=False):
    sync = ingestion.sync
    sync.status = ForwardSyncStatusChoices.QUEUED
    sync.__class__.objects.filter(pk=sync.pk).update(status=sync.status)
    job = Job.enqueue(
        import_string("forward_netbox.jobs.merge_forwardingestion"),
        name=f"{ingestion.name} Merge",
        instance=ingestion,
        user=user,
        remove_branch=remove_branch,
    )
    ingestion.__class__.objects.filter(pk=ingestion.pk).update(merge_job=job)
    ingestion.merge_job = job
    return job


def record_change_totals(
    ingestion,
    *,
    applied,
    failed,
    created=0,
    updated=0,
    deleted=0,
):
    ingestion.applied_change_count = max(0, int(applied))
    ingestion.failed_change_count = max(0, int(failed))
    ingestion.created_change_count = max(0, int(created))
    ingestion.updated_change_count = max(0, int(updated))
    ingestion.deleted_change_count = max(0, int(deleted))
    ingestion.__class__.objects.filter(pk=ingestion.pk).update(
        applied_change_count=ingestion.applied_change_count,
        failed_change_count=ingestion.failed_change_count,
        created_change_count=ingestion.created_change_count,
        updated_change_count=ingestion.updated_change_count,
        deleted_change_count=ingestion.deleted_change_count,
    )


def cleanup_merged_branch(ingestion):
    if not ingestion.branch:
        return
    branching_branch = ingestion.branch
    ingestion.branch = None
    ingestion.__class__.objects.filter(pk=ingestion.pk).update(branch=None)
    branching_branch.delete()
