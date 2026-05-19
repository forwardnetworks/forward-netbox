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
from .execution_ledger import branch_run_state_from_execution_run
from .execution_ledger import execution_step_for_ingestion
from .execution_ledger import latest_execution_run
from .execution_ledger import mark_ingestion_step_merged
from .execution_ledger import mark_run_completed
from .resumable_branching import enqueue_branch_stage_job
from .resumable_branching import update_plan_item_state
from .sync_state import has_pending_branch_run

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
    is_pending_branch_run = branch_run_state.get(
        "pending_ingestion_id"
    ) == ingestion.pk and branch_run_state.get("awaiting_merge")
    is_auto_pending_branch_run = branch_run_state.get(
        "pending_ingestion_id"
    ) == ingestion.pk and branch_run_state.get("auto_merge")
    pending_plan_index = branch_run_state.get("pending_plan_index")
    ledger_step = execution_step_for_ingestion(ingestion)
    ledger_is_final = bool(
        ledger_step
        and ledger_step.run.total_steps
        and int(ledger_step.index) >= int(ledger_step.run.total_steps)
    )
    if mark_baseline_ready is None:
        mark_baseline_ready = not (
            is_pending_branch_run or is_auto_pending_branch_run or ledger_step
        ) or bool(branch_run_state.get("pending_is_final") or ledger_is_final)

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
        if pending_plan_index:
            update_plan_item_state(
                forwardsync,
                pending_plan_index,
                status="merged",
                ingestion_id=ingestion.pk,
            )
            branch_run_state = forwardsync.get_branch_run_state()
        mark_ingestion_step_merged(
            ingestion,
            baseline_ready=mark_baseline_ready,
            merge_job=ingestion.merge_job,
        )
        if is_pending_branch_run:
            if branch_run_state.get("pending_is_final"):
                mark_run_completed(forwardsync, baseline_ready=mark_baseline_ready)
            forwardsync.clear_branch_run_state()
        if remove_branch:
            ingestion._cleanup_merged_branch()
        active_run = latest_execution_run(forwardsync)
        has_more_planned_steps = bool(
            active_run
            and bool(active_run.auto_merge)
            and int(active_run.total_steps or 0) > 0
            and int(active_run.next_step_index or 1) <= int(active_run.total_steps)
        )
        if has_more_planned_steps or has_pending_branch_run(forwardsync):
            forwardsync.status = ForwardSyncStatusChoices.SYNCING
        else:
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


def maybe_enqueue_next_branch_stage(ingestion, user):
    sync = ingestion.sync
    state = sync.get_branch_run_state()
    if not state or not state.get("auto_merge"):
        ledger_step = execution_step_for_ingestion(ingestion)
        ledger_run = ledger_step.run if ledger_step is not None else None
        if not state and ledger_run is None:
            ledger_run = latest_execution_run(sync)
        if not state and ledger_run is not None and ledger_run.auto_merge:
            state = branch_run_state_from_execution_run(ledger_run)
        else:
            return None
    next_plan_index = int(state.get("next_plan_index") or 1)
    total_plan_items = int(state.get("total_plan_items") or 0)
    if total_plan_items and next_plan_index <= total_plan_items:
        return enqueue_branch_stage_job(sync, user=user, adhoc=True)
    return None


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
