from contextlib import contextmanager

from core.exceptions import SyncError
from core.models import Job
from core.signals import pre_sync
from dcim.models import Site
from dcim.models import VirtualChassis
from django.db import transaction
from django.db.models import signals
from django.utils import timezone
from django.utils.module_loading import import_string
from netbox_branching.choices import BranchStatusChoices

from ..choices import ForwardExecutionStepStatusChoices
from ..choices import ForwardSourceStatusChoices
from ..choices import ForwardSyncStatusChoices
from .execution_ledger import execution_step_for_ingestion
from .execution_ledger import latest_execution_run
from .execution_ledger import mark_ingestion_step_merged
from .execution_ledger import mark_run_completed
from .snapshot_freshness import latest_processed_catchup_decision
from .sync_state import get_branch_run_display_state
from .sync_state import has_pending_branch_run

try:
    from dcim.signals import sync_cached_scope_fields
except ImportError:  # pragma: no cover - compatibility with older NetBox point releases
    sync_cached_scope_fields = None


# Bounded automatic merge requeue attempts for merge-timeout recovery in
# auto-merge runs. Beyond this budget, recovery recommendation should remain
# manual-intervention driven.
AUTO_MERGE_STALE_MERGE_REQUEUE_LIMIT = 4


@contextmanager
def suppress_ingest_side_effect_signals():
    """Suppress per-object post_save side effects that produce redundant work
    during bulk ingest (apply and merge phases).

    Suppressed:
    - assign_virtualchassis_master (dcim): recalculates VC master on every
      VirtualChassis save; meaningless mid-ingest, safe to defer.
    - sync_cached_scope_fields (dcim): recalculates Site scope cache on every
      Site save; batched naturally after ingest.
    - notify_object_changed (extras): creates Notification rows per save for
      subscribers; no operator subscribes to ingest-driven churn and the lookup
      fires a DB query per object even with no subscribers.

    Does NOT suppress core.signals.handle_changed_object (ObjectChange /
    Branching diff tracking) — that is intentional and required for Branching
    review.
    """
    from dcim.signals import assign_virtualchassis_master

    disconnect_pairs = [
        (assign_virtualchassis_master, VirtualChassis),
    ]
    if sync_cached_scope_fields is not None:
        disconnect_pairs.append((sync_cached_scope_fields, Site))

    try:
        from extras.signals import notify_object_changed as _notify_object_changed

        _notify_sender = None
        disconnect_pairs.append((_notify_object_changed, _notify_sender))
    except ImportError:  # pragma: no cover
        pass

    for handler, sender in disconnect_pairs:
        if sender is None:
            signals.post_save.disconnect(handler)
            signals.pre_delete.disconnect(handler)
        else:
            signals.post_save.disconnect(handler, sender=sender)
    try:
        yield
    finally:
        for handler, sender in disconnect_pairs:
            if sender is None:
                signals.post_save.connect(handler)
                signals.pre_delete.connect(handler)
            else:
                signals.post_save.connect(handler, sender=sender)


@contextmanager
def suppress_branch_merge_side_effect_signals():
    with suppress_ingest_side_effect_signals():
        yield


def sync_merge_ingestion(ingestion, *, mark_baseline_ready=None, remove_branch=True):
    from .merge import merge_branch

    forwardsync = ingestion.sync
    if forwardsync.status == ForwardSyncStatusChoices.MERGING:
        raise SyncError("Cannot initiate merge; merge already in progress.")

    pre_sync.send(sender=ingestion.__class__, instance=ingestion)

    branch_run_state = get_branch_run_display_state(forwardsync)
    has_pending_compat_state = (
        branch_run_state.get("pending_ingestion_id") == ingestion.pk
    )
    is_pending_branch_run = has_pending_compat_state and branch_run_state.get(
        "awaiting_merge"
    )
    is_auto_pending_branch_run = has_pending_compat_state and branch_run_state.get(
        "auto_merge"
    )
    ledger_step = execution_step_for_ingestion(ingestion)
    pending_plan_index = (
        int(ledger_step.index)
        if ledger_step is not None and getattr(ledger_step, "index", None) is not None
        else int(branch_run_state.get("pending_plan_index") or 0)
    )
    ledger_is_final = bool(
        ledger_step
        and ledger_step.run.total_steps
        and int(ledger_step.index) >= int(ledger_step.run.total_steps)
        and _ledger_step_can_complete_run(ledger_step)
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
        # Post-merge ledger bookkeeping must commit all-or-nothing. merge_branch
        # has already applied the branch into NetBox; if the bookkeeping below
        # were to partially commit (baseline_ready set but the step not marked
        # MERGED, or vice versa) a resume could re-merge an already-applied shard
        # and double-apply it. The atomic block keeps the ledger internally
        # consistent; the merge_forwardingestion re-entry guard (branch already
        # merged/removed -> mark step merged, skip re-merge) covers a crash
        # between the merge and this block.
        with transaction.atomic():
            if mark_baseline_ready:
                ingestion.baseline_ready = True
                ingestion.__class__.objects.filter(pk=ingestion.pk).update(
                    baseline_ready=True
                )
            mark_ingestion_step_merged(
                ingestion,
                baseline_ready=mark_baseline_ready,
                merge_job=ingestion.merge_job,
            )
        if pending_plan_index:
            branch_run_state = get_branch_run_display_state(forwardsync)
        if is_pending_branch_run:
            if branch_run_state.get("pending_is_final"):
                mark_run_completed(forwardsync, baseline_ready=mark_baseline_ready)
        if has_pending_compat_state:
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
    if forwardsync.status == ForwardSyncStatusChoices.COMPLETED:
        decision = latest_processed_catchup_decision(
            forwardsync,
            current_snapshot_id=getattr(ingestion, "snapshot_id", ""),
            current_job=ingestion.merge_job,
        )
        if decision["should_queue"]:
            selector = decision.get("snapshot_selector") or "latestProcessed"
            forwardsync.logger.log_info(
                f"Forward {selector} advanced from "
                f"`{decision['current_snapshot_id']}` to "
                f"`{decision['latest_processed_snapshot_id']}` during the run; "
                "queuing a catch-up sync.",
                obj=forwardsync,
            )
            forwardsync.enqueue_sync_job(
                adhoc=True,
                user=getattr(ingestion.merge_job, "user", None),
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


def _ledger_step_can_complete_run(step):
    run = step.run
    stage_steps = run.steps.filter(kind="stage")
    if not stage_steps.exists():
        return False
    unfinished_other_steps = (
        stage_steps.exclude(pk=step.pk)
        .exclude(
            status__in=[
                ForwardExecutionStepStatusChoices.MERGED,
                ForwardExecutionStepStatusChoices.SKIPPED,
                ForwardExecutionStepStatusChoices.CANCELLED,
            ]
        )
        .exists()
    )
    if unfinished_other_steps:
        return False
    if run.total_steps:
        return stage_steps.count() >= int(run.total_steps)
    return True


def maybe_enqueue_next_branch_stage(
    ingestion,
    user,
    *,
    allow_failed_recovery=False,
):
    # 2.0: single-branch ingest has no "next shard stage" to enqueue and no
    # execution-ledger run/step rows. The per-shard staging + merge-timeout-retry
    # machinery is gone; this is now a no-op.
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
    # Branching keeps the in-memory instance stale while the merge completes.
    # Reload the persisted row before deletion so the Branching delete guard
    # sees the terminal merged state instead of the old merging status.
    branching_branch = branching_branch.__class__.objects.get(pk=branching_branch.pk)
    branching_branch.status = BranchStatusChoices.MERGED
    branching_branch.save(update_fields=["status"])
    branching_branch.delete()
