from contextlib import contextmanager

from core.exceptions import SyncError
from core.models import Job
from core.signals import pre_sync
from dcim.models import Site
from dcim.models import VirtualChassis
from django.db.models import signals
from django.utils import timezone
from django.utils.module_loading import import_string
from netbox_branching.choices import BranchStatusChoices

from ..choices import ForwardExecutionRunStatusChoices
from ..choices import ForwardExecutionStepStatusChoices
from ..choices import ForwardSourceStatusChoices
from ..choices import ForwardSyncStatusChoices
from .execution_ledger import active_execution_run
from .execution_ledger import branch_run_state_from_execution_run
from .execution_ledger import current_retryable_step
from .execution_ledger import execution_step_for_ingestion
from .execution_ledger import latest_execution_run
from .execution_ledger import mark_ingestion_step_merged
from .execution_ledger import mark_run_completed
from .execution_ledger import prepare_stage_step_retry
from .resumable_branching import enqueue_branch_stage_job
from .resumable_branching import update_plan_item_state
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
            branch_run_state = get_branch_run_display_state(forwardsync)
        mark_ingestion_step_merged(
            ingestion,
            baseline_ready=mark_baseline_ready,
            merge_job=ingestion.merge_job,
        )
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
            forwardsync.logger.log_info(
                "Forward latestProcessed advanced from "
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
    sync = ingestion.sync
    auto_merge_timeout_job = _maybe_enqueue_merge_timeout_retry(ingestion, user)
    if auto_merge_timeout_job is not None:
        return auto_merge_timeout_job
    state = get_branch_run_display_state(sync)
    ledger_step = execution_step_for_ingestion(ingestion)
    ledger_run = (
        ledger_step.run if ledger_step is not None else active_execution_run(sync)
    )
    if (
        ledger_run is None
        and getattr(sync, "pk", None)
        and sync.execution_runs.exists()
    ):
        # Once ledger history exists, don't continue work from stale compatibility
        # branch-run payloads when there is no active run.
        return None
    if ledger_run is not None:
        if not ledger_run.auto_merge:
            return None
        if ledger_run.status in {
            ForwardExecutionRunStatusChoices.RUNNING,
            ForwardExecutionRunStatusChoices.WAITING,
        }:
            staged_merge_job = _maybe_enqueue_staged_step_merge(ledger_run, user)
            if staged_merge_job is not None:
                return staged_merge_job
        elif allow_failed_recovery and ledger_run.status in {
            ForwardExecutionRunStatusChoices.FAILED,
            ForwardExecutionRunStatusChoices.TIMEOUT,
            ForwardExecutionRunStatusChoices.CANCELLED,
        }:
            staged_merge_job = _maybe_enqueue_staged_step_merge(ledger_run, user)
            if staged_merge_job is not None:
                return staged_merge_job
            retry_step = current_retryable_step(ledger_run)
            if retry_step is None:
                return None
            prepare_stage_step_retry(retry_step)
            ledger_run.refresh_from_db()
        elif ledger_run.status != ForwardExecutionRunStatusChoices.RUNNING:
            return None
        state = branch_run_state_from_execution_run(ledger_run)
    elif not state or not state.get("auto_merge"):
        return None
    next_plan_index = int(state.get("next_plan_index") or 1)
    total_plan_items = int(state.get("total_plan_items") or 0)
    if total_plan_items and next_plan_index <= total_plan_items:
        return enqueue_branch_stage_job(sync, user=user, adhoc=True)
    return None


def _maybe_enqueue_staged_step_merge(run, user):
    next_plan_index = int(run.next_step_index or 1)
    step = (
        run.steps.filter(
            kind="stage",
            index=next_plan_index,
            status=ForwardExecutionStepStatusChoices.STAGED,
        )
        .select_related("ingestion")
        .order_by("index")
        .first()
    )
    if step is None or step.ingestion_id is None:
        return None
    ingestion = step.ingestion
    if not ingestion.can_queue_merge:
        return None
    if run.status in {
        ForwardExecutionRunStatusChoices.FAILED,
        ForwardExecutionRunStatusChoices.TIMEOUT,
        ForwardExecutionRunStatusChoices.CANCELLED,
    }:
        run.status = ForwardExecutionRunStatusChoices.RUNNING
        run.phase = "queued_merge"
        total = int(run.total_steps or 0)
        run.phase_message = (
            f"Queued merge for shard {step.index}/{total}."
            if total
            else f"Queued merge for shard {step.index}."
        )
        run.latest_heartbeat = timezone.now()
        run.save(
            update_fields=[
                "status",
                "phase",
                "phase_message",
                "latest_heartbeat",
                "updated",
            ]
        )
        sync = run.sync
        sync.status = ForwardSyncStatusChoices.QUEUED
        sync.save(update_fields=["parameters", "status", "last_updated"])
    job = enqueue_merge_job(ingestion, user, remove_branch=True)
    step.status = ForwardExecutionStepStatusChoices.MERGE_QUEUED
    step.merge_job = job
    step.completed = None
    step.heartbeat = timezone.now()
    step.save(
        update_fields=[
            "status",
            "merge_job",
            "completed",
            "heartbeat",
            "updated",
        ]
    )
    update_plan_item_state(
        ingestion.sync,
        step.index,
        status="merge_queued",
        merge_job_id=job.pk,
    )
    ingestion.sync.logger.log_info(
        f"Queued merge for pre-staged shard {step.index}/{run.total_steps}.",
        obj=ingestion,
    )
    return job


def _maybe_enqueue_merge_timeout_retry(ingestion, user):
    sync = ingestion.sync
    run = active_execution_run(sync) or latest_execution_run(sync)
    if run is None or not bool(getattr(run, "auto_merge", False)):
        return None
    step = execution_step_for_ingestion(ingestion)
    if step is None:
        return None
    if step.status != ForwardExecutionStepStatusChoices.MERGE_TIMEOUT:
        return None
    if int(step.retry_count or 0) > AUTO_MERGE_STALE_MERGE_REQUEUE_LIMIT:
        return None
    if not ingestion.can_queue_merge:
        return None
    return enqueue_merge_job(ingestion, user, remove_branch=False)


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
