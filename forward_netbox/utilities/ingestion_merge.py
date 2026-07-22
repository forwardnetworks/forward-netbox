from contextlib import contextmanager

from core.exceptions import SyncError
from core.models import Job
from core.signals import pre_sync
from dcim.models import Site
from dcim.models import VirtualChassis
from dcim.signals import assign_virtualchassis_master
from dcim.signals import sync_cached_scope_fields
from django.db import DEFAULT_DB_ALIAS
from django.db import transaction
from django.db.models import signals
from django.utils import timezone
from django.utils.module_loading import import_string
from django_pglocks import advisory_lock
from extras.signals import notify_object_changed
from netbox.constants import ADVISORY_LOCK_KEYS
from netbox_branching.choices import BranchStatusChoices
from netbox_branching.models import AppliedChange
from netbox_branching.models import ChangeDiff
from rq.timeouts import JobTimeoutException

from ..choices import ForwardCatchupStatusChoices
from ..choices import ForwardSourceStatusChoices
from ..choices import ForwardSyncStatusChoices
from ..exceptions import ForwardPartialMergeError
from .job_queue import enqueue_forward_job
from .runtime_guidance import effective_merge_job_timeout
from .snapshot_freshness import latest_processed_catchup_decision


@contextmanager
def suppress_ingest_side_effect_signals():
    """Suppress per-object post_save side effects that produce redundant work
    during bulk ingest (apply and merge phases).

    Suppressed:
    - assign_virtualchassis_master (dcim): recalculates VC master on every
      VirtualChassis save; meaningless mid-ingest, so it is skipped until the
      final save.
    - sync_cached_scope_fields (dcim): recalculates Site scope cache on every
      Site save; batched naturally after ingest.
    - notify_object_changed (extras): creates Notification rows per save for
      subscribers; no operator subscribes to ingest-driven churn and the lookup
      fires a DB query per object even with no subscribers.

    Does NOT suppress core.signals.handle_changed_object (ObjectChange /
    Branching diff tracking) — that is intentional and required for Branching
    review.
    """
    disconnect_pairs = [
        (assign_virtualchassis_master, VirtualChassis),
        (sync_cached_scope_fields, Site),
        (notify_object_changed, None),
    ]

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


def _post_merge_context(ingestion, mark_baseline_ready):
    if mark_baseline_ready is None:
        mark_baseline_ready = True

    return {
        "mark_baseline_ready": bool(mark_baseline_ready),
    }


def _persist_catchup_state(
    ingestion,
    *,
    status,
    reason="",
    target_snapshot_id="",
    error_type="",
    checked_at=None,
):
    values = {
        "catchup_status": status,
        "catchup_reason": str(reason or "")[:100],
        "catchup_target_snapshot_id": str(target_snapshot_id or "")[:100],
        "catchup_error_type": str(error_type or "")[:255],
        "catchup_checked_at": checked_at,
    }
    ingestion.__class__.objects.filter(pk=ingestion.pk).update(**values)
    for field, value in values.items():
        setattr(ingestion, field, value)


def reconcile_ingestion_catchup(ingestion, *, current_job=None, client=None):
    """Persist and satisfy one finalized ingestion's dynamic-snapshot catch-up."""
    forwardsync = ingestion.sync
    try:
        decision = latest_processed_catchup_decision(
            forwardsync,
            current_snapshot_id=getattr(ingestion, "snapshot_id", ""),
            client=client,
            current_job=current_job,
        )
        reason = decision.get("reason") or ""
        target_snapshot_id = decision.get("latest_processed_snapshot_id") or ""
        queued_job = None
        if decision["should_queue"]:
            selector = decision.get("snapshot_selector") or "latestProcessed"
            forwardsync.logger.log_info(
                f"Forward {selector} advanced from "
                f"`{decision['current_snapshot_id']}` to "
                f"`{decision['latest_processed_snapshot_id']}` during the run; "
                "queuing a catch-up sync.",
                obj=forwardsync,
            )
            queued_job = forwardsync.enqueue_sync_job(
                adhoc=True,
                user=getattr(current_job, "user", None),
                current_job=current_job,
            )

        failed_reasons = {
            "latest_processed_lookup_failed",
            "missing_current_snapshot_id",
            "missing_latest_processed_snapshot_id",
            "missing_network_id",
        }
        if decision["should_queue"] or reason == "active_job_exists":
            status = ForwardCatchupStatusChoices.QUEUED
        elif reason == "sync_not_completed":
            if forwardsync.status in {
                ForwardSyncStatusChoices.QUEUED,
                ForwardSyncStatusChoices.SYNCING,
                ForwardSyncStatusChoices.MERGING,
            }:
                status = ForwardCatchupStatusChoices.QUEUED
            else:
                status = ForwardCatchupStatusChoices.FAILED
        elif reason == "fixed_snapshot_selector":
            status = ForwardCatchupStatusChoices.NOT_APPLICABLE
        elif reason in failed_reasons:
            status = ForwardCatchupStatusChoices.FAILED
        else:
            status = ForwardCatchupStatusChoices.CURRENT
        _persist_catchup_state(
            ingestion,
            status=status,
            reason=reason,
            target_snapshot_id=target_snapshot_id,
            checked_at=timezone.now(),
        )
        return {**decision, "job_id": getattr(queued_job, "pk", None)}
    except JobTimeoutException as exc:
        _persist_catchup_state(
            ingestion,
            status=ForwardCatchupStatusChoices.FAILED,
            reason="catchup_check_exception",
            error_type=exc.__class__.__name__,
            checked_at=timezone.now(),
        )
        raise
    except Exception as exc:
        _persist_catchup_state(
            ingestion,
            status=ForwardCatchupStatusChoices.FAILED,
            reason="catchup_check_exception",
            error_type=exc.__class__.__name__,
            checked_at=timezone.now(),
        )
        raise


def reconcile_catchup_if_ownership_complete(
    ingestion,
    *,
    current_job=None,
    client=None,
):
    """Claim and run catch-up only after this ingestion's ownership converges."""
    from .logging import SyncLogging
    from .ownership import ownership_generation_complete
    from .ownership import ownership_write_lock

    with ownership_write_lock():
        locked = (
            ingestion.__class__.objects.select_for_update()
            .select_related("sync", "sync__source")
            .get(pk=ingestion.pk)
        )
        if not ownership_generation_complete(locked.sync, locked.pk):
            return {
                "checked": False,
                "reason": "ownership_pending",
                "job_id": None,
            }
        if locked.catchup_status not in {
            ForwardCatchupStatusChoices.PENDING,
            ForwardCatchupStatusChoices.FAILED,
        }:
            return {
                "checked": False,
                "reason": "catchup_already_claimed",
                "job_id": None,
            }
        _persist_catchup_state(
            locked,
            status=ForwardCatchupStatusChoices.CHECKING,
            reason="ownership_complete",
            checked_at=timezone.now(),
        )

    if getattr(locked.sync, "logger", None) is None:
        locked.sync.logger = SyncLogging(job=getattr(current_job, "pk", None))
    return reconcile_ingestion_catchup(
        locked,
        current_job=current_job,
        client=client,
    )


def _complete_post_merge_bookkeeping(ingestion, *, context, remove_branch):
    from .ownership import _mark_ownership_pending_locked
    from .ownership import finalize_device_identities_locked
    from .ownership import ownership_write_lock
    from .ownership import required_ownership_domains
    from .workload_state import promote_workload_states_locked

    with ownership_write_lock():
        locked_ingestion = (
            ingestion.__class__.objects.select_for_update()
            .select_related("sync")
            .get(pk=ingestion.pk)
        )
        if locked_ingestion.merge_applied_at is None:
            raise SyncError(
                "Post-merge bookkeeping requires durable merge-applied evidence."
            )
        forwardsync = locked_ingestion.sync.__class__.objects.select_for_update().get(
            pk=locked_ingestion.sync_id
        )
        if context["mark_baseline_ready"]:
            locked_ingestion.baseline_ready = True

        parameters = dict(forwardsync.parameters or {})
        forwardsync.parameters = parameters
        forwardsync.status = ForwardSyncStatusChoices.COMPLETED
        forwardsync.last_synced = timezone.now()
        if parameters.get("stuck_recovery"):
            parameters.pop("stuck_recovery", None)

        finalize_device_identities_locked(locked_ingestion)
        promote_workload_states_locked(locked_ingestion)
        domains = []
        if forwardsync.status == ForwardSyncStatusChoices.COMPLETED:
            domains = required_ownership_domains(forwardsync)
            _mark_ownership_pending_locked(
                forwardsync,
                locked_ingestion.pk,
                locked_ingestion.snapshot_id,
                domains,
            )
        finalized_at = timezone.now()
        locked_ingestion.merge_finalized_at = finalized_at
        locked_ingestion.catchup_status = ForwardCatchupStatusChoices.PENDING
        locked_ingestion.catchup_target_snapshot_id = ""
        locked_ingestion.catchup_reason = ""
        locked_ingestion.catchup_error_type = ""
        locked_ingestion.catchup_checked_at = None
        locked_ingestion.save(
            update_fields=[
                "baseline_ready",
                "merge_finalized_at",
                "catchup_status",
                "catchup_target_snapshot_id",
                "catchup_reason",
                "catchup_error_type",
                "catchup_checked_at",
            ]
        )
        forwardsync.save(update_fields=["parameters", "status", "last_synced"])

    ingestion.baseline_ready = locked_ingestion.baseline_ready
    ingestion.merge_applied_at = locked_ingestion.merge_applied_at
    ingestion.merge_finalized_at = locked_ingestion.merge_finalized_at
    ingestion.catchup_status = locked_ingestion.catchup_status
    ingestion.sync = forwardsync
    if remove_branch:
        ingestion._cleanup_merged_branch()
    if forwardsync.status != ForwardSyncStatusChoices.COMPLETED:
        return


def resume_post_merge_bookkeeping(
    ingestion,
    *,
    mark_baseline_ready=None,
    remove_branch=True,
):
    """Finish bookkeeping after a crash that occurred after branch merge.

    Return ``False`` without mutation unless durable merge-applied evidence is
    present. Return ``True`` after completion, including repeated recovery.
    """
    ingestion.refresh_from_db()
    forwardsync = ingestion.sync
    branch = ingestion.branch
    if ingestion.merge_applied_at is None:
        return False
    if branch is not None:
        branch.refresh_from_db()
        if branch.status != BranchStatusChoices.MERGED:
            return False

    if (
        ingestion.merge_finalized_at is not None
        and ingestion.baseline_ready
        and forwardsync.status == ForwardSyncStatusChoices.COMPLETED
    ):
        if remove_branch and branch is not None:
            ingestion._cleanup_merged_branch()
        if ingestion.catchup_status in {
            ForwardCatchupStatusChoices.PENDING,
            ForwardCatchupStatusChoices.FAILED,
        }:
            reconcile_catchup_if_ownership_complete(
                ingestion,
                current_job=ingestion.merge_job,
            )
        return True

    context = _post_merge_context(ingestion, mark_baseline_ready)
    _complete_post_merge_bookkeeping(
        ingestion,
        context=context,
        remove_branch=remove_branch,
    )
    return True


def sync_merge_ingestion(
    ingestion,
    *,
    mark_baseline_ready=None,
    remove_branch=True,
    claimed_job=None,
):
    from .merge import merge_branch

    forwardsync = ingestion.sync
    forwardsync.refresh_from_db(fields=["status"])
    claimed_job_id = getattr(claimed_job, "pk", None)
    merge_user = getattr(claimed_job, "user", None) or forwardsync.user
    if merge_user is None:
        raise SyncError("Merge attribution requires an invoking user or sync owner.")
    if forwardsync.status == ForwardSyncStatusChoices.MERGING and (
        claimed_job_id is None or ingestion.merge_job_id != claimed_job_id
    ):
        raise SyncError("Cannot initiate merge; merge already in progress.")

    pre_sync.send(sender=ingestion.__class__, instance=ingestion)
    context = _post_merge_context(ingestion, mark_baseline_ready)

    forwardsync.status = ForwardSyncStatusChoices.MERGING
    ForwardSync = forwardsync.__class__
    ForwardSync.objects.filter(pk=forwardsync.pk).update(status=forwardsync.status)

    try:
        with suppress_branch_merge_side_effect_signals():
            merge_branch(
                ingestion=ingestion,
                sync_logger=forwardsync.logger,
                user=merge_user,
            )
        _complete_post_merge_bookkeeping(
            ingestion,
            context=context,
            remove_branch=remove_branch,
        )
    except ForwardPartialMergeError:
        forwardsync.status = ForwardSyncStatusChoices.READY_TO_MERGE
        ForwardSync.objects.filter(pk=forwardsync.pk).update(
            status=forwardsync.status,
        )
        forwardsync.source.status = ForwardSourceStatusChoices.READY
        forwardsync.source.__class__.objects.filter(pk=forwardsync.source.pk).update(
            status=forwardsync.source.status
        )
        raise
    except Exception:
        ingestion.refresh_from_db(fields=["merge_applied_at", "merge_finalized_at"])
        post_merge_failure = ingestion.merge_applied_at is not None
        forwardsync.status = (
            ForwardSyncStatusChoices.MERGING
            if post_merge_failure
            else ForwardSyncStatusChoices.FAILED
        )
        ForwardSync.objects.filter(pk=forwardsync.pk).update(
            status=forwardsync.status,
        )
        forwardsync.source.status = (
            ForwardSourceStatusChoices.READY
            if post_merge_failure
            else ForwardSourceStatusChoices.FAILED
        )
        forwardsync.source.__class__.objects.filter(pk=forwardsync.source.pk).update(
            status=forwardsync.source.status
        )
        raise


def enqueue_merge_job(
    ingestion,
    user,
    remove_branch=False,
    *,
    recovery_sync_job_pks=None,
):
    with advisory_lock(ADVISORY_LOCK_KEYS["job-schedules"]):
        with transaction.atomic():
            locked = ingestion.__class__.objects.select_for_update().get(
                pk=ingestion.pk
            )
            existing_job = (
                Job.objects.filter(pk=locked.merge_job_id).first()
                if locked.merge_job_id
                else None
            )
            if existing_job is not None and not existing_job.completed:
                ingestion.merge_job = existing_job
                return existing_job
            sync = locked.sync.__class__.objects.select_for_update().get(
                pk=locked.sync_id
            )
            sync.status = ForwardSyncStatusChoices.QUEUED
            sync.__class__.objects.filter(pk=sync.pk).update(status=sync.status)
            change_count = (
                locked.branch.get_unmerged_changes().count() if locked.branch_id else 0
            )
            job = enqueue_forward_job(
                import_string("forward_netbox.jobs.merge_forwardingestion"),
                name=f"{locked.name} Merge",
                instance=locked,
                user=user,
                remove_branch=remove_branch,
                recovery_sync_job_pks=list(recovery_sync_job_pks or []),
                job_timeout=effective_merge_job_timeout(change_count),
            )
            ingestion.__class__.objects.filter(pk=locked.pk).update(merge_job=job)
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
    with transaction.atomic(using=DEFAULT_DB_ALIAS):
        locked_ingestion = ingestion.__class__.objects.select_for_update().get(
            pk=ingestion.pk
        )
        if locked_ingestion.branch_id is None:
            ingestion.branch = None
            return

        branching_branch = (
            locked_ingestion.branch.__class__.objects.select_for_update().get(
                pk=locked_ingestion.branch_id
            )
        )
        if branching_branch.status != BranchStatusChoices.MERGED:
            raise SyncError(
                "Merged branch cleanup requires a persisted merged branch state."
            )

        # These rows are branch-owned indexes with no delete hooks. Delete them
        # as sets so Django's Collector does not hydrate millions of rows before
        # Branch.delete() performs its normal row and schema teardown.
        AppliedChange.objects.filter(branch_id=branching_branch.pk)._raw_delete(
            using=DEFAULT_DB_ALIAS
        )
        ChangeDiff.objects.filter(branch_id=branching_branch.pk)._raw_delete(
            using=DEFAULT_DB_ALIAS
        )
        branching_branch.delete()

    ingestion.branch = None
