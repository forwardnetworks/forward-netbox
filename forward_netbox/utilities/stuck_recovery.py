# Recovery for syncs wedged by a dead worker. Before this, a worker killed
# mid-merge left ForwardSync.status=MERGING and Branch.status=MERGING forever;
# the only "recovery" was the next scheduled run failing on the MERGING guard
# and force-flipping the sync to FAILED (a wasted cycle that never happens on
# interval-less syncs). This module classifies a stuck sync and either
# re-enqueues its merge (idempotent because Branching returns the complete
# logical branch on retry) or fails it cleanly so schedules resume.
#
# READ-ONLY detection uses the same RQ-liveness probe as the stuck-job alert;
# recovery takes the job-schedules advisory lock and re-checks liveness inside
# it so an actively-merging (but Redis-slow) branch is never disturbed.
import logging

from core.choices import JobStatusChoices
from core.models import Job
from core.models import ObjectType
from django.utils import timezone
from django_pglocks import advisory_lock
from netbox.constants import ADVISORY_LOCK_KEYS
from netbox_branching.choices import BranchStatusChoices

from ..choices import ForwardIngestionPhaseChoices
from ..choices import ForwardSyncStatusChoices
from .job_liveness import job_has_live_execution
from .sync_facade import sync_run_job_names

logger = logging.getLogger("forward_netbox.stuck_recovery")

# Bounded retries so a genuinely un-mergeable branch cannot requeue forever
# (mirrors the pre-2.0 AUTO_MERGE_STALE_MERGE_REQUEUE_LIMIT precedent).
FORWARD_STUCK_MERGE_REQUEUE_LIMIT = 4
# Grace before a wedged sync is eligible: must exceed job_liveness's 180s
# started-heartbeat window so a briefly-paused live worker is never touched.
RECOVERY_GRACE_SECONDS = 900

_STUCK_STATUSES = (
    ForwardSyncStatusChoices.QUEUED,
    ForwardSyncStatusChoices.SYNCING,
    ForwardSyncStatusChoices.MERGING,
)
_RECOVERABLE_STATUSES = _STUCK_STATUSES + (
    ForwardSyncStatusChoices.COMPLETED,
    ForwardSyncStatusChoices.FAILED,
)


def _latest_ingestion(sync):
    from ..models import ForwardIngestion

    return (
        ForwardIngestion.objects.filter(sync=sync).order_by("-pk").first()
        if getattr(sync, "pk", None)
        else None
    )


def _dedupe_jobs(jobs):
    return list({job.pk: job for job in jobs if job.pk is not None}.values())


def _producer_jobs(sync):
    """Return every active sync producer, including jobs rebound to old ingestions."""
    active_statuses = (
        JobStatusChoices.STATUS_SCHEDULED,
        JobStatusChoices.STATUS_PENDING,
        JobStatusChoices.STATUS_RUNNING,
    )
    names = sync_run_job_names(sync)
    jobs = list(
        sync.jobs.filter(
            status__in=active_statuses,
            name__in=names,
        )
    )
    if getattr(sync, "pk", None):
        from ..models import ForwardIngestion

        ingestion_type = ObjectType.objects.get_for_model(
            ForwardIngestion,
            for_concrete_model=False,
        )
        ingestion_ids = ForwardIngestion.objects.filter(sync=sync).values("pk")
        jobs.extend(
            Job.objects.filter(
                object_type=ingestion_type,
                object_id__in=ingestion_ids,
                status__in=active_statuses,
                name__in=names,
            )
        )
    return _dedupe_jobs(jobs)


def _standing_jobs(sync):
    """Return canonical validation and dependency-preview schedule occurrences."""
    return list(
        sync.jobs.filter(
            name__in=("validation", "dependency preview"),
            status__in=(
                JobStatusChoices.STATUS_SCHEDULED,
                JobStatusChoices.STATUS_PENDING,
                JobStatusChoices.STATUS_RUNNING,
            ),
        )
    )


def _dead_standing_jobs(sync, *, grace_seconds, now=None):
    now = now or timezone.now()
    dead = []
    for job in _standing_jobs(sync):
        anchor = job.started or job.scheduled or job.created
        if (
            job.status == JobStatusChoices.STATUS_SCHEDULED
            and job.scheduled is not None
            and job.scheduled > now
        ):
            continue
        if job_has_live_execution(job):
            continue
        if anchor is not None and (now - anchor).total_seconds() < grace_seconds:
            continue
        dead.append(job)
    return dead


def _overlay_jobs(sync):
    active_statuses = (
        JobStatusChoices.STATUS_PENDING,
        JobStatusChoices.STATUS_RUNNING,
    )
    names = (
        f"{sync.name} - refresh device analysis (auto)",
        f"{sync.name} - reconcile device scope tags (auto)",
        f"{sync.name} - link vsys/vdom parents (auto)",
    )
    return list(sync.jobs.filter(status__in=active_statuses, name__in=names))


def _merge_jobs(sync):
    from ..models import ForwardIngestion

    active_statuses = (
        JobStatusChoices.STATUS_PENDING,
        JobStatusChoices.STATUS_RUNNING,
    )
    return _dedupe_jobs(
        ingestion.merge_job
        for ingestion in ForwardIngestion.objects.filter(
            sync=sync,
            merge_job__status__in=active_statuses,
        ).select_related("merge_job")
        if ingestion.merge_job is not None
    )


def _candidate_job_groups(sync):
    groups = {
        "producer_jobs": _producer_jobs(sync),
        "merge_jobs": _merge_jobs(sync),
        "overlay_jobs": [],
    }
    if sync.status == ForwardSyncStatusChoices.COMPLETED:
        groups["overlay_jobs"] = _overlay_jobs(sync)
    groups["all_jobs"] = _dedupe_jobs(
        job for key, jobs in groups.items() if key != "all_jobs" for job in jobs
    )
    return groups


def _candidate_jobs(sync, ingestion=None):
    return _candidate_job_groups(sync)["all_jobs"]


def _job_verdict_fields(groups):
    return {
        "dead_job_pks": [job.pk for job in groups["all_jobs"]],
        "producer_job_pks": [job.pk for job in groups["producer_jobs"]],
        "merge_job_pks": [job.pk for job in groups["merge_jobs"]],
        "overlay_job_pks": [job.pk for job in groups["overlay_jobs"]],
    }


def _stuck_recovery_state(sync):
    return dict((sync.parameters or {}).get("stuck_recovery") or {})


def classify_stuck_sync(sync, *, grace_seconds=RECOVERY_GRACE_SECONDS):
    """Return a verdict dict for a wedged sync, or None if it is healthy or
    out of scope.

    Verdict actions: "requeue_merge" (dead merge, branch resumable),
    "fail_sync" (dead sync run, or MERGING with no branch left), "give_up"
    (requeue budget exhausted -> fail + record an issue).
    """
    now = timezone.now()
    dead_standing_jobs = _dead_standing_jobs(
        sync,
        grace_seconds=grace_seconds,
        now=now,
    )
    if dead_standing_jobs:
        dead_job_pks = [job.pk for job in dead_standing_jobs]
        return {
            "action": "reconcile_standing_schedules",
            "reason": "dead standing schedule occurrence",
            "dead_job_pks": dead_job_pks,
            "standing_job_pks": dead_job_pks,
            "producer_job_pks": [],
            "merge_job_pks": [],
            "overlay_job_pks": [],
        }

    if sync.status not in _RECOVERABLE_STATUSES:
        # READY_TO_MERGE is the operator review lane; never auto-recover it.
        return None

    ingestion = _latest_ingestion(sync)
    merge_applied = bool(getattr(ingestion, "merge_applied_at", None))
    if sync.status == ForwardSyncStatusChoices.FAILED and not merge_applied:
        return None
    groups = _candidate_job_groups(sync)
    jobs = groups["all_jobs"]
    verdict_jobs = _job_verdict_fields(groups)

    if sync.status == ForwardSyncStatusChoices.COMPLETED:
        from .ownership import ownership_finalization_summary

        finalization = ownership_finalization_summary(sync)
        if finalization["complete"] and not jobs:
            return None

    # Never disturb a branch a live worker is still merging (liveness treats an
    # un-inspectable Redis as alive by design).
    if any(job_has_live_execution(job) for job in jobs):
        return None

    timestamps = [
        ts
        for ts in (
            [
                getattr(job, "started", None)
                or getattr(job, "scheduled", None)
                or getattr(job, "created", None)
                for job in jobs
            ]
            or [getattr(sync, "last_updated", None)]
        )
        if ts is not None
    ]
    if sync.status == ForwardSyncStatusChoices.COMPLETED:
        from ..models import ForwardOwnershipReconciliation

        timestamps.extend(
            timestamp
            for timestamp in ForwardOwnershipReconciliation.objects.filter(
                sync=sync
            ).values_list("started_at", flat=True)
            if timestamp is not None
        )
    newest = max(timestamps) if timestamps else None
    if newest is not None and (now - newest).total_seconds() < grace_seconds:
        return None

    state = _stuck_recovery_state(sync)
    ingestion_id = getattr(ingestion, "pk", None)
    attempts = int(state.get("attempts") or 0)
    if state.get("ingestion_id") != ingestion_id:
        attempts = 0

    branch = getattr(ingestion, "branch", None) if ingestion else None
    branch_status = str(getattr(branch, "status", "") or "")
    is_merging = sync.status == ForwardSyncStatusChoices.MERGING or (
        sync.status == ForwardSyncStatusChoices.FAILED and merge_applied
    )

    if (
        is_merging
        and merge_applied
        and (branch is None or branch_status == BranchStatusChoices.MERGED)
    ):
        return {
            "action": "finalize_merged_bookkeeping",
            "reason": "branch merged before producer bookkeeping completed",
            "ingestion_id": ingestion_id,
            "attempts": attempts,
            **verdict_jobs,
        }

    if sync.status == ForwardSyncStatusChoices.COMPLETED:
        if finalization["complete"]:
            return {
                "action": "finalize_completed_jobs",
                "reason": "ownership converged before producer job termination",
                "ingestion_id": ingestion_id,
                "attempts": attempts,
                **verdict_jobs,
            }
        return {
            "action": "redispatch_ownership",
            "reason": "completed ingestion has unconverged ownership",
            "ingestion_id": ingestion_id,
            "attempts": attempts,
            **verdict_jobs,
        }

    if (
        is_merging
        and branch is not None
        and branch_status
        in (
            BranchStatusChoices.MERGING,
            BranchStatusChoices.READY,
        )
    ):
        if attempts >= FORWARD_STUCK_MERGE_REQUEUE_LIMIT:
            return {
                "action": "give_up",
                "reason": "merge requeue budget exhausted",
                "ingestion_id": ingestion_id,
                "attempts": attempts,
                **verdict_jobs,
            }
        return {
            "action": "requeue_merge",
            "reason": "dead merge worker; branch resumable",
            "ingestion_id": ingestion_id,
            "attempts": attempts,
            **verdict_jobs,
        }

    return {
        "action": "fail_sync",
        "reason": (
            "dead sync-run worker"
            if not is_merging
            else "MERGING with no resumable branch"
        ),
        "ingestion_id": ingestion_id,
        "attempts": attempts,
        **verdict_jobs,
    }


def _terminal_mark(job_pks):
    if job_pks:
        Job.objects.filter(
            pk__in=job_pks,
            status__in=JobStatusChoices.ENQUEUED_STATE_CHOICES,
        ).update(
            status=JobStatusChoices.STATUS_FAILED,
            completed=timezone.now(),
            error="No live RQ execution; auto-recovered by stuck-run recovery.",
        )


def _completed_mark(job_pks):
    if job_pks:
        Job.objects.filter(
            pk__in=job_pks,
            status__in=JobStatusChoices.ENQUEUED_STATE_CHOICES,
        ).update(
            status=JobStatusChoices.STATUS_COMPLETED,
            completed=timezone.now(),
            error="",
        )


def _set_stuck_recovery_state(sync, *, ingestion_id, attempts):
    parameters = dict(sync.parameters or {})
    parameters["stuck_recovery"] = {
        "ingestion_id": ingestion_id,
        "attempts": attempts,
        "last_attempt_at": timezone.now().isoformat(),
    }
    sync.parameters = parameters
    sync.__class__.objects.filter(pk=sync.pk).update(parameters=parameters)


def _reconcile_recovered_producer_schedules(sync, producer_job_pks):
    from ..jobs import _complete_recovered_sync_producers

    _complete_recovered_sync_producers(sync, producer_job_pks)


def recover_stuck_sync(sync, verdict, *, user=None):
    """Act on a classify_stuck_sync verdict. Idempotent and lock-guarded."""
    from ..models import ForwardIngestionIssue
    from ..models import ForwardSync

    with advisory_lock(ADVISORY_LOCK_KEYS["job-schedules"]):
        sync.refresh_from_db()
        ingestion = _latest_ingestion(sync)
        action = verdict["action"]
        if action == "reconcile_standing_schedules":
            current_jobs = _dead_standing_jobs(sync, grace_seconds=0)
        else:
            current_jobs = _candidate_jobs(sync, ingestion)
        expected_job_pks = set(verdict.get("dead_job_pks") or [])
        current_job_pks = {job.pk for job in current_jobs}
        if current_job_pks != expected_job_pks:
            return {
                "action": "skipped",
                "reason": "candidate jobs changed under lock",
            }
        # Re-check liveness under the lock: a worker may have come back.
        if any(job_has_live_execution(job) for job in current_jobs):
            return {"action": "skipped", "reason": "became live under lock"}

        # Classification happens before the command acquires this lock. Recompute
        # it here so an operator transition or another worker cannot leave us
        # applying a stale destructive action to a sync that changed state.
        current_verdict = classify_stuck_sync(sync, grace_seconds=0)
        if current_verdict is None:
            return {"action": "skipped", "reason": "state changed under lock"}
        verdict = current_verdict

        action = verdict["action"]
        branch = getattr(ingestion, "branch", None) if ingestion else None
        if action == "reconcile_standing_schedules":
            from .sync_facade import reconcile_standing_schedules

            _terminal_mark(verdict.get("standing_job_pks") or [])
            reconcile_standing_schedules(sync, user=user or sync.user)
            logger.warning(
                "Recovered standing schedules for ForwardSync %s after a dead occurrence.",
                sync.pk,
            )
            return {"action": "reconciled_standing_schedules"}

        if action == "finalize_completed_jobs":
            _completed_mark(verdict.get("dead_job_pks") or [])
            _reconcile_recovered_producer_schedules(
                sync,
                verdict.get("producer_job_pks") or [],
            )
            logger.warning(
                "Finalized successful producer jobs for ForwardSync %s after "
                "ownership convergence.",
                sync.pk,
            )
            return {"action": "finalized_completed_jobs"}

        if action == "requeue_merge":
            # Keep a dead producer nonterminal until the replacement merge
            # succeeds. The merge worker then terminates it and restores the
            # scheduled occurrence from the same completion-time anchor.
            _terminal_mark(verdict.get("merge_job_pks") or [])
            if branch is not None and str(branch.status) == BranchStatusChoices.MERGING:
                branch.status = BranchStatusChoices.READY
                branch.save(update_fields=["status", "last_updated"])
            attempts = int(verdict.get("attempts") or 0) + 1
            _set_stuck_recovery_state(
                sync, ingestion_id=verdict.get("ingestion_id"), attempts=attempts
            )
            job = ingestion.enqueue_merge_job(
                user or sync.user,
                remove_branch=True,
                recovery_sync_job_pks=verdict.get("producer_job_pks") or [],
            )
            logger.warning(
                "Auto-recovered stuck merge for ForwardSync %s (attempt %s): "
                "re-enqueued merge job %s.",
                sync.pk,
                attempts,
                job.pk,
            )
            return {"action": "requeued_merge", "job_id": job.pk, "attempts": attempts}

        if action == "redispatch_ownership":
            from ..jobs import _enqueue_post_sync_overlays
            from .ingestion_merge import resume_post_merge_bookkeeping

            _terminal_mark(verdict.get("overlay_job_pks") or [])
            if getattr(ingestion, "merge_applied_at", None):
                resume_post_merge_bookkeeping(ingestion, remove_branch=True)

            result = _enqueue_post_sync_overlays(
                sync,
                snapshot_id=getattr(ingestion, "snapshot_id", ""),
                ingestion_id=getattr(ingestion, "pk", None),
            )
            _completed_mark(verdict.get("merge_job_pks") or [])
            _reconcile_recovered_producer_schedules(
                sync,
                verdict.get("producer_job_pks") or [],
            )
            logger.warning(
                "Redispatched unconverged ownership for ForwardSync %s.",
                sync.pk,
            )
            return {"action": "redispatched_ownership", "dispatch": result}

        if action == "finalize_merged_bookkeeping":
            from ..jobs import _enqueue_post_sync_overlays
            from .ingestion_merge import resume_post_merge_bookkeeping
            from .logging import SyncLogging

            merge_job = getattr(ingestion, "merge_job", None)
            sync.logger = SyncLogging(job=getattr(merge_job, "pk", None))
            completed = resume_post_merge_bookkeeping(
                ingestion,
                remove_branch=True,
            )
            if not completed:
                raise RuntimeError(
                    "Merged-branch bookkeeping recovery found a non-merged branch."
                )
            ingestion.refresh_from_db()
            sync.refresh_from_db()
            dispatch = _enqueue_post_sync_overlays(
                sync,
                snapshot_id=ingestion.snapshot_id,
                ingestion_id=ingestion.pk,
            )
            _completed_mark(
                (verdict.get("merge_job_pks") or [])
                + (verdict.get("overlay_job_pks") or [])
            )
            _reconcile_recovered_producer_schedules(
                sync,
                verdict.get("producer_job_pks") or [],
            )
            logger.warning(
                "Completed post-merge bookkeeping for ForwardSync %s.", sync.pk
            )
            return {
                "action": "finalized_merged_bookkeeping",
                "dispatch": dispatch,
            }

        # fail_sync / give_up both terminate the sync so schedules resume.
        _terminal_mark(verdict.get("dead_job_pks") or [])
        sync.status = ForwardSyncStatusChoices.FAILED
        ForwardSync.objects.filter(pk=sync.pk).update(
            status=ForwardSyncStatusChoices.FAILED
        )
        if action == "give_up" and ingestion is not None:
            ForwardIngestionIssue.objects.create(
                ingestion=ingestion,
                phase=ForwardIngestionPhaseChoices.MERGE,
                message=(
                    "Stuck-merge recovery exhausted its retry budget; marked "
                    "the sync FAILED. Inspect the branch and re-run manually."
                ),
                exception="StuckMergeRecoveryExhausted",
            )
        logger.warning(
            "Auto-recovered stuck ForwardSync %s: marked FAILED (%s).",
            sync.pk,
            verdict.get("reason"),
        )
        _reconcile_recovered_producer_schedules(
            sync,
            verdict.get("producer_job_pks") or [],
        )
        return {"action": "failed_sync", "reason": verdict.get("reason")}


def recover_all_stuck_syncs(
    *, apply=False, grace_seconds=RECOVERY_GRACE_SECONDS, user=None
):
    """Classify every candidate sync; act only when apply=True. Returns a list
    of {sync_id, name, verdict, result} for reporting."""
    from ..models import ForwardSync

    results = []
    # Standing validation/preview intent is valid in every sync state, so scan
    # every sync before applying the narrower sync-run recovery classification.
    for sync in ForwardSync.objects.all():
        verdict = classify_stuck_sync(sync, grace_seconds=grace_seconds)
        if verdict is None:
            continue
        entry = {
            "sync_id": sync.pk,
            "name": sync.name,
            "verdict": verdict,
        }
        if apply:
            entry["result"] = recover_stuck_sync(sync, verdict, user=user)
        results.append(entry)
    return results
