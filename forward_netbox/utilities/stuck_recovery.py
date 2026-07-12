# Recovery for syncs wedged by a dead worker. Before this, a worker killed
# mid-merge left ForwardSync.status=MERGING and Branch.status=MERGING forever;
# the only "recovery" was the next scheduled run failing on the MERGING guard
# and force-flipping the sync to FAILED (a wasted cycle that never happens on
# interval-less syncs). This module classifies a stuck sync and either
# re-enqueues its merge (idempotent — merge_forwardingestion resumes from the
# unmerged suffix) or fails it cleanly so schedules resume.
#
# READ-ONLY detection uses the same RQ-liveness probe as the stuck-job alert;
# recovery takes the job-schedules advisory lock and re-checks liveness inside
# it so an actively-merging (but Redis-slow) branch is never disturbed.
import logging

from core.choices import JobStatusChoices
from core.models import Job
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


def _latest_ingestion(sync):
    from ..models import ForwardIngestion

    return (
        ForwardIngestion.objects.filter(sync=sync).order_by("-pk").first()
        if getattr(sync, "pk", None)
        else None
    )


def _candidate_jobs(sync, ingestion):
    jobs = list(
        sync.jobs.filter(
            name__in=sync_run_job_names(sync),
            status__in=(
                JobStatusChoices.STATUS_PENDING,
                JobStatusChoices.STATUS_RUNNING,
            ),
        )
    )
    merge_job = getattr(ingestion, "merge_job", None) if ingestion else None
    if merge_job is not None and merge_job.completed is None:
        jobs.append(merge_job)
    return jobs


def _stuck_recovery_state(sync):
    return dict((sync.parameters or {}).get("stuck_recovery") or {})


def classify_stuck_sync(sync, *, grace_seconds=RECOVERY_GRACE_SECONDS):
    """Return a verdict dict for a wedged sync, or None if it is healthy or
    out of scope.

    Verdict actions: "requeue_merge" (dead merge, branch resumable),
    "fail_sync" (dead sync run, or MERGING with no branch left), "give_up"
    (requeue budget exhausted -> fail + record an issue).
    """
    if sync.status not in _STUCK_STATUSES:
        # READY_TO_MERGE is the operator review lane; never auto-recover it.
        return None

    ingestion = _latest_ingestion(sync)
    jobs = _candidate_jobs(sync, ingestion)

    # Never disturb a branch a live worker is still merging (liveness treats an
    # un-inspectable Redis as alive by design).
    if any(job_has_live_execution(job) for job in jobs):
        return None

    now = timezone.now()
    timestamps = [
        ts
        for ts in (
            [
                getattr(job, "started", None) or getattr(job, "created", None)
                for job in jobs
            ]
            or [getattr(sync, "last_updated", None)]
        )
        if ts is not None
    ]
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
    is_merging = sync.status == ForwardSyncStatusChoices.MERGING

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
            }
        return {
            "action": "requeue_merge",
            "reason": "dead merge worker; branch resumable",
            "ingestion_id": ingestion_id,
            "attempts": attempts,
            "dead_job_pks": [job.pk for job in jobs],
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
        "dead_job_pks": [job.pk for job in jobs],
    }


def _terminal_mark(job_pks):
    if job_pks:
        Job.objects.filter(pk__in=job_pks).update(
            status=JobStatusChoices.STATUS_FAILED,
            completed=timezone.now(),
            error="No live RQ execution; auto-recovered by stuck-run recovery.",
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


def recover_stuck_sync(sync, verdict, *, user=None):
    """Act on a classify_stuck_sync verdict. Idempotent and lock-guarded."""
    from ..models import ForwardIngestionIssue
    from ..models import ForwardSync

    with advisory_lock(ADVISORY_LOCK_KEYS["job-schedules"]):
        sync.refresh_from_db()
        ingestion = _latest_ingestion(sync)
        # Re-check liveness under the lock: a worker may have come back.
        if any(job_has_live_execution(job) for job in _candidate_jobs(sync, ingestion)):
            return {"action": "skipped", "reason": "became live under lock"}

        action = verdict["action"]
        _terminal_mark(verdict.get("dead_job_pks") or [])

        if action == "requeue_merge":
            branch = getattr(ingestion, "branch", None)
            if branch is not None and str(branch.status) == BranchStatusChoices.MERGING:
                branch.status = BranchStatusChoices.READY
                branch.save(update_fields=["status", "last_updated"])
            attempts = int(verdict.get("attempts") or 0) + 1
            _set_stuck_recovery_state(
                sync, ingestion_id=verdict.get("ingestion_id"), attempts=attempts
            )
            job = ingestion.enqueue_merge_job(user or sync.user, remove_branch=True)
            logger.warning(
                "Auto-recovered stuck merge for ForwardSync %s (attempt %s): "
                "re-enqueued merge job %s.",
                sync.pk,
                attempts,
                job.pk,
            )
            return {"action": "requeued_merge", "job_id": job.pk, "attempts": attempts}

        # fail_sync / give_up both terminate the sync so schedules resume.
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
        return {"action": "failed_sync", "reason": verdict.get("reason")}


def recover_all_stuck_syncs(
    *, apply=False, grace_seconds=RECOVERY_GRACE_SECONDS, user=None
):
    """Classify every candidate sync; act only when apply=True. Returns a list
    of {sync_id, name, verdict, result} for reporting."""
    from ..models import ForwardSync

    results = []
    for sync in ForwardSync.objects.filter(status__in=_STUCK_STATUSES):
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
