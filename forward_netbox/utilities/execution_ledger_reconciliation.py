from core.choices import JobStatusChoices
from django.db import transaction
from django.utils import timezone

from ..choices import ForwardExecutionRunStatusChoices
from ..choices import ForwardExecutionStepKindChoices
from ..choices import ForwardExecutionStepStatusChoices
from ..choices import ForwardIngestionPhaseChoices
from ..choices import ForwardSyncStatusChoices
from .sync_state import STALE_BRANCH_PROGRESS_SECONDS


def reconcile_execution_run(run, *, update_run_from_branch_state_fn):
    if run is None:
        return {"run": None, "updated_steps": 0, "updated_run": False}

    updated_steps = 0
    messages = []
    events = []
    now = timezone.now()
    for step in run.steps.select_related("job", "merge_job"):
        changed = False
        old_status = step.status
        reason = ""
        if (
            step.status == ForwardExecutionStepStatusChoices.PENDING
            and step.job_id is not None
        ):
            step.job = None
            changed = True
            reason = "cleared_stale_pending_job_binding"
        if _stage_step_stale_without_branch(step, now):
            step.status = ForwardExecutionStepStatusChoices.FAILED
            step.last_error = step.last_error or (
                "Stage job heartbeat is stale and no branch was recorded; retry the "
                "current step instead of restarting the baseline."
            )
            step.completed = step.completed or now
            step.heartbeat = now
            changed = True
            reason = "stale_stage_without_branch"
        elif _stage_step_stale_with_branch(step, now):
            step.status = ForwardExecutionStepStatusChoices.FAILED
            step.last_error = step.last_error or (
                "Stage job heartbeat is stale after a branch was recorded; discard "
                "the failed shard branch and retry the current step before "
                "continuing."
            )
            step.completed = step.completed or now
            step.heartbeat = now
            changed = True
            reason = "stale_stage_with_branch"
        elif _merge_step_stale(step, now):
            step.status = ForwardExecutionStepStatusChoices.MERGE_TIMEOUT
            step.last_error = step.last_error or (
                "Merge job heartbeat is stale; requeue the merge for the existing "
                "branch before rerunning the shard."
            )
            step.completed = step.completed or now
            step.heartbeat = now
            changed = True
            reason = "stale_merge_job"
        elif _queued_step_has_applied_without_merge_path(step):
            step.status = ForwardExecutionStepStatusChoices.MERGED
            step.completed = step.completed or now
            step.heartbeat = now
            changed = True
            reason = "queued_step_applied_without_merge_path"
        if step.status in {
            ForwardExecutionStepStatusChoices.QUEUED,
            ForwardExecutionStepStatusChoices.RUNNING,
            ForwardExecutionStepStatusChoices.MERGE_QUEUED,
        }:
            job = step.merge_job if step.status == "merge_queued" else step.job
            if job and getattr(job, "completed", None):
                if getattr(job, "status", "") == JobStatusChoices.STATUS_ERRORED:
                    step.status = (
                        ForwardExecutionStepStatusChoices.MERGE_TIMEOUT
                        if step.status == ForwardExecutionStepStatusChoices.MERGE_QUEUED
                        else ForwardExecutionStepStatusChoices.FAILED
                    )
                    step.last_error = step.last_error or (
                        f"Associated job {job.pk} completed with status {job.status}."
                    )
                    reason = "associated_job_errored"
                elif step.status == ForwardExecutionStepStatusChoices.MERGE_QUEUED:
                    step.status = ForwardExecutionStepStatusChoices.MERGED
                    reason = "merge_job_completed"
                elif step.ingestion_id:
                    step.status = ForwardExecutionStepStatusChoices.STAGED
                    reason = "stage_job_completed"
                else:
                    step.status = ForwardExecutionStepStatusChoices.PENDING
                    reason = "stage_job_completed_without_ingestion"
                step.completed = step.completed or job.completed
                step.heartbeat = timezone.now()
                changed = True
        if (
            step.status
            in {
                ForwardExecutionStepStatusChoices.FAILED,
                ForwardExecutionStepStatusChoices.TIMEOUT,
                ForwardExecutionStepStatusChoices.MERGE_TIMEOUT,
            }
            and not step.last_error
        ):
            job = step.merge_job or step.job
            if job:
                step.last_error = (
                    f"Associated job {job.pk} completed with status "
                    f"{getattr(job, 'status', '') or 'unknown'}."
                )
                changed = True
                reason = reason or "failed_step_missing_error"
        if (
            step.status == ForwardExecutionStepStatusChoices.PENDING
            and step.job_id is not None
        ):
            step.job = None
            changed = True
            reason = reason or "cleared_stale_pending_job_binding"
        if changed:
            step.save()
            updated_steps += 1
            messages.append(f"Updated step {step.index} to {step.status}.")
            events.append(_reconciliation_step_event(step, old_status, reason))

    updated_steps += _normalize_inflight_stage_steps(run, events)

    run.sync.refresh_from_db()
    before = run.as_support_summary()
    refreshed = update_run_from_branch_state_fn(run.sync)
    run.refresh_from_db()
    updated_run = False
    if _enforce_monotonic_next_step_index(run):
        updated_run = True
    if (
        not run.steps.exclude(
            status__in=[
                ForwardExecutionStepStatusChoices.MERGED,
                ForwardExecutionStepStatusChoices.SKIPPED,
                ForwardExecutionStepStatusChoices.CANCELLED,
            ]
        ).exists()
        and run.status != ForwardExecutionRunStatusChoices.COMPLETED
    ):
        run.status = ForwardExecutionRunStatusChoices.COMPLETED
        run.phase = "completed"
        run.phase_message = "Forward execution completed."
        run.completed = run.completed or timezone.now()
        run.latest_heartbeat = timezone.now()
        run.save()
        messages.append("Marked execution run completed.")
        events.append(_reconciliation_run_event(run, "marked_completed"))

    after = run.as_support_summary()
    if _align_run_with_active_step(run):
        run.refresh_from_db()
        after = run.as_support_summary()
        updated_run = True
    updated_run = updated_run or (bool(refreshed) and before != after)
    if events:
        _append_reconciliation_events(run, events)
    return {
        "run": run,
        "updated_steps": updated_steps,
        "updated_run": updated_run,
        "messages": messages,
    }


def _normalize_inflight_stage_steps(run, events):
    now = timezone.now()
    updated = 0
    stage_steps = list(
        run.steps.filter(kind=ForwardExecutionStepKindChoices.STAGE).order_by("index")
    )
    inflight = [
        step
        for step in stage_steps
        if step.status
        in {
            ForwardExecutionStepStatusChoices.QUEUED,
            ForwardExecutionStepStatusChoices.RUNNING,
            ForwardExecutionStepStatusChoices.MERGE_QUEUED,
        }
    ]
    if len(inflight) <= 1:
        return 0

    merged_max = max(
        [
            int(step.index)
            for step in stage_steps
            if step.status
            in {
                ForwardExecutionStepStatusChoices.MERGED,
                ForwardExecutionStepStatusChoices.SKIPPED,
                ForwardExecutionStepStatusChoices.CANCELLED,
            }
        ]
        or [0]
    )
    floor_index = max(int(run.next_step_index or 1), merged_max + 1)
    keep = min(
        inflight,
        key=lambda step: (
            0 if step.status == ForwardExecutionStepStatusChoices.RUNNING else 1,
            0 if int(step.index) >= int(floor_index) else 1,
            int(step.index),
        ),
    )
    for step in inflight:
        if step.pk == keep.pk:
            continue
        old_status = step.status
        if step.status == ForwardExecutionStepStatusChoices.MERGE_QUEUED:
            step.status = ForwardExecutionStepStatusChoices.MERGE_TIMEOUT
            step.last_error = step.last_error or (
                "Reconciled duplicate merge_queued step; requeue merge for this shard."
            )
        else:
            step.status = ForwardExecutionStepStatusChoices.PENDING
            step.last_error = step.last_error or (
                "Reconciled duplicate queued/running stage step; this shard was "
                "reset to pending."
            )
            if step.job_id is not None:
                step.job = None
        step.completed = step.completed or now
        step.heartbeat = now
        step.save()
        updated += 1
        events.append(
            _reconciliation_step_event(step, old_status, "duplicate_inflight_step")
        )
    return updated


def _enforce_monotonic_next_step_index(run):
    merged_max = (
        run.steps.filter(
            kind=ForwardExecutionStepKindChoices.STAGE,
            status__in=[
                ForwardExecutionStepStatusChoices.MERGED,
                ForwardExecutionStepStatusChoices.SKIPPED,
                ForwardExecutionStepStatusChoices.CANCELLED,
            ],
        )
        .order_by("-index")
        .values_list("index", flat=True)
        .first()
        or 0
    )
    min_next = int(merged_max) + 1
    current = int(run.next_step_index or 1)
    if current >= min_next:
        return False
    run.next_step_index = min_next
    run.latest_heartbeat = timezone.now()
    run.save(update_fields=["next_step_index", "latest_heartbeat", "updated"])
    return True


def _align_run_with_active_step(run):
    active_step = (
        run.steps.filter(
            kind=ForwardExecutionStepKindChoices.STAGE,
            status__in=[
                ForwardExecutionStepStatusChoices.RUNNING,
                ForwardExecutionStepStatusChoices.QUEUED,
                ForwardExecutionStepStatusChoices.MERGE_QUEUED,
            ],
        )
        .order_by("index")
        .first()
    )
    if active_step is None:
        return False
    desired_index = int(active_step.index)
    desired_phase = (
        "queued_merge"
        if active_step.status == ForwardExecutionStepStatusChoices.MERGE_QUEUED
        else (
            "staging"
            if active_step.status == ForwardExecutionStepStatusChoices.RUNNING
            else "queued"
        )
    )
    total = int(run.total_steps or 0)
    if desired_phase == "queued_merge":
        desired_message = (
            f"Queued merge for shard {desired_index}/{total}."
            if total
            else f"Queued merge for shard {desired_index}."
        )
    elif desired_phase == "staging":
        desired_message = (
            f"Applying shard {desired_index}/{total}."
            if total
            else f"Applying shard {desired_index}."
        )
    else:
        desired_message = (
            f"Queued shard {desired_index}/{total} for Branching execution."
            if total
            else f"Queued shard {desired_index} for Branching execution."
        )
    changed = False
    if int(run.next_step_index or 1) != desired_index:
        run.next_step_index = desired_index
        changed = True
    if run.phase != desired_phase:
        run.phase = desired_phase
        changed = True
    if (run.phase_message or "") != desired_message:
        run.phase_message = desired_message
        changed = True
    if changed:
        run.latest_heartbeat = timezone.now()
        run.save(
            update_fields=[
                "next_step_index",
                "phase",
                "phase_message",
                "latest_heartbeat",
                "updated",
            ]
        )
    return changed


def current_retryable_step(run):
    if run is None:
        return None
    failed = (
        run.steps.filter(
            status__in=[
                ForwardExecutionStepStatusChoices.FAILED,
                ForwardExecutionStepStatusChoices.TIMEOUT,
                ForwardExecutionStepStatusChoices.MERGE_TIMEOUT,
            ]
        )
        .filter(branch__isnull=True, ingestion__branch__isnull=True)
        .order_by("index")
        .first()
    )
    if failed is not None:
        return failed
    return (
        run.steps.filter(
            index=run.next_step_index,
            kind=ForwardExecutionStepKindChoices.STAGE,
            branch__isnull=True,
            ingestion__branch__isnull=True,
        )
        .order_by("index")
        .first()
    )


def current_discardable_step(run):
    if run is None:
        return None
    return (
        run.steps.filter(
            status__in=[
                ForwardExecutionStepStatusChoices.FAILED,
                ForwardExecutionStepStatusChoices.TIMEOUT,
            ]
        )
        .filter(ingestion__branch__isnull=False)
        .order_by("index")
        .first()
    )


def current_mergeable_step(run):
    if run is None:
        return None
    step = (
        run.steps.filter(ingestion__isnull=False)
        .exclude(ingestion__branch__isnull=True)
        .order_by("index")
        .last()
    )
    if step is not None and step.ingestion.can_queue_merge:
        return step
    if (
        step is not None
        and step.status == ForwardExecutionStepStatusChoices.MERGE_TIMEOUT
    ):
        return step
    return None


def discard_stage_branch_for_retry(step, *, prepare_stage_step_retry_fn):
    if step is None:
        return None
    from ..models import ForwardExecutionStep
    from ..models import ForwardIngestion

    with transaction.atomic():
        step = ForwardExecutionStep.objects.select_for_update().get(pk=step.pk)
        if step.status not in {
            ForwardExecutionStepStatusChoices.FAILED,
            ForwardExecutionStepStatusChoices.TIMEOUT,
            ForwardExecutionStepStatusChoices.MERGE_TIMEOUT,
        }:
            return None
        ingestion = None
        if step.ingestion_id is not None:
            ingestion = ForwardIngestion.objects.select_for_update().get(
                pk=step.ingestion_id
            )
        branch = step.branch
        if branch is None and ingestion is not None:
            branch = ingestion.branch
        if branch is None:
            return None
        if getattr(branch, "status", "") == "merged":
            return None

        if ingestion is not None:
            ingestion.issues.create(
                message=(
                    "Discarded failed shard branch before retrying the execution step."
                ),
                phase=ForwardIngestionPhaseChoices.SYNC,
            )
            ingestion.branch = None
            ingestion.save(update_fields=["branch"])
        step.branch = None
        step.save(update_fields=["branch", "updated"])
    branch.delete()
    return prepare_stage_step_retry_fn(step)


def prepare_stage_step_retry(step):
    if step is None:
        return None
    from ..models import ForwardExecutionStep

    with transaction.atomic():
        step = (
            ForwardExecutionStep.objects.select_for_update()
            .select_related("run", "run__sync")
            .get(pk=step.pk)
        )
        run = step.run
        if run is None:
            return None
        if (
            step.status == ForwardExecutionStepStatusChoices.QUEUED
            and run.status == ForwardExecutionRunStatusChoices.RUNNING
        ):
            return None
        if step.status not in {
            ForwardExecutionStepStatusChoices.FAILED,
            ForwardExecutionStepStatusChoices.TIMEOUT,
            ForwardExecutionStepStatusChoices.MERGE_TIMEOUT,
            ForwardExecutionStepStatusChoices.PENDING,
            ForwardExecutionStepStatusChoices.QUEUED,
        }:
            return None

        run.status = ForwardExecutionRunStatusChoices.RUNNING
        run.phase = "queued"
        run.phase_message = f"Queued retry for shard {step.index}."
        run.next_step_index = int(step.index)
        run.latest_heartbeat = timezone.now()
        run.save(
            update_fields=[
                "status",
                "phase",
                "phase_message",
                "next_step_index",
                "latest_heartbeat",
            ]
        )
        run.sync.status = ForwardSyncStatusChoices.QUEUED
        run.sync.save(update_fields=["parameters", "status", "last_updated"])
        step.status = ForwardExecutionStepStatusChoices.QUEUED
        step.completed = None
        step.last_error = ""
        step.retry_count = int(step.retry_count or 0) + 1
        step.heartbeat = timezone.now()
        step.save()
        return step


def _stage_step_stale_without_branch(step, now):
    if step.status != ForwardExecutionStepStatusChoices.RUNNING:
        return False
    if step.kind != ForwardExecutionStepKindChoices.STAGE:
        return False
    if step.branch_id or step.branch_name or step.ingestion_id:
        return False
    timestamp = step.heartbeat or step.started
    if timestamp is None:
        return False
    return (now - timestamp).total_seconds() >= STALE_BRANCH_PROGRESS_SECONDS


def _queued_step_has_applied_without_merge_path(step):
    if step.status != ForwardExecutionStepStatusChoices.QUEUED:
        return False
    if step.kind != ForwardExecutionStepKindChoices.STAGE:
        return False
    if (
        int(step.applied_row_count or 0) <= 0
        and int(step.attempted_row_count or 0) <= 0
    ):
        return False
    if step.merge_job_id:
        return False
    if step.branch_id:
        return False
    if step.ingestion_id is None:
        return False
    ingestion = step.ingestion
    if ingestion is None:
        return False
    if getattr(ingestion, "branch_id", None):
        return False
    return True


def _stage_step_stale_with_branch(step, now):
    if step.status != ForwardExecutionStepStatusChoices.RUNNING:
        return False
    if step.kind != ForwardExecutionStepKindChoices.STAGE:
        return False
    if not (step.branch_id or step.branch_name or step.ingestion_id):
        return False
    timestamp = step.heartbeat or step.started
    if timestamp is None:
        return False
    return (now - timestamp).total_seconds() >= STALE_BRANCH_PROGRESS_SECONDS


def _merge_step_stale(step, now):
    if step.status != ForwardExecutionStepStatusChoices.MERGE_QUEUED:
        return False
    if not step.ingestion_id:
        return False
    branch = step.branch or getattr(step.ingestion, "branch", None)
    if branch is None or getattr(branch, "status", "") == "merged":
        return False
    timestamps = [
        step.heartbeat,
        getattr(step.merge_job, "started", None),
        getattr(step.merge_job, "created", None),
    ]
    timestamp = next((value for value in timestamps if value is not None), None)
    if timestamp is None:
        return False
    return (now - timestamp).total_seconds() >= STALE_BRANCH_PROGRESS_SECONDS


def _append_reconciliation_events(run, events):
    existing = (
        run.reconciliation_events if isinstance(run.reconciliation_events, list) else []
    )
    run.reconciliation_events = [*existing, *events][-100:]
    run.save(update_fields=["reconciliation_events", "updated"])


def _reconciliation_step_event(step, old_status, reason):
    job = (
        step.merge_job
        if step.status == ForwardExecutionStepStatusChoices.MERGE_TIMEOUT
        else step.job
    )
    return {
        "timestamp": timezone.now().isoformat(),
        "type": "step",
        "reason": reason or "status_reconciled",
        "step_id": step.pk,
        "index": step.index,
        "kind": step.kind,
        "model": step.model_string,
        "old_status": old_status,
        "new_status": step.status,
        "job": getattr(job, "pk", None),
        "job_status": getattr(job, "status", "") if job else "",
        "branch": step.branch_id,
        "ingestion": step.ingestion_id,
    }


def _reconciliation_run_event(run, reason):
    return {
        "timestamp": timezone.now().isoformat(),
        "type": "run",
        "reason": reason,
        "run_id": run.pk,
        "status": run.status,
        "phase": run.phase,
    }
