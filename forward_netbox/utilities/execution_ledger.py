from django.utils import timezone

from ..choices import ForwardApplyEngineChoices
from ..choices import ForwardExecutionRunStatusChoices
from ..choices import ForwardExecutionStepKindChoices
from ..choices import ForwardExecutionStepStatusChoices
from .execution_ledger_metrics import (
    apply_engine_decision as _apply_engine_decision_metric,
)
from .execution_ledger_metrics import duration_seconds as _duration_seconds_calc
from .execution_ledger_metrics import (
    execution_run_metrics as _execution_run_metrics_calc,
)
from .execution_ledger_metrics import fetch_explanation as _fetch_explanation_metric
from .execution_ledger_metrics import job_summary as _job_summary_metric
from .execution_ledger_metrics import runtime_bottleneck as _runtime_bottleneck_calc
from .execution_ledger_metrics import sum_optional_float as _sum_optional_float_calc
from .execution_ledger_reconciliation import (
    current_discardable_step as _current_discardable_step_impl,
)
from .execution_ledger_reconciliation import (
    current_mergeable_step as _current_mergeable_step_impl,
)
from .execution_ledger_reconciliation import (
    current_retryable_step as _current_retryable_step_impl,
)
from .execution_ledger_reconciliation import (
    discard_stage_branch_for_retry as _discard_stage_branch_for_retry_impl,
)
from .execution_ledger_reconciliation import (
    prepare_stage_step_retry as _prepare_stage_step_retry_impl,
)
from .execution_ledger_reconciliation import (
    reconcile_execution_run as _reconcile_execution_run_impl,
)
from .execution_ledger_run_store import (
    _execution_step_kwargs_from_plan_item as _execution_step_kwargs_from_plan_item_impl,
)
from .execution_ledger_run_store import (
    active_execution_run as _active_execution_run_impl,
)
from .execution_ledger_run_store import (
    claim_ingestion_merge_step as _claim_ingestion_merge_step_impl,
)
from .execution_ledger_run_store import claim_stage_step as _claim_stage_step_impl
from .execution_ledger_run_store import (
    ensure_branch_execution_run as _ensure_branch_execution_run_impl,
)
from .execution_ledger_run_store import (
    execution_step_for_ingestion as _execution_step_for_ingestion_impl,
)
from .execution_ledger_run_store import (
    ingestion_has_mergeable_execution_step as _ingestion_has_mergeable_execution_step_impl,
)
from .execution_ledger_run_store import (
    ingestion_has_requeueable_merge_timeout_step as _ingestion_has_requeueable_merge_timeout_step_impl,
)
from .execution_ledger_run_store import (
    latest_execution_run as _latest_execution_run_impl,
)
from .execution_ledger_run_store import (
    legacy_execution_run_from_branch_state as _legacy_execution_run_from_branch_state_impl,
)
from .execution_ledger_run_store import (
    mark_ingestion_step_merged as _mark_ingestion_step_merged_impl,
)
from .execution_ledger_run_store import mark_run_completed as _mark_run_completed_impl
from .execution_ledger_run_store import (
    sync_steps_from_plan as _sync_steps_from_plan_impl,
)
from .execution_ledger_run_store import (
    touch_execution_step_progress as _touch_execution_step_progress_impl,
)
from .execution_ledger_run_store import (
    update_run_from_branch_state as _update_run_from_branch_state_impl,
)
from .execution_ledger_run_store import (
    update_step_from_plan_item as _update_step_from_plan_item_impl,
)
from .execution_ledger_run_store import (
    upgrade_branch_run_state_to_execution_run as _upgrade_branch_run_state_to_execution_run_impl,
)
from .execution_ledger_serialization import (
    execution_run_support_bundle as _execution_run_support_bundle,
)
from .execution_ledger_serialization import (
    ingestion_support_summary as _ingestion_support_summary_data,
)
from .sync_state import STALE_BRANCH_PROGRESS_SECONDS


TERMINAL_STAGE_STATUSES = {
    ForwardExecutionStepStatusChoices.STAGED,
    ForwardExecutionStepStatusChoices.MERGE_QUEUED,
    ForwardExecutionStepStatusChoices.MERGED,
    ForwardExecutionStepStatusChoices.SKIPPED,
    ForwardExecutionStepStatusChoices.CANCELLED,
}

ACTIVE_STEP_STATUSES = {
    ForwardExecutionStepStatusChoices.QUEUED,
    ForwardExecutionStepStatusChoices.RUNNING,
    ForwardExecutionStepStatusChoices.MERGE_QUEUED,
}

FAILED_STEP_STATUSES = {
    ForwardExecutionStepStatusChoices.FAILED,
    ForwardExecutionStepStatusChoices.TIMEOUT,
    ForwardExecutionStepStatusChoices.MERGE_TIMEOUT,
}

DISCARDABLE_STAGE_FAILURE_STATUSES = {
    ForwardExecutionStepStatusChoices.FAILED,
    ForwardExecutionStepStatusChoices.TIMEOUT,
}

CLAIMABLE_STAGE_STATUSES = {
    ForwardExecutionStepStatusChoices.PENDING,
    ForwardExecutionStepStatusChoices.QUEUED,
    ForwardExecutionStepStatusChoices.FAILED,
    ForwardExecutionStepStatusChoices.TIMEOUT,
}

TERMINAL_RUN_STATUSES = {
    ForwardExecutionRunStatusChoices.COMPLETED,
    ForwardExecutionRunStatusChoices.FAILED,
    ForwardExecutionRunStatusChoices.TIMEOUT,
    ForwardExecutionRunStatusChoices.CANCELLED,
}

MERGEABLE_STEP_STATUSES = {
    ForwardExecutionStepStatusChoices.STAGED,
    ForwardExecutionStepStatusChoices.MERGE_QUEUED,
    ForwardExecutionStepStatusChoices.MERGE_TIMEOUT,
}


def ensure_branch_execution_run(
    *,
    sync,
    context,
    plan,
    plan_preview,
    validation_run=None,
    job=None,
    max_changes_per_branch,
    auto_merge,
    model_change_density=None,
    next_plan_index=None,
):
    return _ensure_branch_execution_run_impl(
        sync=sync,
        context=context,
        plan=plan,
        plan_preview=plan_preview,
        validation_run=validation_run,
        job=job,
        max_changes_per_branch=max_changes_per_branch,
        auto_merge=auto_merge,
        model_change_density=model_change_density,
        next_plan_index=next_plan_index,
    )


def sync_steps_from_plan(run, plan):
    return _sync_steps_from_plan_impl(run, plan)


def latest_execution_run(sync):
    return _latest_execution_run_impl(sync, terminal_run_statuses=TERMINAL_RUN_STATUSES)


def active_execution_run(sync):
    return _active_execution_run_impl(sync, terminal_run_statuses=TERMINAL_RUN_STATUSES)


def upgrade_branch_run_state_to_execution_run(sync):
    """Create ledger records from a pre-ledger _branch_run compatibility payload."""
    return _upgrade_branch_run_state_to_execution_run_impl(sync)


def _legacy_execution_run_from_branch_state(sync, state):
    return _legacy_execution_run_from_branch_state_impl(sync, state)


def execution_run_support_bundle(run):
    return _execution_run_support_bundle(
        run,
        recommendation_fn=execution_run_recovery_recommendation,
    )


def execution_run_recovery_recommendation(run):
    if run is None:
        return {
            "action": "none",
            "severity": "info",
            "message": "No execution run is available.",
        }
    discardable = current_discardable_step(run)
    if discardable is not None:
        return _recommendation(
            action="discard_branch_retry",
            severity="danger",
            message=(
                "Discard the failed shard branch and retry the current execution step."
            ),
            step=discardable,
        )
    retryable = current_retryable_step(run)
    if retryable is not None and retryable.status in FAILED_STEP_STATUSES:
        return _recommendation(
            action="retry_current_step",
            severity="warning",
            message="Retry the current execution step.",
            step=retryable,
        )
    mergeable = current_mergeable_step(run)
    if mergeable is not None:
        return _recommendation(
            action="requeue_merge",
            severity="warning",
            message="Requeue merge for the staged shard branch.",
            step=mergeable,
        )
    waiting_step = (
        run.steps.filter(status=ForwardExecutionStepStatusChoices.STAGED)
        .order_by("index")
        .first()
    )
    if (
        waiting_step is not None
        or run.status == ForwardExecutionRunStatusChoices.WAITING
    ):
        return _recommendation(
            action="wait_for_review",
            severity="info",
            message="Review and merge the staged Branching shard before continuing.",
            step=waiting_step,
        )
    active_step = (
        run.steps.filter(status__in=ACTIVE_STEP_STATUSES).order_by("index").first()
    )
    if active_step is not None:
        return _recommendation(
            action="wait",
            severity="info",
            message="Wait for the active execution step to finish.",
            step=active_step,
        )
    if run.status == ForwardExecutionRunStatusChoices.COMPLETED:
        return _recommendation(
            action="complete",
            severity="success",
            message="Execution run is complete.",
        )
    if run.status in {
        ForwardExecutionRunStatusChoices.FAILED,
        ForwardExecutionRunStatusChoices.TIMEOUT,
    }:
        return _recommendation(
            action="reconcile",
            severity="warning",
            message="Reconcile the execution run before retrying.",
        )
    return _recommendation(
        action="monitor",
        severity="info",
        message="Monitor the execution run.",
    )


def _recommendation(*, action, severity, message, step=None):
    return {
        "action": action,
        "severity": severity,
        "message": message,
        "step_id": getattr(step, "pk", None),
        "step_index": getattr(step, "index", None),
        "step_status": getattr(step, "status", "") if step is not None else "",
        "model": getattr(step, "model_string", "") if step is not None else "",
    }


def _ingestion_support_summary(ingestion):
    return _ingestion_support_summary_data(ingestion)


def execution_run_bundle_for_sync(sync):
    return execution_run_support_bundle(latest_execution_run(sync))


def branch_run_state_from_execution_run(run):
    if run is None:
        return {}
    steps = list(run.steps.order_by("index", "kind", "pk"))
    state = {
        "state_source": "execution_ledger",
        "state_synthesized": True,
        "snapshot_selector": run.snapshot_selector,
        "snapshot_id": run.snapshot_id,
        "max_changes_per_branch": run.max_changes_per_branch,
        "next_plan_index": int(run.next_step_index or 1),
        "total_plan_items": int(run.total_steps or 0),
        "auto_merge": bool(run.auto_merge),
        "awaiting_merge": False,
        "model_change_density": run.model_change_density or {},
        "validation_run_id": run.validation_run_id,
        "plan_preview": run.plan_preview or {},
        "plan_items": [_plan_item_from_execution_step(step) for step in steps],
        "phase": run.phase or "executing",
        "phase_message": run.phase_message or "Applying planned shard changes.",
        "execution_run_id": run.pk,
    }
    active_step = _active_progress_step(run, steps)
    if active_step is not None:
        state.update(_progress_state_from_execution_step(active_step, run))
    return state


def _active_progress_step(run, steps):
    active_steps = [
        step
        for step in steps
        if step.status == ForwardExecutionStepStatusChoices.RUNNING
    ]
    if active_steps:
        return sorted(active_steps, key=lambda step: (step.index, step.kind, step.pk))[
            0
        ]
    for step in steps:
        if int(step.index) == int(run.next_step_index or 0):
            return step
    return None


def _progress_state_from_execution_step(step, run):
    row_total = int(step.fetched_row_count or step.estimated_changes or 0)
    row_count = int(step.attempted_row_count or 0)
    progress = {
        "current_model_string": step.model_string,
        "current_shard_index": int(step.index),
        "total_plan_items": int(run.total_steps or 0),
    }
    if row_total:
        progress["current_row_total"] = row_total
    if row_count:
        progress["current_row_count"] = row_count
    if step.heartbeat:
        progress["last_progress_at"] = step.heartbeat.isoformat()
    return progress


def _plan_item_from_execution_step(step):
    return {
        "index": int(step.index),
        "model": step.model_string,
        "label": step.label,
        "estimated_changes": int(step.estimated_changes or 0),
        "sync_mode": step.sync_mode,
        "shard_keys": list(step.shard_keys or ()),
        "query_name": step.query_name,
        "execution_mode": step.execution_mode,
        "execution_value": step.execution_value,
        "baseline_snapshot_id": step.baseline_snapshot_id,
        "operation": step.operation or "mixed",
        "apply_engine": step.apply_engine,
        "fetch_mode": step.fetch_mode,
        "fetch_key_family": step.fetch_key_family,
        "fetch_parameters": step.fetch_parameters or {},
        "fetch_column_filters": step.fetch_column_filters or [],
        "status": step.status,
        "ingestion_id": step.ingestion_id,
        "branch_name": step.branch_name or getattr(step.branch, "name", ""),
        "stage_job_id": step.job_id,
        "merge_job_id": step.merge_job_id,
        "retry_count": int(step.retry_count or 0),
        "attempted_row_count": int(step.attempted_row_count or 0),
        "applied_row_count": int(step.applied_row_count or 0),
        "skipped_row_count": int(step.skipped_row_count or 0),
        "failed_row_count": int(step.failed_row_count or 0),
        "last_error": step.last_error or "",
        "updated_at": step.updated.isoformat() if step.updated else "",
    }


def _execution_step_kwargs_from_plan_item(item):
    # Kept for compatibility; primary implementation now lives in run_store.
    return _execution_step_kwargs_from_plan_item_impl(item)


def execution_step_for_ingestion(ingestion):
    return _execution_step_for_ingestion_impl(ingestion)


def ingestion_has_mergeable_execution_step(ingestion):
    return _ingestion_has_mergeable_execution_step_impl(
        ingestion,
        mergeable_step_statuses=MERGEABLE_STEP_STATUSES,
    )


def ingestion_has_requeueable_merge_timeout_step(ingestion):
    return _ingestion_has_requeueable_merge_timeout_step_impl(ingestion)


def claim_ingestion_merge_step(ingestion, job):
    return _claim_ingestion_merge_step_impl(ingestion, job)


def mark_ingestion_step_merged(ingestion, *, baseline_ready=False, merge_job=None):
    return _mark_ingestion_step_merged_impl(
        ingestion,
        baseline_ready=baseline_ready,
        merge_job=merge_job,
    )


def _step_is_final_success(step):
    run = step.run
    if run.total_steps:
        return int(step.index) >= int(run.total_steps)
    return not run.steps.exclude(
        status__in=[
            ForwardExecutionStepStatusChoices.MERGED,
            ForwardExecutionStepStatusChoices.SKIPPED,
            ForwardExecutionStepStatusChoices.CANCELLED,
        ]
    ).exists()


def reconcile_execution_run(run):
    return _reconcile_execution_run_impl(
        run,
        update_run_from_branch_state_fn=update_run_from_branch_state,
    )


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


def current_retryable_step(run):
    return _current_retryable_step_impl(run)


def current_discardable_step(run):
    return _current_discardable_step_impl(run)


def current_mergeable_step(run):
    return _current_mergeable_step_impl(run)


def discard_stage_branch_for_retry(step):
    return _discard_stage_branch_for_retry_impl(
        step,
        prepare_stage_step_retry_fn=prepare_stage_step_retry,
    )


def prepare_stage_step_retry(step):
    return _prepare_stage_step_retry_impl(step)


def update_run_from_branch_state(sync):
    return _update_run_from_branch_state_impl(sync)


def _active_or_latest_execution_run(sync):
    run = active_execution_run(sync)
    if run is None:
        run = latest_execution_run(sync)
    return run


def update_step_from_plan_item(sync, index, **updates):
    return _update_step_from_plan_item_impl(
        sync,
        index,
        terminal_stage_statuses=TERMINAL_STAGE_STATUSES,
        active_execution_run_fn=_active_or_latest_execution_run,
        **updates,
    )


def touch_execution_step_progress(
    sync,
    *,
    model_string,
    shard_index=None,
    row_count=None,
    row_total=None,
):
    return _touch_execution_step_progress_impl(
        sync,
        model_string=model_string,
        shard_index=shard_index,
        row_count=row_count,
        row_total=row_total,
        active_execution_run_fn=_active_or_latest_execution_run,
    )


def claim_stage_step(sync, index, job):
    return _claim_stage_step_impl(
        sync,
        index,
        job,
        claimable_stage_statuses=CLAIMABLE_STAGE_STATUSES,
        active_execution_run_fn=_active_or_latest_execution_run,
    )


def mark_run_completed(sync, *, baseline_ready=False):
    return _mark_run_completed_impl(
        sync,
        baseline_ready=baseline_ready,
        active_execution_run_fn=active_execution_run,
        latest_execution_run_fn=latest_execution_run,
    )


def _run_status_from_sync(sync):
    status = getattr(sync, "status", "")
    if status == "completed":
        return ForwardExecutionRunStatusChoices.COMPLETED
    if status == "failed":
        return ForwardExecutionRunStatusChoices.FAILED
    if status == "timeout":
        return ForwardExecutionRunStatusChoices.TIMEOUT
    if status == "ready_to_merge":
        return ForwardExecutionRunStatusChoices.WAITING
    if status in {"queued", "syncing", "merging"}:
        return ForwardExecutionRunStatusChoices.RUNNING
    return ForwardExecutionRunStatusChoices.QUEUED


def _run_status_from_branch_state(sync, state):
    phase = str((state or {}).get("phase") or "")
    if phase == "completed":
        return ForwardExecutionRunStatusChoices.COMPLETED
    if phase == "failed":
        return ForwardExecutionRunStatusChoices.FAILED
    if phase == "timeout":
        return ForwardExecutionRunStatusChoices.TIMEOUT
    if (state or {}).get("awaiting_merge"):
        return ForwardExecutionRunStatusChoices.WAITING
    return _run_status_from_sync(sync)


def _step_updates(updates):
    mapped = {}
    status = updates.get("status")
    if status:
        mapped["status"] = _normalize_step_status(status)
    if "ingestion_id" in updates:
        mapped["ingestion_id"] = updates.get("ingestion_id")
    if "branch_name" in updates:
        mapped["branch_name"] = updates.get("branch_name") or ""
    if "stage_job_id" in updates:
        mapped["job_id"] = updates.get("stage_job_id")
    if "merge_job_id" in updates:
        mapped["merge_job_id"] = updates.get("merge_job_id")
    if "retry_count" in updates:
        mapped["retry_count"] = int(updates.get("retry_count") or 0)
    if "last_error" in updates:
        mapped["last_error"] = updates.get("last_error") or ""
    if "actual_changes" in updates:
        mapped["actual_changes"] = max(0, int(updates.get("actual_changes") or 0))
    for source, target in (
        ("attempted_row_count", "attempted_row_count"),
        ("applied_row_count", "applied_row_count"),
        ("skipped_row_count", "skipped_row_count"),
        ("failed_row_count", "failed_row_count"),
    ):
        if source in updates:
            mapped[target] = max(0, int(updates.get(source) or 0))
    if "apply_engine" in updates:
        mapped["apply_engine"] = (
            updates.get("apply_engine") or ForwardApplyEngineChoices.ADAPTER
        )
    return mapped


def _normalize_step_status(status):
    status = str(status)
    return {
        "staging": ForwardExecutionStepStatusChoices.RUNNING,
        "queued_merge": ForwardExecutionStepStatusChoices.MERGE_QUEUED,
    }.get(status, status)


def _job_summary(job):
    return _job_summary_metric(job)


def _execution_run_metrics(run, steps):
    return _execution_run_metrics_calc(run, steps)


def _runtime_bottleneck(step_metrics, query_runtime_ms):
    return _runtime_bottleneck_calc(step_metrics, query_runtime_ms)


def _duration_seconds(started, completed):
    return _duration_seconds_calc(started, completed)


def _sum_optional_float(values):
    return _sum_optional_float_calc(values)


def _fetch_explanation(step):
    return _fetch_explanation_metric(step)


def _apply_engine_decision(step):
    return _apply_engine_decision_metric(step)
