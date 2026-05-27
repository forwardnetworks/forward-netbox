from core.models import Job
from django.utils import timezone
from django.utils.module_loading import import_string

from ..choices import ForwardExecutionRunStatusChoices
from ..choices import ForwardSyncStatusChoices
from .branch_budget import shard_fetch_contract
from .execution_ledger import active_execution_run
from .execution_ledger import branch_run_state_from_execution_run
from .execution_ledger import latest_execution_run
from .execution_ledger import reconcile_execution_run
from .execution_ledger import update_step_from_plan_item
from .execution_ledger import upgrade_branch_run_state_to_execution_run
from .sync_state import get_branch_run_display_state
from .sync_state import prune_stale_branch_run_state


RESUMABLE_BRANCHING_PARAMETER = "resumable_branching"
SCHEDULER_OVERLAP_PARAMETER = "scheduler_overlap"
RESUMABLE_LEDGER_FALLBACK_STATUSES = {
    ForwardExecutionRunStatusChoices.FAILED,
    ForwardExecutionRunStatusChoices.TIMEOUT,
}


def resumable_branching_enabled(sync):
    parameters = sync.parameters or {}
    if RESUMABLE_BRANCHING_PARAMETER in parameters:
        return bool(parameters.get(RESUMABLE_BRANCHING_PARAMETER))
    return True


def scheduler_overlap_enabled(sync):
    parameters = sync.parameters or {}
    auto_merge = bool(parameters.get("auto_merge", sync.auto_merge))
    if not auto_merge:
        return False
    if SCHEDULER_OVERLAP_PARAMETER in parameters:
        return bool(parameters.get(SCHEDULER_OVERLAP_PARAMETER))
    backend = str(parameters.get("execution_backend") or "").strip().lower()
    return backend == "branching"


def plan_item_snapshot(item, *, status="pending", existing=None):
    existing = dict(existing or {})
    fetch_contract = shard_fetch_contract(item.model_string, item.shard_keys)
    fetch_mode = item.fetch_mode or fetch_contract.get("fetch_mode") or "model"
    fetch_key_family = (
        item.fetch_key_family or fetch_contract.get("fetch_key_family") or ""
    )
    fetch_parameters = (
        item.fetch_parameters or fetch_contract.get("fetch_parameters") or {}
    )
    query_parameters = (
        item.query_parameters or fetch_contract.get("query_parameters") or {}
    )
    fetch_column_filters = (
        item.fetch_column_filters or fetch_contract.get("fetch_column_filters") or []
    )
    snapshot = {
        "index": int(item.index),
        "model": item.model_string,
        "label": item.label,
        "estimated_changes": int(item.estimated_changes),
        "sync_mode": item.sync_mode,
        "operation": item.operation,
        "shard_keys": list(item.shard_keys or ()),
        "query_name": item.query_name,
        "execution_mode": item.execution_mode,
        "execution_value": item.execution_value,
        "baseline_snapshot_id": item.baseline_snapshot_id,
        "apply_engine": item.apply_engine,
        "apply_engine_reason": item.apply_engine_reason,
        "apply_engine_decision": item.apply_engine_decision,
        "fetch_mode": fetch_mode,
        "fetch_key_family": fetch_key_family,
        "fetch_parameters": fetch_parameters,
        "query_parameters": query_parameters,
        "fetch_column_filters": fetch_column_filters,
        "status": existing.get("status") or status,
        "ingestion_id": existing.get("ingestion_id"),
        "branch_name": existing.get("branch_name") or "",
        "stage_job_id": existing.get("stage_job_id"),
        "merge_job_id": existing.get("merge_job_id"),
        "retry_count": int(existing.get("retry_count") or 0),
        "attempted_row_count": int(existing.get("attempted_row_count") or 0),
        "applied_row_count": int(existing.get("applied_row_count") or 0),
        "skipped_row_count": int(existing.get("skipped_row_count") or 0),
        "failed_row_count": int(existing.get("failed_row_count") or 0),
        "last_error": existing.get("last_error") or "",
        "updated_at": existing.get("updated_at") or "",
    }
    return snapshot


def plan_items_snapshot(plan, *, existing_items=None):
    existing_by_index = {
        int(item.get("index")): item
        for item in existing_items or []
        if isinstance(item, dict) and item.get("index") is not None
    }
    return [
        plan_item_snapshot(item, existing=existing_by_index.get(int(item.index)))
        for item in plan
    ]


def get_plan_items(sync):
    run = active_execution_run(sync)
    if run is not None:
        state = branch_run_state_from_execution_run(run)
        items = state.get("plan_items") or []
        return items if isinstance(items, list) else []
    state = get_branch_run_display_state(sync)
    items = state.get("plan_items") or []
    return items if isinstance(items, list) else []


def update_plan_item_state(sync, index, **updates):
    run = active_execution_run(sync)
    if run is not None:
        return update_step_from_plan_item(sync, index, **updates)
    # Compatibility cache is read-through only after ledger migration.
    return False


def enqueue_branch_stage_job(sync, *, user=None, adhoc=True, overlap_stage=False):
    run = active_execution_run(sync)
    if run is None:
        latest_run = latest_execution_run(sync)
        if (
            latest_run is not None
            and latest_run.status in RESUMABLE_LEDGER_FALLBACK_STATUSES
        ):
            run = latest_run
    if run is None and getattr(sync, "pk", None):
        # Do not enqueue from stale compatibility state once ledger history
        # exists and no resumable run is active.
        if sync.execution_runs.exists():
            prune_stale_branch_run_state(sync)
            return None
        run = upgrade_branch_run_state_to_execution_run(sync)
    state = branch_run_state_from_execution_run(run) if run is not None else {}
    if not state:
        return None
    if run is not None:
        reconcile_execution_run(run)
        run.refresh_from_db()
        state = branch_run_state_from_execution_run(run)
        if not state:
            return None
    if overlap_stage and run is None:
        return None
    next_plan_index = (
        _next_overlap_stage_index(run)
        if overlap_stage
        else int(state.get("next_plan_index") or 1)
    )
    if next_plan_index is None:
        return None
    total_plan_items = int(state.get("total_plan_items") or 0)
    if total_plan_items and next_plan_index > total_plan_items:
        return None
    if run is not None:
        from ..choices import ForwardExecutionStepStatusChoices

        running_step = (
            run.steps.filter(
                kind="stage",
                status=ForwardExecutionStepStatusChoices.RUNNING,
            )
            .order_by("index")
            .first()
        )
        if running_step is not None:
            # A shard is already in flight; don't enqueue another one.
            return None
        existing_inflight = (
            run.steps.filter(
                kind="stage",
                index=next_plan_index,
                status__in=[
                    ForwardExecutionStepStatusChoices.QUEUED,
                    ForwardExecutionStepStatusChoices.RUNNING,
                    ForwardExecutionStepStatusChoices.STAGED,
                    ForwardExecutionStepStatusChoices.MERGE_QUEUED,
                ],
            )
            .order_by("index")
            .first()
        )
        if existing_inflight is not None:
            # Avoid duplicate queue jobs for same index.
            return getattr(existing_inflight, "job", None) or getattr(
                existing_inflight, "merge_job", None
            )
    sync.status = ForwardSyncStatusChoices.QUEUED
    sync.__class__.objects.filter(pk=sync.pk).update(status=sync.status)
    job = Job.enqueue(
        import_string("forward_netbox.jobs.stage_forward_branch_item"),
        instance=sync,
        user=user or sync.user,
        name=(
            f"{sync.name} - shard {next_plan_index}/{total_plan_items}"
            if total_plan_items
            else f"{sync.name} - shard"
        ),
        adhoc=adhoc,
        overlap_stage=bool(overlap_stage),
    )
    state["phase"] = "queued"
    state["phase_message"] = (
        f"Queued shard {next_plan_index}/{total_plan_items} for Branching execution."
        if total_plan_items
        else "Queued next Branching shard."
    )
    state["last_stage_job_id"] = job.pk
    update_step_from_plan_item(
        sync,
        next_plan_index,
        status="queued",
        stage_job_id=job.pk,
    )
    run.status = ForwardExecutionRunStatusChoices.RUNNING
    run.phase = "queued"
    run.phase_message = state["phase_message"]
    run.next_step_index = next_plan_index
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
    return job


def _next_overlap_stage_index(run):
    from ..choices import ForwardExecutionStepStatusChoices

    current_merge_step = (
        run.steps.filter(
            kind="stage",
            status=ForwardExecutionStepStatusChoices.MERGE_QUEUED,
        )
        .order_by("index")
        .first()
    )
    if current_merge_step is None:
        return None
    existing_ahead = (
        run.steps.filter(
            kind="stage",
            index__gt=current_merge_step.index,
            status__in=[
                ForwardExecutionStepStatusChoices.QUEUED,
                ForwardExecutionStepStatusChoices.RUNNING,
                ForwardExecutionStepStatusChoices.STAGED,
                ForwardExecutionStepStatusChoices.MERGE_QUEUED,
            ],
        )
        .order_by("index")
        .first()
    )
    if existing_ahead is not None:
        return None
    pending_step = (
        run.steps.filter(
            kind="stage",
            index__gt=current_merge_step.index,
            status=ForwardExecutionStepStatusChoices.PENDING,
        )
        .order_by("index")
        .first()
    )
    return int(pending_step.index) if pending_step is not None else None
