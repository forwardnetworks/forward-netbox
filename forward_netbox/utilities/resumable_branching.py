from core.models import Job
from django.utils import timezone
from django.utils.module_loading import import_string

from ..choices import ForwardExecutionRunStatusChoices
from ..choices import ForwardSyncStatusChoices
from .branch_budget import shard_fetch_contract
from .execution_ledger import active_execution_run
from .execution_ledger import branch_run_state_from_execution_run
from .execution_ledger import latest_execution_run
from .execution_ledger import update_step_from_plan_item


RESUMABLE_BRANCHING_PARAMETER = "resumable_branching"


def resumable_branching_enabled(sync):
    parameters = sync.parameters or {}
    if RESUMABLE_BRANCHING_PARAMETER in parameters:
        return bool(parameters.get(RESUMABLE_BRANCHING_PARAMETER))
    return True


def plan_item_snapshot(item, *, status="pending", existing=None):
    existing = dict(existing or {})
    fetch_contract = shard_fetch_contract(item.model_string, item.shard_keys)
    snapshot = {
        "index": int(item.index),
        "model": item.model_string,
        "label": item.label,
        "estimated_changes": int(item.estimated_changes),
        "sync_mode": item.sync_mode,
        "shard_keys": list(item.shard_keys or ()),
        "query_name": item.query_name,
        "execution_mode": item.execution_mode,
        "execution_value": item.execution_value,
        "baseline_snapshot_id": item.baseline_snapshot_id,
        "apply_engine": item.apply_engine,
        "apply_engine_reason": item.apply_engine_reason,
        "apply_engine_decision": item.apply_engine_decision,
        **fetch_contract,
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
    state = sync.get_branch_run_state()
    items = state.get("plan_items") or []
    if isinstance(items, list) and items:
        return items
    run = active_execution_run(sync)
    if run is None:
        return []
    state = branch_run_state_from_execution_run(run)
    items = state.get("plan_items") or []
    return items if isinstance(items, list) else []


def update_plan_item_state(sync, index, **updates):
    run = active_execution_run(sync)
    if run is not None:
        return update_step_from_plan_item(sync, index, **updates)
    state = sync.get_branch_run_state()
    items = get_plan_items(sync)
    now = timezone.now().isoformat()
    updated = False
    for item in items:
        if int(item.get("index") or 0) != int(index):
            continue
        item.update(updates)
        item["updated_at"] = now
        updated = True
        break
    if updated:
        state["plan_items"] = items
        sync.set_branch_run_state(state)
        update_step_from_plan_item(sync, index, **updates)
        return True
    return False


def enqueue_branch_stage_job(sync, *, user=None, adhoc=True):
    run = active_execution_run(sync)
    if run is None:
        run = latest_execution_run(sync)
    state = (
        branch_run_state_from_execution_run(run)
        if run is not None
        else sync.get_branch_run_state()
    )
    next_plan_index = int(state.get("next_plan_index") or 1)
    total_plan_items = int(state.get("total_plan_items") or 0)
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
                ],
            )
            .order_by("index")
            .first()
        )
        if existing_inflight is not None:
            # Avoid duplicate queue jobs for same index.
            return getattr(existing_inflight, "job", None)
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
    )
    state["phase"] = "queued"
    state["phase_message"] = (
        f"Queued shard {next_plan_index}/{total_plan_items} for Branching execution."
        if total_plan_items
        else "Queued next Branching shard."
    )
    state["last_stage_job_id"] = job.pk
    if run is None:
        update_plan_item_state(
            sync,
            next_plan_index,
            status="queued",
            stage_job_id=job.pk,
        )
    else:
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
