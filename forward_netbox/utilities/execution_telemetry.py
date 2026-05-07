from .branch_budget import build_branch_budget_hints


def _coerce_int(value):
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _coerce_float(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def build_plan_preview(plan, *, max_changes_per_branch):
    if not plan:
        return {
            "planned_shards": 0,
            "estimated_changes": 0,
            "model_count": 0,
            "retry_risk": "low",
            "slowest_model": {},
            "models": {},
        }

    model_totals = {}
    for item in plan:
        model_entry = model_totals.setdefault(
            item.model_string,
            {
                "estimated_changes": 0,
                "shard_count": 0,
                "max_shard_changes": 0,
                "budget": max_changes_per_branch,
            },
        )
        model_entry["estimated_changes"] += item.estimated_changes
        model_entry["shard_count"] += 1
        model_entry["max_shard_changes"] = max(
            model_entry["max_shard_changes"], item.estimated_changes
        )

    slowest_model = max(
        plan,
        key=lambda item: (item.estimated_changes, item.query_runtime_ms or 0.0),
    )
    max_shard_changes = max(item.estimated_changes for item in plan)
    retry_risk = "low"
    if any(item.estimated_changes >= max_changes_per_branch for item in plan):
        retry_risk = "high"
    elif max_shard_changes >= int(max_changes_per_branch * 0.8):
        retry_risk = "medium"
    elif len(plan) > 1:
        retry_risk = "medium"

    return {
        "planned_shards": len(plan),
        "estimated_changes": sum(item.estimated_changes for item in plan),
        "model_count": len(model_totals),
        "retry_risk": retry_risk,
        "slowest_model": {
            "model": slowest_model.model_string,
            "query_name": slowest_model.query_name,
            "estimated_changes": slowest_model.estimated_changes,
            "query_runtime_ms": slowest_model.query_runtime_ms,
        },
        "models": model_totals,
    }


def build_ingestion_execution_summary(
    *,
    model_results,
    job_logs,
    applied_change_count,
    failed_change_count,
    created_change_count,
    updated_change_count,
    deleted_change_count,
):
    total_rows = 0
    total_deletes = 0
    total_estimated = 0
    total_runtime_ms = 0.0
    retry_count = 0
    slowest_model = {}
    shard_count = 0

    for result in model_results:
        row_count = _coerce_int(result.get("row_count"))
        delete_count = _coerce_int(result.get("delete_count"))
        estimated_changes = _coerce_int(result.get("estimated_changes"))
        runtime_ms = _coerce_float(result.get("runtime_ms"))
        branch_plan_total = _coerce_int(result.get("branch_plan_total"))

        total_rows += row_count
        total_deletes += delete_count
        total_runtime_ms += runtime_ms
        total_estimated += estimated_changes or (row_count + delete_count)
        shard_count = max(shard_count, branch_plan_total)
        if runtime_ms and runtime_ms >= _coerce_float(slowest_model.get("runtime_ms")):
            slowest_model = {
                "model": result.get("model") or "",
                "query_name": result.get("query_name") or "",
                "runtime_ms": runtime_ms,
            }

    for entry in job_logs or []:
        if isinstance(entry, (list, tuple)) and len(entry) >= 5:
            message = str(entry[4] or "")
            if message.startswith("Branch budget retry:"):
                retry_count += 1

    return {
        "model_count": len(model_results),
        "shard_count": shard_count or len(model_results),
        "retry_count": retry_count,
        "estimated_changes": total_estimated,
        "row_count": total_rows,
        "delete_count": total_deletes,
        "runtime_ms": round(total_runtime_ms, 1),
        "slowest_model": slowest_model,
        "applied_change_count": applied_change_count,
        "failed_change_count": failed_change_count,
        "created_change_count": created_change_count,
        "updated_change_count": updated_change_count,
        "deleted_change_count": deleted_change_count,
        "model_results": list(model_results),
    }


def build_branch_run_summary(branch_run_state):
    state = dict(branch_run_state or {})
    summary = {
        "snapshot_id": state.get("snapshot_id") or "",
        "next_plan_index": state.get("next_plan_index"),
        "total_plan_items": state.get("total_plan_items"),
        "awaiting_merge": bool(state.get("awaiting_merge")),
        "validation_run_id": state.get("validation_run_id"),
        "phase": state.get("phase") or "",
        "phase_message": state.get("phase_message") or "",
        "phase_started": state.get("phase_started") or "",
        "last_progress_message": state.get("last_progress_message") or "",
        "last_progress_at": state.get("last_progress_at") or "",
        "current_model_string": state.get("current_model_string") or "",
        "current_shard_index": state.get("current_shard_index"),
        "current_row_count": state.get("current_row_count"),
        "current_row_total": state.get("current_row_total"),
        "plan_preview": state.get("plan_preview") or {},
        "model_change_density": state.get("model_change_density") or {},
    }
    return summary


def build_sync_execution_summary(
    *,
    enabled_models,
    max_changes_per_branch,
    model_change_density,
    branch_run_state,
    latest_ingestion_summary,
):
    summary = {
        "max_changes_per_branch": max_changes_per_branch,
        "model_change_density": dict(model_change_density or {}),
        "enabled_models": list(enabled_models or []),
    }
    summary["branch_budget_hints"] = build_branch_budget_hints(
        summary["enabled_models"],
        max_changes_per_branch=max_changes_per_branch,
        model_change_density=model_change_density,
    )
    if branch_run_state:
        summary["branch_run"] = build_branch_run_summary(branch_run_state)
        if branch_run_state.get("plan_preview"):
            summary["pre_run_estimate"] = branch_run_state["plan_preview"]
    if latest_ingestion_summary:
        summary["latest_ingestion"] = latest_ingestion_summary
    return summary
