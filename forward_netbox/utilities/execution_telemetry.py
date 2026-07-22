from .branch_budget import branch_budget_density_policy_summary
from .branch_budget import build_branch_budget_hints
from .branch_budget import DEFAULT_MODEL_CHANGE_DENSITY
from .branch_budget import delete_dependency_plan_summary
from .density_learning import density_profile_summary


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


def _build_query_mode_summary(model_results):
    execution_mode_counts = {}
    fetch_mode_counts = {}
    result_items = []

    for result in model_results or []:
        execution_mode = str(result.get("execution_mode") or "").strip() or "unknown"
        fetch_mode = str(result.get("fetch_mode") or "").strip() or "unknown"
        execution_mode_counts[execution_mode] = (
            execution_mode_counts.get(execution_mode, 0) + 1
        )
        fetch_mode_counts[fetch_mode] = fetch_mode_counts.get(fetch_mode, 0) + 1

        result_items.append(
            {
                "model": result.get("model") or "",
                "query_name": result.get("query_name") or "",
                "execution_mode": execution_mode,
                "fetch_mode": fetch_mode,
                "row_count": _coerce_int(result.get("row_count")),
                "delete_count": _coerce_int(result.get("delete_count")),
                "query_path_resolution": result.get("query_path_resolution") or {},
            }
        )

    result_items = sorted(
        result_items,
        key=lambda item: (
            -(item["row_count"] + item["delete_count"]),
            str(item["model"]),
            str(item["query_name"]),
        ),
    )[:10]
    return {
        "available": bool(result_items),
        "execution_modes": dict(sorted(execution_mode_counts.items())),
        "fetch_modes": dict(sorted(fetch_mode_counts.items())),
        "top_model_results": result_items,
    }


def build_plan_preview(plan, *, max_changes_per_staging_item):
    if not plan:
        return {
            "planned_shards": 0,
            "estimated_changes": 0,
            "model_count": 0,
            "delete_dependency_plan": {
                "status": "none",
                "delete_rows": 0,
                "delete_shards": 0,
                "delete_model_count": 0,
                "delete_share": 0.0,
                "max_delete_shard_changes": 0,
                "execution_order": [],
                "models": {},
                "warnings": [],
            },
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
                "budget": max_changes_per_staging_item,
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
    if any(item.estimated_changes >= max_changes_per_staging_item for item in plan):
        retry_risk = "high"
    elif max_shard_changes >= int(max_changes_per_staging_item * 0.8):
        retry_risk = "medium"
    elif len(plan) > 1:
        retry_risk = "medium"

    return {
        "planned_shards": len(plan),
        "estimated_changes": sum(item.estimated_changes for item in plan),
        "model_count": len(model_totals),
        "delete_dependency_plan": delete_dependency_plan_summary(
            plan,
            max_changes_per_staging_item=max_changes_per_staging_item,
        ),
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
    job_results=None,
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
    unchanged_row_count = 0
    slowest_model = {}
    shard_count = 0
    query_path_resolution = _build_query_path_resolution_summary(model_results)
    query_modes = _build_query_mode_summary(model_results)

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

    job_statistics = (
        (job_results or {}).get("statistics") if isinstance(job_results, dict) else {}
    )
    if isinstance(job_statistics, dict):
        for stats in job_statistics.values():
            if not isinstance(stats, dict):
                continue
            unchanged_row_count += _coerce_int(stats.get("unchanged"))

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
        "unchanged_row_count": unchanged_row_count,
        "query_path_resolution": query_path_resolution,
        "query_modes": query_modes,
    }


def _build_query_path_resolution_summary(model_results):
    total_specs = 0
    resolved_specs = 0
    model_items = []

    for result in model_results or []:
        resolution = result.get("query_path_resolution") or {}
        if not isinstance(resolution, dict):
            continue
        model = str(result.get("model") or "").strip() or "unknown"
        query_path_spec_count = _coerce_int(resolution.get("query_path_spec_count"))
        resolved_spec_count = _coerce_int(resolution.get("resolved_spec_count"))
        if not query_path_spec_count and not resolved_spec_count:
            continue
        total_specs += query_path_spec_count
        resolved_specs += resolved_spec_count
        model_items.append(
            {
                "model": model,
                "query_path_spec_count": query_path_spec_count,
                "resolved_spec_count": resolved_spec_count,
            }
        )

    model_items = sorted(
        model_items,
        key=lambda item: (
            -int(item["query_path_spec_count"]),
            -int(item["resolved_spec_count"]),
            str(item["model"]),
        ),
    )[:10]
    return {
        "available": bool(total_specs),
        "total_query_path_specs": total_specs,
        "resolved_spec_count": resolved_specs,
        "top_models": model_items,
    }


def build_sync_execution_summary(
    *,
    enabled_models,
    max_changes_per_staging_item,
    model_change_density,
    model_change_density_profile,
    latest_ingestion_summary,
):
    summary = {
        "max_changes_per_staging_item": max_changes_per_staging_item,
        "model_change_density": dict(model_change_density or {}),
        "model_change_density_profile": density_profile_summary(
            density_map=model_change_density,
            density_profile=model_change_density_profile,
            default_density_map=DEFAULT_MODEL_CHANGE_DENSITY,
        ),
        "enabled_models": list(enabled_models or []),
    }
    summary["branch_budget_hints"] = build_branch_budget_hints(
        summary["enabled_models"],
        max_changes_per_staging_item=max_changes_per_staging_item,
        model_change_density=model_change_density,
        model_change_density_profile=model_change_density_profile,
    )
    summary["branch_budget_density_policy"] = branch_budget_density_policy_summary(
        summary["enabled_models"],
        model_change_density=model_change_density,
        model_change_density_profile=model_change_density_profile,
    )
    if latest_ingestion_summary:
        summary["latest_ingestion"] = latest_ingestion_summary
    return summary
