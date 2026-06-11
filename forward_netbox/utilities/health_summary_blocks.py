from collections import Counter
from datetime import timedelta
from math import ceil
from types import SimpleNamespace

from django.conf import settings
from django.utils import timezone

from .. import NetboxForwardConfig
from ..choices import forward_configured_models
from ..choices import ForwardDiffFallbackModeChoices
from ..choices import ForwardExecutionBackendChoices
from ..choices import ForwardExecutionStepKindChoices
from ..choices import ForwardExecutionStepStatusChoices
from .branch_budget import BRANCH_RUN_STATE_PARAMETER
from .branch_budget import DEFAULT_MODEL_CHANGE_DENSITY
from .density_learning import density_profile_summary
from .execution_ledger import active_execution_run
from .execution_ledger import execution_run_recovery_recommendation
from .execution_ledger import latest_execution_run
from .execution_ledger_metrics import diff_baseline_transition_summary
from .execution_ledger_metrics import diff_utilization_summary
from .execution_ledger_metrics import fallback_reason_summary
from .execution_ledger_metrics import partition_retry_summary
from .execution_ledger_metrics import pushdown_efficiency_summary
from .execution_ledger_metrics import pushdown_runtime_summary
from .execution_ledger_metrics import pushdown_tuning_guidance
from .execution_ledger_metrics import recent_pushdown_trend_snapshots
from .execution_ledger_metrics import scheduler_overlap_capacity_evidence
from .execution_ledger_metrics import throughput_smoothing_summary
from .execution_ledger_serialization import (
    dependency_lookup_cache_support_summary as _dependency_lookup_cache_support_summary,
)
from .execution_ledger_serialization import (
    dependency_parent_coverage_support_summary as _dependency_parent_coverage_support_summary,
)
from .execution_telemetry import _build_query_mode_summary
from .forward_api import DEFAULT_NQE_PAGE_SIZE
from .forward_api import MAX_NQE_PAGE_SIZE
from .forward_api import MAX_QUERY_FETCH_CONCURRENCY
from .model_contracts import architecture_contract_for_model
from .runtime_guidance import configured_rq_default_timeout
from .runtime_guidance import source_pushdown_alert_thresholds
from .runtime_guidance import source_query_fetch_concurrency
from .runtime_guidance import source_timeout_seconds
from .sync_primitives import DEPENDENCY_PARENT_DEVICE_MODELS


def _safe_int(value):
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


DEPENDENCY_PREFLIGHT_RULES = (
    {
        "code": "interface_routing_dependency_omitted",
        "selected_model": "dcim.interface",
        "omitted_models": (
            "netbox_routing.bgppeer",
            "netbox_routing.bgppeeraddressfamily",
            "netbox_peering_manager.peeringsession",
        ),
        "message": (
            "dcim.interface delete/prune rows can be blocked by protected "
            "routing or peering references when these models are omitted."
        ),
    },
    {
        "code": "ipaddress_routing_dependency_omitted",
        "selected_model": "ipam.ipaddress",
        "omitted_models": (
            "netbox_routing.bgppeer",
            "netbox_routing.bgppeeraddressfamily",
            "netbox_peering_manager.peeringsession",
        ),
        "message": (
            "ipam.ipaddress delete/prune rows can be blocked by protected "
            "routing or peering references when these models are omitted."
        ),
    },
    {
        "code": "device_child_dependency_omitted",
        "selected_model": "dcim.device",
        "omitted_models": (
            "dcim.interface",
            "dcim.cable",
            "dcim.module",
            "dcim.inventoryitem",
            "ipam.ipaddress",
            "ipam.prefix",
            "netbox_routing.bgppeer",
            "netbox_routing.bgppeeraddressfamily",
            "netbox_peering_manager.peeringsession",
        ),
        "message": (
            "dcim.device delete/prune rows can be blocked by child, IPAM, "
            "routing, or peering references when these models are omitted."
        ),
    },
)

PARENT_DEVICE_DEPENDENT_MODELS = DEPENDENCY_PARENT_DEVICE_MODELS

DELETE_TERMINAL_STEP_STATUSES = {
    ForwardExecutionStepStatusChoices.STAGED,
    ForwardExecutionStepStatusChoices.MERGED,
    ForwardExecutionStepStatusChoices.SKIPPED,
    ForwardExecutionStepStatusChoices.CANCELLED,
    ForwardExecutionStepStatusChoices.FAILED,
    ForwardExecutionStepStatusChoices.TIMEOUT,
    ForwardExecutionStepStatusChoices.MERGE_TIMEOUT,
}
DELETE_ACTIVE_STEP_STATUSES = {
    ForwardExecutionStepStatusChoices.QUEUED,
    ForwardExecutionStepStatusChoices.RUNNING,
    ForwardExecutionStepStatusChoices.MERGE_QUEUED,
}
ADAPTIVE_CAPACITY_TARGET_SHARDS_PER_HOUR = 5.0
ADAPTIVE_CAPACITY_MAX_ISSUES_PER_HOUR = 2.0
ADAPTIVE_CAPACITY_HOLD_MINUTES = 60


def source_summary(sync):
    source = sync.source
    return {
        "id": source.pk,
        "name": source.name,
        "url": source.url,
        "status": source.status,
        "type": source.type,
        "last_synced": source.last_synced.isoformat() if source.last_synced else None,
    }


def runtime_summary(sync):
    branch_plugin_available = True
    try:
        import netbox_branching  # noqa: F401
    except Exception:
        branch_plugin_available = False
    parameters = sync.parameters or {}

    return {
        "plugin_version": NetboxForwardConfig.version,
        "netbox_version": getattr(settings, "VERSION", ""),
        "branching_available": branch_plugin_available,
        "execution_backend": parameters.get(
            "execution_backend",
            ForwardExecutionBackendChoices.BRANCHING,
        ),
        "auto_merge": bool(sync.auto_merge),
        "enable_bulk_orm": bool(parameters.get("enable_bulk_orm", False)),
        "scheduler_overlap": bool(parameters.get("scheduler_overlap", False)),
        "diff_fallback_mode": parameters.get(
            "diff_fallback_mode",
            ForwardDiffFallbackModeChoices.ALLOW_FALLBACK,
        ),
        "max_changes_per_branch": sync.get_max_changes_per_branch(),
        "source_timeout_seconds": source_timeout_seconds(sync),
        "query_fetch_concurrency": source_query_fetch_concurrency(sync),
        "pushdown_alert_thresholds": source_pushdown_alert_thresholds(sync),
        "rq_default_timeout_seconds": configured_rq_default_timeout(),
        "snapshot_selector": sync.get_snapshot_id(),
    }


def query_map_summary(query_map):
    return {
        "id": query_map.pk,
        "name": query_map.name,
        "model": query_map.model_string,
        "mode": query_map.execution_mode,
        "query_repository": query_map.query_repository or "",
        "query_path": query_map.query_path or "",
        "has_query_id": bool(query_map.query_id),
        "has_commit_id": bool(query_map.commit_id),
        "built_in": bool(query_map.built_in),
    }


def validation_summary(validation_run):
    if validation_run is None:
        return None
    return {
        "id": validation_run.pk,
        "status": validation_run.status,
        "allowed": bool(validation_run.allowed),
        "snapshot_selector": validation_run.snapshot_selector,
        "snapshot_id": validation_run.snapshot_id,
        "blocking_reason_count": len(validation_run.blocking_reasons or []),
        "created": (
            validation_run.created.isoformat() if validation_run.created else None
        ),
        "completed": (
            validation_run.completed.isoformat() if validation_run.completed else None
        ),
    }


def ingestion_summary(ingestion):
    if ingestion is None:
        return None
    model_results = list(getattr(ingestion, "model_results", None) or [])
    execution_summary = ingestion.get_execution_summary()
    return {
        "id": ingestion.pk,
        "name": ingestion.name,
        "sync_mode": ingestion.sync_mode or "",
        "baseline_ready": bool(ingestion.baseline_ready),
        "snapshot_selector": ingestion.snapshot_selector or "",
        "snapshot_id": ingestion.snapshot_id or "",
        "branch": ingestion.branch.name if ingestion.branch else "",
        "issue_count": ingestion.issues.count(),
        "applied_change_count": ingestion.applied_change_count,
        "failed_change_count": ingestion.failed_change_count,
        "analysis_summary": ingestion.get_analysis_summary(),
        "execution_summary": execution_summary,
        "workload_preview": ingestion.get_workload_summary(),
        "dependency_lookup_cache": _dependency_lookup_cache_support_summary(
            SimpleNamespace(job=ingestion.job)
        ),
        "dependency_parent_coverage": _dependency_parent_coverage_support_summary(
            SimpleNamespace(job=ingestion.job)
        ),
        "query_path_resolution": query_path_resolution_summary(ingestion),
        "query_modes": _build_query_mode_summary(model_results),
        "created": ingestion.created.isoformat() if ingestion.created else None,
    }


def query_path_resolution_summary(ingestion):
    model_results = list(getattr(ingestion, "model_results", None) or [])
    total_query_path_specs = 0
    artifact_hit_count = 0
    client_resolve_count = 0
    repository_index_count = 0
    for result in model_results:
        resolution = result.get("query_path_resolution") or {}
        if not isinstance(resolution, dict):
            continue
        total_query_path_specs += _safe_int(resolution.get("query_path_spec_count"))
        artifact_hit_count += _safe_int(resolution.get("artifact_hit_count"))
        client_resolve_count += _safe_int(resolution.get("client_resolve_count"))
        repository_index_count += _safe_int(resolution.get("repository_index_count"))
    total_lookups = artifact_hit_count + client_resolve_count
    return {
        "available": bool(total_query_path_specs),
        "total_query_path_specs": total_query_path_specs,
        "artifact_hit_count": artifact_hit_count,
        "client_resolve_count": client_resolve_count,
        "repository_index_count": repository_index_count,
        "cache_hit_rate": (
            round(artifact_hit_count / float(total_lookups), 4)
            if total_lookups
            else None
        ),
    }


def execution_run_summary(run):
    if run is None:
        return None
    return {
        "id": run.pk,
        "backend": run.backend,
        "status": run.status,
        "phase": run.phase,
        "phase_message": run.phase_message,
        "total_steps": run.total_steps,
        "next_step_index": run.next_step_index,
        "baseline_ready": bool(run.baseline_ready),
        "recovery_recommendation": execution_run_recovery_recommendation(run),
        "latest_heartbeat": (
            run.latest_heartbeat.isoformat() if run.latest_heartbeat else None
        ),
        "last_error": run.last_error,
    }


def capacity_summary(run):
    if run is None:
        return {
            "available": False,
            "message": "No execution run is available for capacity projection.",
        }
    steps = list(run.steps.all())
    completed_steps = [
        step
        for step in steps
        if step.status
        in {
            ForwardExecutionStepStatusChoices.STAGED,
            ForwardExecutionStepStatusChoices.MERGED,
            ForwardExecutionStepStatusChoices.SKIPPED,
            ForwardExecutionStepStatusChoices.CANCELLED,
        }
    ]
    durations = [
        step_duration_seconds(step)
        for step in completed_steps
        if step_duration_seconds(step) is not None
    ]
    remaining_steps = max(0, int(run.total_steps or len(steps)) - len(completed_steps))
    average_seconds = round(sum(durations) / len(durations), 3) if durations else None
    max_seconds = round(max(durations), 3) if durations else None
    projected_remaining_seconds = (
        round(average_seconds * remaining_steps, 3)
        if average_seconds is not None
        else None
    )
    return {
        "available": bool(durations),
        "total_steps": int(run.total_steps or len(steps)),
        "completed_steps": len(completed_steps),
        "remaining_steps": remaining_steps,
        "average_completed_step_seconds": average_seconds,
        "max_completed_step_seconds": max_seconds,
        "projected_remaining_seconds": projected_remaining_seconds,
        "message": capacity_message(
            run,
            average_seconds=average_seconds,
            max_seconds=max_seconds,
            remaining_steps=remaining_steps,
        ),
    }


def step_duration_seconds(step):
    if not step.started or not step.completed:
        return None
    try:
        return max(0.0, (step.completed - step.started).total_seconds())
    except (TypeError, ValueError):
        return None


def capacity_message(run, *, average_seconds, max_seconds, remaining_steps):
    if average_seconds is None:
        return (
            "Capacity estimate is unavailable until at least one stage step completes."
        )
    if remaining_steps <= 0:
        return "All planned steps are complete."
    return (
        f"Average completed stage step is {average_seconds:.1f}s "
        f"(max {max_seconds:.1f}s); {remaining_steps} step(s) remain."
    )


def large_run_tuning_summary(sync, *, capacity, query_pushdown, throughput=None):
    capacity = capacity or {}
    query_pushdown = query_pushdown or {}
    throughput = throughput or {}
    runtime = runtime_summary(sync)
    efficiency = query_pushdown.get("efficiency") or {}
    runtime_share = query_pushdown.get("runtime_share") or {}
    diff_utilization = query_pushdown.get("diff_utilization") or {}
    guidance = list(query_pushdown.get("tuning_guidance") or [])
    scheduler_overlap_readiness = throughput.get("scheduler_overlap_readiness") or {}
    backend_advice = execution_backend_advice(
        runtime=runtime,
        capacity=capacity,
        efficiency=efficiency,
        diff_utilization=diff_utilization,
    )
    adaptive_capacity = adaptive_capacity_summary(
        sync,
        capacity=capacity,
        query_pushdown=query_pushdown,
        throughput=throughput,
    )

    actions = []
    if backend_advice.get("action_code"):
        actions.append(
            {
                "code": backend_advice["action_code"],
                "priority": backend_advice["priority"],
                "message": backend_advice["message"],
            }
        )
    if (diff_utilization.get("diff_actual_ratio") is not None) and (
        float(diff_utilization.get("diff_actual_ratio")) < 1.0
    ):
        actions.append(
            {
                "code": "restore_diff_utilization",
                "priority": 1,
                "message": "Restore diff execution before tuning workers or database capacity.",
            }
        )
    if int(efficiency.get("fallback_steps") or 0) > 0:
        actions.append(
            {
                "code": "reduce_fallback_fetch",
                "priority": 2,
                "message": "Reduce fallback-heavy model fetches before increasing branch fanout.",
            }
        )
    projected_remaining = capacity.get("projected_remaining_seconds")
    rq_timeout = runtime.get("rq_default_timeout_seconds")
    if rq_timeout and projected_remaining and projected_remaining >= rq_timeout:
        actions.append(
            {
                "code": "runtime_exceeds_worker_timeout",
                "priority": 3,
                "message": "Projected remaining shard runtime exceeds worker timeout; tune timeout/capacity before rerunning large branches.",
            }
        )

    large_branching_run = bool(
        runtime.get("execution_backend") == ForwardExecutionBackendChoices.BRANCHING
        and int(capacity.get("total_steps") or 0) >= 20
    )
    if large_branching_run and not runtime.get("enable_bulk_orm"):
        actions.append(
            {
                "code": "enable_safe_bulk_orm",
                "priority": 4,
                "message": (
                    "Enable safe bulk ORM models for parity-tested low-risk models; "
                    "adapter-required models remain unchanged."
                ),
            }
        )
    if (
        large_branching_run
        and runtime.get("auto_merge")
        and not runtime.get("scheduler_overlap")
    ):
        actions.append(
            {
                "code": "consider_scheduler_overlap",
                "priority": 5,
                "message": (
                    "If support evidence shows queue or merge wait dominates and "
                    "workers/database have headroom, enable Stage next shard during merge."
                ),
            }
        )

    concurrency = runtime.get("query_fetch_concurrency")
    if concurrency is not None and int(concurrency) <= 2:
        actions.append(
            {
                "code": "query_fetch_concurrency_low",
                "priority": 4,
                "message": "Query fetch concurrency is conservative; increase only after confirming Forward and database headroom.",
            }
        )
    elif concurrency is not None and int(concurrency) >= 12:
        actions.append(
            {
                "code": "query_fetch_concurrency_high",
                "priority": 4,
                "message": "Query fetch concurrency is high; lower it if fallback runtime or database contention rises.",
            }
        )

    actions.extend(
        {
            "code": str(item.get("code") or "query_pushdown_guidance"),
            "priority": 5,
            "message": str(item.get("message") or "").strip(),
        }
        for item in guidance
        if str(item.get("message") or "").strip()
    )
    actions = sorted(
        actions,
        key=lambda item: (int(item["priority"]), str(item["code"])),
    )
    warning_guidance = [
        item for item in guidance if str(item.get("severity") or "").lower() == "warn"
    ]
    if (
        warning_guidance
        or any(item["code"] == "runtime_exceeds_worker_timeout" for item in actions)
        or backend_advice.get("status") == "warn"
        or adaptive_capacity.get("status") == "warn"
    ):
        status = "warn"
    elif actions:
        status = "info"
    elif capacity.get("available") or query_pushdown.get("available"):
        status = "pass"
    else:
        status = "info"

    if actions:
        message = actions[0]["message"]
    elif adaptive_capacity.get("decision") in {
        "recommend_tuning_batch",
        "rollback_latest_tuning_batch",
    }:
        message = adaptive_capacity["message"]
    elif status == "pass":
        message = "No immediate large-run tuning action is indicated by current health signals."
    else:
        message = (
            "Large-run tuning summary is unavailable until execution evidence exists."
        )

    return {
        "status": status,
        "message": message,
        "query_fetch_concurrency": concurrency,
        "worker_timeout_seconds": rq_timeout,
        "source_timeout_seconds": runtime.get("source_timeout_seconds"),
        "execution_backend_advice": backend_advice,
        "scheduler_overlap_readiness": scheduler_overlap_readiness,
        "signals": {
            "execution_backend": runtime.get("execution_backend"),
            "fallback_rate": efficiency.get("fallback_rate"),
            "fallback_runtime_share": runtime_share.get("fallback_runtime_share"),
            "diff_actual_ratio": diff_utilization.get("diff_actual_ratio"),
            "projected_remaining_seconds": projected_remaining,
        },
        "adaptive_capacity": adaptive_capacity,
        "first_order_actions": actions[:6],
    }


def adaptive_capacity_summary(sync, *, capacity, query_pushdown, throughput):
    capacity = capacity or {}
    query_pushdown = query_pushdown or {}
    throughput = throughput or {}
    evidence = _adaptive_capacity_evidence(sync, throughput=throughput)
    batch = _adaptive_tuning_batch(sync, evidence=evidence)
    issue_rate = _optional_float(throughput.get("issue_rate_per_hour"))
    one_hour_rate = _optional_float(throughput.get("shards_per_hour_1h"))
    six_hour_rate = _optional_float(throughput.get("shards_per_hour_6h"))
    sustained_rate = six_hour_rate if six_hour_rate is not None else one_hour_rate
    efficiency = query_pushdown.get("efficiency") or {}
    diff_utilization = query_pushdown.get("diff_utilization") or {}
    fallback_steps = int(efficiency.get("fallback_steps") or 0)
    diff_ratio = _optional_float(diff_utilization.get("diff_actual_ratio"))

    base = {
        "target_shards_per_hour": ADAPTIVE_CAPACITY_TARGET_SHARDS_PER_HOUR,
        "max_safe_issue_rate_per_hour": ADAPTIVE_CAPACITY_MAX_ISSUES_PER_HOUR,
        "hold_minutes": ADAPTIVE_CAPACITY_HOLD_MINUTES,
        "issue_rate_per_hour": issue_rate,
        "shards_per_hour_1h": one_hour_rate,
        "shards_per_hour_6h": six_hour_rate,
        "sustained_shards_per_hour": sustained_rate,
        "capacity_evidence": evidence,
        "next_tuning_batch": batch,
    }

    if not throughput.get("available") or one_hour_rate is None:
        return {
            **base,
            "status": "info",
            "decision": "insufficient_evidence",
            "message": (
                "Adaptive capacity needs at least one hourly throughput checkpoint "
                "before recommending a tuning batch."
            ),
        }

    if issue_rate is None:
        return {
            **base,
            "status": "info",
            "decision": "insufficient_evidence",
            "message": (
                "Adaptive capacity needs an issue-rate checkpoint before tuning "
                "worker, query, or page-size settings."
            ),
        }

    if issue_rate > ADAPTIVE_CAPACITY_MAX_ISSUES_PER_HOUR:
        return {
            **base,
            "status": "warn",
            "decision": "rollback_latest_tuning_batch",
            "message": (
                "Issue rate is above the safe tuning threshold; roll back only the "
                "latest capacity increment, restart workers only, and hold for "
                f"{ADAPTIVE_CAPACITY_HOLD_MINUTES} minutes."
            ),
        }

    if fallback_steps > 0:
        return {
            **base,
            "status": "info",
            "decision": "hold_reduce_fallback_first",
            "message": (
                "Hold capacity steady and reduce fallback-heavy model fetches before "
                "increasing workers, query concurrency, or page size."
            ),
        }

    if diff_ratio is not None and diff_ratio < 1.0:
        return {
            **base,
            "status": "info",
            "decision": "hold_restore_diff_first",
            "message": (
                "Hold capacity steady and restore diff execution before tuning "
                "worker or database capacity."
            ),
        }

    low_throughput = one_hour_rate < ADAPTIVE_CAPACITY_TARGET_SHARDS_PER_HOUR and (
        six_hour_rate is None
        or six_hour_rate < ADAPTIVE_CAPACITY_TARGET_SHARDS_PER_HOUR
    )
    if not low_throughput:
        return {
            **base,
            "status": "pass",
            "decision": "hold_current_settings",
            "message": (
                "Throughput is at or above the tuning target; hold current settings "
                f"for {ADAPTIVE_CAPACITY_HOLD_MINUTES} minutes and continue hourly "
                "checks."
            ),
        }

    if evidence["status"] == "blocked":
        return {
            **base,
            "status": "warn",
            "decision": "capacity_blocked",
            "message": (
                "Throughput is below target, but worker/database capacity evidence "
                "shows no headroom. Do not increase concurrency until that bottleneck "
                "is resolved."
            ),
        }

    if evidence["status"] != "available":
        return {
            **base,
            "status": "info",
            "decision": "insufficient_evidence",
            "message": (
                "Throughput is below target and issue rate is safe, but worker count "
                "or database headroom evidence is missing. Capture active worker "
                "count and database headroom before applying the next tuning batch."
            ),
        }

    return {
        **base,
        "status": "warn",
        "decision": "recommend_tuning_batch",
        "message": (
            "Throughput is below target and issue rate is safe; apply one tuning "
            "batch, restart workers only, then hold for "
            f"{ADAPTIVE_CAPACITY_HOLD_MINUTES} minutes."
        ),
    }


def _adaptive_capacity_evidence(sync, *, throughput):
    parameters = {}
    source_parameters = getattr(getattr(sync, "source", None), "parameters", None) or {}
    sync_parameters = getattr(sync, "parameters", None) or {}
    for candidate in (
        sync_parameters.get("runtime_capacity_evidence"),
        sync_parameters.get("capacity_evidence"),
        source_parameters.get("runtime_capacity_evidence"),
        source_parameters.get("capacity_evidence"),
    ):
        if isinstance(candidate, dict):
            parameters.update(candidate)
    for key in (
        "active_worker_count",
        "worker_count",
        "netbox_worker_count",
        "worker_replicas",
        "database_headroom",
        "db_headroom",
        "worker_headroom",
        "queue_backlog_depth",
    ):
        if key in source_parameters and key not in parameters:
            parameters[key] = source_parameters[key]
        if key in sync_parameters and key not in parameters:
            parameters[key] = sync_parameters[key]

    worker_count = _optional_int(
        parameters.get("active_worker_count")
        or parameters.get("worker_count")
        or parameters.get("netbox_worker_count")
        or parameters.get("worker_replicas")
    )
    queue_backlog_depth = _optional_int(parameters.get("queue_backlog_depth"))
    database_headroom = _headroom_status(
        parameters.get("database_headroom") or parameters.get("db_headroom")
    )
    worker_headroom = _headroom_status(parameters.get("worker_headroom"))
    bottleneck_phase = str((throughput or {}).get("bottleneck_phase") or "unknown")

    if database_headroom == "blocked" or worker_headroom == "blocked":
        status = "blocked"
        message = "Worker or database headroom evidence shows a capacity bottleneck."
    elif worker_count is not None and database_headroom == "available":
        status = "available"
        message = "Worker count and database headroom evidence are available."
    else:
        status = "unknown"
        message = "Worker count or database headroom evidence is missing."

    return {
        "status": status,
        "message": message,
        "active_worker_count": worker_count,
        "queue_backlog_depth": queue_backlog_depth,
        "database_headroom": database_headroom,
        "worker_headroom": worker_headroom,
        "bottleneck_phase": bottleneck_phase,
    }


def _adaptive_tuning_batch(sync, *, evidence):
    current_workers = (evidence or {}).get("active_worker_count")
    current_concurrency = source_query_fetch_concurrency(sync)
    current_page_size = _source_nqe_page_size(sync)
    recommended_workers = (
        max(int(current_workers) + 1, ceil(int(current_workers) * 1.5))
        if current_workers is not None
        else None
    )
    recommended_concurrency = min(
        MAX_QUERY_FETCH_CONCURRENCY,
        max(current_concurrency, ceil(current_concurrency * 1.25)),
    )
    recommended_page_size = min(
        MAX_NQE_PAGE_SIZE,
        max(current_page_size, ceil(current_page_size * 1.2)),
    )
    return {
        "code": "one_capacity_tuning_batch",
        "worker_count": {
            "current": current_workers,
            "recommended": recommended_workers,
            "change": "+50% round up" if current_workers is not None else "unknown",
        },
        "query_fetch_concurrency": {
            "current": current_concurrency,
            "recommended": recommended_concurrency,
            "change": f"+25% round up, cap {MAX_QUERY_FETCH_CONCURRENCY}",
        },
        "nqe_page_size": {
            "current": current_page_size,
            "recommended": recommended_page_size,
            "change": f"+20% round up, cap {MAX_NQE_PAGE_SIZE}",
        },
        "restart_scope": "restart_workers_only",
        "hold_minutes": ADAPTIVE_CAPACITY_HOLD_MINUTES,
        "message": (
            "Increase workers by 50% round up, query_fetch_concurrency by 25% "
            f"cap {MAX_QUERY_FETCH_CONCURRENCY}, nqe_page_size by 20% cap "
            f"{MAX_NQE_PAGE_SIZE}; restart workers only, then hold "
            f"{ADAPTIVE_CAPACITY_HOLD_MINUTES} minutes."
        ),
    }


def _headroom_status(value):
    if value in ("", None):
        return "unknown"
    if isinstance(value, bool):
        return "available" if value else "blocked"
    normalized = str(value).strip().lower()
    if normalized in {"available", "ok", "pass", "sufficient", "headroom", "true"}:
        return "available"
    if normalized in {
        "blocked",
        "limited",
        "none",
        "fail",
        "false",
        "contended",
        "saturated",
    }:
        return "blocked"
    return "unknown"


def _optional_float(value):
    if value in ("", None):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _optional_int(value):
    if value in ("", None):
        return None
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return None
    return parsed if parsed > 0 else None


def dependency_preflight_summary(sync, enabled_models):
    enabled_models = sorted(str(model) for model in (enabled_models or []) if model)
    enabled_model_set = set(enabled_models)
    configured_models = set(forward_configured_models())
    delete_or_prune = _delete_or_prune_evidence(sync)
    warnings = []

    for rule in DEPENDENCY_PREFLIGHT_RULES:
        selected_model = rule["selected_model"]
        if selected_model not in enabled_model_set:
            continue
        if selected_model == "dcim.device" and not delete_or_prune:
            continue
        omitted_models = [
            model
            for model in rule["omitted_models"]
            if model in configured_models and model not in enabled_model_set
        ]
        if not omitted_models:
            continue
        warnings.append(
            {
                "code": rule["code"],
                "status": "warn",
                "selected_model": selected_model,
                "omitted_models": omitted_models,
                "suggested_models": omitted_models,
                "message": (
                    f"{rule['message']} Omitted model(s): "
                    f"{', '.join(omitted_models)}."
                ),
                "delete_dependency_rank": _delete_dependency_rank(selected_model),
                "omitted_delete_dependency_ranks": {
                    model: _delete_dependency_rank(model) for model in omitted_models
                },
            }
        )

    if "dcim.device" not in enabled_model_set and "dcim.device" in configured_models:
        for selected_model in sorted(PARENT_DEVICE_DEPENDENT_MODELS):
            if selected_model not in enabled_model_set:
                continue
            warnings.append(
                {
                    "code": "parent_device_model_omitted",
                    "status": "warn",
                    "selected_model": selected_model,
                    "omitted_models": ["dcim.device"],
                    "suggested_models": ["dcim.device"],
                    "message": (
                        f"{selected_model} rows rely on dcim.device coverage in the "
                        "same sync. Include dcim.device or expect child rows to be "
                        "skipped when parent device rows are missing."
                    ),
                    "delete_dependency_rank": _delete_dependency_rank(selected_model),
                    "omitted_delete_dependency_ranks": {
                        "dcim.device": _delete_dependency_rank("dcim.device"),
                    },
                }
            )

    if warnings:
        return {
            "status": "warn",
            "message": (
                f"{len(warnings)} scoped dependency warning(s) found; include the "
                "suggested models or expect protected delete skips to remain "
                "non-blocking row issues."
            ),
            "enabled_models": enabled_models,
            "delete_or_prune_possible": bool(delete_or_prune),
            "delete_or_prune_evidence": delete_or_prune,
            "warnings": warnings,
        }

    return {
        "status": "pass",
        "message": "No scoped dependency warnings were found for enabled models.",
        "enabled_models": enabled_models,
        "delete_or_prune_possible": bool(delete_or_prune),
        "delete_or_prune_evidence": delete_or_prune,
        "warnings": [],
    }


def _delete_or_prune_evidence(sync):
    evidence = []
    source_parameters = getattr(getattr(sync, "source", None), "parameters", {}) or {}
    if source_parameters.get("device_tag_prune_out_of_scope"):
        evidence.append("device_tag_prune_out_of_scope")

    latest_ingestion = getattr(sync, "last_ingestion", None)
    if latest_ingestion is not None and getattr(
        latest_ingestion, "baseline_ready", False
    ):
        evidence.append("baseline_ready_for_diff_deletes")

    return sorted(set(evidence))


def _delete_dependency_rank(model_string):
    try:
        return architecture_contract_for_model(model_string).delete_dependency_rank
    except Exception:
        return None


def delete_wave_summary(run, latest_ingestion=None):
    if run is None:
        return {
            "available": False,
            "status": "info",
            "phase": "unavailable",
            "message": "No execution run is available for delete-wave visibility.",
            "plan": _empty_delete_dependency_plan(),
            "steps": _delete_wave_step_summary([]),
            "latest_ingestion": _delete_wave_ingestion_summary(latest_ingestion),
            "warning_codes": [],
            "high_risk_models": [],
        }

    plan = dict((run.plan_preview or {}).get("delete_dependency_plan") or {})
    if not plan:
        plan = _empty_delete_dependency_plan()
    steps = list(
        run.steps.filter(kind=ForwardExecutionStepKindChoices.STAGE).order_by(
            "index",
            "pk",
        )
    )
    step_summary = _delete_wave_step_summary(steps)
    phase, message = _delete_wave_phase_message(plan, step_summary)
    latest_ingestion_summary = _delete_wave_ingestion_summary(latest_ingestion)
    dependency_skip_count = int(
        latest_ingestion_summary["dependency_skip_issues"]["count"]
    )
    warnings = list(plan.get("warnings") or [])
    warning_codes = sorted(
        {
            str(item.get("code") or "").strip()
            for item in warnings
            if str(item.get("code") or "").strip()
        }
    )
    high_risk_models = _delete_wave_high_risk_models(plan)

    if warnings or dependency_skip_count:
        status = "warn"
    elif int(plan.get("delete_rows") or 0) > 0:
        status = "info" if phase not in {"complete", "no_deletes"} else "pass"
    else:
        status = "pass"

    if dependency_skip_count:
        message = (
            f"{message} Latest ingestion has {dependency_skip_count} protected "
            "dependency skip issue(s); these are expected non-blocking delete rows."
        )

    return {
        "available": True,
        "status": status,
        "phase": phase,
        "message": message,
        "plan": plan,
        "steps": step_summary,
        "latest_ingestion": latest_ingestion_summary,
        "warning_codes": warning_codes,
        "high_risk_models": high_risk_models,
    }


def _empty_delete_dependency_plan():
    return {
        "status": "none",
        "delete_rows": 0,
        "delete_shards": 0,
        "delete_model_count": 0,
        "delete_share": 0.0,
        "max_delete_shard_changes": 0,
        "execution_order": [],
        "models": {},
        "warnings": [],
    }


def _delete_wave_step_summary(steps):
    delete_steps = [step for step in steps if step.operation == "delete"]
    active_delete_steps = [
        step for step in delete_steps if step.status in DELETE_ACTIVE_STEP_STATUSES
    ]
    completed_delete_steps = [
        step for step in delete_steps if step.status in DELETE_TERMINAL_STEP_STATUSES
    ]
    apply_steps = [step for step in steps if step.operation != "delete"]
    pending_apply_steps = [
        step for step in apply_steps if step.status not in DELETE_TERMINAL_STEP_STATUSES
    ]
    status_counts = Counter(step.status for step in delete_steps)
    return {
        "stage_step_count": len(steps),
        "delete_step_count": len(delete_steps),
        "active_delete_step_count": len(active_delete_steps),
        "completed_delete_step_count": len(completed_delete_steps),
        "pending_apply_step_count": len(pending_apply_steps),
        "status_counts": dict(status_counts),
        "current_delete_step": (
            _delete_step_summary(active_delete_steps[0]) if active_delete_steps else {}
        ),
    }


def _delete_step_summary(step):
    return {
        "id": step.pk,
        "index": step.index,
        "status": step.status,
        "model": step.model_string,
        "estimated_changes": step.estimated_changes,
        "actual_changes": step.actual_changes,
    }


def _delete_wave_phase_message(plan, step_summary):
    delete_rows = int(plan.get("delete_rows") or 0)
    delete_shards = int(plan.get("delete_shards") or 0)
    delete_step_count = int(step_summary.get("delete_step_count") or 0)
    completed_delete_steps = int(step_summary.get("completed_delete_step_count") or 0)
    active_delete_steps = int(step_summary.get("active_delete_step_count") or 0)
    pending_apply_steps = int(step_summary.get("pending_apply_step_count") or 0)

    if not delete_rows:
        return "no_deletes", "No delete wave is planned for the latest execution run."
    if active_delete_steps:
        current = step_summary.get("current_delete_step") or {}
        return (
            "delete",
            (
                "Delete wave is active on "
                f"{current.get('model') or 'unknown model'} step "
                f"{current.get('index') or '?'}."
            ),
        )
    if delete_step_count and completed_delete_steps >= delete_step_count:
        return "complete", "All planned delete-wave stage steps are terminal."
    if pending_apply_steps:
        return (
            "apply_before_delete",
            (
                f"{delete_rows} delete row(s) across {delete_shards} shard(s) are "
                "planned after earlier apply shards complete."
            ),
        )
    if delete_step_count:
        return (
            "delete_pending",
            (
                f"{delete_rows} delete row(s) across {delete_shards} shard(s) are "
                "planned and waiting for delete stage execution."
            ),
        )
    return (
        "planned",
        (
            f"{delete_rows} delete row(s) across {delete_shards} shard(s) are "
            "planned, but execution steps have not been materialized yet."
        ),
    )


def _delete_wave_ingestion_summary(ingestion):
    if ingestion is None:
        return {
            "id": None,
            "deleted_change_count": 0,
            "dependency_skip_issues": {"count": 0, "models": {}},
        }
    issues = ingestion.issues.filter(exception="ForwardDependencySkipError")
    issue_models = Counter(
        model or "unknown" for model in issues.values_list("model", flat=True)
    )
    return {
        "id": ingestion.pk,
        "deleted_change_count": int(ingestion.deleted_change_count or 0),
        "dependency_skip_issues": {
            "count": sum(issue_models.values()),
            "models": dict(issue_models),
        },
    }


def _delete_wave_high_risk_models(plan):
    risk_order = {"critical": 3, "high": 2, "medium": 1, "low": 0, "none": 0}
    models = []
    for model, details in (plan.get("models") or {}).items():
        details = details or {}
        risk = str(details.get("reference_blocker_risk") or "").strip().lower()
        if not risk or risk in {"low", "none"}:
            continue
        models.append(
            {
                "model": model,
                "reference_blocker_risk": risk,
                "delete_rows": _safe_int(details.get("delete_rows")),
                "delete_shards": _safe_int(details.get("delete_shards")),
            }
        )
    models.sort(
        key=lambda item: (
            -risk_order.get(item["reference_blocker_risk"], -1),
            -int(item["delete_rows"] or 0),
            str(item["model"]),
        )
    )
    return models[:5]


def throughput_summary(sync, run, latest_ingestion=None, *, now=None):
    if run is None:
        return {
            "available": False,
            "status": "info",
            "message": "No execution run is available for throughput projection.",
            "current_shard_index": None,
            "total_shards": 0,
            "completed_shards": 0,
            "remaining_shards": 0,
            "shards_per_hour_1h": None,
            "shards_per_hour_6h": None,
            "eta_seconds_low": None,
            "eta_seconds_high": None,
            "current_model": "",
            "active_step_status": "",
            "active_step_age_seconds": None,
            "issue_rate_per_hour": None,
            "queue_wait_seconds_average": None,
            "fetch_time_seconds_average": None,
            "apply_time_seconds_average": None,
            "merge_time_seconds_average": None,
            "fallback_step_count": 0,
            "bottleneck_phase": "unknown",
            "worker_timeout_seconds": configured_rq_default_timeout(),
            "query_fetch_concurrency": source_query_fetch_concurrency(sync),
            "nqe_page_size": _source_nqe_page_size(sync),
            "throughput_smoothing": {
                "status": "info",
                "message": "Run-throughput smoothing evidence is unavailable until timing data exists.",
                "totals": {},
                "observed_counts": {},
                "wait_seconds": None,
                "total_observed_seconds": None,
                "wait_share": None,
                "hotspot_models": [],
                "scheduler_overlap_readiness": {
                    "status": "unknown",
                    "ready": False,
                    "dominant_wait_component": "",
                    "capacity_evidence": scheduler_overlap_capacity_evidence(sync),
                    "blocking_reasons": ["timing_evidence_missing"],
                    "message": (
                        "Scheduler overlap is not assessable until queue, stage, and merge timing evidence exists."
                    ),
                    "required_before_enablement": [
                        "Collect support-bundle throughput_smoothing evidence.",
                        "Confirm dependency order and branch-budget state are reconstructable from the ledger.",
                    ],
                },
            },
            "scheduler_overlap_readiness": {},
            "scheduler_overlap_status": "unknown",
            "scheduler_overlap_message": "Scheduler overlap readiness is unavailable until timing evidence exists.",
            "scheduler_overlap_dominant_wait_component": "",
            "scheduler_overlap_blocking_reasons": ["timing_evidence_missing"],
            "scheduler_overlap_hotspot_models": [],
        }

    now = now or timezone.now()
    steps = list(run.steps.order_by("index", "kind", "pk"))
    stage_steps = [
        step for step in steps if step.kind == ForwardExecutionStepKindChoices.STAGE
    ]
    merge_steps = [
        step for step in steps if step.kind == ForwardExecutionStepKindChoices.MERGE
    ]
    completed_stage_steps = [
        step for step in stage_steps if step.status in DELETE_TERMINAL_STEP_STATUSES
    ]
    active_step = _active_execution_step(steps)
    total_shards = int(len(stage_steps) or run.total_steps)
    completed_shards = len(completed_stage_steps)
    remaining_shards = max(0, total_shards - completed_shards)
    durations = [
        step_duration_seconds(step)
        for step in completed_stage_steps
        if step_duration_seconds(step) is not None
    ]
    average_stage_seconds = sum(durations) / len(durations) if durations else None
    eta_low = None
    eta_high = None
    if average_stage_seconds is not None:
        eta_low = round(average_stage_seconds * remaining_shards * 0.8, 3)
        eta_high = round(average_stage_seconds * remaining_shards * 1.25, 3)

    queue_waits = [
        _seconds_between(step.created, step.started)
        for step in stage_steps
        if _seconds_between(step.created, step.started) is not None
    ]
    fetch_times = [
        float(step.query_runtime_ms) / 1000.0
        for step in completed_stage_steps
        if step.query_runtime_ms is not None
    ]
    apply_times = []
    for step in completed_stage_steps:
        duration = step_duration_seconds(step)
        if duration is None:
            continue
        fetch_seconds = (
            float(step.query_runtime_ms) / 1000.0
            if step.query_runtime_ms is not None
            else 0.0
        )
        apply_times.append(max(0.0, duration - fetch_seconds))
    merge_times = [
        step_duration_seconds(step)
        for step in merge_steps
        if step_duration_seconds(step) is not None
    ]
    fallback_step_count = len(
        [
            step
            for step in stage_steps
            if str(step.fetch_mode or "model")
            in {"model", "full_fallback", "diff_fallback"}
        ]
    )
    throughput_smoothing = throughput_smoothing_summary(
        [
            {
                "model": step.model_string,
                "stage_queue_seconds": _stage_queue_seconds(step),
                "stage_duration_seconds": step_duration_seconds(step),
                "merge_queue_seconds": _merge_queue_seconds(step),
                "merge_wait_seconds": _merge_wait_seconds(step),
                "merge_duration_seconds": _merge_duration_seconds(step),
            }
            for step in steps
        ],
        capacity_evidence=scheduler_overlap_capacity_evidence(sync),
    )
    scheduler_overlap_readiness = (
        throughput_smoothing.get("scheduler_overlap_readiness") or {}
    )

    message = _throughput_message(
        completed_shards=completed_shards,
        total_shards=total_shards,
        remaining_shards=remaining_shards,
        eta_low=eta_low,
        eta_high=eta_high,
        active_step=active_step,
    )
    status = _throughput_status(
        active_step=active_step,
        active_step_age_seconds=_active_step_age_seconds(active_step, now),
        worker_timeout_seconds=configured_rq_default_timeout(),
        completed_shards=completed_shards,
        total_shards=total_shards,
    )
    current_shard_index = (
        active_step.index
        if active_step
        else min(int(run.next_step_index or total_shards), total_shards or 0) or None
    )

    return {
        "available": True,
        "status": status,
        "message": message,
        "current_shard_index": current_shard_index,
        "total_shards": total_shards,
        "completed_shards": completed_shards,
        "remaining_shards": remaining_shards,
        "shards_per_hour_1h": _completed_steps_per_hour(
            completed_stage_steps,
            now=now,
            hours=1,
        ),
        "shards_per_hour_6h": _completed_steps_per_hour(
            completed_stage_steps,
            now=now,
            hours=6,
        ),
        "eta_seconds_low": eta_low,
        "eta_seconds_high": eta_high,
        "current_model": active_step.model_string if active_step else "",
        "active_step_status": active_step.status if active_step else "",
        "active_step_age_seconds": _active_step_age_seconds(active_step, now),
        "issue_rate_per_hour": _issue_rate_per_hour(latest_ingestion, now=now),
        "queue_wait_seconds_average": _average(queue_waits),
        "fetch_time_seconds_average": _average(fetch_times),
        "apply_time_seconds_average": _average(apply_times),
        "merge_time_seconds_average": _average(merge_times),
        "fallback_step_count": fallback_step_count,
        "bottleneck_phase": _bottleneck_phase(
            queue_wait_seconds=_average(queue_waits),
            fetch_seconds=_average(fetch_times),
            apply_seconds=_average(apply_times),
            merge_seconds=_average(merge_times),
        ),
        "worker_timeout_seconds": configured_rq_default_timeout(),
        "query_fetch_concurrency": source_query_fetch_concurrency(sync),
        "nqe_page_size": _source_nqe_page_size(sync),
        "throughput_smoothing": throughput_smoothing,
        "scheduler_overlap_readiness": scheduler_overlap_readiness,
        "scheduler_overlap_status": scheduler_overlap_readiness.get(
            "status", "unknown"
        ),
        "scheduler_overlap_message": scheduler_overlap_readiness.get(
            "message",
            "Scheduler overlap readiness is unavailable until timing evidence exists.",
        ),
        "scheduler_overlap_dominant_wait_component": scheduler_overlap_readiness.get(
            "dominant_wait_component", ""
        ),
        "scheduler_overlap_blocking_reasons": list(
            scheduler_overlap_readiness.get("blocking_reasons") or []
        ),
        "scheduler_overlap_hotspot_models": list(
            scheduler_overlap_readiness.get("hotspot_models") or []
        ),
    }


def _stage_queue_seconds(step):
    job = getattr(step, "job", None)
    queued_at = getattr(job, "created", None) or getattr(step, "created", None)
    started_at = getattr(job, "started", None) or getattr(step, "started", None)
    return _seconds_between(queued_at, started_at)


def _merge_queue_seconds(step):
    merge_job = getattr(step, "merge_job", None)
    return _seconds_between(
        getattr(merge_job, "created", None),
        getattr(merge_job, "started", None),
    )


def _merge_wait_seconds(step):
    merge_job = getattr(step, "merge_job", None)
    return _seconds_between(
        getattr(step, "completed", None),
        getattr(merge_job, "started", None),
    )


def _merge_duration_seconds(step):
    merge_job = getattr(step, "merge_job", None)
    return _seconds_between(
        getattr(merge_job, "started", None),
        getattr(merge_job, "completed", None),
    )


def _active_execution_step(steps):
    for status in (
        ForwardExecutionStepStatusChoices.RUNNING,
        ForwardExecutionStepStatusChoices.QUEUED,
        ForwardExecutionStepStatusChoices.MERGE_QUEUED,
        ForwardExecutionStepStatusChoices.PENDING,
    ):
        for step in steps:
            if step.status == status:
                return step
    return None


def _throughput_message(
    *,
    completed_shards,
    total_shards,
    remaining_shards,
    eta_low,
    eta_high,
    active_step,
):
    if total_shards <= 0:
        return "Execution-run throughput is unavailable until steps are planned."
    if remaining_shards <= 0:
        return f"All {total_shards} planned shard step(s) are terminal."
    eta = "unknown"
    if eta_low is not None and eta_high is not None:
        eta = f"{eta_low:.1f}s to {eta_high:.1f}s"
    if active_step is None:
        return (
            f"{completed_shards}/{total_shards} shard step(s) are terminal; "
            f"{remaining_shards} remain. ETA: {eta}."
        )
    return (
        f"{completed_shards}/{total_shards} shard step(s) are terminal; "
        f"{remaining_shards} remain. Active step {active_step.index} "
        f"{active_step.model_string or 'unknown model'} is {active_step.status}. "
        f"ETA: {eta}."
    )


def _throughput_status(
    *,
    active_step,
    active_step_age_seconds,
    worker_timeout_seconds,
    completed_shards,
    total_shards,
):
    if total_shards and completed_shards >= total_shards:
        return "pass"
    if (
        active_step is not None
        and worker_timeout_seconds
        and active_step_age_seconds
        and active_step_age_seconds >= worker_timeout_seconds
    ):
        return "warn"
    if completed_shards:
        return "pass"
    return "info"


def _completed_steps_per_hour(steps, *, now, hours):
    if not steps:
        return None
    cutoff = now - timedelta(hours=hours)
    count = len([step for step in steps if step.completed and step.completed >= cutoff])
    return round(count / float(hours), 3)


def _active_step_age_seconds(step, now):
    if step is None:
        return None
    started = step.started or step.created
    return _seconds_between(started, now)


def _seconds_between(started, completed):
    if not started or not completed:
        return None
    try:
        return round(max(0.0, (completed - started).total_seconds()), 3)
    except (TypeError, ValueError):
        return None


def _issue_rate_per_hour(ingestion, *, now):
    if ingestion is None:
        return None
    issue_count = ingestion.issues.count()
    if not issue_count:
        return 0.0
    elapsed_seconds = _seconds_between(ingestion.created, now)
    if not elapsed_seconds:
        return None
    return round(issue_count / max(elapsed_seconds / 3600.0, 1 / 3600.0), 3)


def _average(values):
    values = [value for value in values if value is not None]
    if not values:
        return None
    return round(sum(values) / len(values), 3)


def _bottleneck_phase(
    *,
    queue_wait_seconds,
    fetch_seconds,
    apply_seconds,
    merge_seconds,
):
    candidates = {
        "queue": queue_wait_seconds,
        "fetch": fetch_seconds,
        "apply": apply_seconds,
        "merge": merge_seconds,
    }
    candidates = {
        phase: seconds for phase, seconds in candidates.items() if seconds is not None
    }
    if not candidates:
        return "unknown"
    return max(candidates.items(), key=lambda item: item[1])[0]


def _source_nqe_page_size(sync):
    parameters = getattr(getattr(sync, "source", None), "parameters", None) or {}
    value = parameters.get("nqe_page_size")
    if value in ("", None):
        return DEFAULT_NQE_PAGE_SIZE
    try:
        return int(value)
    except (TypeError, ValueError):
        return DEFAULT_NQE_PAGE_SIZE


def execution_backend_advice(*, runtime, capacity, efficiency, diff_utilization):
    runtime = runtime or {}
    capacity = capacity or {}
    efficiency = efficiency or {}
    diff_utilization = diff_utilization or {}

    backend = (
        runtime.get("execution_backend") or ForwardExecutionBackendChoices.BRANCHING
    )
    projected_remaining = capacity.get("projected_remaining_seconds")
    worker_timeout = runtime.get("rq_default_timeout_seconds")
    total_steps = int(capacity.get("total_steps") or 0)
    remaining_steps = int(capacity.get("remaining_steps") or 0)
    fallback_steps = int(efficiency.get("fallback_steps") or 0)
    diff_ratio = diff_utilization.get("diff_actual_ratio")
    timeout_risk = bool(
        worker_timeout
        and projected_remaining
        and float(projected_remaining) >= (float(worker_timeout) * 0.8)
    )

    if backend == ForwardExecutionBackendChoices.FAST_BOOTSTRAP:
        return {
            "status": "info",
            "code": "fast_bootstrap_baseline_active",
            "action_code": "complete_fast_bootstrap_then_branching",
            "priority": 1,
            "message": (
                "Fast bootstrap is active; use it only for a trusted baseline, "
                "then switch back to Branching for steady-state diff review."
            ),
            "recommended_backend": ForwardExecutionBackendChoices.FAST_BOOTSTRAP,
            "next_backend": ForwardExecutionBackendChoices.BRANCHING,
        }

    if diff_ratio is not None and float(diff_ratio) >= 1.0:
        return {
            "status": "pass",
            "code": "branching_diff_path_ready",
            "action_code": "",
            "priority": 99,
            "message": (
                "Branching is appropriate: current evidence shows diff-capable "
                "stage execution."
            ),
            "recommended_backend": ForwardExecutionBackendChoices.BRANCHING,
            "next_backend": ForwardExecutionBackendChoices.BRANCHING,
        }

    if timeout_risk:
        return {
            "status": "warn",
            "code": "branching_timeout_risk_consider_bootstrap",
            "action_code": "consider_fast_bootstrap_for_trusted_baseline",
            "priority": 1,
            "message": (
                "Projected remaining Branching runtime is close to or above worker "
                "timeout; use Fast bootstrap for a trusted first baseline or raise "
                "worker capacity before rerunning."
            ),
            "recommended_backend": ForwardExecutionBackendChoices.FAST_BOOTSTRAP,
            "next_backend": ForwardExecutionBackendChoices.BRANCHING,
        }

    if fallback_steps > 0:
        return {
            "status": "info",
            "code": "branching_fix_pushdown_before_capacity",
            "action_code": "keep_branching_reduce_fallback_first",
            "priority": 3,
            "message": (
                "Branching is still the review path, but fallback fetch is present; "
                "reduce fallback before tuning backend choice or worker capacity."
            ),
            "recommended_backend": ForwardExecutionBackendChoices.BRANCHING,
            "next_backend": ForwardExecutionBackendChoices.BRANCHING,
        }

    if total_steps >= 20 and remaining_steps > 0:
        return {
            "status": "info",
            "code": "large_branching_baseline_monitor",
            "action_code": "monitor_branching_or_use_bootstrap_if_unreviewable",
            "priority": 4,
            "message": (
                "This is a large Branching run; continue if review is required, or "
                "use Fast bootstrap for a trusted baseline that is too large to "
                "review shard-by-shard."
            ),
            "recommended_backend": ForwardExecutionBackendChoices.BRANCHING,
            "next_backend": ForwardExecutionBackendChoices.BRANCHING,
        }

    return {
        "status": "pass",
        "code": "branching_review_path_ok",
        "action_code": "",
        "priority": 99,
        "message": "Branching is appropriate for the current run evidence.",
        "recommended_backend": ForwardExecutionBackendChoices.BRANCHING,
        "next_backend": ForwardExecutionBackendChoices.BRANCHING,
    }


def query_pushdown_summary(run):
    alert_thresholds = _pushdown_alert_thresholds_for_run(run)
    if run is None:
        return {
            "available": False,
            "message": "No execution run is available for query pushdown profiling.",
            "total_stage_steps": 0,
            "fetch_mode_counts": {},
            "fallback_step_count": 0,
            "fallback_reasons": [],
            "fallback_reason_summary": fallback_reason_summary([]),
            "partition_retry_summary": partition_retry_summary([]),
            "slow_models": [],
            "efficiency": pushdown_efficiency_summary(
                fetch_mode_counts_by_model={},
                alert_thresholds=alert_thresholds,
            ),
            "runtime_share": pushdown_runtime_summary(
                [], alert_thresholds=alert_thresholds
            ),
            "diff_utilization": diff_utilization_summary(
                [], alert_thresholds=alert_thresholds
            ),
            "diff_baseline_transition": diff_baseline_transition_summary(
                run,
                [],
            ),
            "tuning_guidance": [],
            "alert_thresholds": alert_thresholds,
            "trend_snapshots": [],
        }
    steps = list(
        run.steps.filter(kind=ForwardExecutionStepKindChoices.STAGE).order_by(
            "index",
            "pk",
        )
    )
    if not steps:
        return {
            "available": False,
            "message": "No stage steps are available for query pushdown profiling.",
            "total_stage_steps": 0,
            "fetch_mode_counts": {},
            "fallback_step_count": 0,
            "fallback_reasons": [],
            "fallback_reason_summary": fallback_reason_summary([]),
            "partition_retry_summary": partition_retry_summary([]),
            "slow_models": [],
            "efficiency": pushdown_efficiency_summary(
                fetch_mode_counts_by_model={},
                alert_thresholds=alert_thresholds,
            ),
            "runtime_share": pushdown_runtime_summary(
                [], alert_thresholds=alert_thresholds
            ),
            "diff_utilization": diff_utilization_summary(
                [], alert_thresholds=alert_thresholds
            ),
            "diff_baseline_transition": diff_baseline_transition_summary(
                run,
                [],
            ),
            "tuning_guidance": [],
            "alert_thresholds": alert_thresholds,
            "trend_snapshots": [],
        }

    fetch_mode_counts = {}
    fallback_reasons = []
    fallback_modes = {"model", "full_fallback", "diff_fallback"}
    model_stats = {}
    fallback_step_count = 0
    for step in steps:
        fetch_mode = str(step.fetch_mode or "model")
        fetch_mode_counts[fetch_mode] = fetch_mode_counts.get(fetch_mode, 0) + 1
        if fetch_mode in fallback_modes:
            fallback_step_count += 1
        reason = str((step.fetch_parameters or {}).get("fallback_reason") or "").strip()
        if reason:
            fallback_reasons.append(reason)

        model_string = str(step.model_string or "").strip() or "unknown"
        stat = model_stats.setdefault(
            model_string,
            {
                "model": model_string,
                "query_runtime_ms": 0.0,
                "fetched_row_count": 0,
                "steps": 0,
                "fetch_modes": set(),
                "fallback_steps": 0,
            },
        )
        stat["query_runtime_ms"] += float(step.query_runtime_ms or 0.0)
        stat["fetched_row_count"] += int(step.fetched_row_count or 0)
        stat["steps"] += 1
        stat["fetch_modes"].add(fetch_mode)
        if fetch_mode in fallback_modes:
            stat["fallback_steps"] += 1

    slow_models = sorted(
        model_stats.values(),
        key=lambda item: item["query_runtime_ms"],
        reverse=True,
    )[:5]
    for item in slow_models:
        item["query_runtime_ms"] = round(item["query_runtime_ms"], 3)
        item["fetch_modes"] = sorted(item["fetch_modes"])

    if fallback_step_count:
        message = (
            f"Profiled {len(steps)} stage step(s); {fallback_step_count} step(s) used "
            "model/full fallback fetch."
        )
    else:
        message = f"Profiled {len(steps)} stage step(s); all steps used shard-aware fetch modes."

    fetch_mode_counts_by_model = {}
    for model_string, stat in model_stats.items():
        model_counts = {}
        for mode in stat["fetch_modes"]:
            model_counts[mode] = 0
        fetch_mode_counts_by_model[model_string] = model_counts
    for step in steps:
        model_string = str(step.model_string or "").strip() or "unknown"
        mode = str(step.fetch_mode or "model")
        model_counts = fetch_mode_counts_by_model.setdefault(model_string, {})
        model_counts[mode] = int(model_counts.get(mode, 0)) + 1

    efficiency = pushdown_efficiency_summary(
        fetch_mode_counts_by_model=fetch_mode_counts_by_model,
        alert_thresholds=alert_thresholds,
    )
    runtime_share = pushdown_runtime_summary(steps, alert_thresholds=alert_thresholds)
    reason_summary = fallback_reason_summary(steps)
    retry_summary = partition_retry_summary(steps)
    diff_utilization = diff_utilization_summary(
        steps, alert_thresholds=alert_thresholds
    )
    diff_transition = diff_baseline_transition_summary(
        run,
        steps,
        diff_utilization=diff_utilization,
    )
    tuning_guidance = pushdown_tuning_guidance(
        efficiency=efficiency,
        runtime_share=runtime_share,
        diff_utilization=diff_utilization,
        partition_retries=retry_summary,
        alert_thresholds=alert_thresholds,
        query_fetch_concurrency=source_query_fetch_concurrency(
            getattr(run, "sync", None)
        ),
    )
    return {
        "available": True,
        "message": message,
        "total_stage_steps": len(steps),
        "fetch_mode_counts": fetch_mode_counts,
        "fallback_step_count": fallback_step_count,
        "fallback_reasons": sorted(set(fallback_reasons)),
        "fallback_reason_summary": reason_summary,
        "partition_retry_summary": retry_summary,
        "slow_models": slow_models,
        "efficiency": efficiency,
        "runtime_share": runtime_share,
        "diff_utilization": diff_utilization,
        "diff_baseline_transition": diff_transition,
        "tuning_guidance": tuning_guidance,
        "alert_thresholds": alert_thresholds,
        "trend_snapshots": recent_pushdown_trend_snapshots(run),
    }


def _pushdown_alert_thresholds_for_run(run):
    if run is None:
        return source_pushdown_alert_thresholds(None)
    sync = getattr(run, "sync", None)
    return source_pushdown_alert_thresholds(sync)


def compatibility_cache_summary(sync, run=None):
    parameters = sync.parameters or {}
    compatibility_state = parameters.get(BRANCH_RUN_STATE_PARAMETER)
    compatibility_key_present = BRANCH_RUN_STATE_PARAMETER in parameters
    compatibility_state_is_dict = isinstance(compatibility_state, dict)
    compatibility_state_size = (
        len(compatibility_state.keys()) if compatibility_state_is_dict else 0
    )
    compatibility_state_keys = (
        sorted(str(key) for key in compatibility_state.keys())[:10]
        if compatibility_state_is_dict
        else []
    )

    latest_run = run if run is not None else latest_execution_run(sync)
    active_run = active_execution_run(sync)
    has_ledger_history = latest_run is not None
    stale_payload_present = bool(
        has_ledger_history and active_run is None and compatibility_key_present
    )
    prune_recommended = bool(stale_payload_present)

    if stale_payload_present:
        message = (
            "Legacy compatibility branch state is still persisted even though ledger "
            "history exists; runtime ignores it and prune is recommended."
        )
    elif has_ledger_history and not compatibility_key_present:
        message = (
            "Compatibility branch-state cache is retired for this sync; execution "
            "state is ledger-only."
        )
    elif has_ledger_history:
        message = (
            "Ledger history exists and compatibility writes are suppressed during "
            "active execution."
        )
    elif compatibility_key_present:
        message = (
            "This sync still uses pre-ledger compatibility branch state; create a "
            "new execution run to migrate to ledger-first control."
        )
    else:
        message = (
            "No compatibility branch-state payload is stored yet and no execution "
            "run history exists."
        )

    return {
        "message": message,
        "ledger_history": bool(has_ledger_history),
        "active_execution_run": bool(active_run is not None),
        "compatibility_state_present": bool(compatibility_key_present),
        "compatibility_state_size": compatibility_state_size,
        "compatibility_state_keys": compatibility_state_keys,
        "writes_suppressed": bool(has_ledger_history),
        "stale_payload_present": stale_payload_present,
        "prune_recommended": prune_recommended,
    }


def density_learning_summary(sync):
    return density_profile_summary(
        density_map=sync.get_model_change_density(),
        density_profile=sync.get_model_change_density_profile(),
        default_density_map=DEFAULT_MODEL_CHANGE_DENSITY,
    )
