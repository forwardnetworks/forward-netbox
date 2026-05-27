from django.conf import settings

from .. import NetboxForwardConfig
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
from .runtime_guidance import configured_rq_default_timeout
from .runtime_guidance import source_pushdown_alert_thresholds
from .runtime_guidance import source_query_fetch_concurrency
from .runtime_guidance import source_timeout_seconds


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
        "created": ingestion.created.isoformat() if ingestion.created else None,
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


def large_run_tuning_summary(sync, *, capacity, query_pushdown):
    capacity = capacity or {}
    query_pushdown = query_pushdown or {}
    runtime = runtime_summary(sync)
    efficiency = query_pushdown.get("efficiency") or {}
    runtime_share = query_pushdown.get("runtime_share") or {}
    diff_utilization = query_pushdown.get("diff_utilization") or {}
    guidance = list(query_pushdown.get("tuning_guidance") or [])
    backend_advice = execution_backend_advice(
        runtime=runtime,
        capacity=capacity,
        efficiency=efficiency,
        diff_utilization=diff_utilization,
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
        "signals": {
            "execution_backend": runtime.get("execution_backend"),
            "fallback_rate": efficiency.get("fallback_rate"),
            "fallback_runtime_share": runtime_share.get("fallback_runtime_share"),
            "diff_actual_ratio": diff_utilization.get("diff_actual_ratio"),
            "projected_remaining_seconds": projected_remaining,
        },
        "first_order_actions": actions[:6],
    }


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
