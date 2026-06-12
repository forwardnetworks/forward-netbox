from collections import Counter
from types import SimpleNamespace

from ..choices import ForwardExecutionRunStatusChoices
from ..choices import ForwardExecutionStepStatusChoices
from .api_usage import evaluate_forward_api_usage
from .branch_budget import BRANCH_RUN_STATE_PARAMETER
from .change_explainability import change_explainability_summary
from .execution_ledger_metrics import apply_engine_decision
from .execution_ledger_metrics import execution_run_metrics
from .execution_ledger_metrics import fetch_explanation
from .execution_ledger_metrics import job_summary
from .plugin_integrations import integration_capability_summary


def execution_run_support_bundle(run, *, recommendation_fn):
    if run is None:
        return {}
    steps = run.steps.order_by("index", "kind")
    step_list = list(steps)
    latest_ingestion = getattr(getattr(run, "sync", None), "last_ingestion", None)
    latest_ingestion_summary = ingestion_support_summary(latest_ingestion)
    sync_health = {}
    live_diagnostics = {}
    sync = getattr(run, "sync", None)
    if sync is not None:
        from .health import sync_health_summary

        sync_health = sync_health_summary(sync)
        live_diagnostics = live_support_diagnostics(sync, sync_health=sync_health)
    dependency_lookup_cache = dependency_lookup_cache_support_summary(run)
    dependency_parent_coverage = dependency_parent_coverage_support_summary(run)
    api_usage = api_usage_support_summary(run)
    recovery_recommendation = recommendation_fn(run)
    metrics = execution_run_metrics(run, step_list)
    failure_summary = execution_run_failure_summary(run, step_list)
    diagnosis_summary = support_bundle_diagnosis_summary(
        run=run,
        sync_health=sync_health,
        latest_ingestion_summary=latest_ingestion_summary,
        live_diagnostics=live_diagnostics,
        dependency_lookup_cache=dependency_lookup_cache,
        dependency_parent_coverage=dependency_parent_coverage,
        api_usage=api_usage,
        recovery_recommendation=recovery_recommendation,
        metrics=metrics,
        failure_summary=failure_summary,
    )
    return {
        "run": run.as_support_summary(),
        "run_job": job_summary(run.job),
        "health": sync_health,
        "latest_ingestion": latest_ingestion_summary,
        "optional_plugin_capabilities": integration_capability_summary(),
        "analysis_summary": (
            latest_ingestion_summary.get("analysis_summary", {})
            if isinstance(latest_ingestion_summary, dict)
            else {}
        ),
        "query_path_resolution": (
            latest_ingestion_summary.get("query_path_resolution", {})
            if isinstance(latest_ingestion_summary, dict)
            else {}
        ),
        "query_modes": (
            latest_ingestion_summary.get("query_modes", {})
            if isinstance(latest_ingestion_summary, dict)
            else {}
        ),
        "query_drift_summary": sync_health.get("query_drift_summary", {}),
        "query_drift_results": (
            sync_health.get("query_modes", {}).get("local_drift", [])
            if isinstance(sync_health, dict)
            else []
        ),
        "dependency_lookup_cache": dependency_lookup_cache,
        "dependency_parent_coverage": dependency_parent_coverage,
        "compatibility_cache": _compatibility_cache_evidence(run),
        "api_usage": api_usage,
        "insights_summary": execution_run_insights_summary(run),
        "live_diagnostics": live_diagnostics,
        "diagnosis_summary": diagnosis_summary,
        "recovery_recommendation": recovery_recommendation,
        "recovery_policy_summary": _recovery_policy_summary(run),
        "metrics": metrics,
        "failure_summary": failure_summary,
        "steps": [
            {
                **step.as_support_summary(),
                "fetch_explanation": fetch_explanation(step),
                "apply_engine_decision": apply_engine_decision(step),
                "job_detail": job_summary(step.job),
                "merge_job_detail": job_summary(step.merge_job),
                "ingestion_detail": ingestion_support_summary(step.ingestion),
            }
            for step in step_list
        ],
    }


def support_bundle_diagnosis_summary(
    *,
    run,
    sync_health,
    latest_ingestion_summary,
    live_diagnostics,
    dependency_lookup_cache,
    dependency_parent_coverage,
    api_usage,
    recovery_recommendation,
    metrics,
    failure_summary,
):
    signals = []
    if (failure_summary or {}).get("available"):
        signals.append(
            _diagnosis_signal(
                "failed_step",
                "danger",
                (failure_summary or {}).get("message") or "Execution step failed.",
                action="inspect_failed_step",
                evidence={
                    "model": (failure_summary or {}).get("model", ""),
                    "step_index": (failure_summary or {}).get("step_index"),
                    "status": (failure_summary or {}).get("status", ""),
                    "query_id": (failure_summary or {}).get("query_id", ""),
                    "query_path": (failure_summary or {}).get("query_path", ""),
                },
            )
        )

    recovery_action = str((recovery_recommendation or {}).get("action") or "").strip()
    if recovery_action and recovery_action not in {
        "complete",
        "monitor",
        "none",
        "wait",
    }:
        signals.append(
            _diagnosis_signal(
                "recovery_action",
                _diagnosis_severity(
                    (recovery_recommendation or {}).get("severity") or "warning"
                ),
                (recovery_recommendation or {}).get("message")
                or "Recovery action is available.",
                action=recovery_action,
                evidence={
                    "step_index": (recovery_recommendation or {}).get("step_index"),
                    "step_status": (recovery_recommendation or {}).get("step_status"),
                },
            )
        )

    local_query_actions = (
        ((sync_health or {}).get("query_drift_summary") or {}).get(
            "remediation_action_codes"
        )
        or ((sync_health or {}).get("query_drift_summary") or {}).get(
            "remediation_actions"
        )
        or []
    )
    live_query_summary = ((live_diagnostics or {}).get("query_drift") or {}).get(
        "live_summary"
    ) or {}
    refresh_count = int(live_query_summary.get("refresh_query_ids_count") or 0)
    if local_query_actions or refresh_count:
        signals.append(
            _diagnosis_signal(
                "query_governance",
                "warning",
                "Query binding drift or query ID refresh evidence is present.",
                action="refresh_query_ids",
                evidence={
                    "local_remediation_actions": local_query_actions,
                    "live_refresh_query_ids_count": refresh_count,
                    "live_warn_count": int(live_query_summary.get("warn_count") or 0),
                },
            )
        )

    if (dependency_parent_coverage or {}).get("available") and int(
        (dependency_parent_coverage or {}).get("missing_parent_count") or 0
    ):
        signals.append(
            _diagnosis_signal(
                "dependency_parent_coverage",
                "warning",
                "Some child rows had missing parent evidence before apply.",
                action="run_dependency_dry_run",
                evidence={
                    "missing_parent_count": int(
                        (dependency_parent_coverage or {}).get("missing_parent_count")
                        or 0
                    ),
                    "blocked_row_count": int(
                        (dependency_parent_coverage or {}).get("blocked_row_count") or 0
                    ),
                    "model_count": int(
                        (dependency_parent_coverage or {}).get("model_count") or 0
                    ),
                },
            )
        )

    api_budget = (api_usage or {}).get("budget") or {}
    if (api_usage or {}).get("available") and api_budget.get("status") == "failed":
        signals.append(
            _diagnosis_signal(
                "forward_api_budget",
                "danger",
                "Forward API usage exceeded the configured safety budget.",
                action="reduce_api_pressure",
                evidence={
                    "failure_reasons": api_budget.get("failure_reasons") or [],
                    "warnings": api_budget.get("warnings") or [],
                },
            )
        )

    issue_count = int((latest_ingestion_summary or {}).get("issue_count") or 0)
    if issue_count:
        issue_models = sorted(
            {
                str(issue.get("model") or "")
                for issue in (latest_ingestion_summary or {}).get("issues") or []
                if issue.get("model")
            }
        )
        signals.append(
            _diagnosis_signal(
                "ingestion_issues",
                "warning",
                f"Latest ingestion recorded {issue_count} issue(s).",
                action="inspect_ingestion_issues",
                evidence={"issue_count": issue_count, "models": issue_models[:10]},
            )
        )

    status = "healthy"
    severity = "info"
    if any(signal["severity"] == "danger" for signal in signals):
        status = "action_required"
        severity = "danger"
    elif signals:
        status = "review_recommended"
        severity = "warning"
    elif not (metrics or {}).get("available", True) and not (sync_health or {}):
        status = "insufficient_evidence"
        severity = "info"
    return {
        "available": True,
        "status": status,
        "severity": severity,
        "signal_count": len(signals),
        "primary_action": signals[0]["action"] if signals else "none",
        "message": (
            signals[0]["message"]
            if signals
            else "No support-bundle diagnosis signals require action."
        ),
        "signals": signals,
    }


def _diagnosis_signal(code, severity, message, *, action, evidence):
    return {
        "code": code,
        "severity": _diagnosis_severity(severity),
        "message": str(message or "").strip(),
        "action": str(action or "").strip(),
        "evidence": evidence or {},
    }


def _diagnosis_severity(value):
    value = str(value or "").strip().lower()
    if value in {"danger", "error", "fail", "failed"}:
        return "danger"
    if value in {"warning", "warn"}:
        return "warning"
    return "info"


def live_support_diagnostics(sync, *, sync_health=None):
    if sync is None or getattr(sync, "source", None) is None:
        return {
            "available": False,
            "message": "No sync source is available for live diagnostics.",
        }
    try:
        client = sync.source.get_client()
    except Exception as exc:
        return {
            "available": False,
            "message": f"Could not construct a Forward client for live diagnostics: {exc}",
        }

    from .health import live_data_file_health_check
    from .health import live_source_health_check
    from .query_binding import live_query_binding_drift

    maps = [
        query_map
        for query_map in sync.get_maps()
        if sync.is_model_enabled(query_map.model_string)
    ]
    if sync_health is None:
        from .health import sync_health_summary

        sync_health = sync_health_summary(sync)
    source_health = _safe_live_diagnostic(live_source_health_check, sync)
    data_file_health = _safe_live_diagnostic(live_data_file_health_check, sync)
    query_drift_results = []
    query_drift_error = ""
    try:
        query_drift_results = [
            live_query_binding_drift(client=client, query_map=query_map)
            for query_map in maps
        ]
    except Exception as exc:
        query_drift_error = str(exc)

    return {
        "available": True,
        "source_health": source_health,
        "query_drift": {
            "available": True,
            "summary": sync_health.get("query_drift_summary", {}),
            "live_summary": _live_query_drift_summary(
                query_drift_results,
                query_drift_error=query_drift_error,
            ),
            "results": query_drift_results,
            "error": query_drift_error,
        },
        "data_file_health": data_file_health,
        "enabled_map_count": len(maps),
    }


def _live_query_drift_summary(query_drift_results, *, query_drift_error=""):
    results = list(query_drift_results or [])
    severity_counts = Counter(
        str(item.get("severity") or "").strip() or "unknown" for item in results
    )
    status_counts = Counter(
        str(item.get("live_status") or item.get("status") or "").strip() or "unknown"
        for item in results
    )
    query_id_results = [item for item in results if item.get("mode") == "query_id"]
    query_id_status_counts = Counter(
        str(item.get("live_status") or item.get("status") or "").strip() or "unknown"
        for item in query_id_results
    )
    remediation_action_counts = Counter(
        str(item.get("remediation_action") or "").strip()
        for item in results
        if item.get("severity") != "pass" and item.get("remediation_action")
    )
    return {
        "total_maps": len(results),
        "checked_maps": sum(1 for item in results if item.get("live_checked")),
        "pass_count": int(severity_counts.get("pass", 0)),
        "warn_count": int(severity_counts.get("warn", 0)),
        "info_count": int(severity_counts.get("info", 0)),
        "status_counts": dict(status_counts),
        "query_id_total": len(query_id_results),
        "query_id_pass_count": int(
            sum(1 for item in query_id_results if item.get("severity") == "pass")
        ),
        "query_id_warn_count": int(
            sum(1 for item in query_id_results if item.get("severity") == "warn")
        ),
        "query_id_info_count": int(
            sum(1 for item in query_id_results if item.get("severity") == "info")
        ),
        "query_id_not_found_count": int(
            query_id_status_counts.get("direct_query_id_not_found", 0)
        ),
        "query_id_ambiguous_count": int(
            query_id_status_counts.get("direct_query_id_ambiguous", 0)
        ),
        "query_id_modified_count": int(
            query_id_status_counts.get("live_repository_source_modified", 0)
        ),
        "query_id_unavailable_count": int(
            query_id_status_counts.get("live_repository_source_unavailable", 0)
        ),
        "lookup_error_count": int(query_id_status_counts.get("live_lookup_failed", 0)),
        "remediation_action_counts": dict(sorted(remediation_action_counts.items())),
        "refresh_query_ids_count": int(
            remediation_action_counts.get("refresh_query_ids", 0)
        ),
        "error": str(query_drift_error or "").strip(),
    }


def _safe_live_diagnostic(func, sync):
    try:
        return func(sync)
    except Exception as exc:
        return {
            "available": False,
            "error": str(exc),
        }


def execution_run_insights_summary(run):
    if run is None:
        return {
            "available": False,
            "message": "No execution run is available.",
        }
    api_usage = api_usage_support_summary(run)
    latest_ingestion = ingestion_support_summary(
        getattr(getattr(run, "sync", None), "last_ingestion", None)
    )
    query_modes = (
        latest_ingestion.get("query_modes", {})
        if isinstance(latest_ingestion, dict)
        else {}
    )
    if not api_usage.get("available") and not query_modes.get("available"):
        return {
            "available": False,
            "message": "No execution telemetry is available.",
        }
    budget = api_usage.get("budget", {}) if isinstance(api_usage, dict) else {}
    metrics = budget.get("metrics", {}) if isinstance(budget, dict) else {}
    return {
        "available": True,
        "budget_status": budget.get("status", "") if isinstance(budget, dict) else "",
        "budget_failure_reasons": (
            budget.get("failure_reasons", []) if isinstance(budget, dict) else []
        ),
        "budget_warnings": (
            budget.get("warnings", []) if isinstance(budget, dict) else []
        ),
        "http_attempts": int(metrics.get("http_attempts") or 0),
        "http_429_failures": int(metrics.get("http_429_failures") or 0),
        "nqe_query_calls": int(metrics.get("nqe_query_calls") or 0),
        "nqe_diff_calls": int(metrics.get("nqe_diff_calls") or 0),
        "nqe_pages": int(metrics.get("nqe_pages") or 0),
        "throttle_sleep_seconds": float(metrics.get("throttle_sleep_seconds") or 0.0),
        "observed_http_attempts_per_minute": metrics.get(
            "observed_http_attempts_per_minute"
        ),
        "headroom_requests_per_minute": metrics.get("headroom_requests_per_minute"),
        "execution_mode_counts": list(
            (query_modes.get("execution_modes") or {}).items()
        ),
        "fetch_mode_counts": list((query_modes.get("fetch_modes") or {}).items()),
        "top_model_results": (query_modes.get("top_model_results") or [])[:3],
    }


def execution_run_failure_summary(run, step_list=None):
    if run is None:
        return {
            "available": False,
            "severity": "info",
            "message": "No execution run is available.",
        }
    steps = (
        list(step_list)
        if step_list is not None
        else list(run.steps.order_by("index", "kind", "pk"))
    )
    failed_steps = [
        step
        for step in steps
        if step.status
        in {
            ForwardExecutionStepStatusChoices.FAILED,
            ForwardExecutionStepStatusChoices.TIMEOUT,
            ForwardExecutionStepStatusChoices.MERGE_TIMEOUT,
        }
    ]
    step = failed_steps[0] if failed_steps else None
    if step is None and not (run.last_error or "").strip():
        return {"available": False, "severity": "info", "message": ""}
    error = ""
    if step is not None:
        error = (step.last_error or "").strip()
    if not error:
        error = (run.last_error or "").strip()
    if not error:
        error = (run.phase_message or "").strip()
    if not error:
        error = "The execution run failed, but no error text is available."
    step_label = ""
    if step is not None:
        step_label = " ".join(
            value
            for value in [
                f"Shard {step.index}",
                step.model_string or "",
                step.query_name or "",
            ]
            if value
        ).strip()
    execution_mode = getattr(step, "execution_mode", "") or ""
    execution_value = getattr(step, "execution_value", "") or ""
    query_id = execution_value if execution_mode == "query_id" else ""
    query_path = execution_value if execution_mode == "query_path" else ""
    message = f"{step_label} failed." if step_label else "The execution run failed."
    summary = {
        "available": True,
        "severity": (
            "danger"
            if run.status
            in {
                ForwardExecutionRunStatusChoices.FAILED,
                ForwardExecutionRunStatusChoices.TIMEOUT,
            }
            else "warning"
        ),
        "message": message,
        "error": error,
        "step_pk": getattr(step, "pk", None),
        "step_index": getattr(step, "index", None),
        "model": getattr(step, "model_string", "") or "",
        "query_name": getattr(step, "query_name", "") or "",
        "execution_mode": execution_mode,
        "execution_value": execution_value,
        "query_id": query_id,
        "query_path": query_path,
        "status": getattr(step, "status", "") or "",
        "run_status": run.status or "",
    }
    return summary


API_USAGE_COUNTER_KEYS = (
    "api_requests_per_minute",
    "http_attempts",
    "http_successes",
    "http_failures",
    "http_timeout_failures",
    "http_transport_failures",
    "http_status_failures",
    "http_429_failures",
    "http_retries",
    "http_status_classes",
    "throttle_sleep_seconds",
    "usage_window_seconds",
    "observed_http_attempts_per_minute",
    "nqe_query_calls",
    "nqe_diff_calls",
    "nqe_pages",
    "nqe_query_pages",
    "nqe_diff_pages",
    "read_cache_hits",
    "read_cache_misses",
    "read_cache_hit_rate",
)


API_USAGE_QUERY_PARAMETER_STEP_LIMIT = 10


def api_usage_support_summary(run):
    job = getattr(run, "job", None)
    job_data = getattr(job, "data", None) if job is not None else None
    if not isinstance(job_data, dict):
        return {
            "available": False,
            "reason": "run_job_data_missing",
            "source": "run_job_data.forward_api_usage",
        }
    raw_summary = job_data.get("forward_api_usage")
    if not isinstance(raw_summary, dict):
        return {
            "available": False,
            "reason": "forward_api_usage_missing",
            "source": "run_job_data.forward_api_usage",
        }

    counters = {
        key: raw_summary[key] for key in API_USAGE_COUNTER_KEYS if key in raw_summary
    }
    budget = raw_summary.get("budget")
    if not isinstance(budget, dict):
        budget = evaluate_forward_api_usage(
            counters,
            source_type=getattr(getattr(run, "source", None), "type", None),
        )
    return {
        "available": True,
        "source": "run_job_data.forward_api_usage",
        "counters": counters,
        "budget": budget,
        "step_query_parameters": _api_usage_step_query_parameter_summary(run),
    }


def dependency_lookup_cache_support_summary(run):
    job = getattr(run, "job", None)
    job_data = getattr(job, "data", None) if job is not None else None
    if not isinstance(job_data, dict):
        return {
            "available": False,
            "reason": "run_job_data_missing",
            "source": "run_job_data.dependency_lookup_cache",
        }
    raw_summary = job_data.get("dependency_lookup_cache")
    if not isinstance(raw_summary, dict):
        return {
            "available": False,
            "reason": "dependency_lookup_cache_missing",
            "source": "run_job_data.dependency_lookup_cache",
        }
    models = raw_summary.get("models")
    if not isinstance(models, list):
        models = []
    return {
        "available": True,
        "source": "run_job_data.dependency_lookup_cache",
        "row_count": int(raw_summary.get("row_count") or 0),
        "primed_target_count": int(raw_summary.get("primed_target_count") or 0),
        "model_count": int(raw_summary.get("model_count") or len(models)),
        "models": models,
    }


def dependency_parent_coverage_support_summary(run):
    job = getattr(run, "job", None)
    job_data = getattr(job, "data", None) if job is not None else None
    if not isinstance(job_data, dict):
        return {
            "available": False,
            "reason": "run_job_data_missing",
            "source": "run_job_data.dependency_parent_coverage",
        }
    raw_summary = job_data.get("dependency_parent_coverage")
    if not isinstance(raw_summary, dict):
        return {
            "available": False,
            "reason": "dependency_parent_coverage_missing",
            "source": "run_job_data.dependency_parent_coverage",
        }
    models = raw_summary.get("models")
    if not isinstance(models, list):
        models = []
    return {
        "available": True,
        "source": "run_job_data.dependency_parent_coverage",
        "row_count": int(raw_summary.get("row_count") or 0),
        "blocked_row_count": int(raw_summary.get("blocked_row_count") or 0),
        "missing_parent_count": int(raw_summary.get("missing_parent_count") or 0),
        "model_count": int(raw_summary.get("model_count") or len(models)),
        "models": models,
    }


def _api_usage_step_query_parameter_summary(run):
    steps = (
        run.steps.order_by("index", "kind")
        if getattr(run, "steps", None) is not None
        else []
    )
    step_items = []
    matching_step_count = 0
    for step in steps:
        query_parameters = dict(step.query_parameters or {})
        if not query_parameters:
            continue
        matching_step_count += 1
        step_items.append(
            {
                "model": step.model_string or "",
                "query_name": step.query_name or "",
                "execution_mode": step.execution_mode or "",
                "fetch_mode": step.fetch_mode or "",
                "query_parameters": query_parameters,
            }
        )
    step_items = step_items[:API_USAGE_QUERY_PARAMETER_STEP_LIMIT]
    return {
        "available": bool(step_items),
        "step_count": matching_step_count,
        "total_step_count": (
            steps.count() if hasattr(steps, "count") else len(step_items)
        ),
        "top_steps": step_items,
    }


def _recovery_policy_summary(run):
    events = (
        run.reconciliation_events if isinstance(run.reconciliation_events, list) else []
    )
    auto_policy_reasons = {
        "failed_stage_with_live_job_auto_restore",
        "queued_stage_without_job_auto_reset",
        "stale_queued_without_branch_auto_reset",
        "stale_stage_without_branch_auto_requeue",
    }
    escalation_reasons = {
        "stale_stage_with_branch",
        "stale_merge_job",
    }
    watchdog_reason = "stale_run_no_progress_watchdog"
    escalation_threshold = 3
    watchdog_threshold = 2
    policy_events = [
        event
        for event in events
        if isinstance(event, dict) and event.get("reason") in auto_policy_reasons
    ]
    escalation_events = [
        event
        for event in events
        if isinstance(event, dict) and event.get("reason") in escalation_reasons
    ]
    watchdog_events = [
        event
        for event in events
        if isinstance(event, dict) and event.get("reason") == watchdog_reason
    ]
    by_reason = {}
    for event in policy_events:
        reason = str(event.get("reason") or "unknown")
        by_reason[reason] = int(by_reason.get(reason, 0)) + 1
    escalation_by_reason = {}
    for event in escalation_events:
        reason = str(event.get("reason") or "unknown")
        escalation_by_reason[reason] = int(escalation_by_reason.get(reason, 0)) + 1
    latest_event = policy_events[-1] if policy_events else None
    escalation_required = any(
        count >= escalation_threshold for count in escalation_by_reason.values()
    )
    watchdog_required = len(watchdog_events) >= watchdog_threshold
    return {
        "auto_policy_event_count": len(policy_events),
        "auto_policy_reasons": by_reason,
        "last_auto_policy_event": latest_event,
        "escalation_event_count": len(escalation_events),
        "escalation_reasons": escalation_by_reason,
        "escalation_threshold": escalation_threshold,
        "escalation_required": escalation_required,
        "watchdog_event_count": len(watchdog_events),
        "watchdog_reason": watchdog_reason,
        "watchdog_threshold": watchdog_threshold,
        "watchdog_required": watchdog_required,
    }


def ingestion_support_summary(ingestion):
    if ingestion is None:
        return None
    issues = list(ingestion.issues.order_by("timestamp")[:25])
    execution_summary = ingestion.get_execution_summary()
    return {
        "id": ingestion.pk,
        "name": ingestion.name,
        "sync_mode": ingestion.sync_mode or "",
        "baseline_ready": bool(ingestion.baseline_ready),
        "snapshot_selector": ingestion.snapshot_selector or "",
        "snapshot_id": ingestion.snapshot_id or "",
        "branch": ingestion.branch_id,
        "branch_name": ingestion.branch.name if ingestion.branch else "",
        "applied_change_count": int(ingestion.applied_change_count or 0),
        "failed_change_count": int(ingestion.failed_change_count or 0),
        "created_change_count": int(ingestion.created_change_count or 0),
        "updated_change_count": int(ingestion.updated_change_count or 0),
        "deleted_change_count": int(ingestion.deleted_change_count or 0),
        "change_explainability": change_explainability_summary(ingestion),
        "execution_summary": execution_summary,
        "query_modes": execution_summary.get("query_modes", {}),
        "query_path_resolution": execution_summary.get("query_path_resolution", {}),
        "dependency_lookup_cache": dependency_lookup_cache_support_summary(
            SimpleNamespace(job=ingestion.job)
        ),
        "dependency_parent_coverage": dependency_parent_coverage_support_summary(
            SimpleNamespace(job=ingestion.job)
        ),
        "analysis_summary": ingestion.get_analysis_summary(),
        "issue_count": ingestion.issues.count(),
        "issues": [
            {
                "id": issue.pk,
                "timestamp": issue.timestamp.isoformat() if issue.timestamp else None,
                "phase": issue.phase,
                "model": issue.model or "",
                "message": issue.message,
                "exception": issue.exception or "",
            }
            for issue in issues
        ],
    }


def _compatibility_cache_evidence(run):
    sync = run.sync
    parameters = sync.parameters or {}
    compatibility_state = parameters.get(BRANCH_RUN_STATE_PARAMETER)
    compatibility_present = BRANCH_RUN_STATE_PARAMETER in parameters
    compatibility_is_dict = isinstance(compatibility_state, dict)
    compatibility_size = len(compatibility_state) if compatibility_is_dict else 0
    compatibility_keys = (
        sorted(str(key) for key in compatibility_state.keys())[:10]
        if compatibility_is_dict
        else []
    )
    compatibility_execution_run_id = None
    if compatibility_is_dict:
        compatibility_execution_run_id = compatibility_state.get("execution_run_id")

    terminal_statuses = {
        ForwardExecutionRunStatusChoices.COMPLETED,
        ForwardExecutionRunStatusChoices.FAILED,
        ForwardExecutionRunStatusChoices.TIMEOUT,
        ForwardExecutionRunStatusChoices.CANCELLED,
    }
    active_run = (
        sync.execution_runs.exclude(status__in=terminal_statuses)
        .order_by("-pk")
        .first()
    )
    latest_run = sync.execution_runs.order_by("-pk").first()
    stale_payload_present = bool(
        latest_run is not None and active_run is None and compatibility_present
    )

    return {
        "ledger_history": bool(latest_run is not None),
        "active_execution_run": bool(active_run is not None),
        "active_execution_run_id": active_run.pk if active_run else None,
        "compatibility_state_present": bool(compatibility_present),
        "compatibility_state_size": compatibility_size,
        "compatibility_state_keys": compatibility_keys,
        "compatibility_execution_run_id": compatibility_execution_run_id,
        "stale_payload_present": stale_payload_present,
        "prune_recommended": bool(stale_payload_present),
    }
