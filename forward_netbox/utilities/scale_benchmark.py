DEFAULT_SCALE_BENCHMARK_THRESHOLDS = {
    "fallback_warn_rate": 0.25,
    "fallback_fail_rate": 0.50,
    "fallback_runtime_warn_share": 0.50,
    "failed_row_warn_count": 1,
    "failed_row_fail_rate": 0.01,
    "diff_warn_ratio": 0.80,
    "wait_warn_share": 0.25,
}

_SEVERITY_RANK = {
    "pass": 0,
    "info": 1,
    "warn": 2,
    "fail": 3,
}


def scale_benchmark_report(bundle, *, thresholds=None):
    """Summarize scale-readiness evidence from an execution support bundle."""
    thresholds = _merged_thresholds(thresholds)
    bundle = bundle or {}
    run = bundle.get("run") or {}
    metrics = bundle.get("metrics") or {}
    steps = bundle.get("steps") or []
    checks = [
        _support_bundle_shape_check(bundle),
        _run_completion_check(run, steps),
        _row_failure_check(metrics, thresholds=thresholds),
        _pushdown_efficiency_check(metrics, thresholds=thresholds),
        _pushdown_runtime_check(metrics, thresholds=thresholds),
        _diff_utilization_check(metrics, thresholds=thresholds),
        _diff_baseline_transition_check(metrics),
        _partition_retry_check(metrics),
        _throughput_smoothing_check(metrics, thresholds=thresholds),
        _apply_engine_check(metrics),
    ]
    overall_status = _overall_status(checks)
    return {
        "status": overall_status,
        "message": _overall_message(overall_status),
        "thresholds": thresholds,
        "run": {
            "id": run.get("id"),
            "backend": run.get("backend", ""),
            "status": run.get("status", ""),
            "total_steps": run.get("total_steps"),
            "next_step_index": run.get("next_step_index"),
            "baseline_ready": bool(run.get("baseline_ready")),
        },
        "summary": {
            "step_count": int(metrics.get("step_count") or len(steps) or 0),
            "estimated_changes": int(metrics.get("estimated_changes") or 0),
            "actual_changes": int(metrics.get("actual_changes") or 0),
            "fetched_row_count": int(metrics.get("fetched_row_count") or 0),
            "attempted_row_count": int(metrics.get("attempted_row_count") or 0),
            "applied_row_count": int(metrics.get("applied_row_count") or 0),
            "skipped_row_count": int(metrics.get("skipped_row_count") or 0),
            "failed_row_count": int(metrics.get("failed_row_count") or 0),
            "retry_count": int(metrics.get("retry_count") or 0),
            "query_runtime_ms": _optional_float(metrics.get("query_runtime_ms")),
            "fetch_modes": list(metrics.get("fetch_modes") or []),
            "apply_engines": list(metrics.get("apply_engines") or []),
        },
        "checks": checks,
        "first_order_actions": _first_order_actions(metrics, checks),
    }


def _merged_thresholds(thresholds):
    merged = dict(DEFAULT_SCALE_BENCHMARK_THRESHOLDS)
    for key, value in (thresholds or {}).items():
        if key not in merged:
            continue
        try:
            merged[key] = float(value)
        except (TypeError, ValueError):
            continue
    return merged


def _support_bundle_shape_check(bundle):
    missing = [
        key
        for key in ("run", "metrics", "steps")
        if key not in (bundle or {}) or bundle.get(key) in (None, "")
    ]
    status = "fail" if missing else "pass"
    return {
        "code": "support_bundle_shape",
        "status": status,
        "message": (
            f"Support bundle is missing required section(s): {', '.join(missing)}."
            if missing
            else "Support bundle includes run, metrics, and step evidence."
        ),
        "evidence": {"missing_sections": missing},
    }


def _run_completion_check(run, steps=None):
    status_value = str((run or {}).get("status") or "").strip()
    total_steps = int((run or {}).get("total_steps") or 0)
    next_step_index = int((run or {}).get("next_step_index") or 0)
    non_terminal_steps = [
        {
            "index": step.get("index"),
            "status": step.get("status"),
        }
        for step in (steps or [])
        if str(step.get("status") or "").strip()
        not in {"merged", "skipped", "failed", "discarded"}
    ]
    if status_value == "completed":
        if non_terminal_steps:
            status = "fail"
            message = "Execution run is marked completed but has non-terminal steps."
        elif total_steps and next_step_index and next_step_index < total_steps:
            status = "fail"
            message = (
                "Execution run is marked completed but next step index is lower "
                "than total steps."
            )
        else:
            status = "pass"
            message = "Execution run completed."
    elif status_value in {"failed", "timeout", "cancelled"}:
        status = "fail"
        message = f"Execution run ended with terminal status `{status_value}`."
    elif status_value:
        status = "warn"
        message = f"Execution run is not complete yet (`{status_value}`)."
    else:
        status = "warn"
        message = "Execution run status is missing."
    return {
        "code": "run_completion",
        "status": status,
        "message": message,
        "evidence": {
            "run_status": status_value,
            "total_steps": total_steps,
            "next_step_index": next_step_index,
            "non_terminal_step_count": len(non_terminal_steps),
            "sample_non_terminal_steps": non_terminal_steps[:10],
        },
    }


def _row_failure_check(metrics, *, thresholds):
    attempted = int((metrics or {}).get("attempted_row_count") or 0)
    failed = int((metrics or {}).get("failed_row_count") or 0)
    failed_rate = round(failed / float(attempted), 4) if attempted else 0.0
    if failed and failed_rate >= float(thresholds["failed_row_fail_rate"]):
        status = "fail"
        message = "Row failure rate exceeds the scale benchmark fail threshold."
    elif failed >= int(thresholds["failed_row_warn_count"]):
        status = "warn"
        message = "Some rows failed; inspect row issues before release."
    else:
        status = "pass"
        message = "No row failures were reported."
    return {
        "code": "row_failures",
        "status": status,
        "message": message,
        "evidence": {
            "attempted_row_count": attempted,
            "failed_row_count": failed,
            "failed_row_rate": failed_rate,
        },
    }


def _pushdown_efficiency_check(metrics, *, thresholds):
    efficiency = (metrics or {}).get("pushdown_efficiency") or {}
    fallback_rate = _optional_float(efficiency.get("fallback_rate"))
    fallback_steps = int(efficiency.get("fallback_steps") or 0)
    total_steps = int(efficiency.get("total_steps") or 0)
    if fallback_rate is not None and fallback_rate >= float(
        thresholds["fallback_fail_rate"]
    ):
        status = "fail"
        message = "Fallback fetch rate exceeds the scale benchmark fail threshold."
    elif fallback_rate is not None and fallback_rate >= float(
        thresholds["fallback_warn_rate"]
    ):
        status = "warn"
        message = "Fallback fetch rate is high enough to affect scale results."
    elif fallback_steps:
        status = "info"
        message = "Fallback fetch occurred but is below warning threshold."
    else:
        status = "pass"
        message = "No fallback fetch steps were reported."
    return {
        "code": "pushdown_efficiency",
        "status": status,
        "message": message,
        "evidence": {
            "fallback_steps": fallback_steps,
            "total_steps": total_steps,
            "fallback_rate": fallback_rate,
            "hotspot_models": list(efficiency.get("hotspot_models") or []),
            "model_fallback_guardrails": list(
                efficiency.get("model_fallback_guardrails") or []
            ),
        },
    }


def _pushdown_runtime_check(metrics, *, thresholds):
    runtime = (metrics or {}).get("pushdown_runtime") or {}
    fallback_share = _optional_float(runtime.get("fallback_runtime_share"))
    if fallback_share is None:
        status = "info"
        message = "No query runtime share evidence is available."
    elif fallback_share >= float(thresholds["fallback_runtime_warn_share"]):
        status = "warn"
        message = "Fallback fetch accounts for a large share of query runtime."
    elif fallback_share > 0:
        status = "info"
        message = "Fallback fetch accounts for part of query runtime."
    else:
        status = "pass"
        message = "No fallback query runtime share was reported."
    return {
        "code": "pushdown_runtime",
        "status": status,
        "message": message,
        "evidence": {
            "fallback_runtime_share": fallback_share,
            "full_fallback_runtime_share": _optional_float(
                runtime.get("full_fallback_runtime_share")
            ),
            "fallback_query_runtime_ms": _optional_float(
                runtime.get("fallback_query_runtime_ms")
            ),
            "total_query_runtime_ms": _optional_float(
                runtime.get("total_query_runtime_ms")
            ),
        },
    }


def _diff_utilization_check(metrics, *, thresholds):
    diff = (metrics or {}).get("diff_utilization") or {}
    eligible = int(diff.get("eligible_steps") or 0)
    diff_steps = int(diff.get("diff_steps") or 0)
    ratio = _optional_float(diff.get("diff_actual_ratio"))
    if not eligible:
        status = "info"
        message = "No diff-eligible query-id/query-path steps were reported."
    elif ratio is not None and ratio < float(thresholds["diff_warn_ratio"]):
        status = "warn"
        message = "Diff utilization is below the benchmark warning threshold."
    else:
        status = "pass"
        message = "Diff-eligible steps used API diffs at the expected rate."
    return {
        "code": "diff_utilization",
        "status": status,
        "message": message,
        "evidence": {
            "eligible_steps": eligible,
            "diff_steps": diff_steps,
            "diff_actual_ratio": ratio,
            "non_diff_reason_counts": diff.get("non_diff_reason_counts") or {},
        },
    }


def _diff_baseline_transition_check(metrics):
    transition = (metrics or {}).get("diff_baseline_transition") or {}
    status = str(transition.get("status") or "info")
    code = str(transition.get("code") or "unknown")
    if status not in {"pass", "info", "warn", "fail"}:
        status = "info"
    return {
        "code": "diff_baseline_transition",
        "status": status,
        "message": transition.get("message")
        or "No baseline-to-diff transition evidence is available.",
        "evidence": {
            "transition_code": code,
            "action_code": transition.get("action_code") or "",
            "backend": transition.get("backend") or "",
            "run_baseline_ready": bool(transition.get("run_baseline_ready")),
            "diff_capable_step_count": int(
                transition.get("diff_capable_step_count") or 0
            ),
            "diff_step_count": int(transition.get("diff_step_count") or 0),
            "raw_or_non_diff_capable_step_count": int(
                transition.get("raw_or_non_diff_capable_step_count") or 0
            ),
            "baseline_snapshot_ids": list(
                transition.get("baseline_snapshot_ids") or []
            ),
            "non_diff_reason_counts": transition.get("non_diff_reason_counts") or {},
        },
    }


def _partition_retry_check(metrics):
    retries = (metrics or {}).get("partition_retry_summary") or {}
    retry_count = int(retries.get("split_retry_count") or 0) + int(
        retries.get("alternate_operator_retry_count") or 0
    )
    success_count = int(retries.get("split_retry_success_count") or 0) + int(
        retries.get("alternate_operator_success_count") or 0
    )
    if retry_count and success_count < retry_count:
        status = "warn"
        message = "Some partition retries still failed before fallback handling."
    elif success_count:
        status = "info"
        message = "Partition retries avoided broader fallback for this run."
    else:
        status = "pass"
        message = "No partition retry pressure was reported."
    return {
        "code": "partition_retry_pressure",
        "status": status,
        "message": message,
        "evidence": {
            "retry_count": retry_count,
            "success_count": success_count,
            "avoided_fallback_retry_count": int(
                retries.get("avoided_fallback_retry_count") or 0
            ),
            "by_model": list(retries.get("by_model") or []),
        },
    }


def _throughput_smoothing_check(metrics, *, thresholds):
    throughput = (metrics or {}).get("throughput_smoothing") or {}
    wait_share = _optional_float(throughput.get("wait_share"))
    readiness = throughput.get("scheduler_overlap_readiness") or {}
    if wait_share is None:
        status = "info"
        message = "No throughput timing evidence is available."
    elif wait_share >= float(thresholds["wait_warn_share"]):
        status = "warn"
        message = "Queue or merge wait is a material share of runtime."
    else:
        status = "pass"
        message = "Queue or merge wait is not a material runtime share."
    return {
        "code": "throughput_smoothing",
        "status": status,
        "message": message,
        "evidence": {
            "wait_share": wait_share,
            "wait_seconds": _optional_float(throughput.get("wait_seconds")),
            "total_observed_seconds": _optional_float(
                throughput.get("total_observed_seconds")
            ),
            "scheduler_overlap_readiness": readiness,
        },
    }


def _apply_engine_check(metrics):
    engines = list((metrics or {}).get("apply_engines") or [])
    step_count = int((metrics or {}).get("step_count") or 0)
    if not engines:
        status = "info"
        message = "No apply-engine evidence is available."
    elif step_count >= 20 and "bulk_orm" not in engines:
        status = "warn"
        message = (
            "Large-run evidence did not exercise the safe bulk ORM apply path; "
            "enable it before using this benchmark as optimized-runtime proof."
        )
    else:
        status = "pass"
        message = "Apply-engine evidence is present."
    return {
        "code": "apply_engine_coverage",
        "status": status,
        "message": message,
        "evidence": {"apply_engines": engines, "step_count": step_count},
    }


def _overall_status(checks):
    if any((check or {}).get("status") == "fail" for check in checks):
        return "fail"
    if any((check or {}).get("status") == "warn" for check in checks):
        return "warn"
    return "pass"


def _overall_message(status):
    if status == "fail":
        return "Scale benchmark found release-blocking evidence."
    if status == "warn":
        return "Scale benchmark found warnings that need review before release."
    return "Scale benchmark evidence is within configured thresholds."


def _first_order_actions(metrics, checks):
    actions = []
    guidance = list((metrics or {}).get("operator_tuning_summary") or [])
    for item in guidance[:5]:
        if isinstance(item, dict):
            actions.append(item)
    if actions:
        return actions
    for check in checks:
        if (check or {}).get("status") not in {"warn", "fail"}:
            continue
        actions.append(
            {
                "code": f"review_{check.get('code')}",
                "severity": check.get("status"),
                "message": check.get("message"),
            }
        )
    return actions


def _optional_float(value):
    if value is None:
        return None
    try:
        return round(float(value), 4)
    except (TypeError, ValueError):
        return None
