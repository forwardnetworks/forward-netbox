from .apply_engine import apply_engine_decision_summary
from .branch_budget import DEFAULT_MODEL_CHANGE_DENSITY
from .density_learning import density_profile_summary
from .runtime_guidance import source_pushdown_alert_thresholds
from .runtime_guidance import source_query_fetch_concurrency


def job_summary(job):
    if job is None:
        return None
    return {
        "pk": job.pk,
        "status": getattr(job, "status", ""),
        "created": getattr(job, "created", None),
        "started": getattr(job, "started", None),
        "completed": getattr(job, "completed", None),
        "duration": getattr(job, "duration", None),
        "data": getattr(job, "data", {}) or {},
        "log_entries": list(getattr(job, "log_entries", []) or []),
    }


def execution_run_metrics(run, steps):
    alert_thresholds = _resolve_pushdown_alert_thresholds(run)
    query_runtime_ms = sum_optional_float(step.query_runtime_ms for step in steps)
    fetch_mode_counts_by_model = {}
    query_runtime_ms_by_model = {}
    fetched_row_count_by_model = {}
    for step in steps:
        model = str(step.model_string or "").strip()
        if not model:
            continue
        mode = str(step.fetch_mode or "model")
        counts = fetch_mode_counts_by_model.setdefault(model, {})
        counts[mode] = int(counts.get(mode, 0)) + 1
        if step.query_runtime_ms is not None:
            query_runtime_ms_by_model[model] = round(
                float(query_runtime_ms_by_model.get(model, 0.0))
                + float(step.query_runtime_ms),
                3,
            )
        fetched_row_count_by_model[model] = int(
            fetched_row_count_by_model.get(model, 0)
        ) + int(step.fetched_row_count or 0)

    step_metrics = [
        {
            "index": step.index,
            "kind": step.kind,
            "model": step.model_string,
            "status": step.status,
            "estimated_changes": int(step.estimated_changes or 0),
            "actual_changes": int(step.actual_changes or 0),
            "fetched_row_count": int(step.fetched_row_count or 0),
            "query_runtime_ms": step.query_runtime_ms,
            "attempted_row_count": int(step.attempted_row_count or 0),
            "applied_row_count": int(step.applied_row_count or 0),
            "skipped_row_count": int(step.skipped_row_count or 0),
            "failed_row_count": int(step.failed_row_count or 0),
            "retry_count": int(step.retry_count or 0),
            "fetch_mode": step.fetch_mode,
            "fetch_explanation": fetch_explanation(step),
            "apply_engine": step.apply_engine,
            "apply_engine_decision": apply_engine_decision(step),
            "stage_duration_seconds": duration_seconds(
                step.started,
                step.completed,
            ),
            "stage_queue_seconds": stage_queue_seconds(step),
            "merge_duration_seconds": duration_seconds(
                getattr(step.merge_job, "started", None),
                getattr(step.merge_job, "completed", None),
            ),
            "merge_queue_seconds": duration_seconds(
                getattr(step.merge_job, "created", None),
                getattr(step.merge_job, "started", None),
            ),
            "merge_wait_seconds": merge_wait_seconds(step),
        }
        for step in steps
    ]
    throughput = throughput_smoothing_summary(step_metrics)
    pushdown_runtime = pushdown_runtime_summary(
        steps, alert_thresholds=alert_thresholds
    )
    diff_utilization = diff_utilization_summary(
        steps, alert_thresholds=alert_thresholds
    )
    diff_transition = diff_baseline_transition_summary(
        run,
        steps,
        diff_utilization=diff_utilization,
    )
    pushdown_efficiency = pushdown_efficiency_summary(
        fetch_mode_counts_by_model=fetch_mode_counts_by_model,
        alert_thresholds=alert_thresholds,
    )
    partition_retries = partition_retry_summary(steps)
    tuning_guidance = pushdown_tuning_guidance(
        efficiency=pushdown_efficiency,
        runtime_share=pushdown_runtime,
        diff_utilization=diff_utilization,
        partition_retries=partition_retries,
        throughput=throughput,
        alert_thresholds=alert_thresholds,
        query_fetch_concurrency=source_query_fetch_concurrency(
            getattr(run, "sync", None)
        ),
    )
    sync = getattr(run, "sync", None)
    model_change_density = (
        sync.get_model_change_density()
        if sync is not None and hasattr(sync, "get_model_change_density")
        else {}
    )
    model_change_density_profile = (
        sync.get_model_change_density_profile()
        if sync is not None and hasattr(sync, "get_model_change_density_profile")
        else {}
    )
    bottleneck = runtime_bottleneck(
        step_metrics,
        query_runtime_ms,
        throughput=throughput,
    )
    return {
        "total_runtime_seconds": duration_seconds(run.created, run.completed),
        "step_count": len(steps),
        "estimated_changes": sum(int(step.estimated_changes or 0) for step in steps),
        "actual_changes": sum(int(step.actual_changes or 0) for step in steps),
        "fetched_row_count": sum(int(step.fetched_row_count or 0) for step in steps),
        "query_runtime_ms": query_runtime_ms,
        "attempted_row_count": sum(
            int(step.attempted_row_count or 0) for step in steps
        ),
        "applied_row_count": sum(int(step.applied_row_count or 0) for step in steps),
        "skipped_row_count": sum(int(step.skipped_row_count or 0) for step in steps),
        "failed_row_count": sum(int(step.failed_row_count or 0) for step in steps),
        "retry_count": sum(int(step.retry_count or 0) for step in steps),
        "fetch_modes": sorted({step.fetch_mode for step in steps if step.fetch_mode}),
        "fetch_mode_counts_by_model": fetch_mode_counts_by_model,
        "query_runtime_ms_by_model": query_runtime_ms_by_model,
        "fetched_row_count_by_model": fetched_row_count_by_model,
        "pushdown_efficiency": pushdown_efficiency,
        "pushdown_runtime": pushdown_runtime,
        "fallback_reason_summary": fallback_reason_summary(steps),
        "partition_retry_summary": partition_retries,
        "diff_utilization": diff_utilization,
        "diff_baseline_transition": diff_transition,
        "throughput_smoothing": throughput,
        "tuning_guidance": tuning_guidance,
        "operator_tuning_summary": operator_tuning_summary(
            bottleneck=bottleneck,
            efficiency=pushdown_efficiency,
            runtime_share=pushdown_runtime,
            diff_utilization=diff_utilization,
            throughput=throughput,
            tuning_guidance=tuning_guidance,
            query_fetch_concurrency=source_query_fetch_concurrency(
                getattr(run, "sync", None)
            ),
        ),
        "model_change_density_profile": density_profile_summary(
            density_map=model_change_density,
            density_profile=model_change_density_profile,
            default_density_map=DEFAULT_MODEL_CHANGE_DENSITY,
        ),
        "pushdown_alert_thresholds": alert_thresholds,
        "trend_snapshots": recent_pushdown_trend_snapshots(run),
        "apply_engines": sorted(
            {step.apply_engine for step in steps if step.apply_engine}
        ),
        "bottleneck": bottleneck,
        "steps": step_metrics,
    }


def pushdown_efficiency_summary(*, fetch_mode_counts_by_model, alert_thresholds=None):
    thresholds = _merge_pushdown_alert_thresholds(alert_thresholds)
    fallback_modes = {"model", "full_fallback", "diff_fallback"}
    hotspot_rate_threshold = float(thresholds["fallback_warn_rate"])
    model_guardrail_min_steps = 2
    total_steps = 0
    fallback_steps = 0
    hotspot_models = []
    model_fallback_guardrails = []

    for model, counts in (fetch_mode_counts_by_model or {}).items():
        model_total = int(sum(int(value or 0) for value in counts.values()))
        model_fallback = int(sum(int(counts.get(mode) or 0) for mode in fallback_modes))
        if model_total <= 0:
            continue
        total_steps += model_total
        fallback_steps += model_fallback
        model_fallback_rate = round(model_fallback / float(model_total), 4)
        if model_total >= 2 and model_fallback_rate >= hotspot_rate_threshold:
            hotspot_models.append(
                {
                    "model": model,
                    "fallback_steps": model_fallback,
                    "total_steps": model_total,
                    "fallback_rate": model_fallback_rate,
                }
            )
        if (
            model_total >= model_guardrail_min_steps
            and model_fallback_rate >= hotspot_rate_threshold
        ):
            model_fallback_guardrails.append(
                {
                    "model": model,
                    "fallback_steps": model_fallback,
                    "total_steps": model_total,
                    "fallback_rate": model_fallback_rate,
                    "fallback_budget_threshold": hotspot_rate_threshold,
                }
            )

    if total_steps <= 0:
        return {
            "status": "info",
            "message": "No stage steps are available for pushdown efficiency scoring.",
            "total_steps": 0,
            "fallback_steps": 0,
            "fallback_rate": None,
            "pushdown_rate": None,
            "hotspot_models": [],
            "model_fallback_guardrails": [],
            "fallback_budget_exceeded_count": 0,
            "fallback_budget_threshold": hotspot_rate_threshold,
            "fallback_budget_min_steps": model_guardrail_min_steps,
        }

    fallback_rate = round(fallback_steps / float(total_steps), 4)
    pushdown_rate = round(1.0 - fallback_rate, 4)
    hotspot_models = sorted(
        hotspot_models,
        key=lambda item: (
            -float(item["fallback_rate"]),
            -int(item["fallback_steps"]),
            str(item["model"]),
        ),
    )[:5]
    model_fallback_guardrails = sorted(
        model_fallback_guardrails,
        key=lambda item: (
            -float(item["fallback_rate"]),
            -int(item["fallback_steps"]),
            str(item["model"]),
        ),
    )[:10]

    if fallback_rate >= float(thresholds["fallback_warn_rate"]) and total_steps >= 4:
        status = "warn"
        message = (
            f"Fallback fetch modes were used on {fallback_steps}/{total_steps} "
            "stage step(s); pushdown efficiency is degraded."
        )
    elif model_fallback_guardrails:
        status = "warn"
        message = (
            "Per-model fallback budget guardrail exceeded for "
            f"{len(model_fallback_guardrails)} model(s)."
        )
    elif fallback_steps > 0:
        status = "info"
        message = (
            f"Fallback fetch modes were used on {fallback_steps}/{total_steps} "
            "stage step(s)."
        )
    else:
        status = "pass"
        message = "All stage steps used shard-aware pushdown fetch modes."

    return {
        "status": status,
        "message": message,
        "total_steps": total_steps,
        "fallback_steps": fallback_steps,
        "fallback_rate": fallback_rate,
        "pushdown_rate": pushdown_rate,
        "hotspot_models": hotspot_models,
        "model_fallback_guardrails": model_fallback_guardrails,
        "fallback_budget_exceeded_count": len(model_fallback_guardrails),
        "fallback_budget_threshold": hotspot_rate_threshold,
        "fallback_budget_min_steps": model_guardrail_min_steps,
    }


def pushdown_runtime_summary(steps, *, alert_thresholds=None):
    thresholds = _merge_pushdown_alert_thresholds(alert_thresholds)
    fallback_modes = {"model", "full_fallback", "diff_fallback"}
    full_fallback_modes = {"full_fallback"}
    query_runtime_ms = sum_optional_float(step.query_runtime_ms for step in steps)
    if not query_runtime_ms:
        return {
            "status": "info",
            "message": "No query runtime is available for pushdown runtime analysis.",
            "total_query_runtime_ms": 0.0,
            "fallback_query_runtime_ms": 0.0,
            "full_fallback_query_runtime_ms": 0.0,
            "fallback_runtime_share": None,
            "full_fallback_runtime_share": None,
        }

    fallback_query_runtime_ms = sum(
        float(step.query_runtime_ms or 0.0)
        for step in steps
        if str(step.fetch_mode or "model") in fallback_modes
    )
    full_fallback_query_runtime_ms = sum(
        float(step.query_runtime_ms or 0.0)
        for step in steps
        if str(step.fetch_mode or "model") in full_fallback_modes
    )
    fallback_runtime_share = round(
        fallback_query_runtime_ms / float(query_runtime_ms),
        4,
    )
    full_fallback_runtime_share = round(
        full_fallback_query_runtime_ms / float(query_runtime_ms),
        4,
    )

    if fallback_runtime_share >= float(thresholds["runtime_fallback_warn_share"]):
        status = "warn"
        message = "Fallback fetch modes account for most query runtime in this run."
    elif fallback_runtime_share > 0:
        status = "info"
        message = "Fallback fetch modes account for part of query runtime."
    else:
        status = "pass"
        message = "Query runtime is fully shard-aware with no fallback share."

    return {
        "status": status,
        "message": message,
        "total_query_runtime_ms": round(float(query_runtime_ms), 3),
        "fallback_query_runtime_ms": round(float(fallback_query_runtime_ms), 3),
        "full_fallback_query_runtime_ms": round(
            float(full_fallback_query_runtime_ms),
            3,
        ),
        "fallback_runtime_share": fallback_runtime_share,
        "full_fallback_runtime_share": full_fallback_runtime_share,
    }


def fallback_reason_summary(steps):
    fallback_modes = {"model", "full_fallback", "diff_fallback"}
    by_reason = {}
    by_model = {}
    total = 0
    for step in steps or []:
        mode = str(getattr(step, "fetch_mode", "") or "model")
        if mode not in fallback_modes:
            continue
        reason = str(
            (getattr(step, "fetch_parameters", None) or {}).get("fallback_reason")
            or _default_fallback_reason_for_mode(mode)
        ).strip()
        if not reason:
            reason = "unknown_fallback_reason"
        model = str(getattr(step, "model_string", "") or "unknown")
        total += 1
        by_reason[reason] = int(by_reason.get(reason, 0)) + 1
        model_reasons = by_model.setdefault(model, {})
        model_reasons[reason] = int(model_reasons.get(reason, 0)) + 1

    top_reasons = [
        {
            "reason": reason,
            "count": count,
            "remediation": _fallback_reason_remediation(reason),
        }
        for reason, count in sorted(
            by_reason.items(),
            key=lambda item: (-int(item[1]), str(item[0])),
        )[:10]
    ]
    model_items = []
    for model, reasons in by_model.items():
        count = sum(int(value) for value in reasons.values())
        model_items.append(
            {
                "model": model,
                "count": count,
                "reasons": [
                    {
                        "reason": reason,
                        "count": reason_count,
                        "remediation": _fallback_reason_remediation(reason),
                    }
                    for reason, reason_count in sorted(
                        reasons.items(),
                        key=lambda item: (-int(item[1]), str(item[0])),
                    )[:5]
                ],
            }
        )
    model_items = sorted(
        model_items,
        key=lambda item: (-int(item["count"]), str(item["model"])),
    )[:10]
    return {
        "total_fallback_steps": total,
        "top_reasons": top_reasons,
        "by_model": model_items,
        "remediation_actions": _fallback_remediation_actions(by_reason),
    }


def partition_retry_summary(steps):
    totals = {
        "partition_retry_step_count": 0,
        "split_retry_count": 0,
        "split_retry_success_count": 0,
        "alternate_operator_retry_count": 0,
        "alternate_operator_success_count": 0,
    }
    by_model = {}
    by_operation = {}

    for step in steps or []:
        retry = (getattr(step, "fetch_parameters", None) or {}).get(
            "partition_retry_summary"
        ) or {}
        if not retry:
            continue
        split_retry_count = int(retry.get("split_retry_count") or 0)
        split_success_count = int(retry.get("split_retry_success_count") or 0)
        alternate_retry_count = int(retry.get("alternate_operator_retry_count") or 0)
        alternate_success_count = int(
            retry.get("alternate_operator_success_count") or 0
        )
        if not (
            split_retry_count
            or split_success_count
            or alternate_retry_count
            or alternate_success_count
        ):
            continue

        totals["partition_retry_step_count"] += 1
        totals["split_retry_count"] += split_retry_count
        totals["split_retry_success_count"] += split_success_count
        totals["alternate_operator_retry_count"] += alternate_retry_count
        totals["alternate_operator_success_count"] += alternate_success_count

        model = str(getattr(step, "model_string", "") or "unknown")
        operation = str(retry.get("operation") or "unknown")
        model_stats = by_model.setdefault(model, _empty_partition_retry_stats(model))
        operation_stats = by_operation.setdefault(
            operation,
            _empty_partition_retry_stats(operation),
        )
        for target in (model_stats, operation_stats):
            target["partition_retry_step_count"] += 1
            target["split_retry_count"] += split_retry_count
            target["split_retry_success_count"] += split_success_count
            target["alternate_operator_retry_count"] += alternate_retry_count
            target["alternate_operator_success_count"] += alternate_success_count

    avoided_fallback_count = int(totals["split_retry_success_count"]) + int(
        totals["alternate_operator_success_count"]
    )
    status = "pass" if avoided_fallback_count else "info"
    message = (
        f"Shard partition retries avoided broader fallback {avoided_fallback_count} time(s)."
        if avoided_fallback_count
        else "No shard partition retry evidence is available for this run."
    )
    return {
        "status": status,
        "message": message,
        "avoided_fallback_retry_count": avoided_fallback_count,
        **totals,
        "by_model": sorted(
            by_model.values(),
            key=lambda item: (
                -int(item["partition_retry_step_count"]),
                str(item["name"]),
            ),
        ),
        "by_operation": sorted(
            by_operation.values(),
            key=lambda item: (
                -int(item["partition_retry_step_count"]),
                str(item["name"]),
            ),
        ),
    }


def _empty_partition_retry_stats(name):
    return {
        "name": str(name or "unknown"),
        "partition_retry_step_count": 0,
        "split_retry_count": 0,
        "split_retry_success_count": 0,
        "alternate_operator_retry_count": 0,
        "alternate_operator_success_count": 0,
    }


def _fallback_remediation_actions(by_reason):
    actions_by_code = {}
    for reason, count in (by_reason or {}).items():
        remediation = _fallback_reason_remediation(reason)
        code = remediation["code"]
        item = actions_by_code.setdefault(
            code,
            {
                "code": code,
                "layer": remediation["layer"],
                "message": remediation["message"],
                "suggestions": remediation["suggestions"],
                "count": 0,
                "reasons": [],
            },
        )
        item["count"] += int(count or 0)
        item["reasons"].append({"reason": reason, "count": int(count or 0)})
    return sorted(
        actions_by_code.values(),
        key=lambda item: (-int(item["count"]), str(item["code"])),
    )


def _fallback_reason_remediation(reason):
    reason_text = str(reason or "").strip()
    reason_lower = reason_text.lower()
    if reason_text == "model_fetch_contract_fallback":
        return {
            "code": "add_or_enable_shard_fetch_contract",
            "layer": "planner_query_contract",
            "message": "The model used the full model fetch contract.",
            "suggestions": [
                "Confirm the model has a shard-safe fetch contract.",
                "Add deterministic NQE column-filter support only if row shape stays unchanged.",
            ],
        }
    if reason_text == "shard_pushdown_failed_full_fallback":
        return {
            "code": "repair_shard_pushdown_runtime",
            "layer": "forward_query_execution",
            "message": "Shard-scoped full-query fetch failed and fell back.",
            "suggestions": [
                "Inspect the original fallback exception in step fetch parameters.",
                "Reduce partition size or fix the NQE/filter shape before tuning branch fanout.",
            ],
        }
    if reason_text == "diff_pushdown_failed_full_fallback":
        return {
            "code": "repair_diff_pushdown_runtime",
            "layer": "forward_diff_execution",
            "message": "Diff shard fetch failed and fell back to a broader query.",
            "suggestions": [
                "Verify query_id/query_path maps can execute diffs for the selected baseline.",
                "Inspect diff fallback exceptions before judging Branching runtime.",
            ],
        }
    if "timeout" in reason_lower or "timed out" in reason_lower:
        return {
            "code": "reduce_query_timeout_pressure",
            "layer": "forward_api_runtime",
            "message": "Fallback appears related to query timeout pressure.",
            "suggestions": [
                "Use shard partition split evidence to identify oversized filter partitions.",
                "Tune query fetch concurrency only after reducing repeated timeout fallbacks.",
            ],
        }
    if "parameter" in reason_lower or "unsupported" in reason_lower:
        return {
            "code": "repair_query_parameter_contract",
            "layer": "nqe_contract",
            "message": "Fallback appears related to query parameter compatibility.",
            "suggestions": [
                "Keep NQE as the source of row semantics and update the query contract.",
                "Avoid Python-side row mutation to compensate for unsupported query parameters.",
            ],
        }
    return {
        "code": "inspect_fallback_exception",
        "layer": "unknown",
        "message": "Fallback reason needs manual classification.",
        "suggestions": [
            "Inspect step fetch parameters and model/query identity in the support bundle.",
            "Classify the cause before changing NQE, concurrency, or branch budgets.",
        ],
    }


def _default_fallback_reason_for_mode(mode):
    if mode == "model":
        return "model_fetch_contract_fallback"
    if mode == "full_fallback":
        return "shard_pushdown_failed_full_fallback"
    if mode == "diff_fallback":
        return "diff_pushdown_failed_full_fallback"
    return "unknown_fallback_reason"


def diff_utilization_summary(steps, *, alert_thresholds=None):
    thresholds = _merge_pushdown_alert_thresholds(alert_thresholds)
    stage_steps = [step for step in steps if str(step.kind or "") == "stage"]
    if not stage_steps:
        return {
            "status": "info",
            "message": "No stage steps are available for diff-utilization scoring.",
            "eligible_steps": 0,
            "diff_steps": 0,
            "diff_actual_ratio": None,
        }

    eligible_modes = {"query_id", "query_path"}
    eligible_steps = [
        step for step in stage_steps if str(step.execution_mode or "") in eligible_modes
    ]
    diff_steps = [
        step for step in eligible_steps if str(step.sync_mode or "") == "diff"
    ]
    non_diff_steps = [
        step for step in eligible_steps if str(step.sync_mode or "") != "diff"
    ]
    eligible_count = len(eligible_steps)
    diff_count = len(diff_steps)
    diff_ratio = (
        round(diff_count / float(eligible_count), 4) if eligible_count else None
    )
    non_diff_reason_counts = _diff_non_diff_reason_counts(non_diff_steps)

    if eligible_count == 0:
        status = "info"
        message = "No diff-eligible stage steps were detected (query_id/query_path)."
    elif diff_count == eligible_count:
        status = "pass"
        message = "All diff-eligible stage steps executed in diff mode."
    elif diff_ratio is not None and diff_ratio <= float(thresholds["diff_warn_ratio"]):
        status = "warn"
        message = (
            "Diff utilization for eligible stage steps is at or below the "
            "configured warning threshold."
        )
    else:
        status = "info"
        message = "Some diff-eligible stage steps executed in diff mode."

    return {
        "status": status,
        "message": message,
        "eligible_steps": eligible_count,
        "diff_steps": diff_count,
        "diff_actual_ratio": diff_ratio,
        "non_diff_reason_counts": non_diff_reason_counts,
    }


def diff_baseline_transition_summary(run, steps, *, diff_utilization=None):
    stage_steps = [step for step in steps or [] if str(step.kind or "") == "stage"]
    diff_utilization = diff_utilization or diff_utilization_summary(stage_steps)
    eligible_modes = {"query_id", "query_path"}
    eligible_steps = [
        step for step in stage_steps if str(step.execution_mode or "") in eligible_modes
    ]
    diff_steps = [
        step for step in eligible_steps if str(step.sync_mode or "") == "diff"
    ]
    raw_or_non_diff_capable_steps = [
        step
        for step in stage_steps
        if str(step.execution_mode or "") not in eligible_modes
    ]
    baseline_snapshot_ids = sorted(
        {
            str(step.baseline_snapshot_id or "").strip()
            for step in stage_steps
            if str(step.baseline_snapshot_id or "").strip()
        }
    )
    reason_counts = dict(diff_utilization.get("non_diff_reason_counts") or {})
    backend = str(getattr(run, "backend", "") or "").strip()

    if not stage_steps:
        status = "info"
        code = "no_stage_steps"
        action_code = "collect_stage_evidence"
        message = "No stage steps are available to assess baseline-to-diff behavior."
    elif backend == "fast_bootstrap":
        status = "info"
        code = "fast_bootstrap_baseline_lane"
        action_code = "complete_bootstrap_then_branching"
        message = (
            "Fast bootstrap is the baseline lane; switch to Branching with "
            "query_id/query_path maps on a later snapshot for API diffs."
        )
    elif not eligible_steps:
        status = "warn"
        code = "no_diff_capable_query_identity"
        action_code = "use_query_id_or_query_path_maps"
        message = (
            "No stage steps used query_id/query_path identity, so API diffs cannot "
            "be used for this run."
        )
    elif len(diff_steps) == len(eligible_steps):
        status = "pass"
        code = "api_diff_active"
        action_code = "keep_query_identity_and_baseline"
        message = "All diff-capable stage steps used API diffs."
    elif reason_counts.get("diff_request_failed_fallback_to_full"):
        status = "warn"
        code = "diff_requested_but_fell_back"
        action_code = "inspect_diff_fallback_exception"
        message = (
            "API diffs were eligible, but at least one diff request fell back to "
            "full execution."
        )
    elif reason_counts.get("baseline_present_but_full_mode"):
        status = "warn"
        code = "baseline_present_but_full_mode"
        action_code = "inspect_baseline_and_query_identity"
        message = (
            "A baseline was recorded on at least one step, but the step still ran "
            "in full mode."
        )
    elif reason_counts.get("missing_or_ineligible_diff_baseline"):
        status = "warn"
        code = "missing_or_ineligible_diff_baseline"
        action_code = "complete_baseline_then_use_newer_snapshot"
        message = (
            "Diff-capable query identity exists, but no eligible prior baseline was "
            "available for at least one step."
        )
    else:
        status = "info"
        code = "partial_or_mixed_diff_transition"
        action_code = "review_non_diff_reason_counts"
        message = "Some diff-capable stage steps did not execute in diff mode."

    return {
        "status": status,
        "code": code,
        "action_code": action_code,
        "message": message,
        "backend": backend,
        "run_baseline_ready": bool(getattr(run, "baseline_ready", False)),
        "snapshot_id": str(getattr(run, "snapshot_id", "") or ""),
        "stage_step_count": len(stage_steps),
        "diff_capable_step_count": len(eligible_steps),
        "diff_step_count": len(diff_steps),
        "raw_or_non_diff_capable_step_count": len(raw_or_non_diff_capable_steps),
        "baseline_snapshot_ids": baseline_snapshot_ids,
        "non_diff_reason_counts": reason_counts,
    }


def recent_pushdown_trend_snapshots(run, *, limit=8):
    if run is None or not getattr(run, "sync_id", None):
        return []
    runs = run.__class__.objects.filter(sync_id=run.sync_id).order_by("-pk")[
        : max(1, int(limit))
    ]
    snapshots = []
    for trend_run in runs:
        alert_thresholds = _resolve_pushdown_alert_thresholds(trend_run)
        stage_steps = list(trend_run.steps.filter(kind="stage"))
        fetch_mode_counts_by_model = {}
        for step in stage_steps:
            model = str(step.model_string or "").strip() or "unknown"
            mode = str(step.fetch_mode or "model")
            model_counts = fetch_mode_counts_by_model.setdefault(model, {})
            model_counts[mode] = int(model_counts.get(mode, 0)) + 1
        efficiency = pushdown_efficiency_summary(
            fetch_mode_counts_by_model=fetch_mode_counts_by_model,
            alert_thresholds=alert_thresholds,
        )
        runtime = pushdown_runtime_summary(
            stage_steps, alert_thresholds=alert_thresholds
        )
        diff = diff_utilization_summary(stage_steps, alert_thresholds=alert_thresholds)
        snapshots.append(
            {
                "run_id": trend_run.pk,
                "status": trend_run.status,
                "created": trend_run.created.isoformat() if trend_run.created else None,
                "snapshot_id": trend_run.snapshot_id or "",
                "efficiency_status": efficiency.get("status"),
                "fallback_rate": efficiency.get("fallback_rate"),
                "pushdown_rate": efficiency.get("pushdown_rate"),
                "full_fallback_runtime_share": runtime.get(
                    "full_fallback_runtime_share"
                ),
                "fallback_runtime_share": runtime.get("fallback_runtime_share"),
                "diff_actual_ratio": diff.get("diff_actual_ratio"),
                "eligible_steps": diff.get("eligible_steps"),
                "diff_steps": diff.get("diff_steps"),
                "non_diff_reason_counts": diff.get("non_diff_reason_counts") or {},
                "baseline_reason_summary": _format_reason_summary(
                    diff.get("non_diff_reason_counts") or {}
                ),
            }
        )
    return list(reversed(snapshots))


def pushdown_trend_history_for_sync(sync, *, limit=180):
    if sync is None:
        return {
            "available": False,
            "selected_limit": max(1, int(limit or 1)),
            "snapshot_count": 0,
            "latest_run_id": None,
            "trends": [],
        }
    selected_limit = max(1, int(limit or 1))
    latest_run = sync.execution_runs.order_by("-pk").first()
    trends = (
        recent_pushdown_trend_snapshots(latest_run, limit=selected_limit)
        if latest_run is not None
        else []
    )
    return {
        "available": bool(latest_run is not None),
        "selected_limit": selected_limit,
        "snapshot_count": len(trends),
        "latest_run_id": latest_run.pk if latest_run is not None else None,
        "trends": trends,
    }


def pushdown_tuning_guidance(
    *,
    efficiency,
    runtime_share,
    diff_utilization,
    alert_thresholds,
    partition_retries=None,
    throughput=None,
    query_fetch_concurrency=None,
):
    actions = []
    efficiency = efficiency or {}
    runtime_share = runtime_share or {}
    diff_utilization = diff_utilization or {}
    partition_retries = partition_retries or {}
    throughput = throughput or {}
    thresholds = _merge_pushdown_alert_thresholds(alert_thresholds)

    fallback_steps = int(efficiency.get("fallback_steps") or 0)
    total_steps = int(efficiency.get("total_steps") or 0)
    fallback_rate = efficiency.get("fallback_rate")
    hotspot_models = list(efficiency.get("hotspot_models") or [])
    model_guardrails = list(efficiency.get("model_fallback_guardrails") or [])
    if total_steps > 0 and fallback_steps > 0:
        hotspot_labels = ", ".join(
            item.get("model", "unknown") for item in hotspot_models
        )
        severity = (
            "warn"
            if fallback_rate is not None
            and float(fallback_rate) >= float(thresholds["fallback_warn_rate"])
            else "info"
        )
        message = (
            f"Fallback fetch appears on {fallback_steps}/{total_steps} stage steps "
            f"(rate {fallback_rate})."
        )
        if hotspot_labels:
            message = f"{message} Hotspot models: {hotspot_labels}."
        actions.append(
            {
                "code": "fallback_pushdown_coverage",
                "severity": severity,
                "message": message,
                "suggestions": [
                    "Prioritize shard-safe fetch contracts for hotspot models first.",
                    "Use query_id/query_path map modes for diff-capable models.",
                    "Inspect fallback reasons in support bundle before tuning concurrency.",
                ],
            }
        )
    if model_guardrails:
        guardrail_models = ", ".join(
            item.get("model", "unknown") for item in model_guardrails
        )
        actions.append(
            {
                "code": "model_fallback_budget_guardrail",
                "severity": "warn",
                "message": (
                    "Per-model fallback budget threshold is exceeded for: "
                    f"{guardrail_models}."
                ),
                "suggestions": [
                    "Prioritize shard-contract improvements for the flagged models.",
                    "Keep branch fanout conservative until per-model fallback rate drops below budget.",
                ],
            }
        )

    fallback_runtime_share = runtime_share.get("fallback_runtime_share")
    if fallback_runtime_share is not None and float(fallback_runtime_share) > 0:
        severity = (
            "warn"
            if float(fallback_runtime_share)
            >= float(thresholds["runtime_fallback_warn_share"])
            else "info"
        )
        suggestions = [
            "Reduce fallback-heavy models before widening branch fanout.",
            "Use pushdown trend export to confirm runtime-share improvement after query changes.",
        ]
        if query_fetch_concurrency is not None and int(query_fetch_concurrency) >= 10:
            suggestions.append(
                "Lower query fetch concurrency if DB contention increases full-query fallback runtime."
            )
        actions.append(
            {
                "code": "fallback_runtime_pressure",
                "severity": severity,
                "message": (
                    "Fallback fetch runtime share is "
                    f"{fallback_runtime_share} (threshold {thresholds['runtime_fallback_warn_share']})."
                ),
                "suggestions": suggestions,
            }
        )

    retry_attempts = int(partition_retries.get("split_retry_count") or 0) + int(
        partition_retries.get("alternate_operator_retry_count") or 0
    )
    retry_successes = int(
        partition_retries.get("split_retry_success_count") or 0
    ) + int(partition_retries.get("alternate_operator_success_count") or 0)
    avoided_fallback_retries = int(
        partition_retries.get("avoided_fallback_retry_count") or 0
    )
    failed_retry_attempts = max(0, retry_attempts - retry_successes)
    if avoided_fallback_retries > 0:
        actions.append(
            {
                "code": "partition_retry_avoided_fallback",
                "severity": "info",
                "message": (
                    "Shard partition retries avoided broader fallback "
                    f"{avoided_fallback_retries} time(s)."
                ),
                "suggestions": [
                    "Monitor this count as a healthy recovery signal, not as a reason to widen branch budgets.",
                    "If the same model retries often, inspect its NQE shard filter contract before changing capacity.",
                ],
            }
        )
    if failed_retry_attempts > 0:
        severity = (
            "warn"
            if avoided_fallback_retries == 0
            or failed_retry_attempts >= avoided_fallback_retries
            else "info"
        )
        actions.append(
            {
                "code": "partition_retry_pressure",
                "severity": severity,
                "message": (
                    "Shard partition retries had "
                    f"{failed_retry_attempts} failed attempt(s) out of {retry_attempts}."
                ),
                "suggestions": [
                    "Inspect per-step fetch_parameters.partition_retry_summary in the support bundle.",
                    "Reduce repeated fallback by fixing the model's NQE filter contract or Forward query runtime pressure.",
                    "Do not raise branch budgets to compensate for partition retry failures.",
                ],
            }
        )

    diff_ratio = diff_utilization.get("diff_actual_ratio")
    if diff_ratio is not None and float(diff_ratio) < 1.0:
        reason_counts = diff_utilization.get("non_diff_reason_counts") or {}
        reason_summary = _format_reason_summary(reason_counts)
        severity = (
            "warn"
            if float(diff_ratio) <= float(thresholds["diff_warn_ratio"])
            else "info"
        )
        message = (
            f"Diff utilization is {diff_ratio} "
            f"({diff_utilization.get('diff_steps')}/{diff_utilization.get('eligible_steps')} steps)."
        )
        if reason_summary:
            message = f"{message} Non-diff reason summary: {reason_summary}."
        actions.append(
            {
                "code": "diff_utilization_recovery",
                "severity": severity,
                "message": message,
                "suggestions": [
                    "Run against a newer processed snapshot when baseline is not eligible.",
                    "Resolve diff-request fallback failures before judging branch runtime.",
                ],
            }
        )

    wait_share = throughput.get("wait_share")
    if wait_share is not None and float(wait_share) >= 0.25:
        hotspot_models = ", ".join(
            item.get("model", "unknown")
            for item in (throughput.get("hotspot_models") or [])[:3]
        )
        message = (
            f"Queue/merge wait share is {wait_share}; execution throughput is "
            "waiting on scheduling or merge handoff."
        )
        if hotspot_models:
            message = f"{message} Hotspot models: {hotspot_models}."
        actions.append(
            {
                "code": "throughput_wait_pressure",
                "severity": "warn",
                "message": message,
                "suggestions": [
                    "Check worker count and database headroom before increasing concurrency.",
                    "Use throughput_smoothing totals to separate queue delay from merge handoff delay.",
                    "Only add scheduler overlap where ledger dependency order and branch budget stay explicit.",
                ],
            }
        )

    return actions


def operator_tuning_summary(
    *,
    bottleneck,
    efficiency,
    runtime_share,
    diff_utilization,
    throughput,
    tuning_guidance,
    query_fetch_concurrency=None,
):
    efficiency = efficiency or {}
    runtime_share = runtime_share or {}
    diff_utilization = diff_utilization or {}
    throughput = throughput or {}
    bottleneck = bottleneck or {}
    tuning_guidance = list(tuning_guidance or [])

    warnings = [
        item
        for item in tuning_guidance
        if str(item.get("severity") or "").lower() == "warn"
    ]
    infos = [
        item
        for item in tuning_guidance
        if str(item.get("severity") or "").lower() == "info"
    ]
    if warnings:
        status = "warn"
    elif infos:
        status = "info"
    elif bottleneck.get("phase"):
        status = "pass"
    else:
        status = "info"

    actions = []
    diff_ratio = diff_utilization.get("diff_actual_ratio")
    if diff_ratio is not None and float(diff_ratio) < 1.0:
        actions.append(
            {
                "code": "restore_diff_utilization",
                "priority": 1,
                "message": "Restore diff execution before tuning worker or database capacity.",
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

    wait_share = throughput.get("wait_share")
    if wait_share is not None and float(wait_share) >= 0.25:
        actions.append(
            {
                "code": "inspect_worker_db_headroom",
                "priority": 3,
                "message": "Queue or merge wait is material; inspect worker and database headroom before adding concurrency.",
            }
        )

    phase = str(bottleneck.get("phase") or "").strip()
    if phase == "forward_query":
        actions.append(
            {
                "code": "query_bound_run",
                "priority": 4,
                "message": "Forward query runtime is the largest measured phase; prioritize query pushdown and diff coverage.",
            }
        )
    elif phase == "row_apply_or_stage_overhead":
        actions.append(
            {
                "code": "apply_bound_run",
                "priority": 4,
                "message": "NetBox row apply/stage time is the largest measured phase; consider parity-gated apply-engine expansion.",
            }
        )
    elif phase == "branching_merge":
        actions.append(
            {
                "code": "merge_bound_run",
                "priority": 4,
                "message": "Branching merge time is the largest measured phase; keep shard size conservative and inspect merge pressure.",
            }
        )
    elif phase == "queue_or_merge_wait":
        actions.append(
            {
                "code": "scheduler_wait_bound_run",
                "priority": 4,
                "message": "Queue or merge wait is the largest measured phase; tune workers/DB before adding scheduler overlap.",
            }
        )

    if query_fetch_concurrency is not None:
        concurrency = int(query_fetch_concurrency)
        if concurrency <= 2:
            actions.append(
                {
                    "code": "query_fetch_concurrency_low",
                    "priority": 5,
                    "message": "Query fetch concurrency is conservative; increase only if Forward and DB headroom are available.",
                }
            )
        elif concurrency >= 12:
            actions.append(
                {
                    "code": "query_fetch_concurrency_high",
                    "priority": 5,
                    "message": "Query fetch concurrency is high; reduce if fallback runtime or DB contention rises.",
                }
            )

    actions.extend(
        {
            "code": str(item.get("code") or "query_pushdown_guidance"),
            "priority": 6,
            "message": str(item.get("message") or "").strip(),
        }
        for item in tuning_guidance
        if str(item.get("message") or "").strip()
    )
    ordered_actions = sorted(
        actions,
        key=lambda item: (int(item["priority"]), str(item["code"])),
    )
    if ordered_actions:
        message = ordered_actions[0]["message"]
    elif phase:
        message = "No immediate tuning action is indicated by the measured run signals."
    else:
        message = "Tuning summary is unavailable until execution timing data exists."

    return {
        "status": status,
        "message": message,
        "primary_bottleneck": phase or "unknown",
        "query_fetch_concurrency": query_fetch_concurrency,
        "signals": {
            "fallback_rate": efficiency.get("fallback_rate"),
            "fallback_runtime_share": runtime_share.get("fallback_runtime_share"),
            "diff_actual_ratio": diff_ratio,
            "wait_share": wait_share,
        },
        "first_order_actions": ordered_actions[:6],
    }


def throughput_smoothing_summary(step_metrics):
    per_model = {}
    totals = {
        "stage_queue_seconds": 0.0,
        "stage_duration_seconds": 0.0,
        "merge_queue_seconds": 0.0,
        "merge_wait_seconds": 0.0,
        "merge_duration_seconds": 0.0,
    }
    observed = {key: 0 for key in totals}

    for step in step_metrics or []:
        model = str(step.get("model") or "unknown")
        model_stats = per_model.setdefault(
            model,
            {
                "step_count": 0,
                "stage_queue_seconds": 0.0,
                "stage_duration_seconds": 0.0,
                "merge_queue_seconds": 0.0,
                "merge_wait_seconds": 0.0,
                "merge_duration_seconds": 0.0,
            },
        )
        model_stats["step_count"] += 1
        for key in totals:
            value = step.get(key)
            if value is None:
                continue
            seconds = float(value)
            totals[key] = round(totals[key] + seconds, 3)
            observed[key] += 1
            model_stats[key] = round(float(model_stats[key]) + seconds, 3)

    total_observed_seconds = round(sum(totals.values()), 3)
    if total_observed_seconds <= 0:
        return {
            "status": "info",
            "message": "No queue/wait/apply timing data is available yet.",
            "totals": totals,
            "observed_counts": observed,
            "hotspot_models": [],
            "scheduler_overlap_readiness": scheduler_overlap_readiness(
                totals=totals,
                observed=observed,
                wait_share=None,
                hotspot_models=[],
            ),
        }

    wait_total = round(
        float(totals["stage_queue_seconds"])
        + float(totals["merge_queue_seconds"])
        + float(totals["merge_wait_seconds"]),
        3,
    )
    wait_share = round(wait_total / total_observed_seconds, 4)
    status = "warn" if wait_share >= 0.25 else "pass"
    message = (
        "Queue/wait time is a material share of measured execution time."
        if status == "warn"
        else "Queue/wait time is not the dominant measured execution cost."
    )
    hotspot_models = []
    for model, stats in per_model.items():
        model_total = sum(
            float(stats.get(key) or 0.0)
            for key in (
                "stage_queue_seconds",
                "stage_duration_seconds",
                "merge_queue_seconds",
                "merge_wait_seconds",
                "merge_duration_seconds",
            )
        )
        if model_total <= 0:
            continue
        model_wait = round(
            float(stats["stage_queue_seconds"])
            + float(stats["merge_queue_seconds"])
            + float(stats["merge_wait_seconds"]),
            3,
        )
        hotspot_models.append(
            {
                "model": model,
                "step_count": int(stats["step_count"]),
                "wait_seconds": model_wait,
                "total_seconds": round(model_total, 3),
                "wait_share": round(model_wait / model_total, 4),
            }
        )
    hotspot_models = sorted(
        hotspot_models,
        key=lambda item: (
            -float(item["wait_share"]),
            -float(item["wait_seconds"]),
            str(item["model"]),
        ),
    )[:5]
    return {
        "status": status,
        "message": message,
        "totals": totals,
        "observed_counts": observed,
        "wait_seconds": wait_total,
        "total_observed_seconds": total_observed_seconds,
        "wait_share": wait_share,
        "hotspot_models": hotspot_models,
        "scheduler_overlap_readiness": scheduler_overlap_readiness(
            totals=totals,
            observed=observed,
            wait_share=wait_share,
            hotspot_models=hotspot_models,
        ),
    }


def scheduler_overlap_readiness(*, totals, observed, wait_share, hotspot_models):
    if wait_share is None:
        return {
            "status": "insufficient_evidence",
            "ready": False,
            "dominant_wait_component": "",
            "message": (
                "Scheduler overlap is not assessable until queue, stage, and merge "
                "timing evidence exists."
            ),
            "required_before_enablement": [
                "Collect support-bundle throughput_smoothing evidence.",
                "Confirm dependency order and branch-budget state are reconstructable from the ledger.",
            ],
        }

    wait_components = {
        "stage_queue": float((totals or {}).get("stage_queue_seconds") or 0.0),
        "merge_queue": float((totals or {}).get("merge_queue_seconds") or 0.0),
        "merge_wait": float((totals or {}).get("merge_wait_seconds") or 0.0),
    }
    dominant_component = max(
        wait_components,
        key=lambda key: (wait_components[key], key),
    )
    observed_wait_count = sum(
        int((observed or {}).get(key) or 0)
        for key in (
            "stage_queue_seconds",
            "merge_queue_seconds",
            "merge_wait_seconds",
        )
    )
    if float(wait_share) < 0.25:
        return {
            "status": "not_indicated",
            "ready": False,
            "dominant_wait_component": dominant_component,
            "message": (
                "Measured wait share is below the scheduler-overlap threshold; "
                "do not add execution overlap for this run profile."
            ),
            "required_before_enablement": [
                "Keep query pushdown, diff utilization, and apply-engine gates green.",
            ],
        }

    status = "candidate_after_capacity_review"
    ready = observed_wait_count >= 3 and bool(hotspot_models)
    if not ready:
        status = "needs_more_runtime_evidence"
    return {
        "status": status,
        "ready": bool(ready),
        "dominant_wait_component": dominant_component,
        "message": (
            "Queue/merge wait is material; scheduler overlap is a candidate only "
            "after worker and database headroom are checked."
            if ready
            else "Queue/merge wait is material, but more repeated timing evidence is needed before implementing scheduler overlap."
        ),
        "required_before_enablement": [
            "Confirm NetBox worker and database headroom.",
            "Keep branch budget and dependency order enforced by execution-ledger state.",
            "Limit overlap to the next eligible shard; do not add non-ledger side queues.",
        ],
    }


def runtime_bottleneck(step_metrics, query_runtime_ms, *, throughput=None):
    query_seconds = (
        round(float(query_runtime_ms) / 1000.0, 3)
        if query_runtime_ms is not None
        else None
    )
    stage_seconds = sum_optional_float(
        step["stage_duration_seconds"] for step in step_metrics
    )
    merge_seconds = sum_optional_float(
        step["merge_duration_seconds"] for step in step_metrics
    )
    wait_seconds = None
    if throughput:
        wait_seconds = throughput.get("wait_seconds")
    apply_or_stage_seconds = None
    if stage_seconds is not None:
        apply_or_stage_seconds = max(
            0.0,
            round(stage_seconds - float(query_seconds or 0), 3),
        )
    candidates = [
        ("forward_query", query_seconds),
        ("row_apply_or_stage_overhead", apply_or_stage_seconds),
        ("branching_merge", merge_seconds),
        ("queue_or_merge_wait", wait_seconds),
    ]
    measured = [(phase, seconds) for phase, seconds in candidates if seconds]
    if not measured:
        return {
            "phase": "unknown",
            "seconds": None,
            "message": "No completed query, stage, or merge timing data is available.",
        }
    phase, seconds = max(measured, key=lambda item: item[1])
    messages = {
        "forward_query": "Forward NQE query runtime is the largest measured phase.",
        "row_apply_or_stage_overhead": (
            "Row apply or stage overhead is the largest measured phase."
        ),
        "branching_merge": "NetBox Branching merge runtime is the largest measured phase.",
        "queue_or_merge_wait": "Queue or merge wait time is the largest measured phase.",
    }
    return {
        "phase": phase,
        "seconds": seconds,
        "message": messages[phase],
    }


def duration_seconds(started, completed):
    if not started or not completed:
        return None
    try:
        return max(0.0, round((completed - started).total_seconds(), 3))
    except (TypeError, ValueError):
        return None


def sum_optional_float(values):
    numeric_values = [float(value) for value in values if value is not None]
    if not numeric_values:
        return None
    return round(sum(numeric_values), 3)


def stage_queue_seconds(step):
    job = getattr(step, "job", None)
    queued_at = getattr(job, "created", None) or step.created
    started_at = getattr(job, "started", None) or step.started
    return duration_seconds(queued_at, started_at)


def merge_wait_seconds(step):
    merge_job = getattr(step, "merge_job", None)
    merge_started = getattr(merge_job, "started", None)
    return duration_seconds(step.completed, merge_started)


def fetch_explanation(step):
    mode = step.fetch_mode or "model"
    if mode == "nqe_column_filter":
        filter_count = len(step.fetch_column_filters or [])
        key_family = step.fetch_key_family or "shard"
        return (
            "Fetched the shard with native Forward NQE column filters "
            f"for {key_family} keys ({filter_count} filter(s))."
        )
    if mode == "shard":
        key_family = step.fetch_key_family or "shard"
        return f"Fetched the shard with persisted {key_family} shard parameters."
    if mode == "diff_fallback":
        return (
            "Used a diff fallback because the query can run Forward diffs but "
            "the current shard could not be safely pushed down."
        )
    if mode == "full_fallback":
        return (
            "Used a full-query fallback because this model has no safe persisted "
            "shard fetch contract."
        )
    if mode == "model":
        return (
            "Fetched the model result and applied the persisted shard locally; "
            "this model does not have a safe narrower fetch contract yet."
        )
    return f"Fetch mode `{mode}` was recorded for this step."


def apply_engine_decision(step):
    if not step.model_string:
        return {}
    return apply_engine_decision_summary(
        sync=step.run.sync,
        model_string=step.model_string,
        backend=step.run.backend,
    )


def _resolve_pushdown_alert_thresholds(run):
    if run is None:
        return _merge_pushdown_alert_thresholds(None)
    sync = getattr(run, "sync", None)
    if sync is None:
        return _merge_pushdown_alert_thresholds(None)
    return _merge_pushdown_alert_thresholds(source_pushdown_alert_thresholds(sync))


def _merge_pushdown_alert_thresholds(alert_thresholds):
    defaults = {
        "fallback_warn_rate": 0.5,
        "runtime_fallback_warn_share": 0.5,
        "diff_warn_ratio": 0.0,
    }
    values = dict(alert_thresholds or {})
    merged = {}
    for key, default in defaults.items():
        raw = values.get(key, default)
        try:
            merged[key] = max(0.0, min(1.0, float(raw)))
        except (TypeError, ValueError):
            merged[key] = float(default)
    return merged


def _diff_non_diff_reason_counts(non_diff_steps):
    counts = {}
    for step in non_diff_steps:
        reason = _diff_non_diff_reason(step)
        counts[reason] = int(counts.get(reason, 0)) + 1
    return counts


def _diff_non_diff_reason(step):
    sync_mode = str(step.sync_mode or "").strip() or "unknown"
    fetch_mode = str(step.fetch_mode or "model").strip() or "model"
    baseline_snapshot_id = str(step.baseline_snapshot_id or "").strip()

    if fetch_mode == "diff_fallback":
        return "diff_request_failed_fallback_to_full"
    if sync_mode in {"full", "unknown"} and not baseline_snapshot_id:
        return "missing_or_ineligible_diff_baseline"
    if sync_mode in {"full", "unknown"} and baseline_snapshot_id:
        return "baseline_present_but_full_mode"
    return f"sync_mode_{sync_mode}"


def _format_reason_summary(reason_counts):
    if not reason_counts:
        return ""
    ordered = sorted(
        reason_counts.items(),
        key=lambda item: (-int(item[1]), str(item[0])),
    )
    return ", ".join(f"{key}:{value}" for key, value in ordered)
