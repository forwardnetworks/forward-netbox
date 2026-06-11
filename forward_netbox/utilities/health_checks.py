from ..choices import ForwardSourceStatusChoices
from ..choices import ForwardValidationStatusChoices
from .execution_ledger import execution_run_recovery_recommendation
from .ingestion_issues import has_blocking_issues
from .runtime_guidance import configured_rq_default_timeout
from .runtime_guidance import source_query_fetch_concurrency
from .runtime_guidance import source_timeout_seconds


def health_checks(
    *,
    sync,
    maps,
    model_summary,
    query_drift,
    query_drift_summary,
    raw_maps,
    data_file_maps,
    validation_run,
    latest_ingestion,
    execution_run,
    capacity_summary,
    query_pushdown,
    large_run_tuning,
    dependency_preflight,
    delete_wave,
    throughput,
    compatibility_cache,
    next_run,
    branching_available_fn,
):
    checks = [
        check(
            name="Forward source",
            status=(
                "pass"
                if sync.source.status == ForwardSourceStatusChoices.READY
                else "warn"
            ),
            message=(
                "Source is marked ready."
                if sync.source.status == ForwardSourceStatusChoices.READY
                else f"Source status is {sync.source.status}."
            ),
        ),
        check(
            name="Branching plugin",
            status="pass" if branching_available_fn() else "fail",
            message=(
                "NetBox Branching imports successfully."
                if branching_available_fn()
                else "NetBox Branching is not importable."
            ),
        ),
        check(
            name="Enabled NQE maps",
            status="pass" if maps else "fail",
            message=f"{len(maps)} enabled map(s) apply to this sync.",
        ),
        check(
            name="Model map coverage",
            status=(
                "pass" if not model_summary["enabled_models_without_map"] else "warn"
            ),
            message=(
                "Every enabled model has at least one enabled NQE map."
                if not model_summary["enabled_models_without_map"]
                else (
                    f"{len(model_summary['enabled_models_without_map'])} enabled "
                    "model(s) have no enabled NQE map."
                )
            ),
        ),
        check(
            name="Diff eligibility",
            status="pass" if next_run["mode"] == "diff_eligible" else "warn",
            message=next_run["message"],
        ),
        check(
            name="Query binding",
            status="pass" if not raw_maps else "warn",
            message=(
                "All enabled maps use query IDs or repository paths."
                if not raw_maps
                else f"{len(raw_maps)} enabled map(s) use raw query text."
            ),
        ),
        check(
            name="Local query drift",
            status=query_drift_check_status(query_drift),
            message=query_drift_check_message(
                query_drift, query_drift_summary=query_drift_summary
            ),
        ),
        check(
            name="Data-file maps",
            status="info" if data_file_maps else "pass",
            message=(
                f"{len(data_file_maps)} enabled map(s) appear to depend on Forward "
                "NQE data files; run a Forward snapshot after upload before relying "
                "on those rows."
                if data_file_maps
                else "No enabled map advertises a data-file dependency."
            ),
        ),
        check(
            name="Latest validation",
            status=validation_check_status(validation_run),
            message=validation_check_message(validation_run),
        ),
        check(
            name="Latest ingestion",
            status=ingestion_check_status(latest_ingestion),
            message=ingestion_check_message(latest_ingestion),
        ),
        check(
            name="Compatibility cache",
            status=compatibility_cache_check_status(compatibility_cache),
            message=compatibility_cache_check_message(compatibility_cache),
        ),
        check(
            name="Pushdown efficiency",
            status=query_pushdown_check_status(query_pushdown),
            message=query_pushdown_check_message(query_pushdown),
        ),
        check(
            name="Large-run tuning",
            status=large_run_tuning_check_status(large_run_tuning),
            message=large_run_tuning_check_message(large_run_tuning),
        ),
        check(
            name="Adaptive capacity",
            status=adaptive_capacity_check_status(
                (large_run_tuning or {}).get("adaptive_capacity")
            ),
            message=adaptive_capacity_check_message(
                (large_run_tuning or {}).get("adaptive_capacity")
            ),
        ),
        check(
            name="Scoped dependency preflight",
            status=dependency_preflight_check_status(dependency_preflight),
            message=dependency_preflight_check_message(dependency_preflight),
        ),
        check(
            name="Delete wave",
            status=delete_wave_check_status(delete_wave),
            message=delete_wave_check_message(delete_wave),
        ),
        check(
            name="Run throughput",
            status=throughput_check_status(throughput),
            message=throughput_check_message(throughput),
        ),
    ]
    if execution_run is not None:
        recommendation = execution_run_recovery_recommendation(execution_run)
        checks.append(
            check(
                name="Execution recovery",
                status=recommendation_status(recommendation),
                message=recommendation.get("message") or "Monitor the execution run.",
            )
        )
    capacity_check = capacity_check_summary(sync, capacity_summary)
    if capacity_check is not None:
        checks.append(capacity_check)
    timeout = timeout_check(sync)
    if timeout is not None:
        checks.append(timeout)
    query_fetch = query_fetch_concurrency_check(sync)
    if query_fetch is not None:
        checks.append(query_fetch)
    return checks


def compatibility_cache_check_status(compatibility_cache):
    if not compatibility_cache:
        return "info"
    if compatibility_cache.get("stale_payload_present"):
        return "warn"
    if compatibility_cache.get("ledger_history"):
        return "pass"
    if compatibility_cache.get("compatibility_state_present"):
        return "warn"
    return "info"


def compatibility_cache_check_message(compatibility_cache):
    if not compatibility_cache:
        return "Compatibility cache diagnostics are unavailable."
    return str(compatibility_cache.get("message") or "").strip()


def query_fetch_concurrency_check(sync):
    concurrency = source_query_fetch_concurrency(sync)
    if concurrency >= 12:
        return check(
            name="Query fetch concurrency",
            status="warn",
            message=(
                f"Source query fetch concurrency is set to {concurrency}. High "
                "parallel query fetch can increase DB and worker contention on "
                "large syncs."
            ),
        )
    if concurrency <= 2:
        return check(
            name="Query fetch concurrency",
            status="info",
            message=(
                f"Source query fetch concurrency is {concurrency}; this is stable "
                "but can underutilize available worker capacity."
            ),
        )
    return check(
        name="Query fetch concurrency",
        status="pass",
        message=f"Source query fetch concurrency is {concurrency}.",
    )


def query_pushdown_check_status(query_pushdown):
    efficiency = (query_pushdown or {}).get("efficiency") or {}
    runtime_share = (query_pushdown or {}).get("runtime_share") or {}
    diff_utilization = (query_pushdown or {}).get("diff_utilization") or {}
    statuses = [
        str(efficiency.get("status") or "info").strip().lower(),
        str(runtime_share.get("status") or "info").strip().lower(),
        str(diff_utilization.get("status") or "info").strip().lower(),
    ]
    if "fail" in statuses:
        return "fail"
    if "warn" in statuses:
        return "warn"
    if "pass" in statuses:
        return "pass"
    return "info"


def query_pushdown_check_message(query_pushdown):
    efficiency = (query_pushdown or {}).get("efficiency") or {}
    runtime_share = (query_pushdown or {}).get("runtime_share") or {}
    diff_utilization = (query_pushdown or {}).get("diff_utilization") or {}
    message = str(efficiency.get("message") or "").strip()
    hotspot_models = list(efficiency.get("hotspot_models") or [])
    runtime_message = str(runtime_share.get("message") or "").strip()
    diff_message = str(diff_utilization.get("message") or "").strip()
    tuning_guidance = list((query_pushdown or {}).get("tuning_guidance") or [])

    message_parts = []
    if message:
        message_parts.append(message)
    if runtime_message:
        message_parts.append(runtime_message)
    if diff_message:
        message_parts.append(diff_message)

    if not hotspot_models:
        message = (
            " ".join(message_parts).strip()
            or "Pushdown efficiency diagnostics are unavailable."
        )
    else:
        hotspot_labels = ", ".join(
            item.get("model", "unknown") for item in hotspot_models
        )
        if message_parts:
            message = (
                f"{' '.join(message_parts).strip()} Hotspot model(s): {hotspot_labels}."
            )
        else:
            message = f"Hotspot model(s): {hotspot_labels}."

    if not tuning_guidance:
        return message
    preview = "; ".join(
        str(item.get("message") or "").strip()
        for item in tuning_guidance[:2]
        if str(item.get("message") or "").strip()
    )
    if not preview:
        return message
    return f"{message} Guidance: {preview}"


def large_run_tuning_check_status(large_run_tuning):
    status = str((large_run_tuning or {}).get("status") or "info").strip().lower()
    if status in {"fail", "warn", "pass"}:
        return status
    return "info"


def large_run_tuning_check_message(large_run_tuning):
    if not large_run_tuning:
        return "Large-run tuning diagnostics are unavailable."
    message = str(large_run_tuning.get("message") or "").strip()
    actions = list(large_run_tuning.get("first_order_actions") or [])
    preview = "; ".join(
        str(item.get("message") or "").strip()
        for item in actions[:2]
        if str(item.get("message") or "").strip()
    )
    if message and preview and preview not in message:
        return f"{message} Next: {preview}"
    return message or preview or "Large-run tuning diagnostics are unavailable."


def adaptive_capacity_check_status(adaptive_capacity):
    status = str((adaptive_capacity or {}).get("status") or "info").strip().lower()
    if status in {"fail", "warn", "pass"}:
        return status
    return "info"


def adaptive_capacity_check_message(adaptive_capacity):
    if not adaptive_capacity:
        return "Adaptive capacity diagnostics are unavailable."
    message = str(adaptive_capacity.get("message") or "").strip()
    return message or "Adaptive capacity diagnostics are unavailable."


def dependency_preflight_check_status(dependency_preflight):
    status = str((dependency_preflight or {}).get("status") or "info").strip().lower()
    if status in {"fail", "warn", "pass"}:
        return status
    return "info"


def dependency_preflight_check_message(dependency_preflight):
    if not dependency_preflight:
        return "Scoped dependency preflight diagnostics are unavailable."
    message = str(dependency_preflight.get("message") or "").strip()
    warnings = list(dependency_preflight.get("warnings") or [])
    preview = "; ".join(
        str(item.get("message") or "").strip()
        for item in warnings[:2]
        if str(item.get("message") or "").strip()
    )
    if message and preview:
        return f"{message} {preview}"
    return (
        message or preview or "Scoped dependency preflight diagnostics are unavailable."
    )


def delete_wave_check_status(delete_wave):
    status = str((delete_wave or {}).get("status") or "info").strip().lower()
    if status in {"fail", "warn", "pass"}:
        return status
    return "info"


def delete_wave_check_message(delete_wave):
    if not delete_wave:
        return "Delete-wave diagnostics are unavailable."
    message = str(delete_wave.get("message") or "").strip()
    plan = (delete_wave or {}).get("plan") or {}
    warnings = list(plan.get("warnings") or [])
    warning_codes = list(delete_wave.get("warning_codes") or [])
    warning_preview = "; ".join(
        str(item.get("message") or "").strip()
        for item in warnings[:2]
        if str(item.get("message") or "").strip()
    )
    suffix = ""
    if warning_codes:
        suffix = f" Warning codes: {', '.join(warning_codes)}."
    if warning_preview:
        suffix = f"{suffix} Warning: {warning_preview}"
    if suffix:
        return f"{message}{suffix}"
    return message or "Delete-wave diagnostics are unavailable."


def throughput_check_status(throughput):
    status = str((throughput or {}).get("status") or "info").strip().lower()
    if status in {"fail", "warn", "pass"}:
        return status
    return "info"


def throughput_check_message(throughput):
    if not throughput:
        return "Run-throughput diagnostics are unavailable."
    message = str(throughput.get("message") or "").strip()
    readiness = throughput.get("scheduler_overlap_readiness") or {}
    readiness_message = str(readiness.get("message") or "").strip()
    dominant_wait_component = str(
        readiness.get("dominant_wait_component") or ""
    ).strip()
    if readiness_message and dominant_wait_component:
        readiness_message = (
            f"{readiness_message} Dominant wait component: {dominant_wait_component}."
        )
    if message and readiness_message:
        return f"{message} {readiness_message}"
    return message or readiness_message or "Run-throughput diagnostics are unavailable."


def query_drift_check_status(query_drift):
    if not query_drift:
        return "warn"
    severities = {item.get("severity") for item in query_drift}
    if "warn" in severities:
        return "warn"
    if "info" in severities:
        return "info"
    return "pass"


def query_drift_check_message(query_drift, *, query_drift_summary=None):
    if not query_drift:
        return "No enabled NQE maps can be checked for local query drift."
    warn_count = len([item for item in query_drift if item.get("severity") == "warn"])
    info_count = len([item for item in query_drift if item.get("severity") == "info"])
    remediation_actions = list(
        (query_drift_summary or {}).get("remediation_actions") or []
    )
    remediation_preview = ""
    if remediation_actions:
        top_action = remediation_actions[0]
        message = str(top_action.get("message") or "").strip()
        count = top_action.get("count")
        if message:
            remediation_preview = (
                f" Top remediation: {count} map(s) need {message.lower()}."
                if count
                else f" Top remediation: {message}."
            )
    if warn_count:
        return (
            f"{warn_count} enabled map(s) differ from or cannot match bundled "
            f"query metadata.{remediation_preview}"
        )
    if info_count:
        return (
            f"{info_count} enabled direct-query-ID map(s) require live Forward "
            f"repository lookup for full drift verification; use Refresh Query "
            f"IDs after local query edits to keep saved IDs aligned.{remediation_preview}"
        )
    return f"Enabled maps match local bundled query metadata.{remediation_preview}"


def validation_check_status(validation_run):
    if validation_run is None:
        return "warn"
    if (
        validation_run.status == ForwardValidationStatusChoices.PASSED
        and validation_run.allowed
    ):
        return "pass"
    if validation_run.status == ForwardValidationStatusChoices.BLOCKED:
        return "fail"
    return "warn"


def validation_check_message(validation_run):
    if validation_run is None:
        return "No validation run exists for this sync."
    if validation_run.allowed:
        return f"Latest validation {validation_run.pk} allowed the sync."
    return f"Latest validation {validation_run.pk} status is {validation_run.status}."


def ingestion_check_status(ingestion):
    if ingestion is None:
        return "warn"
    if ingestion.failed_change_count:
        return "fail"
    if has_blocking_issues(ingestion):
        return "warn"
    if ingestion.baseline_ready:
        return "pass"
    if ingestion.issues.exists():
        return "warn"
    return "warn"


def ingestion_check_message(ingestion):
    if ingestion is None:
        return "No ingestion has run for this sync."
    if ingestion.failed_change_count:
        return (
            f"Latest ingestion {ingestion.pk} has "
            f"{ingestion.failed_change_count} failed change(s)."
        )
    if has_blocking_issues(ingestion):
        issue_count = ingestion.issues.count()
        return (
            f"Latest ingestion {ingestion.pk} recorded {issue_count} issue(s), "
            "including blocking rows."
        )
    issue_count = ingestion.issues.count()
    if issue_count:
        return (
            f"Latest ingestion {ingestion.pk} recorded {issue_count} issue(s), "
            "all currently classified as non-blocking."
        )
    if ingestion.baseline_ready:
        return f"Latest ingestion {ingestion.pk} is baseline-ready."
    return f"Latest ingestion {ingestion.pk} is not marked baseline-ready."


def recommendation_status(recommendation):
    severity = recommendation.get("severity")
    if severity == "success":
        return "pass"
    if severity in {"danger", "warning"}:
        return "warn"
    return "info"


def timeout_check(sync):
    rq_timeout = configured_rq_default_timeout()
    source_timeout = source_timeout_seconds(sync)
    if rq_timeout is None:
        return check(
            name="Worker timeout",
            status="info",
            message="RQ_DEFAULT_TIMEOUT is not configured explicitly.",
        )
    if source_timeout is not None and rq_timeout < source_timeout:
        return check(
            name="Worker timeout",
            status="warn",
            message=(
                f"RQ_DEFAULT_TIMEOUT is {rq_timeout}s, below the Forward source "
                f"timeout of {source_timeout}s."
            ),
        )
    return check(
        name="Worker timeout",
        status="pass",
        message=f"RQ_DEFAULT_TIMEOUT is {rq_timeout}s.",
    )


def capacity_check_summary(sync, capacity_summary):
    if not capacity_summary or not capacity_summary.get("available"):
        return check(
            name="Shard capacity",
            status="info",
            message="No completed execution-step timing is available yet.",
        )
    rq_timeout = configured_rq_default_timeout()
    max_seconds = capacity_summary.get("max_completed_step_seconds")
    projected_remaining = capacity_summary.get("projected_remaining_seconds")
    if rq_timeout and max_seconds and max_seconds >= rq_timeout * 0.8:
        return check(
            name="Shard capacity",
            status="warn",
            message=(
                f"Observed shard duration reached {max_seconds}s, close to the "
                f"configured worker timeout of {rq_timeout}s."
            ),
        )
    if rq_timeout and projected_remaining and projected_remaining >= rq_timeout:
        return check(
            name="Shard capacity",
            status="warn",
            message=(
                f"Projected remaining stage runtime is {projected_remaining}s, "
                f"which exceeds the configured worker timeout of {rq_timeout}s. "
                "Consider increasing RQ timeout, lowering query fetch concurrency, "
                "or using fast bootstrap for baseline initialization."
            ),
        )
    return check(
        name="Shard capacity",
        status="pass",
        message=capacity_summary.get("message") or "Shard timing is available.",
    )


def check(*, name, status, message):
    return {"name": name, "status": status, "message": message}
