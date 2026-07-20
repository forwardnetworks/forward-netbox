from ..choices import ForwardSourceStatusChoices
from ..choices import ForwardValidationStatusChoices
from .ingestion_issues import has_blocking_issues
from .runtime_guidance import configured_rq_default_timeout
from .runtime_guidance import effective_forward_job_timeout
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
    dependency_preflight,
    delete_wave,
    throughput,
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
            name="Enabled map, model not selected",
            status=(
                "warn"
                if model_summary.get("enabled_optional_maps_without_model")
                else "pass"
            ),
            message=(
                "Every enabled optional map has its model selected."
                if not model_summary.get("enabled_optional_maps_without_model")
                else (
                    "These maps are enabled but their model is NOT selected in "
                    "this sync's Model Selection, so they never run and produce "
                    "nothing: "
                    + ", ".join(model_summary["enabled_optional_maps_without_model"])
                    + ". Select the model(s) in Model Selection."
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
    timeout = timeout_check(sync)
    if timeout is not None:
        checks.append(timeout)
    query_fetch = query_fetch_concurrency_check(sync)
    if query_fetch is not None:
        checks.append(query_fetch)
    dlm_notice = dlm_hardware_notice_alias_check(maps)
    if dlm_notice is not None:
        checks.append(dlm_notice)
    dlm_readiness = dlm_dependency_readiness_check(maps, latest_ingestion)
    if dlm_readiness is not None:
        checks.append(dlm_readiness)
    return checks


# DLM maps that attach to an already-synced NetBox object: hardware notices need
# the DeviceType, vulnerabilities/device-software need the Device. If those
# parents are not synced, the rows skip (ForwardDependencySkipError) and the
# operator gets silence unless they read the issue list. This surfaces the last
# run's DLM dependency skips as a readiness signal pointing at the real fix.
_DLM_DEVICE_DEPENDENT_MODELS = (
    "netbox_dlm.hardwarenotice",
    "netbox_dlm.vulnerability",
    "netbox_dlm.devicesoftware",
)


def dlm_dependency_readiness_check(maps, latest_ingestion):
    enabled_models = {
        m.model_string for m in (maps or []) if getattr(m, "enabled", False)
    }
    relevant = [
        model for model in _DLM_DEVICE_DEPENDENT_MODELS if model in enabled_models
    ]
    if not relevant or latest_ingestion is None:
        return None
    skip_counts = {}
    for model in relevant:
        issue_contexts = latest_ingestion.issues.filter(
            model=model, exception__icontains="ForwardDependencySkipError"
        ).values_list("coalesce_fields", flat=True)
        detail_count = 0
        summary_counts = []
        for context in issue_contexts:
            context = context or {}
            if context.get("dependency_skip_summary"):
                try:
                    summary_counts.append(
                        int(context.get("dependency_skip_count") or 0)
                    )
                except (TypeError, ValueError):
                    pass
            else:
                detail_count += 1
        skip_counts[model] = max([detail_count, *summary_counts])
    total = sum(skip_counts.values())
    if total == 0:
        return check(
            name="DLM dependency readiness",
            status="pass",
            message="Enabled DLM maps found their NetBox parents last run.",
        )
    detail = ", ".join(
        f"{model.split('.')[-1]} ×{count}"
        for model, count in skip_counts.items()
        if count
    )
    return check(
        name="DLM dependency readiness",
        status="warn",
        message=(
            f"Last run skipped {total} DLM row(s) ({detail}) because their "
            "device types / devices are not in NetBox yet. DLM notices and "
            "vulnerabilities hang off synced devices — fix device (and "
            "device-type) sync first, then they populate."
        ),
    )


# The DLM hardware-notice maps look up the NetBox DeviceType by the model name
# the query emits. The base device query emits the raw Forward PID; the alias
# variant ("... with NetBox Device Type Aliases") emits the Device-Type-Library
# name. The base vs alias hardware-notice map must match whichever device query
# is active, or every notice skips with "device type not in NetBox yet".
_DLM_NOTICE_BASE = "Forward DLM Hardware Notices"
_DLM_NOTICE_ALIAS = "Forward DLM Hardware Notices with NetBox Aliases"
_DEVICE_TYPE_ALIAS_SUFFIX = " with NetBox Device Type Aliases"


def dlm_hardware_notice_alias_check(maps):
    enabled = {m.name for m in (maps or []) if getattr(m, "enabled", False)}
    if _DLM_NOTICE_BASE not in enabled and _DLM_NOTICE_ALIAS not in enabled:
        return None  # no hardware-notice map enabled — nothing to check
    # An alias device-type query is active when any enabled builtin map carries
    # the device-type alias suffix (it supersedes its base counterpart).
    alias_device_query = any(
        name.endswith(_DEVICE_TYPE_ALIAS_SUFFIX) for name in enabled
    )
    base_notice = _DLM_NOTICE_BASE in enabled
    alias_notice = _DLM_NOTICE_ALIAS in enabled

    if base_notice and alias_notice:
        return check(
            name="DLM hardware-notice alias",
            status="warn",
            message=(
                f"Both '{_DLM_NOTICE_BASE}' and '{_DLM_NOTICE_ALIAS}' are "
                "enabled. The alias map supersedes the base map; disable the "
                "base map to avoid duplicate and skipped hardware notices."
            ),
        )
    if alias_device_query and base_notice and not alias_notice:
        return check(
            name="DLM hardware-notice alias",
            status="warn",
            message=(
                "You run the alias-aware device query but the base "
                f"'{_DLM_NOTICE_BASE}' map. Hardware notices look up the raw "
                "Forward model and will skip ('device type not in NetBox "
                f"yet'). Enable '{_DLM_NOTICE_ALIAS}' instead."
            ),
        )
    if not alias_device_query and alias_notice and not base_notice:
        return check(
            name="DLM hardware-notice alias",
            status="warn",
            message=(
                f"You enabled '{_DLM_NOTICE_ALIAS}' but not the alias-aware "
                "device query, so notices look up Device-Type-Library names "
                f"that do not exist. Use '{_DLM_NOTICE_BASE}' instead, or "
                "enable the alias-aware device query."
            ),
        )
    return check(
        name="DLM hardware-notice alias",
        status="pass",
        message="Hardware-notice map matches the active device query variant.",
    )


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
    return message or "Run-throughput diagnostics are unavailable."


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
            f"repository lookup for full drift verification; use Publish Bundled "
            f"Queries to update built-in maps and convert them to live repository "
            f"paths.{remediation_preview}"
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
    job_timeout = effective_forward_job_timeout()
    source_timeout = source_timeout_seconds(sync)
    if source_timeout is not None and job_timeout < source_timeout:
        return check(
            name="Worker timeout",
            status="warn",
            message=(
                f"Effective Forward job timeout is {job_timeout}s, below the source "
                f"timeout of {source_timeout}s."
            ),
        )
    if rq_timeout is None:
        message = (
            f"Forward jobs enforce a {job_timeout}s minimum; "
            "RQ_DEFAULT_TIMEOUT is not configured explicitly."
        )
    elif rq_timeout != job_timeout:
        message = (
            f"RQ_DEFAULT_TIMEOUT is {rq_timeout}s; Forward jobs enforce an "
            f"effective {job_timeout}s timeout."
        )
    else:
        message = f"Forward job timeout is {job_timeout}s."
    return check(
        name="Worker timeout",
        status="pass",
        message=message,
    )


def check(*, name, status, message):
    return {"name": name, "status": status, "message": message}
