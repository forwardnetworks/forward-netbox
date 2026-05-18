from ..choices import ForwardSourceStatusChoices
from ..choices import ForwardValidationStatusChoices
from .execution_ledger import execution_run_recovery_recommendation
from .runtime_guidance import configured_rq_default_timeout
from .runtime_guidance import source_query_fetch_concurrency
from .runtime_guidance import source_timeout_seconds


def health_checks(
    *,
    sync,
    maps,
    model_summary,
    query_drift,
    raw_maps,
    data_file_maps,
    validation_run,
    latest_ingestion,
    execution_run,
    capacity_summary,
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
                "pass"
                if not model_summary["enabled_models_without_map"]
                else "warn"
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
            message=query_drift_check_message(query_drift),
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


def query_drift_check_status(query_drift):
    if not query_drift:
        return "warn"
    severities = {item.get("severity") for item in query_drift}
    if "warn" in severities:
        return "warn"
    if "info" in severities:
        return "info"
    return "pass"


def query_drift_check_message(query_drift):
    if not query_drift:
        return "No enabled NQE maps can be checked for local query drift."
    warn_count = len([item for item in query_drift if item.get("severity") == "warn"])
    info_count = len([item for item in query_drift if item.get("severity") == "info"])
    if warn_count:
        return f"{warn_count} enabled map(s) differ from or cannot match bundled query metadata."
    if info_count:
        return (
            f"{info_count} enabled direct-query-ID map(s) require live Forward "
            "repository lookup for full drift verification."
        )
    return "Enabled maps match local bundled query metadata."


def validation_check_status(validation_run):
    if validation_run is None:
        return "warn"
    if validation_run.status == ForwardValidationStatusChoices.PASSED and validation_run.allowed:
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
    if ingestion.issues.exists():
        return "warn"
    if ingestion.baseline_ready:
        return "pass"
    return "warn"


def ingestion_check_message(ingestion):
    if ingestion is None:
        return "No ingestion has run for this sync."
    if ingestion.failed_change_count:
        return (
            f"Latest ingestion {ingestion.pk} has "
            f"{ingestion.failed_change_count} failed change(s)."
        )
    issue_count = ingestion.issues.count()
    if issue_count:
        return f"Latest ingestion {ingestion.pk} recorded {issue_count} issue(s)."
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
