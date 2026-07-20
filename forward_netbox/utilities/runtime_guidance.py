from django.conf import settings

from .forward_api import DEFAULT_FORWARD_API_TIMEOUT_SECONDS
from .forward_api import DEFAULT_QUERY_FETCH_CONCURRENCY
from .forward_api import MAX_QUERY_FETCH_CONCURRENCY


MIN_LARGE_BRANCH_RQ_TIMEOUT_SECONDS = 1800
DEFAULT_ESTIMATED_SECONDS_PER_CHANGE = 0.08
BRANCH_TIMEOUT_RISK_RATIO = 0.8
DEFAULT_PUSHDOWN_FALLBACK_WARN_RATE = 0.5
DEFAULT_PUSHDOWN_RUNTIME_FALLBACK_WARN_SHARE = 0.5
DEFAULT_PUSHDOWN_DIFF_WARN_RATIO = 0.0


def configured_rq_default_timeout():
    value = getattr(settings, "RQ_DEFAULT_TIMEOUT", None)
    if value in ("", None):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def source_timeout_seconds(sync):
    parameters = getattr(getattr(sync, "source", None), "parameters", None) or {}
    value = parameters.get("timeout")
    if value in ("", None):
        return DEFAULT_FORWARD_API_TIMEOUT_SECONDS
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def source_query_fetch_concurrency(sync):
    parameters = getattr(getattr(sync, "source", None), "parameters", None) or {}
    value = parameters.get("query_fetch_concurrency")
    if value in ("", None):
        return DEFAULT_QUERY_FETCH_CONCURRENCY
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return DEFAULT_QUERY_FETCH_CONCURRENCY
    return max(1, min(MAX_QUERY_FETCH_CONCURRENCY, parsed))


def source_pushdown_alert_thresholds(sync):
    source = getattr(sync, "source", None)
    parameters = getattr(source, "parameters", None) or {}
    return {
        "fallback_warn_rate": _bounded_ratio(
            parameters.get("pushdown_fallback_warn_rate"),
            DEFAULT_PUSHDOWN_FALLBACK_WARN_RATE,
        ),
        "runtime_fallback_warn_share": _bounded_ratio(
            parameters.get("pushdown_runtime_fallback_warn_share"),
            DEFAULT_PUSHDOWN_RUNTIME_FALLBACK_WARN_SHARE,
        ),
        "diff_warn_ratio": _bounded_ratio(
            parameters.get("pushdown_diff_warn_ratio"),
            DEFAULT_PUSHDOWN_DIFF_WARN_RATIO,
        ),
    }


def _bounded_ratio(value, default):
    if value in ("", None):
        return float(default)
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return float(default)
    return max(0.0, min(1.0, parsed))


def log_worker_timeout_guidance(sync, logger_):
    rq_timeout = configured_rq_default_timeout()
    if rq_timeout is None:
        return

    source_timeout = source_timeout_seconds(sync)
    if source_timeout is not None and rq_timeout < source_timeout:
        logger_.log_warning(
            "NetBox RQ_DEFAULT_TIMEOUT is "
            f"{rq_timeout}s, lower than the Forward source timeout "
            f"({source_timeout}s). Long NQE/preflight calls can be killed by the "
            "NetBox worker before the Forward API timeout is reached.",
            obj=sync,
        )
        return

    if rq_timeout < MIN_LARGE_BRANCH_RQ_TIMEOUT_SECONDS:
        logger_.log_warning(
            "NetBox RQ_DEFAULT_TIMEOUT is "
            f"{rq_timeout}s. Large single-branch baselines can exceed this; "
            "increase the worker timeout for large initial syncs.",
            obj=sync,
        )


def log_branch_plan_timeout_guidance(sync, logger_, plan):
    rq_timeout = configured_rq_default_timeout()
    if rq_timeout is None or rq_timeout >= MIN_LARGE_BRANCH_RQ_TIMEOUT_SECONDS:
        return

    estimated_changes = sum(int(item.estimated_changes or 0) for item in plan)
    if len(plan) <= 1 and estimated_changes <= sync.get_max_changes_per_staging_item():
        return

    logger_.log_warning(
        "Branching plan contains "
        f"{len(plan)} staging item(s) and about {estimated_changes} planned change(s), "
        f"but NetBox RQ_DEFAULT_TIMEOUT is only {rq_timeout}s. If this run times "
        "out, increase the NetBox worker timeout before rerunning; the staging plan "
        "itself will remain bounded by the configured staging-item budget.",
        obj=sync,
    )


def log_branch_plan_capacity_guidance(sync, logger_, plan):
    rq_timeout = configured_rq_default_timeout()
    if rq_timeout is None:
        return
    if not plan:
        return

    projected = _projected_plan_runtime_seconds(sync, plan)
    if projected is None:
        return
    threshold = int(rq_timeout * BRANCH_TIMEOUT_RISK_RATIO)
    if projected < threshold:
        return

    logger_.log_warning(
        "Projected Branching stage runtime is "
        f"{projected}s based on recent shard history, with RQ_DEFAULT_TIMEOUT at "
        f"{rq_timeout}s. This run is at elevated timeout risk; consider reducing "
        "query fetch concurrency, increasing worker timeout, or using Fast "
        "bootstrap for the initial baseline.",
        obj=sync,
    )


def _projected_plan_runtime_seconds(sync, plan):
    estimated_changes = sum(int(item.estimated_changes or 0) for item in plan)
    if estimated_changes <= 0:
        return None
    seconds_per_change = _recent_seconds_per_change(sync)
    if seconds_per_change is None:
        seconds_per_change = DEFAULT_ESTIMATED_SECONDS_PER_CHANGE
    return round(estimated_changes * seconds_per_change, 3)


def _recent_seconds_per_change(sync):
    # 2.0: per-shard execution run/step history was removed, so there is no
    # recorded throughput to derive from; callers fall back to the default
    # estimate.
    return None
