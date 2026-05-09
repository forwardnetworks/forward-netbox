from django.conf import settings

from ..choices import ForwardExecutionBackendChoices
from .forward_api import DEFAULT_FORWARD_API_TIMEOUT_SECONDS


MIN_LARGE_BRANCH_RQ_TIMEOUT_SECONDS = 1800


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


def log_worker_timeout_guidance(sync, logger_, *, execution_backend):
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

    if (
        execution_backend == ForwardExecutionBackendChoices.BRANCHING
        and rq_timeout < MIN_LARGE_BRANCH_RQ_TIMEOUT_SECONDS
    ):
        logger_.log_warning(
            "NetBox RQ_DEFAULT_TIMEOUT is "
            f"{rq_timeout}s. Large Branching baselines can exceed this even with "
            "bounded shards; increase the worker timeout for large syncs or use "
            "Fast bootstrap for a trusted initial baseline.",
            obj=sync,
        )


def log_branch_plan_timeout_guidance(sync, logger_, plan):
    rq_timeout = configured_rq_default_timeout()
    if rq_timeout is None or rq_timeout >= MIN_LARGE_BRANCH_RQ_TIMEOUT_SECONDS:
        return

    estimated_changes = sum(int(item.estimated_changes or 0) for item in plan)
    if len(plan) <= 1 and estimated_changes <= sync.get_max_changes_per_branch():
        return

    logger_.log_warning(
        "Branching plan contains "
        f"{len(plan)} shard(s) and about {estimated_changes} planned change(s), "
        f"but NetBox RQ_DEFAULT_TIMEOUT is only {rq_timeout}s. If this run times "
        "out, increase the NetBox worker timeout before rerunning; the shard plan "
        "itself will remain bounded by the configured branch budget.",
        obj=sync,
    )
