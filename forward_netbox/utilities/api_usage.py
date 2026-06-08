from __future__ import annotations

from typing import Any

from ..choices import ForwardSourceDeploymentChoices
from .forward_api import DEFAULT_FORWARD_SAAS_API_REQUESTS_PER_MINUTE
from .forward_api import FORWARD_SAAS_API_HARD_BLOCK_REQUESTS_PER_MINUTE

FORWARD_SAAS_API_WARNING_REQUESTS_PER_MINUTE = 1900
FORWARD_OBSERVED_RATE_MIN_HTTP_ATTEMPTS = 20
FORWARD_OBSERVED_RATE_MIN_WINDOW_SECONDS = 10.0


def _number(value: Any, default: int | float = 0) -> int | float:
    if isinstance(value, bool):
        return default
    if isinstance(value, (int, float)):
        return value
    try:
        return int(value)
    except (TypeError, ValueError):
        try:
            return float(value)
        except (TypeError, ValueError):
            return default


def _source_type_value(source_type: Any) -> str:
    return str(source_type or "").strip().lower()


def evaluate_forward_api_usage(
    summary: dict | None,
    *,
    source_type: str | None = None,
    warning_requests_per_minute: int = FORWARD_SAAS_API_WARNING_REQUESTS_PER_MINUTE,
    hard_block_requests_per_minute: int = (
        FORWARD_SAAS_API_HARD_BLOCK_REQUESTS_PER_MINUTE
    ),
) -> dict:
    """Classify a Forward API usage summary for release/support gates."""
    payload = dict(summary or {})
    source_type_value = _source_type_value(source_type)
    uses_saas_budget = source_type_value in (
        "",
        ForwardSourceDeploymentChoices.SAAS,
    )
    configured_requests_per_minute = int(
        _number(
            payload.get("api_requests_per_minute"),
            DEFAULT_FORWARD_SAAS_API_REQUESTS_PER_MINUTE if uses_saas_budget else 0,
        )
    )
    http_attempts = int(_number(payload.get("http_attempts"), 0))
    http_429_failures = int(_number(payload.get("http_429_failures"), 0))
    nqe_query_calls = int(_number(payload.get("nqe_query_calls"), 0))
    nqe_diff_calls = int(_number(payload.get("nqe_diff_calls"), 0))
    nqe_pages = int(_number(payload.get("nqe_pages"), 0))
    throttle_sleep_seconds = float(_number(payload.get("throttle_sleep_seconds"), 0.0))
    read_cache_hits = int(_number(payload.get("read_cache_hits"), 0))
    read_cache_misses = int(_number(payload.get("read_cache_misses"), 0))
    usage_window_seconds = float(_number(payload.get("usage_window_seconds"), 0.0))
    observed_http_attempts_per_minute_value = payload.get(
        "observed_http_attempts_per_minute"
    )
    observed_http_attempts_per_minute = (
        float(_number(observed_http_attempts_per_minute_value, 0.0))
        if observed_http_attempts_per_minute_value is not None
        else None
    )
    observed_rate_sample_complete = bool(
        observed_http_attempts_per_minute is not None
        and http_attempts >= FORWARD_OBSERVED_RATE_MIN_HTTP_ATTEMPTS
        and usage_window_seconds >= FORWARD_OBSERVED_RATE_MIN_WINDOW_SECONDS
    )

    failure_reasons: list[str] = []
    warnings: list[str] = []

    if (
        uses_saas_budget
        and configured_requests_per_minute > hard_block_requests_per_minute
    ):
        failure_reasons.append(
            "configured_requests_per_minute_exceeds_forward_saas_hard_block"
        )
    elif (
        uses_saas_budget
        and configured_requests_per_minute >= warning_requests_per_minute
    ):
        warnings.append("configured_requests_per_minute_near_forward_saas_hard_block")

    if (
        source_type_value == ForwardSourceDeploymentChoices.SAAS
        and configured_requests_per_minute <= 0
    ):
        warnings.append("forward_saas_pacing_disabled")

    if http_429_failures > 0:
        warnings.append("forward_api_429_observed")

    if uses_saas_budget and observed_rate_sample_complete:
        if observed_http_attempts_per_minute > hard_block_requests_per_minute:
            failure_reasons.append(
                "observed_http_attempts_per_minute_exceeds_forward_saas_hard_block"
            )
        elif observed_http_attempts_per_minute >= warning_requests_per_minute:
            warnings.append(
                "observed_http_attempts_per_minute_near_forward_saas_hard_block"
            )

    status = "passed"
    if failure_reasons:
        status = "failed"
    elif warnings:
        status = "warning"

    return {
        "status": status,
        "failure_reasons": failure_reasons,
        "warnings": warnings,
        "metrics": {
            "source_type": source_type_value,
            "configured_requests_per_minute": configured_requests_per_minute,
            "warning_requests_per_minute": warning_requests_per_minute,
            "hard_block_requests_per_minute": hard_block_requests_per_minute,
            "headroom_requests_per_minute": (
                hard_block_requests_per_minute - configured_requests_per_minute
            ),
            "http_attempts": http_attempts,
            "observed_http_attempts_per_minute": (
                round(observed_http_attempts_per_minute, 3)
                if observed_http_attempts_per_minute is not None
                else None
            ),
            "observed_rate_sample_complete": observed_rate_sample_complete,
            "observed_rate_min_http_attempts": FORWARD_OBSERVED_RATE_MIN_HTTP_ATTEMPTS,
            "observed_rate_min_window_seconds": (
                FORWARD_OBSERVED_RATE_MIN_WINDOW_SECONDS
            ),
            "usage_window_seconds": round(usage_window_seconds, 3),
            "http_429_failures": http_429_failures,
            "nqe_query_calls": nqe_query_calls,
            "nqe_diff_calls": nqe_diff_calls,
            "nqe_calls": nqe_query_calls + nqe_diff_calls,
            "nqe_pages": nqe_pages,
            "throttle_sleep_seconds": round(throttle_sleep_seconds, 3),
            "read_cache_hits": read_cache_hits,
            "read_cache_misses": read_cache_misses,
            "read_cache_hit_rate": (
                round(read_cache_hits / float(read_cache_hits + read_cache_misses), 3)
                if (read_cache_hits + read_cache_misses)
                else None
            ),
        },
    }
