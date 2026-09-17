"""Pure config-parsing helpers for `ForwardClient`, extracted so they gain a
test surface independent of the transport underneath them (forward-sdk
migration plan, step 2 of 7 - see
docs/03_Plans/active/2026-09-07-forward-sdk-migration.md). No behavior
change: every function here is a direct extraction of a `ForwardClient`
method body, called from exactly the same place (`__init__`).
"""

from ..exceptions import ForwardClientError
from .crypto import decrypt_secret

DEFAULT_FORWARD_API_TIMEOUT_SECONDS = 1200
DEFAULT_FORWARD_API_RETRIES = 2
MAX_NQE_PAGE_SIZE = 10000
DEFAULT_NQE_PAGE_SIZE = 10000
DEFAULT_NQE_FETCH_ALL_MAX_PAGES = 5000
MAX_NQE_FETCH_ALL_MAX_PAGES = 200000
# Absolute ceiling on rows accumulated in memory by a single fetch_all. The
# page-count ceiling alone permits ~50M rows (10k page x 5k pages) before
# firing, enough to OOM the worker on a large unsharded result. Abort earlier
# with an actionable error so the operator shards the model instead of
# crashing.
DEFAULT_NQE_FETCH_ALL_MAX_ROWS = 2_000_000
MAX_NQE_FETCH_ALL_MAX_ROWS = 50_000_000
DEFAULT_NQE_IDENTICAL_FULL_PAGE_STREAK_LIMIT = 25
MAX_NQE_IDENTICAL_FULL_PAGE_STREAK_LIMIT = 1000
DEFAULT_FORWARD_API_REQUESTS_PER_MINUTE = 0
DEFAULT_FORWARD_SAAS_API_REQUESTS_PER_MINUTE = 1800
FORWARD_SAAS_API_HARD_BLOCK_REQUESTS_PER_MINUTE = 2000
MAX_FORWARD_API_REQUESTS_PER_MINUTE = 60000
# Exponential async polling starts at 0.1s, preserving fast-query latency, and
# uses this value only as the slow-query plateau. At the default 1200-poll
# budget, a 5s ceiling permits about 100 minutes before the poll-count guard;
# an enabled per-workload deadline remains the authoritative wall-clock guard.
DEFAULT_NQE_ASYNC_POLL_INTERVAL_SECONDS = 5.0
DEFAULT_NQE_ASYNC_MAX_POLLS = 1200
MAX_NQE_ASYNC_POLL_INTERVAL_SECONDS = 60.0
MAX_NQE_ASYNC_MAX_POLLS = 10000


def decrypt_client_password(raw_password):
    """Decrypt a `ForwardSource`'s stored password for HTTP auth.

    The stored password is encrypted at rest (`ForwardSource.save`); this is
    the one place it is actually decrypted for use.
    """
    try:
        return decrypt_secret(raw_password)
    except ValueError as exc:
        raise ForwardClientError(str(exc)) from exc


def coerce_nqe_page_size(value):
    if value is None:
        return DEFAULT_NQE_PAGE_SIZE
    try:
        size = int(value)
    except (TypeError, ValueError):
        return DEFAULT_NQE_PAGE_SIZE
    return max(1, min(size, MAX_NQE_PAGE_SIZE))


def coerce_retry_count(value):
    if value is None:
        return DEFAULT_FORWARD_API_RETRIES
    try:
        retries = int(value)
    except (TypeError, ValueError):
        return DEFAULT_FORWARD_API_RETRIES
    return max(0, min(retries, 5))


def coerce_nqe_fetch_all_max_pages(value):
    if value is None:
        return DEFAULT_NQE_FETCH_ALL_MAX_PAGES
    try:
        pages = int(value)
    except (TypeError, ValueError):
        return DEFAULT_NQE_FETCH_ALL_MAX_PAGES
    return max(1, min(pages, MAX_NQE_FETCH_ALL_MAX_PAGES))


def coerce_nqe_fetch_all_max_rows(value):
    if value is None:
        return DEFAULT_NQE_FETCH_ALL_MAX_ROWS
    try:
        rows = int(value)
    except (TypeError, ValueError):
        return DEFAULT_NQE_FETCH_ALL_MAX_ROWS
    return max(1, min(rows, MAX_NQE_FETCH_ALL_MAX_ROWS))


def coerce_nqe_identical_full_page_streak_limit(value):
    if value is None:
        return DEFAULT_NQE_IDENTICAL_FULL_PAGE_STREAK_LIMIT
    try:
        streak = int(value)
    except (TypeError, ValueError):
        return DEFAULT_NQE_IDENTICAL_FULL_PAGE_STREAK_LIMIT
    return max(1, min(streak, MAX_NQE_IDENTICAL_FULL_PAGE_STREAK_LIMIT))


def default_api_requests_per_minute(*, source_type, base_url):
    source_type = str(source_type or "").lower()
    if source_type == "saas" or base_url == "https://fwd.app":
        return DEFAULT_FORWARD_SAAS_API_REQUESTS_PER_MINUTE
    return DEFAULT_FORWARD_API_REQUESTS_PER_MINUTE


def coerce_api_requests_per_minute(value, *, source_type, base_url):
    if value in (None, ""):
        return default_api_requests_per_minute(
            source_type=source_type, base_url=base_url
        )
    try:
        requests_per_minute = int(value)
    except (TypeError, ValueError):
        return default_api_requests_per_minute(
            source_type=source_type, base_url=base_url
        )
    return max(0, min(requests_per_minute, MAX_FORWARD_API_REQUESTS_PER_MINUTE))


def coerce_bool(value, default=False):
    if value in (None, ""):
        return bool(default)
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "on"}
    return bool(value)


def coerce_nqe_async_poll_interval_seconds(value):
    if value is None:
        return DEFAULT_NQE_ASYNC_POLL_INTERVAL_SECONDS
    try:
        interval = float(value)
    except (TypeError, ValueError):
        return DEFAULT_NQE_ASYNC_POLL_INTERVAL_SECONDS
    return max(0.0, min(interval, MAX_NQE_ASYNC_POLL_INTERVAL_SECONDS))


def coerce_nqe_async_max_polls(value):
    if value is None:
        return DEFAULT_NQE_ASYNC_MAX_POLLS
    try:
        polls = int(value)
    except (TypeError, ValueError):
        return DEFAULT_NQE_ASYNC_MAX_POLLS
    return max(1, min(polls, MAX_NQE_ASYNC_MAX_POLLS))
