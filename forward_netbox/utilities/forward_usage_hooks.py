"""Feed the plugin's own `ApiUsageTracker` (forward_usage.py, step 2) from the
`forward-sdk`'s request hooks (forward-sdk migration plan, step 5 of 7).

**Why this exists before any client method is converted**: `_request()`'s
retry loop today calls `_record_http_attempt_usage()` once per httpx attempt,
including internal retries, and `evaluate_forward_api_usage` reads the
resulting `observed_http_attempts_per_minute` to FAIL builds above the
Forward SaaS hard block - the plan's own hardest constraint. Approximating
this per call site (one `_record_http_attempt_usage()` call wrapped around
each SDK service call) would under-count: the SDK retries internally, inside
one service call, and a caller has no visibility into how many actual HTTP
attempts that one call made. `Hooks.on_request` fires once per attempt
(`_sync/transport.py:150`, inside the retry loop, before `on_retry`'s own
`http_retries` increment), so wiring it once at client construction, instead
of at each of ~19 call sites, is the only way to get an accurate count
regardless of which method issued the call or how many times the SDK retried
it.

**What this cannot see**: a transport failure (connection refused, TLS,
timeout) never reaches `on_response`, because no response was ever received.
`ForwardClient._call_sdk` (forward_api_impl.py) records
`http_transport_failures`/`http_timeout_failures` at the point it catches
and translates a `ForwardTransportError`, which is the only place that
information exists.

**What this does not cover, on purpose**: the NQE-semantic counters
(`nqe_query_calls`, `nqe_pages`, `nqe_async_*_calls`, ...) are not HTTP-level
concerns a request hook can see - one NQE query call may be several HTTP
requests, or an async poll loop's worth. Those stay recorded by whichever
`ForwardClient` NQE method makes the call, exactly as today, once those
methods are converted in a later sub-commit.
"""

from forward_sdk.telemetry import Hooks

from .forward_client_errors import TRANSIENT_FORWARD_HTTP_STATUS_CODES


class UsageTrackingHooks(Hooks):
    def __init__(self, usage):
        self._usage = usage

    def on_request(self, operation, request):
        self._usage.record_http_attempt()

    def on_response(self, operation, response, elapsed):
        status_code = response.status_code
        self._usage.record_http_status_class(status_code)
        if status_code < 400:
            self._usage.record("http_successes")
            return
        self._usage.record("http_failures")
        self._usage.record("http_status_failures")
        if status_code == 429:
            self._usage.record("http_429_failures")
        if status_code in TRANSIENT_FORWARD_HTTP_STATUS_CODES:
            self._usage.record("http_transient_status_failures")
        else:
            self._usage.record("http_nontransient_status_failures")

    def on_retry(self, operation, attempt, delay, reason):
        self._usage.record("http_retries")

    # `on_sleep` is deliberately not overridden: the only reason the SDK
    # calls it is the rate-limit pacer (`_sync/transport.py:101`), which is
    # this plugin's own `Throttle` via `_CrossProcessThrottleAdapter`
    # (forward_client_factory.py) - and `Throttle.throttle()` already
    # records `throttle_sleep_seconds` on this same `usage` tracker
    # (forward_throttle.py's `_sleep_for_rate_limit`). Overriding it here
    # too would double-count every sleep.
