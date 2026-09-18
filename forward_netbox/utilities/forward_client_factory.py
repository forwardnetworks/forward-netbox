"""Construct a `forward-sdk` client from a `ForwardSource`'s config (forward-sdk
migration plan, step 5 of 7 - see
docs/03_Plans/active/2026-09-07-forward-sdk-migration.md).

Not called from `ForwardClient.__init__` yet. This module is built and
tested standalone first, exactly as step 4's `forward_client_errors.py` was,
so the wiring commit that follows only has to call `get_client(...)` rather
than also work out what it should be.
"""

import time
from urllib.parse import urlparse

from forward_sdk import ForwardClient as SDKForwardClient

from utilities.proxy import resolve_proxies

from .forward_usage_hooks import UsageTrackingHooks

USER_AGENT = "forward-netbox/0.8.6.3"


class _CrossProcessThrottleAdapter:
    """Adapts this plugin's own `Throttle` (forward_throttle.py) to the SDK's
    `Throttle` protocol (`acquire() -> float`, seconds slept).

    Passing this at construction makes the SDK use it EXCLUSIVELY in place
    of its own per-client `RateLimiter` (`Transport.__init__`: `self._throttle
    = throttle or self._default_throttle(config)`) - the two never both run,
    so `rate_limit_rpm` is still passed through for `ClientConfig` bookkeeping
    but has no pacing effect once this adapter is supplied. The plugin's own
    `Throttle` stays the single source of pacing because it is
    Django-cache-backed and coordinates a fleet of workers sharing one
    Forward account; the SDK's own limiter is explicitly documented as
    per-client only ("enough for one program"), which is not this plugin's
    deployment shape.
    """

    def __init__(self, throttle):
        self._throttle = throttle

    def acquire(self) -> float:
        started = time.monotonic()
        self._throttle.throttle()
        return time.monotonic() - started


def _resolve_static_proxy(*, base_url, client, source):
    """The proxy URL for `base_url`'s scheme, resolved once.

    `resolve_proxies` is designed for a per-request call (it takes the
    request `url` and could in principle answer differently per scheme or
    per call), but the SDK accepts one static `proxy` string for the whole
    client's lifetime. Every request this client makes targets the same
    `base_url`, so resolving once against it, at the scheme `base_url`
    itself uses, reproduces today's behavior for the deployments that
    actually configure a proxy: one proxy for one Forward source, not a
    per-request routing decision.
    """
    proxies = resolve_proxies(
        url=base_url, context={"client": client, "source": source}
    )
    if not proxies:
        return None
    scheme = urlparse(base_url).scheme or "https"
    return proxies.get(scheme) or proxies.get(f"{scheme}://")


def get_client(
    *,
    base_url,
    username,
    password,
    verify,
    timeout,
    retries,
    api_requests_per_minute,
    throttle,
    usage,
    client=None,
    source=None,
):
    """Build the `forward-sdk` client this plugin's `ForwardClient` wraps.

    `hooks=UsageTrackingHooks(usage)` is what lets `evaluate_forward_api_usage`
    keep reading an accurate `observed_http_attempts_per_minute` once this
    client, not `_request()`, is making the actual HTTP calls - see
    `forward_usage_hooks.py`'s module docstring for why this has to be wired
    here, once, rather than approximated per call site.
    """
    return SDKForwardClient(
        base_url,
        username=username,
        password=password,
        verify=verify,
        timeout=timeout,
        retries=retries,
        rate_limit_rpm=api_requests_per_minute or None,
        user_agent=USER_AGENT,
        proxy=_resolve_static_proxy(base_url=base_url, client=client, source=source),
        throttle=_CrossProcessThrottleAdapter(throttle),
        hooks=UsageTrackingHooks(usage),
    )
