"""`ForwardClient`'s rate-limiting/throttle logic, extracted into its own
module (forward-sdk migration plan, step 2 of 7). No behavior change.

Two tiers, exactly as before: a Django-cache-backed cross-process throttle
(so a fleet of workers sharing one Forward account stays under its rate
ceiling) with an in-process fallback, shared by ALL `Throttle` instances in
this process - `_RATE_LIMIT_LOCK`/`_RATE_LIMIT_LAST_REQUEST_AT` were
module-level globals in `forward_api_impl.py` before this extraction and
stay module-level globals here, for the same reason: the fallback needs one
shared clock across every client in the process, not one per instance.

`cache_provider` is a zero-arg callable rather than the cache object itself
so `ForwardClient` can keep exposing the same `_shared_rate_limit_cache()`
seam its own tests patch - see `forward_read_cache.py`'s module docstring
for why a live callable, not a captured value, is required for that to work.
"""

import hashlib
import threading
import time

from rq.timeouts import JobTimeoutException

FORWARD_API_RATE_LIMIT_CACHE_TIMEOUT_SECONDS = 120
FORWARD_API_RATE_LIMIT_LOCK_TIMEOUT_SECONDS = 5

_RATE_LIMIT_LOCK = threading.Lock()
_RATE_LIMIT_LAST_REQUEST_AT = {}


class Throttle:
    def __init__(
        self, *, api_request_min_interval, base_url, username, cache_provider, usage
    ):
        self._api_request_min_interval = api_request_min_interval
        self._base_url = base_url
        self._username = username
        self._cache_provider = cache_provider
        self._usage = usage

    def _rate_limit_key(self):
        scope = f"{self._base_url}\0{self._username or ''}"
        digest = hashlib.sha256(scope.encode("utf-8")).hexdigest()
        return f"forward-netbox:forward-api-rate-limit:{digest}"

    def _sleep_for_rate_limit(self, last_request_at, now):
        if last_request_at is None:
            return now
        wait_seconds = self._api_request_min_interval - (now - float(last_request_at))
        if wait_seconds > 0:
            self._usage.record("throttle_sleep_seconds", wait_seconds)
            time.sleep(wait_seconds)
            return time.time()
        return now

    def _throttle_with_shared_cache(self, key, cache):
        lock_key = f"{key}:lock"
        token = f"{id(self)}:{time.time_ns()}"
        while not cache.add(
            lock_key,
            token,
            timeout=FORWARD_API_RATE_LIMIT_LOCK_TIMEOUT_SECONDS,
        ):
            time.sleep(min(self._api_request_min_interval, 0.25))
        try:
            now = time.time()
            last_request_at = cache.get(key)
            now = self._sleep_for_rate_limit(last_request_at, now)
            cache.set(
                key,
                now,
                timeout=FORWARD_API_RATE_LIMIT_CACHE_TIMEOUT_SECONDS,
            )
        finally:
            if cache.get(lock_key) == token:
                cache.delete(lock_key)

    def _throttle_in_process(self, key):
        with _RATE_LIMIT_LOCK:
            now = time.time()
            last_request_at = _RATE_LIMIT_LAST_REQUEST_AT.get(key)
            now = self._sleep_for_rate_limit(last_request_at, now)
            _RATE_LIMIT_LAST_REQUEST_AT[key] = now

    def throttle(self):
        if not self._api_request_min_interval:
            return
        key = self._rate_limit_key()
        cache = self._cache_provider()
        if cache is None:
            self._throttle_in_process(key)
            return
        try:
            self._throttle_with_shared_cache(key, cache)
        except JobTimeoutException:
            raise
        except Exception:
            self._throttle_in_process(key)
