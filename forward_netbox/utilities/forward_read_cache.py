"""`ForwardClient`'s cross-process (shared, Django-cache-backed) read-cache
substrate, extracted into its own module (forward-sdk migration plan, step 2
of 7). No behavior change.

This covers only the shared-cache mechanics that are uniform across every
cached resource: scope derivation, key construction, get/set, and the
query-read generation counter used to invalidate every NQE-query cache entry
at once on a write. It deliberately does NOT cover each resource's own
per-instance in-memory cache dict (`self._networks_cache`,
`self._snapshots_cache`, etc. on `ForwardClient`) - those are shaped
differently per resource and read/written directly inside each `get_*`
method's own body, which is untouched by this step. Extracting those too
would mean rewriting every cached-fetch method's body in the same change
that is supposed to prove the extraction changes nothing, so it is left for
a later, narrower change if wanted.

The zero-arg `cache_provider` callable is passed in rather than imported
directly so the caller (`ForwardClient`) can keep exposing the same
`_shared_read_cache()` seam its own tests patch
(`forward_netbox.utilities.forward_api_impl._shared_read_cache`) - as long as
that callable does a fresh module-global lookup on every call (a bare
function reference or a lambda closing over a module-level name both do),
patching it there still takes effect here.
"""

import hashlib
import json

from rq.timeouts import JobTimeoutException

READ_CACHE_TIMEOUT_SECONDS = 60
READ_CACHE_GENERATION_KEY_SUFFIX = ":query-generation"


class SharedReadCache:
    def __init__(self, *, scope, cache_provider):
        self._scope = scope
        self._cache_provider = cache_provider

    def key(self, kind: str, *parts: object) -> str:
        payload = {
            "kind": kind,
            "scope": self._scope,
            "parts": [str(part) for part in parts],
        }
        encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"))
        digest = hashlib.sha256(encoded.encode("utf-8")).hexdigest()
        return f"{self._scope}:{kind}:{digest}"

    def get(self, key: str):
        cache = self._cache_provider()
        if cache is None:
            return None
        return cache.get(key)

    def set(self, key: str, value):
        cache = self._cache_provider()
        if cache is None:
            return
        cache.set(key, value, timeout=READ_CACHE_TIMEOUT_SECONDS)

    def _generation_key(self) -> str:
        return f"{self._scope}{READ_CACHE_GENERATION_KEY_SUFFIX}"

    def generation(self) -> int:
        cache = self._cache_provider()
        if cache is None:
            return 0
        try:
            return int(cache.get(self._generation_key()) or 0)
        except (TypeError, ValueError):
            return 0

    def bump_generation(self) -> None:
        cache = self._cache_provider()
        if cache is None:
            return
        key = self._generation_key()
        try:
            cache.incr(key)
        except JobTimeoutException:
            raise
        except Exception:
            try:
                current = int(cache.get(key) or 0)
            except (TypeError, ValueError):
                current = 0
            cache.set(key, current + 1, timeout=READ_CACHE_TIMEOUT_SECONDS)


def shared_read_cache_scope(*, source_pk, source_type, base_url, username) -> str:
    scope = {
        "source_pk": source_pk,
        "source_type": source_type or "",
        "base_url": base_url,
        "username": username or "",
    }
    encoded = json.dumps(scope, sort_keys=True, separators=(",", ":"))
    digest = hashlib.sha256(encoded.encode("utf-8")).hexdigest()
    return f"forward-netbox:forward-api-read-cache:{digest}"
