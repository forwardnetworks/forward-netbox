import hashlib
import io
import json
import threading
import time
from urllib.parse import quote

import httpx

from ..exceptions import ForwardClientError
from ..exceptions import ForwardConnectivityError

try:
    from utilities.proxy import resolve_proxies
except ImportError:  # pragma: no cover - NetBox always provides this at runtime.
    resolve_proxies = None

try:
    from django.core.cache import cache as django_cache
except Exception:  # pragma: no cover - Django is always present in NetBox.
    django_cache = None

LATEST_PROCESSED_SNAPSHOT = "latestProcessed"
LATEST_COLLECTED_SNAPSHOT = "latestCollected"
# How many of the most recent processed snapshots to scan when resolving the
# latestCollected selector before giving up.
DEFAULT_LATEST_COLLECTED_SCAN_LIMIT = 10
DEFAULT_FORWARD_API_TIMEOUT_SECONDS = 1200
DEFAULT_FORWARD_API_RETRIES = 2
DEFAULT_FORWARD_API_RETRY_BACKOFF_SECONDS = 2
MAX_NQE_PAGE_SIZE = 10000
DEFAULT_NQE_PAGE_SIZE = 10000
DEFAULT_NQE_FETCH_ALL_MAX_PAGES = 5000
MAX_NQE_FETCH_ALL_MAX_PAGES = 200000
DEFAULT_NQE_IDENTICAL_FULL_PAGE_STREAK_LIMIT = 25
MAX_NQE_IDENTICAL_FULL_PAGE_STREAK_LIMIT = 1000
DEFAULT_QUERY_FETCH_CONCURRENCY = 10
MAX_QUERY_FETCH_CONCURRENCY = 16
DEFAULT_FORWARD_API_REQUESTS_PER_MINUTE = 0
DEFAULT_FORWARD_SAAS_API_REQUESTS_PER_MINUTE = 1800
FORWARD_SAAS_API_HARD_BLOCK_REQUESTS_PER_MINUTE = 2000
MAX_FORWARD_API_REQUESTS_PER_MINUTE = 60000
FORWARD_API_RATE_LIMIT_CACHE_TIMEOUT_SECONDS = 120
FORWARD_API_RATE_LIMIT_LOCK_TIMEOUT_SECONDS = 5
DEFAULT_QUERY_PREFLIGHT_ENABLED = True
DEFAULT_QUERY_DIAGNOSTICS_ENABLED = True
DEFAULT_NQE_ASYNC_POLL_INTERVAL_SECONDS = 1.0
DEFAULT_NQE_ASYNC_MAX_POLLS = 1200
MAX_NQE_ASYNC_POLL_INTERVAL_SECONDS = 60.0
MAX_NQE_ASYNC_MAX_POLLS = 10000
TRANSIENT_FORWARD_HTTP_STATUS_CODES = {408, 429, 502, 503, 504}
NQE_QUERY_REPOSITORIES = {"org", "fwd"}
READ_CACHE_TIMEOUT_SECONDS = 60
READ_CACHE_GENERATION_KEY_SUFFIX = ":query-generation"
_RATE_LIMIT_LOCK = threading.Lock()
_RATE_LIMIT_LAST_REQUEST_AT = {}


def _shared_rate_limit_cache():
    return django_cache


def _shared_read_cache():
    return django_cache


def _normalize_nqe_directory(directory):
    directory = str(directory or "/").strip() or "/"
    if not directory.startswith("/"):
        directory = f"/{directory}"
    if not directory.endswith("/"):
        directory = f"{directory}/"
    return directory


def _normalize_nqe_query_path(query_path):
    query_path = str(query_path or "").strip()
    if not query_path:
        return ""
    if not query_path.startswith("/"):
        query_path = f"/{query_path}"
    return query_path


def _query_in_directory(query_path, directory):
    directory = _normalize_nqe_directory(directory)
    query_path = str(query_path or "")
    return directory == "/" or query_path.startswith(directory)


def _normalize_nqe_query_row(row, *, repository=None):
    query_id = str(row.get("queryId") or "").strip()
    path = str(row.get("path") or "").strip()
    if not query_id or not path:
        return None
    normalized = {
        "queryId": query_id,
        "path": path,
        "intent": str(row.get("intent") or "").strip(),
        "repository": str(row.get("repository") or repository or "").strip(),
        "lastCommitId": str(row.get("lastCommitId") or "").strip(),
    }
    return normalized


def _normalize_nqe_repository(repository):
    repository = str(repository or "org").strip().lower()
    if repository not in NQE_QUERY_REPOSITORIES:
        raise ForwardClientError(f"Unsupported Forward NQE repository `{repository}`.")
    return repository


def _commit_message_payload(message):
    message = str(message or "").strip() or "Publish Forward NetBox NQE maps"
    title, _, body = message.partition("\n")
    return {
        "title": title.strip() or "Publish Forward NetBox NQE maps",
        "body": body.strip(),
    }


def _commit_id_for_nqe_execution(commit_id):
    commit_id = str(commit_id or "").strip()
    if not commit_id:
        return ""
    if commit_id.lower() == "head":
        return ""
    if len(commit_id) < 40 and all(
        char in "0123456789abcdefABCDEF" for char in commit_id
    ):
        return ""
    return commit_id


def _nqe_string_literal(value: str) -> str:
    return json.dumps(value)


def build_device_tag_scope_where(include_tags, exclude_tags, include_match):
    """Return NQE ``where`` clause lines for a device tag scope.

    The returned lines are intended to follow the shared base filters
    (``snapshotInfo.result == completed`` and the FORWARD_CUSTOM vendor guard);
    they do not include those filters themselves. Shared by the live tag-scope
    resolver, the tag-scope preview, and the latestCollected snapshot probe so
    every path builds an identical scope predicate.
    """
    clauses: list[str] = []
    include_exprs = [
        f"{_nqe_string_literal(tag)} in device.tagNames" for tag in include_tags
    ]
    if include_exprs:
        if include_match == "all":
            clauses.extend(f"where {expr}" for expr in include_exprs)
        else:
            clauses.append(f"where ({' || '.join(include_exprs)})")
    for tag in exclude_tags:
        clauses.append(f"where !({_nqe_string_literal(tag)} in device.tagNames)")
    return clauses


class ForwardClient:
    def __init__(self, source):
        self.source = source
        params = source.parameters or {}
        self.timeout = params.get("timeout") or DEFAULT_FORWARD_API_TIMEOUT_SECONDS
        self.retries = self._coerce_retry_count(params.get("retries"))
        self.verify = params.get("verify", True)
        self.nqe_page_size = self._coerce_nqe_page_size(params.get("nqe_page_size"))
        self.nqe_fetch_all_max_pages = self._coerce_nqe_fetch_all_max_pages(
            params.get("nqe_fetch_all_max_pages")
        )
        self.nqe_identical_full_page_streak_limit = (
            self._coerce_nqe_identical_full_page_streak_limit(
                params.get("nqe_identical_full_page_streak_limit")
            )
        )
        self.nqe_async_poll_interval_seconds = (
            self._coerce_nqe_async_poll_interval_seconds(
                params.get("nqe_async_poll_interval_seconds")
            )
        )
        self.nqe_async_max_polls = self._coerce_nqe_async_max_polls(
            params.get("nqe_async_max_polls")
        )
        self.base_url = source.url.rstrip("/")
        self.username = params.get("username")
        self.password = params.get("password")
        self.api_requests_per_minute = self._coerce_api_requests_per_minute(
            params.get("api_requests_per_minute")
        )
        if self.api_requests_per_minute:
            self._api_request_min_interval = 60.0 / self.api_requests_per_minute
        else:
            self._api_request_min_interval = 0.0
        self._api_usage_lock = threading.Lock()
        self._read_cache_lock = threading.Lock()
        self._api_usage_first_http_attempt_at = None
        self._api_usage_last_http_attempt_at = None
        self._api_usage = self._empty_api_usage()
        self._latest_processed_snapshot_cache: dict[str, dict] = {}
        self._snapshots_cache: dict[tuple[str, bool, int], list[dict]] = {}
        self._snapshot_metrics_cache: dict[str, dict] = {}
        self._networks_cache: list[dict] | None = None
        self._committed_nqe_query_cache: dict[tuple[str, str, str], dict] = {}
        self._org_nqe_queries_cache: dict[str, list[dict]] = {}
        self._repository_queries_cache: dict[tuple[str, str], list[dict]] = {}
        self._repository_query_index_cache: dict[tuple[str, str, int], dict] = {}
        self._nqe_query_history_cache: dict[str, list[dict]] = {}
        self._org_nqe_head_commit_id_cache: str | None = None

    def _empty_api_usage(self):
        return {
            "api_requests_per_minute": self.api_requests_per_minute,
            "http_attempts": 0,
            "http_successes": 0,
            "http_failures": 0,
            "http_timeout_failures": 0,
            "http_transport_failures": 0,
            "http_status_failures": 0,
            "http_transient_status_failures": 0,
            "http_nontransient_status_failures": 0,
            "http_429_failures": 0,
            "http_retries": 0,
            "http_status_classes": {},
            "throttle_sleep_seconds": 0.0,
            "nqe_query_calls": 0,
            "nqe_diff_calls": 0,
            "nqe_pages": 0,
            "nqe_query_pages": 0,
            "nqe_diff_pages": 0,
            "nqe_async_query_calls": 0,
            "nqe_async_trigger_calls": 0,
            "nqe_async_status_calls": 0,
            "nqe_async_result_calls": 0,
            "read_cache_hits": 0,
            "read_cache_misses": 0,
        }

    def _record_api_usage(self, key, amount=1):
        with self._api_usage_lock:
            self._api_usage[key] = self._api_usage.get(key, 0) + amount

    def _record_read_cache_hit(self):
        self._record_api_usage("read_cache_hits")

    def _record_read_cache_miss(self):
        self._record_api_usage("read_cache_misses")

    def _invalidate_nqe_query_read_caches(self):
        with self._read_cache_lock:
            self._committed_nqe_query_cache.clear()
            self._org_nqe_queries_cache.clear()
            self._repository_queries_cache.clear()
            self._repository_query_index_cache.clear()
            self._nqe_query_history_cache.clear()
            self._org_nqe_head_commit_id_cache = None
        self._bump_shared_query_read_generation()

    def _shared_read_cache_scope(self) -> str:
        scope = {
            "source_pk": getattr(self.source, "pk", None),
            "source_type": getattr(self.source, "type", None) or "",
            "base_url": self.base_url,
            "username": self.username or "",
        }
        encoded = json.dumps(scope, sort_keys=True, separators=(",", ":"))
        digest = hashlib.sha256(encoded.encode("utf-8")).hexdigest()
        return f"forward-netbox:forward-api-read-cache:{digest}"

    def _shared_read_cache(self):
        return _shared_read_cache()

    def _shared_read_cache_key(self, kind: str, *parts: object) -> str:
        scope = self._shared_read_cache_scope()
        payload = {
            "kind": kind,
            "scope": scope,
            "parts": [str(part) for part in parts],
        }
        encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"))
        digest = hashlib.sha256(encoded.encode("utf-8")).hexdigest()
        return f"{scope}:{kind}:{digest}"

    def _shared_query_read_generation_key(self) -> str:
        return f"{self._shared_read_cache_scope()}{READ_CACHE_GENERATION_KEY_SUFFIX}"

    def _shared_query_read_generation(self) -> int:
        cache = self._shared_read_cache()
        if cache is None:
            return 0
        try:
            return int(cache.get(self._shared_query_read_generation_key()) or 0)
        except (TypeError, ValueError):
            return 0

    def _bump_shared_query_read_generation(self) -> None:
        cache = self._shared_read_cache()
        if cache is None:
            return
        key = self._shared_query_read_generation_key()
        try:
            cache.incr(key)
        except Exception:
            try:
                current = int(cache.get(key) or 0)
            except (TypeError, ValueError):
                current = 0
            cache.set(key, current + 1, timeout=READ_CACHE_TIMEOUT_SECONDS)

    def _shared_read_cache_get(self, key: str):
        cache = self._shared_read_cache()
        if cache is None:
            return None
        return cache.get(key)

    def _shared_read_cache_set(self, key: str, value):
        cache = self._shared_read_cache()
        if cache is None:
            return
        cache.set(key, value, timeout=READ_CACHE_TIMEOUT_SECONDS)

    def _build_nqe_repository_query_index(self, rows: list[dict]) -> dict:
        by_query_id: dict[str, list[dict]] = {}
        by_path: dict[str, dict] = {}
        normalized_rows = []
        for row in rows or []:
            if not isinstance(row, dict):
                continue
            normalized = dict(row)
            normalized_rows.append(normalized)
            query_id = str(normalized.get("queryId") or "").strip()
            if query_id:
                by_query_id.setdefault(query_id, []).append(normalized)
            path = str(normalized.get("path") or "").strip()
            if path:
                by_path[path] = normalized
        return {
            "rows": normalized_rows,
            "by_query_id": by_query_id,
            "by_path": by_path,
        }

    def _copy_nqe_repository_query_index(self, index: dict | None) -> dict:
        index = index or {}
        return {
            "rows": [dict(row) for row in index.get("rows") or []],
            "by_query_id": {
                query_id: [dict(row) for row in rows]
                for query_id, rows in (index.get("by_query_id") or {}).items()
            },
            "by_path": {
                path: dict(row) for path, row in (index.get("by_path") or {}).items()
            },
        }

    def _record_http_attempt_usage(self):
        now = time.monotonic()
        with self._api_usage_lock:
            self._api_usage["http_attempts"] = (
                self._api_usage.get("http_attempts", 0) + 1
            )
            if self._api_usage_first_http_attempt_at is None:
                self._api_usage_first_http_attempt_at = now
            self._api_usage_last_http_attempt_at = now

    def _record_http_status_class(self, status_code):
        if not isinstance(status_code, int):
            return
        class_name = f"{status_code // 100}xx"
        with self._api_usage_lock:
            classes = self._api_usage.setdefault("http_status_classes", {})
            classes[class_name] = classes.get(class_name, 0) + 1

    def api_usage_summary(self):
        with self._api_usage_lock:
            summary = dict(self._api_usage)
            summary["http_status_classes"] = dict(
                self._api_usage.get("http_status_classes") or {}
            )
            first_attempt_at = self._api_usage_first_http_attempt_at
            last_attempt_at = self._api_usage_last_http_attempt_at
        summary["throttle_sleep_seconds"] = round(
            float(summary.get("throttle_sleep_seconds") or 0.0),
            6,
        )
        window_seconds = (
            max(float(last_attempt_at) - float(first_attempt_at), 0.0)
            if first_attempt_at is not None and last_attempt_at is not None
            else 0.0
        )
        http_attempts = int(summary.get("http_attempts") or 0)
        observed_rate = (
            round(((http_attempts - 1) * 60.0) / window_seconds, 3)
            if http_attempts > 1 and window_seconds > 0
            else None
        )
        summary["usage_window_seconds"] = round(window_seconds, 6)
        summary["observed_http_attempts_per_minute"] = observed_rate
        read_cache_hits = int(summary.get("read_cache_hits") or 0)
        read_cache_misses = int(summary.get("read_cache_misses") or 0)
        total_read_cache_lookups = read_cache_hits + read_cache_misses
        summary["read_cache_hit_rate"] = (
            round(read_cache_hits / float(total_read_cache_lookups), 6)
            if total_read_cache_lookups
            else None
        )
        return summary

    def reset_api_usage_summary(self):
        with self._api_usage_lock:
            self._api_usage_first_http_attempt_at = None
            self._api_usage_last_http_attempt_at = None
            self._api_usage = self._empty_api_usage()

    def _coerce_nqe_page_size(self, value):
        if value is None:
            return DEFAULT_NQE_PAGE_SIZE
        try:
            size = int(value)
        except (TypeError, ValueError):
            return DEFAULT_NQE_PAGE_SIZE
        return max(1, min(size, MAX_NQE_PAGE_SIZE))

    def _coerce_retry_count(self, value):
        if value is None:
            return DEFAULT_FORWARD_API_RETRIES
        try:
            retries = int(value)
        except (TypeError, ValueError):
            return DEFAULT_FORWARD_API_RETRIES
        return max(0, min(retries, 5))

    def _coerce_nqe_fetch_all_max_pages(self, value):
        if value is None:
            return DEFAULT_NQE_FETCH_ALL_MAX_PAGES
        try:
            pages = int(value)
        except (TypeError, ValueError):
            return DEFAULT_NQE_FETCH_ALL_MAX_PAGES
        return max(1, min(pages, MAX_NQE_FETCH_ALL_MAX_PAGES))

    def _coerce_nqe_identical_full_page_streak_limit(self, value):
        if value is None:
            return DEFAULT_NQE_IDENTICAL_FULL_PAGE_STREAK_LIMIT
        try:
            streak = int(value)
        except (TypeError, ValueError):
            return DEFAULT_NQE_IDENTICAL_FULL_PAGE_STREAK_LIMIT
        return max(1, min(streak, MAX_NQE_IDENTICAL_FULL_PAGE_STREAK_LIMIT))

    def _coerce_api_requests_per_minute(self, value):
        if value in (None, ""):
            return self._default_api_requests_per_minute()
        try:
            requests_per_minute = int(value)
        except (TypeError, ValueError):
            return self._default_api_requests_per_minute()
        return max(
            0,
            min(requests_per_minute, MAX_FORWARD_API_REQUESTS_PER_MINUTE),
        )

    def _default_api_requests_per_minute(self):
        source_type = str(getattr(self.source, "type", "") or "").lower()
        if source_type == "saas" or self.base_url == "https://fwd.app":
            return DEFAULT_FORWARD_SAAS_API_REQUESTS_PER_MINUTE
        return DEFAULT_FORWARD_API_REQUESTS_PER_MINUTE

    def _coerce_bool(self, value, default=False):
        if value in (None, ""):
            return bool(default)
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            return value.strip().lower() in {"1", "true", "yes", "on"}
        return bool(value)

    def _coerce_nqe_async_poll_interval_seconds(self, value):
        if value is None:
            return DEFAULT_NQE_ASYNC_POLL_INTERVAL_SECONDS
        try:
            interval = float(value)
        except (TypeError, ValueError):
            return DEFAULT_NQE_ASYNC_POLL_INTERVAL_SECONDS
        return max(0.0, min(interval, MAX_NQE_ASYNC_POLL_INTERVAL_SECONDS))

    def _coerce_nqe_async_max_polls(self, value):
        if value is None:
            return DEFAULT_NQE_ASYNC_MAX_POLLS
        try:
            polls = int(value)
        except (TypeError, ValueError):
            return DEFAULT_NQE_ASYNC_MAX_POLLS
        return max(1, min(polls, MAX_NQE_ASYNC_MAX_POLLS))

    def _page_signature(self, rows):
        if not rows:
            return None
        first = rows[0]
        last = rows[-1]
        return (
            len(rows),
            json.dumps(first, sort_keys=True, default=str),
            json.dumps(last, sort_keys=True, default=str),
        )

    def _api_url(self, path):
        base_url = self.base_url
        if base_url.endswith("/api"):
            return f"{base_url}{path}"
        return f"{base_url}/api{path}"

    def _headers(self, *, accept=None):
        headers = {
            "Accept": accept or "application/json",
            "Content-Type": "application/json",
            "User-Agent": "forward-netbox/0.8.6.3",
        }
        return headers

    def _auth(self):
        if self.username and self.password:
            return (self.username, self.password)
        return None

    def _rate_limit_key(self):
        scope = f"{self.base_url}\0{self.username or ''}"
        digest = hashlib.sha256(scope.encode("utf-8")).hexdigest()
        return f"forward-netbox:forward-api-rate-limit:{digest}"

    def _sleep_for_rate_limit(self, last_request_at, now):
        if last_request_at is None:
            return now
        wait_seconds = self._api_request_min_interval - (now - float(last_request_at))
        if wait_seconds > 0:
            self._record_api_usage("throttle_sleep_seconds", wait_seconds)
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

    def _throttle_request(self):
        if not self._api_request_min_interval:
            return
        key = self._rate_limit_key()
        cache = _shared_rate_limit_cache()
        if cache is None:
            self._throttle_in_process(key)
            return
        try:
            self._throttle_with_shared_cache(key, cache)
        except Exception:
            self._throttle_in_process(key)

    def _proxy_mounts(self, url):
        if resolve_proxies is None:
            return None
        proxies = resolve_proxies(
            url=url,
            context={
                "client": self,
                "source": self.source,
            },
        )
        if not proxies:
            return None

        mounts = {}
        for protocol, proxy_url in proxies.items():
            if not proxy_url:
                continue
            protocol = str(protocol).rstrip(":/")
            mounts[f"{protocol}://"] = httpx.HTTPTransport(proxy=proxy_url)
        return mounts or None

    def _request(self, method, path, *, params=None, json_body=None, headers=None):
        url = self._api_url(path)
        last_connectivity_error = None
        for attempt in range(self.retries + 1):
            try:
                with httpx.Client(
                    timeout=self.timeout,
                    verify=self.verify,
                    mounts=self._proxy_mounts(url),
                ) as client:
                    self._throttle_request()
                    self._record_http_attempt_usage()
                    response = client.request(
                        method,
                        url,
                        params=params,
                        json=json_body,
                        headers={**self._headers(), **(headers or {})},
                        auth=self._auth(),
                    )
                response.raise_for_status()
                self._record_api_usage("http_successes")
                self._record_http_status_class(getattr(response, "status_code", None))
                return response
            except httpx.TimeoutException as exc:
                self._record_api_usage("http_failures")
                self._record_api_usage("http_timeout_failures")
                last_connectivity_error = ForwardConnectivityError(
                    "Forward API request timed out while connecting to Forward."
                )
                last_connectivity_error.__cause__ = exc
            except httpx.RequestError as exc:
                self._record_api_usage("http_failures")
                self._record_api_usage("http_transport_failures")
                last_connectivity_error = ForwardConnectivityError(
                    f"Could not connect to Forward API endpoint: {exc}"
                )
                last_connectivity_error.__cause__ = exc
            except httpx.HTTPStatusError as exc:
                status_code = exc.response.status_code
                self._record_api_usage("http_failures")
                self._record_api_usage("http_status_failures")
                self._record_http_status_class(status_code)
                if status_code in TRANSIENT_FORWARD_HTTP_STATUS_CODES:
                    self._record_api_usage("http_transient_status_failures")
                    if status_code == 429:
                        self._record_api_usage("http_429_failures")
                    last_connectivity_error = ForwardConnectivityError(
                        "Forward API request returned transient HTTP "
                        f"{status_code}; retry attempts were exhausted."
                    )
                    last_connectivity_error.__cause__ = exc
                else:
                    self._record_api_usage("http_nontransient_status_failures")
                    raise ForwardClientError(
                        "Forward API request failed with HTTP "
                        f"{status_code}: {exc.response.text}"
                    ) from exc
            except httpx.HTTPError as exc:
                self._record_api_usage("http_failures")
                self._record_api_usage("http_transport_failures")
                raise ForwardClientError(f"Forward API request failed: {exc}") from exc
            if attempt < self.retries:
                self._record_api_usage("http_retries")
                time.sleep(DEFAULT_FORWARD_API_RETRY_BACKOFF_SECONDS * (attempt + 1))
        raise last_connectivity_error

    def get_networks(self):
        shared_cache_key = self._shared_read_cache_key("networks")
        with self._read_cache_lock:
            cached_networks = self._networks_cache
        if cached_networks is not None:
            self._record_read_cache_hit()
            return [dict(item) for item in cached_networks]
        cached_networks = self._shared_read_cache_get(shared_cache_key)
        if cached_networks is not None:
            with self._read_cache_lock:
                self._networks_cache = [dict(item) for item in cached_networks]
            self._record_read_cache_hit()
            return [dict(item) for item in cached_networks]
        self._record_read_cache_miss()
        response = self._request("GET", "/networks")
        data = response.json()
        networks = []
        for item in data or []:
            network_id = str(item.get("id", "")).strip()
            name = str(item.get("name", "")).strip()
            if not network_id or not name:
                continue
            networks.append(
                {
                    "id": network_id,
                    "name": name,
                    "label": f"{name} ({network_id})",
                }
            )
        with self._read_cache_lock:
            self._networks_cache = [dict(item) for item in networks]
        self._shared_read_cache_set(shared_cache_key, [dict(item) for item in networks])
        return networks

    def get_snapshots(self, network_id, *, include_archived=False, limit=100):
        network_id = str(network_id or "").strip()
        cache_key = (network_id, bool(include_archived), int(limit))
        shared_cache_key = self._shared_read_cache_key(
            "snapshots", network_id, bool(include_archived), int(limit)
        )
        with self._read_cache_lock:
            cached_snapshots = self._snapshots_cache.get(cache_key)
        if cached_snapshots is not None:
            self._record_read_cache_hit()
            return [dict(item) for item in cached_snapshots]
        cached_snapshots = self._shared_read_cache_get(shared_cache_key)
        if cached_snapshots is not None:
            with self._read_cache_lock:
                self._snapshots_cache[cache_key] = [
                    dict(item) for item in cached_snapshots
                ]
            self._record_read_cache_hit()
            return [dict(item) for item in cached_snapshots]
        self._record_read_cache_miss()
        response = self._request(
            "GET",
            f"/networks/{network_id}/snapshots",
            params={
                "includeArchived": str(bool(include_archived)).lower(),
                "limit": limit,
            },
        )
        data = response.json() or {}
        snapshots = []
        for item in data.get("snapshots") or []:
            snapshot_id = str(item.get("id", "")).strip()
            if not snapshot_id:
                continue
            state = str(item.get("state", "")).strip()
            created = str(item.get("createdAt", "")).strip()
            processed = str(item.get("processedAt", "")).strip()
            label_parts = [snapshot_id]
            if state:
                label_parts.append(state)
            if processed:
                label_parts.append(processed)
            elif created:
                label_parts.append(created)
            snapshots.append(
                {
                    "id": snapshot_id,
                    "state": state,
                    "created_at": created,
                    "processed_at": processed,
                    "label": " | ".join(label_parts),
                }
            )
        with self._read_cache_lock:
            self._snapshots_cache[cache_key] = [dict(item) for item in snapshots]
        self._shared_read_cache_set(
            shared_cache_key, [dict(item) for item in snapshots]
        )
        return snapshots

    def get_latest_processed_snapshot(self, network_id):
        network_id = str(network_id or "").strip()
        shared_cache_key = self._shared_read_cache_key(
            "latest-processed-snapshot", network_id
        )
        with self._read_cache_lock:
            cached_snapshot = self._latest_processed_snapshot_cache.get(network_id)
        if cached_snapshot is not None:
            self._record_read_cache_hit()
            return dict(cached_snapshot)
        cached_snapshot = self._shared_read_cache_get(shared_cache_key)
        if cached_snapshot is not None:
            with self._read_cache_lock:
                self._latest_processed_snapshot_cache[network_id] = dict(
                    cached_snapshot
                )
            self._record_read_cache_hit()
            return dict(cached_snapshot)
        self._record_read_cache_miss()
        response = self._request(
            "GET", f"/networks/{network_id}/snapshots/latestProcessed"
        )
        snapshot = response.json() or {}
        if isinstance(snapshot, dict):
            with self._read_cache_lock:
                self._latest_processed_snapshot_cache[network_id] = dict(snapshot)
            self._shared_read_cache_set(shared_cache_key, dict(snapshot))
        return snapshot

    def get_latest_processed_snapshot_id(self, network_id):
        snapshot = self.get_latest_processed_snapshot(network_id)
        snapshot_id = str(snapshot.get("id", "")).strip()
        if not snapshot_id:
            raise ForwardClientError(
                "Forward latestProcessed snapshot response did not include an ID."
            )
        return snapshot_id

    def _processed_snapshots_newest_first(self, network_id):
        """Return processed snapshots for a network, newest processed first."""
        snapshots = [
            dict(snapshot)
            for snapshot in self.get_snapshots(network_id)
            if str(snapshot.get("state", "")).strip().upper() == "PROCESSED"
        ]
        snapshots.sort(
            key=lambda snapshot: (
                str(snapshot.get("processed_at") or "").strip(),
                str(snapshot.get("created_at") or "").strip(),
                str(snapshot.get("id") or "").strip(),
            ),
            reverse=True,
        )
        return snapshots

    def get_latest_collected_snapshot_id(
        self,
        network_id,
        *,
        include_tags=None,
        exclude_tags=None,
        include_match="any",
        scan_limit=DEFAULT_LATEST_COLLECTED_SCAN_LIMIT,
    ):
        """Resolve the newest processed snapshot that has a freshly-collected
        in-scope device.

        Walks the most recent processed snapshots (newest first, bounded by
        ``scan_limit``) and returns the first whose device-tag scope contains at
        least one device with ``snapshotInfo.result == completed``. This skips
        snapshots where the in-scope devices were backfilled because collection
        was canceled. Raises ``ForwardClientError`` when no scanned snapshot has
        a collected in-scope device.
        """
        network_id = str(network_id or "").strip()
        if not network_id:
            raise ForwardClientError(
                "get_latest_collected_snapshot_id requires a network_id."
            )
        include_tags = [
            str(tag).strip() for tag in (include_tags or []) if str(tag).strip()
        ]
        exclude_tags = [
            str(tag).strip() for tag in (exclude_tags or []) if str(tag).strip()
        ]
        if include_match not in {"any", "all"}:
            include_match = "any"
        try:
            scan_limit = int(scan_limit)
        except (TypeError, ValueError):
            scan_limit = DEFAULT_LATEST_COLLECTED_SCAN_LIMIT
        if scan_limit < 1:
            scan_limit = 1

        scope_where = build_device_tag_scope_where(
            include_tags, exclude_tags, include_match
        )
        probe_query = "\n".join(
            [
                "foreach device in network.devices",
                "where device.snapshotInfo.result == DeviceSnapshotResult.completed",
                "where device.platform.vendor != Vendor.FORWARD_CUSTOM",
                *scope_where,
                "select {name: device.name}",
            ]
        )

        snapshots = self._processed_snapshots_newest_first(network_id)
        if not snapshots:
            raise ForwardClientError(
                "No processed snapshot is available for the configured network "
                "to resolve the latestCollected selector."
            )

        scanned = 0
        for snapshot in snapshots[:scan_limit]:
            snapshot_id = str(snapshot.get("id") or "").strip()
            if not snapshot_id:
                continue
            scanned += 1
            rows = self.run_nqe_query(
                query=probe_query,
                network_id=network_id,
                snapshot_id=snapshot_id,
                limit=1,
                fetch_all=False,
            )
            if any(str(row.get("name") or "").strip() for row in rows):
                return snapshot_id

        scope_hint = (
            f" matching device tag scope (include={include_tags or ['-']}, "
            f"include_match={include_match}, exclude={exclude_tags or ['-']})"
            if (include_tags or exclude_tags)
            else ""
        )
        raise ForwardClientError(
            f"None of the {scanned} most recent processed snapshot(s) have a "
            f"collected device{scope_hint}; every in-scope device appears "
            "backfilled (collection canceled). Pin a specific snapshot, widen "
            "the device tag scope, or re-run collection in Forward."
        )

    def get_snapshot_metrics(self, snapshot_id):
        snapshot_id = str(snapshot_id or "").strip()
        shared_cache_key = self._shared_read_cache_key("snapshot-metrics", snapshot_id)
        with self._read_cache_lock:
            cached_metrics = self._snapshot_metrics_cache.get(snapshot_id)
        if cached_metrics is not None:
            self._record_read_cache_hit()
            return dict(cached_metrics)
        cached_metrics = self._shared_read_cache_get(shared_cache_key)
        if cached_metrics is not None:
            with self._read_cache_lock:
                self._snapshot_metrics_cache[snapshot_id] = dict(cached_metrics)
            self._record_read_cache_hit()
            return dict(cached_metrics)
        self._record_read_cache_miss()
        response = self._request("GET", f"/snapshots/{snapshot_id}/metrics")
        metrics = response.json() or {}
        if isinstance(metrics, dict):
            with self._read_cache_lock:
                self._snapshot_metrics_cache[snapshot_id] = dict(metrics)
            self._shared_read_cache_set(shared_cache_key, dict(metrics))
        return metrics

    def trigger_snapshot_reachability(self, network_id, snapshot_id):
        """Trigger advanced reachability computation for a snapshot (FWD-53559).

        POSTs to start computation, then polls until the job reaches a terminal
        state. Returns the final status payload. Raises ForwardClientError on
        failure or timeout.
        """
        network_id = str(network_id or "").strip()
        snapshot_id = str(snapshot_id or "").strip()
        if not network_id or not snapshot_id:
            raise ForwardClientError(
                "trigger_snapshot_reachability requires both network_id and snapshot_id."
            )
        self._record_api_usage("reachability_trigger_calls")
        response = self._request(
            "POST",
            "/networks/{network_id}/snapshots/{snapshot_id}/reachability".format(
                network_id=quote(str(network_id), safe=""),
                snapshot_id=quote(str(snapshot_id), safe=""),
            ),
        )
        data = response.json() or {}
        job_key = str(
            data.get("jobKey") or data.get("executionKey") or data.get("id") or ""
        ).strip()
        state = str(data.get("status") or "").strip().upper()
        if state in ("COMPLETED", "DONE", "READY") or not job_key:
            return data
        return self._wait_for_reachability_completion(
            network_id=network_id,
            snapshot_id=snapshot_id,
            job_key=job_key,
            status=data,
        )

    def _wait_for_reachability_completion(
        self, *, network_id, snapshot_id, job_key, status
    ):
        current_status = status or {}
        poll_ceiling = self.nqe_async_poll_interval_seconds
        for poll_index in range(self.nqe_async_max_polls + 1):
            state = str(current_status.get("status") or "").strip().upper()
            if state in ("COMPLETED", "DONE", "READY"):
                return current_status
            if state in ("FAILED", "ERROR"):
                error = current_status.get("error") or current_status
                raise ForwardClientError(
                    f"Forward reachability computation failed for snapshot "
                    f"`{snapshot_id}`: {error}"
                )
            if poll_index >= self.nqe_async_max_polls:
                break
            if poll_ceiling:
                sleep_seconds = min(poll_ceiling, 0.1 * (2**poll_index))
                time.sleep(sleep_seconds)
            self._record_api_usage("reachability_status_calls")
            response = self._request(
                "GET",
                "/networks/{network_id}/snapshots/{snapshot_id}/reachability/{job_key}".format(
                    network_id=quote(str(network_id), safe=""),
                    snapshot_id=quote(str(snapshot_id), safe=""),
                    job_key=quote(str(job_key), safe=""),
                ),
            )
            current_status = response.json() or {}
        raise ForwardClientError(
            f"Forward reachability computation did not complete after "
            f"{self.nqe_async_max_polls} poll(s) for snapshot `{snapshot_id}`."
        )

    def get_org_nqe_queries(self, *, directory="/"):
        directory = _normalize_nqe_directory(directory)
        shared_cache_key = self._shared_read_cache_key(
            "org-nqe-queries", directory, self._shared_query_read_generation()
        )
        cached_queries = self._org_nqe_queries_cache.get(directory)
        if cached_queries is not None:
            self._record_read_cache_hit()
            return [dict(row) for row in cached_queries]
        cached_queries = self._shared_read_cache_get(shared_cache_key)
        if cached_queries is not None:
            self._org_nqe_queries_cache[directory] = list(cached_queries)
            self._record_read_cache_hit()
            return [dict(row) for row in cached_queries]
        self._record_read_cache_miss()
        params = {"dir": directory}
        response = self._request("GET", "/nqe/queries", params=params or None)
        data = response.json() or []
        rows = data if isinstance(data, list) else []
        self._org_nqe_queries_cache[directory] = list(rows)
        self._shared_read_cache_set(shared_cache_key, list(rows))
        return rows if isinstance(rows, list) else []

    def get_nqe_repository_queries(self, *, repository="org", directory="/"):
        repository = _normalize_nqe_repository(repository)
        directory = _normalize_nqe_directory(directory)
        if repository == "org":
            rows = self.get_org_nqe_queries(directory=directory)
            return [
                normalized
                for row in rows
                if (normalized := _normalize_nqe_query_row(row, repository=repository))
            ]

        cache_key = (repository, directory)
        shared_cache_key = self._shared_read_cache_key(
            "repository-nqe-queries",
            repository,
            directory,
            self._shared_query_read_generation(),
        )
        cached_queries = self._repository_queries_cache.get(cache_key)
        if cached_queries is not None:
            self._record_read_cache_hit()
            return [dict(row) for row in cached_queries]
        cached_queries = self._shared_read_cache_get(shared_cache_key)
        if cached_queries is not None:
            self._repository_queries_cache[cache_key] = [
                dict(row) for row in cached_queries
            ]
            self._record_read_cache_hit()
            return [dict(row) for row in cached_queries]
        self._record_read_cache_miss()

        response = self._request(
            "GET",
            f"/nqe/repos/{repository}/commits/head/queries",
        )
        data = response.json() or {}
        rows = data.get("queries") if isinstance(data, dict) else []
        normalized_rows = [
            normalized
            for row in rows or []
            if _query_in_directory(row.get("path"), directory)
            if (normalized := _normalize_nqe_query_row(row, repository=repository))
        ]
        self._repository_queries_cache[cache_key] = [
            dict(row) for row in normalized_rows
        ]
        self._shared_read_cache_set(
            shared_cache_key, [dict(row) for row in normalized_rows]
        )
        return normalized_rows

    def get_nqe_repository_query_index(self, *, repository="org", directory="/"):
        repository = _normalize_nqe_repository(repository)
        directory = _normalize_nqe_directory(directory)
        generation = self._shared_query_read_generation()
        cache_key = (repository, directory, generation)
        with self._read_cache_lock:
            cached_index = self._repository_query_index_cache.get(cache_key)
        if cached_index is not None:
            self._record_read_cache_hit()
            return self._copy_nqe_repository_query_index(cached_index)
        shared_cache_key = self._shared_read_cache_key(
            "repository-nqe-query-index",
            repository,
            directory,
            generation,
        )
        cached_index = self._shared_read_cache_get(shared_cache_key)
        if cached_index is not None:
            with self._read_cache_lock:
                self._repository_query_index_cache[cache_key] = (
                    self._copy_nqe_repository_query_index(cached_index)
                )
            self._record_read_cache_hit()
            return self._copy_nqe_repository_query_index(cached_index)
        rows = self.get_nqe_repository_queries(
            repository=repository,
            directory=directory,
        )
        index = self._build_nqe_repository_query_index(rows)
        with self._read_cache_lock:
            self._repository_query_index_cache[cache_key] = (
                self._copy_nqe_repository_query_index(index)
            )
        self._shared_read_cache_set(shared_cache_key, index)
        return index

    def get_committed_nqe_query(
        self,
        *,
        repository="org",
        query_path="",
        commit_id="head",
        query_index: dict | None = None,
        require_source_code=False,
    ):
        repository = _normalize_nqe_repository(repository)
        query_path = _normalize_nqe_query_path(query_path)
        commit_id = str(commit_id or "head").strip() or "head"
        if not query_path:
            raise ForwardClientError("Forward NQE query path is required.")
        if commit_id == "head":
            if query_index is None:
                try:
                    query_index = self.get_nqe_repository_query_index(
                        repository=repository,
                        directory="/",
                    )
                except Exception:
                    query_index = {}
            indexed_query = (query_index.get("by_path") or {}).get(query_path)
            if indexed_query and indexed_query.get("queryId"):
                # Ensure source code is available when requested for canonicality checks.
                # Some API responses return directory rows without source text.
                has_source = any(
                    indexed_query.get(key) for key in ("sourceCode", "source", "query")
                )
                if not require_source_code or has_source:
                    query = dict(indexed_query)
                    query.setdefault("repository", repository)
                    query.setdefault("intent", "")
                    query.setdefault("lastCommitId", "")
                    cache_key = (
                        repository,
                        query_path,
                        commit_id,
                        bool(require_source_code),
                    )
                    self._committed_nqe_query_cache[cache_key] = dict(query)
                    shared_cache_key = self._shared_read_cache_key(
                        "committed-nqe-query",
                        repository,
                        query_path,
                        commit_id,
                        bool(require_source_code),
                        self._shared_query_read_generation(),
                    )
                    self._shared_read_cache_set(shared_cache_key, dict(query))
                    return query
                commit_id = (
                    str(indexed_query.get("lastCommitId") or commit_id).strip()
                    or "head"
                )

        cache_key = (repository, query_path, commit_id, bool(require_source_code))
        shared_cache_key = self._shared_read_cache_key(
            "committed-nqe-query",
            repository,
            query_path,
            commit_id,
            bool(require_source_code),
            self._shared_query_read_generation(),
        )
        cached_query = self._committed_nqe_query_cache.get(cache_key)
        if cached_query is not None:
            self._record_read_cache_hit()
            return dict(cached_query)
        cached_query = self._shared_read_cache_get(shared_cache_key)
        if cached_query is not None:
            self._committed_nqe_query_cache[cache_key] = dict(cached_query)
            self._record_read_cache_hit()
            return dict(cached_query)
        self._record_read_cache_miss()
        response = self._request(
            "GET",
            f"/nqe/repos/{repository}/commits/{quote(commit_id, safe='')}/queries",
            params={"path": query_path, "with": "sourceCode"},
        )
        data = response.json() or {}
        if not isinstance(data, dict):
            raise ForwardClientError(
                f"Forward NQE repository lookup for `{query_path}` returned an invalid response."
            )
        if isinstance(data.get("queries"), list):
            for query in data["queries"]:
                if isinstance(query, dict) and query.get("path") == query_path:
                    normalized = dict(query)
                    normalized.setdefault("lastCommitId", "")
                    self._committed_nqe_query_cache[cache_key] = dict(normalized)
                    self._shared_read_cache_set(shared_cache_key, dict(normalized))
                    return normalized
            raise ForwardClientError(
                f"Forward NQE repository lookup did not include `{query_path}`."
            )
        normalized = dict(data)
        last_commit = normalized.get("lastCommit") or {}
        normalized.setdefault(
            "lastCommitId",
            str(last_commit.get("id") or normalized.get("lastCommitId") or "").strip(),
        )
        self._committed_nqe_query_cache[cache_key] = dict(normalized)
        self._shared_read_cache_set(shared_cache_key, dict(normalized))
        return normalized

    def resolve_nqe_query_reference(
        self, *, repository="org", query_path="", commit_id=None, query_index=None
    ):
        repository = _normalize_nqe_repository(repository)
        query_path = _normalize_nqe_query_path(query_path)
        explicit_commit_id = str(commit_id or "").strip()
        requested_commit_id = explicit_commit_id or "head"
        query = self.get_committed_nqe_query(
            repository=repository,
            query_path=query_path,
            commit_id=requested_commit_id,
            query_index=query_index,
        )
        query_id = str(query.get("queryId") or "").strip()
        if not query_id:
            raise ForwardClientError(
                f"Forward NQE query `{repository}:{query_path}` did not include a query ID."
            )
        last_commit = query.get("lastCommit") or {}
        resolved_commit_id = str(
            explicit_commit_id
            or last_commit.get("id")
            or query.get("lastCommitId")
            or ""
        ).strip()
        return {
            "queryId": query_id,
            "commitId": resolved_commit_id,
            "repository": str(query.get("repository") or repository).strip(),
            "path": str(query.get("path") or query_path).strip(),
            "intent": str(query.get("intent") or "").strip(),
        }

    def get_nqe_query_history(self, query_id):
        query_id = str(query_id or "").strip()
        if not query_id:
            return []
        shared_cache_key = self._shared_read_cache_key(
            "nqe-query-history", query_id, self._shared_query_read_generation()
        )
        cached_history = self._nqe_query_history_cache.get(query_id)
        if cached_history is not None:
            self._record_read_cache_hit()
            return [dict(row) for row in cached_history]
        cached_history = self._shared_read_cache_get(shared_cache_key)
        if cached_history is not None:
            self._nqe_query_history_cache[query_id] = list(cached_history)
            self._record_read_cache_hit()
            return [dict(row) for row in cached_history]
        self._record_read_cache_miss()
        response = self._request(
            "GET",
            f"/nqe/queries/{quote(query_id, safe='')}/history",
        )
        data = response.json() or {}
        commits = data.get("commits") if isinstance(data, dict) else []
        rows = commits or []
        self._nqe_query_history_cache[query_id] = list(rows)
        self._shared_read_cache_set(shared_cache_key, list(rows))
        return rows

    def add_org_nqe_query(self, *, query_path, source_code):
        query_path = _normalize_nqe_query_path(query_path)
        if not query_path:
            raise ForwardClientError("Forward NQE query path is required.")
        self._invalidate_nqe_query_read_caches()
        self._request(
            "POST",
            "/users/current/nqe/changes",
            params={"action": "addQuery", "path": query_path},
            json_body={"sourceCode": source_code},
        )

    def edit_org_nqe_query(self, *, query_path, source_code, query_id, commit_id):
        query_path = _normalize_nqe_query_path(query_path)
        query_id = str(query_id or "").strip()
        commit_id = str(commit_id or "").strip()
        if not query_path:
            raise ForwardClientError("Forward NQE query path is required.")
        if not query_id or not commit_id:
            raise ForwardClientError(
                "Forward NQE query ID and commit ID are required to update an existing query."
            )
        self._invalidate_nqe_query_read_caches()
        self._request(
            "POST",
            "/users/current/nqe/changes",
            params={"action": "editQuery", "path": query_path},
            json_body={
                "sourceCode": source_code,
                "basis": {
                    "queryId": query_id,
                    "commitId": commit_id,
                },
            },
        )

    def get_org_nqe_head_commit_id(self):
        shared_cache_key = self._shared_read_cache_key(
            "org-head-commit", self._shared_query_read_generation()
        )
        if self._org_nqe_head_commit_id_cache is not None:
            self._record_read_cache_hit()
            return self._org_nqe_head_commit_id_cache
        cached_commit = self._shared_read_cache_get(shared_cache_key)
        if cached_commit is not None:
            self._org_nqe_head_commit_id_cache = str(
                (cached_commit or {}).get("value") or ""
            )
            self._record_read_cache_hit()
            return self._org_nqe_head_commit_id_cache
        self._record_read_cache_miss()
        response = self._request("GET", "/nqe/repos/org/commits/head")
        data = response.json()
        commit_id = ""
        if isinstance(data, dict):
            commit_id = str(data.get("id") or data.get("commitId") or "").strip()
        else:
            commit_id = str(data or "").strip()
        self._org_nqe_head_commit_id_cache = commit_id
        self._shared_read_cache_set(shared_cache_key, {"value": commit_id})
        return commit_id

    def commit_org_nqe_queries(self, *, query_paths, message):
        import json as _json

        query_paths = [
            _normalize_nqe_query_path(query_path)
            for query_path in query_paths
            if _normalize_nqe_query_path(query_path)
        ]
        if not query_paths:
            return ""
        self._invalidate_nqe_query_read_caches()
        try:
            self._request(
                "POST",
                "/nqe/repos/org/commits",
                json_body={
                    "paths": query_paths,
                    "accessSettings": [],
                    "message": _commit_message_payload(message),
                },
            )
        except ForwardClientError as exc:
            msg = str(exc)
            if "HTTP 409" not in msg or "INVALID_CHANGE_PATH" not in msg:
                raise
            # Some paths had no staged changes (API accepted the edit but content
            # was already identical). Parse the no-change paths and retry without
            # them so the real changes still get committed.
            try:
                json_start = msg.index("{")
                body = _json.loads(msg[json_start:])
                no_change_text = body.get("message", "")
                prefix = "User has no changes at the following paths: "
                if prefix in no_change_text:
                    no_change_paths = {
                        p.strip()
                        for p in no_change_text[
                            no_change_text.index(prefix) + len(prefix) :
                        ]
                        .rstrip(".")
                        .split(",")
                    }
                    query_paths = [p for p in query_paths if p not in no_change_paths]
            except (ValueError, KeyError):
                raise exc
            if not query_paths:
                return self.get_org_nqe_head_commit_id()
            self._request(
                "POST",
                "/nqe/repos/org/commits",
                json_body={
                    "paths": query_paths,
                    "accessSettings": [],
                    "message": _commit_message_payload(message),
                },
            )
        return self.get_org_nqe_head_commit_id()

    def _parse_nqe_records(self, data):
        items = data.get("items") or []
        records = []
        for item in items:
            if isinstance(item, dict) and isinstance(item.get("fields"), dict):
                records.append(item["fields"])
            elif isinstance(item, dict):
                records.append(item)
            else:
                records.append(json.loads(json.dumps(item)))
        return records, data.get("totalNumItems")

    def _parse_nqe_lines(self, text):
        records = []
        for line in io.StringIO(str(text or "")):
            line = line.rstrip("\r\n")
            if not line:
                continue
            records.append(json.loads(line))
        return records, None

    def _parse_nqe_async_result(self, response):
        content_type = str(
            (getattr(response, "headers", {}) or {}).get("content-type") or ""
        ).lower()
        if "jsonl" in content_type or "ndjson" in content_type:
            return self._parse_nqe_lines(getattr(response, "text", ""))
        payload = response.json() or {}
        return self._parse_nqe_records(payload)

    def _parse_nqe_diff_rows(self, data):
        rows = data.get("rows") or []
        parsed_rows = []
        for row in rows:
            if not isinstance(row, dict):
                parsed_rows.append(json.loads(json.dumps(row)))
                continue
            parsed_rows.append(
                {
                    "type": row.get("type"),
                    "before": row.get("before"),
                    "after": row.get("after"),
                }
            )
        return parsed_rows, data.get("totalNumRows")

    def _nqe_async_execution_payload(
        self,
        *,
        query=None,
        query_id=None,
        commit_id=None,
        parameters=None,
        column_filters=None,
    ):
        payload = {"parameters": parameters or {}}
        if column_filters:
            payload["columnFilters"] = column_filters
        if query_id:
            payload["queryId"] = query_id
            execution_commit_id = _commit_id_for_nqe_execution(commit_id)
            if execution_commit_id:
                payload["commitId"] = execution_commit_id
        else:
            payload["query"] = query
        return payload

    def _nqe_async_status_state(self, status_payload):
        return str((status_payload or {}).get("status") or "").strip().upper()

    def _nqe_async_outcome(self, status_payload):
        return str((status_payload or {}).get("outcome") or "").strip().upper()

    def _nqe_async_error_summary(self, status_payload):
        error = (status_payload or {}).get("error")
        if isinstance(error, dict):
            return (
                error.get("message")
                or error.get("reason")
                or json.dumps(error, sort_keys=True, default=str)
            )
        if error:
            return str(error)
        return json.dumps(status_payload or {}, sort_keys=True, default=str)

    def _start_nqe_async_execution(
        self,
        *,
        query=None,
        query_id=None,
        commit_id=None,
        network_id,
        snapshot_id,
        parameters=None,
        column_filters=None,
    ):
        payload = self._nqe_async_execution_payload(
            query=query,
            query_id=query_id,
            commit_id=commit_id,
            parameters=parameters,
            column_filters=column_filters,
        )
        self._record_api_usage("nqe_async_trigger_calls")
        response = self._request(
            "POST",
            f"/networks/{quote(str(network_id), safe='')}/nqe-executions",
            params={"snapshotId": snapshot_id},
            json_body=payload,
        )
        data = response.json() or {}
        execution_key = str(data.get("executionKey") or "").strip()
        if not execution_key:
            raise ForwardClientError(
                "Forward async NQE execution did not return `executionKey`."
            )
        return execution_key, data

    def _get_nqe_async_status(self, *, network_id, execution_key):
        self._record_api_usage("nqe_async_status_calls")
        response = self._request(
            "GET",
            "/networks/{network_id}/nqe-executions/{execution_key}".format(
                network_id=quote(str(network_id), safe=""),
                execution_key=quote(str(execution_key), safe=""),
            ),
        )
        return response.json() or {}

    def _wait_for_nqe_async_completion(self, *, network_id, execution_key, status):
        current_status = status or {}
        poll_ceiling = self.nqe_async_poll_interval_seconds
        for poll_index in range(self.nqe_async_max_polls + 1):
            state = self._nqe_async_status_state(current_status)
            outcome = self._nqe_async_outcome(current_status)
            if state == "COMPLETED":
                if outcome == "OK":
                    return current_status
                raise ForwardClientError(
                    "Forward async NQE execution completed with outcome "
                    f"`{outcome or '<unknown>'}`: "
                    f"{self._nqe_async_error_summary(current_status)}"
                )
            if poll_index >= self.nqe_async_max_polls:
                break
            if poll_ceiling:
                # Exponential backoff: 0.1s → 0.2 → 0.4 → 0.8 → ceiling.
                # Fast queries return in <0.1s; slow ones plateau at the
                # configured ceiling so the poll budget stays meaningful.
                sleep_seconds = min(poll_ceiling, 0.1 * (2**poll_index))
                time.sleep(sleep_seconds)
            current_status = self._get_nqe_async_status(
                network_id=network_id,
                execution_key=execution_key,
            )
        raise ForwardClientError(
            "Forward async NQE execution did not complete after "
            f"{self.nqe_async_max_polls} status poll(s)."
        )

    def _fetch_nqe_async_result_page(
        self,
        *,
        network_id,
        execution_key,
        limit,
        offset,
    ):
        self._record_api_usage("nqe_pages")
        self._record_api_usage("nqe_query_pages")
        self._record_api_usage("nqe_async_result_calls")
        response = self._request(
            "GET",
            "/networks/{network_id}/nqe-executions/{execution_key}/result".format(
                network_id=quote(str(network_id), safe=""),
                execution_key=quote(str(execution_key), safe=""),
            ),
            params={"offset": offset, "limit": limit},
            headers={
                "Accept": "application/x-ndjson, application/jsonl;q=0.9, application/json;q=0.1"
            },
        )
        return self._parse_nqe_async_result(response)

    def run_nqe_query(
        self,
        *,
        query=None,
        query_id=None,
        commit_id=None,
        network_id=None,
        snapshot_id=None,
        parameters=None,
        limit=None,
        offset=0,
        item_format="JSON",
        column_filters=None,
        fetch_all=False,
    ):
        if bool(query) == bool(query_id):
            raise ForwardClientError(
                "Exactly one of `query` or `query_id` must be supplied."
            )
        if limit is None:
            limit = self.nqe_page_size
        if limit < 1:
            raise ForwardClientError("`limit` must be at least 1.")
        if not network_id or not snapshot_id:
            raise ForwardClientError(
                "Async NQE requires both `network_id` and `snapshot_id`."
            )
        if str(item_format or "JSON").upper() != "JSON":
            raise ForwardClientError("Async NQE only supports JSON item format.")

        return self._run_nqe_query_async(
            query=query,
            query_id=query_id,
            commit_id=commit_id,
            network_id=network_id,
            snapshot_id=snapshot_id,
            parameters=parameters,
            limit=limit,
            offset=offset,
            column_filters=column_filters,
            fetch_all=fetch_all,
        )

    def _run_nqe_query_async(
        self,
        *,
        query=None,
        query_id=None,
        commit_id=None,
        network_id,
        snapshot_id,
        parameters=None,
        limit=None,
        offset=0,
        column_filters=None,
        fetch_all=False,
    ):
        self._record_api_usage("nqe_query_calls")
        self._record_api_usage("nqe_async_query_calls")
        execution_key, initial_status = self._start_nqe_async_execution(
            query=query,
            query_id=query_id,
            commit_id=commit_id,
            network_id=network_id,
            snapshot_id=snapshot_id,
            parameters=parameters,
            column_filters=column_filters,
        )
        self._wait_for_nqe_async_completion(
            network_id=network_id,
            execution_key=execution_key,
            status=initial_status,
        )
        records, total_num_items = self._fetch_nqe_async_result_page(
            network_id=network_id,
            execution_key=execution_key,
            limit=limit,
            offset=offset,
        )
        if not fetch_all:
            return records

        all_records = list(records)
        expected_total = int(total_num_items) if total_num_items is not None else None
        last_page_size = len(records)
        fetched_pages = 1
        identical_full_page_streak = 0
        previous_full_page_signature = (
            self._page_signature(records)
            if expected_total is None and len(records) == limit
            else None
        )

        while True:
            if expected_total is not None and len(all_records) >= expected_total:
                return all_records
            if expected_total is None and last_page_size < limit:
                return all_records
            if fetched_pages >= self.nqe_fetch_all_max_pages:
                raise ForwardClientError(
                    "Forward async NQE result pagination exceeded "
                    f"{self.nqe_fetch_all_max_pages} page(s) while fetching "
                    f"`{query_id or '<raw-query>'}`."
                )

            next_offset = offset + len(all_records)
            page_records, page_total = self._fetch_nqe_async_result_page(
                network_id=network_id,
                execution_key=execution_key,
                limit=limit,
                offset=next_offset,
            )
            fetched_pages += 1
            if expected_total is None and page_total is not None:
                expected_total = int(page_total)
            last_page_size = len(page_records)
            if expected_total is None and last_page_size == limit and page_records:
                signature = self._page_signature(page_records)
                if signature == previous_full_page_signature:
                    identical_full_page_streak += 1
                else:
                    identical_full_page_streak = 0
                previous_full_page_signature = signature
                if (
                    identical_full_page_streak
                    >= self.nqe_identical_full_page_streak_limit
                ):
                    raise ForwardClientError(
                        "Forward async NQE result pagination did not advance; received "
                        f"{identical_full_page_streak + 1} identical full page(s) "
                        f"for `{query_id or '<raw-query>'}`. "
                        "Verify Forward API pagination for this execution."
                    )
            else:
                identical_full_page_streak = 0
                previous_full_page_signature = None
            if not page_records:
                if expected_total is not None and len(all_records) < expected_total:
                    raise ForwardClientError(
                        "Forward async NQE result pagination ended early: "
                        f"fetched {len(all_records)} rows but API reported {expected_total}."
                    )
                return all_records
            all_records.extend(page_records)

    def run_nqe_diff(
        self,
        *,
        query_id,
        before_snapshot_id,
        after_snapshot_id,
        commit_id=None,
        parameters=None,
        limit=None,
        offset=0,
        item_format="JSON",
        column_filters=None,
        fetch_all=False,
    ):
        if not query_id:
            raise ForwardClientError("`query_id` must be supplied.")
        if not before_snapshot_id or not after_snapshot_id:
            raise ForwardClientError(
                "Both `before_snapshot_id` and `after_snapshot_id` must be supplied."
            )
        if limit is None:
            limit = self.nqe_page_size
        if limit < 1:
            raise ForwardClientError("`limit` must be at least 1.")

        def fetch_page(page_offset):
            self._record_api_usage("nqe_pages")
            self._record_api_usage("nqe_diff_pages")
            payload = {
                "queryId": query_id,
                "options": {
                    "limit": limit,
                    "offset": page_offset,
                    "itemFormat": item_format,
                },
            }
            if commit_id:
                payload["commitId"] = commit_id
            if parameters:
                payload["parameters"] = parameters
            if column_filters:
                payload["options"]["columnFilters"] = column_filters

            response = self._request(
                "POST",
                f"/nqe-diffs/{before_snapshot_id}/{after_snapshot_id}",
                json_body=payload,
            )
            return self._parse_nqe_diff_rows(response.json() or {})

        self._record_api_usage("nqe_diff_calls")
        rows, total_num_rows = fetch_page(offset)
        if not fetch_all:
            return rows

        all_rows = list(rows)
        expected_total = int(total_num_rows) if total_num_rows is not None else None
        last_page_size = len(rows)
        fetched_pages = 1
        identical_full_page_streak = 0
        previous_full_page_signature = (
            self._page_signature(rows)
            if expected_total is None and len(rows) == limit
            else None
        )

        while True:
            if expected_total is not None and len(all_rows) >= expected_total:
                return all_rows
            if expected_total is None and last_page_size < limit:
                return all_rows
            if fetched_pages >= self.nqe_fetch_all_max_pages:
                raise ForwardClientError(
                    "Forward NQE diff pagination exceeded "
                    f"{self.nqe_fetch_all_max_pages} page(s) while fetching "
                    f"`{query_id}`."
                )

            next_offset = offset + len(all_rows)
            page_rows, page_total = fetch_page(next_offset)
            fetched_pages += 1
            if expected_total is None and page_total is not None:
                expected_total = int(page_total)
            last_page_size = len(page_rows)
            if expected_total is None and last_page_size == limit and page_rows:
                signature = self._page_signature(page_rows)
                if signature == previous_full_page_signature:
                    identical_full_page_streak += 1
                else:
                    identical_full_page_streak = 0
                previous_full_page_signature = signature
                if (
                    identical_full_page_streak
                    >= self.nqe_identical_full_page_streak_limit
                ):
                    raise ForwardClientError(
                        "Forward NQE diff pagination did not advance; received "
                        f"{identical_full_page_streak + 1} identical full page(s) "
                        f"for `{query_id}`. Verify Forward API pagination for this query."
                    )
            else:
                identical_full_page_streak = 0
                previous_full_page_signature = None
            if not page_rows:
                if expected_total is not None and len(all_rows) < expected_total:
                    raise ForwardClientError(
                        "Forward NQE diff pagination ended early: "
                        f"fetched {len(all_rows)} rows but API reported {expected_total}."
                    )
                return all_rows
            all_rows.extend(page_rows)
