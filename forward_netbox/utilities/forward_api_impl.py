import hashlib
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
TRANSIENT_FORWARD_HTTP_STATUS_CODES = {408, 429, 502, 503, 504}
NQE_QUERY_REPOSITORIES = {"org", "fwd"}
_RATE_LIMIT_LOCK = threading.Lock()
_RATE_LIMIT_LAST_REQUEST_AT = {}


def _shared_rate_limit_cache():
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
        self._api_usage_first_http_attempt_at = None
        self._api_usage_last_http_attempt_at = None
        self._api_usage = self._empty_api_usage()

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
        }

    def _record_api_usage(self, key, amount=1):
        with self._api_usage_lock:
            self._api_usage[key] = self._api_usage.get(key, 0) + amount

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

    def _headers(self):
        return {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "User-Agent": "forward-netbox/0.8.6.3",
        }

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

    def _request(self, method, path, *, params=None, json_body=None):
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
                        headers=self._headers(),
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
        return networks

    def get_snapshots(self, network_id, *, include_archived=False, limit=100):
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
        return snapshots

    def get_latest_processed_snapshot(self, network_id):
        response = self._request(
            "GET", f"/networks/{network_id}/snapshots/latestProcessed"
        )
        return response.json() or {}

    def get_latest_processed_snapshot_id(self, network_id):
        snapshot = self.get_latest_processed_snapshot(network_id)
        snapshot_id = str(snapshot.get("id", "")).strip()
        if not snapshot_id:
            raise ForwardClientError(
                "Forward latestProcessed snapshot response did not include an ID."
            )
        return snapshot_id

    def get_snapshot_metrics(self, snapshot_id):
        response = self._request("GET", f"/snapshots/{snapshot_id}/metrics")
        return response.json() or {}

    def get_org_nqe_queries(self, *, directory="/"):
        directory = _normalize_nqe_directory(directory)
        params = {"dir": directory}
        response = self._request("GET", "/nqe/queries", params=params or None)
        data = response.json() or []
        return data if isinstance(data, list) else []

    def get_nqe_repository_queries(self, *, repository="org", directory="/"):
        repository = _normalize_nqe_repository(repository)
        directory = _normalize_nqe_directory(directory)
        if repository == "org":
            rows = self.get_org_nqe_queries(directory=directory)
            normalized_rows = [
                normalized
                for row in rows
                if (normalized := _normalize_nqe_query_row(row, repository=repository))
            ]
            if normalized_rows or directory != "/":
                return normalized_rows

        response = self._request(
            "GET",
            f"/nqe/repos/{repository}/commits/head/queries",
        )
        data = response.json() or {}
        rows = data.get("queries") if isinstance(data, dict) else []
        return [
            normalized
            for row in rows or []
            if _query_in_directory(row.get("path"), directory)
            if (normalized := _normalize_nqe_query_row(row, repository=repository))
        ]

    def get_committed_nqe_query(
        self, *, repository="org", query_path="", commit_id="head"
    ):
        repository = _normalize_nqe_repository(repository)
        query_path = _normalize_nqe_query_path(query_path)
        commit_id = str(commit_id or "head").strip() or "head"
        if not query_path:
            raise ForwardClientError("Forward NQE query path is required.")
        response = self._request(
            "GET",
            f"/nqe/repos/{repository}/commits/{quote(commit_id, safe='')}/queries",
            params={"path": query_path},
        )
        data = response.json() or {}
        if not isinstance(data, dict):
            raise ForwardClientError(
                f"Forward NQE repository lookup for `{query_path}` returned an invalid response."
            )
        if isinstance(data.get("queries"), list):
            for query in data["queries"]:
                if isinstance(query, dict) and query.get("path") == query_path:
                    return query
            raise ForwardClientError(
                f"Forward NQE repository lookup did not include `{query_path}`."
            )
        return data

    def resolve_nqe_query_reference(
        self, *, repository="org", query_path="", commit_id=None
    ):
        query = self.get_committed_nqe_query(
            repository=repository,
            query_path=query_path,
            commit_id=commit_id or "head",
        )
        query_id = str(query.get("queryId") or "").strip()
        if not query_id:
            raise ForwardClientError(
                f"Forward NQE query `{repository}:{query_path}` did not include a query ID."
            )
        last_commit = query.get("lastCommit") or {}
        resolved_commit_id = str(
            commit_id or last_commit.get("id") or query.get("lastCommitId") or ""
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
        response = self._request(
            "GET",
            f"/nqe/queries/{quote(query_id, safe='')}/history",
        )
        data = response.json() or {}
        commits = data.get("commits") if isinstance(data, dict) else []
        return commits or []

    def add_org_nqe_query(self, *, query_path, source_code):
        query_path = _normalize_nqe_query_path(query_path)
        if not query_path:
            raise ForwardClientError("Forward NQE query path is required.")
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
        response = self._request("GET", "/nqe/repos/org/commits/head")
        data = response.json()
        if isinstance(data, dict):
            return str(data.get("id") or data.get("commitId") or "").strip()
        return str(data or "").strip()

    def commit_org_nqe_queries(self, *, query_paths, message):
        query_paths = [
            _normalize_nqe_query_path(query_path)
            for query_path in query_paths
            if _normalize_nqe_query_path(query_path)
        ]
        if not query_paths:
            return ""
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

        def fetch_page(page_offset):
            self._record_api_usage("nqe_pages")
            self._record_api_usage("nqe_query_pages")
            payload = {
                "parameters": parameters or {},
                "queryOptions": {
                    "limit": limit,
                    "offset": page_offset,
                    "itemFormat": item_format,
                },
            }
            if column_filters:
                payload["queryOptions"]["columnFilters"] = column_filters
            if query_id:
                payload["queryId"] = query_id
                if commit_id:
                    payload["commitId"] = commit_id
            else:
                payload["query"] = query
            req_params = {}
            if network_id:
                req_params["networkId"] = network_id
            if snapshot_id:
                req_params["snapshotId"] = snapshot_id

            response = self._request(
                "POST",
                "/nqe",
                params=req_params,
                json_body=payload,
            )

            return self._parse_nqe_records(response.json() or {})

        self._record_api_usage("nqe_query_calls")
        records, total_num_items = fetch_page(offset)
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
                    "Forward NQE pagination exceeded "
                    f"{self.nqe_fetch_all_max_pages} page(s) while fetching "
                    f"`{query_id or '<raw-query>'}`."
                )

            next_offset = offset + len(all_records)
            page_records, page_total = fetch_page(next_offset)
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
                        "Forward NQE pagination did not advance; received "
                        f"{identical_full_page_streak + 1} identical full page(s) "
                        f"for `{query_id or '<raw-query>'}`. "
                        "Verify Forward API pagination for this query."
                    )
            else:
                identical_full_page_streak = 0
                previous_full_page_signature = None
            if not page_records:
                if expected_total is not None and len(all_records) < expected_total:
                    raise ForwardClientError(
                        "Forward NQE pagination ended early: "
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
