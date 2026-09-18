import hashlib
import json
import threading
import time

import httpx
from django.core.cache import cache as django_cache
from forward_sdk import QueryRef
from forward_sdk.errors import ForwardError as SDKForwardError
from forward_sdk.errors import ForwardTransportError as SDKForwardTransportError
from rq.timeouts import JobTimeoutException

from ..exceptions import ForwardClientError
from ..exceptions import ForwardFetchBudgetExceededError
from .forward_client_config import coerce_api_requests_per_minute
from .forward_client_config import coerce_nqe_async_max_polls
from .forward_client_config import coerce_nqe_async_poll_interval_seconds
from .forward_client_config import coerce_nqe_fetch_all_max_pages
from .forward_client_config import coerce_nqe_fetch_all_max_rows
from .forward_client_config import coerce_nqe_identical_full_page_streak_limit
from .forward_client_config import coerce_nqe_page_size
from .forward_client_config import coerce_retry_count
from .forward_client_config import decrypt_client_password
from .forward_client_config import DEFAULT_FORWARD_API_REQUESTS_PER_MINUTE
from .forward_client_config import DEFAULT_FORWARD_API_RETRIES
from .forward_client_config import DEFAULT_FORWARD_API_TIMEOUT_SECONDS
from .forward_client_config import DEFAULT_FORWARD_SAAS_API_REQUESTS_PER_MINUTE
from .forward_client_config import DEFAULT_NQE_ASYNC_MAX_POLLS
from .forward_client_config import DEFAULT_NQE_ASYNC_POLL_INTERVAL_SECONDS
from .forward_client_config import DEFAULT_NQE_FETCH_ALL_MAX_PAGES
from .forward_client_config import DEFAULT_NQE_IDENTICAL_FULL_PAGE_STREAK_LIMIT
from .forward_client_config import DEFAULT_NQE_PAGE_SIZE
from .forward_client_config import FORWARD_SAAS_API_HARD_BLOCK_REQUESTS_PER_MINUTE
from .forward_client_config import MAX_FORWARD_API_REQUESTS_PER_MINUTE
from .forward_client_config import MAX_NQE_ASYNC_MAX_POLLS
from .forward_client_config import MAX_NQE_ASYNC_POLL_INTERVAL_SECONDS
from .forward_client_config import MAX_NQE_FETCH_ALL_MAX_PAGES
from .forward_client_config import MAX_NQE_IDENTICAL_FULL_PAGE_STREAK_LIMIT
from .forward_client_config import MAX_NQE_PAGE_SIZE
from .forward_client_errors import TRANSIENT_FORWARD_HTTP_STATUS_CODES
from .forward_client_errors import translate_client_exception
from .forward_client_factory import get_client
from .forward_read_cache import shared_read_cache_scope
from .forward_read_cache import SharedReadCache
from .forward_throttle import _RATE_LIMIT_LAST_REQUEST_AT
from .forward_throttle import Throttle
from .forward_usage import ApiUsageTracker

# Re-exported for forward_api.py's facade import (`from .forward_api_impl import
# X`) and, for _RATE_LIMIT_LAST_REQUEST_AT, for a test seam patched by name at
# forward_api_impl._RATE_LIMIT_LAST_REQUEST_AT. Listing them here rather than
# using per-line noqa markers survives black wrapping a long import line into
# parentheses, which moves a trailing noqa comment off the line flake8 anchors
# the warning to.
__all__ = [
    "DEFAULT_FORWARD_API_REQUESTS_PER_MINUTE",
    "DEFAULT_FORWARD_API_RETRIES",
    "DEFAULT_FORWARD_SAAS_API_REQUESTS_PER_MINUTE",
    "DEFAULT_NQE_ASYNC_MAX_POLLS",
    "DEFAULT_NQE_ASYNC_POLL_INTERVAL_SECONDS",
    "DEFAULT_NQE_FETCH_ALL_MAX_PAGES",
    "DEFAULT_NQE_IDENTICAL_FULL_PAGE_STREAK_LIMIT",
    "DEFAULT_NQE_PAGE_SIZE",
    "FORWARD_SAAS_API_HARD_BLOCK_REQUESTS_PER_MINUTE",
    "MAX_FORWARD_API_REQUESTS_PER_MINUTE",
    "MAX_NQE_ASYNC_MAX_POLLS",
    "MAX_NQE_ASYNC_POLL_INTERVAL_SECONDS",
    "MAX_NQE_FETCH_ALL_MAX_PAGES",
    "MAX_NQE_IDENTICAL_FULL_PAGE_STREAK_LIMIT",
    "MAX_NQE_PAGE_SIZE",
    "TRANSIENT_FORWARD_HTTP_STATUS_CODES",
    "_RATE_LIMIT_LAST_REQUEST_AT",
]

LATEST_PROCESSED_SNAPSHOT = "latestProcessed"
LATEST_COLLECTED_SNAPSHOT = "latestCollected"
# How many of the most recent processed snapshots to scan when resolving the
# latestCollected selector before giving up.
DEFAULT_LATEST_COLLECTED_SCAN_LIMIT = 10
DEFAULT_QUERY_FETCH_CONCURRENCY = 10
MAX_QUERY_FETCH_CONCURRENCY = 16
DEFAULT_QUERY_DIAGNOSTICS_ENABLED = True
NQE_QUERY_REPOSITORIES = {"org", "fwd"}
NQE_LIBRARY_WRITE_ROLES = {"ADMIN", "OPERATOR"}


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


def build_endpoint_tag_scope_where(include_tags, exclude_tags, include_match):
    """Return NQE ``where`` clause lines for the SNMP-endpoint tag scope.

    Mirrors the endpoint branch of the bundled device queries (same
    "all"/"any" include semantics as ``build_device_tag_scope_where``, applied
    to ``endpoint.tagNames``). The endpoint scope probe and the query branch
    must keep identical predicates: if the probe excluded an endpoint the
    branch still emits, the local scope filter would drop the emitted row —
    and with prune-out-of-scope enabled, previously imported endpoints would
    be DELETED. Pass ``include_tags=[]`` when include scoping of endpoints is
    disabled (the default; endpoints then filter by exclude tags only).
    """
    clauses: list[str] = []
    include_exprs = [
        f"{_nqe_string_literal(tag)} in endpoint.tagNames" for tag in include_tags
    ]
    if include_exprs:
        if include_match == "all":
            clauses.extend(f"where {expr}" for expr in include_exprs)
        else:
            clauses.append(f"where ({' || '.join(include_exprs)})")
    for tag in exclude_tags:
        clauses.append(f"where !({_nqe_string_literal(tag)} in endpoint.tagNames)")
    return clauses


def build_endpoint_device_eligibility_where(*, sync_generic_endpoints=False):
    """Return the endpoint-device eligibility predicate shared by live probes.

    Cisco CIMC is a management controller belonging to its physical server. The
    APIC CIMC inventory map models it as an inventory item, so emitting the same
    controller as a standalone SNMP endpoint creates a duplicate NetBox device.
    """
    return [
        "let endpointNameLower = toLowerCase(toString(endpoint.name))",
        "let endpointProfileName = toLowerCase(toString(endpoint.profileName))",
        'let endpointSysDescrOpt = max(foreach o in endpoint.snmpOutputs where o.requestedOid == "1.3.6.1.2.1.1.1" select max(foreach e in o.rawOidEntries select e.rawValue))',
        'let endpointSysObjIdOpt = max(foreach o in endpoint.snmpOutputs where o.requestedOid == "1.3.6.1.2.1.1.2" select max(foreach e in o.rawOidEntries select e.rawValue))',
        'let endpointSysDescr = if isPresent(endpointSysDescrOpt) then toLowerCase(endpointSysDescrOpt) else ""',
        'let endpointSysObjId = if isPresent(endpointSysObjIdOpt) then endpointSysObjIdOpt else ""',
        'let isCimc = matches(endpointNameLower, "*cimc*")',
        '  || matches(endpointProfileName, "*cimc*")',
        '  || matches(endpointSysDescr, "*cisco integrated management controller*")',
        "where !isCimc",
        'let isAvocent = matches(endpointSysObjId, "1.3.6.1.4.1.10418.*")',
        '  || matches(endpointSysObjId, "1.3.6.1.4.1.2925.*")',
        '  || matches(endpointSysDescr, "*avocent*")',
        '  || matches(endpointSysDescr, "*cyclades*")',
        '  || matches(endpointSysDescr, "*alterpath*")',
        'let isOpengear = matches(endpointSysObjId, "1.3.6.1.4.1.25049.*")',
        '  || matches(endpointSysDescr, "*opengear*")',
        "let isConsoleServer = isAvocent || isOpengear",
        *([] if sync_generic_endpoints else ["where isConsoleServer"]),
    ]


class ForwardClient:
    def __init__(self, source):
        self.source = source
        params = source.parameters or {}
        self.timeout = params.get("timeout") or DEFAULT_FORWARD_API_TIMEOUT_SECONDS
        self.retries = coerce_retry_count(params.get("retries"))
        self.verify = params.get("verify", True)
        self.nqe_page_size = coerce_nqe_page_size(params.get("nqe_page_size"))
        self.nqe_fetch_all_max_pages = coerce_nqe_fetch_all_max_pages(
            params.get("nqe_fetch_all_max_pages")
        )
        self.nqe_fetch_all_max_rows = coerce_nqe_fetch_all_max_rows(
            params.get("nqe_fetch_all_max_rows")
        )
        self.nqe_identical_full_page_streak_limit = (
            coerce_nqe_identical_full_page_streak_limit(
                params.get("nqe_identical_full_page_streak_limit")
            )
        )
        self.nqe_async_poll_interval_seconds = coerce_nqe_async_poll_interval_seconds(
            params.get("nqe_async_poll_interval_seconds")
        )
        self.nqe_async_max_polls = coerce_nqe_async_max_polls(
            params.get("nqe_async_max_polls")
        )
        self.base_url = source.url.rstrip("/")
        self.username = params.get("username")
        self.password = decrypt_client_password(params.get("password"))
        self.api_requests_per_minute = coerce_api_requests_per_minute(
            params.get("api_requests_per_minute"),
            source_type=getattr(self.source, "type", None),
            base_url=self.base_url,
        )
        if self.api_requests_per_minute:
            self._api_request_min_interval = 60.0 / self.api_requests_per_minute
        else:
            self._api_request_min_interval = 0.0
        self._usage = ApiUsageTracker(self.api_requests_per_minute)
        self._throttle = Throttle(
            api_request_min_interval=self._api_request_min_interval,
            base_url=self.base_url,
            username=self.username,
            cache_provider=lambda: _shared_rate_limit_cache(),
            usage=self._usage,
        )
        self._read_cache = SharedReadCache(
            scope=shared_read_cache_scope(
                source_pk=getattr(self.source, "pk", None),
                source_type=getattr(self.source, "type", None),
                base_url=self.base_url,
                username=self.username,
            ),
            cache_provider=lambda: _shared_read_cache(),
        )
        self._sdk_client = get_client(
            base_url=self.base_url,
            username=self.username,
            password=self.password,
            verify=self.verify,
            timeout=self.timeout,
            retries=self.retries,
            api_requests_per_minute=self.api_requests_per_minute,
            throttle=self._throttle,
            usage=self._usage,
            client=self,
            source=self.source,
        )
        self._read_cache_lock = threading.Lock()
        self._latest_processed_snapshot_cache: dict[str, dict] = {}
        self._snapshots_cache: dict[tuple[str, bool, int], list[dict]] = {}
        self._snapshot_data_file_hashes_cache: dict[tuple[str, str], dict[str, str]] = (
            {}
        )
        self._snapshot_metrics_cache: dict[str, dict] = {}
        self._networks_cache: list[dict] | None = None
        self._committed_nqe_query_cache: dict[tuple[str, str, str], dict] = {}
        self._org_nqe_queries_cache: dict[str, list[dict]] = {}
        self._repository_queries_cache: dict[tuple[str, str], list[dict]] = {}
        self._repository_query_index_cache: dict[tuple[str, str, int], dict] = {}
        self._nqe_query_history_cache: dict[str, list[dict]] = {}
        self._org_nqe_head_commit_id_cache: str | None = None

    def _record_api_usage(self, key, amount=1):
        self._usage.record(key, amount)

    def _record_nqe_execution_signature(self, kind, identity):
        self._usage.record_nqe_execution_signature(kind, identity)

    def _record_read_cache_hit(self):
        self._usage.record_read_cache_hit()

    def _record_read_cache_miss(self):
        self._usage.record_read_cache_miss()

    def _invalidate_nqe_query_read_caches(self):
        with self._read_cache_lock:
            self._committed_nqe_query_cache.clear()
            self._org_nqe_queries_cache.clear()
            self._repository_queries_cache.clear()
            self._repository_query_index_cache.clear()
            self._nqe_query_history_cache.clear()
            self._org_nqe_head_commit_id_cache = None
        self._read_cache.bump_generation()

    def _shared_read_cache(self):
        return _shared_read_cache()

    def _shared_read_cache_key(self, kind: str, *parts: object) -> str:
        return self._read_cache.key(kind, *parts)

    def _shared_query_read_generation(self) -> int:
        return self._read_cache.generation()

    def _bump_shared_query_read_generation(self) -> None:
        self._read_cache.bump_generation()

    def _shared_read_cache_get(self, key: str):
        return self._read_cache.get(key)

    def _shared_read_cache_set(self, key: str, value):
        self._read_cache.set(key, value)

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
        self._usage.record_http_attempt()

    def api_usage_summary(self):
        return self._usage.summary()

    def reset_api_usage_summary(self):
        self._usage.reset()

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

    def _rate_limit_key(self):
        return self._throttle._rate_limit_key()

    def _throttle_request(self):
        self._throttle.throttle()

    def _call_sdk(self, fn, *args, **kwargs):
        """Call an `forward-sdk` service method, translating its exceptions.

        `UsageTrackingHooks` (wired in `__init__` via `get_client`) already
        records `http_attempts`/`http_successes`/status-classified failures/
        `http_retries` for every SDK call, from the transport's own request
        hooks - see that module's docstring for why per-call-site
        approximation would under-count. The one thing hooks cannot see is a
        transport failure (no response was ever received), so
        `http_failures`/`http_timeout_failures`/`http_transport_failures` are
        recorded here, at the only place that information exists.
        """
        try:
            return fn(*args, **kwargs)
        except SDKForwardTransportError as exc:
            self._record_api_usage("http_failures")
            if isinstance(exc.__cause__, httpx.TimeoutException):
                self._record_api_usage("http_timeout_failures")
            else:
                self._record_api_usage("http_transport_failures")
            raise translate_client_exception(exc) from exc
        except SDKForwardError as exc:
            raise translate_client_exception(exc) from exc

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
        sdk_networks = self._call_sdk(self._sdk_client.networks.list)
        networks = []
        for item in sdk_networks or []:
            network_id = str(item.id or "").strip()
            name = str(item.name or "").strip()
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
        sdk_snapshots = self._call_sdk(
            self._sdk_client.snapshots.list,
            network_id,
            include_archived=bool(include_archived),
            limit=limit,
        )
        snapshots = []
        for item in sdk_snapshots or []:
            snapshot_id = str(item.id or "").strip()
            if not snapshot_id:
                continue
            state = str(item.state or "").strip()
            created = str(item.created_at or "").strip()
            processed = str(item.processed_at or "").strip()
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
        # `include_predicted=False` (the SDK default) excludes a snapshot
        # Forward created to analyse a change set - a network using Predict
        # would otherwise very often resolve "latest processed" to a
        # simulation rather than a state the network was ever actually in.
        # Forward's own now-deprecated `latestProcessed` selector, which this
        # replaces, had no such filter; adopted deliberately as a
        # correctness fix, not preserved as a quirk - see this plan's
        # Decision Log.
        sdk_snapshot = self._call_sdk(
            self._sdk_client.snapshots.latest_processed, network_id
        )
        snapshot = (
            {}
            if sdk_snapshot is None
            else {
                "id": str(sdk_snapshot.id or ""),
                "state": str(sdk_snapshot.state or ""),
                "createdAt": str(sdk_snapshot.created_at or ""),
                "processedAt": str(sdk_snapshot.processed_at or ""),
            }
        )
        if snapshot:
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

    def get_device_mgmt_tags(
        self,
        network_id,
        snapshot_id,
        *,
        include_tags=None,
        exclude_tags=None,
        include_match="any",
    ):
        """Return ``{device_name: [Mgmt_* tag, ...]}`` for the in-scope devices.

        Used by the primary-IP-from-tag feature: the ``Mgmt_<iface>`` device tags
        are not synced into NetBox, so they are read directly from Forward. Only
        management tags are returned; the device-tag scope mirrors the sync's
        include/exclude scope so the same devices are considered.
        """
        network_id = str(network_id or "").strip()
        if not network_id:
            raise ForwardClientError("get_device_mgmt_tags requires a network_id.")
        snapshot_id = str(snapshot_id or "").strip()
        if not snapshot_id:
            raise ForwardClientError("get_device_mgmt_tags requires a snapshot_id.")
        include_tags = [
            str(tag).strip() for tag in (include_tags or []) if str(tag).strip()
        ]
        exclude_tags = [
            str(tag).strip() for tag in (exclude_tags or []) if str(tag).strip()
        ]
        if include_match not in {"any", "all"}:
            include_match = "any"
        scope_where = build_device_tag_scope_where(
            include_tags, exclude_tags, include_match
        )
        query = "\n".join(
            [
                "foreach device in network.devices",
                "where device.snapshotInfo.result == DeviceSnapshotResult.completed",
                "where device.platform.vendor != Vendor.FORWARD_CUSTOM",
                *scope_where,
                "foreach tag in device.tagNames",
                "select {device: device.name, tag: tag}",
            ]
        )
        rows = self.run_nqe_query(
            query=query,
            network_id=network_id,
            snapshot_id=snapshot_id,
            fetch_all=True,
        )
        device_tags: dict[str, list[str]] = {}
        for row in rows or []:
            device = str(row.get("device") or "").strip()
            tag = str(row.get("tag") or "").strip()
            if not device or not tag or not tag.lower().startswith("mgmt_"):
                continue
            tags = device_tags.setdefault(device, [])
            if tag not in tags:
                tags.append(tag)
        return device_tags

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
        metrics = self._call_sdk(self._sdk_client.snapshots.metrics, snapshot_id)
        if isinstance(metrics, dict):
            with self._read_cache_lock:
                self._snapshot_metrics_cache[snapshot_id] = dict(metrics)
            self._shared_read_cache_set(shared_cache_key, dict(metrics))
        return metrics

    def get_snapshot_data_file_hashes(self, network_id, snapshot_id):
        """Return snapshot-correct NQE data-file content hashes by file name."""

        network_id = str(network_id or "").strip()
        snapshot_id = str(snapshot_id or "").strip()
        if not network_id or not snapshot_id:
            raise ForwardClientError(
                "Snapshot data-file hashes require network and snapshot IDs."
            )
        cache_key = (network_id, snapshot_id)
        with self._read_cache_lock:
            cached = self._snapshot_data_file_hashes_cache.get(cache_key)
        if cached is not None:
            self._record_read_cache_hit()
            return dict(cached)
        self._record_read_cache_miss()
        payload = (
            self._call_sdk(
                self._sdk_client.data_files.get_data_files,
                network_id=network_id,
                view="snapshot",
                snapshot_id=snapshot_id,
            )
            or []
        )
        if isinstance(payload, dict):
            rows = (
                payload.get("dataFiles")
                or payload.get("items")
                or payload.get("results")
                or []
            )
        else:
            rows = payload
        if not isinstance(rows, list):
            raise ForwardClientError(
                "Forward snapshot data-file response had an unsupported shape."
            )
        hashes = {}
        for row in rows:
            if not isinstance(row, dict):
                continue
            name = str(
                row.get("dataFileName") or row.get("name") or row.get("fileName") or ""
            ).strip()
            content_hash = str(
                row.get("contentMd5Hex")
                or row.get("contentHash")
                or row.get("md5")
                or ""
            ).strip()
            if name and content_hash:
                normalized_hash = f"md5:{content_hash.lower()}"
                hashes[name] = normalized_hash
                if name.lower().endswith(".json"):
                    hashes[name[:-5]] = normalized_hash
        with self._read_cache_lock:
            self._snapshot_data_file_hashes_cache[cache_key] = dict(hashes)
        return hashes

    def _get_org_nqe_queries(self, *, directory="/"):
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
        sdk_rows = self._call_sdk(self._sdk_client.nqe.queries, directory=directory)
        # This listing carries no commit per query - only intent/path/queryId
        # (`NqeQuery` has no `last_commit_id` field at all, matching the raw
        # endpoint exactly) - `get_committed_nqe_query`'s fallback to the
        # commits endpoint below depends on that being true.
        rows = [
            {
                "queryId": str(item.query_id or ""),
                "path": str(item.path or ""),
                "intent": str(item.intent or ""),
            }
            for item in sdk_rows or []
        ]
        self._org_nqe_queries_cache[directory] = list(rows)
        self._shared_read_cache_set(shared_cache_key, list(rows))
        return rows if isinstance(rows, list) else []

    def _get_nqe_repository_queries(self, *, repository="org", directory="/"):
        repository = _normalize_nqe_repository(repository)
        directory = _normalize_nqe_directory(directory)
        if repository == "org":
            rows = self._get_org_nqe_queries(directory=directory)
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

        sdk_rows = self._call_sdk(
            self._sdk_client.nqe.repo.queries, repository=repository
        )
        rows = [
            {
                "queryId": str(item.query_id or ""),
                "path": str(item.path or ""),
                "intent": str(item.intent or ""),
                "repository": repository,
                "lastCommitId": str(item.last_commit_id or ""),
            }
            for item in sdk_rows or []
        ]
        normalized_rows = [
            normalized
            for row in rows
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
        rows = self._get_nqe_repository_queries(
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
                except JobTimeoutException:
                    raise
                except Exception:
                    query_index = {}
            indexed_query = (query_index.get("by_path") or {}).get(query_path)
            if indexed_query and indexed_query.get("queryId"):
                # Ensure source code is available when requested for canonicality checks.
                # Some API responses return directory rows without source text.
                has_source = any(
                    indexed_query.get(key) for key in ("sourceCode", "source", "query")
                )
                # The directory listing carries no commit for org queries, only
                # intent/path/queryId/repository. Returning such a row leaves the
                # caller with an empty commit, which the execution contract
                # rejects as unresolved_full_commit - so every org-bound map was
                # refused and whole syncs fetched nothing. Fall through to the
                # commits endpoint, which does report lastCommitId.
                indexed_commit = str(
                    indexed_query.get("commitId")
                    or indexed_query.get("lastCommitId")
                    or (indexed_query.get("lastCommit") or {}).get("id")
                    or ""
                ).strip()
                if indexed_commit and (not require_source_code or has_source):
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
        # `NqeRepository.queries()` already collapses Forward's two response
        # shapes here (a wrapped `{"queries": [...]}` listing at `head`, or a
        # bare single object for a specific commit + path) into one uniform
        # `list[RepositoryQuery]` - see `queries_from_payload` in the SDK's
        # own `nqe/repository.py`, which is exactly the branching this method
        # used to hand-roll.
        sdk_queries = self._call_sdk(
            self._sdk_client.nqe.repo.queries,
            repository=repository,
            commit_id=commit_id,
            path=query_path,
            with_source=True,
        )
        matched = next(
            (item for item in sdk_queries or [] if item.path == query_path), None
        )
        if matched is None:
            raise ForwardClientError(
                f"Forward NQE repository lookup did not include `{query_path}`."
            )
        # `RepositoryQuery` carries the commit under one of two names
        # depending on what was asked for - a flat `last_commit_id` when
        # listing at head, a nested `last_commit.id` for a specific commit -
        # its own docstring warns that reading only one "loses the pin".
        last_commit_id = matched.last_commit_id or (
            matched.last_commit.id if matched.last_commit else None
        )
        normalized = {
            "queryId": str(matched.query_id or ""),
            "path": str(matched.path or ""),
            "intent": str(matched.intent or ""),
            "repository": repository,
            "lastCommitId": str(last_commit_id or ""),
            "sourceCode": matched.source_code,
        }
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
        rows = self._call_sdk(self._sdk_client.nqe.repo.history, query_id) or []
        self._nqe_query_history_cache[query_id] = list(rows)
        self._shared_read_cache_set(shared_cache_key, list(rows))
        return rows

    def has_nqe_library_write_permission(self):
        """Return whether the current login may write the org NQE library."""
        current_user = self._call_sdk(self._sdk_client.user_accounts.get_current_user)
        roles = getattr(current_user, "roles", None)
        if roles is None:
            return False

        org_roles = getattr(roles, "org", None) or []
        if isinstance(org_roles, str):
            org_roles = [org_roles]
        normalized_org_roles = {
            str(role or "").strip().upper()
            for role in org_roles
            if str(role or "").strip()
        }
        if normalized_org_roles.intersection(NQE_LIBRARY_WRITE_ROLES):
            return True

        network_roles = getattr(roles, "network", None) or {}
        if not isinstance(network_roles, dict):
            return False
        network_id = str((self.source.parameters or {}).get("network_id") or "").strip()
        network_role = str(network_roles.get(network_id) or "").strip().upper()
        return network_role in NQE_LIBRARY_WRITE_ROLES

    def add_org_nqe_query(self, *, query_path, source_code):
        query_path = _normalize_nqe_query_path(query_path)
        if not query_path:
            raise ForwardClientError("Forward NQE query path is required.")
        self._invalidate_nqe_query_read_caches()
        self._call_sdk(self._sdk_client.nqe.repo.stage_add, query_path, source_code)

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
        self._call_sdk(
            self._sdk_client.nqe.repo.stage_edit,
            query_path,
            source_code,
            query_id=query_id,
            commit_id=commit_id,
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
        commit_id = str(
            self._call_sdk(self._sdk_client.nqe.repo.head_commit_id) or ""
        ).strip()
        self._org_nqe_head_commit_id_cache = commit_id
        self._shared_read_cache_set(shared_cache_key, {"value": commit_id})
        return commit_id

    def commit_org_nqe_queries(self, *, query_paths, message):
        query_paths = [
            _normalize_nqe_query_path(query_path)
            for query_path in query_paths
            if _normalize_nqe_query_path(query_path)
        ]
        if not query_paths:
            return ""
        self._invalidate_nqe_query_read_caches()
        # `NqeRepository.commit` already does the no-staged-changes retry this
        # method used to hand-roll: it drops paths Forward's 409
        # INVALID_CHANGE_PATH names as unchanged and retries with the rest,
        # returning a `CommitReport` whose own `commit_id` is unset when
        # every path turned out to be a no-op - fall back to
        # `get_org_nqe_head_commit_id` in that case, matching this method's
        # own long-standing behavior of always resolving a real commit id.
        payload = _commit_message_payload(message)
        report = self._call_sdk(
            self._sdk_client.nqe.repo.commit,
            query_paths,
            title=payload["title"],
            body=payload["body"],
        )
        if not report.commit_id:
            return self.get_org_nqe_head_commit_id()
        # The SDK already resolved the fresh head commit as part of
        # `commit()`, bypassing this client's own cache entirely - populate
        # it explicitly so a `get_org_nqe_head_commit_id()` call right after
        # (this client's or, via the shared cache, another worker's) does
        # not repeat a fetch this method already made.
        self._org_nqe_head_commit_id_cache = report.commit_id
        self._shared_read_cache_set(
            self._shared_read_cache_key(
                "org-head-commit", self._shared_query_read_generation()
            ),
            {"value": report.commit_id},
        )
        return report.commit_id

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

    def _nqe_query_ref(self, *, query=None, query_id=None, commit_id=None):
        """Build the `QueryRef` (or raw source string) `execute()` takes.

        `_commit_id_for_nqe_execution` runs first so an abbreviated hash
        falls back to head exactly as it always has. The SDK's own
        `QueryRef.by_id` sanitizes commit ids too, but by *raising* on an
        abbreviated one - deliberately, on the theory that silently
        discarding a pin answers a different question and reports success.
        Right as a new default; a behavior change for an existing one, so
        this method keeps making the choice `_commit_id_for_nqe_execution`
        already encoded rather than adopting the SDK's stricter one here.
        """
        if not query_id:
            return query
        return QueryRef.by_id(
            query_id, commit_id=_commit_id_for_nqe_execution(commit_id) or None
        )

    def _start_nqe_execution(
        self,
        *,
        query=None,
        query_id=None,
        commit_id=None,
        network_id,
        snapshot_id,
        parameters=None,
    ):
        self._record_api_usage("nqe_async_trigger_calls")
        ref = self._nqe_query_ref(query=query, query_id=query_id, commit_id=commit_id)
        return self._call_sdk(
            self._sdk_client.nqe.execute,
            ref,
            network_id=network_id,
            snapshot_id=snapshot_id,
            parameters=parameters or None,
        )

    def _wait_for_nqe_execution(self, execution, *, deadline=None):
        # `NqeExecution.wait(timeout=...)` is purely time-based; there is no
        # poll-COUNT ceiling to hand it, and `timeout=None` means "wait
        # forever" - unlike the poll loop this replaces, which always had a
        # hard stop via `nqe_async_max_polls` even with no `deadline` set.
        # Derive an equivalent duration ceiling from the same operator-
        # configured knobs so a runaway execution still cannot hang a
        # worker indefinitely when no per-workload deadline is in play.
        poll_interval = self.nqe_async_poll_interval_seconds or 5.0
        poll_budget_seconds = self.nqe_async_max_polls * poll_interval
        if deadline is None:
            timeout = poll_budget_seconds
        else:
            timeout = min(poll_budget_seconds, max(0.0, deadline - time.monotonic()))
        self._record_api_usage("nqe_async_status_calls")
        self._call_sdk(execution.wait, poll_interval=poll_interval, timeout=timeout)

    def _fetch_nqe_async_result_page(self, execution, *, limit, offset):
        self._record_api_usage("nqe_pages")
        self._record_api_usage("nqe_query_pages")
        self._record_api_usage("nqe_async_result_calls")
        result = self._call_sdk(execution.result_page, offset=offset, limit=limit)
        return self._parse_nqe_records(
            {"items": result.items or [], "totalNumItems": result.total_num_items}
        )

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
        fetch_all=False,
        deadline=None,
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

        self._record_nqe_execution_signature(
            "query",
            {
                "query": hashlib.sha256((query or "").encode("utf-8")).hexdigest(),
                "query_id": query_id or "",
                "commit_id": commit_id or "",
                "network_id": network_id,
                "snapshot_id": snapshot_id,
                "parameters": parameters or {},
            },
        )

        return self._run_nqe_query_async(
            query=query,
            query_id=query_id,
            commit_id=commit_id,
            network_id=network_id,
            snapshot_id=snapshot_id,
            parameters=parameters,
            deadline=deadline,
            limit=limit,
            offset=offset,
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
        fetch_all=False,
        deadline=None,
    ):
        self._record_api_usage("nqe_query_calls")
        self._record_api_usage("nqe_async_query_calls")
        execution = self._start_nqe_execution(
            query=query,
            query_id=query_id,
            commit_id=commit_id,
            network_id=network_id,
            snapshot_id=snapshot_id,
            parameters=parameters,
        )
        self._wait_for_nqe_execution(execution, deadline=deadline)
        # `execution.result_page` has no per-call deadline of its own -
        # `wait()`'s own timeout already covers the execute-then-wait phase,
        # but a slow wait that used almost the whole budget could still let
        # this first fetch run unbounded without this check.
        if deadline is not None and time.monotonic() >= deadline:
            raise ForwardFetchBudgetExceededError(
                "Forward NQE fetch exceeded the per-workload wall-clock budget"
            )
        records, total_num_items = self._fetch_nqe_async_result_page(
            execution, limit=limit, offset=offset
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
            if deadline is not None and time.monotonic() >= deadline:
                raise ForwardFetchBudgetExceededError(
                    "Forward NQE fetch exceeded the per-workload wall-clock budget"
                )
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
            if len(all_records) >= self.nqe_fetch_all_max_rows:
                raise ForwardClientError(
                    "Forward async NQE result exceeded the in-memory row ceiling "
                    f"({self.nqe_fetch_all_max_rows} rows) while fetching "
                    f"`{query_id or '<raw-query>'}`. Shard this model (or raise "
                    "nqe_fetch_all_max_rows) to avoid exhausting worker memory."
                )

            next_offset = offset + len(all_records)
            page_records, page_total = self._fetch_nqe_async_result_page(
                execution, limit=limit, offset=next_offset
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
        limit=None,
        offset=0,
        item_format="JSON",
        fetch_all=False,
        deadline=None,
    ):
        if not query_id:
            raise ForwardClientError("`query_id` must be supplied.")
        if not before_snapshot_id or not after_snapshot_id:
            raise ForwardClientError(
                "Both `before_snapshot_id` and `after_snapshot_id` must be supplied."
            )
        # `client.nqe.diff_page` has no item-format option at all - no real
        # caller has ever requested anything but the default, but silently
        # dropping the parameter would turn an unsupported request into one
        # that looks like it succeeded.
        if str(item_format or "JSON").upper() != "JSON":
            raise ForwardClientError("NQE diff only supports JSON item format.")
        if limit is None:
            limit = self.nqe_page_size
        if limit < 1:
            raise ForwardClientError("`limit` must be at least 1.")

        self._record_nqe_execution_signature(
            "diff",
            {
                "query_id": query_id,
                "commit_id": commit_id or "",
                "before_snapshot_id": before_snapshot_id,
                "after_snapshot_id": after_snapshot_id,
            },
        )

        # Same abbreviated-hash-falls-back-to-head treatment as
        # run_nqe_query's _nqe_query_ref: QueryRef.by_id raises on one
        # rather than silently discarding the pin, and this method never
        # checked for one at all before, so this is a deliberate applied
        # consistency fix, not a preserved quirk.
        ref = QueryRef.by_id(
            query_id, commit_id=_commit_id_for_nqe_execution(commit_id) or None
        )

        def fetch_page(page_offset):
            self._record_api_usage("nqe_pages")
            self._record_api_usage("nqe_diff_pages")
            result = self._call_sdk(
                self._sdk_client.nqe.diff_page,
                ref,
                before=before_snapshot_id,
                after=after_snapshot_id,
                offset=page_offset,
                limit=limit,
            )
            data = {
                "rows": [
                    {
                        "type": str(row.type) if row.type is not None else None,
                        "before": row.before,
                        "after": row.after,
                    }
                    for row in result.rows or []
                ],
                "totalNumRows": result.total_num_rows,
            }
            return self._parse_nqe_diff_rows(data)

        # `client.nqe.diff_page` has no per-call deadline of its own (unlike
        # `_request()`, which checked one on every retry attempt) - checked
        # explicitly here so an already-exceeded deadline is still honored
        # before the first page fetch, not just before the second and later
        # ones the loop below already guards.
        if deadline is not None and time.monotonic() >= deadline:
            raise ForwardFetchBudgetExceededError(
                "Forward NQE fetch exceeded the per-workload wall-clock budget"
            )
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
            if deadline is not None and time.monotonic() >= deadline:
                raise ForwardFetchBudgetExceededError(
                    "Forward NQE fetch exceeded the per-workload wall-clock budget"
                )
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
