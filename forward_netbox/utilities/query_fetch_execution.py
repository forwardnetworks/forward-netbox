import json
import re
import time
from concurrent.futures import as_completed
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from dataclasses import field
from dataclasses import replace
from typing import Any

from django.db import close_old_connections
from django.db import connection
from django.db import connections
from django.utils.text import slugify
from rq.timeouts import JobTimeoutException

from ..choices import FORWARD_OPTIONAL_MODELS
from ..choices import ForwardApplyEngineChoices
from ..choices import ForwardDiffFallbackModeChoices
from ..exceptions import ForwardClientError
from ..exceptions import ForwardConnectivityError
from ..exceptions import ForwardFetchBudgetExceededError
from ..exceptions import ForwardQueryError
from .apply_engine import apply_engine_decision_for
from .branch_budget import BranchPlanItem
from .branch_budget import BranchWorkload
from .branch_budget import DEVICE_SHARD_MODELS
from .branch_budget import row_shard_key
from .branch_budget import shard_fetch_contract
from .forward_api import build_device_tag_scope_where
from .forward_api import build_endpoint_device_eligibility_where
from .forward_api import build_endpoint_tag_scope_where
from .forward_api import DEFAULT_QUERY_FETCH_CONCURRENCY
from .forward_api import LATEST_COLLECTED_SNAPSHOT
from .forward_api import LATEST_PROCESSED_SNAPSHOT
from .forward_api import MAX_QUERY_FETCH_CONCURRENCY
from .model_contracts import architecture_default_coalesce_fields_for_model
from .query_diagnostics import (
    append_ipaddress_diagnostics as sync_append_ipaddress_diagnostics,
)
from .query_diagnostics import (
    append_ipaddress_parent_prefix_diagnostics as sync_append_ipaddress_parent_prefix_diagnostics,
)
from .query_diagnostics import (
    append_routing_diagnostics as sync_append_routing_diagnostics,
)
from .query_diagnostics import diagnostic_row_count as sync_diagnostic_row_count
from .query_diagnostics import (
    summarize_routing_import_diagnostic_rows as sync_summarize_routing_import_diagnostic_rows,
)
from .query_diagnostics import (
    summarize_unassignable_ipaddress_rows as sync_summarize_unassignable_ipaddress_rows,
)
from .query_registry import ensure_unique_query_spec_executions
from .query_registry import get_query_specs
from .query_registry import optional_builtin_query_names_for_model
from .query_registry import resolve_query_specs_for_client
from .sync import ForwardSyncRunner
from .sync_contracts import validate_row_shape_for_model
from .sync_facade import effective_scope_endpoints_by_include_tags
from .workload_normalization import normalize_dependency_workloads
from .workload_state import apply_durable_workload_deltas

# Models whose NQE query filters `device.name in forward_netbox_shard_keys`, so a
# device-tag scope can be pushed to the Forward fetch as device-name shard keys
# (reducing the fetch to in-scope devices instead of fetching the whole network
# and discarding it locally). Network-scoped models (prefix/vlan/vrf/site/
# platform/devicetype) and the ACI fabric models use different shard semantics
# and keep the post-fetch local scope filter.
DEVICE_NAME_SCOPED_MODELS = DEVICE_SHARD_MODELS | {
    "dcim.device",
    "dcim.virtualchassis",
    "netbox_dlm.softwareversion",
    "netbox_dlm.hardwarenotice",
    "netbox_dlm.devicesoftware",
    "netbox_dlm.vulnerability",
}

# Bounded retry for a single workload (shard) fetch when the Forward NQE call
# fails transiently. The HTTP client already retries at the request level; this
# adds a coarser query-level retry so a transient NQE-execution failure (async
# poll timeout, engine busy, connection reset) does not fail the shard — a
# failed shard is dropped from the rebuilt plan and desyncs the resumable
# branching executor's claimed index (the class of crash Partner hit on ipam.vlan).
DEFAULT_WORKLOAD_FETCH_RETRY_ATTEMPTS = 2
DEFAULT_WORKLOAD_FETCH_RETRY_BACKOFF_SECONDS = 3.0
DEFAULT_WORKLOAD_FETCH_TIMEOUT_SECONDS = 0
_TRANSIENT_FETCH_ERROR_TOKENS = (
    "timeout",
    "timed out",
    "temporarily",
    "unavailable",
    "connection reset",
    "connection aborted",
    "429",
    "500",
    "502",
    "503",
    "504",
)


def _is_transient_fetch_error(exc):
    """True when a workload fetch error is worth retrying.

    Connectivity errors are always transient. Other client errors are retried
    only when the message looks transient (timeout / 429 / 5xx / reset).
    ForwardQueryError (a malformed or unpublished query, or a source defect) is
    never transient — retrying it just wastes time.
    """
    if isinstance(exc, ForwardFetchBudgetExceededError):
        return False
    if isinstance(exc, ForwardConnectivityError):
        return True
    if isinstance(exc, ForwardQueryError):
        return False
    if isinstance(exc, ForwardClientError):
        message = str(exc or "").lower()
        return any(token in message for token in _TRANSIENT_FETCH_ERROR_TOKENS)
    return False


def _nqe_string_literal(value: str) -> str:
    return json.dumps(value)


DEFAULT_SAMPLE_ROW_LIMIT = 5


_SENSITIVE_EXCEPTION_PATTERNS = (
    (
        re.compile(
            r"([?&](?:networkId|snapshotId|queryId|commitId)=)[^&\s\"']+",
            re.IGNORECASE,
        ),
        r"\1<redacted>",
    ),
    (
        re.compile(
            r"\b(network(?:[ _-]?id)?|snapshot(?:[ _-]?id)?|query(?:[ _-]?id)?|commit(?:[ _-]?id)?)"
            r"(\b[\"']?\s*[:=]\s*[\"']?)([^,\s\"'}]+)",
            re.IGNORECASE,
        ),
        r"\1\2<redacted>",
    ),
    (
        re.compile(
            r"(/(?:networks|snapshots|nqe/queries)/)([^/?#\s\"']+)",
            re.IGNORECASE,
        ),
        r"\1<redacted>",
    ),
    (
        re.compile(
            r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b",
            re.IGNORECASE,
        ),
        "<redacted-email>",
    ),
    (re.compile(r"\b\d{5,}\b"), "<redacted-number>"),
)


def _safe_exception_summary(exc: Exception) -> str:
    message = str(exc or "").strip()
    if not message:
        return exc.__class__.__name__
    for pattern, replacement in _SENSITIVE_EXCEPTION_PATTERNS:
        message = pattern.sub(replacement, message)
    return message


@dataclass(frozen=True)
class ForwardQueryContext:
    network_id: str
    snapshot_selector: str
    snapshot_id: str
    ingestion_id: int | None = None
    snapshot_info: dict[str, Any] = field(default_factory=dict)
    snapshot_metrics: dict[str, Any] = field(default_factory=dict)
    query_parameters: dict[str, Any] = field(default_factory=dict)
    maps: list[Any] = field(default_factory=list)
    device_tag_include_tags: list[str] = field(default_factory=list)
    device_tag_exclude_tags: list[str] = field(default_factory=list)
    device_tag_include_match: str = "any"
    device_tag_prune_out_of_scope: bool = False
    # Forward tags the operator selected to sync as NetBox device tags (feeds the
    # sync_device_tags query parameter of the device-tag sync query).
    sync_device_tags: list[str] = field(default_factory=list)
    # Opt-in: import Forward SNMP endpoints (e.g. Avocent) as NetBox devices.
    sync_endpoints: bool = False
    # Broad MIB-2-only endpoint import. False keeps endpoint import limited to
    # recognized Avocent/Opengear console servers.
    sync_generic_endpoints: bool = False
    # Opt-in: endpoints must also carry the device include tags (default: the
    # include scope narrows modeled devices only; exclude tags always apply).
    scope_endpoints_by_include_tags: bool = False
    scoped_device_names: set[str] = field(default_factory=set)
    scoped_site_names: set[str] = field(default_factory=set)
    scoped_matched_tags: dict[str, list[str]] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        return {
            "network_id": self.network_id,
            "snapshot_selector": self.snapshot_selector,
            "snapshot_id": self.snapshot_id,
            "ingestion_id": self.ingestion_id,
            "snapshot_info": self.snapshot_info,
            "snapshot_metrics": self.snapshot_metrics,
            "query_parameters": self.query_parameters,
            "maps": self.maps,
            "device_tag_include_tags": self.device_tag_include_tags,
            "device_tag_exclude_tags": self.device_tag_exclude_tags,
            "device_tag_include_match": self.device_tag_include_match,
            "device_tag_prune_out_of_scope": self.device_tag_prune_out_of_scope,
            "sync_device_tags": self.sync_device_tags,
            "sync_endpoints": self.sync_endpoints,
            "sync_generic_endpoints": self.sync_generic_endpoints,
            "scope_endpoints_by_include_tags": self.scope_endpoints_by_include_tags,
            "scoped_device_count": len(self.scoped_device_names),
            "scoped_site_count": len(self.scoped_site_names),
            # Full per-device matched-tag map (not a count): the branch apply path
            # reads this back to tag each device with the include tags it carries.
            "scoped_matched_tags": {
                str(k): list(v) for k, v in self.scoped_matched_tags.items()
            },
        }


def _extract_device_names(value: Any) -> set[str]:
    names: set[str] = set()
    if isinstance(value, str):
        candidate = value.strip()
        if candidate:
            names.add(candidate)
        return names
    if isinstance(value, dict):
        nested_name = value.get("name")
        if isinstance(nested_name, str) and nested_name.strip():
            names.add(nested_name.strip())
        return names
    if isinstance(value, list):
        for item in value:
            names.update(_extract_device_names(item))
    return names


_DEVICE_FIELD_NAMES = {
    "device",
    "device_name",
    "peer_device",
    "local_device",
    "remote_device",
    "a_device",
    "z_device",
    "a_device_name",
    "z_device_name",
}

_PRIMARY_SCOPE_DEVICE_FIELD_BY_MODEL = {
    "netbox_routing.bgppeer": "device",
    "netbox_routing.bgpaddressfamily": "device",
    "netbox_routing.bgppeeraddressfamily": "device",
    "netbox_routing.ospfinstance": "device",
    "netbox_routing.ospfarea": "device",
    "netbox_routing.ospfinterface": "device",
    "netbox_peering_manager.peeringsession": "device",
    "netbox_dlm.devicesoftware": "name",
    "netbox_dlm.vulnerability": "name",
}


def _row_device_names(model_string: str, row: dict[str, Any]) -> set[str]:
    names: set[str] = set()
    if model_string == "dcim.device":
        device_name = str(row.get("name") or "").strip()
        if device_name:
            names.add(device_name)
        return names
    primary_field = _PRIMARY_SCOPE_DEVICE_FIELD_BY_MODEL.get(model_string)
    if primary_field:
        return _extract_device_names(row.get(primary_field))
    for key, value in row.items():
        key_lower = str(key).lower()
        if key_lower in _DEVICE_FIELD_NAMES:
            names.update(_extract_device_names(value))
        elif key_lower.endswith("_device"):
            names.update(_extract_device_names(value))
    return names


def _row_site_names(row: dict[str, Any]) -> set[str]:
    names: set[str] = set()
    for key in ("site", "site_name", "name", "slug"):
        value = row.get(key)
        if isinstance(value, str) and value.strip():
            names.add(value.strip().lower())
    return names


@dataclass(frozen=True)
class ForwardModelResult:
    model_string: str
    query_name: str
    execution_mode: str
    execution_value: str
    sync_mode: str
    row_count: int
    delete_count: int = 0
    failure_count: int = 0
    runtime_ms: float | None = None
    snapshot_id: str = ""
    baseline_snapshot_id: str = ""
    diagnostics: list[dict[str, Any]] = field(default_factory=list)
    apply_engine: str = ForwardApplyEngineChoices.ADAPTER
    apply_engine_reason: str = ""
    apply_engine_decision: dict[str, Any] = field(default_factory=dict)
    fetch_mode: str = "model"
    fetch_key_family: str = ""
    fetch_parameters: dict[str, Any] = field(default_factory=dict)
    query_parameters: dict[str, Any] = field(default_factory=dict)
    query_path_resolution: dict[str, Any] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        return {
            "model": self.model_string,
            "query_name": self.query_name,
            "execution_mode": self.execution_mode,
            "execution_value": self.execution_value,
            "sync_mode": self.sync_mode,
            "row_count": self.row_count,
            "delete_count": self.delete_count,
            "failure_count": self.failure_count,
            "runtime_ms": self.runtime_ms,
            "snapshot_id": self.snapshot_id,
            "baseline_snapshot_id": self.baseline_snapshot_id,
            "diagnostics": self.diagnostics,
            "apply_engine": self.apply_engine,
            "apply_engine_reason": self.apply_engine_reason,
            "apply_engine_decision": self.apply_engine_decision,
            "fetch_mode": self.fetch_mode,
            "fetch_key_family": self.fetch_key_family,
            "fetch_parameters": self.fetch_parameters,
            "query_parameters": self.query_parameters,
            "query_path_resolution": self.query_path_resolution,
        }


class ForwardQueryFetcher:
    def __init__(self, sync, client, logger_):
        self.sync = sync
        self.client = client
        self.logger = logger_
        self.model_results: list[ForwardModelResult] = []
        self._failed_model_results: dict[str, ForwardModelResult] = {}
        self._resolved_specs_cache: dict[str, list[Any]] = {}
        self._incremental_baseline_cache: dict[tuple[Any, ...], Any] = {}
        self._query_path_resolution_cache: dict[str, dict[str, Any]] = {}
        self._resolved_context_cache: dict[tuple[Any, ...], ForwardQueryContext] = {}
        self.pending_workload_states = []

    def resolve_context(self) -> ForwardQueryContext:
        network_id = self.sync.get_network_id()
        snapshot_selector = self.sync.get_snapshot_id()
        snapshot_id = self.sync.resolve_snapshot_id(self.client)
        if not network_id:
            raise ForwardQueryError(
                "Forward sync requires a network ID on the sync or its source."
            )
        if not snapshot_id:
            raise ForwardQueryError(
                "Forward sync requires a snapshot ID for NQE execution."
            )

        source_parameters = dict(getattr(self.sync.source, "parameters", {}) or {})
        include_tags = source_parameters.get("device_tag_include_tags") or []
        exclude_tags = source_parameters.get("device_tag_exclude_tags") or []
        include_tags = [str(tag).strip() for tag in include_tags if str(tag).strip()]
        exclude_tags = [str(tag).strip() for tag in exclude_tags if str(tag).strip()]
        include_match = str(
            source_parameters.get("device_tag_include_match") or "any"
        ).strip()
        if include_match not in {"any", "all"}:
            include_match = "any"
        prune_out_of_scope = bool(
            source_parameters.get("device_tag_prune_out_of_scope")
        )
        sync_device_tags = source_parameters.get("sync_device_tags") or []
        sync_device_tags = sorted(
            {str(tag).strip() for tag in sync_device_tags if str(tag).strip()}
        )
        sync_endpoints = bool(source_parameters.get("sync_endpoints"))
        sync_generic_endpoints = bool(source_parameters.get("sync_generic_endpoints"))
        scope_endpoints_by_include_tags = effective_scope_endpoints_by_include_tags(
            source_parameters
        )
        context_cache_key = (
            network_id,
            snapshot_selector,
            snapshot_id,
            tuple(include_tags),
            tuple(exclude_tags),
            include_match,
            prune_out_of_scope,
            sync_endpoints,
            sync_generic_endpoints,
            scope_endpoints_by_include_tags,
        )
        cached_context = self._resolved_context_cache.get(context_cache_key)
        if cached_context is not None:
            return cached_context
        snapshot_info = self._resolve_snapshot_info(
            network_id=network_id,
            snapshot_selector=snapshot_selector,
            snapshot_id=snapshot_id,
        )
        snapshot_metrics = {}
        try:
            snapshot_metrics = self.client.get_snapshot_metrics(snapshot_id)
        except JobTimeoutException:
            raise
        except Exception as exc:
            self.logger.log_warning(
                "Unable to fetch Forward snapshot metrics for the selected snapshot: "
                f"{_safe_exception_summary(exc)}",
                obj=self.sync,
            )
        (
            scoped_device_names,
            scoped_site_names,
            scoped_matched_tags,
            endpoint_scope_failed,
        ) = self._resolve_scoped_tag_scope(
            network_id=network_id,
            snapshot_id=snapshot_id,
            include_tags=include_tags,
            exclude_tags=exclude_tags,
            include_match=include_match,
            sync_endpoints=sync_endpoints,
            sync_generic_endpoints=sync_generic_endpoints,
            scope_endpoints_by_include_tags=scope_endpoints_by_include_tags,
        )
        if endpoint_scope_failed:
            # The scoped set carries no endpoint names, so emitting endpoint
            # rows would get them dropped locally and then pruned as deletes.
            sync_endpoints = False
        context = ForwardQueryContext(
            network_id=network_id,
            snapshot_selector=snapshot_selector,
            snapshot_id=snapshot_id,
            ingestion_id=None,
            snapshot_info=snapshot_info or {},
            snapshot_metrics=snapshot_metrics or {},
            query_parameters=self.sync.get_query_parameters(),
            maps=self.sync.get_maps(),
            device_tag_include_tags=include_tags,
            device_tag_exclude_tags=exclude_tags,
            device_tag_include_match=include_match,
            device_tag_prune_out_of_scope=prune_out_of_scope,
            sync_device_tags=sync_device_tags,
            sync_endpoints=sync_endpoints,
            sync_generic_endpoints=sync_generic_endpoints,
            scope_endpoints_by_include_tags=scope_endpoints_by_include_tags,
            scoped_device_names=scoped_device_names,
            scoped_site_names=scoped_site_names,
            scoped_matched_tags=scoped_matched_tags,
        )
        self._resolved_context_cache[context_cache_key] = context
        return context

    def _resolve_scoped_tag_scope(
        self,
        *,
        network_id: str,
        snapshot_id: str,
        include_tags: list[str],
        exclude_tags: list[str],
        include_match: str,
        sync_endpoints: bool = False,
        sync_generic_endpoints: bool = False,
        scope_endpoints_by_include_tags: bool = False,
    ) -> tuple[set[str], set[str], dict[str, list[str]], bool]:
        if not include_tags and not exclude_tags:
            return set(), set(), {}, False
        scope_where = build_device_tag_scope_where(
            include_tags, exclude_tags, include_match
        )
        where = [
            "where device.snapshotInfo.result == DeviceSnapshotResult.completed",
            "where device.platform.vendor != Vendor.FORWARD_CUSTOM",
            *scope_where,
        ]
        query = "\n".join(
            [
                "foreach device in network.devices",
                *where,
                "select {",
                "  name: device.name,",
                '  site: if isPresent(device.locationName) then toLowerCase(device.locationName) else "unknown",',
                "  tagNames: device.tagNames",
                "}",
            ]
        )
        try:
            rows = self.client.run_nqe_query(
                query=query,
                network_id=network_id,
                snapshot_id=snapshot_id,
                fetch_all=True,
            )
        except (ForwardClientError, ForwardConnectivityError, ForwardQueryError) as exc:
            raise ForwardQueryError(
                "Forward device tag filter query failed: "
                f"{_safe_exception_summary(exc)}"
            ) from exc
        names = {
            str(row.get("name") or "").strip()
            for row in rows
            if str(row.get("name") or "").strip()
        }
        sites = set()
        # Per-device matched include tags: which of the sync's include tags each
        # in-scope device actually carries. Intersect HERE (include_tags is in
        # scope) so the persisted payload is bounded by the include-tag count,
        # not the device's full tag list. In include_match="all" mode every
        # in-scope device carries all include tags, so the intersection equals
        # include_tags (identical to the historical single-tag/all behaviour);
        # in "any" mode it is exactly the carried subset.
        include_tag_set = set(include_tags)
        matched_tags_by_device: dict[str, list[str]] = {}
        for row in rows:
            name = str(row.get("name") or "").strip()
            if not name:
                continue
            device_tags = row.get("tagNames") or []
            matched = sorted(include_tag_set.intersection(str(t) for t in device_tags))
            if matched:
                matched_tags_by_device[name] = matched
        for row in rows:
            site = str(row.get("site") or "").strip().lower()
            if not site:
                continue
            sites.add(site)
            site_slug = slugify(site)
            if site_slug:
                sites.add(site_slug)
        if names:
            self.logger.log_info(
                f"Resolved device tag scope with {len(names)} matched devices "
                f"(include={include_tags or ['-']}, include_match={include_match}, "
                f"exclude={exclude_tags or ['-']}).",
                obj=self.sync,
            )
        else:
            self._warn_if_scope_all_backfilled(
                network_id=network_id,
                snapshot_id=snapshot_id,
                scope_where=scope_where,
                include_tags=include_tags,
                exclude_tags=exclude_tags,
                include_match=include_match,
            )
        endpoint_scope_failed = False
        if sync_endpoints:
            collected_device_count = len(names)
            if include_tags and not scope_endpoints_by_include_tags:
                self.logger.log_warning(
                    "SNMP endpoint import is not constrained by the configured "
                    "include tags. Endpoints without those tags can import; "
                    "enable `Scope SNMP Endpoints by Include Tags` on the "
                    "Forward source to apply the same include scope.",
                    obj=self.sync,
                )
            endpoint_scope = self._resolve_scoped_endpoint_scope(
                network_id=network_id,
                snapshot_id=snapshot_id,
                exclude_tags=exclude_tags,
                include_tags=include_tags,
                include_match=include_match,
                sync_generic_endpoints=sync_generic_endpoints,
                scope_endpoints_by_include_tags=scope_endpoints_by_include_tags,
            )
            if endpoint_scope is None:
                endpoint_scope_failed = True
            else:
                endpoint_names, endpoint_matched_tags = endpoint_scope
                names |= endpoint_names
                for name, endpoint_tags in endpoint_matched_tags.items():
                    matched_tags_by_device[name] = sorted(
                        set(matched_tags_by_device.get(name, ())) | set(endpoint_tags)
                    )
                if not collected_device_count and endpoint_names:
                    # The endpoint union makes dcim.device non-empty even when
                    # the tag scope matched no collected devices, which used to
                    # present as a confusing partial import (devices appear;
                    # interfaces/IPs — which only collected devices can have —
                    # come back empty). Say so explicitly.
                    self.logger.log_warning(
                        "Device tag scope matched 0 collected devices; "
                        f"{len(endpoint_names)} SNMP endpoint(s) will still "
                        "import, but interfaces and IP addresses require "
                        "collected devices in scope — verify the snapshot "
                        "selection (e.g. latestCollected) and the include tag "
                        "membership.",
                        obj=self.sync,
                    )
        return names, sites, matched_tags_by_device, endpoint_scope_failed

    def _resolve_scoped_endpoint_names(
        self,
        *,
        network_id: str,
        snapshot_id: str,
        exclude_tags: list[str],
        include_tags: list[str] | None = None,
        include_match: str = "any",
        sync_generic_endpoints: bool = False,
        scope_endpoints_by_include_tags: bool = False,
    ) -> set[str] | None:
        endpoint_scope = self._resolve_scoped_endpoint_scope(
            network_id=network_id,
            snapshot_id=snapshot_id,
            exclude_tags=exclude_tags,
            include_tags=include_tags,
            include_match=include_match,
            sync_generic_endpoints=sync_generic_endpoints,
            scope_endpoints_by_include_tags=scope_endpoints_by_include_tags,
        )
        return None if endpoint_scope is None else endpoint_scope[0]

    def _resolve_scoped_endpoint_scope(
        self,
        *,
        network_id: str,
        snapshot_id: str,
        exclude_tags: list[str],
        include_tags: list[str] | None = None,
        include_match: str = "any",
        sync_generic_endpoints: bool = False,
        scope_endpoints_by_include_tags: bool = False,
    ) -> tuple[set[str], dict[str, list[str]]] | None:
        """Names and matched include tags for endpoint rows the query emits.

        By default the device-tag include scope narrows the modeled-device
        universe only: on networks whose SNMP endpoints (e.g. Avocent console
        servers) carry none of the device scoping tags, requiring them would
        silently exclude every endpoint. When ``scope_endpoints_by_include_tags``
        is enabled, endpoints must also carry the include tags ("all"/"any" per
        ``include_match``) — the same gate the bundled device queries' endpoint
        branch applies, kept in lockstep via ``build_endpoint_tag_scope_where``.
        When endpoint import is enabled the resulting endpoint names are unioned
        into the scoped-device set so the local scope filter (and out-of-scope
        prune) keeps their rows. Exclude tags always apply as the safety valve.

        Returns ``None`` when the probe fails. The caller must then disable
        endpoint emission for the run entirely (not just proceed with an empty
        set): the device query would still emit endpoint rows, the local scope
        filter would drop them, and with prune-out-of-scope enabled the dropped
        rows would be emitted as DELETES of previously imported endpoints.
        """
        scoped_include_tags = (
            list(include_tags or []) if scope_endpoints_by_include_tags else []
        )
        where = [
            "where !isEmpty(endpoint.snmpOutputs)",
            *build_endpoint_tag_scope_where(
                scoped_include_tags, exclude_tags, include_match
            ),
            *build_endpoint_device_eligibility_where(
                sync_generic_endpoints=sync_generic_endpoints
            ),
        ]
        query = "\n".join(
            [
                "foreach endpoint in network.endpoints",
                *where,
                "select {",
                "  name: endpoint.name,",
                "  tagNames: endpoint.tagNames,",
                '  endpointKind: if isAvocent then "avocent" else if isOpengear then "opengear" else "generic"',
                "}",
            ]
        )
        try:
            rows = self.client.run_nqe_query(
                query=query,
                network_id=network_id,
                snapshot_id=snapshot_id,
                fetch_all=True,
            )
        except (ForwardClientError, ForwardConnectivityError, ForwardQueryError) as exc:
            self.logger.log_warning(
                "Forward endpoint scope probe failed; SNMP endpoint import is "
                f"disabled for this run: {_safe_exception_summary(exc)}",
                obj=self.sync,
            )
            return None
        endpoint_names = {
            str(row.get("name") or "").strip()
            for row in rows
            if str(row.get("name") or "").strip()
        }
        include_tag_set = set(include_tags or [])
        matched_tags_by_endpoint = {}
        for row in rows:
            name = str(row.get("name") or "").strip()
            if not name:
                continue
            matched = sorted(
                include_tag_set.intersection(
                    str(tag) for tag in (row.get("tagNames") or [])
                )
            )
            if matched:
                matched_tags_by_endpoint[name] = matched
        if endpoint_names:
            kind_counts = {
                kind: sum(
                    1
                    for row in rows
                    if str(row.get("endpointKind") or "generic") == kind
                )
                for kind in ("avocent", "opengear", "generic")
            }
            self.logger.log_info(
                f"Added {len(endpoint_names)} SNMP endpoint(s) to the device tag "
                "scope for opt-in endpoint import "
                f"(Avocent={kind_counts['avocent']}, "
                f"Opengear={kind_counts['opengear']}, "
                f"generic={kind_counts['generic']}).",
                obj=self.sync,
            )
        return endpoint_names, matched_tags_by_endpoint

    def _warn_if_scope_all_backfilled(
        self,
        *,
        network_id: str,
        snapshot_id: str,
        scope_where: list[str],
        include_tags: list[str],
        exclude_tags: list[str],
        include_match: str,
    ) -> None:
        """Distinguish "tag matched nothing" from "every match was backfilled".

        Re-probes the same tag scope without the ``completed`` collection filter.
        When that returns devices, the scope matches real devices that were all
        backfilled (collection canceled), so the sync would silently apply zero
        changes. Emit a warning so the cause is visible and point at the
        latestCollected selector. Best-effort: probe failures are swallowed so
        they never mask the (already-empty) scope result.
        """
        scope_label = (
            f"include={include_tags or ['-']}, include_match={include_match}, "
            f"exclude={exclude_tags or ['-']}"
        )
        probe_query = "\n".join(
            [
                "foreach device in network.devices",
                "where device.platform.vendor != Vendor.FORWARD_CUSTOM",
                *scope_where,
                "select {name: device.name}",
            ]
        )
        try:
            probe_rows = self.client.run_nqe_query(
                query=probe_query,
                network_id=network_id,
                snapshot_id=snapshot_id,
                limit=1,
                fetch_all=False,
            )
        except (ForwardClientError, ForwardConnectivityError, ForwardQueryError):
            probe_rows = []
        any_backfilled = any(str(row.get("name") or "").strip() for row in probe_rows)
        if any_backfilled:
            self.logger.log_warning(
                "Resolved device tag scope with 0 collected devices "
                f"({scope_label}) in snapshot {snapshot_id}, but matching devices "
                "exist that were backfilled because collection was canceled. "
                "Nothing will sync from this snapshot. Switch the sync snapshot "
                "selector to `latestCollected` to fall back to the most recent "
                "snapshot with collected devices, pin a specific snapshot, or "
                "re-run collection in Forward.",
                obj=self.sync,
            )
        else:
            self.logger.log_info(
                f"Resolved device tag scope with 0 matched devices ({scope_label}).",
                obj=self.sync,
            )

    def _drop_unavailable_integration_models(self, model_strings):
        """Drop models whose exact optional-plugin contract is unavailable.

        A model can be enabled in the sync config even when its integration
        plugin is absent, missing required models or package metadata, or at an
        unsupported version. Skip it before query execution so health reporting
        and runtime behavior enforce the same registry decision.
        """
        from .plugin_integrations.registry import integration_capability
        from .plugin_integrations.registry import optional_integration_for_model

        kept = []
        capabilities = {}
        for model_string in model_strings:
            integration = optional_integration_for_model(model_string)
            if integration:
                capability = capabilities.setdefault(
                    integration.key,
                    integration_capability(integration),
                )
            else:
                capability = None
            if capability is not None and not capability["available"]:
                self.logger.log_warning(
                    f"Skipping `{model_string}`: the optional "
                    f"`{integration.app_label}` integration is unavailable "
                    f"({capability['availability_status']}): "
                    f"{capability['availability_reason']}",
                    obj=self.sync,
                )
                continue
            kept.append(model_string)
        return kept

    def _query_jobs(self, context: ForwardQueryContext, *, model_strings=None):
        jobs = []
        enabled_models = self._drop_unavailable_integration_models(
            list(model_strings or self.sync.get_model_strings())
        )
        resolved_specs, spec_errors = self._resolve_specs_for_models(
            model_strings=enabled_models,
            maps=context.maps,
        )
        for model_string in enabled_models:
            if model_string in spec_errors:
                self._record_model_failure(
                    context,
                    model_string,
                    None,
                    spec_errors[model_string],
                    sync_mode="planning",
                )
                continue
            try:
                specs = resolved_specs.get(model_string, [])
                if not specs:
                    raise ForwardQueryError(
                        self._missing_query_specs_message(model_string)
                    )
                effective_specs = [
                    replace(
                        spec,
                        parameters=self._query_parameters_for_scope(
                            spec,
                            context,
                            None,
                        ),
                    )
                    for spec in specs
                ]
                ensure_unique_query_spec_executions(
                    effective_specs,
                )
            except ForwardQueryError as exc:
                self._record_model_failure(
                    context,
                    model_string,
                    None,
                    exc,
                    sync_mode="planning",
                )
                continue
            coalesce_fields = self._coalesce_fields(model_string, specs)
            for spec in specs:
                jobs.append((model_string, spec, coalesce_fields))
        return jobs

    def _missing_query_specs_message(self, model_string: str) -> str:
        optional_map_names = optional_builtin_query_names_for_model(model_string)
        if optional_map_names:
            quoted_names = ", ".join(f"`{name}`" for name in optional_map_names)
            return (
                f"No enabled NQE maps were resolved for {model_string}. "
                f"Enable the {quoted_names} NQE Map or disable the `{model_string}` "
                "model on the sync."
            )
        if model_string in FORWARD_OPTIONAL_MODELS:
            return (
                f"No enabled NQE maps were resolved for {model_string}. "
                f"Enable at least one NQE Map for `{model_string}` or disable the "
                f"`{model_string}` model on the sync."
            )
        return (
            f"No enabled built-in or custom query maps were resolved for {model_string}. "
            "Enable at least one NQE Map for this model before running the sync."
        )

    def fetch_workloads(
        self,
        context: ForwardQueryContext,
        *,
        validate_rows=True,
        model_strings=None,
        shard_scope=None,
        include_diagnostics,
    ) -> list[BranchWorkload]:
        workloads = []
        self.model_results = list(self._failed_model_results.values())
        jobs = self._build_workload_jobs(
            context,
            model_strings=model_strings,
            shard_scope=shard_scope,
        )
        # Building workload jobs can add new model-level failures (for example,
        # query-path resolution errors). Refresh the seeded failure results so
        # they are always visible in model_results even when no workload job ran.
        self.model_results = list(self._failed_model_results.values())
        if not jobs:
            return workloads
        self.logger.log_info(
            f"Fetching workload rows for {len(jobs)} query map job(s)."
        )
        max_workers = self._query_fetch_worker_count(len(jobs))
        results = [None] * len(jobs)
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(
                    self._run_thread_job,
                    self._run_workload_job,
                    (context, validate_rows, job),
                ): index
                for index, job in enumerate(jobs)
            }
            completed = 0
            for future in as_completed(futures):
                index = futures[future]
                model_result, workload = future.result()
                results[index] = (model_result, workload)
                completed += 1
                self.logger.log_info(
                    f"Fetched workload job {completed}/{len(jobs)} for "
                    f"{model_result.model_string} "
                    f"({model_result.execution_mode} `{model_result.execution_value}`) "
                    f"in {model_result.runtime_ms}ms."
                )
        for result in results:
            if result is None:
                continue
            model_result, workload = result
            self.model_results.append(model_result)
            if workload is not None:
                workloads.append(workload)
        existing_cable_ids_by_endpoint = self._existing_cable_ids_by_endpoint(workloads)
        workloads, normalization_summaries = normalize_dependency_workloads(
            workloads,
            existing_cable_ids_by_endpoint=existing_cable_ids_by_endpoint,
        )
        self._record_workload_normalization_summaries(normalization_summaries)
        if include_diagnostics and self._query_diagnostics_enabled():
            self._append_ipaddress_diagnostics(context)
            self._append_ipaddress_parent_prefix_diagnostics(workloads)
            self._append_routing_diagnostics(context)
        workloads, self.pending_workload_states, state_summaries = (
            apply_durable_workload_deltas(self.sync, workloads)
        )
        self._record_durable_workload_state_summaries(state_summaries)
        return workloads

    def _existing_cable_ids_by_endpoint(self, workloads):
        cable_device_names = {
            str(row.get(field) or "").strip()
            for workload in workloads
            if workload.model_string == "dcim.cable"
            for row in workload.upsert_rows
            for field in ("device", "remote_device")
            if str(row.get(field) or "").strip()
        }
        if not cable_device_names:
            return {}
        from dcim.models import Interface

        return {
            (str(device_name), str(interface_name)): int(cable_id)
            for device_name, interface_name, cable_id in Interface.objects.filter(
                device__name__in=cable_device_names,
                cable_id__isnull=False,
            ).values_list("device__name", "name", "cable_id")
        }

    def _record_workload_normalization_summaries(self, summaries):
        if not summaries:
            return
        by_identity = {
            (
                summary["model"],
                summary["query_name"],
                summary["execution_value"],
            ): summary
            for summary in summaries
        }
        updated_results = []
        for result in self.model_results:
            summary = by_identity.get(
                (result.model_string, result.query_name, result.execution_value)
            )
            if summary is None:
                updated_results.append(result)
                continue
            diagnostic = {
                "type": "dependency_workload_normalization",
                "excluded_row_count": summary["excluded_row_count"],
                "reason_counts": summary["reason_counts"],
                "enrichment_counts": summary.get("enrichment_counts") or {},
            }
            updated_results.append(
                replace(
                    result,
                    row_count=summary["kept_row_count"],
                    diagnostics=[*result.diagnostics, diagnostic],
                )
            )
            if summary["excluded_row_count"]:
                self.logger.log_info(
                    f"Excluded {summary['excluded_row_count']} non-representable "
                    f"{summary['model']} row(s) before branch planning; kept "
                    f"{summary['kept_row_count']}/{summary['input_row_count']} "
                    f"({summary['reason_counts']}).",
                    obj=self.sync,
                )
            if summary.get("enrichment_counts"):
                self.logger.log_info(
                    f"Enriched {summary['model']} workload before branch planning "
                    f"({summary['enrichment_counts']}).",
                    obj=self.sync,
                )
        self.model_results = updated_results

    def _record_durable_workload_state_summaries(self, summaries):
        if not summaries:
            return
        by_model = {summary["model"]: summary for summary in summaries}
        updated_results = []
        for result in self.model_results:
            summary = by_model.get(result.model_string)
            if summary is None:
                updated_results.append(result)
                continue
            diagnostic = {
                "type": "durable_workload_state",
                "mode": summary["mode"],
                "target_row_count": summary["target_rows"],
                "staged_upsert_count": summary["upsert_rows"],
                "staged_delete_count": summary["delete_rows"],
                "bootstrap_delete_count": summary["bootstrap_delete_rows"],
                "protected_delete_count": summary["protected_delete_rows"],
                "tombstone_count": summary["tombstone_rows"],
                "unrepresented_peer": summary["unrepresented_peer"],
                "compressed_bytes": summary["compressed_bytes"],
            }
            updated_results.append(
                replace(result, diagnostics=[*result.diagnostics, diagnostic])
            )
        self.model_results = updated_results
        for summary in summaries:
            self.logger.log_info(
                "Durable workload state for "
                f"{summary['model']}: mode={summary['mode']} "
                f"target={summary['target_rows']} "
                f"upserts={summary['upsert_rows']} "
                f"deletes={summary['delete_rows']} "
                f"bootstrap_deletes={summary['bootstrap_delete_rows']} "
                f"compressed_bytes={summary['compressed_bytes']}.",
                obj=self.sync,
            )

    def _workload_fetch_retry_config(self):
        """Return ``(attempts, backoff_seconds)`` for transient fetch retries.

        Tunable per source via ``workload_fetch_retry_attempts`` /
        ``workload_fetch_retry_backoff_seconds`` (the latter may be 0 to disable
        sleeping, e.g. in tests).
        """
        source = getattr(self.sync, "source", None)
        parameters = dict(getattr(source, "parameters", {}) or {})
        try:
            attempts = int(
                parameters.get(
                    "workload_fetch_retry_attempts",
                    DEFAULT_WORKLOAD_FETCH_RETRY_ATTEMPTS,
                )
            )
        except (TypeError, ValueError):
            attempts = DEFAULT_WORKLOAD_FETCH_RETRY_ATTEMPTS
        try:
            backoff = float(
                parameters.get(
                    "workload_fetch_retry_backoff_seconds",
                    DEFAULT_WORKLOAD_FETCH_RETRY_BACKOFF_SECONDS,
                )
            )
        except (TypeError, ValueError):
            backoff = DEFAULT_WORKLOAD_FETCH_RETRY_BACKOFF_SECONDS
        return max(0, attempts), max(0.0, backoff)

    def _workload_fetch_timeout_seconds(self):
        source = getattr(self.sync, "source", None)
        parameters = dict(getattr(source, "parameters", {}) or {})
        try:
            timeout = int(
                parameters.get(
                    "workload_fetch_timeout_seconds",
                    DEFAULT_WORKLOAD_FETCH_TIMEOUT_SECONDS,
                )
            )
        except (TypeError, ValueError):
            timeout = DEFAULT_WORKLOAD_FETCH_TIMEOUT_SECONDS
        return max(0, timeout)

    def _query_diagnostics_enabled(self) -> bool:
        source = getattr(self.sync, "source", None)
        parameters = dict(getattr(source, "parameters", {}) or {})
        configured = parameters.get("query_diagnostics_enabled")
        if configured is None:
            return True
        return configured

    def _diff_fallback_mode(self) -> str:
        sync_parameters = dict(getattr(self.sync, "parameters", {}) or {})
        configured = str(
            sync_parameters.get(
                "diff_fallback_mode",
                ForwardDiffFallbackModeChoices.ALLOW_FALLBACK,
            )
            or ForwardDiffFallbackModeChoices.ALLOW_FALLBACK
        ).strip()
        valid = {choice[0] for choice in ForwardDiffFallbackModeChoices.CHOICES}
        if configured not in valid:
            raise ForwardQueryError(
                f"Unsupported diff fallback mode `{configured}` on sync {self.sync.pk}."
            )
        return configured

    def _require_diff_execution(self) -> bool:
        return self._diff_fallback_mode() == ForwardDiffFallbackModeChoices.REQUIRE_DIFF

    def _build_workload_jobs(
        self,
        context: ForwardQueryContext,
        *,
        model_strings=None,
        shard_scope=None,
    ):
        jobs = []
        enabled_models = self._drop_unavailable_integration_models(
            list(model_strings or self.sync.get_model_strings())
        )
        resolved_specs, spec_errors = self._resolve_specs_for_models(
            model_strings=enabled_models,
            maps=context.maps,
        )
        for model_string in enabled_models:
            if model_string in self._failed_model_results:
                continue
            if model_string in spec_errors:
                self._record_model_failure(
                    context,
                    model_string,
                    None,
                    spec_errors[model_string],
                    sync_mode="planning",
                )
                continue
            try:
                specs = resolved_specs.get(model_string, [])
                if not specs:
                    raise ForwardQueryError(
                        self._missing_query_specs_message(model_string)
                    )
                coalesce_fields = self._coalesce_fields(model_string, specs)
                baseline = self._incremental_baseline_for_specs(context, specs)
                scoped_specs = [
                    (
                        spec,
                        self._scope_for_spec(model_string, spec, shard_scope),
                    )
                    for spec in specs
                ]
                effective_specs = [
                    replace(
                        spec,
                        parameters=self._query_parameters_for_scope(
                            spec,
                            context,
                            scope,
                        ),
                    )
                    for spec, scope in scoped_specs
                ]
                ensure_unique_query_spec_executions(effective_specs)
                has_runtime_parameters = any(
                    bool(spec.parameters) for spec in effective_specs
                )
                if baseline is not None and has_runtime_parameters:
                    if self._require_diff_execution():
                        raise ForwardQueryError(
                            "Diff execution is required, but Forward NQE diffs do "
                            "not accept runtime query parameters for "
                            f"{model_string}. Use Allow full fallback for "
                            "parameterized maps."
                        )
                    baseline = None
                    self.logger.log_info(
                        f"Using full execution for every {model_string} map because "
                        "at least one map has runtime parameters and Forward NQE "
                        "diffs are parameterless.",
                        obj=self.sync,
                    )
            except ForwardQueryError as exc:
                self._record_model_failure(
                    context,
                    model_string,
                    None,
                    exc,
                    sync_mode="planning",
                )
                continue
            if baseline is not None:
                self.logger.log_info(
                    f"Selected Forward diff baseline ingestion `{baseline.pk}` "
                    f"on snapshot `{baseline.snapshot_id}` for {model_string}.",
                    obj=self.sync,
                )
            for spec, scope in scoped_specs:
                jobs.append(
                    (
                        model_string,
                        spec,
                        baseline,
                        coalesce_fields,
                        scope,
                    )
                )
        return jobs

    def _resolved_specs_for_model(self, *, model_string: str, maps):
        cached = self._resolved_specs_cache.get(model_string)
        if cached is not None:
            return cached
        specs = get_query_specs(model_string, maps=maps)
        resolved = self._resolve_query_specs(model_string, specs)
        self._resolved_specs_cache[model_string] = resolved
        return resolved

    def _resolve_specs_for_models(self, *, model_strings, maps):
        started = time.perf_counter()
        resolved_specs: dict[str, list[Any]] = {}
        spec_errors: dict[str, Exception] = {}
        unresolved_models = []
        for model_string in list(model_strings or []):
            cached = self._resolved_specs_cache.get(model_string)
            if cached is not None:
                resolved_specs[model_string] = cached
            else:
                unresolved_models.append(model_string)

        if not unresolved_models:
            elapsed_ms = round((time.perf_counter() - started) * 1000, 1)
            self.logger.log_info(
                f"Resolved query specs for {len(resolved_specs)} model(s) from cache in {elapsed_ms}ms.",
                obj=self.sync,
            )
            return resolved_specs, spec_errors

        def resolve_model_specs(model_string: str):
            specs = get_query_specs(model_string, maps=maps)
            return self._resolve_query_specs(model_string, specs)

        max_workers = self._query_fetch_worker_count(len(unresolved_models))
        if max_workers <= 1:
            for model_string in unresolved_models:
                try:
                    resolved = resolve_model_specs(model_string)
                except JobTimeoutException:
                    raise
                except Exception as exc:
                    spec_errors[model_string] = exc
                    continue
                self._resolved_specs_cache[model_string] = resolved
                resolved_specs[model_string] = resolved
            elapsed_ms = round((time.perf_counter() - started) * 1000, 1)
            self.logger.log_info(
                "Resolved query specs for "
                f"{len(resolved_specs)} model(s) with {len(spec_errors)} failure(s) "
                f"using 1 worker in {elapsed_ms}ms.",
                obj=self.sync,
            )
            return resolved_specs, spec_errors

        indexed_results: list[tuple[str, list[Any] | None, Exception | None]] = [
            ("", None, None)
        ] * len(unresolved_models)
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(
                    self._run_thread_job,
                    resolve_model_specs,
                    model_string,
                ): index
                for index, model_string in enumerate(unresolved_models)
            }
            for future in as_completed(futures):
                index = futures[future]
                model_string = unresolved_models[index]
                try:
                    resolved = future.result()
                except JobTimeoutException:
                    raise
                except Exception as exc:
                    indexed_results[index] = (model_string, None, exc)
                    continue
                indexed_results[index] = (model_string, resolved, None)

        for model_string, resolved, exc in indexed_results:
            if exc is not None:
                spec_errors[model_string] = exc
                continue
            if resolved is None:
                continue
            self._resolved_specs_cache[model_string] = resolved
            resolved_specs[model_string] = resolved
        elapsed_ms = round((time.perf_counter() - started) * 1000, 1)
        self.logger.log_info(
            "Resolved query specs for "
            f"{len(resolved_specs)} model(s) with {len(spec_errors)} failure(s) "
            f"using {max_workers} worker(s) in {elapsed_ms}ms.",
            obj=self.sync,
        )
        return resolved_specs, spec_errors

    def _incremental_baseline_for_specs(
        self, context: ForwardQueryContext, specs: list[Any]
    ):
        if context.snapshot_selector != LATEST_PROCESSED_SNAPSHOT:
            return None
        if not specs or any(not getattr(spec, "diff_query_id", None) for spec in specs):
            return None
        cache_key = (
            context.snapshot_selector,
            context.snapshot_id,
            getattr(context, "ingestion_id", None),
        )
        if cache_key in self._incremental_baseline_cache:
            return self._incremental_baseline_cache[cache_key]
        baseline = self.sync.incremental_diff_baseline(
            specs=specs,
            current_snapshot_id=context.snapshot_id,
            exclude_ingestion_id=getattr(context, "ingestion_id", None),
            client=self.client,
        )
        self._incremental_baseline_cache[cache_key] = baseline
        return baseline

    def _resolve_query_specs(self, model_string: str, specs):
        resolved_specs = resolve_query_specs_for_client(specs, self.client)
        self._query_path_resolution_cache[model_string] = (
            self._build_query_path_resolution_summary(model_string, specs)
        )
        return resolved_specs

    def _build_query_path_resolution_summary(
        self, model_string: str, specs
    ) -> dict[str, Any]:
        query_path_spec_count = sum(
            1 for spec in specs if getattr(spec, "query_path", None)
        )
        return {
            "available": bool(query_path_spec_count),
            "query_path_spec_count": query_path_spec_count,
            "resolved_spec_count": query_path_spec_count,
            "message": (
                f"Resolved {query_path_spec_count} query_path spec(s) with "
                "Forward lookups."
                if query_path_spec_count
                else "No query_path specs were present for this model."
            ),
        }

    def _run_workload_job(self, payload):
        context, validate_rows, job = payload
        model_string, spec, baseline, coalesce_fields, shard_scope = job
        baseline_snapshot_id = getattr(baseline, "snapshot_id", "") or ""
        started = time.perf_counter()
        budget = self._workload_fetch_timeout_seconds()
        deadline = (time.monotonic() + budget) if budget else None
        attempts, backoff = self._workload_fetch_retry_config()
        for attempt in range(attempts + 1):
            try:
                rows, delete_rows, sync_mode, fetch_meta = self._fetch_spec_rows(
                    model_string,
                    spec,
                    baseline,
                    context,
                    coalesce_fields,
                    shard_scope=shard_scope,
                    return_fetch_meta=True,
                    deadline=deadline,
                )
                if validate_rows:
                    self.validate_rows(
                        model_string,
                        rows,
                        delete_rows,
                        coalesce_fields,
                    )
                break
            except (
                ForwardClientError,
                ForwardConnectivityError,
                ForwardQueryError,
            ) as exc:
                if (
                    attempt < attempts
                    and _is_transient_fetch_error(exc)
                    and (deadline is None or time.monotonic() < deadline)
                ):
                    self.logger.log_warning(
                        (
                            f"Transient fetch error for {model_string} "
                            f"(`{spec.execution_value}`); retrying "
                            f"{attempt + 1}/{attempts}: {exc}"
                        ),
                        obj=self.sync,
                    )
                    if backoff > 0:
                        time.sleep(backoff * (attempt + 1))
                    continue
                runtime_ms = round((time.perf_counter() - started) * 1000, 1)
                if isinstance(exc, ForwardFetchBudgetExceededError):
                    self.logger.log_warning(
                        f"Fetch budget exceeded for {model_string} after {budget} second(s): {exc}",
                        obj=self.sync,
                    )
                return (
                    self._failure_result(
                        context,
                        model_string,
                        spec,
                        exc,
                        sync_mode="planning",
                        runtime_ms=runtime_ms,
                    ),
                    None,
                )
        runtime_ms = round((time.perf_counter() - started) * 1000, 1)
        apply_engine_decision = apply_engine_decision_for(
            sync=self.sync,
            model_string=model_string,
        )
        model_result = ForwardModelResult(
            model_string=model_string,
            query_name=spec.query_name,
            execution_mode=spec.execution_mode,
            execution_value=spec.execution_value,
            sync_mode=sync_mode,
            row_count=len(rows),
            delete_count=len(delete_rows),
            runtime_ms=runtime_ms,
            snapshot_id=context.snapshot_id,
            baseline_snapshot_id=baseline_snapshot_id if sync_mode == "diff" else "",
            apply_engine=apply_engine_decision.selected_engine,
            apply_engine_reason=apply_engine_decision.reason,
            apply_engine_decision=apply_engine_decision.as_dict(),
            fetch_mode=fetch_meta.get("fetch_mode") or "model",
            fetch_key_family=fetch_meta.get("fetch_key_family") or "",
            fetch_parameters=dict(fetch_meta.get("fetch_parameters") or {}),
            query_parameters=dict(fetch_meta.get("query_parameters") or {}),
            query_path_resolution=dict(
                self._query_path_resolution_cache.get(model_string) or {}
            ),
        )
        workload = None
        if (
            rows
            or delete_rows
            or (sync_mode == "full" and bool(fetch_meta.get("query_parameters")))
        ):
            workload = BranchWorkload(
                model_string=model_string,
                label=f"{model_string} | {spec.query_name}",
                upsert_rows=rows,
                delete_rows=delete_rows,
                sync_mode=sync_mode,
                coalesce_fields=coalesce_fields,
                query_name=spec.query_name,
                execution_mode=spec.execution_mode,
                execution_value=spec.execution_value,
                query_runtime_ms=runtime_ms,
                baseline_snapshot_id=(
                    baseline_snapshot_id if sync_mode == "diff" else ""
                ),
                apply_engine=apply_engine_decision.selected_engine,
                apply_engine_reason=apply_engine_decision.reason,
                apply_engine_decision=apply_engine_decision.as_dict(),
                fetch_mode=fetch_meta.get("fetch_mode") or "model",
                fetch_key_family=fetch_meta.get("fetch_key_family") or "",
                fetch_parameters=dict(fetch_meta.get("fetch_parameters") or {}),
                query_parameters=dict(fetch_meta.get("query_parameters") or {}),
            )
        return model_result, workload

    def _scope_for_spec(self, model_string, spec, shard_scope):
        if not shard_scope:
            return None
        if str(shard_scope.get("model") or "") != model_string:
            return None
        if shard_scope.get("query_name") and shard_scope.get("query_name") != getattr(
            spec, "query_name", ""
        ):
            return None
        if shard_scope.get("execution_value") and shard_scope.get(
            "execution_value"
        ) != getattr(spec, "execution_value", ""):
            return None
        shard_keys = tuple(shard_scope.get("shard_keys") or ())
        if not shard_keys:
            return None
        return {
            "shard_keys": shard_keys,
            **shard_fetch_contract(model_string, shard_keys),
        }

    def _record_model_failure(
        self,
        context: ForwardQueryContext,
        model_string: str,
        spec,
        exc: Exception,
        *,
        sync_mode: str,
    ) -> None:
        if model_string in self._failed_model_results:
            return
        result = self._failure_result(
            context,
            model_string,
            spec,
            exc,
            sync_mode=sync_mode,
        )
        self._failed_model_results[model_string] = result
        self.logger.log_warning(
            f"Skipping {model_string} because Forward query validation failed: {_safe_exception_summary(exc)}",
            obj=self.sync,
        )

    def _failure_result(
        self,
        context: ForwardQueryContext,
        model_string: str,
        spec,
        exc: Exception,
        *,
        sync_mode: str,
        runtime_ms: float | None = None,
    ) -> ForwardModelResult:
        return ForwardModelResult(
            model_string=model_string,
            query_name=getattr(spec, "query_name", "") or model_string,
            execution_mode=getattr(spec, "execution_mode", "") or "",
            execution_value=getattr(spec, "execution_value", "") or "",
            sync_mode=sync_mode,
            row_count=0,
            delete_count=0,
            failure_count=1,
            runtime_ms=runtime_ms,
            snapshot_id=context.snapshot_id,
            **self._apply_engine_result_fields(model_string),
            query_path_resolution=dict(
                self._query_path_resolution_cache.get(model_string) or {}
            ),
            diagnostics=[
                {
                    "name": (
                        "fetch_budget_exceeded"
                        if isinstance(exc, ForwardFetchBudgetExceededError)
                        else "query_validation_failure"
                    ),
                    "message": self._failure_message(model_string, spec, exc),
                }
            ],
        )

    def _failure_message(self, model_string: str, spec, exc: Exception) -> str:
        message = _safe_exception_summary(exc)
        if model_string != "dcim.virtualchassis" or spec is None:
            return message

        binding = self._virtual_chassis_binding_message(spec)
        if not binding:
            return message
        return f"{message} {binding}"

    def _virtual_chassis_binding_message(self, spec) -> str:
        mode = getattr(spec, "execution_mode", "") or ""
        value = getattr(spec, "execution_value", "") or ""
        if mode == "query_id" and value:
            return (
                f"Forward Virtual Chassis is bound to query_id `{value}`; "
                "upgrading the plugin will not rewrite the published Forward query."
            )
        if mode == "query_path" and value:
            return (
                f"Forward Virtual Chassis is bound to repository query `{value}`; "
                "republish that query before retrying."
            )
        if mode == "query":
            return "Forward Virtual Chassis is using bundled raw query text."
        return ""

    def _append_ipaddress_diagnostics(self, context: ForwardQueryContext) -> None:
        return sync_append_ipaddress_diagnostics(self, context)

    def _append_ipaddress_parent_prefix_diagnostics(self, workloads) -> None:
        return sync_append_ipaddress_parent_prefix_diagnostics(self, workloads)

    def _run_ipaddress_unassignable_diagnostic(
        self,
        context: ForwardQueryContext,
    ) -> dict[str, Any] | None:
        return sync_append_ipaddress_diagnostics(self, context)

    def _summarize_unassignable_ipaddress_rows(self, rows: list[dict]) -> dict:
        return sync_summarize_unassignable_ipaddress_rows(rows)

    def _append_routing_diagnostics(self, context: ForwardQueryContext) -> None:
        return sync_append_routing_diagnostics(self, context)

    def _run_routing_import_diagnostic(
        self,
        context: ForwardQueryContext,
    ) -> dict[str, Any] | None:
        return sync_append_routing_diagnostics(self, context)

    def _summarize_routing_import_diagnostic_rows(self, rows: list[dict]) -> dict:
        return sync_summarize_routing_import_diagnostic_rows(rows)

    def _diagnostic_row_count(self, row: dict) -> int:
        return sync_diagnostic_row_count(row)

    def fetch_sample_results(
        self,
        context: ForwardQueryContext,
        *,
        row_limit=DEFAULT_SAMPLE_ROW_LIMIT,
        model_strings=None,
    ) -> list[ForwardModelResult]:
        self.model_results = []
        jobs = self._query_jobs(context, model_strings=model_strings)
        if not jobs:
            return self.model_results
        max_workers = self._query_fetch_worker_count(len(jobs))
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            for result in executor.map(
                self._run_thread_job,
                ((self._run_sample_job, (context, row_limit, job)) for job in jobs),
            ):
                self.model_results.append(result)
        self._append_ipaddress_diagnostics(context)
        self._append_routing_diagnostics(context)
        return self.model_results

    def _run_sample_job(self, payload):
        context, row_limit, job = payload
        model_string, spec, coalesce_fields = job
        started = time.perf_counter()
        rows = self._run_nqe_query(
            spec=spec,
            context=context,
            parameters=self._query_parameters_for_scope(spec, context, None),
            limit=row_limit,
            fetch_all=False,
        )
        rows, _ = self._apply_device_tag_scope(model_string, rows, context)
        self.validate_rows(model_string, rows, [], coalesce_fields)
        runtime_ms = round((time.perf_counter() - started) * 1000, 1)
        return ForwardModelResult(
            model_string=model_string,
            query_name=spec.query_name,
            execution_mode=spec.execution_mode,
            execution_value=spec.execution_value,
            sync_mode="sample",
            row_count=len(rows),
            runtime_ms=runtime_ms,
            snapshot_id=context.snapshot_id,
            **self._apply_engine_result_fields(model_string),
            query_path_resolution=dict(
                self._query_path_resolution_cache.get(model_string) or {}
            ),
        )

    def _apply_engine_result_fields(self, model_string: str) -> dict[str, Any]:
        decision = apply_engine_decision_for(
            sync=self.sync,
            model_string=model_string,
        )
        return {
            "apply_engine": decision.selected_engine,
            "apply_engine_reason": decision.reason,
            "apply_engine_decision": decision.as_dict(),
        }

    def validate_rows(
        self,
        model_string: str,
        rows: list[dict],
        delete_rows: list[dict],
        coalesce_fields: list[list[str]],
    ) -> None:
        for row in rows:
            validate_row_shape_for_model(model_string, row, coalesce_fields)
        for row in delete_rows:
            validate_row_shape_for_model(model_string, row, coalesce_fields)
        if model_string == "dcim.virtualchassis":
            self._validate_virtual_chassis_positions(rows)

    def _validate_virtual_chassis_positions(self, rows: list[dict]) -> None:
        seen_positions = {}
        for row in rows:
            vc_name = row.get("vc_name") or row.get("name")
            position = row.get("vc_position")
            device = row.get("device")
            if position in (None, ""):
                continue
            key = (vc_name, position)
            if key not in seen_positions:
                seen_positions[key] = device
                continue
            if seen_positions[key] == device:
                continue
            raise ForwardQueryError(
                "Duplicate virtual chassis position returned by Forward NQE: "
                f"`{vc_name}` position `{position}` is assigned to both "
                f"`{seen_positions[key]}` and `{device}`."
            )

    def _resolve_snapshot_info(
        self,
        *,
        network_id: str,
        snapshot_selector: str,
        snapshot_id: str,
    ) -> dict[str, Any]:
        if (
            snapshot_selector == snapshot_id
            or snapshot_selector == LATEST_COLLECTED_SNAPSHOT
        ):
            for snapshot in self.client.get_snapshots(network_id):
                if snapshot["id"] == snapshot_id:
                    return {
                        "id": snapshot["id"],
                        "state": snapshot.get("state") or "",
                        "createdAt": snapshot.get("created_at") or "",
                        "processedAt": snapshot.get("processed_at") or "",
                    }
            return {}
        if snapshot_selector == LATEST_PROCESSED_SNAPSHOT:
            return self.client.get_latest_processed_snapshot(network_id)
        return {}

    def _fetch_spec_rows(
        self,
        model_string,
        spec,
        baseline,
        context: ForwardQueryContext,
        coalesce_fields,
        *,
        shard_scope=None,
        return_fetch_meta=False,
        deadline=None,
    ):
        original_shard_scope = dict(shard_scope or {}) if shard_scope else None

        def _return(rows, delete_rows, sync_mode, metadata):
            metadata = dict(metadata or {})
            if return_fetch_meta:
                return rows, delete_rows, sync_mode, metadata
            return rows, delete_rows, sync_mode

        runner = ForwardSyncRunner(
            sync=self.sync,
            ingestion=None,
            client=self.client,
            logger_=self.logger,
        )
        runner._model_coalesce_fields[model_string] = coalesce_fields
        if shard_scope:
            fetch_mode = str(shard_scope.get("fetch_mode") or "model")
            if fetch_mode not in {"model", "nqe_parameters"}:
                raise ForwardQueryError(
                    f"Unsupported shard fetch mode `{fetch_mode}` for `{model_string}`."
                )
        metadata_shard_scope = original_shard_scope or shard_scope
        requested_fetch_mode = "model"
        fetch_key_family = ""
        fetch_parameters = {}
        if metadata_shard_scope:
            requested_fetch_mode = metadata_shard_scope.get("fetch_mode") or "model"
            fetch_key_family = metadata_shard_scope.get("fetch_key_family") or ""
            fetch_parameters = dict(metadata_shard_scope.get("fetch_parameters") or {})
        parameters = spec.merged_parameters(context.query_parameters)
        if metadata_shard_scope:
            if metadata_shard_scope.get("fetch_mode") == "nqe_parameters":
                parameters = {
                    **parameters,
                    **(metadata_shard_scope.get("fetch_parameters") or {}),
                }
            if metadata_shard_scope.get("query_parameters"):
                parameters = {
                    **parameters,
                    **(metadata_shard_scope.get("query_parameters") or {}),
                }
            if metadata_shard_scope.get("fetch_mode") != "model":
                self.logger.log_info(
                    f"Fetching {model_string} shard using {metadata_shard_scope['fetch_mode']} scope.",
                    obj=self.sync,
                )
        parameters = self._apply_context_tag_parameters(spec, parameters, context)
        parameters = self._validate_query_parameters(model_string, spec, parameters)
        query_parameters = dict(parameters)
        if (
            baseline is not None
            and spec.run_query_id
            and context.device_tag_prune_out_of_scope
            and context.scoped_device_names
        ):
            if self._require_diff_execution():
                raise ForwardQueryError(
                    "Diff execution is required, but prune-out-of-scope requires full "
                    f"query execution for {model_string}. Disable prune or allow diff fallback."
                )
            self.logger.log_info(
                f"Tag prune mode enabled for {model_string}; running full query execution "
                "to compute out-of-scope deletions.",
                obj=self.sync,
            )
        elif baseline is not None and spec.run_query_id and parameters:
            message = (
                "Forward NQE diffs do not accept runtime query parameters; "
                f"{model_string} requires parameterized execution for "
                f"`{spec.execution_value}`."
            )
            if self._require_diff_execution():
                raise ForwardQueryError(
                    f"Diff execution is required, but {message} "
                    "Use Allow full fallback for parameterized maps."
                )
            self.logger.log_info(
                f"{message} Running full async query execution instead.",
                obj=self.sync,
            )
            fallback_parameters = dict(fetch_parameters)
            fallback_parameters["fallback_reason"] = "parameterized_query"
            fetch_parameters = fallback_parameters
            requested_fetch_mode = "diff_fallback"
        elif baseline is not None and spec.run_query_id:
            try:
                diff_rows = self._run_nqe_diff(
                    spec=spec,
                    context=context,
                    before_snapshot_id=baseline.snapshot_id,
                    deadline=deadline,
                )
                rows, delete_rows = runner._split_diff_rows(model_string, diff_rows)
                rows, _ = self._apply_device_tag_scope(model_string, rows, context)
                delete_rows, _ = self._apply_device_tag_scope(
                    model_string, delete_rows, context
                )
                if original_shard_scope:
                    rows, delete_rows = self._filter_rows_to_shard(
                        model_string,
                        rows,
                        delete_rows,
                        coalesce_fields,
                        original_shard_scope,
                    )
                return _return(
                    rows,
                    delete_rows,
                    "diff",
                    {
                        "fetch_mode": requested_fetch_mode,
                        "fetch_key_family": fetch_key_family,
                        "fetch_parameters": fetch_parameters,
                        "query_parameters": query_parameters,
                    },
                )
            except (ForwardClientError, ForwardConnectivityError) as exc:
                if isinstance(exc, ForwardFetchBudgetExceededError):
                    raise
                safe_exc = _safe_exception_summary(exc)
                if self._require_diff_execution():
                    raise ForwardQueryError(
                        "Diff execution is required and Forward NQE diff failed for "
                        f"{model_string} using `{spec.execution_value}`: {safe_exc}"
                    ) from exc
                self.logger.log_warning(
                    f"Forward NQE diff failed for {model_string} using `{spec.execution_value}`; "
                    f"falling back to full query execution: {safe_exc}",
                    obj=self.sync,
                )
                fallback_parameters = dict(fetch_parameters)
                fallback_parameters["fallback_reason"] = safe_exc
                requested_mode = requested_fetch_mode if shard_scope else "diff"
                requested_fetch_mode = (
                    "diff_fallback" if requested_mode != "model" else "model"
                )
                fetch_parameters = fallback_parameters
        elif baseline is not None and not spec.run_query_id:
            if self._require_diff_execution():
                raise ForwardQueryError(
                    "Diff execution is required, but "
                    f"`{spec.execution_value}` for {model_string} has no query_id. "
                    "Use query_path/query_id map mode or allow diff fallback."
                )
            self.logger.log_warning(
                f"Forward diffs require a query_id; `{spec.execution_value}` is still raw query text, so running a full query for {model_string} instead.",
                obj=self.sync,
            )
        elif spec.run_query_id:
            latest_baseline = self.sync.latest_baseline_ingestion(
                exclude_ingestion_id=getattr(context, "ingestion_id", None)
            )
            if (
                latest_baseline is not None
                and latest_baseline.snapshot_id == context.snapshot_id
            ):
                if self._require_diff_execution():
                    raise ForwardQueryError(
                        "Diff execution is required, but no newer processed snapshot is "
                        f"available for {model_string}; latest baseline ingestion `{latest_baseline.pk}` "
                        f"already matches snapshot `{context.snapshot_id}`."
                    )
                self.logger.log_info(
                    f"Forward diffs require a newer processed snapshot than the latest baseline; "
                    f"baseline ingestion `{latest_baseline.pk}` already matches snapshot `{context.snapshot_id}`, "
                    f"so running full query execution for {model_string} instead.",
                    obj=self.sync,
                )

        try:
            rows = self._run_nqe_query(
                spec=spec,
                context=context,
                parameters=parameters,
                fetch_all=True,
                deadline=deadline,
            )
        except (ForwardClientError, ForwardConnectivityError) as exc:
            if isinstance(exc, ForwardFetchBudgetExceededError):
                raise
            safe_exc = _safe_exception_summary(exc)
            if shard_scope and shard_scope.get("fetch_mode") != "model":
                raise ForwardQueryError(
                    "Shard-scoped NQE fetch failed and full-model fallback is disabled "
                    f"for {model_string}: {safe_exc}"
                ) from exc
            raise
        if original_shard_scope:
            rows, _ = self._filter_rows_to_shard(
                model_string,
                rows,
                [],
                coalesce_fields,
                original_shard_scope,
            )
        filtered_rows, removed_rows = self._apply_device_tag_scope(
            model_string, rows, context
        )
        delete_rows = removed_rows if context.device_tag_prune_out_of_scope else []
        return _return(
            filtered_rows,
            delete_rows,
            "full",
            {
                "fetch_mode": requested_fetch_mode,
                "fetch_key_family": fetch_key_family,
                "fetch_parameters": fetch_parameters,
                "query_parameters": query_parameters,
            },
        )

    def _query_parameters_for_scope(self, spec, context: ForwardQueryContext, scope):
        parameters = spec.merged_parameters(context.query_parameters)
        model_string = getattr(spec, "model_string", "") or getattr(
            spec, "query_name", ""
        )
        if scope:
            if scope.get("fetch_mode") == "nqe_parameters":
                parameters = {
                    **parameters,
                    **(scope.get("fetch_parameters") or {}),
                }
            if scope.get("query_parameters"):
                parameters = {
                    **parameters,
                    **(scope.get("query_parameters") or {}),
                }
        parameters = self._apply_context_tag_parameters(
            spec, dict(parameters or {}), context
        )
        return self._validate_query_parameters(model_string, spec, parameters)

    def _apply_context_tag_parameters(
        self,
        spec,
        parameters: dict[str, Any],
        context: ForwardQueryContext,
    ) -> dict[str, Any]:
        spec_parameters = getattr(spec, "parameters", {}) or {}
        if not isinstance(spec_parameters, dict):
            spec_parameters = {}
        accepts_device_tag_parameters = any(
            key in spec_parameters
            for key in (
                "device_tag_include_tags",
                "device_tag_exclude_tags",
                "device_tag_include_match",
            )
        )
        sanitized_parameters = {
            key: value
            for key, value in parameters.items()
            if not str(key).startswith("device_tag_")
        }
        # Push the resolved device-tag scope into the Forward fetch as device-name
        # shard keys for device-keyed queries, so the fetch returns only in-scope
        # devices instead of the whole network (the post-fetch filter would
        # otherwise discard the out-of-scope rows after paying to fetch them).
        # Skip when a per-shard key set is already present (do not widen it).
        scoped_device_names = context.scoped_device_names
        if (
            scoped_device_names
            and getattr(spec, "model_string", "") in DEVICE_NAME_SCOPED_MODELS
            and "forward_netbox_shard_keys" in spec_parameters
            and not sanitized_parameters.get("forward_netbox_shard_keys")
        ):
            sanitized_parameters["forward_netbox_shard_keys"] = sorted(
                scoped_device_names
            )
        # Push the operator's selected sync-tags into any query that declares the
        # sync_device_tags parameter (the device-tag sync query), so the user picks
        # exactly which Forward tags become NetBox device tags.
        if "sync_device_tags" in spec_parameters:
            sanitized_parameters["sync_device_tags"] = sorted(
                getattr(context, "sync_device_tags", None) or []
            )
        if "sync_endpoints" in spec_parameters:
            sanitized_parameters["sync_endpoints"] = bool(
                getattr(context, "sync_endpoints", False)
            )
        if "sync_generic_endpoints" in spec_parameters:
            sanitized_parameters["sync_generic_endpoints"] = bool(
                getattr(context, "sync_generic_endpoints", False)
            )
        if "scope_endpoints_by_include_tags" in spec_parameters:
            sanitized_parameters["scope_endpoints_by_include_tags"] = bool(
                getattr(context, "scope_endpoints_by_include_tags", False)
            )
        if not accepts_device_tag_parameters:
            return sanitized_parameters
        tag_parameters = {
            "device_tag_include_tags": list(context.device_tag_include_tags or []),
            "device_tag_include_match": context.device_tag_include_match or "any",
            "device_tag_exclude_tags": list(context.device_tag_exclude_tags or []),
        }
        return {**sanitized_parameters, **tag_parameters}

    def _validate_query_parameters(
        self,
        model_string: str,
        spec,
        parameters: dict[str, Any],
    ) -> dict[str, Any]:
        spec_parameters = getattr(spec, "parameters", {}) or {}
        if not isinstance(spec_parameters, dict):
            spec_parameters = {}
        if not spec_parameters:
            return parameters
        unexpected = sorted(key for key in parameters if key not in spec_parameters)
        if unexpected:
            query_name = getattr(spec, "query_name", "") or model_string
            raise ForwardQueryError(
                "Forward NQE map "
                f"`{query_name}` for {model_string} produced unsupported parameter(s): "
                f"{', '.join(unexpected)}. Update the query contract instead of "
                "injecting runtime parameters."
            )
        return parameters

    def _apply_device_tag_scope(
        self, model_string: str, rows: list[dict], context: ForwardQueryContext
    ) -> tuple[list[dict], list[dict]]:
        scoped_devices = context.scoped_device_names or set()
        tag_scope_enabled = bool(
            context.device_tag_include_tags or context.device_tag_exclude_tags
        )
        if not scoped_devices:
            if tag_scope_enabled:
                if rows:
                    self.logger.log_info(
                        f"Applied device-tag scope to {model_string}: kept 0/{len(rows)} rows.",
                        obj=self.sync,
                    )
                return [], list(rows)
            return rows, []
        filtered = []
        removed = []
        for row in rows:
            row_devices = _row_device_names(model_string, row)
            if not row_devices:
                if model_string == "dcim.site" and context.scoped_site_names:
                    row_sites = _row_site_names(row)
                    if row_sites.intersection(context.scoped_site_names):
                        filtered.append(row)
                    else:
                        removed.append(row)
                    continue
                filtered.append(row)
                continue
            if model_string == "dcim.cable" and row_devices.issubset(scoped_devices):
                filtered.append(row)
                continue
            if model_string != "dcim.cable" and row_devices.intersection(
                scoped_devices
            ):
                filtered.append(row)
                continue
            removed.append(row)
        removed_count = len(removed)
        if removed_count:
            self.logger.log_info(
                f"Applied device-tag scope to {model_string}: kept {len(filtered)}/{len(rows)} rows.",
                obj=self.sync,
            )
        return filtered, removed

    def _filter_rows_to_shard(
        self,
        model_string,
        rows,
        delete_rows,
        coalesce_fields,
        shard_scope,
    ):
        shard_keys = set(shard_scope.get("shard_keys") or ())
        if not shard_keys:
            return rows, delete_rows

        def in_scope(row):
            try:
                return row_shard_key(model_string, row, coalesce_fields) in shard_keys
            except ForwardQueryError:
                return False

        return [row for row in rows if in_scope(row)], [
            row for row in delete_rows if in_scope(row)
        ]

    def _run_thread_job(self, func, payload=None):
        if payload is None:
            func, payload = func
        close_old_connections()
        try:
            return func(payload)
        finally:
            connection.close()
            connections.close_all()

    def _coalesce_fields(self, model_string, specs) -> list[list[str]]:
        if specs:
            return [list(field_set) for field_set in specs[0].coalesce_fields] or (
                architecture_default_coalesce_fields_for_model(model_string)
            )
        return architecture_default_coalesce_fields_for_model(model_string)

    def _query_fetch_worker_count(self, job_count: int) -> int:
        source_parameters = (
            getattr(getattr(self.sync, "source", None), "parameters", None) or {}
        )
        configured = source_parameters.get("query_fetch_concurrency")
        if configured in ("", None):
            worker_limit = self._default_query_fetch_concurrency()
        else:
            try:
                worker_limit = int(configured)
            except (TypeError, ValueError):
                worker_limit = self._default_query_fetch_concurrency()
        worker_limit = max(1, min(MAX_QUERY_FETCH_CONCURRENCY, worker_limit))
        return max(1, min(worker_limit, int(job_count)))

    def _default_query_fetch_concurrency(self) -> int:
        return DEFAULT_QUERY_FETCH_CONCURRENCY

    def _run_nqe_query(
        self,
        *,
        spec,
        context: ForwardQueryContext,
        parameters: dict[str, Any],
        limit: int | None = None,
        fetch_all: bool = False,
        deadline=None,
    ):
        return self.client.run_nqe_query(
            query=spec.query,
            query_id=spec.run_query_id,
            commit_id=spec.commit_id,
            network_id=context.network_id,
            snapshot_id=context.snapshot_id,
            parameters=parameters,
            limit=limit,
            fetch_all=fetch_all,
            deadline=deadline,
        )

    def _run_nqe_diff(
        self,
        *,
        spec,
        context: ForwardQueryContext,
        before_snapshot_id: str,
        deadline=None,
    ):
        return self.client.run_nqe_diff(
            query_id=spec.run_query_id,
            commit_id=spec.commit_id,
            before_snapshot_id=before_snapshot_id,
            after_snapshot_id=context.snapshot_id,
            fetch_all=True,
            deadline=deadline,
        )


def plan_item_model_result(
    item: BranchPlanItem,
    context: dict[str, Any],
    *,
    total_plan_items: int,
) -> dict[str, Any]:
    return {
        "model": item.model_string,
        "query_name": item.query_name or item.label,
        "execution_mode": item.execution_mode,
        "execution_value": item.execution_value,
        "sync_mode": item.sync_mode,
        "operation": item.operation,
        "row_count": len(item.upsert_rows),
        "delete_count": len(item.delete_rows),
        "failure_count": 0,
        "runtime_ms": item.query_runtime_ms,
        "snapshot_id": context["snapshot_id"],
        "baseline_snapshot_id": item.baseline_snapshot_id,
        "branch_plan_index": item.index,
        "branch_plan_total": total_plan_items,
        "estimated_changes": item.estimated_changes,
        "shard_key_count": len(item.shard_keys),
        "apply_engine": item.apply_engine,
        "apply_engine_reason": item.apply_engine_reason,
        "apply_engine_decision": item.apply_engine_decision,
    }
