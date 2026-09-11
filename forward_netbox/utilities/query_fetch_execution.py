import json
import time
from concurrent.futures import as_completed
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from dataclasses import field
from dataclasses import replace
from threading import Lock
from typing import Any

from django.db import close_old_connections
from django.db import connection
from django.db import connections
from django.db import DatabaseError
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
from .contributor_baseline import compatible_current_relation
from .contributor_baseline import ContributorBaselineExpectation
from .contributor_baseline import ContributorBaselineUnavailable
from .contributor_baseline import ContributorRelationContract
from .contributor_baseline import ContributorRelationSeed
from .contributor_baseline import ContributorWorkRelation
from .contributor_baseline import decode_scope_payload
from .contributor_baseline import stage_contributor_baseline
from .diagnostics import exception_type
from .diagnostics import failure_classifier
from .diagnostics import failure_reason
from .diagnostics import safe_exception_summary
from .diagnostics import safe_operation_failure
from .forward_api import build_device_tag_scope_where
from .forward_api import build_endpoint_device_eligibility_where
from .forward_api import build_endpoint_tag_scope_where
from .forward_api import DEFAULT_QUERY_FETCH_CONCURRENCY
from .forward_api import LATEST_COLLECTED_SNAPSHOT
from .forward_api import LATEST_PROCESSED_SNAPSHOT
from .forward_api import MAX_QUERY_FETCH_CONCURRENCY
from .full_removal_reconciliation import coalesce_identity
from .full_removal_reconciliation import compute_full_removals
from .full_removal_reconciliation import network_complete_removals
from .full_removal_reconciliation import previous_full_rows
from .full_removal_reconciliation import prune_removals_allowed
from .full_removal_reconciliation import RemovalReconciliationRefused
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
from .query_execution_contract import canonical_sha256
from .query_execution_contract import compatible_baseline_evidence
from .query_execution_contract import diff_artifact_key
from .query_execution_contract import DiffArtifact
from .query_execution_contract import DiffArtifactStore
from .query_execution_contract import resolve_execution_contract
from .query_execution_contract import resolve_model_execution_contract
from .query_execution_contract import ResolvedExecutionContract
from .query_execution_contract import scope_config_fingerprint
from .query_execution_contract import scope_membership_fingerprint
from .query_registry import ensure_unique_query_spec_executions
from .query_registry import get_query_specs
from .query_registry import only_legacy_safe_default_parameters
from .query_registry import optional_builtin_query_names_for_model
from .query_registry import QuerySpec
from .query_registry import resolve_query_specs_for_client
from .sync import ForwardSyncRunner
from .sync_contracts import validate_row_shape_for_model
from .sync_facade import effective_scope_endpoints_by_include_tags
from .tier3_reducers import contributor_target_key
from .tier3_reducers import diff_normalized_model_rows
from .tier3_reducers import is_tier3_reducer
from .tier3_reducers import reduce_contributor_rows
from .tier3_reducers import scope_side_from_context
from .tier3_reducers import scope_side_from_payload
from .tier3_reducers import scope_state_from_context
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
DEFAULT_DIFF_FETCH_TIMEOUT_SECONDS = 60
DEFAULT_DIFF_TIMEOUT_CIRCUIT_BREAKER_THRESHOLD = 1
MAX_DIFF_TIMEOUT_CIRCUIT_BREAKER_THRESHOLD = 100
DIFF_BUDGET_FALLBACK_REASON = "diff_budget_exceeded:ForwardFetchBudgetExceededError"
DIFF_CIRCUIT_OPEN_FALLBACK_REASON = "diff_circuit_open:ForwardFetchBudgetExceededError"
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


def _safe_exception_summary(exc: Exception) -> str:
    """Classifier plus a value-free reason, for every fetch-path failure.

    This used to return `f"{exc.__class__.__name__}."`, which destroyed the
    reason at capture - before the logger, before the job record, before the
    database - so no downstream tooling could recover it at any cost. Delegates
    to the shared formatter so every call site gains the reason at once.
    """
    return safe_exception_summary(exc)


# Shapes Forward uses when it refuses to RUN a query because the request does
# not match the query's own declaration. Matched to select fixed wording; the
# server's text is never carried into the message.
_NQE_CONTRACT_REJECTION_MARKERS = (
    "is not a parameter to the given query",
    "nqe_runtime_error",
)


def _is_nqe_contract_rejection(exc: Exception) -> bool:
    haystack = str(exc or "").casefold()
    return any(marker in haystack for marker in _NQE_CONTRACT_REJECTION_MARKERS)


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
    # Opt-in: apply the matched include tags to their NetBox devices.
    apply_device_scope_tags: bool = False
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
            "apply_device_scope_tags": self.apply_device_scope_tags,
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
    # Why THIS model failed. `failure_count` alone says a model failed; without
    # these two, a run in which every model failed for one reason and a run in
    # which each failed for its own present identically. Both are value-free: an
    # exception class name and a slug from the diagnostics catalogue.
    failure_exception: str = ""
    failure_reason: str = ""
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
    execution_contract_fingerprint: str = ""
    map_set_fingerprint: str = ""
    scope_config_fingerprint: str = ""
    scope_membership_fingerprint: str = ""

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
            "failure_exception": self.failure_exception,
            "failure_reason": self.failure_reason,
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
            "execution_contract_fingerprint": (self.execution_contract_fingerprint),
            "map_set_fingerprint": self.map_set_fingerprint,
            "scope_config_fingerprint": self.scope_config_fingerprint,
            "scope_membership_fingerprint": self.scope_membership_fingerprint,
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
        self._snapshot_scope_context_cache: dict[
            tuple[Any, ...], ForwardQueryContext
        ] = {}
        self._diff_artifacts = DiffArtifactStore()
        self._diff_timeout_lock = Lock()
        self._diff_timeout_counts: dict[str, int] = {}
        self.pending_workload_states = []
        # Full normalised rows per model, captured before the durable delta
        # narrows them. The drift preview measures against these; the plan
        # is built from the narrowed workloads.
        self.comparison_rows_by_model: dict[str, list[dict]] = {}
        self._contributor_lock = Lock()
        self._pending_contributor_seeds: dict[str, ContributorRelationSeed] = {}
        self._pending_contributor_work_relations: list[ContributorWorkRelation] = []
        self._expected_contributor_contracts: dict[str, ContributorRelationContract] = (
            {}
        )
        self._contributor_map_set_fingerprint = ""
        self._contributor_staging_blocked = False

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
        apply_device_scope_tags = bool(source_parameters.get("apply_device_scope_tags"))
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
            tuple(sync_device_tags),
            apply_device_scope_tags,
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
        except Exception as exc:  # noqa: BLE001 - metrics are best-effort
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
            apply_device_scope_tags=apply_device_scope_tags,
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
        from .plugin_integrations.registry import (
            integration_capability,
            optional_integration_for_model,
        )

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
        capture_comparison_rows=False,
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
        # Capture the full normalised rows BEFORE the durable delta narrows
        # them, because the delta is computed against this plugin's own record
        # of what Forward last returned - not against NetBox.
        #
        # `apply_durable_workload_deltas` drops any workload whose upsert and
        # delete lists are both empty, which is correct for a PLAN (there is
        # nothing to stage) and wrong for a MEASUREMENT. The drift preview built
        # its comparison from the surviving workloads, so a model that had not
        # changed since the last run vanished from the comparison entirely,
        # reported "Not measured", and fell back to an estimate of every fetched
        # row. The models in perfect sync were the ones shown as maximally
        # uncertain - one deployment saw 30 of 32 models that way.
        #
        # These rows are what a comparison has to read to say anything true
        # about drift: an empty delta means Forward has not changed, and says
        # nothing about whether NetBox still matches it.
        #
        # Opt-in, because it pins every fetched row for the lifetime of the
        # fetcher. On the sync path the rows the delta discards become garbage
        # immediately, and keeping them alive there would be a memory
        # regression at the scale that makes this worth measuring at all. Only
        # the preview asks for them.
        self.comparison_rows_by_model = {}
        if capture_comparison_rows:
            for workload in workloads:
                self.comparison_rows_by_model.setdefault(
                    workload.model_string, []
                ).extend(workload.upsert_rows or [])
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
        from dcim.models import CableTermination, Interface

        # CableTermination is the authoritative relationship. Branch merge
        # does not retain Interface's signal-derived cable cache, so a
        # populated baseline can legitimately have null Interface.cable_id for
        # every connected endpoint.
        cable_id_by_interface_id = {
            int(interface_id): int(cable_id)
            for interface_id, cable_id in CableTermination.objects.filter(
                termination_type__app_label="dcim",
                termination_type__model="interface",
                _device__name__in=cable_device_names,
            ).values_list("termination_id", "cable_id")
        }
        if not cable_id_by_interface_id:
            return {}
        return {
            (str(device_name), str(interface_name)): cable_id_by_interface_id[
                int(interface_id)
            ]
            for interface_id, device_name, interface_name in Interface.objects.filter(
                pk__in=cable_id_by_interface_id
            ).values_list("pk", "device__name", "name")
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

    def _diff_fetch_timeout_seconds(self):
        source = getattr(self.sync, "source", None)
        parameters = dict(getattr(source, "parameters", {}) or {})
        try:
            timeout = int(
                parameters.get(
                    "diff_fetch_timeout_seconds",
                    DEFAULT_DIFF_FETCH_TIMEOUT_SECONDS,
                )
            )
        except (TypeError, ValueError):
            timeout = DEFAULT_DIFF_FETCH_TIMEOUT_SECONDS
        return max(1, timeout)

    def _diff_timeout_circuit_breaker_threshold(self):
        source = getattr(self.sync, "source", None)
        parameters = dict(getattr(source, "parameters", {}) or {})
        try:
            threshold = int(
                parameters.get(
                    "diff_timeout_circuit_breaker_threshold",
                    DEFAULT_DIFF_TIMEOUT_CIRCUIT_BREAKER_THRESHOLD,
                )
            )
        except (TypeError, ValueError):
            threshold = DEFAULT_DIFF_TIMEOUT_CIRCUIT_BREAKER_THRESHOLD
        return max(
            1,
            min(MAX_DIFF_TIMEOUT_CIRCUIT_BREAKER_THRESHOLD, threshold),
        )

    def _diff_deadline(self, workload_deadline=None):
        deadline = time.monotonic() + self._diff_fetch_timeout_seconds()
        if workload_deadline is not None:
            deadline = min(deadline, workload_deadline)
        return deadline

    def _diff_circuit_key(self, contract):
        return str(getattr(contract, "fingerprint", "") or "")

    def _diff_circuit_is_open(self, contract) -> bool:
        key = self._diff_circuit_key(contract)
        if not key:
            return False
        with self._diff_timeout_lock:
            count = self._diff_timeout_counts.get(key, 0)
        return count >= self._diff_timeout_circuit_breaker_threshold()

    def _record_diff_timeout(self, contract) -> bool:
        key = self._diff_circuit_key(contract)
        if not key:
            return False
        with self._diff_timeout_lock:
            count = self._diff_timeout_counts.get(key, 0) + 1
            self._diff_timeout_counts[key] = count
        return count >= self._diff_timeout_circuit_breaker_threshold()

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
        contract_preflight_blocked = False
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
                effective_specs = self._hydrate_snapshot_data_file_hashes(
                    effective_specs,
                    context,
                )
                ensure_unique_query_spec_executions(effective_specs)
                contracts = [
                    resolve_execution_contract(
                        effective_spec,
                        effective_parameters=effective_spec.parameters,
                    )
                    for (_spec, _scope), effective_spec in zip(
                        scoped_specs,
                        effective_specs,
                        strict=True,
                    )
                ]
                model_contract = resolve_model_execution_contract(
                    model_string,
                    contracts,
                    context=context,
                )
                self._report_contract_compatibility_issues(
                    model_string,
                    model_contract.maps,
                )
                unsafe_full_contract = next(
                    (
                        contract
                        for contract in model_contract.maps
                        if not contract.full_eligible
                    ),
                    None,
                )
                if unsafe_full_contract is not None:
                    contract_preflight_blocked = True
                    raise ForwardQueryError(
                        "Execution contract preflight rejected an unsafe full "
                        f"contract for {model_string}: "
                        f"{unsafe_full_contract.full_reason_code}."
                    )
                self._register_expected_contributor_contracts(model_contract)
                if self._require_diff_execution() and not model_contract.diff_eligible:
                    raise ForwardQueryError(
                        "Diff execution is required, but the resolved execution "
                        f"contract for {model_string} is full-only "
                        f"({model_contract.reason_code})."
                    )
                baseline = (
                    self._incremental_baseline_for_specs(
                        context,
                        model_contract,
                    )
                    if model_contract.diff_eligible
                    else None
                )
                if self._require_diff_execution() and baseline is None:
                    raise ForwardQueryError(
                        "Diff execution is required, but no compatible baseline "
                        f"with complete contract and before-scope provenance exists "
                        f"for {model_string}."
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
            scopes_by_contract = {
                contract.fingerprint: scope
                for contract, (_spec, scope) in zip(
                    contracts,
                    scoped_specs,
                    strict=True,
                )
            }
            for contract in model_contract.maps:
                jobs.append(
                    (
                        model_string,
                        contract,
                        baseline,
                        coalesce_fields,
                        scopes_by_contract.get(contract.fingerprint),
                        model_contract,
                    )
                )
        if contract_preflight_blocked:
            self.logger.log_warning(
                "Execution contract preflight blocked all Forward workload "
                "execution because at least one enabled map has an unsafe full "
                "contract. No NQE workload request was scheduled.",
                obj=self.sync,
            )
            return []
        with self._contributor_lock:
            self._contributor_map_set_fingerprint = canonical_sha256(
                {
                    "contracts": sorted(self._expected_contributor_contracts),
                }
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
                    self._log_spec_resolution_error(model_string, exc)
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
                self._log_spec_resolution_error(model_string, exc)
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

    def _log_spec_resolution_error(
        self,
        model_string: str,
        exc: Exception,
    ) -> None:
        # This is intentionally an info-level diagnostic classifier. Support-bundle
        # redaction replaces warning/error message bodies, while this helper emits
        # only the model contract and exception class, never exception content.
        self.logger.log_info(
            safe_operation_failure(
                f"Forward query spec resolution for {model_string}",
                exc,
            ),
            obj=self.sync,
        )

    def _incremental_baseline_for_specs(
        self, context: ForwardQueryContext, model_contract
    ):
        if context.snapshot_selector != LATEST_PROCESSED_SNAPSHOT:
            return None
        if not getattr(model_contract, "diff_eligible", False):
            return None
        cache_key = (
            model_contract.model_string,
            model_contract.map_set_fingerprint,
            model_contract.scope_config_fingerprint,
            context.snapshot_selector,
            context.snapshot_id,
            getattr(context, "ingestion_id", None),
        )
        if cache_key in self._incremental_baseline_cache:
            return self._incremental_baseline_cache[cache_key]
        baseline = self.sync.incremental_diff_baseline(
            model_contract=model_contract,
            current_snapshot_id=context.snapshot_id,
            exclude_ingestion_id=getattr(context, "ingestion_id", None),
            client=self.client,
        )
        self._incremental_baseline_cache[cache_key] = baseline
        return baseline

    @staticmethod
    def _model_contract_issue_rows(contract):
        issues = []
        if not contract.full_eligible:
            issues.append(f"full:{contract.full_reason_code}")
        if not contract.diff_eligible:
            issues.append(f"diff:{contract.diff_reason_code}")
        return issues

    def _report_contract_compatibility_issues(self, model_string, contracts):
        # A full-contract issue skips the model outright; a diff-contract issue
        # is inert unless diffs are required. Reporting both as warnings buried
        # the blocking ones under routine diff noise, so they are split by
        # whether they actually stop this run.
        def _rows(predicate, issue_selector):
            return [
                (
                    contract.query_name or model_string,
                    contract.map_id,
                    issue_selector(contract),
                )
                for contract in contracts
                if predicate(contract)
            ]

        def _render(rows):
            return "; ".join(
                f"{name}[{map_id}]: {issues}" for name, map_id, issues in rows
            )

        blocking = _rows(
            lambda contract: not contract.full_eligible,
            lambda contract: f"full:{contract.full_reason_code}",
        )
        if blocking:
            self.logger.log_warning(
                f"Execution contract preflight found {len(blocking)} rejected "
                f"map(s) for {model_string}, so this model will be skipped and "
                "its data will not be synced: " + _render(blocking),
                obj=self.sync,
            )

        # Not a problem, but not nothing either: this map runs a query the
        # plugin holds no copy of, so no source, declaration, or parameter check
        # stands behind it and the first sign of a mismatch is Forward refusing
        # the execution. Said once here so the state is visible rather than
        # inferred from a reason code nothing prints.
        unverifiable = _rows(
            lambda contract: contract.full_reason_code == "remote_source_only",
            lambda contract: "full:remote_source_only",
        )
        if unverifiable:
            self.logger.log_info(
                f"{len(unverifiable)} map(s) for {model_string} run a Forward "
                "query this plugin holds no copy of, so their source and "
                "parameters cannot be checked before execution: "
                + _render(unverifiable),
                obj=self.sync,
            )

        # Still a warning: a map that cannot diff silently falls back to a full
        # fetch, which the operator should know about even in a full-only run.
        diff_only = _rows(
            lambda contract: contract.full_eligible and not contract.diff_eligible,
            lambda contract: f"diff:{contract.diff_reason_code}",
        )
        if diff_only:
            self.logger.log_warning(
                f"Execution contract preflight found {len(diff_only)} map(s) for "
                f"{model_string} that cannot run a diff; this model still syncs "
                "in full: " + _render(diff_only),
                obj=self.sync,
            )

    def _resolve_query_specs(self, model_string: str, specs):
        resolved_specs = resolve_query_specs_for_client(specs, self.client)
        self._query_path_resolution_cache[model_string] = (
            self._build_query_path_resolution_summary(model_string, specs)
        )
        return resolved_specs

    def _hydrate_snapshot_data_file_hashes(self, specs, context):
        required = {
            name
            for spec in specs
            for name in getattr(spec, "required_data_files", ()) or ()
        }
        if not required:
            return specs
        try:
            available = self.client.get_snapshot_data_file_hashes(
                context.network_id,
                context.snapshot_id,
            )
        except JobTimeoutException:
            raise
        except Exception as exc:  # noqa: BLE001 - missing hashes fail closed
            self.logger.log_warning(
                safe_operation_failure("Snapshot data-file fingerprint lookup", exc),
                obj=self.sync,
            )
            available = {}
        return [
            replace(
                spec,
                data_file_hashes={
                    name: available[name]
                    for name in getattr(spec, "required_data_files", ()) or ()
                    if name in available
                },
            )
            for spec in specs
        ]

    @staticmethod
    def _contributor_relation_contract(contract):
        diff_revision = contract.diff_revision
        if diff_revision is None:
            raise ContributorBaselineUnavailable(
                "Tier 3 contributor execution requires a verified diff revision."
            )
        return ContributorRelationContract(
            model_string=contract.model_string,
            map_id=contract.map_id,
            contract_key=contract.contract_key,
            query_path=(diff_revision.query_path or contract.full_revision.query_path),
            query_id=contract.full_revision.query_id,
            full_commit_id=contract.full_revision.commit_id,
            full_source_sha256=contract.full_revision.source_sha256,
            diff_query_id=diff_revision.query_id,
            diff_commit_id=diff_revision.commit_id,
            diff_source_sha256=diff_revision.source_sha256,
            contract_fingerprint=contract.fingerprint,
            reducer_id=contract.reducer_id,
            reducer_version=contract.reducer_version,
            normalization_version=contract.normalization_version,
            identity_version=contract.identity_version,
        )

    def _register_expected_contributor_contracts(self, model_contract):
        with self._contributor_lock:
            for contract in model_contract.maps:
                if (
                    contract.diff_eligible
                    and contract.diff_ownership_mode == "contributor_relation"
                    and is_tier3_reducer(contract.reducer_id)
                ):
                    relation_contract = self._contributor_relation_contract(contract)
                    self._expected_contributor_contracts[contract.fingerprint] = (
                        relation_contract
                    )

    def _register_contributor_seed(
        self,
        contract,
        rows,
        *,
        work_relation=None,
    ):
        relation_contract = self._contributor_relation_contract(contract)
        reducer_id = contract.reducer_id
        seed = ContributorRelationSeed(
            contract=relation_contract,
            rows=rows,
            target_key=lambda row, reducer_id=reducer_id: contributor_target_key(
                reducer_id,
                row,
            ),
        )
        with self._contributor_lock:
            existing = self._pending_contributor_seeds.get(contract.fingerprint)
            if existing is not None:
                self._contributor_staging_blocked = True
                raise ContributorBaselineUnavailable(
                    "Tier 3 contributor relation was produced more than once."
                )
            self._pending_contributor_seeds[contract.fingerprint] = seed
            if work_relation is not None:
                self._pending_contributor_work_relations.append(work_relation)

    def _block_contributor_staging(self):
        with self._contributor_lock:
            self._contributor_staging_blocked = True

    def close_pending_contributor_relations(self):
        with self._contributor_lock:
            work_relations = list(self._pending_contributor_work_relations)
            self._pending_contributor_work_relations = []
            self._pending_contributor_seeds = {}
        for work_relation in work_relations:
            work_relation.close()

    def stage_pending_contributor_baseline(self, ingestion, context) -> int:
        with self._contributor_lock:
            expected = dict(self._expected_contributor_contracts)
            seeds = dict(self._pending_contributor_seeds)
            blocked = self._contributor_staging_blocked
            map_set_fingerprint = self._contributor_map_set_fingerprint
        if not expected:
            self.close_pending_contributor_relations()
            return 0
        if blocked or set(seeds) != set(expected):
            self.logger.log_warning(
                "Contributor baseline staging was skipped because the complete "
                "eligible Tier 3 relation set was not produced.",
                obj=ingestion,
            )
            self.close_pending_contributor_relations()
            return 0
        try:
            stage_contributor_baseline(
                ingestion,
                [seeds[key] for key in sorted(seeds)],
                network_fingerprint=canonical_sha256(
                    {"network_id": str(context.network_id or "")}
                ),
                map_set_fingerprint=map_set_fingerprint,
                scope_config_fingerprint=scope_config_fingerprint(context),
                scope_membership_fingerprint=scope_membership_fingerprint(context),
                scope_state=scope_state_from_context(context),
            )
        except JobTimeoutException:
            raise
        except (ContributorBaselineUnavailable, DatabaseError) as exc:
            self.logger.log_warning(
                safe_operation_failure("Contributor baseline staging", exc),
                obj=ingestion,
            )
            return 0
        finally:
            self.close_pending_contributor_relations()
        return len(seeds)

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
        if len(job) != 6:
            return self._execute_workload_job(payload)
        (
            model_string,
            requested_contract,
            baseline,
            coalesce_fields,
            _shard_scope,
            model_contract,
        ) = job
        if baseline is None or not model_contract.diff_eligible:
            return self._execute_workload_job(payload)
        baseline_evidence = compatible_baseline_evidence(baseline, model_contract)
        if baseline_evidence is None:
            return self._execute_workload_job(payload)
        artifact_key = diff_artifact_key(
            model_contract,
            before_snapshot_id=baseline.snapshot_id,
            after_snapshot_id=context.snapshot_id,
            before_scope_membership_fingerprint=(
                baseline_evidence.scope_membership_fingerprint
            ),
        )

        def build_artifact():
            map_results = []
            for contract in model_contract.maps:
                result, workload = self._execute_workload_job(
                    (
                        context,
                        validate_rows,
                        (
                            model_string,
                            contract,
                            baseline,
                            coalesce_fields,
                            None,
                            model_contract,
                        ),
                    )
                )
                map_results.append((contract.fingerprint, result, workload))
            return DiffArtifact(key=artifact_key, map_results=tuple(map_results))

        artifact = self._diff_artifacts.get_or_build(artifact_key, build_artifact)
        for contract_fingerprint, result, workload in artifact.map_results:
            if contract_fingerprint == requested_contract.fingerprint:
                return result, workload
        raise ForwardQueryError(
            f"Diff artifact for {model_string} omitted a resolved map contract."
        )

    def _execute_workload_job(self, payload):
        context, validate_rows, job = payload
        model_string, spec_or_contract, baseline, coalesce_fields, shard_scope = job[:5]
        model_contract = job[5] if len(job) == 6 else None
        if isinstance(spec_or_contract, ResolvedExecutionContract):
            contract = spec_or_contract
            spec = contract.spec
        elif not callable(getattr(spec_or_contract, "merged_parameters", None)):
            contract = spec_or_contract
            spec = spec_or_contract
            model_contract = None
        else:
            spec = spec_or_contract
            contract = resolve_execution_contract(
                spec,
                effective_parameters=self._query_parameters_for_scope(
                    spec,
                    context,
                    shard_scope,
                ),
            )
            model_contract = resolve_model_execution_contract(
                model_string,
                [contract],
                context=context,
            )
        baseline_snapshot_id = getattr(baseline, "snapshot_id", "") or ""
        started = time.perf_counter()
        budget = self._workload_fetch_timeout_seconds()
        deadline = (time.monotonic() + budget) if budget else None
        attempts, backoff = self._workload_fetch_retry_config()
        for attempt in range(attempts + 1):
            try:
                rows, delete_rows, sync_mode, fetch_meta = self._fetch_spec_rows(
                    model_string,
                    contract,
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
                        contract=contract,
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
            scope_membership_fingerprint=(
                model_contract.after_scope_membership_fingerprint
                if model_contract is not None
                else ""
            ),
            execution_contract_fingerprint=(
                model_contract.execution_contract_fingerprint
                if model_contract is not None
                else ""
            ),
            map_set_fingerprint=(
                model_contract.map_set_fingerprint if model_contract is not None else ""
            ),
            scope_config_fingerprint=(
                model_contract.scope_config_fingerprint
                if model_contract is not None
                else ""
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
            f"Skipping {model_string} because Forward query validation failed: "
            f"{_safe_exception_summary(exc)}",
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
            failure_exception=exc.__class__.__name__,
            failure_reason=failure_reason(exc) or "unrecognized-fetch-failure",
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
        query_binding = self._query_id_binding_failure_message(spec, exc)
        if query_binding:
            message = f"{message} {query_binding}"
        if model_string != "dcim.virtualchassis" or spec is None:
            return message

        binding = self._virtual_chassis_binding_message(spec)
        if not binding:
            return message
        return f"{message} {binding}"

    def _query_id_binding_failure_message(self, spec, exc: Exception) -> str:
        """Name the map behind a failed ID-bound execution.

        A map bound to a query ID with no commit runs whatever Forward has at
        head, so its query can change under us between one sync and the next.
        When that happens the run fails HERE, per model, rather than at
        preflight - which is the intended trade, but only if the failure says
        which map it was. `_safe_exception_summary` deliberately keeps no
        exception content, so without this the operator sees one word.

        The rejection sentence below is fixed text chosen by matching a shape in
        the exception, never the exception's own words, so no server-provided
        content is persisted.
        """

        if spec is None or (getattr(spec, "execution_mode", "") or "") != "query_id":
            return ""
        query_name = str(getattr(spec, "query_name", "") or "").strip()
        map_id = getattr(spec, "map_id", None)
        label = f"`{query_name}`" if query_name else "the bound map"
        if map_id is not None:
            label = f"{label} [{map_id}]"
        revision = (
            "a pinned commit"
            if str(getattr(spec, "commit_id", "") or "").strip()
            else "Forward's latest commit"
        )
        binding = (
            f"Map {label} is bound to Forward query ID "
            f"`{getattr(spec, 'execution_value', '') or ''}` and ran at {revision}"
        )
        if _is_nqe_contract_rejection(exc):
            return (
                f"{binding}. Forward rejected the parameters this map supplies, "
                "so the query behind that ID no longer declares them. Re-publish "
                "the bundled query to that ID, or rebind the map."
            )
        return f"{binding}."

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
        contract=None,
    ) -> None:
        # A map bound to a query ID runs at whatever Forward has at head, so the
        # row shape is confirmed here, against the rows actually returned, not by
        # pre-reading a commit. Name the map when it fails: a bare "Row for
        # `dcim.device` is missing required fields" does not say which of several
        # enabled maps produced it, and this failure stops one model rather than
        # the whole run.
        for row in rows:
            self._validate_row_shape(model_string, row, coalesce_fields, contract)
        for row in delete_rows:
            self._validate_row_shape(model_string, row, coalesce_fields, contract)
        if model_string == "dcim.virtualchassis":
            self._validate_virtual_chassis_positions(rows)

    def _validate_row_shape(self, model_string, row, coalesce_fields, contract) -> None:
        try:
            validate_row_shape_for_model(model_string, row, coalesce_fields)
        except ForwardQueryError as exc:
            if contract is None:
                raise
            query_name = getattr(contract, "query_name", "") or model_string
            map_id = getattr(contract, "map_id", None)
            revision = (
                "unpinned head"
                if getattr(contract, "full_unpinned_head", False)
                else "the pinned commit"
            )
            raise ForwardQueryError(
                f"{exc} Returned by map `{query_name}`"
                f"{f' [{map_id}]' if map_id is not None else ''} running at "
                f"{revision}. The query no longer returns the fields "
                f"{model_string} requires; re-resolve or republish that query."
            ) from exc

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

    def _run_nqe_provenance_query(
        self,
        *,
        contract,
        context,
        deadline=None,
    ):
        diff_revision = contract.diff_revision
        if diff_revision is None:
            raise ContributorBaselineUnavailable(
                "Tier 3 full provenance requires a verified diff revision."
            )
        return self.client.run_nqe_query(
            query_id=diff_revision.query_id,
            commit_id=diff_revision.commit_id,
            network_id=context.network_id,
            snapshot_id=context.snapshot_id,
            parameters={},
            fetch_all=True,
            deadline=deadline,
        )

    def _fetch_tier3_contributor_rows(
        self,
        *,
        model_string,
        contract,
        baseline,
        context,
        coalesce_fields,
        deadline,
    ):
        relation_contract = self._contributor_relation_contract(contract)
        after_scope = scope_side_from_context(context)
        fallback_reason = "cache_miss"
        work_relation = None
        if baseline is not None and not self._diff_circuit_is_open(contract):
            before_scope_fingerprint = self._baseline_scope_membership_fingerprint(
                baseline,
                model_string,
            )
            if before_scope_fingerprint:
                expectation = ContributorBaselineExpectation(
                    before_snapshot_id=str(baseline.snapshot_id or ""),
                    network_fingerprint=canonical_sha256(
                        {"network_id": str(context.network_id or "")}
                    ),
                    map_set_fingerprint=self._contributor_map_set_fingerprint,
                    scope_config_fingerprint=scope_config_fingerprint(context),
                    scope_membership_fingerprint=before_scope_fingerprint,
                    contract=relation_contract,
                )
                relation, fallback_reason = compatible_current_relation(
                    self.sync,
                    expectation,
                )
                if relation is not None:
                    try:
                        before_scope = scope_side_from_payload(
                            decode_scope_payload(relation.baseline)
                        )
                        work_relation = ContributorWorkRelation(relation)
                        before_rows = reduce_contributor_rows(
                            contract.reducer_id,
                            work_relation.iter_rows(),
                            before_scope,
                        )
                        diff_rows = self._run_nqe_diff(
                            spec=contract.spec,
                            contract=contract,
                            context=context,
                            before_snapshot_id=baseline.snapshot_id,
                            deadline=self._diff_deadline(deadline),
                        )
                        work_relation.apply_diff(
                            diff_rows,
                            target_key=lambda row: contributor_target_key(
                                contract.reducer_id,
                                row,
                            ),
                        )
                        after_rows = reduce_contributor_rows(
                            contract.reducer_id,
                            work_relation.iter_rows(),
                            after_scope,
                        )
                        upserts, deletes = diff_normalized_model_rows(
                            model_string,
                            before_rows,
                            after_rows,
                            coalesce_fields,
                        )
                        self._register_contributor_seed(
                            contract,
                            work_relation.iter_rows(),
                            work_relation=work_relation,
                        )
                        return (
                            upserts,
                            deletes,
                            "diff",
                            {
                                "contributor_before_rows": relation.row_count,
                                "contributor_diff_rows": len(diff_rows),
                                "reduced_before_rows": len(before_rows),
                                "reduced_after_rows": len(after_rows),
                            },
                        )
                    except JobTimeoutException:
                        if work_relation is not None:
                            work_relation.close()
                        raise
                    except (
                        ContributorBaselineUnavailable,
                        ForwardClientError,
                        ForwardConnectivityError,
                    ) as exc:
                        if work_relation is not None:
                            work_relation.close()
                            work_relation = None
                        if isinstance(exc, ForwardFetchBudgetExceededError):
                            circuit_open = self._record_diff_timeout(contract)
                            fallback_reason = DIFF_BUDGET_FALLBACK_REASON
                            if self._require_diff_execution():
                                raise ForwardQueryError(
                                    "Diff execution is required and the bounded "
                                    f"Tier 3 diff budget was exceeded for {model_string}."
                                ) from exc
                            self.logger.log_warning(
                                "Bounded Tier 3 contributor diff execution exceeded "
                                f"its budget for {model_string}; falling back to full "
                                "provenance execution."
                                + (
                                    " The per-contract diff circuit is now open "
                                    "for this run."
                                    if circuit_open
                                    else ""
                                ),
                                obj=self.sync,
                            )
                        else:
                            fallback_reason = exc.__class__.__name__
                            if self._require_diff_execution():
                                raise ForwardQueryError(
                                    "Diff execution is required and Tier 3 "
                                    f"reconstruction failed for {model_string}."
                                ) from exc
                            self.logger.log_warning(
                                "Tier 3 contributor reconstruction failed closed "
                                f"for {model_string}; running full provenance "
                                f"execution ({failure_classifier(exc)}).",
                                obj=self.sync,
                            )
        elif baseline is not None:
            fallback_reason = DIFF_CIRCUIT_OPEN_FALLBACK_REASON
            if self._require_diff_execution():
                raise ForwardQueryError(
                    "Diff execution is required, but the Tier 3 diff circuit is open "
                    f"for {model_string}."
                )

        provenance_rows = self._run_nqe_provenance_query(
            contract=contract,
            context=context,
            deadline=deadline,
        )
        reduced_rows = reduce_contributor_rows(
            contract.reducer_id,
            provenance_rows,
            after_scope,
        )
        self._register_contributor_seed(contract, provenance_rows)
        return (
            reduced_rows,
            [],
            "full",
            {
                "fallback_reason": fallback_reason,
                "contributor_after_rows": len(provenance_rows),
                "reduced_after_rows": len(reduced_rows),
                "authoritative_full_state": True,
            },
        )

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
        if isinstance(spec, ResolvedExecutionContract):
            contract = spec
            spec = contract.spec
        elif isinstance(spec, QuerySpec):
            contract = resolve_execution_contract(
                spec,
                effective_parameters=self._query_parameters_for_scope(
                    spec,
                    context,
                    shard_scope,
                ),
            )
        else:
            contract = None

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
        parameters = (
            dict(contract.full_effective_parameters)
            if contract is not None
            else self._query_parameters_for_scope(spec, context, shard_scope)
        )
        if metadata_shard_scope:
            if metadata_shard_scope.get("fetch_mode") != "model":
                self.logger.log_info(
                    f"Fetching {model_string} shard using {metadata_shard_scope['fetch_mode']} scope.",
                    obj=self.sync,
                )
        query_parameters = dict(parameters)
        ownership_mode = (
            contract.diff_ownership_mode if contract is not None else "global"
        )
        if (
            contract is not None
            and contract.diff_eligible
            and ownership_mode == "contributor_relation"
            and is_tier3_reducer(contract.reducer_id)
        ):
            try:
                rows, delete_rows, sync_mode, tier3_metadata = (
                    self._fetch_tier3_contributor_rows(
                        model_string=model_string,
                        contract=contract,
                        baseline=baseline,
                        context=context,
                        coalesce_fields=coalesce_fields,
                        deadline=deadline,
                    )
                )
            except JobTimeoutException:
                self._block_contributor_staging()
                raise
            except (
                ContributorBaselineUnavailable,
                ForwardClientError,
                ForwardConnectivityError,
                ForwardQueryError,
                DatabaseError,
            ):
                self._block_contributor_staging()
                raise
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
                sync_mode,
                {
                    "fetch_mode": (
                        "tier3_diff" if sync_mode == "diff" else "full_provenance"
                    ),
                    "fetch_key_family": fetch_key_family,
                    "fetch_parameters": {
                        **fetch_parameters,
                        **tier3_metadata,
                    },
                    "query_parameters": query_parameters,
                },
            )
        before_scoped_devices: set[str] = set()
        diff_block_reason = ""
        if (
            baseline is not None
            and contract is not None
            and contract.diff_eligible
            and ownership_mode != "global"
        ):
            (
                verified_before_devices,
                diff_block_reason,
            ) = self._verified_tier2_before_scope(
                model_string=model_string,
                baseline=baseline,
                context=context,
            )
            if verified_before_devices is not None:
                before_scoped_devices = verified_before_devices
        if (
            baseline is not None
            and contract is not None
            and contract.diff_eligible
            and not diff_block_reason
            and self._diff_circuit_is_open(contract)
        ):
            diff_block_reason = DIFF_CIRCUIT_OPEN_FALLBACK_REASON
        if (
            baseline is not None
            and spec.run_query_id
            and context.device_tag_prune_out_of_scope
            and context.scoped_device_names
            and ownership_mode == "global"
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
        elif (
            baseline is not None
            and contract is not None
            and contract.diff_eligible
            and not diff_block_reason
        ):
            try:
                diff_rows = self._run_nqe_diff(
                    spec=spec,
                    contract=contract,
                    context=context,
                    before_snapshot_id=baseline.snapshot_id,
                    deadline=self._diff_deadline(deadline),
                )
                diff_rows = self._reduce_tier2_diff_rows_to_scope(
                    model_string=model_string,
                    diff_rows=diff_rows,
                    ownership_mode=ownership_mode,
                    before_scoped_devices=before_scoped_devices,
                    context=context,
                )
                rows, delete_rows = runner._split_diff_rows(model_string, diff_rows)
                if ownership_mode == "global":
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
                    circuit_open = self._record_diff_timeout(contract)
                    if self._require_diff_execution():
                        raise ForwardQueryError(
                            "Diff execution is required and the bounded Forward NQE "
                            f"diff budget was exceeded for {model_string}."
                        ) from exc
                    if contract is not None and not contract.full_eligible:
                        raise ForwardQueryError(
                            "Diff execution for "
                            f"{model_string} failed, and full execution is not "
                            f"contractually safe (`{contract.full_reason_code}`)."
                        ) from exc
                    self.logger.log_warning(
                        "Bounded Forward NQE diff execution exceeded its budget for "
                        f"{model_string}; falling back to full query execution "
                        f"({failure_classifier(exc)})."
                        + (
                            " The per-contract diff circuit is now open for this run."
                            if circuit_open
                            else ""
                        ),
                        obj=self.sync,
                    )
                    fallback_parameters = dict(fetch_parameters)
                    fallback_parameters["fallback_reason"] = DIFF_BUDGET_FALLBACK_REASON
                    requested_mode = requested_fetch_mode if shard_scope else "diff"
                    requested_fetch_mode = (
                        "diff_fallback" if requested_mode != "model" else "model"
                    )
                    fetch_parameters = fallback_parameters
                else:
                    safe_exc = _safe_exception_summary(exc)
                    if self._require_diff_execution():
                        raise ForwardQueryError(
                            "Diff execution is required and Forward NQE diff failed for "
                            f"{model_string} using `{spec.execution_value}`: {safe_exc}"
                        ) from exc
                    if contract is not None and not contract.full_eligible:
                        raise ForwardQueryError(
                            "Diff execution for "
                            f"{model_string} failed, and full execution is not "
                            f"contractually safe (`{contract.full_reason_code}`)."
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
        elif baseline is not None:
            reason_code = diff_block_reason or (
                contract.reason_code if contract is not None else "unresolved_contract"
            )
            if self._require_diff_execution():
                if diff_block_reason:
                    raise ForwardQueryError(
                        "Diff execution is required, but safe diff execution for "
                        f"{model_string} is unavailable ({reason_code})."
                    )
                raise ForwardQueryError(
                    "Diff execution is required, but the resolved execution "
                    f"contract for {model_string} is full-only ({reason_code})."
                )
            if contract is not None and not contract.full_eligible:
                raise ForwardQueryError(
                    "Diff execution is unavailable and full execution is not "
                    f"contractually safe for {model_string}: "
                    f"{contract.full_reason_code}."
                )
            if diff_block_reason:
                warning = (
                    f"Safe diff execution for {model_string} is unavailable "
                    f"({reason_code}); running full query execution for "
                    f"`{spec.execution_value}`."
                )
            else:
                warning = (
                    f"Resolved execution contract for {model_string} is full-only "
                    f"({reason_code}); running full query execution for "
                    f"`{spec.execution_value}`."
                )
            self.logger.log_warning(warning, obj=self.sync)
            fallback_parameters = dict(fetch_parameters)
            fallback_parameters["fallback_reason"] = reason_code
            fetch_parameters = fallback_parameters
            requested_fetch_mode = "diff_fallback"
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
            if contract is not None and not contract.full_eligible:
                raise ForwardQueryError(
                    "Full execution is not allowed by the resolved contract "
                    f"for {model_string}: {contract.full_reason_code}."
                )
            rows = self._run_nqe_query(
                spec=spec,
                contract=contract,
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
        # Prune-out-of-scope is an operator decision about DEVICES leaving tag
        # scope, so it may remove devices, their sites, and the rows derived
        # from them - but not a shared catalogue or global IPAM, which device
        # scope cannot speak for. Without this the prune deleted whatever the
        # scope filter dropped, which is how a customer kept getting
        # `netbox_dlm.softwareversion` protected-delete skips after both other
        # delete producers were gated.
        delete_rows = []
        if context.device_tag_prune_out_of_scope and removed_rows:
            if prune_removals_allowed(model_string):
                delete_rows = removed_rows
            else:
                self.logger.log_warning(
                    f"Held back {len(removed_rows)} out-of-scope delete(s) for "
                    f"{model_string}: Prune orphans removes devices and the "
                    "rows derived from them, and this model is a shared "
                    "catalogue or global IPAM that device scope does not speak "
                    "for. The rows stay in NetBox.",
                    obj=self.sync,
                )
        delete_rows = delete_rows + self._full_run_removals(
            model_string=model_string,
            current_rows=filtered_rows,
            coalesce_fields=coalesce_fields,
            already_removed=delete_rows,
            shard_scope=original_shard_scope,
        )
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

    @staticmethod
    def _tag_scope_enabled(context: ForwardQueryContext) -> bool:
        return bool(context.device_tag_include_tags or context.device_tag_exclude_tags)

    def _scope_context_for_snapshot(
        self,
        context: ForwardQueryContext,
        *,
        snapshot_id: str,
    ) -> ForwardQueryContext:
        cache_key = (
            context.network_id,
            snapshot_id,
            tuple(context.device_tag_include_tags),
            tuple(context.device_tag_exclude_tags),
            context.device_tag_include_match,
            context.sync_endpoints,
            context.sync_generic_endpoints,
            context.scope_endpoints_by_include_tags,
        )
        cached = self._snapshot_scope_context_cache.get(cache_key)
        if cached is not None:
            return cached

        if self._tag_scope_enabled(context):
            (
                scoped_device_names,
                scoped_site_names,
                scoped_matched_tags,
                endpoint_scope_failed,
            ) = self._resolve_scoped_tag_scope(
                network_id=context.network_id,
                snapshot_id=snapshot_id,
                include_tags=context.device_tag_include_tags,
                exclude_tags=context.device_tag_exclude_tags,
                include_match=context.device_tag_include_match,
                sync_endpoints=context.sync_endpoints,
                sync_generic_endpoints=context.sync_generic_endpoints,
                scope_endpoints_by_include_tags=(
                    context.scope_endpoints_by_include_tags
                ),
            )
            if endpoint_scope_failed:
                raise ForwardQueryError(
                    "Forward before-snapshot ownership scope could not be "
                    "verified for diff execution."
                )
        else:
            scoped_device_names = set()
            scoped_site_names = set()
            scoped_matched_tags = {}

        resolved = replace(
            context,
            snapshot_id=str(snapshot_id or ""),
            scoped_device_names=set(scoped_device_names),
            scoped_site_names=set(scoped_site_names),
            scoped_matched_tags=dict(scoped_matched_tags),
        )
        self._snapshot_scope_context_cache[cache_key] = resolved
        return resolved

    @staticmethod
    def _baseline_scope_membership_fingerprint(
        baseline,
        model_string: str,
    ) -> str:
        fingerprints = {
            str(row.get("scope_membership_fingerprint") or "")
            for row in list(getattr(baseline, "model_results", None) or [])
            if isinstance(row, dict)
            and str(row.get("model") or row.get("model_string") or "") == model_string
        }
        if len(fingerprints) != 1 or "" in fingerprints:
            return ""
        return next(iter(fingerprints))

    def _verified_tier2_before_scope(
        self,
        *,
        model_string: str,
        baseline,
        context: ForwardQueryContext,
    ) -> tuple[set[str] | None, str]:
        try:
            before_context = self._scope_context_for_snapshot(
                context,
                snapshot_id=baseline.snapshot_id,
            )
        except (ForwardClientError, ForwardConnectivityError, ForwardQueryError):
            return None, "unverified_before_scope"

        expected_fingerprint = self._baseline_scope_membership_fingerprint(
            baseline,
            model_string,
        )
        if (
            not expected_fingerprint
            or scope_membership_fingerprint(before_context) != expected_fingerprint
        ):
            return None, "before_scope_fingerprint_mismatch"

        before_devices = set(before_context.scoped_device_names)
        if self._tag_scope_enabled(context) and before_devices != set(
            context.scoped_device_names
        ):
            # Tier 2 rows do not carry tag membership. An unchanged model row
            # whose device entered or left tag scope is absent from the Forward
            # change-only response, so a diff cannot reconstruct the scoped
            # result when membership changed externally.
            return None, "scope_membership_changed"
        return before_devices, ""

    @staticmethod
    def _tier2_side_is_owned(
        *,
        model_string: str,
        row: dict[str, Any],
        ownership_mode: str,
        scoped_devices: set[str],
        scope_enabled: bool,
    ) -> bool:
        if not scope_enabled:
            return True
        row_devices = _row_device_names(model_string, row)
        if ownership_mode == "device":
            if len(row_devices) != 1:
                raise ForwardQueryError(
                    f"Forward Tier 2 ownership row for {model_string} did not "
                    "identify exactly one device."
                )
            return bool(row_devices.intersection(scoped_devices))
        if ownership_mode == "cable_either_endpoint":
            if not row_devices:
                raise ForwardQueryError(
                    "Forward Tier 2 cable ownership row did not identify an "
                    "endpoint device."
                )
            return bool(row_devices.intersection(scoped_devices))
        raise ForwardQueryError(
            f"Unsupported Forward diff ownership mode `{ownership_mode}` for "
            f"{model_string}."
        )

    def _reduce_tier2_diff_rows_to_scope(
        self,
        *,
        model_string: str,
        diff_rows: list[dict[str, Any]],
        ownership_mode: str,
        before_scoped_devices: set[str],
        context: ForwardQueryContext,
    ) -> list[dict[str, Any]]:
        """Apply asymmetric side-local ownership before diff rows are split."""

        if ownership_mode == "global":
            return list(diff_rows)
        scope_enabled = self._tag_scope_enabled(context)
        after_scoped_devices = set(context.scoped_device_names)
        reduced = []
        for diff_row in diff_rows:
            change_type = diff_row.get("type")
            before = diff_row.get("before")
            after = diff_row.get("after")
            before_owned = False
            after_owned = False

            if change_type in {"DELETED", "MODIFIED"}:
                if not isinstance(before, dict):
                    raise ForwardQueryError(
                        f"Forward diff row for {model_string} was missing "
                        f"`before` data for {change_type}."
                    )
                before_owned = self._tier2_side_is_owned(
                    model_string=model_string,
                    row=before,
                    ownership_mode=ownership_mode,
                    scoped_devices=before_scoped_devices,
                    scope_enabled=scope_enabled,
                )
            if change_type in {"ADDED", "MODIFIED"}:
                if not isinstance(after, dict):
                    raise ForwardQueryError(
                        f"Forward diff row for {model_string} was missing "
                        f"`after` data for {change_type}."
                    )
                after_owned = self._tier2_side_is_owned(
                    model_string=model_string,
                    row=after,
                    ownership_mode=ownership_mode,
                    scoped_devices=after_scoped_devices,
                    scope_enabled=scope_enabled,
                )

            if change_type == "ADDED":
                if after_owned:
                    reduced.append(diff_row)
                continue
            if change_type == "DELETED":
                if before_owned:
                    reduced.append(diff_row)
                continue
            if change_type != "MODIFIED":
                raise ForwardQueryError(
                    f"Forward diff row for {model_string} had unsupported type "
                    f"`{change_type}`."
                )
            if before_owned and after_owned:
                reduced.append(diff_row)
            elif before_owned:
                reduced.append(
                    {
                        "type": "DELETED",
                        "before": before,
                        "after": None,
                    }
                )
            elif after_owned:
                reduced.append(
                    {
                        "type": "ADDED",
                        "before": None,
                        "after": after,
                    }
                )
        return reduced

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
            if model_string == "dcim.cable" and row_devices.intersection(
                scoped_devices
            ):
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

    def _full_run_removals(
        self,
        *,
        model_string,
        current_rows,
        coalesce_fields,
        already_removed,
        shard_scope,
    ):
        """Baseline rows this full result no longer contains.

        A full execution used to compute no removals at all, so any row written
        by a map that was later re-pointed at a different query stayed forever.
        The promoted contributor baseline is the record of what was written, and
        comparing against it needs no extra Forward call.

        Refusals here are per-model and never fail the run: not removing is
        always a safe outcome, and taking the whole sync down over an advisory
        comparison would trade a cosmetic problem for an outage.
        """
        if shard_scope:
            # A shard holds part of the model by construction, so everything
            # outside it is "absent" and would be removed. Only a whole-model
            # fetch can speak for the whole model.
            return []
        try:
            previous_rows = previous_full_rows(self.sync, model_string)
            removals = compute_full_removals(
                model_string,
                current_rows=current_rows,
                previous_rows=previous_rows,
                coalesce_fields=coalesce_fields,
            )
            # A network-complete result speaks for the whole table, so it also
            # reaches rows orphaned before the current baseline was written -
            # which the comparison above cannot see, because no baseline it can
            # read ever mentioned them.
            complete_removals, refusal = network_complete_removals(
                model_string,
                current_rows=current_rows,
            )
            if refusal:
                self.logger.log_warning(
                    f"Removal reconciliation for {model_string} did not run "
                    f"against the full result: {refusal}.",
                    obj=self.sync,
                )
            removals = removals + complete_removals
        except RemovalReconciliationRefused as exc:
            self.logger.log_warning(str(exc), obj=self.sync)
            return []
        except JobTimeoutException:
            raise
        except Exception as exc:
            self.logger.log_warning(
                f"Removal reconciliation for {model_string} could not run "
                f"({exception_type(exc)}); nothing was removed for this model.",
                obj=self.sync,
            )
            return []
        if not removals:
            return []
        # The device-tag scope pass may already have named some of these.
        seen = set()
        for row in already_removed:
            key = coalesce_identity(model_string, row, coalesce_fields)
            if key is not None:
                seen.add(key)
        deduped = []
        for row in removals:
            key = coalesce_identity(model_string, row, coalesce_fields)
            if key is None or key in seen:
                continue
            seen.add(key)
            deduped.append(row)
        if deduped:
            self.logger.log_info(
                f"Removal reconciliation staged {len(deduped)} delete(s) for "
                f"{model_string}: rows the promoted baseline recorded that this "
                "full result no longer returns.",
                obj=self.sync,
            )
        return deduped

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
        contract: ResolvedExecutionContract | None = None,
        context: ForwardQueryContext,
        parameters: dict[str, Any],
        limit: int | None = None,
        fetch_all: bool = False,
        deadline=None,
    ):
        if contract is not None and not contract.full_eligible:
            raise ForwardQueryError(
                "Full execution is not allowed by the resolved contract for "
                f"{getattr(spec, 'model_string', 'unknown model')}: "
                f"{contract.full_reason_code}."
            )
        full_revision = contract.full_revision if contract is not None else None
        call_kwargs = {
            "query": spec.query,
            "query_id": (
                full_revision.query_id or None
                if full_revision is not None
                else spec.run_query_id
            ),
            "commit_id": (
                full_revision.commit_id or None
                if full_revision is not None
                else spec.commit_id
            ),
            "network_id": context.network_id,
            "snapshot_id": context.snapshot_id,
            "parameters": parameters,
            "limit": limit,
            "fetch_all": fetch_all,
            "deadline": deadline,
        }
        try:
            return self.client.run_nqe_query(**call_kwargs)
        except JobTimeoutException:
            raise
        except ForwardClientError as exc:
            is_legacy_resolved_path = bool(spec.query_path and spec.run_query_id)
            is_parameter_error = "parameter" in str(exc).casefold()
            if not (
                is_legacy_resolved_path
                and is_parameter_error
                and only_legacy_safe_default_parameters(parameters)
            ):
                raise
            self.logger.log_info(
                "Retrying read-only legacy path query for "
                f"{spec.model_string} without unsupported default parameters.",
                obj=self.sync,
            )
            return self.client.run_nqe_query(
                **{
                    **call_kwargs,
                    "parameters": {},
                }
            )

    def _run_nqe_diff(
        self,
        *,
        spec,
        contract: ResolvedExecutionContract | None = None,
        context: ForwardQueryContext,
        before_snapshot_id: str,
        deadline=None,
    ):
        if contract is None or not contract.diff_eligible:
            reason_code = (
                contract.reason_code
                if contract is not None
                else "unresolved_diff_contract"
            )
            raise ForwardQueryError(
                "Diff execution is not allowed by the resolved contract for "
                f"{getattr(spec, 'model_string', 'unknown model')}: "
                f"{reason_code}."
            )
        diff_revision = contract.diff_revision
        if diff_revision is None:
            raise ForwardQueryError(
                "Diff execution is not allowed by the resolved contract for "
                f"{getattr(spec, 'model_string', 'unknown model')}: "
                "unresolved_diff_revision."
            )
        return self.client.run_nqe_diff(
            query_id=diff_revision.query_id or None,
            commit_id=diff_revision.commit_id or None,
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
