import re
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
from django.utils.text import slugify

from ..choices import FORWARD_OPTIONAL_MODELS
from ..choices import ForwardApplyEngineChoices
from ..choices import ForwardDiffFallbackModeChoices
from ..choices import ForwardExecutionBackendChoices
from ..choices import ForwardExecutionStepKindChoices
from ..choices import ForwardExecutionStepStatusChoices
from ..exceptions import ForwardClientError
from ..exceptions import ForwardConnectivityError
from ..exceptions import ForwardQueryError
from .apply_engine import apply_engine_decision_for
from .branch_budget import BranchPlanItem
from .branch_budget import BranchWorkload
from .branch_budget import row_shard_key
from .branch_budget import shard_fetch_contract
from .execution_ledger import active_execution_run
from .fetch_artifacts import fetch_artifact_key
from .fetch_artifacts import load_fetch_artifact
from .fetch_artifacts import load_runtime_artifact
from .fetch_artifacts import sanitize_fetch_artifact_metadata
from .fetch_artifacts import save_fetch_artifact
from .fetch_artifacts import save_runtime_artifact
from .forward_api import DEFAULT_QUERY_FETCH_CONCURRENCY
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
from .query_registry import get_query_specs
from .query_registry import optional_builtin_query_names_for_model
from .query_registry import resolve_query_specs_for_client
from .sync import ForwardSyncRunner
from .sync_contracts import validate_row_shape_for_model

DEFAULT_PREFLIGHT_ROW_LIMIT = 5
MAX_PREFLIGHT_ROW_LIMIT = 100
SHARD_FETCH_COLUMN_FILTER_CHUNK_SIZE = 250
DEFAULT_SHARD_FETCH_PREFETCH_STEPS = 6
MAX_SHARD_FETCH_PREFETCH_STEPS = 50


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
    scoped_device_names: set[str] = field(default_factory=set)
    scoped_site_names: set[str] = field(default_factory=set)

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
            "scoped_device_count": len(self.scoped_device_names),
            "scoped_site_count": len(self.scoped_site_names),
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


def _partition_column_filters(column_filters):
    if not column_filters:
        return [None]
    if len(column_filters) != 1:
        return [column_filters]
    filter_item = dict(column_filters[0] or {})
    if filter_item.get("operator") != "EQUALS_ANY":
        return [column_filters]
    values = list(filter_item.get("values") or [])
    if len(values) <= SHARD_FETCH_COLUMN_FILTER_CHUNK_SIZE:
        return [column_filters]

    partitions = []
    for start in range(0, len(values), SHARD_FETCH_COLUMN_FILTER_CHUNK_SIZE):
        end = start + SHARD_FETCH_COLUMN_FILTER_CHUNK_SIZE
        partition_item = dict(filter_item)
        partition_item["values"] = values[start:end]
        partitions.append([partition_item])
    return partitions or [column_filters]


def _split_column_filter_partition(partition):
    if not partition or len(partition) != 1:
        return []
    filter_item = dict(partition[0] or {})
    if filter_item.get("operator") != "EQUALS_ANY":
        return []
    values = list(filter_item.get("values") or [])
    if len(values) <= 1:
        return []
    midpoint = len(values) // 2
    split_partitions = []
    for subset in (values[:midpoint], values[midpoint:]):
        if not subset:
            continue
        split_filter = dict(filter_item)
        split_filter["values"] = subset
        split_partitions.append([split_filter])
    return split_partitions


def _alternate_single_value_column_filter_partition(partition):
    if not partition or len(partition) != 1:
        return []
    filter_item = dict(partition[0] or {})
    operator = filter_item.get("operator")
    if operator == "DEFAULT":
        value = filter_item.get("value")
        if value in ("", None):
            return []
        alternate_filter = dict(filter_item)
        alternate_filter.pop("value", None)
        alternate_filter["operator"] = "EQUALS_ANY"
        alternate_filter["values"] = [value]
        return [alternate_filter]
    if operator == "EQUALS_ANY":
        values = list(filter_item.get("values") or [])
        if len(values) != 1:
            return []
        alternate_filter = dict(filter_item)
        alternate_filter.pop("values", None)
        alternate_filter["operator"] = "DEFAULT"
        alternate_filter["value"] = values[0]
        return [alternate_filter]
    return []


def _default_column_filter_partitions_for_equals_any(partition):
    if not partition or len(partition) != 1:
        return []
    filter_item = dict(partition[0] or {})
    if filter_item.get("operator") != "EQUALS_ANY":
        return []
    values = [
        value
        for value in list(filter_item.get("values") or [])
        if value not in ("", None)
    ]
    if len(values) <= 1:
        return []
    partitions = []
    for value in values:
        default_filter = dict(filter_item)
        default_filter.pop("values", None)
        default_filter["operator"] = "DEFAULT"
        default_filter["value"] = value
        partitions.append([default_filter])
    return partitions


def _partition_error_allows_split_retry(exc: Exception) -> bool:
    if isinstance(exc, ForwardConnectivityError):
        return True
    message = str(exc or "").lower()
    if "'value' is required" in message or '"value" is required' in message:
        return True
    if "timeout" in message or "timed out" in message:
        return True
    if re.search(r"\bhttp\s+4\d\d\b", message):
        return False
    if "bad request" in message:
        return False
    return True


def _is_value_required_filter_error(exc: Exception) -> bool:
    message = str(exc or "").lower()
    return "'value' is required" in message or '"value" is required' in message


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
    fetch_column_filters: list[dict[str, Any]] = field(default_factory=list)

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
            "fetch_column_filters": self.fetch_column_filters,
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
        self._parameter_fallback_log_keys: set[tuple[str, str, str]] = set()

    def resolve_context(self, *, branch_run_state=None) -> ForwardQueryContext:
        branch_run_state = branch_run_state or {}
        network_id = self.sync.get_network_id()
        snapshot_selector = (
            branch_run_state.get("snapshot_selector") or self.sync.get_snapshot_id()
        )
        snapshot_id = branch_run_state.get("snapshot_id")
        if not snapshot_id:
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
        if not include_tags and source_parameters.get("device_tag_include"):
            include_tags = [source_parameters.get("device_tag_include")]
        if not exclude_tags and source_parameters.get("device_tag_exclude"):
            exclude_tags = [source_parameters.get("device_tag_exclude")]
        include_tags = [str(tag).strip() for tag in include_tags if str(tag).strip()]
        exclude_tags = [str(tag).strip() for tag in exclude_tags if str(tag).strip()]
        include_match = str(
            source_parameters.get("device_tag_include_match") or "any"
        ).strip()
        if include_match not in {"any", "all"}:
            include_match = "any"
        context_artifact = self._context_artifact_descriptor(
            network_id=network_id,
            snapshot_selector=snapshot_selector,
            snapshot_id=snapshot_id,
            include_tags=include_tags,
            exclude_tags=exclude_tags,
            include_match=include_match,
        )
        cached_context = self._load_context_artifact(context_artifact)
        if cached_context is None:
            snapshot_info = self._resolve_snapshot_info(
                network_id=network_id,
                snapshot_selector=snapshot_selector,
                snapshot_id=snapshot_id,
                branch_run_state=branch_run_state,
            )
            snapshot_metrics = {}
            try:
                snapshot_metrics = self.client.get_snapshot_metrics(snapshot_id)
            except Exception as exc:
                self.logger.log_warning(
                    "Unable to fetch Forward snapshot metrics for the selected snapshot: "
                    f"{_safe_exception_summary(exc)}",
                    obj=self.sync,
                )
            scoped_device_names, scoped_site_names = self._resolve_scoped_tag_scope(
                network_id=network_id,
                snapshot_id=snapshot_id,
                include_tags=include_tags,
                exclude_tags=exclude_tags,
                include_match=include_match,
            )
            self._save_context_artifact(
                context_artifact,
                snapshot_info=snapshot_info,
                snapshot_metrics=snapshot_metrics,
                scoped_device_names=scoped_device_names,
                scoped_site_names=scoped_site_names,
            )
        else:
            snapshot_info = dict(cached_context.get("snapshot_info") or {})
            snapshot_metrics = dict(cached_context.get("snapshot_metrics") or {})
            scoped_device_names = set(cached_context.get("scoped_device_names") or [])
            scoped_site_names = set(cached_context.get("scoped_site_names") or [])
        prune_out_of_scope = bool(
            source_parameters.get("device_tag_prune_out_of_scope")
        )

        return ForwardQueryContext(
            network_id=network_id,
            snapshot_selector=snapshot_selector,
            snapshot_id=snapshot_id,
            ingestion_id=self._resolve_context_ingestion_id(branch_run_state),
            snapshot_info=snapshot_info or {},
            snapshot_metrics=snapshot_metrics or {},
            query_parameters=self.sync.get_query_parameters(),
            maps=self.sync.get_maps(),
            device_tag_include_tags=include_tags,
            device_tag_exclude_tags=exclude_tags,
            device_tag_include_match=include_match,
            device_tag_prune_out_of_scope=prune_out_of_scope,
            scoped_device_names=scoped_device_names,
            scoped_site_names=scoped_site_names,
        )

    def _context_artifact_descriptor(
        self,
        *,
        network_id: str,
        snapshot_selector: str,
        snapshot_id: str,
        include_tags: list[str],
        exclude_tags: list[str],
        include_match: str,
    ) -> dict[str, Any] | None:
        run = active_execution_run(self.sync)
        if run is None:
            return None
        artifact_run_id = f"shared-sync-{getattr(self.sync, 'pk', 'unknown')}"
        payload = {
            "version": 2,
            "artifact_scope": "query_context",
            "cache_scope": "shared_sync",
            "sync_id": getattr(self.sync, "pk", None),
            "network_hash": fetch_artifact_key({"network_id": network_id}),
            "snapshot_selector": snapshot_selector or "",
            "snapshot_id": snapshot_id or "",
            "device_tag_include": sorted(include_tags or []),
            "device_tag_exclude": sorted(exclude_tags or []),
            "device_tag_include_match": include_match or "any",
        }
        return {
            "key": fetch_artifact_key(payload),
            "run_id": artifact_run_id,
        }

    def _load_context_artifact(self, descriptor: dict[str, Any] | None):
        if descriptor is None:
            return None
        payload, _meta = load_runtime_artifact(
            descriptor["key"],
            run_id=descriptor["run_id"],
        )
        if not isinstance(payload, dict):
            return None
        return payload

    def _save_context_artifact(
        self,
        descriptor: dict[str, Any] | None,
        *,
        snapshot_info: dict[str, Any],
        snapshot_metrics: dict[str, Any],
        scoped_device_names: set[str],
        scoped_site_names: set[str],
    ) -> None:
        if descriptor is None:
            return
        save_runtime_artifact(
            descriptor["key"],
            run_id=descriptor["run_id"],
            payload={
                "snapshot_info": dict(snapshot_info or {}),
                "snapshot_metrics": dict(snapshot_metrics or {}),
                "scoped_device_names": sorted(scoped_device_names or set()),
                "scoped_site_names": sorted(scoped_site_names or set()),
            },
        )

    def _resolve_context_ingestion_id(self, branch_run_state):
        run = active_execution_run(self.sync)
        if run is not None:
            target_index = int(run.next_step_index or 1)
            stage_steps = run.steps.filter(kind="stage")
            candidate = stage_steps.filter(index=target_index).order_by("pk").first()
            if candidate is None:
                candidate = (
                    stage_steps.filter(status__in=["running", "queued", "staged"])
                    .order_by("index", "pk")
                    .first()
                )
            if candidate is not None and candidate.ingestion_id:
                return int(candidate.ingestion_id)
        return (
            branch_run_state.get("ingestion_id")
            or branch_run_state.get("pending_ingestion_id")
            or branch_run_state.get("current_ingestion_id")
        )

    def _resolve_scoped_tag_scope(
        self,
        *,
        network_id: str,
        snapshot_id: str,
        include_tags: list[str],
        exclude_tags: list[str],
        include_match: str,
    ) -> tuple[set[str], set[str]]:
        if not include_tags and not exclude_tags:
            return set(), set()
        where = [
            "where device.snapshotInfo.result == DeviceSnapshotResult.completed",
            "where device.platform.vendor != Vendor.FORWARD_CUSTOM",
        ]
        include_exprs = [
            f'"{tag.replace("\"", "\\\"")}" in device.tagNames' for tag in include_tags
        ]
        if include_exprs:
            if include_match == "all":
                where.extend([f"where {expr}" for expr in include_exprs])
            else:
                where.append(f"where ({' || '.join(include_exprs)})")
        for tag in exclude_tags:
            exclude_literal = tag.replace('"', '\\"')
            where.append(f'where !("{exclude_literal}" in device.tagNames)')
        query = "\n".join(
            [
                "foreach device in network.devices",
                *where,
                "select {",
                "  name: device.name,",
                '  site: if isPresent(device.locationName) then toLowerCase(device.locationName) else "unknown"',
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
        for row in rows:
            site = str(row.get("site") or "").strip().lower()
            if not site:
                continue
            sites.add(site)
            site_slug = slugify(site)
            if site_slug:
                sites.add(site_slug)
        self.logger.log_info(
            f"Resolved device tag scope with {len(names)} matched devices "
            f"(include={include_tags or ['-']}, include_match={include_match}, exclude={exclude_tags or ['-']}).",
            obj=self.sync,
        )
        return names, sites

    def run_preflight(
        self,
        context: ForwardQueryContext,
        *,
        row_limit=None,
        model_strings=None,
    ) -> None:
        if row_limit is None:
            row_limit = self._preflight_row_limit()
        self.logger.log_info(
            "Running Forward query preflight before building the sync workload.",
            obj=self.sync,
        )
        jobs = self._query_jobs(context, model_strings=model_strings)
        if not jobs:
            return
        max_workers = self._query_fetch_worker_count(len(jobs))
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            for model_string, spec, preflight_rows, error in executor.map(
                self._run_thread_job,
                ((self._run_preflight_job, (context, row_limit, job)) for job in jobs),
            ):
                if error is not None:
                    self._record_model_failure(
                        context,
                        model_string,
                        spec,
                        error,
                        sync_mode="preflight",
                    )
                    continue
                self.logger.log_info(
                    f"Preflight validated {len(preflight_rows)} rows for {model_string} from {spec.execution_mode} `{spec.execution_value}`.",
                    obj=self.sync,
                )

    def _run_preflight_job(self, payload):
        context, row_limit, job = payload
        model_string, spec, coalesce_fields = job
        try:
            preflight_rows = self._run_nqe_query_with_parameter_fallback(
                spec=spec,
                context=context,
                parameters=spec.merged_parameters(context.query_parameters),
                limit=row_limit,
                fetch_all=False,
            )
            preflight_rows, _ = self._apply_device_tag_scope(
                model_string, preflight_rows, context
            )
            for row in preflight_rows:
                validate_row_shape_for_model(model_string, row, coalesce_fields)
            return model_string, spec, preflight_rows, None
        except (ForwardClientError, ForwardConnectivityError, ForwardQueryError) as exc:
            return model_string, spec, [], exc

    def _query_jobs(self, context: ForwardQueryContext, *, model_strings=None):
        jobs = []
        enabled_models = list(model_strings or self.sync.get_model_strings())
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
                    sync_mode="preflight",
                )
                continue
            try:
                specs = resolved_specs.get(model_string, [])
                if not specs:
                    raise ForwardQueryError(
                        self._missing_query_specs_message(model_string)
                    )
            except ForwardQueryError as exc:
                self._record_model_failure(
                    context,
                    model_string,
                    None,
                    exc,
                    sync_mode="preflight",
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
        include_diagnostics=True,
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
        if include_diagnostics and self._query_diagnostics_enabled():
            self._append_ipaddress_diagnostics(context)
            self._append_ipaddress_parent_prefix_diagnostics(workloads)
            self._append_routing_diagnostics(context)
        return workloads

    def _query_diagnostics_enabled(self) -> bool:
        source = getattr(self.sync, "source", None)
        parameters = dict(getattr(source, "parameters", {}) or {})
        configured = parameters.get("query_diagnostics_enabled")
        if configured is None:
            return True
        if isinstance(configured, str):
            return configured.strip().lower() in {"1", "true", "yes", "on"}
        return bool(configured)

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
            return ForwardDiffFallbackModeChoices.ALLOW_FALLBACK
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
        enabled_models = list(model_strings or self.sync.get_model_strings())
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
            baseline = self._incremental_baseline_for_specs(context, specs)
            if baseline is not None:
                self.logger.log_info(
                    f"Selected Forward diff baseline ingestion `{baseline.pk}` "
                    f"on snapshot `{baseline.snapshot_id}` for {model_string}.",
                    obj=self.sync,
                )
            for spec in specs:
                jobs.append(
                    (
                        model_string,
                        spec,
                        baseline,
                        coalesce_fields,
                        self._scope_for_spec(model_string, spec, shard_scope),
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
        )
        self._incremental_baseline_cache[cache_key] = baseline
        return baseline

    def _resolve_query_specs(self, model_string: str, specs):
        run = active_execution_run(self.sync)
        if run is None:
            return resolve_query_specs_for_client(specs, self.client)
        resolved_specs = []
        for spec in specs:
            if not getattr(spec, "query_path", None):
                resolved_specs.append(spec)
                continue
            descriptor = self._query_spec_artifact_descriptor(
                model_string=model_string,
                spec=spec,
            )
            cached = self._load_query_spec_artifact(descriptor)
            if cached is not None:
                resolved_query_id = str(cached.get("resolved_query_id") or "").strip()
                resolved_commit_id = str(cached.get("commit_id") or "").strip()
                if resolved_query_id:
                    resolved_specs.append(
                        replace(
                            spec,
                            resolved_query_id=resolved_query_id,
                            commit_id=resolved_commit_id or spec.commit_id,
                        )
                    )
                    continue
            resolved_spec = spec.resolve(self.client)
            self._save_query_spec_artifact(descriptor, resolved_spec)
            resolved_specs.append(resolved_spec)
        return resolved_specs

    def _query_spec_artifact_descriptor(self, *, model_string: str, spec):
        source = getattr(self.sync, "source", None)
        source_parameters = dict(getattr(source, "parameters", None) or {})
        source_scope_hash = fetch_artifact_key(
            {
                "source_url": getattr(source, "url", "") or "",
                "source_username": source_parameters.get("username") or "",
                "source_type": getattr(source, "type", "") or "",
            }
        )
        artifact_run_id = f"shared-sync-{getattr(self.sync, 'pk', 'unknown')}"
        payload = {
            "version": 1,
            "artifact_scope": "query_spec",
            "cache_scope": "shared_sync",
            "sync_id": getattr(self.sync, "pk", None),
            "source_scope_hash": source_scope_hash,
            "model": model_string,
            "query_name": getattr(spec, "query_name", ""),
            "query_repository": getattr(spec, "query_repository", "") or "",
            "query_path": getattr(spec, "query_path", "") or "",
            "requested_commit_id": getattr(spec, "commit_id", "") or "",
        }
        return {
            "key": fetch_artifact_key(payload),
            "run_id": artifact_run_id,
        }

    def _load_query_spec_artifact(self, descriptor):
        payload, _meta = load_runtime_artifact(
            descriptor["key"],
            run_id=descriptor["run_id"],
        )
        if not isinstance(payload, dict):
            return None
        return payload

    def _save_query_spec_artifact(self, descriptor, spec):
        save_runtime_artifact(
            descriptor["key"],
            run_id=descriptor["run_id"],
            payload={
                "resolved_query_id": getattr(spec, "resolved_query_id", None),
                "commit_id": getattr(spec, "commit_id", None),
            },
        )

    def _diagnostic_artifact_descriptor(
        self,
        *,
        diagnostic_name: str,
        context: ForwardQueryContext,
    ):
        run = active_execution_run(self.sync)
        if run is None:
            return None
        payload = {
            "version": 1,
            "artifact_scope": "diagnostic_result",
            "sync_id": getattr(self.sync, "pk", None),
            "run_id": run.pk,
            "diagnostic_name": diagnostic_name,
            "network_id": context.network_id,
            "snapshot_id": context.snapshot_id,
            "query_parameters": dict(context.query_parameters or {}),
            "device_tag_include_tags": sorted(context.device_tag_include_tags or []),
            "device_tag_exclude_tags": sorted(context.device_tag_exclude_tags or []),
            "device_tag_include_match": str(context.device_tag_include_match or ""),
            "device_tag_prune_out_of_scope": bool(
                context.device_tag_prune_out_of_scope
            ),
            "scoped_device_count": len(context.scoped_device_names or set()),
        }
        return {
            "key": fetch_artifact_key(payload),
            "run_id": run.pk,
        }

    def _load_cached_diagnostic_result(
        self,
        *,
        diagnostic_name: str,
        context: ForwardQueryContext,
    ) -> tuple[bool, dict[str, Any] | None]:
        descriptor = self._diagnostic_artifact_descriptor(
            diagnostic_name=diagnostic_name,
            context=context,
        )
        if descriptor is None:
            return False, None
        payload, _meta = load_runtime_artifact(
            descriptor["key"],
            run_id=descriptor["run_id"],
        )
        if not isinstance(payload, dict):
            return False, None
        has_diagnostic = bool(payload.get("has_diagnostic", False))
        if not has_diagnostic:
            return True, None
        diagnostic = payload.get("diagnostic")
        if not isinstance(diagnostic, dict):
            return False, None
        return True, diagnostic

    def _store_cached_diagnostic_result(
        self,
        *,
        diagnostic_name: str,
        context: ForwardQueryContext,
        diagnostic: dict[str, Any] | None,
    ) -> None:
        descriptor = self._diagnostic_artifact_descriptor(
            diagnostic_name=diagnostic_name,
            context=context,
        )
        if descriptor is None:
            return
        save_runtime_artifact(
            descriptor["key"],
            run_id=descriptor["run_id"],
            payload={
                "has_diagnostic": bool(isinstance(diagnostic, dict)),
                "diagnostic": dict(diagnostic or {}),
            },
        )

    def _run_workload_job(self, payload):
        context, validate_rows, job = payload
        model_string, spec, baseline, coalesce_fields, shard_scope = job
        baseline_snapshot_id = getattr(baseline, "snapshot_id", "") or ""
        started = time.perf_counter()
        try:
            rows, delete_rows, sync_mode, fetch_meta = self._fetch_spec_rows(
                model_string,
                spec,
                baseline,
                context,
                coalesce_fields,
                shard_scope=shard_scope,
                return_fetch_meta=True,
            )
            runtime_ms = round((time.perf_counter() - started) * 1000, 1)
            if validate_rows:
                self.validate_rows(
                    model_string,
                    rows,
                    delete_rows,
                    coalesce_fields,
                )
        except (ForwardClientError, ForwardConnectivityError, ForwardQueryError) as exc:
            runtime_ms = round((time.perf_counter() - started) * 1000, 1)
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
        apply_engine_decision = apply_engine_decision_for(
            sync=self.sync,
            model_string=model_string,
            backend=None,
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
            fetch_column_filters=list(fetch_meta.get("fetch_column_filters") or []),
        )
        workload = None
        if rows or delete_rows:
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
                fetch_column_filters=list(fetch_meta.get("fetch_column_filters") or []),
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
            diagnostics=[
                {
                    "name": "query_validation_failure",
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
                "refresh or republish that query before retrying."
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
        row_limit=DEFAULT_PREFLIGHT_ROW_LIMIT,
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
        rows = self._run_nqe_query_with_parameter_fallback(
            spec=spec,
            context=context,
            parameters=spec.merged_parameters(context.query_parameters),
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
        )

    def _apply_engine_result_fields(self, model_string: str) -> dict[str, Any]:
        decision = apply_engine_decision_for(
            sync=self.sync,
            model_string=model_string,
            backend=None,
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
        branch_run_state: dict[str, Any],
    ) -> dict[str, Any]:
        if snapshot_selector == snapshot_id or branch_run_state:
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
    ):
        fetch_artifact_descriptor = None
        model_fetch_artifact_descriptor = None
        original_shard_scope = dict(shard_scope or {}) if shard_scope else None
        prefetch_shard_scopes: list[dict[str, Any]] = []

        def _return(rows, delete_rows, sync_mode, metadata):
            metadata = dict(metadata or {})
            if fetch_artifact_descriptor is not None:
                fetch_parameters = dict(metadata.get("fetch_parameters") or {})
                existing_artifact = dict(fetch_parameters.get("fetch_artifact") or {})
                if existing_artifact.get("status") != "hit":
                    artifact_meta = save_fetch_artifact(
                        fetch_artifact_descriptor["key"],
                        run_id=fetch_artifact_descriptor["run_id"],
                        rows=list(rows or []),
                        delete_rows=list(delete_rows or []),
                        sync_mode=sync_mode,
                        fetch_meta=metadata,
                    )
                    fetch_parameters["fetch_artifact"] = (
                        sanitize_fetch_artifact_metadata(artifact_meta)
                    )
                    metadata["fetch_parameters"] = fetch_parameters
            if return_fetch_meta:
                return rows, delete_rows, sync_mode, metadata
            return rows, delete_rows, sync_mode

        def _load_model_fetch_artifact():
            if model_fetch_artifact_descriptor is None:
                return None, {}
            payload, artifact_meta = load_fetch_artifact(
                model_fetch_artifact_descriptor["key"],
                run_id=model_fetch_artifact_descriptor["run_id"],
            )
            if payload is None:
                return None, artifact_meta
            return list(payload.get("rows") or []), artifact_meta

        def _save_model_fetch_artifact(rows, metadata):
            if model_fetch_artifact_descriptor is None:
                return {}
            return save_fetch_artifact(
                model_fetch_artifact_descriptor["key"],
                run_id=model_fetch_artifact_descriptor["run_id"],
                rows=list(rows or []),
                delete_rows=[],
                sync_mode="full",
                fetch_meta=metadata,
            )

        runner = ForwardSyncRunner(
            sync=self.sync,
            ingestion=None,
            client=self.client,
            logger_=self.logger,
        )
        runner._model_coalesce_fields[model_string] = coalesce_fields
        if shard_scope:
            prefetch_shard_scopes = self._sibling_shard_prefetch_scopes(
                model_string=model_string,
                spec=spec,
                shard_scope=shard_scope,
                context=context,
            )
            if len(prefetch_shard_scopes) > 1:
                combined_scope = self._combine_column_filter_shard_scopes(
                    prefetch_shard_scopes
                )
                if combined_scope is not None:
                    shard_scope = combined_scope
        metadata_shard_scope = original_shard_scope or shard_scope
        column_filters = None
        column_filter_batches = [None]
        requested_fetch_mode = "model"
        fetch_key_family = ""
        fetch_parameters = {}
        fetch_column_filters = []
        if shard_scope and shard_scope.get("fetch_mode") == "nqe_column_filter":
            column_filters = shard_scope.get("fetch_column_filters") or None
            column_filter_batches = _partition_column_filters(column_filters)
        if metadata_shard_scope:
            requested_fetch_mode = metadata_shard_scope.get("fetch_mode") or "model"
            fetch_key_family = metadata_shard_scope.get("fetch_key_family") or ""
            fetch_parameters = dict(metadata_shard_scope.get("fetch_parameters") or {})
            fetch_column_filters = list(
                metadata_shard_scope.get("fetch_column_filters") or []
            )
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
        parameters = self._apply_context_tag_parameters(parameters, context)
        query_parameters = dict(parameters or {})
        fetch_artifact_descriptor = self._fetch_artifact_descriptor(
            model_string=model_string,
            spec=spec,
            baseline=baseline,
            context=context,
            shard_scope=shard_scope,
            query_parameters=query_parameters,
            fetch_parameters=fetch_parameters,
            fetch_column_filters=fetch_column_filters,
        )
        model_fetch_artifact_descriptor = self._fetch_artifact_descriptor(
            model_string=model_string,
            spec=spec,
            baseline=baseline,
            context=context,
            shard_scope=shard_scope,
            query_parameters=query_parameters,
            fetch_parameters={},
            fetch_column_filters=[],
            artifact_scope="model_fallback",
        )
        if fetch_artifact_descriptor is not None:
            payload, artifact_meta = load_fetch_artifact(
                fetch_artifact_descriptor["key"],
                run_id=fetch_artifact_descriptor["run_id"],
            )
            if payload is not None:
                cached_metadata = dict(payload.get("fetch_meta") or {})
                cached_fetch_parameters = dict(
                    cached_metadata.get("fetch_parameters") or {}
                )
                cached_fetch_parameters["fetch_artifact"] = (
                    sanitize_fetch_artifact_metadata(artifact_meta)
                )
                cached_metadata["fetch_parameters"] = cached_fetch_parameters
                return _return(
                    list(payload.get("rows") or []),
                    list(payload.get("delete_rows") or []),
                    str(payload.get("sync_mode") or "full"),
                    cached_metadata,
                )

        if shard_scope and shard_scope.get("fetch_mode") != "model":
            model_artifact_rows, model_artifact_meta = _load_model_fetch_artifact()
            if model_artifact_rows is not None:
                rows = list(model_artifact_rows)
                rows, _ = self._filter_rows_to_shard(
                    model_string,
                    rows,
                    [],
                    coalesce_fields,
                    shard_scope,
                )
                filtered_rows, removed_rows = self._apply_device_tag_scope(
                    model_string,
                    rows,
                    context,
                )
                fallback_parameters = dict(fetch_parameters)
                fallback_parameters["fallback_reason"] = "reused_model_fetch_artifact"
                fallback_parameters["model_fetch_artifact"] = (
                    sanitize_fetch_artifact_metadata(model_artifact_meta)
                )
                delete_rows = (
                    removed_rows if context.device_tag_prune_out_of_scope else []
                )
                return _return(
                    filtered_rows,
                    delete_rows,
                    "full",
                    {
                        "fetch_mode": "full_fallback",
                        "fetch_key_family": fetch_key_family,
                        "fetch_parameters": fallback_parameters,
                        "query_parameters": query_parameters,
                        "fetch_column_filters": fetch_column_filters,
                    },
                )

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
        elif baseline is not None and spec.run_query_id:
            partition_retry_summary = {}
            try:
                diff_rows = self._fetch_partitioned_rows(
                    model_string=model_string,
                    partitions=column_filter_batches,
                    operation="diff",
                    retry_summary=partition_retry_summary,
                    fetch_partition=lambda partition: self._run_nqe_diff_without_parameters(
                        spec=spec,
                        context=context,
                        before_snapshot_id=baseline.snapshot_id,
                        column_filters=partition,
                    ),
                )
                rows, delete_rows = runner._split_diff_rows(model_string, diff_rows)
                rows, _ = self._apply_device_tag_scope(model_string, rows, context)
                delete_rows, _ = self._apply_device_tag_scope(
                    model_string, delete_rows, context
                )
                self._save_prefetched_shard_artifacts(
                    prefetch_shard_scopes[1:],
                    model_string=model_string,
                    spec=spec,
                    baseline=baseline,
                    context=context,
                    coalesce_fields=coalesce_fields,
                    rows=rows,
                    delete_rows=delete_rows,
                    sync_mode="diff",
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
                        "fetch_parameters": self._fetch_parameters_with_retry_summary(
                            fetch_parameters,
                            partition_retry_summary,
                        ),
                        "query_parameters": query_parameters,
                        "fetch_column_filters": fetch_column_filters,
                    },
                )
            except (ForwardClientError, ForwardConnectivityError) as exc:
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
                self._attach_partition_retry_summary(
                    fallback_parameters,
                    partition_retry_summary,
                )
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

        partition_retry_summary = {}
        try:
            rows = self._fetch_partitioned_rows(
                model_string=model_string,
                partitions=column_filter_batches,
                operation="full",
                retry_summary=partition_retry_summary,
                fetch_partition=lambda partition: self._run_nqe_query_with_parameter_fallback(
                    spec=spec,
                    context=context,
                    parameters=parameters,
                    column_filters=partition,
                    fetch_all=True,
                    allow_parameter_fallback=(
                        not shard_scope
                        or shard_scope.get("fetch_mode") != "nqe_parameters"
                    ),
                ),
            )
        except (ForwardClientError, ForwardConnectivityError) as exc:
            if not shard_scope or shard_scope.get("fetch_mode") == "model":
                raise
            safe_exc = _safe_exception_summary(exc)
            fallback_message = (
                f"Forward shard-scoped NQE fetch failed for {model_string} using "
                f"{shard_scope['fetch_mode']}; falling back to full model fetch: {safe_exc}"
            )
            self.logger.log_info(fallback_message, obj=self.sync)
            fallback_parameters = dict(fetch_parameters)
            fallback_parameters["fallback_reason"] = safe_exc
            self._attach_partition_retry_summary(
                fallback_parameters,
                partition_retry_summary,
            )
            model_artifact_rows, model_artifact_meta = _load_model_fetch_artifact()
            if model_artifact_rows is None:
                rows = self._run_nqe_query_with_parameter_fallback(
                    spec=spec,
                    context=context,
                    parameters=parameters,
                    column_filters=None,
                    fetch_all=True,
                )
                model_artifact_meta = _save_model_fetch_artifact(
                    rows,
                    {
                        "fetch_mode": "model_fallback_source",
                        "fetch_key_family": "",
                        "fetch_parameters": {
                            "fallback_reason": safe_exc,
                            "source_fetch_mode": shard_scope.get("fetch_mode")
                            or "model",
                        },
                        "query_parameters": query_parameters,
                        "fetch_column_filters": [],
                    },
                )
            else:
                rows = model_artifact_rows
            fallback_parameters["model_fetch_artifact"] = (
                sanitize_fetch_artifact_metadata(model_artifact_meta)
            )
            requested_mode = shard_scope.get("fetch_mode") or "model"
            requested_fetch_mode = (
                "full_fallback" if requested_mode != "model" else "model"
            )
            fetch_parameters = fallback_parameters
        self._save_prefetched_shard_artifacts(
            prefetch_shard_scopes[1:],
            model_string=model_string,
            spec=spec,
            baseline=baseline,
            context=context,
            coalesce_fields=coalesce_fields,
            rows=rows,
            delete_rows=[],
            sync_mode="full",
        )
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
                "fetch_parameters": self._fetch_parameters_with_retry_summary(
                    fetch_parameters,
                    partition_retry_summary,
                ),
                "query_parameters": query_parameters,
                "fetch_column_filters": fetch_column_filters,
            },
        )

    def _sibling_shard_prefetch_limit(self) -> int:
        parameters = getattr(self.sync, "parameters", None) or {}
        configured = parameters.get("shard_fetch_prefetch_steps")
        if configured in ("", None):
            configured = DEFAULT_SHARD_FETCH_PREFETCH_STEPS
        try:
            value = int(configured)
        except (TypeError, ValueError):
            return DEFAULT_SHARD_FETCH_PREFETCH_STEPS
        return max(1, min(value, MAX_SHARD_FETCH_PREFETCH_STEPS))

    def _sibling_shard_prefetch_scopes(
        self,
        *,
        model_string,
        spec,
        shard_scope,
        context: ForwardQueryContext,
    ) -> list[dict[str, Any]]:
        if context.device_tag_prune_out_of_scope:
            return [dict(shard_scope or {})]
        if not shard_scope or shard_scope.get("fetch_mode") != "nqe_column_filter":
            return [dict(shard_scope or {})] if shard_scope else []
        current_filter = self._single_equals_any_filter(
            shard_scope.get("fetch_column_filters")
        )
        if current_filter is None:
            return [dict(shard_scope)]
        limit = self._sibling_shard_prefetch_limit()
        if limit <= 1:
            return [dict(shard_scope)]
        run = active_execution_run(self.sync)
        if run is None:
            return [dict(shard_scope)]
        current_keys = tuple(str(key) for key in shard_scope.get("shard_keys") or ())
        if not current_keys:
            return [dict(shard_scope)]

        step_queryset = run.steps.filter(
            kind=ForwardExecutionStepKindChoices.STAGE,
            model_string=model_string,
            query_name=getattr(spec, "query_name", "") or "",
            execution_mode=getattr(spec, "execution_mode", "") or "",
            execution_value=getattr(spec, "execution_value", "") or "",
            status__in=[
                ForwardExecutionStepStatusChoices.PENDING,
                ForwardExecutionStepStatusChoices.QUEUED,
                ForwardExecutionStepStatusChoices.RUNNING,
                ForwardExecutionStepStatusChoices.FAILED,
                ForwardExecutionStepStatusChoices.TIMEOUT,
            ],
        ).order_by("index", "pk")
        steps = list(step_queryset)
        current_position = None
        current_key_set = set(current_keys)
        for index, step in enumerate(steps):
            if set(str(key) for key in (step.shard_keys or [])) == current_key_set:
                current_position = index
                break
        if current_position is None:
            return [dict(shard_scope)]

        scopes = [dict(shard_scope)]
        for step in steps[current_position + 1 :]:
            if len(scopes) >= limit:
                break
            candidate_scope = {
                "fetch_mode": step.fetch_mode,
                "fetch_key_family": step.fetch_key_family,
                "fetch_parameters": dict(step.fetch_parameters or {}),
                "query_parameters": dict(step.query_parameters or {}),
                "fetch_column_filters": list(step.fetch_column_filters or []),
                "shard_keys": list(step.shard_keys or []),
            }
            if not self._column_filter_scope_compatible(shard_scope, candidate_scope):
                break
            scopes.append(candidate_scope)
        return scopes

    def _column_filter_scope_compatible(self, first_scope, second_scope) -> bool:
        if second_scope.get("fetch_mode") != "nqe_column_filter":
            return False
        if (first_scope.get("fetch_key_family") or "") != (
            second_scope.get("fetch_key_family") or ""
        ):
            return False
        first_filter = self._single_equals_any_filter(
            first_scope.get("fetch_column_filters")
        )
        second_filter = self._single_equals_any_filter(
            second_scope.get("fetch_column_filters")
        )
        if first_filter is None or second_filter is None:
            return False
        return first_filter.get("columnName") == second_filter.get(
            "columnName"
        ) and first_filter.get("operator") == second_filter.get("operator")

    def _single_equals_any_filter(self, column_filters):
        if not column_filters or len(column_filters) != 1:
            return None
        filter_item = dict(column_filters[0] or {})
        if filter_item.get("operator") != "EQUALS_ANY":
            return None
        return filter_item

    def _combine_column_filter_shard_scopes(self, scopes):
        if not scopes:
            return None
        first_scope = dict(scopes[0])
        first_filter = self._single_equals_any_filter(
            first_scope.get("fetch_column_filters")
        )
        if first_filter is None:
            return None
        combined_values = []
        combined_shard_keys = []
        for scope in scopes:
            filter_item = self._single_equals_any_filter(
                scope.get("fetch_column_filters")
            )
            if filter_item is None:
                return None
            for value in filter_item.get("values") or []:
                if value not in combined_values:
                    combined_values.append(value)
            for key in scope.get("shard_keys") or []:
                if key not in combined_shard_keys:
                    combined_shard_keys.append(key)
        combined_filter = dict(first_filter)
        combined_filter["values"] = combined_values
        first_scope["fetch_column_filters"] = [combined_filter]
        first_scope["shard_keys"] = combined_shard_keys
        return first_scope

    def _save_prefetched_shard_artifacts(
        self,
        scopes,
        *,
        model_string,
        spec,
        baseline,
        context: ForwardQueryContext,
        coalesce_fields,
        rows,
        delete_rows,
        sync_mode,
    ):
        if not scopes:
            return
        for scope in scopes:
            scope = dict(scope or {})
            scoped_rows, scoped_delete_rows = self._filter_rows_to_shard(
                model_string,
                list(rows or []),
                list(delete_rows or []),
                coalesce_fields,
                scope,
            )
            query_parameters = self._query_parameters_for_scope(spec, context, scope)
            fetch_parameters = dict(scope.get("fetch_parameters") or {})
            fetch_parameters["prefetch_artifact"] = {
                "source": "sibling_shard_prefetch",
                "prefetched": True,
            }
            descriptor = self._fetch_artifact_descriptor(
                model_string=model_string,
                spec=spec,
                baseline=baseline,
                context=context,
                shard_scope=scope,
                query_parameters=query_parameters,
                fetch_parameters=dict(scope.get("fetch_parameters") or {}),
                fetch_column_filters=list(scope.get("fetch_column_filters") or []),
            )
            if descriptor is None:
                continue
            save_fetch_artifact(
                descriptor["key"],
                run_id=descriptor["run_id"],
                rows=scoped_rows,
                delete_rows=scoped_delete_rows,
                sync_mode=sync_mode,
                fetch_meta={
                    "fetch_mode": scope.get("fetch_mode") or "model",
                    "fetch_key_family": scope.get("fetch_key_family") or "",
                    "fetch_parameters": fetch_parameters,
                    "query_parameters": query_parameters,
                    "fetch_column_filters": list(
                        scope.get("fetch_column_filters") or []
                    ),
                },
            )

    def _query_parameters_for_scope(self, spec, context: ForwardQueryContext, scope):
        parameters = spec.merged_parameters(context.query_parameters)
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
        return self._apply_context_tag_parameters(dict(parameters or {}), context)

    def _fetch_artifact_descriptor(
        self,
        *,
        model_string,
        spec,
        baseline,
        context: ForwardQueryContext,
        shard_scope,
        query_parameters,
        fetch_parameters,
        fetch_column_filters,
        artifact_scope="shard",
    ):
        if not shard_scope:
            return None
        run = active_execution_run(self.sync)
        if run is None:
            return None
        artifact_scope = str(artifact_scope or "shard")
        cache_scope = (
            "shared_sync" if artifact_scope == "model_fallback" else "run_local"
        )
        shard_keys = list(shard_scope.get("shard_keys") or [])
        if not shard_keys and artifact_scope != "model_fallback":
            return None
        artifact_run_id = (
            f"shared-sync-{getattr(self.sync, 'pk', 'unknown')}"
            if cache_scope == "shared_sync"
            else run.pk
        )
        payload = {
            "version": 1,
            "artifact_scope": artifact_scope,
            "cache_scope": cache_scope,
            "sync_id": getattr(self.sync, "pk", None),
            "model": model_string,
            "query_name": getattr(spec, "query_name", ""),
            "execution_mode": getattr(spec, "execution_mode", ""),
            "execution_value": getattr(spec, "execution_value", ""),
            "run_query_id": getattr(spec, "run_query_id", None),
            "commit_id": getattr(spec, "commit_id", None),
            "query_hash": fetch_artifact_key({"query": getattr(spec, "query", "")}),
            "snapshot_id": context.snapshot_id,
            "baseline_snapshot_id": getattr(baseline, "snapshot_id", "") or "",
            "network_hash": fetch_artifact_key({"network_id": context.network_id}),
            "fetch_mode": shard_scope.get("fetch_mode") or "model",
            "fetch_key_family": shard_scope.get("fetch_key_family") or "",
            "fetch_parameters": fetch_parameters or {},
            "query_parameters": query_parameters or {},
            "fetch_column_filters": fetch_column_filters or [],
            "shard_keys": [] if artifact_scope == "model_fallback" else shard_keys,
            "device_tag_include_hash": fetch_artifact_key(
                {"tags": sorted(context.device_tag_include_tags or [])}
            ),
            "device_tag_exclude_hash": fetch_artifact_key(
                {"tags": sorted(context.device_tag_exclude_tags or [])}
            ),
            "device_tag_include_match": context.device_tag_include_match,
            "device_tag_prune_out_of_scope": context.device_tag_prune_out_of_scope,
            "scoped_device_hash": fetch_artifact_key(
                {"devices": sorted(context.scoped_device_names or [])}
            ),
        }
        if cache_scope == "run_local":
            payload["run_id"] = run.pk
        return {
            "key": fetch_artifact_key(payload),
            "run_id": artifact_run_id,
        }

    def _apply_context_tag_parameters(
        self, parameters: dict[str, Any], context: ForwardQueryContext
    ) -> dict[str, Any]:
        if "device_tag_include_tags" not in parameters:
            return parameters
        tag_parameters = {
            "device_tag_include_tags": list(context.device_tag_include_tags or []),
            "device_tag_include_match": context.device_tag_include_match or "any",
            "device_tag_exclude_tags": list(context.device_tag_exclude_tags or []),
        }
        return {**parameters, **tag_parameters}

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
            if row_devices.intersection(scoped_devices):
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

    def _fetch_partitioned_rows(
        self,
        *,
        model_string: str,
        partitions: list[Any],
        operation: str,
        fetch_partition,
        retry_summary=None,
    ) -> list[dict[str, Any]]:
        partition_list = list(partitions or [None])
        operation_label = str(operation or "full")
        stats_lock = Lock()
        value_required_retry_logged = {"alternate": False, "split": False}

        if retry_summary is not None:
            retry_summary["operation"] = operation_label
            retry_summary["partition_count"] = len(partition_list)

        def record_retry_stat(name, amount=1):
            if retry_summary is None:
                return
            with stats_lock:
                retry_summary[name] = int(retry_summary.get(name) or 0) + int(amount)

        def log_value_required_retry_once(retry_kind):
            if retry_kind not in value_required_retry_logged:
                return
            with stats_lock:
                already_logged = value_required_retry_logged[retry_kind]
                if already_logged:
                    return
                value_required_retry_logged[retry_kind] = True
            if retry_kind == "alternate":
                self.logger.log_info(
                    f"{model_string} {operation_label} partition filter required an "
                    "alternate single-value operator; retrying automatically.",
                    obj=self.sync,
                )
            else:
                self.logger.log_info(
                    f"{model_string} {operation_label} partition filter required "
                    "smaller split partitions; retrying automatically.",
                    obj=self.sync,
                )

        def fetch_partition_with_retry(partition):
            try:
                return list(fetch_partition(partition) or [])
            except (
                ForwardClientError,
                ForwardConnectivityError,
                ForwardQueryError,
            ) as exc:
                default_partitions = []
                if _is_value_required_filter_error(exc):
                    default_partitions = (
                        _default_column_filter_partitions_for_equals_any(partition)
                    )
                if default_partitions:
                    record_retry_stat(
                        "alternate_operator_retry_count", len(default_partitions)
                    )
                    log_value_required_retry_once("alternate")
                    rows = []
                    for default_partition in default_partitions:
                        rows.extend(fetch_partition_with_retry(default_partition))
                    record_retry_stat("alternate_operator_success_count")
                    return rows
                split_partitions = _split_column_filter_partition(partition)
                if not split_partitions:
                    alternate_partition = (
                        _alternate_single_value_column_filter_partition(partition)
                    )
                    if alternate_partition:
                        record_retry_stat("alternate_operator_retry_count")
                        if _is_value_required_filter_error(exc):
                            log_value_required_retry_once("alternate")
                        elif isinstance(exc, ForwardConnectivityError):
                            safe_exc = _safe_exception_summary(exc)
                            self.logger.log_info(
                                f"{model_string} {operation_label} single-value partition "
                                "fetch failed; retrying with alternate column-filter "
                                f"operator before full fallback: {safe_exc}",
                                obj=self.sync,
                            )
                        else:
                            safe_exc = _safe_exception_summary(exc)
                            self.logger.log_warning(
                                f"{model_string} {operation_label} single-value partition "
                                "fetch failed; retrying with alternate column-filter "
                                f"operator before full fallback: {safe_exc}",
                                obj=self.sync,
                            )
                        rows = list(fetch_partition(alternate_partition) or [])
                        record_retry_stat("alternate_operator_success_count")
                        return rows
                    raise
                if not _partition_error_allows_split_retry(exc):
                    record_retry_stat("non_retryable_partition_failure_count")
                    raise
                record_retry_stat("split_retry_count", len(split_partitions))
                if _is_value_required_filter_error(exc):
                    log_value_required_retry_once("split")
                elif isinstance(exc, ForwardConnectivityError):
                    safe_exc = _safe_exception_summary(exc)
                    self.logger.log_info(
                        f"{model_string} {operation_label} partition fetch failed; "
                        f"retrying as {len(split_partitions)} smaller partition(s): {safe_exc}",
                        obj=self.sync,
                    )
                else:
                    safe_exc = _safe_exception_summary(exc)
                    self.logger.log_warning(
                        f"{model_string} {operation_label} partition fetch failed; "
                        f"retrying as {len(split_partitions)} smaller partition(s): {safe_exc}",
                        obj=self.sync,
                    )
                retried_rows: list[dict[str, Any]] = []
                for split_partition in split_partitions:
                    retried_rows.extend(fetch_partition_with_retry(split_partition))
                record_retry_stat("split_retry_success_count")
                return retried_rows

        if len(partition_list) <= 1:
            return fetch_partition_with_retry(partition_list[0])

        max_workers = self._query_fetch_worker_count(len(partition_list))
        if max_workers <= 1:
            rows: list[dict[str, Any]] = []
            for partition in partition_list:
                rows.extend(fetch_partition_with_retry(partition))
            return rows

        self.logger.log_info(
            f"Fetching {model_string} with {len(partition_list)} {operation} partition(s) "
            f"using {max_workers} worker(s).",
            obj=self.sync,
        )
        indexed_rows: list[list[dict[str, Any]] | None] = [None] * len(partition_list)
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(
                    self._run_thread_job,
                    fetch_partition_with_retry,
                    partition,
                ): index
                for index, partition in enumerate(partition_list)
            }
            for future in as_completed(futures):
                index = futures[future]
                indexed_rows[index] = list(future.result() or [])

        rows: list[dict[str, Any]] = []
        for partition_rows in indexed_rows:
            rows.extend(partition_rows or [])
        return rows

    def _run_thread_job(self, func, payload=None):
        if payload is None:
            func, payload = func
        close_old_connections()
        try:
            return func(payload)
        finally:
            connection.close()
            connections.close_all()

    def _fetch_parameters_with_retry_summary(self, fetch_parameters, retry_summary):
        parameters = dict(fetch_parameters or {})
        self._attach_partition_retry_summary(parameters, retry_summary)
        return parameters

    def _attach_partition_retry_summary(self, fetch_parameters, retry_summary):
        summary = self._public_partition_retry_summary(retry_summary)
        if summary:
            fetch_parameters["partition_retry_summary"] = summary

    def _public_partition_retry_summary(self, retry_summary):
        retry_summary = dict(retry_summary or {})
        retry_keys = (
            "split_retry_count",
            "alternate_operator_retry_count",
            "split_retry_success_count",
            "alternate_operator_success_count",
            "non_retryable_partition_failure_count",
        )
        if not any(int(retry_summary.get(key) or 0) for key in retry_keys):
            return {}
        summary = {
            "operation": str(retry_summary.get("operation") or ""),
            "partition_count": int(retry_summary.get("partition_count") or 0),
            "split_retry_count": int(retry_summary.get("split_retry_count") or 0),
            "split_retry_success_count": int(
                retry_summary.get("split_retry_success_count") or 0
            ),
            "alternate_operator_retry_count": int(
                retry_summary.get("alternate_operator_retry_count") or 0
            ),
            "alternate_operator_success_count": int(
                retry_summary.get("alternate_operator_success_count") or 0
            ),
        }
        non_retryable_count = int(
            retry_summary.get("non_retryable_partition_failure_count") or 0
        )
        if non_retryable_count:
            summary["non_retryable_partition_failure_count"] = non_retryable_count
        return summary

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
        sync_parameters = getattr(self.sync, "parameters", None) or {}
        backend = sync_parameters.get(
            "execution_backend",
            ForwardExecutionBackendChoices.BRANCHING,
        )
        if backend == ForwardExecutionBackendChoices.FAST_BOOTSTRAP:
            return MAX_QUERY_FETCH_CONCURRENCY
        return DEFAULT_QUERY_FETCH_CONCURRENCY

    def _preflight_row_limit(self) -> int:
        source_parameters = (
            getattr(getattr(self.sync, "source", None), "parameters", None) or {}
        )
        configured = source_parameters.get("query_preflight_row_limit")
        try:
            parsed = int(configured)
        except (TypeError, ValueError):
            parsed = DEFAULT_PREFLIGHT_ROW_LIMIT
        return max(1, min(MAX_PREFLIGHT_ROW_LIMIT, parsed))

    def _supports_parameter_fallback_error(self, exc: Exception) -> bool:
        message = str(exc)
        unrecognized_parameters = (
            "Unrecognized field" in message and "parameters" in message
        )
        return (
            "does not take parameters" in message
            or "Variable parameters not in scope" in message
            or "Parameters were provided, but a main query does not take parameters"
            in message
            or unrecognized_parameters
            or "not marked as ignorable" in message
        )

    def _run_nqe_query_with_parameter_fallback(
        self,
        *,
        spec,
        context: ForwardQueryContext,
        parameters: dict[str, Any],
        limit: int | None = None,
        column_filters=None,
        fetch_all: bool = False,
        allow_parameter_fallback: bool = True,
    ):
        try:
            return self.client.run_nqe_query(
                query=spec.query,
                query_id=spec.run_query_id,
                commit_id=spec.commit_id,
                network_id=context.network_id,
                snapshot_id=context.snapshot_id,
                parameters=parameters,
                limit=limit,
                column_filters=column_filters,
                fetch_all=fetch_all,
            )
        except (ForwardClientError, ForwardConnectivityError) as exc:
            if (
                not allow_parameter_fallback
                or not parameters
                or not self._supports_parameter_fallback_error(exc)
            ):
                raise
            fallback_key = (
                str(getattr(spec, "query_name", "") or ""),
                str(getattr(spec, "execution_value", "") or ""),
                str(getattr(spec, "run_query_id", "") or ""),
            )
            if fallback_key not in self._parameter_fallback_log_keys:
                self._parameter_fallback_log_keys.add(fallback_key)
                self.logger.log_info(
                    f"Forward query `{spec.execution_value}` rejected query parameters; retrying without parameters.",
                    obj=self.sync,
                )
            return self.client.run_nqe_query(
                query=spec.query,
                query_id=spec.run_query_id,
                commit_id=spec.commit_id,
                network_id=context.network_id,
                snapshot_id=context.snapshot_id,
                parameters={},
                limit=limit,
                column_filters=column_filters,
                fetch_all=fetch_all,
            )

    def _run_nqe_diff_without_parameters(
        self,
        *,
        spec,
        context: ForwardQueryContext,
        before_snapshot_id: str,
        column_filters=None,
    ):
        return self.client.run_nqe_diff(
            query_id=spec.run_query_id,
            commit_id=spec.commit_id,
            parameters={},
            before_snapshot_id=before_snapshot_id,
            after_snapshot_id=context.snapshot_id,
            column_filters=column_filters,
            fetch_all=True,
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
