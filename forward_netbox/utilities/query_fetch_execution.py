import time
from concurrent.futures import as_completed
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from dataclasses import field
from typing import Any

from ..choices import FORWARD_OPTIONAL_MODELS
from ..choices import ForwardApplyEngineChoices
from ..exceptions import ForwardClientError
from ..exceptions import ForwardConnectivityError
from ..exceptions import ForwardQueryError
from .apply_engine import apply_engine_decision_for
from .branch_budget import BranchPlanItem
from .branch_budget import BranchWorkload
from .branch_budget import row_shard_key
from .branch_budget import shard_fetch_contract
from .forward_api import DEFAULT_QUERY_FETCH_CONCURRENCY
from .forward_api import LATEST_PROCESSED_SNAPSHOT
from .forward_api import MAX_QUERY_FETCH_CONCURRENCY
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
from .sync_contracts import default_coalesce_fields_for_model
from .sync_contracts import validate_row_shape_for_model

DEFAULT_PREFLIGHT_ROW_LIMIT = 50
SHARD_FETCH_COLUMN_FILTER_CHUNK_SIZE = 250


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
    scoped_device_names: set[str] = field(default_factory=set)

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
            "scoped_device_count": len(self.scoped_device_names),
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


def _row_device_names(model_string: str, row: dict[str, Any]) -> set[str]:
    names: set[str] = set()
    if model_string == "dcim.device":
        device_name = str(row.get("name") or "").strip()
        if device_name:
            names.add(device_name)
        return names
    for key, value in row.items():
        key_lower = str(key).lower()
        if key_lower in _DEVICE_FIELD_NAMES:
            names.update(_extract_device_names(value))
        elif key_lower.endswith("_device"):
            names.update(_extract_device_names(value))
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
        }


class ForwardQueryFetcher:
    def __init__(self, sync, client, logger_):
        self.sync = sync
        self.client = client
        self.logger = logger_
        self.model_results: list[ForwardModelResult] = []
        self._failed_model_results: dict[str, ForwardModelResult] = {}

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
                f"Unable to fetch Forward snapshot metrics for `{snapshot_id}`: {exc}",
                obj=self.sync,
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
        scoped_device_names = self._resolve_scoped_device_names(
            network_id=network_id,
            snapshot_id=snapshot_id,
            include_tags=include_tags,
            exclude_tags=exclude_tags,
            include_match=include_match,
        )

        return ForwardQueryContext(
            network_id=network_id,
            snapshot_selector=snapshot_selector,
            snapshot_id=snapshot_id,
            ingestion_id=(
                branch_run_state.get("ingestion_id")
                or branch_run_state.get("pending_ingestion_id")
                or branch_run_state.get("current_ingestion_id")
            ),
            snapshot_info=snapshot_info or {},
            snapshot_metrics=snapshot_metrics or {},
            query_parameters=self.sync.get_query_parameters(),
            maps=self.sync.get_maps(),
            device_tag_include_tags=include_tags,
            device_tag_exclude_tags=exclude_tags,
            device_tag_include_match=include_match,
            scoped_device_names=scoped_device_names,
        )

    def _resolve_scoped_device_names(
        self,
        *,
        network_id: str,
        snapshot_id: str,
        include_tags: list[str],
        exclude_tags: list[str],
        include_match: str,
    ) -> set[str]:
        if not include_tags and not exclude_tags:
            return set()
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
                "select {name: device.name}",
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
                f"Forward device tag filter query failed: {exc}"
            ) from exc
        names = {
            str(row.get("name") or "").strip()
            for row in rows
            if str(row.get("name") or "").strip()
        }
        self.logger.log_info(
            f"Resolved device tag scope with {len(names)} devices "
            f"(include={include_tags or ['-']}, include_match={include_match}, exclude={exclude_tags or ['-']}).",
            obj=self.sync,
        )
        return names

    def run_preflight(
        self,
        context: ForwardQueryContext,
        *,
        row_limit=DEFAULT_PREFLIGHT_ROW_LIMIT,
        model_strings=None,
    ) -> None:
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
                self._run_preflight_job,
                ((context, row_limit, job) for job in jobs),
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
            preflight_rows = self._apply_device_tag_scope(
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
        for model_string in enabled_models:
            try:
                specs = get_query_specs(model_string, maps=context.maps)
                specs = resolve_query_specs_for_client(specs, self.client)
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
    ) -> list[BranchWorkload]:
        workloads = []
        self.model_results = list(self._failed_model_results.values())
        jobs = self._build_workload_jobs(
            context,
            model_strings=model_strings,
            shard_scope=shard_scope,
        )
        if not jobs:
            return workloads
        self.logger.log_info(
            f"Fetching workload rows for {len(jobs)} query map job(s)."
        )
        max_workers = self._query_fetch_worker_count(len(jobs))
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [
                executor.submit(self._run_workload_job, (context, validate_rows, job))
                for job in jobs
            ]
            completed = 0
            for future in as_completed(futures):
                model_result, workload = future.result()
                self.model_results.append(model_result)
                if workload is not None:
                    workloads.append(workload)
                completed += 1
                self.logger.log_info(
                    f"Fetched workload job {completed}/{len(jobs)} for "
                    f"{model_result.model_string} "
                    f"({model_result.execution_mode} `{model_result.execution_value}`) "
                    f"in {model_result.runtime_ms}ms."
                )
        self._append_ipaddress_diagnostics(context)
        self._append_ipaddress_parent_prefix_diagnostics(workloads)
        self._append_routing_diagnostics(context)
        return workloads

    def _build_workload_jobs(
        self,
        context: ForwardQueryContext,
        *,
        model_strings=None,
        shard_scope=None,
    ):
        jobs = []
        enabled_models = list(model_strings or self.sync.get_model_strings())
        for model_string in enabled_models:
            if model_string in self._failed_model_results:
                continue
            try:
                specs = get_query_specs(model_string, maps=context.maps)
                specs = resolve_query_specs_for_client(specs, self.client)
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
            exclude_ingestion_id = getattr(context, "ingestion_id", None)
            baseline = self.sync.incremental_diff_baseline(
                specs=specs,
                current_snapshot_id=context.snapshot_id,
                exclude_ingestion_id=exclude_ingestion_id,
            )
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

    def _run_workload_job(self, payload):
        context, validate_rows, job = payload
        model_string, spec, baseline, coalesce_fields, shard_scope = job
        baseline_snapshot_id = getattr(baseline, "snapshot_id", "") or ""
        started = time.perf_counter()
        try:
            rows, delete_rows, sync_mode = self._fetch_spec_rows(
                model_string,
                spec,
                baseline,
                context,
                coalesce_fields,
                shard_scope=shard_scope,
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
            f"Skipping {model_string} because Forward query validation failed: {exc}",
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
        message = str(exc)
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
                self._run_sample_job,
                ((context, row_limit, job) for job in jobs),
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
        rows = self._apply_device_tag_scope(model_string, rows, context)
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
    ):
        runner = ForwardSyncRunner(
            sync=self.sync,
            ingestion=None,
            client=self.client,
            logger_=self.logger,
        )
        runner._model_coalesce_fields[model_string] = coalesce_fields
        column_filters = None
        column_filter_batches = [None]
        if shard_scope and shard_scope.get("fetch_mode") == "nqe_column_filter":
            column_filters = shard_scope.get("fetch_column_filters") or None
            column_filter_batches = _partition_column_filters(column_filters)
        parameters = spec.merged_parameters(context.query_parameters)
        if shard_scope:
            if shard_scope.get("fetch_mode") == "nqe_parameters":
                parameters = {
                    **parameters,
                    **(shard_scope.get("fetch_parameters") or {}),
                }
            if shard_scope.get("query_parameters"):
                parameters = {
                    **parameters,
                    **(shard_scope.get("query_parameters") or {}),
                }
            if shard_scope.get("fetch_mode") != "model":
                self.logger.log_info(
                    f"Fetching {model_string} shard using {shard_scope['fetch_mode']} scope.",
                    obj=self.sync,
                )

        if baseline is not None and spec.run_query_id:
            try:
                diff_rows = []
                for partition in column_filter_batches:
                    diff_rows.extend(
                        self._run_nqe_diff_without_parameters(
                            spec=spec,
                            context=context,
                            before_snapshot_id=baseline.snapshot_id,
                            column_filters=partition,
                        )
                    )
                rows, delete_rows = runner._split_diff_rows(model_string, diff_rows)
                rows = self._apply_device_tag_scope(model_string, rows, context)
                delete_rows = self._apply_device_tag_scope(
                    model_string, delete_rows, context
                )
                if shard_scope:
                    rows, delete_rows = self._filter_rows_to_shard(
                        model_string,
                        rows,
                        delete_rows,
                        coalesce_fields,
                        shard_scope,
                    )
                return rows, delete_rows, "diff"
            except (ForwardClientError, ForwardConnectivityError) as exc:
                self.logger.log_warning(
                    f"Forward NQE diff failed for {model_string} using `{spec.execution_value}`; "
                    f"falling back to full query execution: {exc}",
                    obj=self.sync,
                )
        elif baseline is not None and not spec.run_query_id:
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
                self.logger.log_warning(
                    f"Forward diffs require a newer processed snapshot than the latest baseline; "
                    f"baseline ingestion `{latest_baseline.pk}` already matches snapshot `{context.snapshot_id}`, "
                    f"so running full query execution for {model_string} instead.",
                    obj=self.sync,
                )

        try:
            rows = []
            for partition in column_filter_batches:
                rows.extend(
                    self._run_nqe_query_with_parameter_fallback(
                        spec=spec,
                        context=context,
                        parameters=parameters,
                        column_filters=partition,
                        fetch_all=True,
                    )
                )
        except (ForwardClientError, ForwardConnectivityError) as exc:
            if not shard_scope or shard_scope.get("fetch_mode") == "model":
                raise
            self.logger.log_warning(
                f"Forward shard-scoped NQE fetch failed for {model_string} using "
                f"{shard_scope['fetch_mode']}; falling back to full model fetch: {exc}",
                obj=self.sync,
            )
            rows = self._run_nqe_query_with_parameter_fallback(
                spec=spec,
                context=context,
                parameters=parameters,
                column_filters=None,
                fetch_all=True,
            )
        if shard_scope:
            rows, _ = self._filter_rows_to_shard(
                model_string,
                rows,
                [],
                coalesce_fields,
                shard_scope,
            )
        rows = self._apply_device_tag_scope(model_string, rows, context)
        return rows, [], "full"

    def _apply_device_tag_scope(
        self, model_string: str, rows: list[dict], context: ForwardQueryContext
    ) -> list[dict]:
        scoped_devices = context.scoped_device_names or set()
        if not scoped_devices:
            return rows
        filtered = []
        for row in rows:
            row_devices = _row_device_names(model_string, row)
            if not row_devices:
                filtered.append(row)
                continue
            if row_devices.intersection(scoped_devices):
                filtered.append(row)
        removed = len(rows) - len(filtered)
        if removed:
            self.logger.log_info(
                f"Applied device-tag scope to {model_string}: kept {len(filtered)}/{len(rows)} rows.",
                obj=self.sync,
            )
        return filtered

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

    def _coalesce_fields(self, model_string, specs) -> list[list[str]]:
        if specs:
            return [list(field_set) for field_set in specs[0].coalesce_fields] or (
                default_coalesce_fields_for_model(model_string)
            )
        return default_coalesce_fields_for_model(model_string)

    def _query_fetch_worker_count(self, job_count: int) -> int:
        source_parameters = (
            getattr(getattr(self.sync, "source", None), "parameters", None) or {}
        )
        configured = source_parameters.get("query_fetch_concurrency")
        try:
            worker_limit = int(configured)
        except (TypeError, ValueError):
            worker_limit = DEFAULT_QUERY_FETCH_CONCURRENCY
        worker_limit = max(1, min(MAX_QUERY_FETCH_CONCURRENCY, worker_limit))
        return max(1, min(worker_limit, int(job_count)))

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
            if not parameters or not self._supports_parameter_fallback_error(exc):
                raise
            self.logger.log_warning(
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
