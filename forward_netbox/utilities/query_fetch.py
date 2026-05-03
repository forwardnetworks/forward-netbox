import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from dataclasses import field
from typing import Any

from ..exceptions import ForwardClientError
from ..exceptions import ForwardConnectivityError
from ..exceptions import ForwardQueryError
from .branch_budget import BranchPlanItem
from .branch_budget import BranchWorkload
from .forward_api import LATEST_PROCESSED_SNAPSHOT
from .query_registry import get_query_specs
from .sync import ForwardSyncRunner
from .sync_contracts import default_coalesce_fields_for_model
from .sync_contracts import validate_row_shape_for_model

DEFAULT_PREFLIGHT_ROW_LIMIT = 50
DEFAULT_QUERY_FETCH_CONCURRENCY = 4


@dataclass(frozen=True)
class ForwardQueryContext:
    network_id: str
    snapshot_selector: str
    snapshot_id: str
    snapshot_info: dict[str, Any] = field(default_factory=dict)
    snapshot_metrics: dict[str, Any] = field(default_factory=dict)
    query_parameters: dict[str, Any] = field(default_factory=dict)
    maps: list[Any] = field(default_factory=list)

    def as_dict(self) -> dict[str, Any]:
        return {
            "network_id": self.network_id,
            "snapshot_selector": self.snapshot_selector,
            "snapshot_id": self.snapshot_id,
            "snapshot_info": self.snapshot_info,
            "snapshot_metrics": self.snapshot_metrics,
            "query_parameters": self.query_parameters,
            "maps": self.maps,
        }


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
        }


class ForwardQueryFetcher:
    def __init__(self, sync, client, logger_):
        self.sync = sync
        self.client = client
        self.logger = logger_
        self.model_results: list[ForwardModelResult] = []

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

        return ForwardQueryContext(
            network_id=network_id,
            snapshot_selector=snapshot_selector,
            snapshot_id=snapshot_id,
            snapshot_info=snapshot_info or {},
            snapshot_metrics=snapshot_metrics or {},
            query_parameters=self.sync.get_query_parameters(),
            maps=self.sync.get_maps(),
        )

    def run_preflight(
        self,
        context: ForwardQueryContext,
        *,
        row_limit=DEFAULT_PREFLIGHT_ROW_LIMIT,
    ) -> None:
        self.logger.log_info(
            "Running Forward query preflight before full multi-branch planning.",
            obj=self.sync,
        )
        jobs = self._query_jobs(context)
        if not jobs:
            return
        max_workers = self._query_fetch_worker_count(len(jobs))
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            for model_string, spec, preflight_rows in executor.map(
                self._run_preflight_job,
                ((context, row_limit, job) for job in jobs),
            ):
                self.logger.log_info(
                    f"Preflight validated {len(preflight_rows)} rows for {model_string} from {spec.execution_mode} `{spec.execution_value}`.",
                    obj=self.sync,
                )

    def _run_preflight_job(self, payload):
        context, row_limit, job = payload
        model_string, spec, coalesce_fields = job
        preflight_rows = self.client.run_nqe_query(
            query=spec.query,
            query_id=spec.query_id,
            commit_id=spec.commit_id,
            network_id=context.network_id,
            snapshot_id=context.snapshot_id,
            parameters=spec.merged_parameters(context.query_parameters),
            limit=row_limit,
            fetch_all=False,
        )
        for row in preflight_rows:
            validate_row_shape_for_model(model_string, row, coalesce_fields)
        return model_string, spec, preflight_rows

    def _query_jobs(self, context: ForwardQueryContext):
        jobs = []
        for model_string in self.sync.get_model_strings():
            specs = get_query_specs(model_string, maps=context.maps)
            if not specs:
                raise ForwardQueryError(
                    f"No enabled built-in or custom query maps were resolved for {model_string}."
                )
            coalesce_fields = self._coalesce_fields(model_string, specs)
            for spec in specs:
                jobs.append((model_string, spec, coalesce_fields))
        return jobs

    def fetch_workloads(
        self,
        context: ForwardQueryContext,
        *,
        validate_rows=True,
    ) -> list[BranchWorkload]:
        workloads = []
        self.model_results = []
        jobs = self._build_workload_jobs(context)
        if not jobs:
            return workloads
        max_workers = self._query_fetch_worker_count(len(jobs))
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            for model_result, workload in executor.map(
                self._run_workload_job,
                ((context, validate_rows, job) for job in jobs),
            ):
                self.model_results.append(model_result)
                if workload is not None:
                    workloads.append(workload)
        return workloads

    def _build_workload_jobs(self, context: ForwardQueryContext):
        jobs = []
        for model_string in self.sync.get_model_strings():
            specs = get_query_specs(model_string, maps=context.maps)
            coalesce_fields = self._coalesce_fields(model_string, specs)
            baseline = self.sync.incremental_diff_baseline(
                specs=specs,
                current_snapshot_id=context.snapshot_id,
            )
            for spec in specs:
                jobs.append((model_string, spec, baseline, coalesce_fields))
        return jobs

    def _run_workload_job(self, payload):
        context, validate_rows, job = payload
        model_string, spec, baseline, coalesce_fields = job
        baseline_snapshot_id = getattr(baseline, "snapshot_id", "") or ""
        started = time.perf_counter()
        rows, delete_rows, sync_mode = self._fetch_spec_rows(
            model_string,
            spec,
            baseline,
            context,
            coalesce_fields,
        )
        runtime_ms = round((time.perf_counter() - started) * 1000, 1)
        if validate_rows:
            self.validate_rows(
                model_string,
                rows,
                delete_rows,
                coalesce_fields,
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
            )
        return model_result, workload

    def fetch_sample_results(
        self,
        context: ForwardQueryContext,
        *,
        row_limit=DEFAULT_PREFLIGHT_ROW_LIMIT,
    ) -> list[ForwardModelResult]:
        self.model_results = []
        jobs = self._query_jobs(context)
        if not jobs:
            return self.model_results
        max_workers = self._query_fetch_worker_count(len(jobs))
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            for result in executor.map(
                self._run_sample_job,
                ((context, row_limit, job) for job in jobs),
            ):
                self.model_results.append(result)
        return self.model_results

    def _run_sample_job(self, payload):
        context, row_limit, job = payload
        model_string, spec, coalesce_fields = job
        started = time.perf_counter()
        rows = self.client.run_nqe_query(
            query=spec.query,
            query_id=spec.query_id,
            commit_id=spec.commit_id,
            network_id=context.network_id,
            snapshot_id=context.snapshot_id,
            parameters=spec.merged_parameters(context.query_parameters),
            limit=row_limit,
            fetch_all=False,
        )
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
        )

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
    ):
        runner = ForwardSyncRunner(
            sync=self.sync,
            ingestion=None,
            client=self.client,
            logger_=self.logger,
        )
        runner._model_coalesce_fields[model_string] = coalesce_fields

        if baseline is not None and spec.query_id:
            try:
                diff_rows = self.client.run_nqe_diff(
                    query_id=spec.query_id,
                    commit_id=spec.commit_id,
                    before_snapshot_id=baseline.snapshot_id,
                    after_snapshot_id=context.snapshot_id,
                    fetch_all=True,
                )
                rows, delete_rows = runner._split_diff_rows(model_string, diff_rows)
                return rows, delete_rows, "diff"
            except (ForwardClientError, ForwardConnectivityError) as exc:
                self.logger.log_warning(
                    f"Forward NQE diff failed for {model_string} using `{spec.execution_value}`; "
                    f"falling back to full query execution: {exc}",
                    obj=self.sync,
                )

        rows = self.client.run_nqe_query(
            query=spec.query,
            query_id=spec.query_id,
            commit_id=spec.commit_id,
            network_id=context.network_id,
            snapshot_id=context.snapshot_id,
            parameters=spec.merged_parameters(context.query_parameters),
            fetch_all=True,
        )
        return rows, [], "full"

    def _coalesce_fields(self, model_string, specs) -> list[list[str]]:
        if specs:
            return [list(field_set) for field_set in specs[0].coalesce_fields] or (
                default_coalesce_fields_for_model(model_string)
            )
        return default_coalesce_fields_for_model(model_string)

    def _query_fetch_worker_count(self, job_count: int) -> int:
        return max(1, min(DEFAULT_QUERY_FETCH_CONCURRENCY, int(job_count)))


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
    }
