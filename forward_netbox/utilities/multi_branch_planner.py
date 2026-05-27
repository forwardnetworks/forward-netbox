from dataclasses import replace

from .branch_budget import build_branch_plan_with_density
from .branch_budget import DEFAULT_DENSITY_SAFETY_FACTOR
from .branch_budget import DEFAULT_MAX_CHANGES_PER_BRANCH
from .query_fetch import DEFAULT_PREFLIGHT_ROW_LIMIT  # noqa: F401
from .query_fetch import ForwardQueryFetcher


class ForwardMultiBranchPlanner:
    def __init__(self, sync, client, logger_, *, branch_run_state=None):
        self.sync = sync
        self.client = client
        self.logger = logger_
        self.branch_run_state = branch_run_state or {}
        self.model_results = []

    def build_plan(
        self,
        *,
        max_changes_per_branch=DEFAULT_MAX_CHANGES_PER_BRANCH,
        run_preflight=True,
        model_change_density=None,
        model_change_density_profile=None,
        model_strings=None,
        shard_scope=None,
    ):
        fetcher = ForwardQueryFetcher(self.sync, self.client, self.logger)
        context = fetcher.resolve_context(branch_run_state=self.branch_run_state)
        if run_preflight and self._query_preflight_enabled():
            fetcher.run_preflight(context, model_strings=model_strings)
        workloads = fetcher.fetch_workloads(
            context,
            model_strings=model_strings,
            shard_scope=shard_scope,
            include_diagnostics=shard_scope is None,
        )
        self.model_results = [result.as_dict() for result in fetcher.model_results]
        plan = build_branch_plan_with_density(
            workloads,
            max_changes_per_branch=max_changes_per_branch,
            model_change_density=model_change_density,
            model_change_density_profile=model_change_density_profile,
            safety_factor=DEFAULT_DENSITY_SAFETY_FACTOR,
        )
        plan = preserve_single_shard_scope(plan, shard_scope=shard_scope)
        return context.as_dict(), plan

    def _query_preflight_enabled(self) -> bool:
        source = getattr(self.sync, "source", None)
        parameters = dict(getattr(source, "parameters", {}) or {})
        configured = parameters.get("query_preflight_enabled")
        if configured is None:
            return True
        if isinstance(configured, str):
            return configured.strip().lower() in {"1", "true", "yes", "on"}
        return bool(configured)


def preserve_single_shard_scope(plan, *, shard_scope=None):
    if not shard_scope or len(plan) != 1:
        return plan
    shard_keys = tuple(sorted(str(key) for key in shard_scope.get("shard_keys") or ()))
    if not shard_keys:
        return plan
    item = plan[0]
    if item.shard_keys:
        return plan
    if str(shard_scope.get("model") or "") != item.model_string:
        return plan
    if (
        shard_scope.get("query_name")
        and shard_scope.get("query_name") != item.query_name
    ):
        return plan
    if (
        shard_scope.get("execution_value")
        and shard_scope.get("execution_value") != item.execution_value
    ):
        return plan
    return [replace(item, shard_keys=shard_keys)]
