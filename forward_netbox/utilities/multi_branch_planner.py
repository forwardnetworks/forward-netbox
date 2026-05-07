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
    ):
        fetcher = ForwardQueryFetcher(self.sync, self.client, self.logger)
        context = fetcher.resolve_context(branch_run_state=self.branch_run_state)
        if run_preflight:
            fetcher.run_preflight(context)
        workloads = fetcher.fetch_workloads(context)
        self.model_results = [result.as_dict() for result in fetcher.model_results]
        plan = build_branch_plan_with_density(
            workloads,
            max_changes_per_branch=max_changes_per_branch,
            model_change_density=model_change_density,
            safety_factor=DEFAULT_DENSITY_SAFETY_FACTOR,
        )
        return context.as_dict(), plan
