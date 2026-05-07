from core.exceptions import SyncError

from .branch_budget import DEFAULT_MAX_CHANGES_PER_BRANCH
from .execution_telemetry import build_plan_preview
from .multi_branch_lifecycle import cleanup_overflow_branch
from .multi_branch_lifecycle import create_noop_ingestion
from .multi_branch_lifecycle import record_model_density
from .multi_branch_lifecycle import reindex_plan
from .multi_branch_lifecycle import run_plan_item
from .multi_branch_lifecycle import set_runtime_phase
from .multi_branch_lifecycle import split_overflow_item
from .multi_branch_lifecycle import validation_run_from_state
from .multi_branch_planner import ForwardMultiBranchPlanner
from .validation import ForwardValidationRunner


class BranchBudgetExceeded(SyncError):
    def __init__(self, *, item, branch, ingestion, actual_changes, budget):
        super().__init__(
            f"Branch `{branch}` produced {actual_changes} changes, exceeding "
            f"the branch budget of {budget}."
        )
        self.item = item
        self.branch = branch
        self.ingestion = ingestion
        self.actual_changes = actual_changes
        self.budget = budget


class ForwardMultiBranchExecutor:
    def __init__(self, sync, client, logger_, *, user=None, job=None):
        self.sync = sync
        self.client = client
        self.logger = logger_
        self.user = user
        self.job = job
        self.current_ingestion = None
        self.last_model_results = []
        self.last_validation_run = None

    def plan(
        self,
        *,
        max_changes_per_branch=DEFAULT_MAX_CHANGES_PER_BRANCH,
        run_preflight=True,
        model_change_density=None,
    ):
        planner = ForwardMultiBranchPlanner(
            self.sync,
            self.client,
            self.logger,
            branch_run_state=self.sync.get_branch_run_state(),
        )
        context, plan = planner.build_plan(
            max_changes_per_branch=max_changes_per_branch,
            run_preflight=run_preflight,
            model_change_density=model_change_density,
        )
        self.last_model_results = planner.model_results
        return context, plan

    def _load_execution_context(self, *, max_changes_per_branch):
        run_state = self.sync.get_branch_run_state()
        persisted_density = self.sync.get_model_change_density()
        run_state_density = run_state.get("model_change_density") or {}
        model_change_density = {
            **persisted_density,
            **{
                key: value
                for key, value in run_state_density.items()
                if isinstance(key, str)
            },
        }
        if run_state.get("awaiting_merge"):
            raise SyncError(
                "Forward sync is waiting for the current shard branch to be merged."
            )
        next_plan_index = int(run_state.get("next_plan_index") or 1)
        self._set_runtime_phase(
            "planning",
            "Resolving snapshot, running query preflight, and building shard plan.",
            next_plan_index=next_plan_index,
        )
        context, plan = self.plan(
            max_changes_per_branch=max_changes_per_branch,
            run_preflight=next_plan_index <= 1,
            model_change_density=model_change_density,
        )
        plan_preview = build_plan_preview(
            plan,
            max_changes_per_branch=self.max_changes_per_branch,
        )
        if next_plan_index <= 1:
            self._set_runtime_phase(
                "validating",
                "Recording plan validation results.",
                total_plan_items=len(plan),
            )
            self.last_validation_run = ForwardValidationRunner(
                self.sync,
                self.client,
                self.logger,
                job=self.job,
            ).record_plan_validation(
                context,
                plan,
                self.last_model_results,
            )
        else:
            self.last_validation_run = validation_run_from_state(run_state)
        return context, plan, plan_preview, next_plan_index, model_change_density

    def _persist_execution_state(
        self,
        context,
        plan,
        plan_preview,
        *,
        next_plan_index,
        auto_merge,
        model_change_density,
    ):
        self.sync.set_branch_run_state(
            {
                "snapshot_selector": context["snapshot_selector"],
                "snapshot_id": context["snapshot_id"],
                "max_changes_per_branch": self.max_changes_per_branch,
                "next_plan_index": next_plan_index,
                "total_plan_items": len(plan),
                "auto_merge": auto_merge,
                "awaiting_merge": False,
                "model_change_density": model_change_density,
                "validation_run_id": getattr(self.last_validation_run, "pk", None),
                "plan_preview": plan_preview,
                "phase": "executing",
                "phase_message": "Applying planned shard changes.",
            }
        )

    def _handle_branch_budget_exceeded(self, exc, plan, current_index):
        self._record_model_density(
            exc.item.model_string,
            estimated_changes=exc.item.estimated_changes,
            actual_changes=exc.actual_changes,
        )
        self._cleanup_overflow_branch(exc)
        split_items = self._split_overflow_item(exc.item)
        if len(split_items) <= 1:
            raise SyncError(str(exc))
        self.logger.log_warning(
            f"Auto-splitting shard {exc.item.index} for {exc.item.model_string} "
            f"after {exc.actual_changes} actual changes exceeded the branch budget "
            f"of {self.max_changes_per_branch}.",
            obj=self.sync,
        )
        plan.pop(current_index)
        for split_item in reversed(split_items):
            plan.insert(current_index, split_item)
        return self._reindex_plan(plan)

    def _execute_planned_items(self, context, plan, plan_preview, *, next_plan_index):
        ingestions = []
        current_index = next_plan_index - 1
        while current_index < len(plan):
            item = plan[current_index]
            is_final = current_index == len(plan) - 1
            self._set_runtime_phase(
                "executing",
                f"Applying shard {item.index}/{len(plan)} for {item.model_string}.",
                next_plan_index=item.index,
                total_plan_items=len(plan),
            )
            try:
                ingestion = self._run_plan_item(
                    item,
                    context,
                    mark_baseline_ready=is_final,
                    merge=self.sync.auto_merge,
                    total_plan_items=len(plan),
                    plan_preview=plan_preview,
                )
            except BranchBudgetExceeded as exc:
                plan = self._handle_branch_budget_exceeded(exc, plan, current_index)
                self.sync.set_branch_run_state(
                    {
                        "snapshot_selector": context["snapshot_selector"],
                        "snapshot_id": context["snapshot_id"],
                        "max_changes_per_branch": self.max_changes_per_branch,
                        "next_plan_index": current_index + 1,
                        "total_plan_items": len(plan),
                        "auto_merge": self.sync.auto_merge,
                        "awaiting_merge": False,
                        "model_change_density": self.model_change_density,
                        "validation_run_id": getattr(
                            self.last_validation_run,
                            "pk",
                            None,
                        ),
                        "plan_preview": build_plan_preview(
                            plan,
                            max_changes_per_branch=self.max_changes_per_branch,
                        ),
                    }
                )
                continue

            ingestions.append(ingestion)
            if not self.sync.auto_merge:
                return ingestions
            current_index += 1

        self.sync.clear_branch_run_state()
        return ingestions

    def run(self, *, max_changes_per_branch=DEFAULT_MAX_CHANGES_PER_BRANCH):
        self.max_changes_per_branch = max_changes_per_branch
        self._set_runtime_phase("initializing", "Starting sync preflight.")
        context, plan, plan_preview, next_plan_index, model_change_density = (
            self._load_execution_context(max_changes_per_branch=max_changes_per_branch)
        )
        self.model_change_density = model_change_density

        if not plan:
            self.logger.log_info("No Forward changes were returned for this run.")
            self.sync.clear_branch_run_state()
            return [create_noop_ingestion(self, context)]

        if next_plan_index > len(plan):
            self.sync.clear_branch_run_state()
            self.logger.log_info("Forward multi-branch sync already completed.")
            return []

        self._persist_execution_state(
            context,
            plan,
            plan_preview,
            next_plan_index=next_plan_index,
            auto_merge=self.sync.auto_merge,
            model_change_density=self.model_change_density,
        )
        return self._execute_planned_items(
            context,
            plan,
            plan_preview,
            next_plan_index=next_plan_index,
        )

    def _set_runtime_phase(
        self, phase, message, *, next_plan_index=None, total_plan_items=None
    ):
        return set_runtime_phase(
            self,
            phase,
            message,
            next_plan_index=next_plan_index,
            total_plan_items=total_plan_items,
        )

    def _create_noop_ingestion(self, context):
        return create_noop_ingestion(self, context)

    def _run_plan_item(
        self,
        item,
        context,
        *,
        mark_baseline_ready,
        merge,
        total_plan_items,
        plan_preview,
    ):
        return run_plan_item(
            self,
            item,
            context,
            mark_baseline_ready=mark_baseline_ready,
            merge=merge,
            total_plan_items=total_plan_items,
            plan_preview=plan_preview,
        )

    def _validation_run_from_state(self, run_state):
        return validation_run_from_state(run_state)

    def _record_model_density(self, model_string, *, estimated_changes, actual_changes):
        return record_model_density(
            self,
            model_string,
            estimated_changes=estimated_changes,
            actual_changes=actual_changes,
        )

    def _split_overflow_item(self, item):
        return split_overflow_item(self, item)

    def _cleanup_overflow_branch(self, exc):
        return cleanup_overflow_branch(exc)

    def _reindex_plan(self, plan):
        return reindex_plan(plan)
