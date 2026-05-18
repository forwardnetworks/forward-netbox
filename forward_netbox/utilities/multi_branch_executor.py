from core.exceptions import SyncError

from ..choices import ForwardSyncStatusChoices
from .branch_budget import DEFAULT_MAX_CHANGES_PER_BRANCH
from .execution_ledger import active_execution_run
from .execution_ledger import branch_run_state_from_execution_run
from .execution_ledger import ensure_branch_execution_run
from .execution_ledger import mark_run_completed
from .execution_telemetry import build_plan_preview
from .multi_branch_lifecycle import cleanup_overflow_branch
from .multi_branch_lifecycle import create_noop_ingestion
from .multi_branch_lifecycle import record_model_density
from .multi_branch_lifecycle import reindex_plan
from .multi_branch_lifecycle import resplit_future_items_for_model
from .multi_branch_lifecycle import run_plan_item
from .multi_branch_lifecycle import set_runtime_phase
from .multi_branch_lifecycle import split_overflow_item
from .multi_branch_lifecycle import validation_run_from_state
from .multi_branch_planner import ForwardMultiBranchPlanner
from .resumable_branching import enqueue_branch_stage_job
from .resumable_branching import get_plan_items
from .resumable_branching import resumable_branching_enabled
from .resumable_branching import update_plan_item_state
from .runtime_guidance import log_branch_plan_capacity_guidance
from .runtime_guidance import log_branch_plan_timeout_guidance
from .sync_state import is_waiting_for_branch_merge
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
        model_strings=None,
        shard_scope=None,
        branch_run_state=None,
    ):
        planner = ForwardMultiBranchPlanner(
            self.sync,
            self.client,
            self.logger,
            branch_run_state=branch_run_state or self.sync.get_branch_run_state(),
        )
        context, plan = planner.build_plan(
            max_changes_per_branch=max_changes_per_branch,
            run_preflight=run_preflight,
            model_change_density=model_change_density,
            model_strings=model_strings,
            shard_scope=shard_scope,
        )
        self.last_model_results = planner.model_results
        return context, plan

    def _load_execution_context(
        self,
        *,
        max_changes_per_branch,
        model_strings=None,
        shard_scope=None,
    ):
        run_state = self.sync.get_branch_run_state()
        if not run_state:
            active_run = active_execution_run(self.sync)
            if active_run is not None:
                run_state = branch_run_state_from_execution_run(active_run)
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
        if run_state.get("awaiting_merge") or is_waiting_for_branch_merge(self.sync):
            raise SyncError(
                "Forward sync is waiting for the current shard branch to be merged."
            )
        next_plan_index = int(run_state.get("next_plan_index") or 1)
        has_persisted_plan = bool(get_plan_items(self.sync))
        self._set_runtime_phase(
            "planning",
            "Resolving snapshot, running query preflight, and building shard plan.",
            next_plan_index=next_plan_index,
        )
        context, plan = self.plan(
            max_changes_per_branch=max_changes_per_branch,
            run_preflight=next_plan_index <= 1 and not has_persisted_plan,
            model_change_density=model_change_density,
            model_strings=model_strings,
            shard_scope=shard_scope,
            branch_run_state=run_state,
        )
        plan_preview = build_plan_preview(
            plan,
            max_changes_per_branch=max_changes_per_branch,
        )
        log_branch_plan_timeout_guidance(self.sync, self.logger, plan)
        log_branch_plan_capacity_guidance(self.sync, self.logger, plan)
        if next_plan_index <= 1 and not has_persisted_plan:
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

    def _create_planning_ingestion(self, context):
        from ..models import ForwardIngestion

        ingestion = ForwardIngestion.objects.create(
            sync=self.sync,
            job=self.job,
            validation_run=self.last_validation_run,
            snapshot_selector=context["snapshot_selector"],
            snapshot_id=context["snapshot_id"],
            snapshot_info=context["snapshot_info"],
            snapshot_metrics=context["snapshot_metrics"],
            model_results=self.last_model_results,
        )
        self.current_ingestion = ingestion
        return ingestion

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
        plan = self._reindex_plan(plan)
        plan, added_items = self._resplit_future_items_for_model(
            plan,
            start_index=current_index + len(split_items),
            model_string=exc.item.model_string,
        )
        if added_items:
            self.logger.log_warning(
                f"Re-split {added_items} remaining shard(s) for "
                f"{exc.item.model_string} using observed branch change density.",
                obj=self.sync,
            )
        return plan

    def _execute_planned_items(self, context, plan, plan_preview, *, next_plan_index):
        ingestions = []
        run_has_issues = False
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
            update_plan_item_state(
                self.sync,
                item.index,
                status="staging",
                last_error="",
            )
            try:
                ingestion = self._run_plan_item(
                    item,
                    context,
                    mark_baseline_ready=is_final and not run_has_issues,
                    merge=self.sync.auto_merge,
                    total_plan_items=len(plan),
                    plan_preview=plan_preview,
                )
            except BranchBudgetExceeded as exc:
                plan = self._handle_branch_budget_exceeded(exc, plan, current_index)
                ensure_branch_execution_run(
                    sync=self.sync,
                    context=context,
                    plan=plan,
                    plan_preview=build_plan_preview(
                        plan,
                        max_changes_per_branch=self.max_changes_per_branch,
                    ),
                    validation_run=self.last_validation_run,
                    job=self.job,
                    max_changes_per_branch=self.max_changes_per_branch,
                    auto_merge=self.sync.auto_merge,
                    model_change_density=self.model_change_density,
                    next_plan_index=current_index + 1,
                )
                self._set_runtime_phase(
                    "queued",
                    "Branch budget retry split the current shard; queued the smaller shard.",
                    next_plan_index=current_index + 1,
                    total_plan_items=len(plan),
                )
                continue

            ingestions.append(ingestion)
            if ingestion.issues.exists():
                run_has_issues = True
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

        if self.job and resumable_branching_enabled(self.sync):
            ensure_branch_execution_run(
                sync=self.sync,
                context=context,
                plan=plan,
                plan_preview=plan_preview,
                validation_run=self.last_validation_run,
                job=self.job,
                max_changes_per_branch=self.max_changes_per_branch,
                auto_merge=self.sync.auto_merge,
                model_change_density=self.model_change_density,
                next_plan_index=next_plan_index,
            )
            ingestion = self._create_planning_ingestion(context)
            enqueue_branch_stage_job(
                self.sync,
                user=self.user,
                adhoc=True,
            )
            self.resumable_started = True
            self.logger.log_info(
                "Forward Branching plan persisted; queued the first shard job.",
                obj=self.sync,
            )
            return [ingestion]
        ensure_branch_execution_run(
            sync=self.sync,
            context=context,
            plan=plan,
            plan_preview=plan_preview,
            validation_run=self.last_validation_run,
            job=self.job,
            max_changes_per_branch=self.max_changes_per_branch,
            auto_merge=self.sync.auto_merge,
            model_change_density=self.model_change_density,
            next_plan_index=next_plan_index,
        )
        return self._execute_planned_items(
            context,
            plan,
            plan_preview,
            next_plan_index=next_plan_index,
        )

    def run_next_plan_item(
        self,
        *,
        max_changes_per_branch=DEFAULT_MAX_CHANGES_PER_BRANCH,
    ):
        self.max_changes_per_branch = max_changes_per_branch
        state = self.sync.get_branch_run_state()
        run = active_execution_run(self.sync)
        if not state and run is not None:
            state = branch_run_state_from_execution_run(run)
        next_plan_index = int(
            state.get("next_plan_index")
            or (run.next_step_index if run is not None else 1)
            or 1
        )
        persisted_item = self._persisted_plan_item(next_plan_index)
        model_strings = (
            [persisted_item["model"]]
            if persisted_item and persisted_item.get("model")
            else None
        )
        context, plan, plan_preview, next_plan_index, model_change_density = (
            self._load_execution_context(
                max_changes_per_branch=max_changes_per_branch,
                model_strings=model_strings,
                shard_scope=persisted_item,
            )
        )
        self.model_change_density = model_change_density
        total_plan_items = int(state.get("total_plan_items") or len(plan))
        full_plan_preview = state.get("plan_preview") or plan_preview
        item = self._select_plan_item(plan, persisted_item, next_plan_index)
        if item is None:
            self.sync.clear_branch_run_state()
            mark_run_completed(self.sync, baseline_ready=True)
            self.sync.status = ForwardSyncStatusChoices.COMPLETED
            self.sync.__class__.objects.filter(pk=self.sync.pk).update(
                status=self.sync.status
            )
            self.logger.log_info("Forward multi-branch sync already completed.")
            return []

        item = self._with_global_index(item, next_plan_index)
        is_final = next_plan_index >= total_plan_items
        update_plan_item_state(
            self.sync,
            item.index,
            status="staging",
            last_error="",
        )
        self._set_runtime_phase(
            "staging",
            f"Applying shard {item.index}/{total_plan_items} for {item.model_string}.",
            next_plan_index=item.index,
            total_plan_items=total_plan_items,
        )
        try:
            ingestion = self._run_plan_item(
                item,
                context,
                mark_baseline_ready=is_final,
                merge=False,
                total_plan_items=total_plan_items,
                plan_preview=full_plan_preview,
                automated_merge=self.sync.auto_merge,
            )
        except BranchBudgetExceeded as exc:
            plan = self._handle_branch_budget_exceeded(exc, plan, 0)
            ensure_branch_execution_run(
                sync=self.sync,
                context=context,
                plan=plan,
                plan_preview=build_plan_preview(
                    plan,
                    max_changes_per_branch=self.max_changes_per_branch,
                ),
                validation_run=self.last_validation_run,
                job=self.job,
                max_changes_per_branch=self.max_changes_per_branch,
                auto_merge=self.sync.auto_merge,
                model_change_density=self.model_change_density,
                next_plan_index=next_plan_index,
            )
            self._set_runtime_phase(
                "queued",
                "Branch budget retry split the current shard; queued the smaller shard.",
                next_plan_index=next_plan_index,
                total_plan_items=len(plan),
            )
            enqueue_branch_stage_job(self.sync, user=self.user, adhoc=True)
            return []
        return [ingestion]

    def _persisted_plan_item(self, index):
        for item in get_plan_items(self.sync):
            if int(item.get("index") or 0) == int(index):
                return item
        return None

    def _select_plan_item(self, plan, persisted_item, index):
        if not persisted_item:
            if 1 <= int(index) <= len(plan):
                return plan[int(index) - 1]
            return None
        if not persisted_item.get("model"):
            if 1 <= int(index) <= len(plan):
                return plan[int(index) - 1]
            return None
        shard_keys = set(persisted_item.get("shard_keys") or [])
        candidates = [
            item
            for item in plan
            if item.model_string == persisted_item.get("model")
            and item.query_name == persisted_item.get("query_name", item.query_name)
            and item.execution_value
            == persisted_item.get("execution_value", item.execution_value)
        ]
        if shard_keys:
            for item in candidates:
                if set(item.shard_keys or ()) == shard_keys:
                    return item
        if len(candidates) == 1:
            return candidates[0]
        for item in candidates:
            if int(item.estimated_changes) == int(
                persisted_item.get("estimated_changes") or 0
            ):
                return item
        return None

    def _with_global_index(self, item, index):
        if int(item.index) == int(index):
            return item
        return item.__class__(
            index=int(index),
            model_string=item.model_string,
            label=item.label,
            estimated_changes=item.estimated_changes,
            upsert_rows=item.upsert_rows,
            delete_rows=item.delete_rows,
            sync_mode=item.sync_mode,
            coalesce_fields=item.coalesce_fields,
            shard_keys=item.shard_keys,
            query_name=item.query_name,
            execution_mode=item.execution_mode,
            execution_value=item.execution_value,
            query_runtime_ms=item.query_runtime_ms,
            baseline_snapshot_id=item.baseline_snapshot_id,
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
        automated_merge=False,
    ):
        return run_plan_item(
            self,
            item,
            context,
            mark_baseline_ready=mark_baseline_ready,
            merge=merge,
            total_plan_items=total_plan_items,
            plan_preview=plan_preview,
            automated_merge=automated_merge,
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

    def _resplit_future_items_for_model(self, plan, *, start_index, model_string):
        return resplit_future_items_for_model(
            self,
            plan,
            start_index=start_index,
            model_string=model_string,
        )
