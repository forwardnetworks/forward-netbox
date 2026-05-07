from core.exceptions import SyncError
from core.models import ObjectType
from django.utils import timezone
from netbox.context import current_request
from netbox_branching.choices import BranchStatusChoices
from netbox_branching.contextvars import active_branch
from netbox_branching.models import Branch

from ..choices import ForwardIngestionPhaseChoices
from ..choices import ForwardSyncStatusChoices
from .branch_budget import BranchWorkload
from .branch_budget import DEFAULT_DENSITY_SAFETY_FACTOR
from .branch_budget import DEFAULT_MAX_CHANGES_PER_BRANCH
from .branch_budget import effective_row_budget_for_model
from .branch_budget import split_workload
from .branching import build_branch_name
from .branching import build_branch_request
from .execution_telemetry import build_plan_preview
from .multi_branch_planner import ForwardMultiBranchPlanner
from .query_fetch import DEFAULT_PREFLIGHT_ROW_LIMIT  # noqa: F401
from .query_fetch import plan_item_model_result
from .sync import ForwardSyncRunner
from .validation import ForwardValidationRunner

AUTO_SPLIT_MIN_ROWS_PER_BRANCH = 1


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

    def run(self, *, max_changes_per_branch=DEFAULT_MAX_CHANGES_PER_BRANCH):
        self.max_changes_per_branch = max_changes_per_branch
        self._set_runtime_phase(
            "initializing",
            "Starting sync preflight.",
        )
        run_state = self.sync.get_branch_run_state()
        persisted_density = self.sync.get_model_change_density()
        run_state_density = run_state.get("model_change_density") or {}
        self.model_change_density = {
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
            model_change_density=self.model_change_density,
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
            self.last_validation_run = self._validation_run_from_state(run_state)

        if not plan:
            self.logger.log_info("No Forward changes were returned for this run.")
            self.sync.clear_branch_run_state()
            return [self._create_noop_ingestion(context)]

        if next_plan_index > len(plan):
            self.sync.clear_branch_run_state()
            self.logger.log_info("Forward multi-branch sync already completed.")
            return []

        self.sync.set_branch_run_state(
            {
                "snapshot_selector": context["snapshot_selector"],
                "snapshot_id": context["snapshot_id"],
                "max_changes_per_branch": max_changes_per_branch,
                "next_plan_index": next_plan_index,
                "total_plan_items": len(plan),
                "auto_merge": self.sync.auto_merge,
                "awaiting_merge": False,
                "model_change_density": self.model_change_density,
                "validation_run_id": getattr(self.last_validation_run, "pk", None),
                "plan_preview": plan_preview,
                "phase": "executing",
                "phase_message": "Applying planned shard changes.",
            }
        )

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

    def _set_runtime_phase(
        self,
        phase,
        message,
        *,
        next_plan_index=None,
        total_plan_items=None,
    ):
        state = self.sync.get_branch_run_state()
        if next_plan_index is not None:
            state["next_plan_index"] = int(next_plan_index)
        if total_plan_items is not None:
            state["total_plan_items"] = int(total_plan_items)
        if state.get("phase") != str(phase):
            state["phase_started"] = timezone.now().isoformat()
        state["phase"] = str(phase)
        state["phase_message"] = str(message)
        self.sync.set_branch_run_state(state)
        self.logger.log_info(message, obj=self.sync)

    def _create_noop_ingestion(self, context):
        from ..models import ForwardIngestion

        ingestion = ForwardIngestion.objects.create(
            sync=self.sync,
            job=self.job,
            snapshot_selector=context["snapshot_selector"],
            snapshot_id=context["snapshot_id"],
            snapshot_info=context["snapshot_info"],
            snapshot_metrics=context["snapshot_metrics"],
            baseline_ready=True,
            model_results=self.last_model_results,
            validation_run=self.last_validation_run,
        )
        if self.job:
            self.job.object_type = ObjectType.objects.get_for_model(ingestion)
            self.job.object_id = ingestion.pk
            self.job.save(update_fields=["object_type", "object_id"])
        return ingestion

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
        from ..models import ForwardIngestion

        self.sync.status = ForwardSyncStatusChoices.SYNCING
        self.sync.__class__.objects.filter(pk=self.sync.pk).update(
            status=self.sync.status
        )
        ingestion = ForwardIngestion.objects.create(
            sync=self.sync,
            job=self.job,
            validation_run=self.last_validation_run,
        )
        self.current_ingestion = ingestion
        branch = Branch(
            name=build_branch_name(sync=self.sync, ingestion=ingestion, item=item)
        )
        branch.save(provision=False)
        ingestion.branch = branch
        ingestion.save(update_fields=["branch"])

        if self.job:
            self.job.object_type = ObjectType.objects.get_for_model(ingestion)
            self.job.object_id = ingestion.pk
            self.job.save(update_fields=["object_type", "object_id"])

        branch.provision(user=self.user)
        branch.refresh_from_db()
        if branch.status == BranchStatusChoices.FAILED:
            self.logger.log_failure(f"Branch failed: `{branch}`", obj=branch)
            raise SyncError("Branch creation failed.")

        self.logger.log_info(
            f"New branch created {branch.name} for {item.estimated_changes} estimated changes.",
            obj=branch,
        )
        self._run_item_in_branch(
            item,
            context,
            ingestion,
            branch,
            total_plan_items=total_plan_items,
        )
        if ingestion.issues.exists():
            messages = list(ingestion.issues.values_list("message", flat=True)[:5])
            raise SyncError(
                "Forward multi-branch shard completed with issues: "
                + "; ".join(messages)
            )

        actual_changes = branch.get_unmerged_changes().count()
        self._record_model_density(
            item.model_string,
            estimated_changes=item.estimated_changes,
            actual_changes=actual_changes,
        )
        if actual_changes > self.max_changes_per_branch:
            raise BranchBudgetExceeded(
                item=item,
                branch=branch,
                ingestion=ingestion,
                actual_changes=actual_changes,
                budget=self.max_changes_per_branch,
            )

        if not merge:
            self.sync.set_branch_run_state(
                {
                    "snapshot_selector": context["snapshot_selector"],
                    "snapshot_id": context["snapshot_id"],
                    "max_changes_per_branch": self.max_changes_per_branch,
                    "next_plan_index": item.index + 1,
                    "total_plan_items": total_plan_items,
                    "auto_merge": False,
                    "awaiting_merge": True,
                    "pending_ingestion_id": ingestion.pk,
                    "pending_plan_index": item.index,
                    "pending_is_final": mark_baseline_ready,
                    "model_change_density": self.model_change_density,
                    "validation_run_id": getattr(self.last_validation_run, "pk", None),
                    "plan_preview": plan_preview,
                }
            )
            self.sync.status = ForwardSyncStatusChoices.READY_TO_MERGE
            self.sync.__class__.objects.filter(pk=self.sync.pk).update(
                status=self.sync.status,
            )
            self.logger.log_info(
                f"Forward sync paused after shard {item.index}/{total_plan_items}; merge the branch to continue.",
                obj=ingestion,
            )
            return ingestion

        ingestion.sync_merge(mark_baseline_ready=mark_baseline_ready)
        if mark_baseline_ready:
            self.sync.clear_branch_run_state()
        else:
            self.sync.set_branch_run_state(
                {
                    "snapshot_selector": context["snapshot_selector"],
                    "snapshot_id": context["snapshot_id"],
                    "max_changes_per_branch": self.max_changes_per_branch,
                    "next_plan_index": item.index + 1,
                    "total_plan_items": total_plan_items,
                    "auto_merge": True,
                    "awaiting_merge": False,
                    "model_change_density": self.model_change_density,
                    "validation_run_id": getattr(self.last_validation_run, "pk", None),
                    "plan_preview": plan_preview,
                }
            )
        return ingestion

    def _validation_run_from_state(self, run_state):
        validation_run_id = run_state.get("validation_run_id")
        if not validation_run_id:
            return None
        try:
            from ..models import ForwardValidationRun

            return ForwardValidationRun.objects.get(pk=validation_run_id)
        except Exception:
            return None

    def _run_item_in_branch(
        self, item, context, ingestion, branch, *, total_plan_items
    ):
        runner = ForwardSyncRunner(
            sync=self.sync,
            ingestion=ingestion,
            client=self.client,
            logger_=self.logger,
        )
        runner._model_coalesce_fields[item.model_string] = item.coalesce_fields
        ingestion.snapshot_selector = context["snapshot_selector"]
        ingestion.snapshot_id = context["snapshot_id"]
        ingestion.snapshot_info = context["snapshot_info"]
        ingestion.snapshot_metrics = context["snapshot_metrics"]
        ingestion.sync_mode = item.sync_mode
        ingestion.model_results = [
            plan_item_model_result(
                item,
                context,
                total_plan_items=total_plan_items,
            )
        ]
        ingestion.save(
            update_fields=[
                "snapshot_selector",
                "snapshot_id",
                "snapshot_info",
                "snapshot_metrics",
                "sync_mode",
                "model_results",
            ],
        )
        self.logger.init_statistics(item.model_string, 0)
        self.logger.add_statistics_total(item.model_string, item.estimated_changes)

        current_branch = active_branch.get()
        request_token = None
        if current_request.get() is None:
            request_token = current_request.set(build_branch_request(self.user))
        try:
            active_branch.set(branch)
            try:
                runner._apply_model_rows(item.model_string, item.upsert_rows)
                if item.delete_rows:
                    runner._delete_model_rows(item.model_string, item.delete_rows)
            finally:
                active_branch.set(None)
        finally:
            active_branch.set(current_branch)
            if request_token is not None:
                current_request.reset(request_token)

    def _record_model_density(self, model_string, *, estimated_changes, actual_changes):
        if estimated_changes <= 0 or actual_changes <= 0:
            return
        current_density = self.model_change_density.get(model_string)
        observed_density = float(actual_changes) / float(estimated_changes)
        if current_density is None:
            updated_density = observed_density
        else:
            # Weighted moving average to dampen single-shard outliers.
            updated_density = (0.7 * float(current_density)) + (0.3 * observed_density)
        self.model_change_density[model_string] = max(0.01, updated_density)
        self.sync.set_model_change_density(self.model_change_density)

    def _split_overflow_item(self, item):
        row_budget = effective_row_budget_for_model(
            item.model_string,
            max_changes_per_branch=self.max_changes_per_branch,
            model_change_density=self.model_change_density,
            safety_factor=DEFAULT_DENSITY_SAFETY_FACTOR,
        )
        row_budget = max(AUTO_SPLIT_MIN_ROWS_PER_BRANCH, row_budget)
        if row_budget >= item.estimated_changes:
            row_budget = max(
                AUTO_SPLIT_MIN_ROWS_PER_BRANCH,
                item.estimated_changes // 2,
            )

        workload = BranchWorkload(
            model_string=item.model_string,
            label=item.label,
            upsert_rows=list(item.upsert_rows),
            delete_rows=list(item.delete_rows),
            sync_mode=item.sync_mode,
            coalesce_fields=list(item.coalesce_fields),
            query_name=item.query_name,
            execution_mode=item.execution_mode,
            execution_value=item.execution_value,
            query_runtime_ms=item.query_runtime_ms,
            baseline_snapshot_id=item.baseline_snapshot_id,
        )
        split_items = split_workload(
            workload,
            max_changes_per_branch=row_budget,
        )
        return split_items

    def _cleanup_overflow_branch(self, exc):
        ingestion = exc.ingestion
        branch = exc.branch
        if ingestion is not None and branch is not None:
            ingestion.issues.create(
                message=(
                    f"Branch budget retry: shard produced {exc.actual_changes} changes "
                    f"against budget {exc.budget}; auto-splitting and retrying."
                ),
                phase=ForwardIngestionPhaseChoices.SYNC,
            )
            ingestion.branch = None
            ingestion.save(update_fields=["branch"])
            branch.delete()

    def _reindex_plan(self, plan):
        return [
            item.__class__(
                index=index,
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
            for index, item in enumerate(plan, start=1)
        ]
