from core.exceptions import SyncError
from core.models import ObjectType
from django.utils import timezone
from netbox.context import current_request
from netbox_branching.choices import BranchStatusChoices
from netbox_branching.contextvars import active_branch
from netbox_branching.models import Branch

from ..choices import ForwardExecutionBackendChoices
from ..choices import ForwardExecutionRunStatusChoices
from ..choices import ForwardIngestionPhaseChoices
from ..choices import ForwardSyncStatusChoices
from ..exceptions import ForwardQueryError
from .apply_engine import select_apply_engine
from .branch_budget import BranchWorkload
from .branch_budget import DEFAULT_DENSITY_SAFETY_FACTOR
from .branch_budget import effective_workload_row_budget
from .branch_budget import soft_budget_limit
from .branch_budget import split_workload
from .branching import build_branch_name
from .branching import build_branch_request
from .query_fetch import plan_item_model_result
from .resumable_branching import update_plan_item_state
from .sync import ForwardSyncRunner
from .sync_state import clear_branch_run_progress_fields
from .sync_state import touch_branch_run_progress

AUTO_SPLIT_MIN_ROWS_PER_BRANCH = 1


def set_runtime_phase(
    executor, phase, message, *, next_plan_index=None, total_plan_items=None
):
    from .execution_ledger import active_execution_run

    run = active_execution_run(executor.sync)
    if run is not None:
        run.status = ForwardExecutionRunStatusChoices.RUNNING
        run.phase = str(phase)
        run.phase_message = str(message)
        if next_plan_index is not None:
            run.next_step_index = int(next_plan_index)
        if total_plan_items is not None:
            run.total_steps = int(total_plan_items)
        run.latest_heartbeat = timezone.now()
        run.save(
            update_fields=[
                "status",
                "phase",
                "phase_message",
                "next_step_index",
                "total_steps",
                "latest_heartbeat",
            ]
        )
        executor.logger.log_info(message, obj=executor.sync)
        return
    state = executor.sync.get_branch_run_state()
    if not state:
        executor.logger.log_info(message, obj=executor.sync)
        return
    if state.get("phase") != str(phase) or state.get("phase_message") != str(message):
        clear_branch_run_progress_fields(state)
    if next_plan_index is not None:
        state["next_plan_index"] = int(next_plan_index)
    if total_plan_items is not None:
        state["total_plan_items"] = int(total_plan_items)
    if state.get("phase") != str(phase):
        state["phase_started"] = timezone.now().isoformat()
    state["phase"] = str(phase)
    state["phase_message"] = str(message)
    executor.sync.set_branch_run_state(state)
    executor.logger.log_info(message, obj=executor.sync)


def create_noop_ingestion(executor, context):
    from ..models import ForwardIngestion

    ingestion = ForwardIngestion.objects.create(
        sync=executor.sync,
        job=executor.job,
        snapshot_selector=context["snapshot_selector"],
        snapshot_id=context["snapshot_id"],
        snapshot_info=context["snapshot_info"],
        snapshot_metrics=context["snapshot_metrics"],
        baseline_ready=True,
        model_results=executor.last_model_results,
        validation_run=executor.last_validation_run,
    )
    if executor.job:
        executor.job.object_type = ObjectType.objects.get_for_model(ingestion)
        executor.job.object_id = ingestion.pk
        executor.job.save(update_fields=["object_type", "object_id"])
    return ingestion


def run_plan_item(
    executor,
    item,
    context,
    *,
    mark_baseline_ready,
    merge,
    total_plan_items,
    plan_preview,
    automated_merge=False,
):
    from ..models import ForwardIngestion

    executor.sync.status = ForwardSyncStatusChoices.SYNCING
    executor.sync.__class__.objects.filter(pk=executor.sync.pk).update(
        status=executor.sync.status
    )
    ingestion = ForwardIngestion.objects.create(
        sync=executor.sync,
        job=executor.job,
        validation_run=executor.last_validation_run,
    )
    executor.current_ingestion = ingestion
    branch = Branch(
        name=build_branch_name(sync=executor.sync, ingestion=ingestion, item=item)
    )
    branch.save(provision=False)
    ingestion.branch = branch
    ingestion.save(update_fields=["branch"])

    if executor.job:
        executor.job.object_type = ObjectType.objects.get_for_model(ingestion)
        executor.job.object_id = ingestion.pk
        executor.job.save(update_fields=["object_type", "object_id"])

    branch.provision(user=executor.user)
    branch.refresh_from_db()
    if branch.status == BranchStatusChoices.FAILED:
        executor.logger.log_failure(f"Branch failed: `{branch}`", obj=branch)
        raise SyncError("Branch creation failed.")

    executor.logger.log_info(
        f"New branch created {branch.name} for {item.estimated_changes} estimated changes.",
        obj=branch,
    )
    run_item_in_branch(
        executor,
        item,
        context,
        ingestion,
        branch,
        total_plan_items=total_plan_items,
    )
    row_counts = model_row_counters(executor.logger, item.model_string)
    update_plan_item_state(executor.sync, item.index, **row_counts)
    if ingestion.issues.exists():
        messages = list(ingestion.issues.values_list("message", flat=True)[:5])
        executor.logger.log_warning(
            "Forward multi-branch shard completed with row issues and will "
            "continue with later shards: " + "; ".join(messages),
            obj=ingestion,
        )
        mark_baseline_ready = False

    actual_changes = branch.get_unmerged_changes().count()
    record_model_density(
        executor,
        item.model_string,
        estimated_changes=item.estimated_changes,
        actual_changes=actual_changes,
    )
    update_plan_item_state(
        executor.sync,
        item.index,
        actual_changes=actual_changes,
        branch_name=branch.name,
    )
    if actual_changes > executor.max_changes_per_branch:
        soft_limit = soft_budget_limit(executor.max_changes_per_branch)
        if actual_changes <= soft_limit:
            executor.logger.log_warning(
                f"Shard {item.index}/{total_plan_items} for {item.model_string} exceeded "
                f"the branch budget guideline ({executor.max_changes_per_branch}) with "
                f"{actual_changes} changes; accepting because it is within the soft "
                f"overrun limit ({soft_limit}).",
                obj=ingestion,
            )
        else:
            from .multi_branch_executor import BranchBudgetExceeded

            raise BranchBudgetExceeded(
                item=item,
                branch=branch,
                ingestion=ingestion,
                actual_changes=actual_changes,
                budget=executor.max_changes_per_branch,
            )

    if not merge:
        phase = "queued_merge" if automated_merge else "awaiting_merge"
        phase_message = (
            f"Queued merge for shard {item.index}/{total_plan_items}."
            if automated_merge
            else f"Forward sync paused after shard {item.index}/{total_plan_items}; merge the branch to continue."
        )
        update_plan_item_state(
            executor.sync,
            item.index,
            status="staged",
            ingestion_id=ingestion.pk,
            branch_name=branch.name,
            actual_changes=actual_changes,
            **row_counts,
        )
        executor.sync.status = (
            ForwardSyncStatusChoices.QUEUED
            if automated_merge
            else ForwardSyncStatusChoices.READY_TO_MERGE
        )
        executor.sync.__class__.objects.filter(pk=executor.sync.pk).update(
            status=executor.sync.status,
        )
        from .execution_ledger import active_execution_run

        run = active_execution_run(executor.sync)
        if run is not None:
            run.status = ForwardExecutionRunStatusChoices.WAITING
            run.phase = phase
            run.phase_message = phase_message
            run.next_step_index = item.index + 1
            run.total_steps = total_plan_items
            run.auto_merge = bool(automated_merge)
            run.latest_heartbeat = timezone.now()
            run.save(
                update_fields=[
                    "status",
                    "phase",
                    "phase_message",
                    "next_step_index",
                    "total_steps",
                    "auto_merge",
                    "latest_heartbeat",
                ]
            )
        else:
            executor.sync.set_branch_run_state(
                {
                    "snapshot_selector": context["snapshot_selector"],
                    "snapshot_id": context["snapshot_id"],
                    "max_changes_per_branch": executor.max_changes_per_branch,
                    "next_plan_index": item.index + 1,
                    "total_plan_items": total_plan_items,
                    "auto_merge": True,
                    "awaiting_merge": False,
                    "model_change_density": executor.model_change_density,
                    "validation_run_id": getattr(
                        executor.last_validation_run,
                        "pk",
                        None,
                    ),
                    "plan_preview": plan_preview,
                }
            )
        executor.logger.log_info(
            phase_message,
            obj=ingestion,
        )
        if automated_merge:
            merge_job = ingestion.enqueue_merge_job(
                user=executor.user,
                remove_branch=True,
            )
            update_plan_item_state(
                executor.sync,
                item.index,
                status="merge_queued",
                merge_job_id=merge_job.pk,
            )
        return ingestion

    update_plan_item_state(
        executor.sync,
        item.index,
        status="staged",
        ingestion_id=ingestion.pk,
        branch_name=branch.name,
        actual_changes=actual_changes,
        **row_counts,
    )
    ingestion.sync_merge(mark_baseline_ready=mark_baseline_ready)
    if mark_baseline_ready:
        executor.sync.clear_branch_run_state()
    else:
        from .execution_ledger import active_execution_run

        run = active_execution_run(executor.sync)
        if run is None:
            executor.sync.set_branch_run_state(
                {
                    "snapshot_selector": context["snapshot_selector"],
                    "snapshot_id": context["snapshot_id"],
                    "max_changes_per_branch": executor.max_changes_per_branch,
                    "next_plan_index": item.index + 1,
                    "total_plan_items": total_plan_items,
                    "auto_merge": True,
                    "awaiting_merge": False,
                    "model_change_density": executor.model_change_density,
                    "validation_run_id": getattr(
                        executor.last_validation_run,
                        "pk",
                        None,
                    ),
                    "plan_preview": plan_preview,
                }
            )
    return ingestion


def validation_run_from_state(run_state):
    validation_run_id = run_state.get("validation_run_id")
    if not validation_run_id:
        return None
    try:
        from ..models import ForwardValidationRun

        return ForwardValidationRun.objects.get(pk=validation_run_id)
    except Exception:
        return None


def run_item_in_branch(executor, item, context, ingestion, branch, *, total_plan_items):
    runner = ForwardSyncRunner(
        sync=executor.sync,
        ingestion=ingestion,
        client=executor.client,
        logger_=executor.logger,
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
    executor.logger.init_statistics(item.model_string, 0)
    executor.logger.add_statistics_total(item.model_string, item.estimated_changes)
    touch_branch_run_progress(
        executor.sync,
        phase_message=(
            f"Applying shard {item.index}/{total_plan_items} for {item.model_string}."
        ),
        model_string=item.model_string,
        shard_index=item.index,
        total_plan_items=total_plan_items,
        row_count=0,
        row_total=item.estimated_changes,
    )

    current_branch = active_branch.get()
    request_token = None
    if current_request.get() is None:
        request_token = current_request.set(build_branch_request(executor.user))
    try:
        active_branch.set(branch)
        try:
            engine = select_apply_engine(
                sync=executor.sync,
                model_string=item.model_string,
                backend=ForwardExecutionBackendChoices.BRANCHING,
            )
            engine.apply_upserts(runner, item.model_string, item.upsert_rows)
            if item.delete_rows:
                engine.apply_deletes(runner, item.model_string, item.delete_rows)
        finally:
            active_branch.set(None)
    finally:
        active_branch.set(current_branch)
        if request_token is not None:
            current_request.reset(request_token)


def record_model_density(executor, model_string, *, estimated_changes, actual_changes):
    if estimated_changes <= 0 or actual_changes <= 0:
        return
    current_density = executor.model_change_density.get(model_string)
    observed_density = float(actual_changes) / float(estimated_changes)
    if current_density is None:
        updated_density = observed_density
    else:
        updated_density = (0.7 * float(current_density)) + (0.3 * observed_density)
    executor.model_change_density[model_string] = max(0.01, updated_density)
    executor.sync.set_model_change_density(executor.model_change_density)


def model_row_counters(logger_, model_string):
    stats = (
        (getattr(logger_, "log_data", {}) or {})
        .get("statistics", {})
        .get(
            model_string,
            {},
        )
    )
    return {
        "attempted_row_count": int(stats.get("current") or 0),
        "applied_row_count": int(stats.get("applied") or 0),
        "skipped_row_count": int(stats.get("skipped") or 0),
        "failed_row_count": int(stats.get("failed") or 0),
    }


def split_overflow_item(executor, item):
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
        apply_engine=item.apply_engine,
        apply_engine_reason=item.apply_engine_reason,
        apply_engine_decision=item.apply_engine_decision,
        operation=item.operation,
    )
    row_budget = effective_workload_row_budget(
        workload,
        max_changes_per_branch=executor.max_changes_per_branch,
        model_change_density=executor.model_change_density,
        safety_factor=DEFAULT_DENSITY_SAFETY_FACTOR,
    )
    row_budget = max(AUTO_SPLIT_MIN_ROWS_PER_BRANCH, row_budget)
    if row_budget >= item.estimated_changes:
        row_budget = max(
            AUTO_SPLIT_MIN_ROWS_PER_BRANCH,
            item.estimated_changes // 2,
        )

    return split_workload(
        workload,
        max_changes_per_branch=row_budget,
    )


def resplit_future_items_for_model(executor, plan, *, start_index, model_string):
    updated_plan = []
    added_items = 0
    for index, item in enumerate(plan):
        item_workload = BranchWorkload(
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
            apply_engine=item.apply_engine,
            apply_engine_reason=item.apply_engine_reason,
            apply_engine_decision=item.apply_engine_decision,
            operation=item.operation,
        )
        row_budget = effective_workload_row_budget(
            item_workload,
            max_changes_per_branch=executor.max_changes_per_branch,
            model_change_density=executor.model_change_density,
            safety_factor=DEFAULT_DENSITY_SAFETY_FACTOR,
        )
        row_budget = max(AUTO_SPLIT_MIN_ROWS_PER_BRANCH, row_budget)
        if (
            index < start_index
            or item.model_string != model_string
            or item.estimated_changes <= row_budget
        ):
            updated_plan.append(item)
            continue

        try:
            split_items = split_overflow_item(executor, item)
        except ForwardQueryError:
            updated_plan.append(item)
            continue
        if len(split_items) <= 1:
            updated_plan.append(item)
            continue
        updated_plan.extend(split_items)
        added_items += len(split_items) - 1

    if not added_items:
        return plan, 0
    return reindex_plan(updated_plan), added_items


def cleanup_overflow_branch(exc):
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


def reindex_plan(plan):
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
            apply_engine=item.apply_engine,
            apply_engine_reason=item.apply_engine_reason,
            apply_engine_decision=item.apply_engine_decision,
            operation=item.operation,
        )
        for index, item in enumerate(plan, start=1)
    ]
