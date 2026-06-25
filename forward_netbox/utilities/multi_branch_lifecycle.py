# Branch-staging lifecycle helpers shared by the executors.
#
# Trimmed for 2.0: single-branch is the only execution path, so the per-shard
# orchestration (run_plan_item, overflow splitting, density learning, resumable
# plan-item state, overlap scheduling) is gone. What remains is the staging
# primitive (run_item_in_branch), the runtime-phase heartbeat, and the no-change
# ingestion helper — all reused by ForwardSingleBranchExecutor and the
# fast-bootstrap base it extends.
from core.models import ObjectType
from django.utils import timezone
from netbox.context import current_request
from netbox_branching.contextvars import active_branch

from ..choices import ForwardExecutionBackendChoices
from ..choices import ForwardExecutionRunStatusChoices
from .apply_engine import select_apply_engine
from .branching import build_branch_request
from .query_fetch import plan_item_model_result
from .sync import ForwardSyncRunner
from .sync_state import get_branch_run_display_state
from .sync_state import touch_branch_run_progress


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
    state = get_branch_run_display_state(executor.sync)
    if state and state.get("state_source") == "execution_ledger":
        # Display state is synthesized from the execution ledger and must remain
        # read-only. Runtime phase mutations are persisted only on real runs.
        pass
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


def run_item_in_branch(executor, item, context, ingestion, branch, *, total_plan_items):
    runner = ForwardSyncRunner(
        sync=executor.sync,
        ingestion=ingestion,
        client=executor.client,
        logger_=executor.logger,
    )
    runner._model_coalesce_fields[item.model_string] = item.coalesce_fields
    # Per-device scope-tag map resolved at fetch time (apply_device_scope_tags);
    # without this, branched syncs would tag nothing.
    runner._scope_matched_tags = {
        str(k): list(v) for k, v in (context.get("scoped_matched_tags") or {}).items()
    }
    ingestion.snapshot_selector = context["snapshot_selector"]
    ingestion.snapshot_id = context["snapshot_id"]
    ingestion.snapshot_info = context["snapshot_info"]
    ingestion.snapshot_metrics = context["snapshot_metrics"]
    ingestion.sync_mode = item.sync_mode
    ingestion.model_results = [
        plan_item_model_result(item, context, total_plan_items=total_plan_items)
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
            f"Applying {item.model_string} ({item.index}/{total_plan_items})."
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
                backend=ForwardExecutionBackendChoices.SINGLE_BRANCH,
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
