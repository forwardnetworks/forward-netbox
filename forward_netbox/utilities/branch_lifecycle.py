# Branch-staging lifecycle helpers for the single production executor.
from core.models import ObjectType
from netbox.context import current_request
from netbox_branching.contextvars import active_branch

from .apply_engine import select_apply_engine
from .branching import build_branch_request
from .query_fetch import plan_item_model_result
from .sync import ForwardSyncRunner


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
    initialized_models = getattr(executor, "_statistics_initialized_models", None)
    if initialized_models is None:
        initialized_models = set()
        executor._statistics_initialized_models = initialized_models
    if item.model_string not in initialized_models:
        executor.logger.init_statistics(item.model_string, 0)
        initialized_models.add(item.model_string)
    executor.logger.add_statistics_total(item.model_string, item.estimated_changes)
    executor.logger.log_info(
        f"Applying {item.model_string} ({item.index}/{total_plan_items}).",
        obj=executor.sync,
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
            )
            engine.apply_upserts(runner, item.model_string, item.upsert_rows)
            if item.delete_rows:
                engine.apply_deletes(runner, item.model_string, item.delete_rows)
            if item.model_string == "dcim.device":
                from .ownership import record_device_identity_candidates

                record_device_identity_candidates(
                    ingestion,
                    getattr(runner, "_device_identity_candidates", set()),
                )
        finally:
            active_branch.set(None)
    finally:
        active_branch.set(current_branch)
        if request_token is not None:
            current_request.reset(request_token)
