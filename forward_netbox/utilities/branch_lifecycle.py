# Branch-staging lifecycle helpers for the single production executor.
from core.models import ObjectType
from netbox.context import current_request
from netbox_branching.contextvars import active_branch

from ..choices import ForwardApplyEngineChoices
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
        baseline_ready=False,
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


def run_item_direct_to_main(executor, item, context, ingestion, *, total_plan_items):
    """Apply one proved first-baseline plan item with no active branch context."""
    runner = ForwardSyncRunner(
        sync=executor.sync,
        ingestion=ingestion,
        client=executor.client,
        logger_=executor.logger,
    )
    runner._model_coalesce_fields[item.model_string] = item.coalesce_fields
    runner._scope_matched_tags = {
        str(k): list(v) for k, v in (context.get("scoped_matched_tags") or {}).items()
    }
    ingestion.sync_mode = item.sync_mode
    ingestion.model_results = [
        plan_item_model_result(item, context, total_plan_items=total_plan_items)
    ]
    ingestion.save(update_fields=["sync_mode", "model_results"])
    initialized_models = getattr(executor, "_statistics_initialized_models", None)
    if initialized_models is None:
        initialized_models = set()
        executor._statistics_initialized_models = initialized_models
    if item.model_string not in initialized_models:
        executor.logger.init_statistics(item.model_string, 0)
        initialized_models.add(item.model_string)
    executor.logger.add_statistics_total(item.model_string, item.estimated_changes)
    executor.logger.log_info(
        f"Fast baseline applying {item.model_string} ({item.index}/{total_plan_items}).",
        obj=executor.sync,
    )

    if active_branch.get() is not None or current_request.get() is not None:
        raise RuntimeError(
            "Fast baseline requires no active branch or request context."
        )
    from .fast_baseline_models import (
        FAST_BASELINE_SET_BASED_MODELS,
        fast_baseline_apply_upserts,
    )

    used_bulk_spec = fast_baseline_apply_upserts(
        runner,
        item.model_string,
        item.upsert_rows,
    )
    model_engines = getattr(executor, "_fast_baseline_model_engines", None)
    if model_engines is None:
        model_engines = executor._fast_baseline_model_engines = {}
    if used_bulk_spec:
        model_engines[item.model_string] = "fast_baseline_set_based"
    else:
        if item.model_string in FAST_BASELINE_SET_BASED_MODELS:
            raise RuntimeError(
                f"Fast baseline dependency resolution failed for {item.model_string}."
            )
        model_engines[item.model_string] = "normal_apply_engine"
        engine = select_apply_engine(sync=executor.sync, model_string=item.model_string)
        engine.apply_upserts(runner, item.model_string, item.upsert_rows)
        if engine.name == ForwardApplyEngineChoices.ADAPTER:
            adapter_counts = dict(
                getattr(ingestion, "_fast_baseline_adapter_row_counts", {}) or {}
            )
            adapter_counts[item.model_string] = int(
                adapter_counts.get(item.model_string, 0)
            ) + len(item.upsert_rows)
            ingestion._fast_baseline_adapter_row_counts = adapter_counts
    if item.delete_rows:
        from .workload_normalization import (
            CVE_WITHOUT_IN_SCOPE_VULNERABILITY_DELETE_CONTRACT,
        )

        if not (
            item.model_string == "netbox_dlm.cve"
            and item.sync_mode == "full"
            and item.derived_delete_contract
            == CVE_WITHOUT_IN_SCOPE_VULNERABILITY_DELETE_CONTRACT
            and int(item.derived_delete_count or 0) == len(item.delete_rows)
        ):
            raise RuntimeError("Fast baseline cannot apply unproved deletes.")
        omitted = len(item.delete_rows)
        executor.logger.increment_statistics(
            item.model_string,
            outcome="skipped",
            amount=omitted,
        )
        ingestion._fast_baseline_omitted_proven_noop_deletes = (
            int(getattr(ingestion, "_fast_baseline_omitted_proven_noop_deletes", 0))
            + omitted
        )
    if item.model_string == "dcim.device":
        from .ownership import record_device_identity_candidates

        record_device_identity_candidates(
            ingestion,
            getattr(runner, "_device_identity_candidates", set()),
        )
