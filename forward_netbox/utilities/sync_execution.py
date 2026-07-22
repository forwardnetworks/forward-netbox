from rq.timeouts import JobTimeoutException

from ..choices import ForwardDiffFallbackModeChoices
from ..exceptions import ForwardClientError
from ..exceptions import ForwardConnectivityError
from ..exceptions import ForwardQueryError
from ..exceptions import ForwardSyncDataError
from .apply_engine import select_apply_engine
from .delete_policy import should_suppress_aci_deletes
from .diagnostics import safe_operation_failure
from .forward_api import LATEST_COLLECTED_SNAPSHOT
from .ingestion_merge import suppress_ingest_side_effect_signals
from .model_contracts import architecture_default_coalesce_fields_for_model
from .query_registry import get_query_specs
from .query_registry import resolve_query_specs_for_client
from .sync_contracts import validate_row_shape_for_model


def run_sync_stage(runner):
    runner.logger.log_info("Starting Forward ingestion sync stage.", obj=runner.sync)
    network_id = runner.sync.get_network_id()
    snapshot_selector = runner.sync.get_snapshot_id()
    snapshot_id = runner.sync.resolve_snapshot_id(runner.client)
    query_parameters = runner.sync.get_query_parameters()
    maps = runner.sync.get_maps()

    if not network_id:
        raise ForwardQueryError(
            "Forward sync requires a network ID on the sync or its source."
        )
    if not snapshot_id:
        raise ForwardQueryError(
            "Forward sync requires a snapshot ID for NQE execution."
        )

    snapshot_info = {}
    # latestCollected resolves to a concrete snapshot that is not the
    # latestProcessed snapshot, so look its metadata up by id alongside a fixed
    # selector. Only the plain latestProcessed selector reads the latest
    # processed snapshot directly.
    if (
        snapshot_selector == snapshot_id
        or snapshot_selector == LATEST_COLLECTED_SNAPSHOT
    ):
        for snapshot in runner.client.get_snapshots(network_id):
            if snapshot["id"] == snapshot_id:
                snapshot_info = {
                    "id": snapshot["id"],
                    "state": snapshot.get("state") or "",
                    "createdAt": snapshot.get("created_at") or "",
                    "processedAt": snapshot.get("processed_at") or "",
                }
                break
    else:
        snapshot_info = runner.client.get_latest_processed_snapshot(network_id)

    snapshot_metrics = {}
    try:
        snapshot_metrics = runner.client.get_snapshot_metrics(snapshot_id)
    except JobTimeoutException:
        raise
    except Exception as exc:
        runner.logger.log_warning(
            safe_operation_failure("Forward snapshot metrics fetch", exc),
            obj=runner.sync,
        )

    runner.ingestion.snapshot_selector = snapshot_selector
    runner.ingestion.snapshot_id = snapshot_id
    runner.ingestion.snapshot_info = snapshot_info or {}
    runner.ingestion.snapshot_metrics = snapshot_metrics or {}
    runner.ingestion.save(
        update_fields=[
            "snapshot_selector",
            "snapshot_id",
            "snapshot_info",
            "snapshot_metrics",
        ]
    )

    runner.logger.log_info(
        f"Using snapshot `{snapshot_id}` for network `{network_id}`.",
        obj=runner.sync,
    )
    baseline_ingestion = runner.sync.latest_baseline_ingestion(
        exclude_ingestion_id=runner.ingestion.pk
    )
    if baseline_ingestion is None:
        runner.logger.log_info(
            "No eligible diff baseline exists yet; this run will establish the first baseline if it completes without issues.",
            obj=runner.sync,
        )
    else:
        runner.logger.log_info(
            f"Latest eligible diff baseline is ingestion `{baseline_ingestion.pk}` on snapshot `{baseline_ingestion.snapshot_id}`.",
            obj=runner.sync,
        )

    pending_deletes: dict[str, list[dict]] = {}
    used_full = False
    used_diff = False

    with suppress_ingest_side_effect_signals():
        for model_string in runner.sync.get_model_strings():
            runner.logger.log_info(
                f"Starting model ingestion for {model_string}.", obj=runner.sync
            )
            try:
                specs = get_query_specs(model_string, maps=maps)
                specs = resolve_query_specs_for_client(specs, runner.client)
                if specs:
                    runner._model_coalesce_fields[model_string] = [
                        list(field_set) for field_set in specs[0].coalesce_fields
                    ] or architecture_default_coalesce_fields_for_model(model_string)
                else:
                    runner._model_coalesce_fields[model_string] = (
                        architecture_default_coalesce_fields_for_model(model_string)
                    )
                runner.logger.init_statistics(model_string, 0)
                model_delete_rows = pending_deletes.setdefault(model_string, [])
                latest_baseline = runner.sync.latest_baseline_ingestion(
                    exclude_ingestion_id=runner.ingestion.pk
                )
                model_baseline = runner.sync.incremental_diff_baseline(
                    specs=specs,
                    current_snapshot_id=snapshot_id,
                    exclude_ingestion_id=runner.ingestion.pk,
                    client=runner.client,
                )
                if (
                    latest_baseline is not None
                    and latest_baseline.snapshot_id == snapshot_id
                    and any(spec.run_query_id for spec in specs)
                ):
                    runner.logger.log_info(
                        f"Forward diffs require a newer processed snapshot than the latest baseline; "
                        f"baseline ingestion `{latest_baseline.pk}` already matches snapshot `{snapshot_id}`, "
                        f"so running full query execution for {model_string} instead.",
                        obj=runner.sync,
                    )
                for spec in specs:
                    rows = []
                    delete_rows = []
                    effective_parameters = spec.merged_parameters(query_parameters)
                    if model_baseline is not None and spec.run_query_id:
                        if effective_parameters:
                            if (runner.sync.parameters or {}).get(
                                "diff_fallback_mode",
                                ForwardDiffFallbackModeChoices.ALLOW_FALLBACK,
                            ) == ForwardDiffFallbackModeChoices.REQUIRE_DIFF:
                                raise ForwardQueryError(
                                    "Diff execution is required, but Forward NQE diffs "
                                    "do not accept runtime query parameters for "
                                    f"{model_string} using `{spec.execution_value}`. "
                                    "Use Allow full fallback for parameterized maps."
                                )
                            runner.logger.log_info(
                                "Forward NQE diffs do not accept runtime query "
                                f"parameters for {model_string} using "
                                f"`{spec.execution_value}`; running full async "
                                "query execution instead.",
                                obj=runner.sync,
                            )
                            model_baseline = None
                        else:
                            try:
                                runner.logger.log_info(
                                    f"Running Forward NQE diff `{spec.execution_value}` for {model_string} "
                                    f"between snapshots `{model_baseline.snapshot_id}` and `{snapshot_id}`.",
                                    obj=runner.sync,
                                )
                                diff_rows = runner.client.run_nqe_diff(
                                    query_id=spec.run_query_id,
                                    commit_id=spec.commit_id,
                                    before_snapshot_id=model_baseline.snapshot_id,
                                    after_snapshot_id=snapshot_id,
                                    fetch_all=True,
                                )
                                rows, delete_rows = runner._split_diff_rows(
                                    model_string, diff_rows
                                )
                                used_diff = True
                                runner.logger.log_info(
                                    f"Fetched {len(diff_rows)} diff rows for {model_string} from query_id `{spec.execution_value}`.",
                                    obj=runner.sync,
                                )
                            except (
                                ForwardClientError,
                                ForwardConnectivityError,
                            ) as exc:
                                runner.logger.log_warning(
                                    safe_operation_failure(
                                        f"Forward NQE diff for {model_string}", exc
                                    )
                                    + " Falling back to full query execution.",
                                    obj=runner.sync,
                                )
                                model_baseline = None

                    if model_baseline is None or not spec.run_query_id:
                        if model_baseline is not None and not spec.run_query_id:
                            runner.logger.log_warning(
                                f"Forward diffs require a query_id; `{spec.execution_value}` is still raw query text, so running a full query for {model_string} instead.",
                                obj=runner.sync,
                            )
                        runner.logger.log_info(
                            f"Running Forward {spec.execution_mode} `{spec.execution_value}` for {model_string}.",
                            obj=runner.sync,
                        )
                        rows = runner.client.run_nqe_query(
                            query=spec.query,
                            query_id=spec.run_query_id,
                            commit_id=spec.commit_id,
                            network_id=network_id,
                            snapshot_id=snapshot_id,
                            parameters=effective_parameters,
                            fetch_all=True,
                        )
                        used_full = True
                        runner.logger.log_info(
                            f"Fetched {len(rows)} rows for {model_string} from {spec.execution_mode} `{spec.execution_value}`.",
                            obj=runner.sync,
                        )

                    for row in rows:
                        validate_row_shape_for_model(
                            model_string,
                            row,
                            runner._model_coalesce_fields[model_string],
                        )
                    for row in delete_rows:
                        validate_row_shape_for_model(
                            model_string,
                            row,
                            runner._model_coalesce_fields[model_string],
                        )
                    runner.logger.add_statistics_total(
                        model_string, len(rows) + len(delete_rows)
                    )
                    engine = select_apply_engine(
                        sync=runner.sync,
                        model_string=model_string,
                    )
                    engine.apply_upserts(runner, model_string, rows)
                    model_delete_rows.extend(delete_rows)
                stats = runner.logger.log_data.get("statistics", {}).get(
                    model_string, {}
                )
                runner.logger.log_info(
                    f"Completed {model_string}: applied={stats.get('applied', 0)} failed={stats.get('failed', 0)} skipped={stats.get('skipped', 0)} total={stats.get('total', 0)}.",
                    obj=runner.sync,
                )
            except ForwardQueryError as exc:
                runner._record_issue(
                    model_string,
                    str(exc),
                    {},
                    exception=exc,
                )
                runner.logger.log_warning(
                    safe_operation_failure(f"Validation for {model_string}", exc),
                    obj=runner.sync,
                )
                continue
            except ForwardSyncDataError as exc:
                runner.logger.log_warning(
                    safe_operation_failure(f"Row processing for {model_string}", exc),
                    obj=runner.sync,
                )
                continue

        for model_string in reversed(runner.sync.get_model_strings()):
            delete_rows = pending_deletes.get(model_string, [])
            if not delete_rows:
                continue
            if should_suppress_aci_deletes(runner.sync, model_string):
                runner.logger.log_warning(
                    f"Held back {len(delete_rows)} delete(s) for {model_string}: "
                    "ACI inventory is not auto-pruned because a failed APIC "
                    "collection empties the fabric query and would delete real "
                    "objects. Fix Forward collection, or set aci_allow_deletes to "
                    "apply ACI deletes.",
                    obj=runner.sync,
                )
                continue
            try:
                engine = select_apply_engine(
                    sync=runner.sync,
                    model_string=model_string,
                )
                engine.apply_deletes(runner, model_string, delete_rows)
            except ForwardSyncDataError as exc:
                runner.logger.log_warning(
                    safe_operation_failure(
                        f"Delete processing for {model_string}", exc
                    ),
                    obj=runner.sync,
                )
                continue

    if used_diff and used_full:
        runner.ingestion.sync_mode = "hybrid"
    elif used_diff:
        runner.ingestion.sync_mode = "diff"
    else:
        runner.ingestion.sync_mode = "full"
    runner.ingestion.save(update_fields=["sync_mode"])
    runner.logger.log_info("Finished Forward ingestion sync stage.", obj=runner.sync)
