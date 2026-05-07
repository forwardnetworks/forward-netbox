from ..choices import ForwardIngestionPhaseChoices
from ..exceptions import ForwardClientError
from ..exceptions import ForwardConnectivityError
from ..exceptions import ForwardQueryError
from ..exceptions import ForwardSyncDataError
from .query_registry import get_query_specs
from .sync_contracts import default_coalesce_fields_for_model
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
    if snapshot_selector == snapshot_id:
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
    except Exception as exc:
        runner.logger.log_warning(
            f"Unable to fetch Forward snapshot metrics for `{snapshot_id}`: {exc}",
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

    for model_string in runner.sync.get_model_strings():
        runner.logger.log_info(
            f"Starting model ingestion for {model_string}.", obj=runner.sync
        )
        try:
            specs = get_query_specs(model_string, maps=maps)
            if specs:
                runner._model_coalesce_fields[model_string] = [
                    list(field_set) for field_set in specs[0].coalesce_fields
                ] or default_coalesce_fields_for_model(model_string)
            else:
                runner._model_coalesce_fields[model_string] = (
                    default_coalesce_fields_for_model(model_string)
                )
            runner.logger.init_statistics(model_string, 0)
            model_delete_rows = pending_deletes.setdefault(model_string, [])
            model_baseline = runner.sync.incremental_diff_baseline(
                specs=specs,
                current_snapshot_id=snapshot_id,
                exclude_ingestion_id=runner.ingestion.pk,
            )
            for spec in specs:
                rows = []
                delete_rows = []
                if model_baseline is not None and spec.query_id:
                    try:
                        runner.logger.log_info(
                            f"Running Forward NQE diff `{spec.execution_value}` for {model_string} "
                            f"between snapshots `{model_baseline.snapshot_id}` and `{snapshot_id}`.",
                            obj=runner.sync,
                        )
                        diff_rows = runner.client.run_nqe_diff(
                            query_id=spec.query_id,
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
                    except (ForwardClientError, ForwardConnectivityError) as exc:
                        runner.logger.log_warning(
                            f"Forward NQE diff failed for {model_string} using `{spec.execution_value}`; falling back to full query execution: {exc}",
                            obj=runner.sync,
                        )
                        model_baseline = None

                if model_baseline is None or not spec.query_id:
                    runner.logger.log_info(
                        f"Running Forward {spec.execution_mode} `{spec.execution_value}` for {model_string}.",
                        obj=runner.sync,
                    )
                    rows = runner.client.run_nqe_query(
                        query=spec.query,
                        query_id=spec.query_id,
                        commit_id=spec.commit_id,
                        network_id=network_id,
                        snapshot_id=snapshot_id,
                        parameters=spec.merged_parameters(query_parameters),
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
                runner._apply_model_rows(model_string, rows)
                model_delete_rows.extend(delete_rows)
            stats = runner.logger.log_data.get("statistics", {}).get(model_string, {})
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
                f"Aborted {model_string} due to validation failure: {exc}",
                obj=runner.sync,
            )
            continue
        except ForwardSyncDataError as exc:
            runner.logger.log_warning(
                f"Aborted {model_string} after row failure: {exc}",
                obj=runner.sync,
            )
            continue

    for model_string in reversed(runner.sync.get_model_strings()):
        delete_rows = pending_deletes.get(model_string, [])
        if not delete_rows:
            continue
        try:
            runner._delete_model_rows(model_string, delete_rows)
        except ForwardSyncDataError as exc:
            runner.logger.log_warning(
                f"Aborted delete phase for {model_string} after row failure: {exc}",
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
