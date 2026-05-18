from collections import Counter

from .execution_ledger import latest_execution_run
from .forward_api import LATEST_PROCESSED_SNAPSHOT
from .health_apply_fetch import apply_engine_summary as _apply_engine_summary_impl
from .health_apply_fetch import fetch_contract_summary as _fetch_contract_summary_impl
from .health_apply_fetch import model_summary as _model_summary_impl
from .health_checks import capacity_check_summary as _capacity_check_impl
from .health_checks import check as _check_impl
from .health_checks import health_checks as _health_checks_impl
from .health_checks import ingestion_check_message as _ingestion_check_message_impl
from .health_checks import ingestion_check_status as _ingestion_check_status_impl
from .health_checks import query_drift_check_message as _query_drift_check_message_impl
from .health_checks import query_drift_check_status as _query_drift_check_status_impl
from .health_checks import (
    query_fetch_concurrency_check as _query_fetch_concurrency_check_impl,
)
from .health_checks import recommendation_status as _recommendation_status_impl
from .health_checks import timeout_check as _timeout_check_impl
from .health_checks import validation_check_message as _validation_check_message_impl
from .health_checks import validation_check_status as _validation_check_status_impl
from .health_summary_blocks import capacity_message as _capacity_message_impl
from .health_summary_blocks import capacity_summary as _capacity_summary_impl
from .health_summary_blocks import execution_run_summary as _execution_run_summary_impl
from .health_summary_blocks import ingestion_summary as _ingestion_summary_impl
from .health_summary_blocks import query_map_summary as _query_map_summary_impl
from .health_summary_blocks import runtime_summary as _runtime_summary_impl
from .health_summary_blocks import source_summary as _source_summary_impl
from .health_summary_blocks import step_duration_seconds as _step_duration_seconds_impl
from .health_summary_blocks import validation_summary as _validation_summary_impl
from .query_binding import local_query_binding_drift
from .sync_facade import resolve_snapshot_id


DATA_FILE_HINTS = (
    "data file",
    "netbox_device_type_aliases",
    "netbox_feature_tag_rules",
    "with netbox",
    "with rules",
)
DATA_FILE_PROBES = {
    "netbox_device_type_aliases": {
        "label": "NetBox Device Type Aliases",
        "extension": "netbox_device_type_aliases",
        "hints": (
            "netbox_device_type_aliases",
            "device type aliases",
            "netbox aliases",
            "with netbox",
        ),
    },
    "netbox_feature_tag_rules": {
        "label": "NetBox Feature Tag Rules",
        "extension": "netbox_feature_tag_rules",
        "hints": (
            "netbox_feature_tag_rules",
            "feature tag rules",
            "with rules",
        ),
    },
}


def sync_health_summary(sync):
    maps = [
        query_map
        for query_map in sync.get_maps()
        if sync.is_model_enabled(query_map.model_string)
    ]
    latest_ingestion = sync.last_ingestion
    validation_run = sync.latest_validation_run
    execution_run = latest_execution_run(sync)
    capacity_summary = _capacity_summary(execution_run)
    query_mode_counts = Counter(query_map.execution_mode for query_map in maps)
    raw_maps = [query_map for query_map in maps if query_map.execution_mode == "query"]
    data_file_maps = [
        query_map for query_map in maps if _looks_data_file_dependent(query_map)
    ]
    model_summary = _model_summary(sync, maps)
    apply_engines = _apply_engine_summary(sync, model_summary["enabled_models"])
    fetch_contracts = _fetch_contract_summary(model_summary["enabled_models"])
    query_drift = [local_query_binding_drift(query_map) for query_map in maps]
    next_run = _next_run_expectation(sync, maps, raw_maps)
    checks = _health_checks(
        sync=sync,
        maps=maps,
        model_summary=model_summary,
        query_drift=query_drift,
        raw_maps=raw_maps,
        data_file_maps=data_file_maps,
        validation_run=validation_run,
        latest_ingestion=latest_ingestion,
        execution_run=execution_run,
        capacity_summary=capacity_summary,
        next_run=next_run,
    )

    return {
        "source": _source_summary(sync),
        "runtime": _runtime_summary(sync),
        "models": model_summary,
        "apply_engines": apply_engines,
        "fetch_contracts": fetch_contracts,
        "query_modes": {
            "query": query_mode_counts.get("query", 0),
            "query_id": query_mode_counts.get("query_id", 0),
            "query_path": query_mode_counts.get("query_path", 0),
            "diff_capable": len(maps) - len(raw_maps),
            "maps": [_query_map_summary(query_map) for query_map in maps],
            "raw_query_maps": [_query_map_summary(query_map) for query_map in raw_maps],
            "data_file_maps": [
                _query_map_summary(query_map) for query_map in data_file_maps
            ],
            "local_drift": query_drift,
        },
        "latest_validation": _validation_summary(validation_run),
        "latest_ingestion": _ingestion_summary(latest_ingestion),
        "latest_execution_run": _execution_run_summary(execution_run),
        "capacity": capacity_summary,
        "next_run": next_run,
        "checks": checks,
    }


def live_source_health_check(sync):
    source = sync.source
    parameters = source.parameters or {}
    configured_network_id = str(parameters.get("network_id") or "").strip()
    result = {
        "source": {
            "id": source.pk,
            "name": source.name,
            "status": source.status,
            "url": source.url,
            "type": source.type,
        },
        "reachable": False,
        "configured_network_id_present": bool(configured_network_id),
        "configured_network_visible": None,
        "network_count": None,
        "latest_processed_snapshot_available": None,
        "checks": [],
    }
    try:
        client = source.get_client()
        networks = client.get_networks()
    except Exception as exc:
        result["checks"].append(
            _check(
                name="Forward API reachability",
                status="fail",
                message=f"Forward API lookup failed: {exc}",
            )
        )
        return result

    result["reachable"] = True
    result["network_count"] = len(networks)
    result["checks"].append(
        _check(
            name="Forward API reachability",
            status="pass",
            message=f"Forward API returned {len(networks)} visible network(s).",
        )
    )
    if not configured_network_id:
        result["checks"].append(
            _check(
                name="Configured network",
                status="warn",
                message="No Forward network is configured on this source.",
            )
        )
        return result

    result["configured_network_visible"] = any(
        str(network.get("id") or "").strip() == configured_network_id
        for network in networks
    )
    result["checks"].append(
        _check(
            name="Configured network",
            status="pass" if result["configured_network_visible"] else "fail",
            message=(
                "Configured Forward network is visible to this source."
                if result["configured_network_visible"]
                else "Configured Forward network was not returned by Forward."
            ),
        )
    )
    if not result["configured_network_visible"]:
        return result

    try:
        client.get_latest_processed_snapshot_id(configured_network_id)
    except Exception as exc:
        result["latest_processed_snapshot_available"] = False
        result["checks"].append(
            _check(
                name="Latest processed snapshot",
                status="fail",
                message=f"latestProcessed snapshot lookup failed: {exc}",
            )
        )
        return result

    result["latest_processed_snapshot_available"] = True
    result["checks"].append(
        _check(
            name="Latest processed snapshot",
            status="pass",
            message="latestProcessed snapshot is available for the configured network.",
        )
    )
    return result


def live_data_file_health_check(sync):
    maps = [
        query_map
        for query_map in sync.get_maps()
        if sync.is_model_enabled(query_map.model_string)
    ]
    required_data_files = _required_data_files(maps)
    result = {
        "enabled_data_file_map_count": len(
            [query_map for query_map in maps if _looks_data_file_dependent(query_map)]
        ),
        "required_data_files": sorted(required_data_files),
        "snapshot_selector": sync.get_snapshot_id(),
        "checks": [],
        "results": [],
    }
    if not required_data_files:
        result["checks"].append(
            _check(
                name="Data-file freshness",
                status="pass",
                message="No enabled maps require Forward NQE data files.",
            )
        )
        return result

    network_id = sync.get_network_id()
    if not network_id:
        result["checks"].append(
            _check(
                name="Data-file freshness",
                status="fail",
                message="No Forward network is configured on this source.",
            )
        )
        return result

    client = sync.source.get_client()
    try:
        resolved_snapshot_id = resolve_snapshot_id(sync, client=client)
    except Exception as exc:
        result["checks"].append(
            _check(
                name="Data-file freshness",
                status="fail",
                message=f"Could not resolve selected Forward snapshot: {exc}",
            )
        )
        return result

    for data_file_name in sorted(required_data_files):
        probe = DATA_FILE_PROBES[data_file_name]
        probe_result = _probe_data_file(
            client=client,
            network_id=network_id,
            snapshot_id=resolved_snapshot_id,
            data_file_name=data_file_name,
            extension=probe["extension"],
            label=probe["label"],
        )
        result["results"].append(probe_result)

    missing = [item for item in result["results"] if item["status"] != "present"]
    result["checks"].append(
        _check(
            name="Data-file freshness",
            status="pass" if not missing else "warn",
            message=(
                "Every enabled data-file-backed map has rows visible in the selected snapshot."
                if not missing
                else (
                    f"{len(missing)} required data file(s) are missing, empty, or "
                    "not visible in the selected snapshot."
                )
            ),
        )
    )
    return result


def _required_data_files(query_maps):
    required = set()
    for query_map in query_maps:
        haystack = " ".join(
            str(value or "").lower()
            for value in (query_map.name, query_map.query_path, query_map.query)
        )
        for data_file_name, probe in DATA_FILE_PROBES.items():
            if any(hint in haystack for hint in probe["hints"]):
                required.add(data_file_name)
    return required


def _probe_data_file(
    *,
    client,
    network_id,
    snapshot_id,
    data_file_name,
    extension,
    label,
):
    query = _data_file_probe_query(data_file_name=data_file_name, extension=extension)
    try:
        rows = client.run_nqe_query(
            query=query,
            network_id=network_id,
            snapshot_id=snapshot_id,
            limit=1,
        )
    except Exception as exc:
        return {
            "data_file": data_file_name,
            "label": label,
            "status": "lookup_failed",
            "value_present": False,
            "row_count": None,
            "message": (
                "Forward NQE could not read this data-file extension from the "
                f"selected snapshot: {exc}"
            ),
        }

    row = rows[0] if rows else {}
    value_present = bool(row.get("value_present"))
    row_count = row.get("row_count")
    if value_present and row_count is not None and int(row_count) > 0:
        status = "present"
        message = f"{label} is visible in the selected snapshot."
    elif value_present:
        status = "empty"
        message = f"{label} is visible in the selected snapshot but has no rows."
    else:
        status = "not_captured"
        message = (
            f"{label} is defined but no value is captured in the selected snapshot; "
            "run or reprocess a Forward snapshot after uploading the data file."
        )
    return {
        "data_file": data_file_name,
        "label": label,
        "status": status,
        "value_present": value_present,
        "row_count": row_count,
        "message": message,
    }


def _data_file_probe_query(*, data_file_name, extension):
    return f"""
foreach x in fromTo(1, 1)
let data_file = network.extensions.{extension}
select {{
  data_file: "{data_file_name}",
  value_present: isPresent(data_file.value),
  row_count: if isPresent(data_file.value) then length(data_file.value) else 0
}}
""".strip()


def _source_summary(sync):
    return _source_summary_impl(sync)


def _runtime_summary(sync):
    return _runtime_summary_impl(sync)


def _model_summary(sync, maps):
    return _model_summary_impl(sync, maps)


def _apply_engine_summary(sync, model_strings):
    return _apply_engine_summary_impl(sync, model_strings)


def _fetch_contract_summary(model_strings):
    return _fetch_contract_summary_impl(model_strings)


def _query_map_summary(query_map):
    return _query_map_summary_impl(query_map)


def _validation_summary(validation_run):
    return _validation_summary_impl(validation_run)


def _ingestion_summary(ingestion):
    return _ingestion_summary_impl(ingestion)


def _execution_run_summary(run):
    return _execution_run_summary_impl(run)


def _capacity_summary(run):
    return _capacity_summary_impl(run)


def _step_duration_seconds(step):
    return _step_duration_seconds_impl(step)


def _capacity_message(run, *, average_seconds, max_seconds, remaining_steps):
    return _capacity_message_impl(
        run,
        average_seconds=average_seconds,
        max_seconds=max_seconds,
        remaining_steps=remaining_steps,
    )


def _next_run_expectation(sync, maps, raw_maps):
    reasons = []
    blockers = []
    if sync.get_snapshot_id() != LATEST_PROCESSED_SNAPSHOT:
        reasons.append("snapshot_selector_is_fixed")
        blockers.append(
            {
                "reason": "snapshot_selector_is_fixed",
                "scope": "sync",
                "message": (
                    "The sync is pinned to a fixed snapshot selector, so the next "
                    "run cannot be treated as a latestProcessed diff candidate."
                ),
            }
        )
    if not maps:
        reasons.append("no_enabled_maps")
        blockers.append(
            {
                "reason": "no_enabled_maps",
                "scope": "sync",
                "message": "No enabled NQE maps are available for this sync.",
            }
        )
    if raw_maps:
        reasons.append("raw_query_maps_cannot_use_forward_diffs")
        blockers.extend(
            {
                "reason": "raw_query_maps_cannot_use_forward_diffs",
                "scope": "map",
                "map": query_map.name,
                "model": query_map.model_string,
                "execution_mode": query_map.execution_mode,
                "message": (
                    "Raw query text maps cannot use Forward nqe-diffs; bind this "
                    "map by repository path or direct query ID for diff eligibility."
                ),
            }
            for query_map in raw_maps
        )

    baseline = sync.latest_baseline_ingestion()
    if baseline is None:
        reasons.append("no_baseline_ready_ingestion")
        blockers.append(
            {
                "reason": "no_baseline_ready_ingestion",
                "scope": "sync",
                "message": (
                    "No successful baseline-ready ingestion is available for this sync."
                ),
            }
        )

    if reasons:
        mode = "full_or_reconciliation"
        message = (
            "The next run is expected to use a full or reconciliation path before "
            "it can rely on Forward diffs."
        )
    else:
        mode = "diff_eligible"
        message = (
            "The next run is eligible to use Forward diffs if the resolved latest "
            "snapshot differs from the recorded baseline snapshot."
        )
    return {
        "mode": mode,
        "message": message,
        "reasons": reasons,
        "blockers": blockers,
        "baseline_ingestion": baseline.pk if baseline else None,
        "baseline_snapshot_id": baseline.snapshot_id if baseline else "",
    }


def _health_checks(
    *,
    sync,
    maps,
    model_summary,
    query_drift,
    raw_maps,
    data_file_maps,
    validation_run,
    latest_ingestion,
    execution_run,
    capacity_summary,
    next_run,
):
    return _health_checks_impl(
        sync=sync,
        maps=maps,
        model_summary=model_summary,
        query_drift=query_drift,
        raw_maps=raw_maps,
        data_file_maps=data_file_maps,
        validation_run=validation_run,
        latest_ingestion=latest_ingestion,
        execution_run=execution_run,
        capacity_summary=capacity_summary,
        next_run=next_run,
        branching_available_fn=_branching_available,
    )


def _query_fetch_concurrency_check(sync):
    return _query_fetch_concurrency_check_impl(sync)


def _query_drift_check_status(query_drift):
    return _query_drift_check_status_impl(query_drift)


def _query_drift_check_message(query_drift):
    return _query_drift_check_message_impl(query_drift)


def _branching_available():
    try:
        import netbox_branching  # noqa: F401
    except Exception:
        return False
    return True


def _validation_check_status(validation_run):
    return _validation_check_status_impl(validation_run)


def _validation_check_message(validation_run):
    return _validation_check_message_impl(validation_run)


def _ingestion_check_status(ingestion):
    return _ingestion_check_status_impl(ingestion)


def _ingestion_check_message(ingestion):
    return _ingestion_check_message_impl(ingestion)


def _recommendation_status(recommendation):
    return _recommendation_status_impl(recommendation)


def _timeout_check(sync):
    return _timeout_check_impl(sync)


def _capacity_check(sync, capacity_summary):
    return _capacity_check_impl(sync, capacity_summary)


def _looks_data_file_dependent(query_map):
    haystack = " ".join(
        str(value or "").lower()
        for value in (
            query_map.name,
            query_map.query_path,
            query_map.query,
        )
    )
    return any(hint in haystack for hint in DATA_FILE_HINTS)


def _check(*, name, status, message):
    return _check_impl(name=name, status=status, message=message)
