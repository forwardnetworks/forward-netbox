from dataclasses import replace
from typing import Any

from .query_registry import IPADDRESS_UNASSIGNABLE_DIAGNOSTIC_QUERY_NAME
from .query_registry import ROUTING_IMPORT_DIAGNOSTIC_QUERY_NAME
from .query_registry import ipaddress_unassignable_diagnostic_query
from .query_registry import routing_import_diagnostic_query

IPADDRESS_DIAGNOSTIC_DETAIL_LIMIT = 20
IPADDRESS_DIAGNOSTIC_LABELS = {
    "ipv4-subnet-network-id": "IPv4 subnet network IDs",
    "ipv4-broadcast-address": "IPv4 broadcast addresses",
    "ipv6-subnet-network-id": "IPv6 subnet network IDs",
}
ROUTING_DIAGNOSTIC_DETAIL_LIMIT = 20
ROUTING_DIAGNOSTIC_LABELS = {
    "bgp-neighbor-without-local-as": "BGP neighbors without local AS",
    "bgp-unsupported-address-family": "BGP unsupported address families",
    "ospf-neighbor-without-remote-peer": "OSPF neighbors without inferred remote peers",
    "ospf-neighbor-without-reverse-peer": "OSPF neighbors without reverse peer inference",
}
ROUTING_DIAGNOSTIC_MODELS = {
    "netbox_routing.bgppeer",
    "netbox_routing.bgpaddressfamily",
    "netbox_routing.bgppeeraddressfamily",
    "netbox_routing.ospfinstance",
    "netbox_routing.ospfarea",
    "netbox_routing.ospfinterface",
    "netbox_peering_manager.peeringsession",
}


def append_ipaddress_diagnostics(fetcher, context):
    if "ipam.ipaddress" not in fetcher.sync.get_model_strings():
        return
    diagnostic = run_ipaddress_unassignable_diagnostic(fetcher, context)
    if not diagnostic:
        return
    fetcher.model_results = [
        (
            replace(result, diagnostics=[*result.diagnostics, diagnostic])
            if result.model_string == "ipam.ipaddress"
            else result
        )
        for result in fetcher.model_results
    ]


def run_ipaddress_unassignable_diagnostic(fetcher, context):
    try:
        rows = fetcher.client.run_nqe_query(
            query=ipaddress_unassignable_diagnostic_query(),
            network_id=context.network_id,
            snapshot_id=context.snapshot_id,
            parameters=context.query_parameters,
            fetch_all=True,
        )
    except Exception as exc:
        fetcher.logger.log_warning(
            "Unable to run Forward IP address assignment diagnostics; "
            f"filtered address counts will not be reported: {exc}",
            obj=fetcher.sync,
        )
        return None

    diagnostic = summarize_unassignable_ipaddress_rows(rows)
    if diagnostic["total"] <= 0:
        return None

    count_summary = ", ".join(
        f"{IPADDRESS_DIAGNOSTIC_LABELS.get(reason, reason)}={count}"
        for reason, count in sorted(diagnostic["counts"].items())
    )
    fetcher.logger.log_warning(
        "Forward IP Addresses filtered "
        f"{diagnostic['total']} interface addresses that NetBox cannot assign: "
        f"{count_summary}.",
        obj=fetcher.sync,
    )
    for example in diagnostic["examples"]:
        fetcher.logger.log_warning(
            "Filtered unassignable IP address "
            f"`{example['address']}` on `{example['device']}` "
            f"`{example['interface']}` ({example['reason']}).",
            obj=fetcher.sync,
        )
    suppressed = diagnostic["total"] - len(diagnostic["examples"])
    if suppressed > 0:
        fetcher.logger.log_warning(
            "Suppressed "
            f"{suppressed} additional filtered IP address diagnostic examples "
            f"after the first {IPADDRESS_DIAGNOSTIC_DETAIL_LIMIT}.",
            obj=fetcher.sync,
        )
    return diagnostic


def summarize_unassignable_ipaddress_rows(rows: list[dict]) -> dict:
    counts: dict[str, int] = {}
    examples: list[dict[str, str]] = []
    for row in rows:
        reason = str(row.get("reason") or "unknown")
        counts[reason] = counts.get(reason, 0) + 1
        if len(examples) >= IPADDRESS_DIAGNOSTIC_DETAIL_LIMIT:
            continue
        examples.append(
            {
                "reason": IPADDRESS_DIAGNOSTIC_LABELS.get(reason, reason),
                "device": str(row.get("device") or ""),
                "interface": str(row.get("interface") or ""),
                "address": str(row.get("address") or ""),
            }
        )
    return {
        "name": "unassignable_interface_addresses",
        "query_name": IPADDRESS_UNASSIGNABLE_DIAGNOSTIC_QUERY_NAME,
        "total": sum(counts.values()),
        "counts": counts,
        "examples": examples,
    }


def append_routing_diagnostics(fetcher, context):
    enabled_models = set(fetcher.sync.get_model_strings())
    target_models = enabled_models & ROUTING_DIAGNOSTIC_MODELS
    if not target_models:
        return
    diagnostic = run_routing_import_diagnostic(fetcher, context)
    if not diagnostic:
        return
    fetcher.model_results = [
        (
            replace(result, diagnostics=[*result.diagnostics, diagnostic])
            if result.model_string in target_models
            else result
        )
        for result in fetcher.model_results
    ]


def run_routing_import_diagnostic(fetcher, context):
    try:
        rows = fetcher.client.run_nqe_query(
            query=routing_import_diagnostic_query(),
            network_id=context.network_id,
            snapshot_id=context.snapshot_id,
            parameters=context.query_parameters,
            fetch_all=True,
        )
    except Exception as exc:
        fetcher.logger.log_warning(
            "Unable to run Forward routing import diagnostics; skipped routing "
            f"row counts will not be reported: {exc}",
            obj=fetcher.sync,
        )
        return None

    diagnostic = summarize_routing_import_diagnostic_rows(rows)
    if diagnostic["total"] <= 0:
        return None

    count_summary = ", ".join(
        f"{ROUTING_DIAGNOSTIC_LABELS.get(reason, reason)}={count}"
        for reason, count in sorted(diagnostic["counts"].items())
    )
    fetcher.logger.log_warning(
        "Forward routing diagnostics found "
        f"{diagnostic['total']} rows that the beta routing maps cannot import: "
        f"{count_summary}.",
        obj=fetcher.sync,
    )
    for example in diagnostic["examples"]:
        fetcher.logger.log_warning(
            "Routing diagnostic "
            f"`{example['reason']}` for `{example['device']}` "
            f"`{example['interface']}` ({example['detail']}).",
            obj=fetcher.sync,
        )
    suppressed = diagnostic.get("suppressed_examples", 0)
    if suppressed > 0:
        fetcher.logger.log_warning(
            "Suppressed "
            f"{suppressed} additional routing diagnostic examples after the "
            f"first {ROUTING_DIAGNOSTIC_DETAIL_LIMIT}.",
            obj=fetcher.sync,
        )
    return diagnostic


def summarize_routing_import_diagnostic_rows(rows: list[dict]) -> dict:
    counts: dict[str, int] = {}
    examples: list[dict[str, str]] = []
    for row in rows:
        reason = str(row.get("reason") or "unknown")
        count = diagnostic_row_count(row)
        counts[reason] = counts.get(reason, 0) + count
        if len(examples) >= ROUTING_DIAGNOSTIC_DETAIL_LIMIT:
            continue
        examples.append(
            {
                "reason": ROUTING_DIAGNOSTIC_LABELS.get(reason, reason),
                "model_target": str(row.get("model_target") or ""),
                "protocol": str(row.get("protocol") or ""),
                "device": str(row.get("device") or ""),
                "interface": str(row.get("interface") or ""),
                "detail": str(row.get("detail") or ""),
            }
        )
    return {
        "name": "routing_import_skipped_rows",
        "query_name": ROUTING_IMPORT_DIAGNOSTIC_QUERY_NAME,
        "total": sum(counts.values()),
        "counts": counts,
        "examples": examples,
        "suppressed_examples": max(len(rows) - len(examples), 0),
    }


def diagnostic_row_count(row: dict) -> int:
    try:
        count = int(row.get("count") or 1)
    except (TypeError, ValueError):
        return 1
    return max(count, 1)
