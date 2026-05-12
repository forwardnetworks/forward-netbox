from bisect import bisect_right
from dataclasses import replace
from ipaddress import ip_interface
from ipaddress import ip_network

from .query_registry import ipaddress_unassignable_diagnostic_query
from .query_registry import IPADDRESS_UNASSIGNABLE_DIAGNOSTIC_QUERY_NAME
from .query_registry import routing_import_diagnostic_query
from .query_registry import ROUTING_IMPORT_DIAGNOSTIC_QUERY_NAME

IPADDRESS_DIAGNOSTIC_DETAIL_LIMIT = 20
IPADDRESS_DIAGNOSTIC_LABELS = {
    "ipv4-subnet-network-id": "IPv4 subnet network IDs",
    "ipv4-broadcast-address": "IPv4 broadcast addresses",
    "ipv6-subnet-network-id": "IPv6 subnet network IDs",
}
IPADDRESS_PARENT_PREFIX_DETAIL_LIMIT = 20
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


def append_ipaddress_parent_prefix_diagnostics(fetcher, workloads):
    enabled_models = set(fetcher.sync.get_model_strings())
    if not {"ipam.ipaddress", "ipam.prefix"}.issubset(enabled_models):
        return

    ip_workloads = []
    prefix_workloads = []
    for workload in workloads:
        if workload.model_string == "ipam.ipaddress":
            ip_workloads.append(workload)
        elif workload.model_string == "ipam.prefix":
            prefix_workloads.append(workload)
    if not ip_workloads or not prefix_workloads:
        return
    if any(
        workload.sync_mode != "full" for workload in [*ip_workloads, *prefix_workloads]
    ):
        return

    ip_rows = [row for workload in ip_workloads for row in workload.upsert_rows]
    prefix_rows = [row for workload in prefix_workloads for row in workload.upsert_rows]

    diagnostic = summarize_ipaddress_parent_prefix_rows(
        ip_rows,
        prefix_rows,
    )
    if diagnostic["total"] <= 0:
        return

    fetcher.logger.log_warning(
        "Forward IP Addresses found "
        f"{diagnostic['total']} rows without an imported covering prefix.",
        obj=fetcher.sync,
    )
    for example in diagnostic["examples"]:
        fetcher.logger.log_warning(
            "IP address without imported parent prefix "
            f"`{example['address']}` on `{example['device']}` "
            f"`{example['interface']}` (vrf `{example['vrf']}`).",
            obj=fetcher.sync,
        )
    suppressed = diagnostic.get("suppressed_examples", 0)
    if suppressed > 0:
        fetcher.logger.log_warning(
            "Suppressed "
            f"{suppressed} additional parent-prefix diagnostic examples after "
            f"the first {IPADDRESS_PARENT_PREFIX_DETAIL_LIMIT}.",
            obj=fetcher.sync,
        )

    fetcher.model_results = [
        (
            replace(result, diagnostics=[*result.diagnostics, diagnostic])
            if result.model_string == "ipam.ipaddress"
            else result
        )
        for result in fetcher.model_results
    ]


def summarize_ipaddress_parent_prefix_rows(
    ip_rows: list[dict],
    prefix_rows: list[dict],
) -> dict:
    prefix_intervals: dict[tuple[str, int], list[tuple[int, int]]] = {}
    for row in prefix_rows:
        try:
            prefix = ip_network(str(row.get("prefix") or ""), strict=False)
        except ValueError:
            continue
        key = (str(row.get("vrf") or ""), prefix.version)
        prefix_intervals.setdefault(key, []).append(
            (int(prefix.network_address), int(prefix.broadcast_address))
        )
    merged_intervals = {
        key: _merge_ip_intervals(intervals)
        for key, intervals in prefix_intervals.items()
    }
    interval_starts = {
        key: [start for start, _end in intervals]
        for key, intervals in merged_intervals.items()
    }

    counts = {"ipv4": 0, "ipv6": 0}
    examples: list[dict[str, str]] = []
    for row in ip_rows:
        try:
            address = ip_interface(str(row.get("address") or ""))
        except ValueError:
            continue
        key = (str(row.get("vrf") or ""), address.version)
        intervals = merged_intervals.get(key, [])
        starts = interval_starts.get(key, [])
        if _ip_is_in_intervals(int(address.ip), starts, intervals):
            continue
        version = "ipv4" if address.version == 4 else "ipv6"
        counts[version] += 1
        if len(examples) >= IPADDRESS_PARENT_PREFIX_DETAIL_LIMIT:
            continue
        examples.append(
            {
                "device": str(row.get("device") or ""),
                "interface": str(row.get("interface") or ""),
                "vrf": str(row.get("vrf") or "global"),
                "address": str(row.get("address") or ""),
            }
        )

    total = sum(counts.values())
    return {
        "name": "ipaddress_missing_parent_prefix",
        "query_name": "Forward IP Address Parent Prefix Diagnostics",
        "total": total,
        "counts": {key: value for key, value in counts.items() if value},
        "examples": examples,
        "suppressed_examples": max(total - len(examples), 0),
    }


def _merge_ip_intervals(intervals: list[tuple[int, int]]) -> list[tuple[int, int]]:
    merged = []
    for start, end in sorted(intervals):
        if not merged or start > merged[-1][1] + 1:
            merged.append((start, end))
            continue
        merged[-1] = (merged[-1][0], max(merged[-1][1], end))
    return merged


def _ip_is_in_intervals(
    value: int,
    starts: list[int],
    intervals: list[tuple[int, int]],
) -> bool:
    if not intervals:
        return False
    index = bisect_right(starts, value) - 1
    return index >= 0 and value <= intervals[index][1]


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
