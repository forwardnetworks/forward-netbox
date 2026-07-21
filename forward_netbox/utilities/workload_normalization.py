from collections import defaultdict
from dataclasses import replace

from .branch_budget import BranchWorkload
from .sync_contracts import canonical_cable_endpoint_identity
from .sync_routing_impl import routing_interface_lookup_candidates


def normalize_dependency_workloads(
    workloads: list[BranchWorkload],
    *,
    existing_cable_ids_by_endpoint: dict[tuple[str, str], int] | None = None,
) -> tuple[list[BranchWorkload], list[dict]]:
    """Normalize cross-model facts in authoritative full workloads.

    Full device and interface maps are authoritative for the branch being built.
    Diff or partial fetches are deliberately left alone because their unchanged
    parents may already exist in NetBox. Full Inventory Item rows also provide
    the single-chassis serial for Device without another Forward query.
    """

    device_names, devices_authoritative = _full_device_names(workloads)
    interface_pairs, interfaces_authoritative = _full_interface_pairs(workloads)
    chassis_serials_by_device, inventory_authoritative = (
        _full_inventory_chassis_serials(workloads)
    )
    vulnerability_cve_ids, vulnerabilities_authoritative = (
        _full_vulnerability_cve_ids(workloads)
    )
    associated_software_versions, software_associations_authoritative = (
        _full_associated_software_versions(workloads)
    )
    existing_cable_ids_by_endpoint = dict(existing_cable_ids_by_endpoint or {})
    normalized = []
    summaries = []

    for workload in workloads:
        rows = list(workload.upsert_rows)
        delete_rows = list(workload.delete_rows)
        reason_counts: dict[str, int] = defaultdict(int)
        enrichment_counts: dict[str, int] = defaultdict(int)
        if workload.sync_mode == "full" and workload.model_string == "dcim.device":
            rows, enriched_count = _enrich_device_serials(
                rows,
                chassis_serials_by_device=chassis_serials_by_device,
                inventory_authoritative=inventory_authoritative,
            )
            if enriched_count:
                enrichment_counts["single_chassis_serial"] = enriched_count
        elif workload.sync_mode == "full" and workload.model_string == "dcim.cable":
            rows = _filter_cable_parent_coverage(
                rows,
                device_names=device_names,
                devices_authoritative=devices_authoritative,
                interface_pairs=interface_pairs,
                interfaces_authoritative=interfaces_authoritative,
                reason_counts=reason_counts,
            )
            rows = _select_representable_cables(
                rows,
                existing_cable_ids_by_endpoint=existing_cable_ids_by_endpoint,
                reason_counts=reason_counts,
            )
        elif (
            workload.sync_mode == "full"
            and workload.model_string == "netbox_routing.ospfinterface"
        ):
            rows = _filter_ospf_interface_coverage(
                rows,
                device_names=device_names,
                devices_authoritative=devices_authoritative,
                interface_pairs=interface_pairs,
                interfaces_authoritative=interfaces_authoritative,
                reason_counts=reason_counts,
            )
        elif (
            workload.sync_mode == "full"
            and workload.model_string == "netbox_dlm.cve"
            and vulnerabilities_authoritative
        ):
            rows, excluded_cve_rows = _filter_cves_by_vulnerability_coverage(
                rows,
                vulnerability_cve_ids=vulnerability_cve_ids,
                reason_counts=reason_counts,
            )
            delete_rows = _merge_delete_rows(
                delete_rows,
                excluded_cve_rows,
                identity_fields=("cve_id",),
            )
        elif (
            workload.sync_mode == "full"
            and workload.model_string == "netbox_dlm.softwareversion"
            and software_associations_authoritative
        ):
            rows, excluded_version_rows = _filter_software_versions_by_association(
                rows,
                associated_versions=associated_software_versions,
                reason_counts=reason_counts,
            )
            missing_associated_versions = _missing_associated_software_versions(
                rows,
                associated_versions=associated_software_versions,
            )
            if missing_associated_versions:
                rows.extend(missing_associated_versions)
                enrichment_counts["associated_software_version"] = len(
                    missing_associated_versions
                )
            delete_rows = _merge_delete_rows(
                delete_rows,
                excluded_version_rows,
                identity_fields=("platform_slug", "version"),
            )

        excluded_count = sum(reason_counts.values())
        normalized_workload = (
            replace(workload, upsert_rows=rows, delete_rows=delete_rows)
            if excluded_count
            or enrichment_counts
            or delete_rows != workload.delete_rows
            else workload
        )
        normalized.append(normalized_workload)
        if excluded_count or enrichment_counts:
            summaries.append(
                {
                    "model": workload.model_string,
                    "query_name": workload.query_name,
                    "execution_value": workload.execution_value,
                    "input_row_count": len(workload.upsert_rows),
                    "kept_row_count": len(rows),
                    "excluded_row_count": excluded_count,
                    "reason_counts": dict(sorted(reason_counts.items())),
                    "enrichment_counts": dict(sorted(enrichment_counts.items())),
                }
            )
    return normalized, summaries


def _full_device_names(workloads):
    device_workloads = [
        workload
        for workload in workloads
        if workload.model_string == "dcim.device" and workload.sync_mode == "full"
    ]
    return (
        {
            str(row.get("name") or "").strip()
            for workload in device_workloads
            for row in workload.upsert_rows
            if str(row.get("name") or "").strip()
        },
        bool(device_workloads),
    )


def _full_interface_pairs(workloads):
    interface_workloads = [
        workload
        for workload in workloads
        if workload.model_string == "dcim.interface" and workload.sync_mode == "full"
    ]
    return (
        {
            (
                str(row.get("device") or "").strip(),
                str(row.get("name") or "").strip(),
            )
            for workload in interface_workloads
            for row in workload.upsert_rows
            if str(row.get("device") or "").strip()
            and str(row.get("name") or "").strip()
        },
        bool(interface_workloads),
    )


def _full_inventory_chassis_serials(workloads):
    inventory_workloads = [
        workload
        for workload in workloads
        if workload.model_string == "dcim.inventoryitem"
        and workload.sync_mode == "full"
    ]
    serials_by_device: dict[str, set[str]] = defaultdict(set)
    for workload in inventory_workloads:
        for row in workload.upsert_rows:
            part_type = str(row.get("part_type") or row.get("role") or "").strip()
            if part_type.upper() != "CHASSIS":
                continue
            device_name = str(row.get("device") or "").strip()
            serial = str(row.get("serial") or "").strip()
            if device_name and serial:
                serials_by_device[device_name].add(serial[:50])
    return dict(serials_by_device), bool(inventory_workloads)


def _enrich_device_serials(
    rows,
    *,
    chassis_serials_by_device,
    inventory_authoritative,
):
    if not inventory_authoritative:
        return rows, 0
    enriched = []
    enriched_count = 0
    for row in rows:
        if str(row.get("serial") or "").strip():
            enriched.append(row)
            continue
        device_name = str(row.get("name") or "").strip()
        serials = chassis_serials_by_device.get(device_name, set())
        if len(serials) != 1:
            enriched.append(row)
            continue
        enriched.append({**row, "serial": next(iter(serials))})
        enriched_count += 1
    return enriched, enriched_count


def _full_vulnerability_cve_ids(workloads):
    vulnerability_workloads = [
        workload
        for workload in workloads
        if workload.model_string == "netbox_dlm.vulnerability"
        and workload.sync_mode == "full"
    ]
    return (
        {
            str(row.get("cve_id") or "").strip()
            for workload in vulnerability_workloads
            for row in workload.upsert_rows
            if str(row.get("cve_id") or "").strip()
        },
        bool(vulnerability_workloads),
    )


def _full_associated_software_versions(workloads):
    association_workloads = [
        workload
        for workload in workloads
        if workload.model_string
        in {"netbox_dlm.devicesoftware", "netbox_dlm.vulnerability"}
        and workload.sync_mode == "full"
    ]
    versions = {}
    for workload in association_workloads:
        for row in workload.upsert_rows:
            identity = (
                str(row.get("platform_slug") or "").strip(),
                str(row.get("version") or "").strip(),
            )
            if all(identity):
                candidate = {
                    field: row.get(field)
                    for field in (
                        "platform",
                        "platform_slug",
                        "version",
                        "end_of_support",
                        "documentation_url",
                    )
                    if row.get(field) not in (None, "")
                }
                current = versions.get(identity)
                if current is None or _software_version_row_rank(candidate) > (
                    _software_version_row_rank(current)
                ):
                    versions[identity] = candidate
    return versions, bool(association_workloads)


def _software_version_row_rank(row):
    return (
        len(row),
        tuple(str(row.get(field) or "") for field in sorted(row)),
    )


def _filter_cable_parent_coverage(
    rows,
    *,
    device_names,
    devices_authoritative,
    interface_pairs,
    interfaces_authoritative,
    reason_counts,
):
    kept = []
    for row in rows:
        endpoints = _cable_endpoints(row)
        if endpoints is None:
            reason_counts["invalid_endpoint_identity"] += 1
            continue
        if devices_authoritative and any(
            device_name not in device_names for device_name, _ in endpoints
        ):
            reason_counts["device_not_in_workload"] += 1
            continue
        if interfaces_authoritative and any(
            endpoint not in interface_pairs for endpoint in endpoints
        ):
            reason_counts["interface_not_in_workload"] += 1
            continue
        kept.append(row)
    return kept


def _select_representable_cables(
    rows,
    *,
    existing_cable_ids_by_endpoint,
    reason_counts,
):
    rows_by_identity = {}
    for row in rows:
        identity = canonical_cable_endpoint_identity(row)
        if identity is None:
            reason_counts["invalid_endpoint_identity"] += 1
            continue
        if identity in rows_by_identity:
            reason_counts["duplicate_identity"] += 1
            continue
        rows_by_identity[identity] = row

    selected = []
    selected_identities = set()
    used_endpoints = set()
    identities = sorted(rows_by_identity)

    for identity in identities:
        left, right = identity
        left_cable_id = existing_cable_ids_by_endpoint.get(left)
        right_cable_id = existing_cable_ids_by_endpoint.get(right)
        if left_cable_id and left_cable_id == right_cable_id:
            selected.append(rows_by_identity[identity])
            selected_identities.add(identity)
            used_endpoints.update(identity)

    occupied_endpoints = {
        endpoint
        for endpoint, cable_id in existing_cable_ids_by_endpoint.items()
        if cable_id
    }
    used_endpoints.update(occupied_endpoints)

    for identity in identities:
        row = rows_by_identity[identity]
        if identity in selected_identities:
            continue
        if any(endpoint in occupied_endpoints for endpoint in identity):
            reason_counts["existing_endpoint_conflict"] += 1
            continue
        if any(endpoint in used_endpoints for endpoint in identity):
            reason_counts["competing_candidate"] += 1
            continue
        selected.append(row)
        selected_identities.add(identity)
        used_endpoints.update(identity)
    return selected


def _filter_ospf_interface_coverage(
    rows,
    *,
    device_names,
    devices_authoritative,
    interface_pairs,
    interfaces_authoritative,
    reason_counts,
):
    kept = []
    for row in rows:
        device_name = str(row.get("device") or "").strip()
        if devices_authoritative and device_name not in device_names:
            reason_counts["device_not_in_workload"] += 1
            continue
        if interfaces_authoritative:
            candidates = routing_interface_lookup_candidates(row.get("local_interface"))
            if not any(
                (device_name, candidate) in interface_pairs for candidate in candidates
            ):
                reason_counts["interface_not_in_workload"] += 1
                continue
        kept.append(row)
    return kept


def _filter_cves_by_vulnerability_coverage(
    rows,
    *,
    vulnerability_cve_ids,
    reason_counts,
):
    kept = []
    excluded = []
    for row in rows:
        cve_id = str(row.get("cve_id") or "").strip()
        if cve_id not in vulnerability_cve_ids:
            reason_counts["no_in_scope_vulnerability"] += 1
            excluded.append(row)
            continue
        kept.append(row)
    return kept, excluded


def _filter_software_versions_by_association(
    rows,
    *,
    associated_versions,
    reason_counts,
):
    kept = []
    excluded = []
    for row in rows:
        identity = (
            str(row.get("platform_slug") or "").strip(),
            str(row.get("version") or "").strip(),
        )
        if identity not in associated_versions:
            reason_counts["no_in_scope_device_or_vulnerability"] += 1
            excluded.append(row)
            continue
        kept.append(row)
    return kept, excluded


def _missing_associated_software_versions(rows, *, associated_versions):
    present = {
        (
            str(row.get("platform_slug") or "").strip(),
            str(row.get("version") or "").strip(),
        )
        for row in rows
    }
    return [
        dict(associated_versions[identity])
        for identity in sorted(set(associated_versions) - present)
    ]


def _merge_delete_rows(existing_rows, generated_rows, *, identity_fields):
    by_identity = {}
    for row in [*existing_rows, *generated_rows]:
        identity = tuple(str(row.get(field) or "").strip() for field in identity_fields)
        if all(identity):
            by_identity[identity] = row
    return list(by_identity.values())


def _cable_endpoints(row):
    identity = canonical_cable_endpoint_identity(row)
    if identity is None:
        return None
    return identity
