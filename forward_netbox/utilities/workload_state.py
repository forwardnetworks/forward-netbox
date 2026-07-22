import hashlib
import json
import zlib
from dataclasses import dataclass
from dataclasses import replace

from django.apps import apps
from django.db import transaction
from django.db.models import Q

from ..exceptions import ForwardQueryError
from .delete_policy import should_suppress_aci_deletes
from .sync_contracts import canonical_cable_endpoint_identity
from .sync_contracts import row_coalesce_field_is_complete


PAYLOAD_VERSION = 2
STATE_ACTIONS = frozenset({"upsert", "delete"})


@dataclass(frozen=True)
class PendingWorkloadState:
    model_string: str
    parameter_hash: str
    identity_contract_hash: str
    payload: bytes
    payload_checksum: str
    row_count: int


def _canonical_json(value) -> str:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        default=str,
    )


def _digest(value) -> str:
    return hashlib.sha256(_canonical_json(value).encode("utf-8")).hexdigest()


def canonical_row_identity(model_string, row, coalesce_fields) -> str:
    if model_string == "dcim.cable":
        endpoints = canonical_cable_endpoint_identity(row)
        if endpoints is not None:
            return _canonical_json({"cable_endpoints": endpoints})

    for field_set in coalesce_fields:
        if all(
            row_coalesce_field_is_complete(model_string, row, field_name)
            for field_name in field_set
        ):
            return _canonical_json(
                {field_name: row.get(field_name) for field_name in field_set}
            )
    raise ForwardQueryError(
        f"Unable to derive durable workload identity for `{model_string}`."
    )


def build_state_entries(model_string, rows, coalesce_fields, *, action="upsert"):
    if action not in STATE_ACTIONS:
        raise ValueError(f"Unsupported durable workload-state action: {action}")
    entries = {}
    for row in rows:
        identity = canonical_row_identity(model_string, row, coalesce_fields)
        normalized_row = row
        entries[identity] = {
            "action": action,
            "row_hash": _digest(normalized_row),
            "row": normalized_row,
        }
    return entries


def encode_state_entries(entries) -> tuple[bytes, str]:
    compressor = zlib.compressobj(level=6)
    payload_buffer = bytearray()
    payload_buffer.extend(
        compressor.compress(
            (_canonical_json({"version": PAYLOAD_VERSION}) + "\n").encode("utf-8")
        )
    )
    for identity, value in sorted(entries.items()):
        line = (
            _canonical_json(
                [
                    identity,
                    value["action"],
                    value["row_hash"],
                    value["row"],
                ]
            )
            + "\n"
        )
        payload_buffer.extend(compressor.compress(line.encode("utf-8")))
    payload_buffer.extend(compressor.flush())
    payload = bytes(payload_buffer)
    return payload, hashlib.sha256(payload).hexdigest()


def decode_state_entries(payload, checksum):
    payload = bytes(payload)
    actual_checksum = hashlib.sha256(payload).hexdigest()
    if actual_checksum != str(checksum or ""):
        raise ForwardQueryError("Durable workload-state checksum validation failed.")
    entries = {}
    decompressor = zlib.decompressobj()
    line_buffer = bytearray()
    header_seen = False

    def consume_line(raw_line):
        nonlocal header_seen
        try:
            item = json.loads(raw_line.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ForwardQueryError(
                "Durable workload-state payload is invalid."
            ) from exc
        if not header_seen:
            header_seen = True
            if not isinstance(item, dict) or item.get("version") != PAYLOAD_VERSION:
                raise ForwardQueryError(
                    "Durable workload-state payload version is unsupported."
                )
            return
        if not isinstance(item, list) or len(item) != 4:
            raise ForwardQueryError("Durable workload-state row is invalid.")
        identity, action, row_hash, row = item
        if (
            not isinstance(identity, str)
            or action not in STATE_ACTIONS
            or not isinstance(row_hash, str)
            or not isinstance(row, dict)
        ):
            raise ForwardQueryError("Durable workload-state row is invalid.")
        entries[identity] = {
            "action": action,
            "row_hash": row_hash,
            "row": row,
        }

    try:
        for offset in range(0, len(payload), 64 * 1024):
            line_buffer.extend(
                decompressor.decompress(payload[offset : offset + 64 * 1024])
            )
            while b"\n" in line_buffer:
                raw_line, _, remainder = line_buffer.partition(b"\n")
                line_buffer = bytearray(remainder)
                if raw_line:
                    consume_line(raw_line)
        line_buffer.extend(decompressor.flush())
        if line_buffer:
            consume_line(bytes(line_buffer))
    except zlib.error as exc:
        raise ForwardQueryError("Durable workload-state payload is invalid.") from exc
    if not header_seen:
        raise ForwardQueryError("Durable workload-state payload is invalid.")
    if not decompressor.eof or decompressor.unused_data:
        raise ForwardQueryError("Durable workload-state payload is invalid.")
    return entries


def _parameter_hash(workloads) -> str:
    return _digest(
        [
            {
                "execution_mode": workload.execution_mode,
                "execution_value": workload.execution_value,
                "query_name": workload.query_name,
                "query_parameters": workload.query_parameters,
            }
            for workload in sorted(
                workloads,
                key=lambda item: (
                    item.query_name,
                    item.execution_mode,
                    item.execution_value,
                ),
            )
        ]
    )


def _identity_contract_hash(workloads) -> str:
    return _digest(
        {
            "model": workloads[0].model_string,
            "coalesce_fields": workloads[0].coalesce_fields,
        }
    )


def _load_current_state(sync, model_string):
    from ..models import ForwardWorkloadState

    return (
        ForwardWorkloadState.objects.filter(
            sync=sync,
            model_string=model_string,
            is_current=True,
        )
        .select_related("ingestion")
        .first()
    )


def _peer_delete_protection(sync, model_string, identity_contract_hash):
    """Return peer upserts and whether any enabled peer is unrepresented.

    A local delta is authoritative only for one sync's parameter scope. A
    global NetBox object cannot be deleted while another completed sync still
    asserts it, or while that peer has not established a comparable durable
    state yet.
    """

    from ..models import ForwardIngestion
    from ..models import ForwardSync
    from ..models import ForwardWorkloadState

    baseline_peer_sync_ids = set(
        ForwardIngestion.objects.filter(baseline_ready=True)
        .exclude(sync=sync)
        .values_list("sync_id", flat=True)
    )
    peer_sync_ids = {
        peer.pk
        for peer in ForwardSync.objects.filter(pk__in=baseline_peer_sync_ids).only(
            "pk", "parameters"
        )
        if peer.is_model_enabled(model_string)
    }
    if not peer_sync_ids:
        return set(), False, []

    peer_states = list(
        ForwardWorkloadState.objects.filter(
            sync_id__in=peer_sync_ids,
            model_string=model_string,
            is_current=True,
        ).order_by("sync_id")
    )
    represented_sync_ids = {state.sync_id for state in peer_states}
    unrepresented_peer = represented_sync_ids != peer_sync_ids
    protected_identities = set()
    protected_rows = []
    for state in peer_states:
        if state.identity_contract_hash != identity_contract_hash:
            unrepresented_peer = True
            continue
        entries = decode_state_entries(state.payload, state.payload_checksum)
        protected_identities.update(
            identity
            for identity, value in entries.items()
            if value["action"] == "upsert"
        )
        protected_rows.extend(
            value["row"] for value in entries.values() if value["action"] == "upsert"
        )
    return protected_identities, unrepresented_peer, protected_rows


def _merge_rows(workloads, attribute):
    rows = []
    for workload in workloads:
        rows.extend(getattr(workload, attribute))
    return rows


def _active_model_rows(sync, workloads, model_string):
    """Return the current/peer authoritative union, or None when incomplete."""

    model_workloads = [
        workload for workload in workloads if workload.model_string == model_string
    ]
    if (
        not model_workloads
        or not all(workload.sync_mode == "full" for workload in model_workloads)
        or not any(bool(workload.query_parameters) for workload in model_workloads)
    ):
        return None
    coalesce_fields = model_workloads[0].coalesce_fields
    if any(
        workload.coalesce_fields != coalesce_fields for workload in model_workloads[1:]
    ):
        raise ForwardQueryError(
            f"Parameterized full maps for `{model_string}` disagree on durable identity."
        )
    _, unrepresented_peer, peer_rows = _peer_delete_protection(
        sync,
        model_string,
        _identity_contract_hash(model_workloads),
    )
    if unrepresented_peer:
        return None
    return [*_merge_rows(model_workloads, "upsert_rows"), *peer_rows]


def _association_catalog_protection(sync, workloads):
    device_software_rows = _active_model_rows(
        sync,
        workloads,
        "netbox_dlm.devicesoftware",
    )
    vulnerability_rows = _active_model_rows(
        sync,
        workloads,
        "netbox_dlm.vulnerability",
    )

    def version_identities(rows):
        if rows is None:
            return set()
        return {
            (
                str(row.get("platform_slug") or "").strip(),
                str(row.get("version") or "").strip(),
            )
            for row in rows
            if str(row.get("platform_slug") or "").strip()
            and str(row.get("version") or "").strip()
        }

    return {
        "device_software_authoritative": device_software_rows is not None,
        "device_software_versions": version_identities(device_software_rows),
        "vulnerability_authoritative": vulnerability_rows is not None,
        "vulnerability_versions": version_identities(vulnerability_rows),
        "vulnerability_cves": {
            str(row.get("cve_id") or "").strip()
            for row in vulnerability_rows or []
            if str(row.get("cve_id") or "").strip()
        },
    }


def _locally_referenced_delete_identities(
    model_string,
    delete_entries,
    *,
    association_protection,
):
    if not delete_entries:
        return set()
    if model_string == "netbox_dlm.cve":
        cve_ids = {
            str(value["row"].get("cve_id") or "").strip()
            for value in delete_entries.values()
        }
        if association_protection["vulnerability_authoritative"]:
            linked_cve_ids = cve_ids & association_protection["vulnerability_cves"]
        else:
            CVE = apps.get_model("netbox_dlm", "CVE")
            linked_cve_ids = set(
                CVE.objects.filter(cve_id__in=cve_ids)
                .filter(vulnerabilities__isnull=False)
                .values_list("cve_id", flat=True)
                .distinct()
            )
        return {
            identity
            for identity, value in delete_entries.items()
            if str(value["row"].get("cve_id") or "").strip() in linked_cve_ids
        }
    if model_string == "netbox_dlm.softwareversion":
        SoftwareVersion = apps.get_model("netbox_dlm", "SoftwareVersion")
        row_identities = {
            (
                str(value["row"].get("platform_slug") or "").strip(),
                str(value["row"].get("version") or "").strip(),
            )
            for value in delete_entries.values()
        }
        protected = set(
            SoftwareVersion.objects.filter(
                platform__slug__in={item[0] for item in row_identities},
                version__in={item[1] for item in row_identities},
            )
            .filter(Q(image_files__isnull=False) | Q(validated_rules__isnull=False))
            .values_list("platform__slug", "version")
            .distinct()
        )
        if association_protection["device_software_authoritative"]:
            protected.update(association_protection["device_software_versions"])
        else:
            protected.update(
                SoftwareVersion.objects.filter(
                    platform__slug__in={item[0] for item in row_identities},
                    version__in={item[1] for item in row_identities},
                    devices_running__isnull=False,
                )
                .values_list("platform__slug", "version")
                .distinct()
            )
        if association_protection["vulnerability_authoritative"]:
            protected.update(association_protection["vulnerability_versions"])
        else:
            protected.update(
                SoftwareVersion.objects.filter(
                    platform__slug__in={item[0] for item in row_identities},
                    version__in={item[1] for item in row_identities},
                    vulnerabilities__isnull=False,
                )
                .values_list("platform__slug", "version")
                .distinct()
            )
        return {
            identity
            for identity, value in delete_entries.items()
            if (
                str(value["row"].get("platform_slug") or "").strip(),
                str(value["row"].get("version") or "").strip(),
            )
            in protected
        }
    if model_string == "dcim.device":
        return _claimed_device_delete_identities(delete_entries)
    return set()


def _claimed_device_delete_identities(delete_entries):
    from ..models import ForwardDeviceIdentity
    from ..models import ForwardDeviceTagClaim
    from ..models import ForwardPreservedDeviceTagAssignment
    from ..models import ForwardVirtualParentClaim

    names = {
        str(value["row"].get("name") or "").strip() for value in delete_entries.values()
    }
    identity_rows = list(
        ForwardDeviceIdentity.objects.filter(source_device_key__in=names).values(
            "device_id", "source_device_key", "sync_id"
        )
    )
    device_ids = {row["device_id"] for row in identity_rows}
    protected_device_ids = set(
        ForwardDeviceTagClaim.objects.filter(device_id__in=device_ids).values_list(
            "device_id", flat=True
        )
    )
    protected_device_ids.update(
        ForwardPreservedDeviceTagAssignment.objects.filter(
            device_id__in=device_ids
        ).values_list("device_id", flat=True)
    )
    protected_device_ids.update(
        ForwardVirtualParentClaim.objects.filter(device_id__in=device_ids).values_list(
            "device_id", flat=True
        )
    )
    protected_device_ids.update(
        ForwardVirtualParentClaim.objects.filter(
            parent_device_id__in=device_ids
        ).values_list("parent_device_id", flat=True)
    )
    identity_syncs_by_device = {}
    for row in identity_rows:
        identity_syncs_by_device.setdefault(row["device_id"], set()).add(row["sync_id"])
    protected_device_ids.update(
        device_id
        for device_id, sync_ids in identity_syncs_by_device.items()
        if len(sync_ids) > 1
    )
    protected_names = {
        row["source_device_key"]
        for row in identity_rows
        if row["device_id"] in protected_device_ids
    }
    return {
        identity
        for identity, value in delete_entries.items()
        if str(value["row"].get("name") or "").strip() in protected_names
    }


def _owned_device_rows(sync, coalesce_fields):
    from ..models import ForwardDeviceIdentity

    rows = []
    identities = (
        ForwardDeviceIdentity.objects.filter(sync=sync)
        .order_by("source_device_key")
        .values(
            "source_device_key",
            "device__site__name",
            "device__site__slug",
        )
    )
    for identity in identities:
        values = {
            "name": identity["source_device_key"],
            "site": identity["device__site__name"],
            "site_slug": identity["device__site__slug"],
        }
        for field_set in coalesce_fields:
            row = {field: values.get(field) for field in field_set}
            if all(value not in (None, "") for value in row.values()):
                rows.append(row)
                break
    return rows


def _software_version_catalog_rows():
    SoftwareVersion = apps.get_model("netbox_dlm", "SoftwareVersion")
    return list(
        SoftwareVersion.objects.order_by("platform__slug", "version").values(
            "platform__slug",
            "version",
        )
    )


def _deduplicate_rows(model_string, rows, coalesce_fields):
    by_identity = {}
    for row in rows:
        identity = canonical_row_identity(model_string, row, coalesce_fields)
        by_identity[identity] = row
    return list(by_identity.values())


def _sync_exclusive_device_ids(sync):
    from ..models import ForwardDeviceIdentity

    device_ids = set(
        ForwardDeviceIdentity.objects.filter(sync=sync).values_list(
            "device_id", flat=True
        )
    )
    if not device_ids:
        return set()
    shared_ids = set(
        ForwardDeviceIdentity.objects.filter(device_id__in=device_ids)
        .exclude(sync=sync)
        .values_list("device_id", flat=True)
    )
    return device_ids - shared_ids


def _bootstrap_dlm_rows(sync, model_string):
    """Return legacy DLM rows exclusively attributable to this sync's devices."""

    device_ids = _sync_exclusive_device_ids(sync)
    if not device_ids:
        return []
    if model_string == "netbox_dlm.vulnerability":
        Vulnerability = apps.get_model("netbox_dlm", "Vulnerability")
        rows = Vulnerability.objects.filter(device_id__in=device_ids).values(
            "device__name",
            "cve__cve_id",
            "software_version__platform__name",
            "software_version__platform__slug",
            "software_version__version",
        )
        return [
            {
                "name": row["device__name"],
                "cve_id": row["cve__cve_id"],
                "platform": row["software_version__platform__name"],
                "platform_slug": row["software_version__platform__slug"],
                "version": row["software_version__version"],
            }
            for row in rows
        ]
    if model_string == "netbox_dlm.devicesoftware":
        DeviceSoftware = apps.get_model("netbox_dlm", "DeviceSoftware")
        rows = DeviceSoftware.objects.filter(device_id__in=device_ids).values(
            "device__name",
            "software_version__platform__name",
            "software_version__platform__slug",
            "software_version__version",
        )
        return [
            {
                "name": row["device__name"],
                "platform": row["software_version__platform__name"],
                "platform_slug": row["software_version__platform__slug"],
                "version": row["software_version__version"],
            }
            for row in rows
        ]
    return []


def apply_durable_workload_deltas(sync, workloads):
    """Derive local deltas for parameterized full model workloads.

    Native Forward diffs remain untouched. Full workloads are consolidated per
    model so multiple query maps cannot delete an identity still supplied by a
    sibling map.
    """

    positions_by_model = {}
    for position, workload in enumerate(workloads):
        positions_by_model.setdefault(workload.model_string, []).append(position)
    association_protection = _association_catalog_protection(sync, workloads)

    replacements = {}
    removed_positions = set()
    pending_states = []
    summaries = []
    for model_string, positions in positions_by_model.items():
        model_workloads = [workloads[position] for position in positions]
        if not all(
            workload.sync_mode == "full" for workload in model_workloads
        ) or not any(bool(workload.query_parameters) for workload in model_workloads):
            continue
        coalesce_fields = model_workloads[0].coalesce_fields
        if any(
            workload.coalesce_fields != coalesce_fields
            for workload in model_workloads[1:]
        ):
            raise ForwardQueryError(
                f"Parameterized full maps for `{model_string}` disagree on durable identity."
            )

        target_rows = _merge_rows(model_workloads, "upsert_rows")
        target_entries = build_state_entries(
            model_string,
            target_rows,
            coalesce_fields,
        )
        parameter_hash = _parameter_hash(model_workloads)
        identity_contract_hash = _identity_contract_hash(model_workloads)
        current_state = _load_current_state(sync, model_string)
        compatible = bool(
            current_state is not None
            and current_state.parameter_hash == parameter_hash
            and current_state.identity_contract_hash == identity_contract_hash
        )

        explicit_deletes = _deduplicate_rows(
            model_string,
            _merge_rows(model_workloads, "delete_rows"),
            coalesce_fields,
        )
        bootstrap_delete_identities = set()
        ownership_delete_identities = set()
        catalog_delete_identities = set()
        if model_string == "dcim.device" and (current_state is None or compatible):
            ownership_entries = build_state_entries(
                model_string,
                _owned_device_rows(sync, coalesce_fields),
                coalesce_fields,
            )
            ownership_deletes = [
                value["row"]
                for identity, value in ownership_entries.items()
                if identity not in target_entries
            ]
            ownership_delete_identities = {
                identity
                for identity in ownership_entries
                if identity not in target_entries
            }
            explicit_deletes = _deduplicate_rows(
                model_string,
                [*explicit_deletes, *ownership_deletes],
                coalesce_fields,
            )
        if model_string == "netbox_dlm.softwareversion" and (
            current_state is None or compatible
        ):
            catalog_rows = [
                {
                    "platform_slug": row["platform__slug"],
                    "version": row["version"],
                }
                for row in _software_version_catalog_rows()
            ]
            catalog_entries = build_state_entries(
                model_string,
                catalog_rows,
                coalesce_fields,
            )
            catalog_deletes = [
                value["row"]
                for identity, value in catalog_entries.items()
                if identity not in target_entries
            ]
            catalog_delete_identities = {
                identity
                for identity in catalog_entries
                if identity not in target_entries
            }
            explicit_deletes = _deduplicate_rows(
                model_string,
                [*explicit_deletes, *catalog_deletes],
                coalesce_fields,
            )
        if current_state is None:
            bootstrap_rows = _bootstrap_dlm_rows(sync, model_string)
            bootstrap_entries = build_state_entries(
                model_string,
                bootstrap_rows,
                coalesce_fields,
            )
            bootstrap_deletes = [
                value["row"]
                for identity, value in bootstrap_entries.items()
                if identity not in target_entries
            ]
            bootstrap_delete_identities = {
                identity
                for identity in bootstrap_entries
                if identity not in target_entries
            }
            explicit_deletes = _deduplicate_rows(
                model_string,
                [*explicit_deletes, *bootstrap_deletes],
                coalesce_fields,
            )
        explicit_delete_entries = build_state_entries(
            model_string,
            explicit_deletes,
            coalesce_fields,
            action="delete",
        )
        for identity in target_entries:
            explicit_delete_entries.pop(identity, None)
        protected_identities, unrepresented_peer, _ = _peer_delete_protection(
            sync,
            model_string,
            identity_contract_hash,
        )
        proposed_delete_count = len(explicit_delete_entries)
        suppress_deletes = should_suppress_aci_deletes(sync, model_string)
        reference_protected_identities = _locally_referenced_delete_identities(
            model_string,
            explicit_delete_entries,
            association_protection=association_protection,
        )
        if suppress_deletes or unrepresented_peer:
            explicit_delete_entries = {}
        else:
            for identity in protected_identities | reference_protected_identities:
                explicit_delete_entries.pop(identity, None)
        protected_delete_count = proposed_delete_count - len(explicit_delete_entries)
        bootstrap_delete_count = sum(
            identity in explicit_delete_entries
            for identity in bootstrap_delete_identities
        )
        explicit_deletes = [value["row"] for value in explicit_delete_entries.values()]
        if compatible:
            previous_entries = decode_state_entries(
                current_state.payload,
                current_state.payload_checksum,
            )
            changed_rows = [
                value["row"]
                for identity, value in target_entries.items()
                if identity not in previous_entries
                or previous_entries[identity]["action"] != "upsert"
                or previous_entries[identity]["row_hash"] != value["row_hash"]
            ]
            missing_entries = {
                identity: {
                    **value,
                    "action": "delete",
                }
                for identity, value in previous_entries.items()
                if value["action"] == "upsert" and identity not in target_entries
            }
            proposed_missing_count = len(missing_entries)
            missing_reference_protected = _locally_referenced_delete_identities(
                model_string,
                missing_entries,
                association_protection=association_protection,
            )
            if suppress_deletes or unrepresented_peer:
                missing_entries = {}
            else:
                for identity in protected_identities | missing_reference_protected:
                    missing_entries.pop(identity, None)
            protected_delete_count += proposed_missing_count - len(missing_entries)
            newly_explicit_deletes = [
                value["row"]
                for identity, value in explicit_delete_entries.items()
                if identity not in previous_entries
                or previous_entries[identity]["action"] != "delete"
            ]
            delete_rows = _deduplicate_rows(
                model_string,
                [
                    *newly_explicit_deletes,
                    *(value["row"] for value in missing_entries.values()),
                ],
                coalesce_fields,
            )
            if suppress_deletes:
                state_entries = {**previous_entries, **target_entries}
            else:
                state_entries = {
                    **target_entries,
                    **explicit_delete_entries,
                    **missing_entries,
                }
            mode = "local_delta"
        else:
            changed_rows = [value["row"] for value in target_entries.values()]
            delete_rows = explicit_deletes
            state_entries = {**target_entries, **explicit_delete_entries}
            mode = (
                "seed_reconcile"
                if current_state is None and bootstrap_delete_count
                else "seed" if current_state is None else "contract_reset"
            )

        payload, payload_checksum = encode_state_entries(state_entries)
        pending_states.append(
            PendingWorkloadState(
                model_string=model_string,
                parameter_hash=parameter_hash,
                identity_contract_hash=identity_contract_hash,
                payload=payload,
                payload_checksum=payload_checksum,
                row_count=len(target_entries),
            )
        )
        first = model_workloads[0]
        replacements[positions[0]] = replace(
            first,
            label=f"{model_string} | durable parameterized workload",
            upsert_rows=changed_rows,
            delete_rows=delete_rows,
            query_name="Durable parameterized workload",
            execution_mode="local_delta" if compatible else first.execution_mode,
            execution_value=model_string,
        )
        removed_positions.update(positions[1:])
        summaries.append(
            {
                "model": model_string,
                "mode": mode,
                "target_rows": len(target_entries),
                "upsert_rows": len(changed_rows),
                "delete_rows": len(delete_rows),
                "bootstrap_delete_rows": bootstrap_delete_count,
                "ownership_delete_rows": sum(
                    identity in explicit_delete_entries
                    for identity in ownership_delete_identities
                ),
                "catalog_delete_rows": sum(
                    identity in explicit_delete_entries
                    for identity in catalog_delete_identities
                ),
                "protected_delete_rows": protected_delete_count,
                "unrepresented_peer": unrepresented_peer,
                "tombstone_rows": sum(
                    value["action"] == "delete" for value in state_entries.values()
                ),
                "compressed_bytes": len(payload),
            }
        )

    normalized = []
    for position, workload in enumerate(workloads):
        if position in removed_positions:
            continue
        candidate = replacements.get(position, workload)
        if candidate.estimated_changes:
            normalized.append(candidate)
    return normalized, pending_states, summaries


def stage_workload_states(ingestion, pending_states):
    from ..models import ForwardWorkloadState

    if not pending_states:
        return 0
    ForwardWorkloadState.objects.filter(ingestion=ingestion).delete()
    ForwardWorkloadState.objects.bulk_create(
        [
            ForwardWorkloadState(
                sync=ingestion.sync,
                ingestion=ingestion,
                model_string=state.model_string,
                parameter_hash=state.parameter_hash,
                identity_contract_hash=state.identity_contract_hash,
                payload=state.payload,
                payload_checksum=state.payload_checksum,
                row_count=state.row_count,
                snapshot_id=str(ingestion.snapshot_id or ""),
                is_current=False,
            )
            for state in pending_states
        ],
        batch_size=100,
    )
    return len(pending_states)


def promote_workload_states_locked(ingestion):
    from ..models import ForwardWorkloadState

    pending = list(
        ForwardWorkloadState.objects.select_for_update()
        .filter(ingestion=ingestion)
        .order_by("model_string")
    )
    for state in pending:
        old_states = (
            ForwardWorkloadState.objects.select_for_update()
            .filter(
                sync=ingestion.sync,
                model_string=state.model_string,
            )
            .exclude(pk=state.pk)
        )
        old_states.filter(is_current=True).update(is_current=False)
        state.is_current = True
        state.save(update_fields=["is_current"])
        old_states.delete()
    return len(pending)


def stage_and_promote_noop_workload_states(ingestion, pending_states):
    with transaction.atomic():
        stage_workload_states(ingestion, pending_states)
        return promote_workload_states_locked(ingestion)
