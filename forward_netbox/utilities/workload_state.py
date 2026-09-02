import hashlib
import json
import zlib
from dataclasses import dataclass
from dataclasses import replace

from django.apps import apps
from django.db import transaction
from django.db.models import Q
from django.utils import timezone
from rq.timeouts import JobTimeoutException

from ..exceptions import ForwardQueryError
from .delete_policy import should_suppress_aci_deletes
from .sync_contracts import canonical_cable_endpoint_identity
from .sync_contracts import row_coalesce_field_is_complete

PAYLOAD_VERSION = 2
STATE_ACTIONS = frozenset({"upsert", "delete"})
CANONICAL_ROW_IDENTITY_HASH_SCHEME = "sha256_canonical_row_identity_v1"


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
    if model_string == "ipam.fhrpgroup":
        # Durable full-state reconciliation must retain every participant.
        # Group-level coalesce alone would overwrite all but one assignment and
        # can manufacture a later delete for a still-present group member.
        fields = (
            "protocol",
            "group_id",
            "address",
            "vrf",
            "device",
            "interface",
        )
        if all(row.get(field) not in (None, "") for field in fields[:3] + fields[4:]):
            return _canonical_json(
                {"fhrp_participant": {field: row.get(field) for field in fields}}
            )

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


def canonical_row_identity_hash(model_string, row, coalesce_fields) -> str:
    """Return the sole hashed form of the production durable row identity."""

    identity = canonical_row_identity(model_string, row, coalesce_fields)
    return hashlib.sha256(identity.encode("utf-8")).hexdigest()


def compare_canonical_delete_identities(
    before_target_identities,
    after_target_identities,
    captured_delete_identities,
) -> dict:
    """Compare one model's logical deletes by exact canonical identity.

    Counts are identity counts, not raw contributor-row counts.  A delete is
    correct only when its identity was in the complete before target and is no
    longer in the complete after target.
    """

    before = set(before_target_identities or ())
    after = set(after_target_identities or ())
    captured = set(captured_delete_identities or ())
    expected = before - after
    matched = expected & captured
    spurious = captured - expected
    missing = expected - captured
    return {
        "before_target_count": len(before),
        "after_target_count": len(after),
        "full_pair_expected_delete_count": len(expected),
        "captured_staging_delete_count": len(captured),
        "matched_delete_count": len(matched),
        "spurious_delete_count": len(spurious),
        "spurious_identity_hash_sample": sorted(spurious)[:20],
        "missing_delete_count": len(missing),
        "missing_identity_hash_sample": sorted(missing)[:20],
        "exact_match": not spurious and not missing,
    }


def canonical_identity_hash_scheme_errors(
    observed_schemes: dict[str, str],
) -> list[str]:
    """Fail-closed diagnostics for oracle artifacts from another namespace."""

    return [
        f"identity_scheme_mismatch:{source}:{scheme or 'missing'}"
        for source, scheme in sorted((observed_schemes or {}).items())
        if scheme != CANONICAL_ROW_IDENTITY_HASH_SCHEME
    ]


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

    from ..models import ForwardIngestion, ForwardSync, ForwardWorkloadState

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
        # The hand-list above omitted `InventoryItemSoftware`, which is exactly
        # what a deployment's six protected-delete skips named. Read the rest
        # from the schema so the next relation does not need remembering.
        candidates = list(
            SoftwareVersion.objects.filter(
                platform__slug__in={item[0] for item in row_identities},
                version__in={item[1] for item in row_identities},
            ).values_list("pk", "platform__slug", "version")
        )
        held_pks = _reference_protected_pks(
            SoftwareVersion,
            [pk for pk, _slug, _version in candidates],
            # Already modelled above, and from this run's rows rather than the
            # database when the run is authoritative for them - so a child this
            # run is deleting must not read as a blocker here.
            ignore_labels=("netbox_dlm.DeviceSoftware", "netbox_dlm.Vulnerability"),
        )
        protected.update(
            (slug, version) for pk, slug, version in candidates if pk in held_pks
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


# Ownership rows the device delete path releases on its way through, so a
# device holding only these is still deletable. The tag-claim and virtual-parent
# cases are separately held back by `_claimed_device_delete_identities`, which
# is where that policy belongs; here they must not masquerade as a reference.
OWNERSHIP_RELEASED_ON_DEVICE_DELETE = (
    "forward_netbox.ForwardDeviceIdentity",
    "forward_netbox.ForwardDeviceTagClaim",
    "forward_netbox.ForwardVirtualParentClaim",
    "forward_netbox.ForwardPreservedDeviceTagAssignment",
)


def _reference_protected_pks(model_class, pks, *, ignore_labels=()):
    """Which of these rows the database will refuse to delete, read from the schema.

    Every guard above this one hand-lists the relations it knows about, and each
    hand-list has been wrong in the field. `netbox_dlm.softwareversion` protected
    against image files, validated rules, device software and vulnerabilities -
    but not `InventoryItemSoftware`, which is what a deployment's protected-delete
    skips actually named. `dcim.device` listed no references at all.

    So read the relations from the model rather than from memory.
    `protecting_relations` returns every PROTECT/RESTRICT relation including the
    hidden ones, so a relation added later is covered without anyone
    remembering to come back here.

    This is a hold-back, not a prediction of success: a row with no protecting
    reference may still fail for another reason. The point is the converse - a
    row WITH one cannot succeed, and staging it anyway is what produced a
    refused delete recorded as done.

    Asked of Django's own deletion collector rather than of the relation list,
    because a one-level scan is not enough. `protecting_relations(Device)` does
    not name `netbox_routing.BGPPeer` at all: a `BGPRouter` attaches to a device
    through a GENERIC key, which carries no database constraint, and the
    protection only appears further down the cascade the delete would perform -
    router, then scope, then peer. A scan of the model's own reverse relations
    reports that device as deletable and is wrong.

    `Collector.collect` is the code `.delete()` itself runs, so it follows
    cascades, generic relations and hidden relations by construction, and its
    verdict is the database's.

    Fails CLOSED. If the collector raises anything this does not recognise, the
    row is held back rather than staged: on a destructive path, "cannot tell"
    must mean "do not delete".
    """
    from django.db import DEFAULT_DB_ALIAS
    from django.db.models.deletion import Collector
    from django.db.models.deletion import ProtectedError
    from django.db.models.deletion import RestrictedError

    candidate_pks = {pk for pk in pks if pk is not None}
    if not candidate_pks:
        return set()
    objects = list(model_class.objects.filter(pk__in=candidate_pks))
    if not objects:
        return set()

    ignored = set(ignore_labels)

    def _blocked(batch):
        collector = Collector(using=DEFAULT_DB_ALIAS)
        try:
            collector.collect(batch)
        except (ProtectedError, RestrictedError) as exc:
            blockers = (
                getattr(exc, "protected_objects", None)
                or getattr(exc, "restricted_objects", None)
                or ()
            )
            labels = {type(obj)._meta.label for obj in blockers}
            # A reference the delete path itself clears is not a blocker. The
            # plugin's own ownership rows are released immediately before the
            # device delete, and an association row already being deleted in
            # this run goes first by delete ordering. Without this the guard
            # holds back EVERY managed device, because each one carries a
            # PROTECT `ForwardDeviceIdentity` - which is how the first version
            # of this silently disabled the sweep it was meant to make safe.
            #
            # An empty label set means the exception carried nothing readable,
            # so it counts as a blocker under the fail-closed rule.
            return not labels or bool(labels - ignored)
        except JobTimeoutException:
            raise
        except Exception:  # noqa: BLE001 - unknown means unsafe, see docstring
            return True
        return False

    # The whole set in one pass first: on a converged estate nothing is held and
    # this costs one collect rather than one per row. Only when something IS
    # held does it matter which, and only then is the per-row walk paid for.
    if not _blocked(objects):
        return set()
    return {obj.pk for obj in objects if _blocked([obj])}


def _claimed_device_delete_identities(delete_entries):
    from ..models import (
        ForwardDeviceIdentity,
        ForwardDeviceTagClaim,
        ForwardPreservedDeviceTagAssignment,
        ForwardVirtualParentClaim,
    )

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
    # And whatever the database itself will refuse.
    #
    # Every check above is about plugin OWNERSHIP - a claim, a preserved
    # assignment, a second sync. None of them asks whether anything still
    # points at the device. A deployment's sync therefore staged ten device
    # deletes that `netbox_routing.bgppeer` rows held, and each one failed at
    # apply time and was recorded as a skip. Worse, the durable state then
    # tombstoned them as deleted, so nothing retried and the report went quiet
    # while the devices were still there.
    #
    # Routing children are not produced as deletes by any path - their queries
    # are device-name-scoped, so an out-of-scope device's peers are never even
    # fetched - which means this is not an ordering problem that will resolve
    # itself on a later run. The reference is permanent until an operator acts.
    from dcim.models import Device

    protected_device_ids.update(
        _reference_protected_pks(
            Device,
            device_ids,
            ignore_labels=OWNERSHIP_RELEASED_ON_DEVICE_DELETE,
        )
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
    """Rows for every device identity this sync owns, plus name -> device pk.

    The pk map exists for the quarantine join: absence streaks are recorded
    against the Device row, while the sweep reasons in coalesce identities.
    """
    from ..models import ForwardDeviceIdentity

    rows = []
    device_id_by_name = {}
    identities = (
        ForwardDeviceIdentity.objects.filter(sync=sync)
        .order_by("source_device_key")
        .values(
            "source_device_key",
            "device_id",
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
        device_id_by_name[identity["source_device_key"]] = identity["device_id"]
        for field_set in coalesce_fields:
            row = {field: values.get(field) for field in field_set}
            if all(value not in (None, "") for value in row.values()):
                rows.append(row)
                break
    return rows, device_id_by_name


# The models whose durable-state delta may sweep a CATALOGUE - rows the sync
# did not write in this run and may never have written at all - for deletes.
# An explicit allowlist, because the sweep below deleted every row in the
# `SoftwareVersion` table that the current result did not name, operator-
# created rows included, and nothing named that as a policy: it was a branch
# keyed on a model string. This is the last whole-table delete producer now
# that the device sweep is quarantined, and it carries the rule its siblings
# do: name the allowlist, state which gate is not bypassed, pin the negative
# space with tests.
#
# The gate it does NOT bypass: `_locally_referenced_delete_identities` and the
# peer protection below still hold every candidate back while something
# references it. Attribution decides what is a CANDIDATE; reference protection
# decides what is deleted.
CATALOG_SWEEP_MODELS = frozenset({"netbox_dlm.softwareversion"})


def _software_version_catalog_rows(sync):
    """Software versions this sync can attribute to itself, as catalogue rows.

    A version is attributable when a `DeviceSoftware` or `InventoryItemSoftware`
    row on a device only this sync manages references it - the same
    attribution `_bootstrap_dlm_rows` uses for the association models. A
    version referenced by nothing, or only by another sync's devices, or only
    by an operator's, is not this sync's to sweep: it stays whatever the
    current result says.

    The table-wide enumeration this replaces deleted operator-created versions
    on every full run and was carried as open through three release plans.
    """
    device_ids = _sync_exclusive_device_ids(sync)
    if not device_ids:
        return []
    SoftwareVersion = apps.get_model("netbox_dlm", "SoftwareVersion")
    DeviceSoftware = apps.get_model("netbox_dlm", "DeviceSoftware")
    InventoryItemSoftware = apps.get_model("netbox_dlm", "InventoryItemSoftware")
    version_ids = set(
        DeviceSoftware.objects.filter(device_id__in=device_ids).values_list(
            "software_version_id", flat=True
        )
    )
    version_ids.update(
        InventoryItemSoftware.objects.filter(
            inventory_item__device_id__in=device_ids
        ).values_list("software_version_id", flat=True)
    )
    if not version_ids:
        return []
    return list(
        SoftwareVersion.objects.filter(pk__in=version_ids)
        .order_by("platform__slug", "version")
        .values("platform__slug", "version")
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
        ownership_quarantine_held = 0
        ownership_shrink_held = 0
        if model_string == "dcim.device" and (current_state is None or compatible):
            owned_rows, device_id_by_name = _owned_device_rows(sync, coalesce_fields)
            ownership_entries = build_state_entries(
                model_string,
                owned_rows,
                coalesce_fields,
            )
            absent_entries = {
                identity: value
                for identity, value in ownership_entries.items()
                if identity not in target_entries
            }
            # Absence from one result is not evidence. Deleting a device is the
            # one act in this function an operator cannot undo, and the operator
            # path - Prune orphans - already refuses to do it on a single
            # observation: it requires the absence QUARANTINE (consecutive
            # absent runs AND elapsed hours, both operator-tunable, fail-closed
            # for a device with no absence record), because a device disabled in
            # Forward is indistinguishable from one that was deleted. This sweep
            # deleted on the first absence, gated only by ownership claims - and
            # the scope claim is released by the same first observation, so the
            # gate evaporated exactly one run before the delete fired.
            #
            # Same act, same evidence bar: the sweep now deletes only identities
            # whose absence streak has cleared the same quarantine the prune
            # uses. The streaks are maintained for every sync by the post-sync
            # scope-tags job, and they lag this fetch by one run, which errs in
            # the only acceptable direction - later, never sooner.
            absent_ids = {
                device_id_by_name.get(str(value["row"].get("name") or "").strip())
                for value in absent_entries.values()
            } - {None}
            from .scope_reconciliation import partition_quarantined_orphans

            partition = partition_quarantined_orphans(sync, sorted(absent_ids))
            eligible_ids = set(partition["eligible_pks"])
            ownership_quarantine_held = len(absent_ids) - len(eligible_ids)
            # And the prune's other refusal, for the same reason it has one: a
            # query that returns most of the fleet passes every per-device test,
            # and every device missing from it then looks individually
            # deletable. A shrink past the ratio is treated as a scope or query
            # fault, not as attrition - unattended, there is no operator to ask,
            # so the answer is to delete nothing and let the streaks keep
            # advancing until someone looks.
            from .scope_reconciliation import SCOPE_SHRINK_REFUSAL_FLOOR
            from .scope_reconciliation import SCOPE_SHRINK_REFUSAL_RATIO

            previously_managed = len(ownership_entries)
            if (
                previously_managed
                and len(eligible_ids) > SCOPE_SHRINK_REFUSAL_FLOOR
                and len(eligible_ids) / previously_managed > SCOPE_SHRINK_REFUSAL_RATIO
            ):
                ownership_shrink_held = len(eligible_ids)
                eligible_ids = set()
            ownership_deletes = [
                value["row"]
                for identity, value in absent_entries.items()
                if device_id_by_name.get(str(value["row"].get("name") or "").strip())
                in eligible_ids
            ]
            ownership_delete_identities = {
                identity
                for identity, value in absent_entries.items()
                if device_id_by_name.get(str(value["row"].get("name") or "").strip())
                in eligible_ids
            }
            explicit_deletes = _deduplicate_rows(
                model_string,
                [*explicit_deletes, *ownership_deletes],
                coalesce_fields,
            )
        if model_string in CATALOG_SWEEP_MODELS and (
            current_state is None or compatible
        ):
            catalog_rows = [
                {
                    "platform_slug": row["platform__slug"],
                    "version": row["version"],
                }
                for row in _software_version_catalog_rows(sync)
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
                "ownership_quarantine_held_rows": ownership_quarantine_held,
                "ownership_shrink_held_rows": ownership_shrink_held,
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


def refused_delete_identities(ingestion):
    """The delete identities this ingestion staged but did not perform."""
    from .sync_reporting import REFUSED_DELETE_IDENTITIES_KEY

    recorded = (getattr(ingestion, "snapshot_info", None) or {}).get(
        REFUSED_DELETE_IDENTITIES_KEY
    ) or {}
    return {
        model_string: {identity for identity in identities if identity}
        for model_string, identities in recorded.items()
        if isinstance(identities, (list, tuple, set))
    }


def _without_refused_deletes(state, refused):
    """Drop the delete entries this run did not actually perform.

    The state is staged before the branch applies and promoted at merge, so it
    records the deletes the DELTA computed rather than the ones that happened.
    Promoting a refused delete as done is what made the next run skip it -
    `newly_explicit_deletes` treats a previous `delete` entry as settled - so
    the row stayed in NetBox, the plugin believed it was gone, and nothing
    retried. Dropping the entry instead leaves the identity absent from the
    promoted state, which is exactly the state the delta reads as "not yet
    deleted": the next run recomputes the delete and tries again.

    Returns the state unchanged when nothing was refused, so the ordinary path
    does no decode/encode work.
    """
    if not refused:
        return state
    entries = decode_state_entries(state.payload, state.payload_checksum)
    kept = {
        identity: value
        for identity, value in entries.items()
        if not (value["action"] == "delete" and identity in refused)
    }
    if len(kept) == len(entries):
        return state
    payload, checksum = encode_state_entries(kept)
    state.payload = payload
    state.payload_checksum = checksum
    state.row_count = len(kept)
    state.save(update_fields=["payload", "payload_checksum", "row_count"])
    return state


def promote_workload_states_locked(ingestion):
    from ..models import ForwardWorkloadState

    refused_by_model = refused_delete_identities(ingestion)
    pending = list(
        ForwardWorkloadState.objects.select_for_update()
        .filter(ingestion=ingestion)
        .order_by("model_string")
    )
    for state in pending:
        state = _without_refused_deletes(
            state, refused_by_model.get(state.model_string)
        )
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
    from .contributor_baseline import promote_contributor_baselines_fail_closed

    with transaction.atomic():
        locked_sync = ingestion.sync.__class__.objects.select_for_update().get(
            pk=ingestion.sync_id
        )
        locked_ingestion = ingestion.__class__.objects.select_for_update().get(
            pk=ingestion.pk
        )
        stage_workload_states(locked_ingestion, pending_states)
        promoted = promote_workload_states_locked(locked_ingestion)
        finalized_at = timezone.now()
        locked_ingestion.merge_applied_at = finalized_at
        locked_ingestion.save(update_fields=["merge_applied_at"])
        promote_contributor_baselines_fail_closed(
            locked_ingestion,
            logger=locked_sync.logger,
        )
        locked_ingestion.baseline_ready = True
        locked_ingestion.merge_finalized_at = finalized_at
        locked_ingestion.save(
            update_fields=[
                "baseline_ready",
                "merge_finalized_at",
            ]
        )
        ingestion.baseline_ready = True
        ingestion.merge_applied_at = finalized_at
        ingestion.merge_finalized_at = finalized_at
        ingestion.sync = locked_sync
        return promoted
