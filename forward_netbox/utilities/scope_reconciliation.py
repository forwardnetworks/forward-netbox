# Shared device-scope reconciliation + orphan prune logic.
#
# Used by both the forward_device_scope_reconciliation_audit management command
# and the sync-detail UI panel so the CLI and UI always agree.
import re
from datetime import datetime
from datetime import timezone as dt_timezone

from dcim.models import Device
from django.db import transaction
from django.db.models.deletion import ProtectedError
from django.utils import timezone

from .branch_budget import DELETE_DEPENDENCY_MODEL_RANK
from .forward_api import build_device_tag_scope_where
from .sync_facade import device_tag_scope

SAMPLE_LIMIT = 25

# Protector-sweep bound: each iteration deletes at least one full layer of
# PROTECT-ing rows (peers -> address families -> ...), so real chains resolve in
# a handful of passes; the cap only guards against a pathological cycle.
PRUNE_PROTECTOR_SWEEP_LIMIT = 20

# Forward renders a failed collection result as
# ``DeviceSnapshotResult.collectionFailed(DeviceCollectionError.AUTHENTICATION_FAILED)``.
# Pull the specific DeviceCollectionError token so operators can see *why* a
# device is backfilled (auth vs timeout vs incomplete setup) without a manual
# Forward API probe.
_COLLECTION_ERROR_RE = re.compile(r"DeviceCollectionError\.([A-Za-z0-9_]+)")


def _collection_failure_reason(reason_str):
    """Map a stringified ``device.snapshotInfo.result`` to a short reason token.

    ``DeviceSnapshotResult.collectionFailed(DeviceCollectionError.X)`` -> ``X``;
    ``DeviceSnapshotResult.completed`` -> ``completed``; anything unparseable
    (including a missing reason on older payloads) -> ``unknown``.
    """
    if not reason_str:
        return "unknown"
    match = _COLLECTION_ERROR_RE.search(str(reason_str))
    if match:
        return match.group(1)
    token = str(reason_str).rsplit(".", 1)[-1].strip()
    return token or "unknown"


def _stale_days(ts_str):
    """Whole days between an ISO collection/backfill timestamp and now.

    Returns ``None`` when the timestamp is missing or unparseable so callers can
    render a placeholder instead of a misleading ``0``.
    """
    if not ts_str:
        return None
    try:
        parsed = datetime.fromisoformat(str(ts_str).replace("Z", "+00:00"))
    except (ValueError, TypeError):
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=dt_timezone.utc)
    return max(0, (timezone.now() - parsed).days)


# NetBox tag applied to devices that are tagged-in-scope but were backfilled
# (not freshly collected) in the latest Forward snapshot, so operators can find
# them with a normal device-list filter (?tag=forward-backfilled).
BACKFILLED_TAG_SLUG = "forward-backfilled"
BACKFILLED_TAG_NAME = "Forward Backfilled"
BACKFILLED_TAG_COLOR = "ffc107"
BACKFILLED_TAG_DESCRIPTION = (
    "Tagged in scope but backfilled (not freshly collected) in the latest "
    "Forward snapshot. Maintained by the Forward sync scope reconciliation."
)

# NetBox tag applied to devices that match NONE of the sync's included Forward
# tags (out of scope). Unlike backfilled devices (in scope, kept), these are the
# removable orphans — review and delete them via Scope Reconciliation -> Prune
# orphans. Maintained alongside the backfilled tag so operators can filter
# /dcim/devices/?tag=forward-out-of-scope.
OUT_OF_SCOPE_TAG_SLUG = "forward-out-of-scope"
OUT_OF_SCOPE_TAG_NAME = "Forward Out Of Scope"
OUT_OF_SCOPE_TAG_COLOR = "f44336"
OUT_OF_SCOPE_TAG_DESCRIPTION = (
    "Matches none of the sync's included Forward device tags (out of scope). "
    "Removable via Scope Reconciliation -> Prune orphans. Maintained by the "
    "Forward sync scope reconciliation."
)


def compute_scope_reconciliation(sync) -> dict:
    """Compare NetBox devices against the sync's Forward device tag scope.

    Returns counts plus the resolved sets (so callers can prune). Raises the
    underlying client/query exception on failure.
    """
    network_id = sync.get_network_id()
    if not network_id:
        raise ValueError("Sync source has no network configured.")

    include_tags, exclude_tags, include_match = device_tag_scope(sync)
    scope_where = build_device_tag_scope_where(
        include_tags, exclude_tags, include_match
    )

    client = sync.source.get_client()
    snapshot_id = sync.resolve_snapshot_id(client)
    query = "\n".join(
        [
            "foreach device in network.devices",
            "where device.platform.vendor != Vendor.FORWARD_CUSTOM",
            *scope_where,
            "select {",
            "  name: device.name,",
            "  completed: device.snapshotInfo.result "
            "== DeviceSnapshotResult.completed,",
            "  reason: toString(device.snapshotInfo.result),",
            "  collectionTime: device.snapshotInfo.collectionTime,",
            "  backfillTime: device.snapshotInfo.backfillTime,",
            '  location: if isPresent(device.locationName) then toLowerCase(device.locationName) else ""',
            "}",
        ]
    )
    rows = client.run_nqe_query(
        query=query,
        network_id=network_id,
        snapshot_id=snapshot_id,
        fetch_all=True,
    )

    from django.utils.text import slugify as _slugify

    row_by_name = {}
    tagged_names = set()
    completed_names = set()
    forward_site_slugs = set()
    for row in rows:
        name = str(row.get("name") or "").strip()
        if not name:
            continue
        tagged_names.add(name)
        row_by_name[name] = row
        if row.get("completed"):
            completed_names.add(name)
        loc = str(row.get("location") or "").strip()
        if loc:
            sl = _slugify(loc)
            if sl:
                forward_site_slugs.add(sl)
    backfilled_names = tagged_names - completed_names

    netbox_names = {
        name
        for name in Device.objects.values_list("name", flat=True)
        if (name or "").strip()
    }

    out_of_scope = netbox_names - tagged_names
    present_backfilled = netbox_names & backfilled_names
    missing_in_netbox = completed_names - netbox_names

    # Identity-aware: resolve the out-of-scope device NAMES to explicit PKs here, at
    # scope-compute time, so downstream prune deletes exactly these rows. Device
    # names are not globally unique, so re-matching by name at delete time is
    # fragile. NOTE: scope MEMBERSHIP is still name-keyed — distinguishing two
    # same-named devices in different sites needs a Forward location -> NetBox site
    # mapping (a separate, larger change); until then a name present in Forward
    # scope conservatively protects every NetBox device with that name.
    out_of_scope_pks = list(
        Device.objects.filter(name__in=out_of_scope).values_list("pk", flat=True)
    )

    # Why are the in-scope devices backfilled? Group by the Forward collection
    # error so operators can act (rotate creds for AUTHENTICATION_FAILED, check
    # reachability for CONNECTION_TIMEOUT, finish onboarding for INCOMPLETE_SETUP)
    # without running a manual probe.
    reason_breakdown = {}
    for name in backfilled_names:
        reason = _collection_failure_reason((row_by_name.get(name) or {}).get("reason"))
        reason_breakdown[reason] = reason_breakdown.get(reason, 0) + 1
    reason_breakdown = dict(
        sorted(reason_breakdown.items(), key=lambda kv: (-kv[1], kv[0]))
    )

    present_backfilled_detail = []
    for name in sorted(present_backfilled)[:SAMPLE_LIMIT]:
        row = row_by_name.get(name) or {}
        present_backfilled_detail.append(
            {
                "name": name,
                "reason": _collection_failure_reason(row.get("reason")),
                "stale_days": _stale_days(
                    row.get("backfillTime") or row.get("collectionTime")
                ),
            }
        )

    # Compute empty orphan sites for the preview (current DB state; prune re-queries
    # after device deletion so sites that become empty then are also removed).
    from dcim.models import Site

    if forward_site_slugs:
        occupied_site_ids = _occupied_site_ids()
        empty_orphan_sites = list(
            Site.objects.exclude(slug__in=forward_site_slugs)
            .exclude(pk__in=occupied_site_ids)
            .values_list("name", flat=True)
            .order_by("name")
        )
    else:
        empty_orphan_sites = []

    return {
        "sync_id": sync.pk,
        "sync_name": sync.name,
        "snapshot_selector": sync.get_snapshot_id(),
        "include_tags": sorted(include_tags),
        "exclude_tags": sorted(exclude_tags),
        "include_match": include_match,
        "netbox_device_count": len(netbox_names),
        "forward_in_scope_completed": len(completed_names),
        "forward_tagged_backfilled": len(backfilled_names),
        "netbox_present_backfilled": len(present_backfilled),
        "netbox_out_of_scope": len(out_of_scope),
        "netbox_empty_orphan_site_count": len(empty_orphan_sites),
        "forward_missing_in_netbox": len(missing_in_netbox),
        "backfilled_reason_breakdown": reason_breakdown,
        "out_of_scope_sample": sorted(out_of_scope)[:SAMPLE_LIMIT],
        "empty_orphan_site_sample": empty_orphan_sites[:SAMPLE_LIMIT],
        "present_backfilled_sample": sorted(present_backfilled)[:SAMPLE_LIMIT],
        "present_backfilled_detail_sample": present_backfilled_detail,
        "missing_in_netbox_sample": sorted(missing_in_netbox)[:SAMPLE_LIMIT],
        # Internal sets for prune/tag; not meant for JSON serialization.
        "_tagged_names": tagged_names,
        "_forward_site_slugs": forward_site_slugs,
        "_out_of_scope": out_of_scope,
        "_out_of_scope_pks": out_of_scope_pks,
        "_present_backfilled": present_backfilled,
    }


class EmptyForwardScopeError(RuntimeError):
    """Raised when the Forward scope query returns no devices, so pruning would
    treat every NetBox device as an orphan."""


def prune_orphan_devices(sync, *, report=None) -> dict:
    """Delete NetBox devices not present in the sync's Forward scope.

    Safety: refuses when the Forward query returned 0 devices. Tagged-but-
    backfilled devices are preserved. Returns counts. Pass ``report`` (from
    ``compute_scope_reconciliation``) to avoid re-running the Forward query.
    """
    if report is None:
        report = compute_scope_reconciliation(sync)
    out_of_scope = report["_out_of_scope"]
    if not out_of_scope:
        return {"pruned_device_count": 0, "out_of_scope_sample": []}
    if not report["_tagged_names"]:
        raise EmptyForwardScopeError(
            "The Forward scope query returned 0 devices; refusing to prune because "
            "every NetBox device would be treated as an orphan."
        )

    orphans = sorted(out_of_scope)
    # Delete by the explicit device PKs resolved at scope-compute time
    # (identity-aware) rather than re-matching the non-unique device name at delete
    # time. Fall back to a name resolution if an older report without PKs is passed.
    orphan_pks = list(report.get("_out_of_scope_pks") or [])
    if not orphan_pks and orphans:
        orphan_pks = list(
            Device.objects.filter(name__in=orphans).values_list("pk", flat=True)
        )
    deleted_total, protector_tally = _delete_devices_sweeping_protectors(orphan_pks)
    result = {
        "pruned_device_count": len(orphans),
        "pruned_object_count": deleted_total,
        "out_of_scope_sample": orphans[:SAMPLE_LIMIT],
    }
    if protector_tally:
        result["pruned_dependent_rows"] = protector_tally
    return result


def _group_protected_objects_by_rank(protected_objects):
    """Group PROTECT-ing rows by model, children-first.

    Ordered by ``DELETE_DEPENDENCY_MODEL_RANK`` so dependent plugin rows (e.g.
    a peering session) delete before the rows they themselves protect (e.g. a
    BGP peer); unknown models sort last.
    """
    groups: dict[str, list] = {}
    for obj in protected_objects or ():
        groups.setdefault(obj._meta.label_lower, []).append(obj)
    return sorted(
        groups.items(),
        key=lambda item: DELETE_DEPENDENCY_MODEL_RANK.get(item[0], 10_000),
    )


def _delete_devices_sweeping_protectors(orphan_pks):
    """Delete devices by PK, sweeping PROTECT-ing rows that block the cascade.

    Optional plugins hold ``on_delete=PROTECT`` references into a pruned
    device's cascade set — e.g. netbox_routing ``BGPPeer.peer``/``source``
    point at the device's interface IPs (field report: the prune job failed
    wholesale with ProtectedError listing BGP peers). Django hands us exactly
    the blocking rows in ``exc.protected_objects``, so the sweep is
    plugin-agnostic: no plugin imports, and a row referencing only in-scope
    devices never appears. A blocker owned by an in-scope neighbor (its
    ``peer`` FK targets a pruned device's IP) is deleted too — its FK target
    must go, and the next sync recreates it from Forward data.

    One transaction per batch (not one for the whole prune) so a single stuck
    batch cannot roll back every other deletion.
    """
    deleted_total = 0
    protector_tally: dict[str, int] = {}
    for start in range(0, len(orphan_pks), 500):
        batch = orphan_pks[start : start + 500]
        with transaction.atomic():
            for _attempt in range(PRUNE_PROTECTOR_SWEEP_LIMIT):
                try:
                    deleted, _ = Device.objects.filter(pk__in=batch).delete()
                    deleted_total += deleted
                    break
                except ProtectedError as exc:
                    for label, objects in _group_protected_objects_by_rank(
                        exc.protected_objects
                    ):
                        model = objects[0].__class__
                        pks = [obj.pk for obj in objects]
                        try:
                            swept, _ = model.objects.filter(pk__in=pks).delete()
                        except ProtectedError:
                            # This layer is itself protected (e.g. a peering
                            # session guards a BGP peer); the next iteration's
                            # ProtectedError surfaces the outer layer first.
                            continue
                        if swept:
                            protector_tally[label] = (
                                protector_tally.get(label, 0) + swept
                            )
            else:
                raise ProtectedError(
                    "Pruning devices kept hitting protected references after "
                    f"{PRUNE_PROTECTOR_SWEEP_LIMIT} sweep passes "
                    f"(swept so far: {protector_tally or 'none'}).",
                    set(),
                )
    return deleted_total, protector_tally


def _occupied_site_ids() -> set:
    """Site PKs referenced by ANY related object (FK), across every relation.

    A site is "truly empty" only when nothing points to it. We union the site
    foreign keys of every reverse relation (devices, racks, prefixes, VLANs, VMs,
    power panels, locations, clusters, wireless LANs, circuit/cable terminations,
    …) rather than just devices+racks. This matters for two reasons NetBox's own
    FK ``on_delete`` rules impose:
      * PROTECT (Device, Rack, PowerPanel, VLAN, VirtualMachine) — deleting a site
        that still has one of these raises ``ProtectedError``.
      * CASCADE (Prefix, Location, Cluster, WirelessLAN, CircuitTermination) —
        deleting the site would silently destroy those children.
    Either way such a site is not "truly empty" and must be kept. Many-to-many
    relations (e.g. ConfigContext.sites) do not pin a site and are skipped.
    """
    from dcim.models import Site

    occupied = set()
    for rel in Site._meta.related_objects:
        if rel.many_to_many:
            continue
        attname = rel.field.attname  # e.g. "site_id" / "_site_id"
        occupied.update(
            rel.related_model.objects.exclude(**{attname: None}).values_list(
                attname, flat=True
            )
        )
    occupied.discard(None)
    return occupied


def prune_orphan_sites(sync, *, report=None) -> dict:
    """Delete truly-empty NetBox sites absent from the sync's Forward location scope.

    Only removes sites that nothing references (no devices, racks, prefixes, VLANs,
    VMs, power panels, locations, clusters, …) — see ``_occupied_site_ids``. A site
    with any remaining object is kept, so the prune neither hits a NetBox PROTECT
    error nor cascade-deletes child objects. Re-queries current DB state so sites
    emptied by the device prune in the same job are also removed. Deletes one site
    at a time and skips any that unexpectedly raise ``ProtectedError`` so a single
    surprise relation cannot abort the whole prune. Safety: refuses when the
    Forward scope returned 0 devices or no location data.
    """
    from django.db.models.deletion import ProtectedError

    from dcim.models import Site

    if report is None:
        report = compute_scope_reconciliation(sync)
    if not report["_tagged_names"]:
        raise EmptyForwardScopeError(
            "Forward scope returned 0 devices; refusing site prune."
        )
    forward_site_slugs = report.get("_forward_site_slugs") or set()
    if not forward_site_slugs:
        return {"pruned_site_count": 0, "pruned_site_object_count": 0, "skipped": 0}
    occupied_site_ids = _occupied_site_ids()
    prunable_pks = list(
        Site.objects.exclude(slug__in=forward_site_slugs)
        .exclude(pk__in=occupied_site_ids)
        .values_list("pk", flat=True)
    )
    if not prunable_pks:
        return {"pruned_site_count": 0, "pruned_site_object_count": 0, "skipped": 0}
    pruned_sites = 0
    pruned_objects = 0
    skipped = 0
    for pk in prunable_pks:
        try:
            with transaction.atomic():
                deleted, _ = Site.objects.filter(pk=pk).delete()
            pruned_sites += 1
            pruned_objects += deleted
        except ProtectedError:
            # A relation not covered by the occupancy union still pins this site;
            # leave it rather than fail the whole prune.
            skipped += 1
    return {
        "pruned_site_count": pruned_sites,
        "pruned_site_object_count": pruned_objects,
        "skipped": skipped,
    }


def _apply_maintained_device_tag(device_names, *, slug, name, color, description):
    """Make the tag's device set exactly ``device_names`` (idempotent).

    Adds the tag to devices in the set that lack it and removes it from devices
    that carry it but are no longer in the set. Returns ``{added, removed, total}``.
    """
    from extras.models import Tag

    tag, _ = Tag.objects.get_or_create(
        slug=slug,
        defaults={"name": name, "color": color, "description": description},
    )
    want_ids = set(
        Device.objects.filter(name__in=device_names).values_list("pk", flat=True)
        if device_names
        else []
    )
    currently_tagged_ids = set(
        Device.objects.filter(tags__slug=slug).values_list("pk", flat=True)
    )
    added = 0
    removed = 0
    with transaction.atomic():
        for device in Device.objects.filter(pk__in=want_ids - currently_tagged_ids):
            device.tags.add(tag)
            added += 1
        for device in Device.objects.filter(pk__in=currently_tagged_ids - want_ids):
            device.tags.remove(tag)
            removed += 1
    return {"added": added, "removed": removed, "total": len(want_ids)}


def tag_backfilled_devices(sync, *, report=None) -> dict:
    """Maintain the ``forward-backfilled`` and ``forward-out-of-scope`` device tags.

    ``forward-backfilled`` marks devices that are tagged-in-scope but were not
    freshly collected in the latest snapshot (kept on purpose).
    ``forward-out-of-scope`` marks NetBox devices that match none of the sync's
    included Forward tags (the removable orphans). Both are idempotent — after
    running, each tag's device set exactly matches the current bucket, so operators
    can filter ``/dcim/devices/?tag=forward-backfilled`` or
    ``?tag=forward-out-of-scope``.
    """
    if report is None:
        report = compute_scope_reconciliation(sync)

    backfilled = _apply_maintained_device_tag(
        report["_present_backfilled"],
        slug=BACKFILLED_TAG_SLUG,
        name=BACKFILLED_TAG_NAME,
        color=BACKFILLED_TAG_COLOR,
        description=BACKFILLED_TAG_DESCRIPTION,
    )
    out_of_scope = _apply_maintained_device_tag(
        report["_out_of_scope"],
        slug=OUT_OF_SCOPE_TAG_SLUG,
        name=OUT_OF_SCOPE_TAG_NAME,
        color=OUT_OF_SCOPE_TAG_COLOR,
        description=OUT_OF_SCOPE_TAG_DESCRIPTION,
    )
    return {
        "tag_slug": BACKFILLED_TAG_SLUG,
        "tagged": backfilled["added"],
        "untagged": backfilled["removed"],
        "total_backfilled": backfilled["total"],
        "out_of_scope_tag_slug": OUT_OF_SCOPE_TAG_SLUG,
        "out_of_scope_tagged": out_of_scope["added"],
        "out_of_scope_untagged": out_of_scope["removed"],
        "total_out_of_scope": out_of_scope["total"],
    }
