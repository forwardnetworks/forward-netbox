# Shared device-scope reconciliation + orphan prune logic.
#
# Used by both the forward_device_scope_reconciliation_audit management command
# and the sync-detail UI panel so the CLI and UI always agree.
import heapq
import re
from datetime import datetime
from datetime import timezone as dt_timezone

from dcim.models import Device
from django.db import transaction
from django.db.models.deletion import ProtectedError
from django.utils import timezone

from .bulk_delete import lock_related_writes_for_delete
from .forward_api import build_device_tag_scope_where
from .forward_api import build_endpoint_device_eligibility_where
from .forward_api import build_endpoint_tag_scope_where
from .post_sync import current_post_sync_snapshot
from .sync_facade import device_tag_scope
from .sync_facade import effective_scope_endpoints_by_include_tags

SAMPLE_LIMIT = 25

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


def compute_scope_reconciliation(sync, *, snapshot_id=None) -> dict:
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
    snapshot_id = str(snapshot_id or "").strip() or sync.resolve_snapshot_id(client)
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
            "  tagNames: device.tagNames,",
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
    endpoint_names, endpoint_matched_tags = _endpoint_scope_names(
        sync,
        client=client,
        network_id=network_id,
        snapshot_id=snapshot_id,
        include_tags=include_tags,
        exclude_tags=exclude_tags,
        include_match=include_match,
    )

    from django.utils.text import slugify as _slugify

    row_by_name = {}
    matched_include_tags_by_name = {}
    include_tag_set = set(include_tags)
    device_tagged_names = set()
    device_completed_names = set()
    forward_site_slugs = set()
    for row in rows:
        name = str(row.get("name") or "").strip()
        if not name:
            continue
        device_tagged_names.add(name)
        row_by_name[name] = row
        matched_tags = sorted(
            include_tag_set.intersection(
                str(tag) for tag in (row.get("tagNames") or [])
            )
        )
        if matched_tags:
            matched_include_tags_by_name[name] = matched_tags
        if row.get("completed"):
            device_completed_names.add(name)
        loc = str(row.get("location") or "").strip()
        if loc:
            sl = _slugify(loc)
            if sl:
                forward_site_slugs.add(sl)
    backfilled_names = device_tagged_names - device_completed_names
    tagged_names = device_tagged_names | endpoint_names
    completed_names = device_completed_names | endpoint_names
    matched_include_tags_by_name.update(endpoint_matched_tags)

    netbox_names = {
        name
        for name in Device.objects.values_list("name", flat=True)
        if (name or "").strip()
    }

    from ..models import ForwardDeviceTagClaim

    previously_managed = list(
        ForwardDeviceTagClaim.objects.filter(sync=sync, claim_type="scope")
        .select_related("device")
        .values_list("device_id", "device__name")
    )
    previously_managed_names = {name for _, name in previously_managed}
    # A sync may classify only devices it previously claimed. Treating every
    # NetBox device absent from this sync as out of scope creates contradictory
    # negative claims in multi-source deployments.
    out_of_scope = (previously_managed_names & netbox_names) - tagged_names
    present_backfilled = netbox_names & backfilled_names
    missing_in_netbox = completed_names - netbox_names
    missing_scope_tag_targets = set(matched_include_tags_by_name) - netbox_names
    present_scope_tags_by_name = {
        name: tag_names
        for name, tag_names in matched_include_tags_by_name.items()
        if name in netbox_names
    }

    out_of_scope_pks = [
        device_id for device_id, name in previously_managed if name in out_of_scope
    ]

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
        "forward_in_scope_completed": len(device_completed_names),
        "forward_in_scope_endpoints": len(endpoint_names),
        "forward_tagged_backfilled": len(backfilled_names),
        "netbox_present_backfilled": len(present_backfilled),
        "netbox_out_of_scope": len(out_of_scope),
        "netbox_empty_orphan_site_count": len(empty_orphan_sites),
        "forward_missing_in_netbox": len(missing_in_netbox),
        "scope_tag_targets_missing_in_netbox": len(missing_scope_tag_targets),
        "backfilled_reason_breakdown": reason_breakdown,
        "out_of_scope_sample": sorted(out_of_scope)[:SAMPLE_LIMIT],
        "empty_orphan_site_sample": empty_orphan_sites[:SAMPLE_LIMIT],
        "present_backfilled_sample": sorted(present_backfilled)[:SAMPLE_LIMIT],
        "present_backfilled_detail_sample": present_backfilled_detail,
        "missing_in_netbox_sample": sorted(missing_in_netbox)[:SAMPLE_LIMIT],
        "scope_tag_targets_missing_sample": sorted(missing_scope_tag_targets)[
            :SAMPLE_LIMIT
        ],
        # Internal sets for prune/tag; not meant for JSON serialization.
        "_tagged_names": tagged_names,
        "_device_tagged_names": device_tagged_names,
        "_forward_site_slugs": forward_site_slugs,
        "_out_of_scope": out_of_scope,
        "_out_of_scope_pks": out_of_scope_pks,
        "_present_backfilled": present_backfilled,
        "_matched_include_tags_by_name": present_scope_tags_by_name,
    }


def _endpoint_scope_names(
    sync,
    *,
    client,
    network_id,
    snapshot_id,
    include_tags,
    exclude_tags,
    include_match,
) -> tuple[set[str], dict[str, list[str]]]:
    """Return endpoint-import names protected by reconciliation and prune."""
    source_parameters = dict(getattr(sync.source, "parameters", {}) or {})
    if not source_parameters.get("sync_endpoints"):
        return set(), {}

    endpoint_include_tags = (
        list(include_tags)
        if effective_scope_endpoints_by_include_tags(source_parameters)
        else []
    )
    query = "\n".join(
        [
            "foreach endpoint in network.endpoints",
            "where !isEmpty(endpoint.snmpOutputs)",
            *build_endpoint_tag_scope_where(
                endpoint_include_tags,
                exclude_tags,
                include_match,
            ),
            *build_endpoint_device_eligibility_where(
                sync_generic_endpoints=bool(
                    source_parameters.get("sync_generic_endpoints")
                )
            ),
            "select { name: endpoint.name, tagNames: endpoint.tagNames }",
        ]
    )
    rows = client.run_nqe_query(
        query=query,
        network_id=network_id,
        snapshot_id=snapshot_id,
        fetch_all=True,
    )
    names = {
        str(row.get("name") or "").strip()
        for row in rows
        if str(row.get("name") or "").strip()
    }
    include_tag_set = set(include_tags)
    matched = {}
    for row in rows:
        name = str(row.get("name") or "").strip()
        if not name:
            continue
        tag_names = sorted(
            include_tag_set.intersection(
                str(tag) for tag in (row.get("tagNames") or [])
            )
        )
        if tag_names:
            matched[name] = tag_names
    return names, matched


class EmptyForwardScopeError(RuntimeError):
    """Raised when an empty Forward scope would make a mutation unsafe."""


def _require_nonempty_forward_scope(report, *, operation):
    if not report.get("_tagged_names"):
        raise EmptyForwardScopeError(
            "The Forward scope query returned 0 devices or endpoints; refusing "
            f"to {operation} because every NetBox device would be treated as "
            "out of scope."
        )


def _prunable_device_order(device_ids):
    """Return child-before-parent order and fail-closed cyclic identities."""
    from ..models import ForwardVirtualParentClaim

    candidate_ids = set(device_ids)
    dependencies = {device_id: set() for device_id in candidate_ids}
    dependents = {device_id: set() for device_id in candidate_ids}
    for child_id, parent_id in ForwardVirtualParentClaim.objects.filter(
        device_id__in=candidate_ids,
        parent_device_id__in=candidate_ids,
    ).values_list("device_id", "parent_device_id"):
        if child_id == parent_id:
            dependencies[parent_id].add(child_id)
            continue
        dependencies[parent_id].add(child_id)
        dependents[child_id].add(parent_id)

    ready = [
        device_id
        for device_id, required_ids in dependencies.items()
        if not required_ids
    ]
    heapq.heapify(ready)
    ordered = []
    while ready:
        device_id = heapq.heappop(ready)
        ordered.append(device_id)
        for parent_id in sorted(dependents[device_id]):
            dependencies[parent_id].discard(device_id)
            if not dependencies[parent_id]:
                heapq.heappush(ready, parent_id)
    cyclic_ids = candidate_ids.difference(ordered)
    return ordered, cyclic_ids


def prune_orphan_devices(sync, *, report=None) -> dict:
    """Delete NetBox devices not present in the sync's Forward scope.

    Safety: refuses when the Forward query returned 0 devices. Tagged-but-
    backfilled devices are preserved. Returns counts. Pass ``report`` (from
    ``compute_scope_reconciliation``) to avoid re-running the Forward query.
    """
    if report is None:
        report = compute_scope_reconciliation(sync)
    out_of_scope = report["_out_of_scope"]
    if not report.get("_device_tagged_names", report["_tagged_names"]):
        raise EmptyForwardScopeError(
            "The Forward scope query returned 0 devices; refusing to prune because "
            "every NetBox device would be treated as an orphan."
        )
    if not out_of_scope:
        return {"pruned_device_count": 0, "out_of_scope_sample": []}

    orphans = sorted(out_of_scope)
    # Delete by the explicit device PKs resolved at scope-compute time
    # (identity-aware) rather than re-matching the non-unique device name at delete
    # time. Reports without exact identity evidence fail closed.
    orphan_pks = list(report.get("_out_of_scope_pks") or [])
    if not orphan_pks and orphans:
        raise ValueError(
            "Orphan prune requires exact device identity evidence from the current "
            "scope reconciliation report."
        )
    from .ownership import ownership_write_lock
    from .ownership import _release_prunable_device_ownership_locked

    deleted_total = 0
    pruned_device_ids = []
    protected_tally = {}
    ownership_blocked_ids = set()
    pending_device_ids = set(orphan_pks)
    while pending_device_ids:
        ordered_device_ids, cyclic_device_ids = _prunable_device_order(
            pending_device_ids
        )
        if cyclic_device_ids:
            ownership_blocked_ids.update(cyclic_device_ids)
            protected_tally["forward_netbox.forwardvirtualparentclaim"] = (
                protected_tally.get(
                    "forward_netbox.forwardvirtualparentclaim",
                    0,
                )
                + len(cyclic_device_ids)
            )
        retry_device_ids = set()
        pass_progress = False
        for device_id in ordered_device_ids:
            try:
                with ownership_write_lock():
                    release = _release_prunable_device_ownership_locked(
                        sync,
                        [device_id],
                    )
                    if release["blocked_device_ids"]:
                        retry_device_ids.add(device_id)
                        continue
                    lock_related_writes_for_delete(
                        Device,
                        using=Device.objects.db,
                    )
                    deleted, _ = Device.objects.filter(pk=device_id).delete()
                    deleted_total += deleted
                    pruned_device_ids.append(device_id)
                    pass_progress = True
            except ProtectedError as exc:
                for obj in exc.protected_objects:
                    label = obj._meta.label_lower
                    protected_tally[label] = protected_tally.get(label, 0) + 1
        if not retry_device_ids:
            break
        if not pass_progress:
            ownership_blocked_ids.update(retry_device_ids)
            break
        pending_device_ids = retry_device_ids
    result = {
        "pruned_device_count": len(pruned_device_ids),
        "pruned_object_count": deleted_total,
        "out_of_scope_sample": orphans[:SAMPLE_LIMIT],
        "ownership_blocked_device_count": len(ownership_blocked_ids),
        "protected_device_count": len(orphan_pks)
        - len(pruned_device_ids)
        - len(ownership_blocked_ids),
    }
    if protected_tally:
        result["protected_by_model"] = protected_tally
    return result


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
    if not report.get("_device_tagged_names", report["_tagged_names"]):
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


def _apply_maintained_device_tag(
    sync,
    device_names,
    *,
    slug,
    name,
    color,
    description,
    claim_type,
    generation,
    snapshot_id,
    mark_domain=True,
    materialize=True,
):
    """Reconcile one sync generation's claims for a maintained status tag."""
    from .ownership import reconcile_source_device_tag_claims

    result = reconcile_source_device_tag_claims(
        sync,
        device_names,
        slug=slug,
        name=name,
        color=color,
        description=description,
        claim_type=claim_type,
        generation=generation,
        snapshot_id=snapshot_id,
        mark_domain=mark_domain,
        materialize=materialize,
    )
    return {
        "added": result["assignments_added"],
        "removed": result["assignments_removed"],
        **result,
    }


def tag_backfilled_devices(
    sync,
    *,
    report=None,
    snapshot_id=None,
    ingestion_id=None,
) -> dict:
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
        report = compute_scope_reconciliation(sync, snapshot_id=snapshot_id)
    _require_nonempty_forward_scope(
        report,
        operation="maintain device scope tags",
    )

    with transaction.atomic(), current_post_sync_snapshot(
        sync,
        snapshot_id,
        ingestion_id=ingestion_id,
    ) as generation:
        backfilled = _apply_maintained_device_tag(
            sync,
            report["_present_backfilled"],
            slug=BACKFILLED_TAG_SLUG,
            name=BACKFILLED_TAG_NAME,
            color=BACKFILLED_TAG_COLOR,
            description=BACKFILLED_TAG_DESCRIPTION,
            claim_type="backfilled",
            generation=generation["generation"],
            snapshot_id=generation["snapshot_id"],
            mark_domain=False,
            materialize=False,
        )
        out_of_scope = _apply_maintained_device_tag(
            sync,
            report["_out_of_scope"],
            slug=OUT_OF_SCOPE_TAG_SLUG,
            name=OUT_OF_SCOPE_TAG_NAME,
            color=OUT_OF_SCOPE_TAG_COLOR,
            description=OUT_OF_SCOPE_TAG_DESCRIPTION,
            claim_type="out_of_scope",
            generation=generation["generation"],
            snapshot_id=generation["snapshot_id"],
            mark_domain=False,
            materialize=False,
        )
        source_parameters = getattr(sync.source, "parameters", None) or {}
        managed_scope_cleanup = {
            "claims_added": 0,
            "claims_released": 0,
            "assignments_added": 0,
            "assignments_removed": 0,
            "current": True,
        }
        from ..models import ForwardDeviceTagClaim
        from ..models import ForwardOwnershipReconciliation

        has_scope_ownership = (
            ForwardDeviceTagClaim.objects.filter(
                sync=sync,
                claim_type="scope",
            ).exists()
            or ForwardOwnershipReconciliation.objects.filter(
                sync=sync,
                domain=ForwardOwnershipReconciliation.Domain.SCOPE_TAGS,
            ).exists()
        )
        if source_parameters.get("apply_device_scope_tags") or has_scope_ownership:
            from .ownership import reconcile_sync_scope_tag_claims

            managed_scope_cleanup = reconcile_sync_scope_tag_claims(
                sync,
                (
                    report.get("_matched_include_tags_by_name", {})
                    if source_parameters.get("apply_device_scope_tags")
                    else {}
                ),
                generation=generation["generation"],
                snapshot_id=generation["snapshot_id"],
            )
        from .ownership import finalize_device_tag_domain

        status_materialized = finalize_device_tag_domain(
            sync,
            ForwardOwnershipReconciliation.Domain.STATUS_TAGS,
            generation["generation"],
            generation["snapshot_id"],
        )
    return {
        "tag_slug": BACKFILLED_TAG_SLUG,
        "tagged": status_materialized["by_claim_type"]
        .get("backfilled", {})
        .get("assignments_added", 0),
        "untagged": status_materialized["by_claim_type"]
        .get("backfilled", {})
        .get("assignments_removed", 0),
        "backfilled_claims_added": backfilled["claims_added"],
        "backfilled_claims_released": backfilled["claims_released"],
        "total_backfilled": backfilled["total"],
        "out_of_scope_tag_slug": OUT_OF_SCOPE_TAG_SLUG,
        "out_of_scope_tagged": status_materialized["by_claim_type"]
        .get("out_of_scope", {})
        .get("assignments_added", 0),
        "out_of_scope_untagged": status_materialized["by_claim_type"]
        .get("out_of_scope", {})
        .get("assignments_removed", 0),
        "out_of_scope_claims_added": out_of_scope["claims_added"],
        "out_of_scope_claims_released": out_of_scope["claims_released"],
        "total_out_of_scope": out_of_scope["total"],
        "scope_claims_released": managed_scope_cleanup["claims_released"],
        "out_of_scope_scope_tags_removed": managed_scope_cleanup["assignments_removed"],
        "scope_claims_added": managed_scope_cleanup["claims_added"],
        "scope_tags_added": managed_scope_cleanup["assignments_added"],
        "ownership_current": bool(
            status_materialized["current"] and managed_scope_cleanup["current"]
        ),
    }
