"""Shared device-scope reconciliation + orphan prune logic.

Used by both the `forward_device_scope_reconciliation_audit` management command
and the sync-detail UI panel so the CLI and UI always agree.
"""

from __future__ import annotations

from dcim.models import Device
from django.db import transaction

from .forward_api import build_device_tag_scope_where
from .sync_facade import device_tag_scope

SAMPLE_LIMIT = 25


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
            "== DeviceSnapshotResult.completed",
            "}",
        ]
    )
    rows = client.run_nqe_query(
        query=query,
        network_id=network_id,
        snapshot_id=snapshot_id,
        fetch_all=True,
    )

    tagged_names = {
        str(row.get("name") or "").strip()
        for row in rows
        if str(row.get("name") or "").strip()
    }
    completed_names = {
        str(row.get("name") or "").strip()
        for row in rows
        if row.get("completed") and str(row.get("name") or "").strip()
    }
    backfilled_names = tagged_names - completed_names

    netbox_names = {
        name
        for name in Device.objects.values_list("name", flat=True)
        if (name or "").strip()
    }

    out_of_scope = netbox_names - tagged_names
    present_backfilled = netbox_names & backfilled_names
    missing_in_netbox = completed_names - netbox_names

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
        "forward_missing_in_netbox": len(missing_in_netbox),
        "out_of_scope_sample": sorted(out_of_scope)[:SAMPLE_LIMIT],
        "present_backfilled_sample": sorted(present_backfilled)[:SAMPLE_LIMIT],
        "missing_in_netbox_sample": sorted(missing_in_netbox)[:SAMPLE_LIMIT],
        # Internal sets for prune; not meant for JSON serialization.
        "_tagged_names": tagged_names,
        "_out_of_scope": out_of_scope,
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
    deleted_total = 0
    with transaction.atomic():
        for start in range(0, len(orphans), 500):
            batch = orphans[start : start + 500]
            deleted, _ = Device.objects.filter(name__in=batch).delete()
            deleted_total += deleted
    return {
        "pruned_device_count": len(orphans),
        "pruned_object_count": deleted_total,
        "out_of_scope_sample": orphans[:SAMPLE_LIMIT],
    }
