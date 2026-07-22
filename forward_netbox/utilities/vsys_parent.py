# Link virtual-context firewalls (Palo Alto vsys / Fortinet vdom) to their
# physical chassis. Forward collects each vsys/vdom as its own device whose
# `system.physicalName` names the physical chassis; a context is detectable when
# `physicalName` is present and differs from `device.name`. The sync imports all
# of them as flat NetBox devices, so this post-process stamps each virtual
# device's `forward_parent_device` object custom field (created in migration
# 0029) with its physical parent device. Non-destructive and idempotent:
# re-running reconciles the link, and a device that is no longer virtual (or
# whose parent left NetBox) is unlinked.
from django.db import transaction


PARENT_DEVICE_CF = "forward_parent_device"


def _virtual_device_rows(sync, client=None, *, fetch_rows=None, snapshot_id=None):
    """Return [(name, physical_name), ...] for Forward virtual-context devices.

    ``fetch_rows(sync, client) -> list[dict]`` is injectable for tests.
    """
    if fetch_rows is not None:
        rows = fetch_rows(sync, client)
    else:
        network_id = sync.get_network_id()
        if not network_id:
            raise ValueError("Sync source has no network configured.")
        client = client or sync.source.get_client()
        snapshot_id = str(snapshot_id or "").strip() or sync.resolve_snapshot_id(client)
        query = "\n".join(
            [
                "foreach device in network.devices",
                "where isPresent(device.system.physicalName)"
                " && device.system.physicalName != device.name",
                "select {",
                "  name: device.name,",
                "  parent: device.system.physicalName",
                "}",
            ]
        )
        rows = client.run_nqe_query(
            query=query,
            network_id=network_id,
            snapshot_id=snapshot_id,
            fetch_all=True,
        )
    pairs = []
    for row in rows or []:
        name = str(row.get("name") or "").strip()
        parent = str(row.get("parent") or "").strip()
        if name and parent and name != parent:
            pairs.append((name, parent))
    return pairs


def link_vsys_parents(
    sync,
    client=None,
    logger=None,
    *,
    fetch_rows=None,
    snapshot_id=None,
    ingestion_id=None,
) -> dict:
    """Model virtual-context firewalls (Palo vsys / Fortinet vdom) two ways, both
    additive and idempotent, never deleting a device:
    (1) set the ``forward_parent_device`` custom field to the chassis, and
    (2) create a NetBox ``VirtualDeviceContext`` for the context under the chassis.

    Returns counts: ``linked`` (CF set/changed), ``cleared`` (a device no longer a
    virtual context had its link removed), ``already`` (CF already correct),
    ``orphan_parent`` (chassis not in NetBox — skipped), ``vdc_created`` /
    ``vdc_existing`` (VirtualDeviceContexts created / already present).
    """
    if (sync.parameters or {}).get("auto_link_vsys_parents") is False:
        pairs = []
    else:
        pairs = _virtual_device_rows(
            sync,
            client,
            fetch_rows=fetch_rows,
            snapshot_id=snapshot_id,
        )
    from .post_sync import current_post_sync_snapshot

    with transaction.atomic(), current_post_sync_snapshot(
        sync,
        snapshot_id,
        ingestion_id=ingestion_id,
    ) as generation:
        result = _apply_virtual_device_pairs(
            sync,
            pairs,
            generation=generation["generation"],
            snapshot_id=generation["snapshot_id"],
        )
    if result["conflicts"]:
        from .ownership import OwnershipConflictError

        raise OwnershipConflictError(
            "Virtual-parent claims disagree for one or more devices; durable "
            "claims were retained and the existing parent links were preserved."
        )
    return result


def _apply_virtual_device_pairs(
    sync,
    pairs,
    *,
    generation=None,
    snapshot_id=None,
):
    from .ownership import reconcile_virtual_parent_claims
    from .ownership import resolve_device_identities

    names = {name for name, _ in pairs} | {parent for _, parent in pairs}
    pk_by_name, missing, ambiguous = resolve_device_identities(
        sync,
        names,
        generation=generation,
        snapshot_id=snapshot_id,
    )
    desired = {}  # child_pk -> parent_pk (parent must exist in NetBox)
    orphan_parent = 0
    out_of_scope = 0
    for name, parent in pairs:
        child_pk = pk_by_name.get(name)
        if child_pk is None:
            out_of_scope += 1
            continue
        parent_pk = pk_by_name.get(parent)
        if parent_pk is None:
            orphan_parent += 1
            continue
        desired[child_pk] = parent_pk

    result = reconcile_virtual_parent_claims(
        sync,
        desired,
        generation=generation,
        snapshot_id=snapshot_id,
    )
    return {
        "cf": PARENT_DEVICE_CF,
        "virtual_devices": len(pairs),
        "orphan_parent": orphan_parent,
        "out_of_scope": out_of_scope,
        "unresolved_identity": len(missing),
        "ambiguous_identity": len(ambiguous),
        **result,
    }
