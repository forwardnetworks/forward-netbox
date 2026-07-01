# Link virtual-context firewalls (Palo Alto vsys / Fortinet vdom) to their
# physical chassis. Forward collects each vsys/vdom as its own device whose
# `system.physicalName` names the physical chassis; a context is detectable when
# `physicalName` is present and differs from `device.name`. The sync imports all
# of them as flat NetBox devices, so this post-process stamps each virtual
# device's `forward_parent_device` object custom field (created in migration
# 0029) with its physical parent device. Non-destructive and idempotent:
# re-running reconciles the link, and a device that is no longer virtual (or
# whose parent left NetBox) is unlinked.
PARENT_DEVICE_CF = "forward_parent_device"


def _virtual_device_rows(sync, client=None, *, fetch_rows=None):
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
        snapshot_id = sync.resolve_snapshot_id(client)
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


def link_vsys_parents(sync, client=None, logger=None, *, fetch_rows=None) -> dict:
    """Model virtual-context firewalls (Palo vsys / Fortinet vdom) two ways, both
    additive and idempotent, never deleting a device:
    (1) set the ``forward_parent_device`` custom field to the chassis, and
    (2) create a NetBox ``VirtualDeviceContext`` for the context under the chassis.

    Returns counts: ``linked`` (CF set/changed), ``cleared`` (a device no longer a
    virtual context had its link removed), ``already`` (CF already correct),
    ``orphan_parent`` (chassis not in NetBox — skipped), ``vdc_created`` /
    ``vdc_existing`` (VirtualDeviceContexts created / already present).
    """
    from dcim.models import Device
    from dcim.models import VirtualDeviceContext

    pairs = _virtual_device_rows(sync, client, fetch_rows=fetch_rows)

    # name -> pk for every device we might touch (children + their parents).
    names = {name for name, _ in pairs} | {parent for _, parent in pairs}
    pk_by_name = dict(Device.objects.filter(name__in=names).values_list("name", "pk"))
    desired = {}  # child_pk -> parent_pk (parent must exist in NetBox)
    orphan_parent = 0
    for name, parent in pairs:
        child_pk = pk_by_name.get(name)
        if child_pk is None:
            continue  # virtual device not in NetBox (out of scope) — nothing to link
        parent_pk = pk_by_name.get(parent)
        if parent_pk is None:
            orphan_parent += 1
            continue
        desired[child_pk] = parent_pk

    linked = 0
    cleared = 0
    already = 0
    # Apply desired links.
    for device in Device.objects.filter(pk__in=desired):
        want = desired[device.pk]
        if device.custom_field_data.get(PARENT_DEVICE_CF) == want:
            already += 1
            continue
        device.custom_field_data[PARENT_DEVICE_CF] = want
        device.save()
        linked += 1
    # Self-heal: any device currently linked but no longer a desired child gets
    # its link cleared (it stopped being a virtual context, or its parent left).
    stale = Device.objects.filter(custom_field_data__has_key=PARENT_DEVICE_CF).exclude(
        pk__in=desired
    )
    for device in stale:
        if device.custom_field_data.get(PARENT_DEVICE_CF) in (None, ""):
            continue
        device.custom_field_data[PARENT_DEVICE_CF] = None
        device.save()
        cleared += 1

    # Also model each vsys/vdom as a NetBox VirtualDeviceContext (VDC) under its
    # physical chassis — the NetBox-native representation of a firewall context.
    # Additive + idempotent: create-or-find by (chassis device, context name); we
    # never delete a VDC (an operator may curate them) and never touch the vsys
    # device rows, so this is safe alongside the flat-device import.
    vdc_created = 0
    vdc_existing = 0
    for name, parent in pairs:
        parent_pk = pk_by_name.get(parent)
        if parent_pk is None or pk_by_name.get(name) is None:
            continue
        _, created = VirtualDeviceContext.objects.get_or_create(
            device_id=parent_pk,
            name=name,
            defaults={"status": "active"},
        )
        if created:
            vdc_created += 1
        else:
            vdc_existing += 1

    return {
        "cf": PARENT_DEVICE_CF,
        "virtual_devices": len(pairs),
        "linked": linked,
        "already": already,
        "cleared": cleared,
        "orphan_parent": orphan_parent,
        "vdc_created": vdc_created,
        "vdc_existing": vdc_existing,
    }
