"""Find the interfaces NetBox will refuse on their untagged VLAN.

`Interface.clean()` raises on `untagged_vlan` for two unrelated reasons, and a
sync that hits either one records the rule but not the row: issue context is
reduced to key names before it is persisted, because persisted diagnostics carry
schema identifiers and never customer data. That is the right storage policy and
it leaves the operator unable to find one bad interface among tens of thousands.

This closes that gap the way the other audits do — computed on demand, written
to the operator's own console, persisted nowhere.

Both rules are reported because a sync cannot tell them apart at the point of
failure any better than the operator can:

- `cross_site` — `untagged_vlan.site` is neither the device's site nor null.
  Usually a device that moved sites: the plugin writes `site` on an existing
  device through `bulk_update`, which runs neither `save()` nor `clean()`, so
  nothing revalidates that device's interfaces and the VLAN of the old site
  stays behind.
- `no_mode` — an untagged VLAN on an interface with no 802.1Q mode. NetBox's own
  `save()` nulls the VLAN in that case, so this state is only reachable through
  a writer that bypasses it.
"""

CROSS_SITE_REMEDIATION = (
    "NetBox requires an interface's untagged VLAN to belong to the device's "
    "site or to be global (no site). Either move the VLAN to the device's "
    "site, clear the VLAN from the interface, or make the VLAN global. Until "
    "then the sync refuses to write these interfaces."
)
NO_MODE_REMEDIATION = (
    "NetBox does not accept an untagged VLAN on an interface with no 802.1Q "
    "mode. Either set the interface mode or clear the untagged VLAN. This "
    "pairing cannot be created through the NetBox UI or API, so it was written "
    "by something that bypassed model validation."
)


def _interface_row(interface):
    vlan = interface.untagged_vlan
    return {
        "device": interface.device.name,
        "interface": interface.name,
        "device_site": interface.device.site.name if interface.device.site else None,
        "vlan": vlan.name if vlan else None,
        "vlan_vid": vlan.vid if vlan else None,
        "vlan_site": vlan.site.name if vlan and vlan.site else None,
        "mode": interface.mode or None,
    }


def cross_site_untagged_vlan_interfaces(device_ids):
    """Interfaces on ``device_ids`` whose untagged VLAN belongs to another site.

    The exact predicate `Interface.clean()` refuses: the VLAN has a site, and it
    is not the device's. A global VLAN (no site) is valid anywhere.
    """
    from dcim.models import Interface
    from django.db.models import F

    return Interface.objects.filter(
        device_id__in=list(device_ids),
        untagged_vlan__isnull=False,
        untagged_vlan__site__isnull=False,
    ).exclude(untagged_vlan__site_id=F("device__site_id"))


def clear_cross_site_untagged_vlans(runner, device_ids, *, using=None):
    """Revalidate a device's interfaces after the plugin writes the device.

    The mechanism behind a customer's post-2.7.2 interface refusals: a device
    written with a new `site` keeps its interfaces' untagged VLANs from the old
    site, because neither `bulk_update` nor `save()` runs `Interface.clean()`,
    and the next interface sync is then refused on every one of them - by
    NetBox, correctly. Nothing detected the state at the point the plugin made
    it; the audit command found it afterwards, on request.

    Called from both device apply paths for every device they UPDATE, so it
    covers a move however the device came to match - the bulk path matches on
    `(name, site)` and cannot move a device, the row path can under a
    name-only coalesce, and an operator can move one by hand. Cheap: one query
    per batch on the bulk path, one per device on the row path.

    Clears only when this sync manages `dcim.interface`. Otherwise the
    interfaces are someone else's, so the state is reported and left alone -
    the audit's remediation text applies. When cleared, the next interface
    apply writes the VLAN Forward actually reports for the new site.
    """
    device_ids = [pk for pk in device_ids if pk is not None]
    if not device_ids:
        return {"cleared": 0, "reported": 0, "devices": []}
    offending = cross_site_untagged_vlan_interfaces(device_ids)
    if using:
        offending = offending.using(using)
    by_device = {}
    for device_id in offending.values_list("device_id", flat=True):
        by_device[device_id] = by_device.get(device_id, 0) + 1
    if not by_device:
        return {"cleared": 0, "reported": 0, "devices": []}
    total = sum(by_device.values())
    manages_interfaces = bool(
        getattr(getattr(runner, "sync", None), "is_model_enabled", lambda m: False)(
            "dcim.interface"
        )
    )
    # Device pks, not names: this reaches the job log and an issue row, and a
    # pk is an internal identifier where a name is customer data.
    devices = ", ".join(f"pk {pk} ({count})" for pk, count in sorted(by_device.items()))
    if manages_interfaces:
        offending.update(untagged_vlan=None)
        message = (
            f"Cleared {total} untagged VLAN(s) that belong to a different site "
            f"than their device on {len(by_device)} device(s) this sync wrote "
            f"({devices}). NetBox refuses an interface whose untagged VLAN is "
            "site-scoped to another site; the next interface sync writes the "
            "VLAN Forward reports for the device's current site."
        )
    else:
        message = (
            f"{total} interface(s) on {len(by_device)} device(s) this sync wrote "
            f"carry an untagged VLAN from a different site ({devices}). NetBox "
            "will refuse writes to those interfaces. Left in place because this "
            "sync does not manage dcim.interface; see "
            "forward_interface_vlan_audit for the rows and the remedy."
        )
    record = getattr(runner, "_record_aggregated_skip_warning", None)
    if callable(record):
        record(
            model_string="dcim.device",
            reason="cross-site-untagged-vlan",
            warning_message=message,
        )
    return {
        "cleared": total if manages_interfaces else 0,
        "reported": 0 if manages_interfaces else total,
        "devices": sorted(by_device),
    }


def audit_interface_untagged_vlans(*, sample_limit=25, owned_only=False):
    """Every interface NetBox would refuse on its untagged VLAN.

    Counts are exact; the sampled rows are bounded, because the whole point is
    to be runnable on a deployment with tens of thousands of interfaces.

    ``owned_only`` restricts the audit to devices some Forward sync created
    (holding a `ForwardDeviceIdentity`), so the count reads as "rows a sync
    will refuse" rather than every interface in NetBox.
    """
    from dcim.models import Interface
    from django.db.models import F
    from django.db.models import Q

    assigned = Interface.objects.filter(untagged_vlan__isnull=False)
    if owned_only:
        from ..models import ForwardDeviceIdentity

        assigned = assigned.filter(
            device_id__in=ForwardDeviceIdentity.objects.values("device_id")
        )

    # `untagged_vlan.site not in [device.site, None]` — a global VLAN is valid
    # on any device, which is why the null site is excluded rather than compared.
    cross_site = assigned.filter(untagged_vlan__site__isnull=False).exclude(
        untagged_vlan__site_id=F("device__site_id")
    )
    # `not mode and untagged_vlan`. Empty string and NULL both count as unset.
    no_mode = assigned.filter(Q(mode="") | Q(mode__isnull=True))

    limit = max(int(sample_limit or 0), 0)
    payload = {
        "cross_site_count": cross_site.count(),
        "no_mode_count": no_mode.count(),
        "sample_limit": limit,
        "owned_only": bool(owned_only),
        "cross_site": [],
        "no_mode": [],
    }
    if limit:
        related = ("device", "device__site", "untagged_vlan", "untagged_vlan__site")
        payload["cross_site"] = [
            _interface_row(interface)
            for interface in cross_site.select_related(*related).order_by(
                "device__name", "name"
            )[:limit]
        ]
        payload["no_mode"] = [
            _interface_row(interface)
            for interface in no_mode.select_related(*related).order_by(
                "device__name", "name"
            )[:limit]
        ]
    if payload["cross_site_count"]:
        payload["cross_site_remediation"] = CROSS_SITE_REMEDIATION
    if payload["no_mode_count"]:
        payload["no_mode_remediation"] = NO_MODE_REMEDIATION
    return payload
