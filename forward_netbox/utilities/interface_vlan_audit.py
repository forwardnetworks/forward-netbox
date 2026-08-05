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


def audit_interface_untagged_vlans(*, sample_limit=25):
    """Every interface NetBox would refuse on its untagged VLAN.

    Counts are exact; the sampled rows are bounded, because the whole point is
    to be runnable on a deployment with tens of thousands of interfaces.
    """
    from dcim.models import Interface
    from django.db.models import F
    from django.db.models import Q

    assigned = Interface.objects.filter(untagged_vlan__isnull=False)

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
