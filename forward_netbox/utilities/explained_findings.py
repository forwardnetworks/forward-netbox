# Things that look wrong but may not be, each with a reason and what to do.
#
# A sync leaves behind counts an operator cannot read at a glance: devices with
# no primary IP, devices marked uncovered, addresses assigned to nothing. A
# customer asked what each was before anyone could say whether it mattered. Every
# row here carries a reason in plain words, whether it is fixable, and the next
# action, so the question is answered where the count appears. Counts only.
from .primary_ip_reasons import BY_DESIGN
from .primary_ip_reasons import NEEDS_A_PERSON


def _row(finding, count, reason, fixable, action):
    return {
        "finding": finding,
        "count": count,
        "reason": reason,
        "fixable": fixable,
        "action": action,
    }


def _primary_ip_rows(sync):
    from dcim.models import Device

    from ..models import ForwardDeviceIdentity
    from .primary_ip import primary_ip_from_management_ip_enabled
    from .primary_ip import primary_ip_from_mgmt_tag_enabled
    from .primary_ip_reasons import primary_ip_reasons

    settings_on = primary_ip_from_mgmt_tag_enabled(
        sync
    ) and primary_ip_from_management_ip_enabled(sync)
    bound = ForwardDeviceIdentity.objects.filter(sync=sync).values_list(
        "device_id", flat=True
    )
    missing = Device.objects.filter(
        pk__in=bound, primary_ip4__isnull=True, primary_ip6__isnull=True
    )
    rows = []
    for item in primary_ip_reasons(missing, settings_on=settings_on):
        rows.append(
            _row(
                "Devices without a primary IP (this sync)",
                item["count"],
                item["why"],
                item["fixable"],
                item["what_to_do"],
            )
        )
    return rows


def _uncovered_row():
    from dcim.models import Device

    from .scope_reconciliation import UNCOVERED_TAG_SLUG

    count = Device.objects.filter(tags__slug=UNCOVERED_TAG_SLUG).count()
    if not count:
        return None
    return _row(
        "Uncovered devices (all of NetBox)",
        count,
        "NetBox holds the device but a sync's latest Forward result does not "
        "return it, so nothing updates it any more (it left the device-tag scope, "
        "was disabled, or was never collected).",
        NEEDS_A_PERSON,
        "Open Scope Reconciliation. Prune the ones that are gone, or put the "
        "Forward tag back on the ones that should still sync.",
    )


def _unassigned_ip_row():
    from .bgp_neighbour_ips import bgp_neighbour_ip_addresses
    from .bgp_neighbour_ips import routing_installed
    from .bgp_neighbour_ips import unassigned_ip_addresses

    finding = "IP addresses assigned to nothing (all of NetBox)"
    unassigned = unassigned_ip_addresses()
    total = unassigned.count()
    if not total:
        return []
    needed = (
        bgp_neighbour_ip_addresses(unassigned).count() if routing_installed() else 0
    )
    rows = []
    if needed:
        rows.append(
            _row(
                finding,
                needed,
                "A BGP peer's neighbour is a remote device, so the sync holds its "
                "address with no interface. Expected, not an orphan.",
                BY_DESIGN,
                "Run `manage.py forward_tag_bgp_neighbours --user <name>` to tag "
                "them so they can be filtered out of IPAM.",
            )
        )
    if total - needed > 0:
        rows.append(
            _row(
                finding,
                total - needed,
                "No interface and no BGP peer uses the address, so it may be a "
                "real orphan (or one added by hand).",
                NEEDS_A_PERSON,
                "Review them in IPAM and delete the ones nothing needs.",
            )
        )
    return rows


def explained_findings(sync):
    """Every explained finding for a sync; empty when there is nothing to explain."""
    rows = _primary_ip_rows(sync)
    uncovered = _uncovered_row()
    if uncovered:
        rows.append(uncovered)
    rows.extend(_unassigned_ip_row())
    return rows
