# Why a device has no primary IP, one reason per device, and whether anything can
# be done about it.
#
# A customer's bundle said 844 devices had no primary IP, and the fallback summary
# accounted for 648 of them. The rest were left to a person to work out. Every
# device is given exactly one reason here from what NetBox holds, so the count
# reconciles and the ones nothing can fix are named as such instead of reading as
# a failure. Counts only.
from collections import Counter

FIXABLE = "yes"
BY_DESIGN = "no, by design"
NEEDS_A_PERSON = "needs a person"

# (reason, fixable, why it has no primary IP, what to do about it)
REASONS = (
    (
        "not_refreshed_by_forward",
        NEEDS_A_PERSON,
        "The device is tagged forward-uncovered: Forward no longer returns it, or it "
        "left this sync's device-tag scope. Nothing refreshes its addresses, so none "
        "can be promoted.",
        "List the uncovered devices in Counts explained to see which, then bring it "
        "back into scope or prune it in Scope Reconciliation.",
    ),
    (
        "shares_an_address_with_another_device",
        BY_DESIGN,
        "Every address it holds is already another device's primary IP, or is held "
        "by another device without one, and NetBox allows one primary-IP owner per "
        "address (virtual systems and HA pairs report their parent's address).",
        "Nothing to do. Set a different address by hand if it needs its own.",
    ),
    (
        "has_an_address_but_none_was_chosen",
        FIXABLE,
        "It holds interface addresses but none became primary.",
        "Turn on the primary-IP settings, or give the device a Mgmt_ tag in Forward.",
    ),
    (
        "interfaces_carry_no_address",
        NEEDS_A_PERSON,
        "It has interfaces but none carries an address in NetBox. Either Forward "
        "gives the device no interface address (some firewalls have a management "
        "address but none on an interface), or every address it has also belongs "
        "to another device (virtual systems and HA peers) and NetBox gives each "
        "address to one device.",
        "Usually nothing to fix. If the device must show an address, add it to an "
        "interface by hand and set it as primary.",
    ),
    (
        "no_interfaces_synced",
        NEEDS_A_PERSON,
        "No interfaces were synced for it, so no address could be.",
        "Check that Forward collected the device (a collection failure leaves it empty).",
    ),
)


def _host(address):
    return str(getattr(address, "ip", address) or "")


ALREADY_ON_ADVICE = (
    "The primary-IP settings are already on, so look in the ingestion job log for "
    "the management-address summary, or give the device a Mgmt_ tag in Forward."
)


def classify_primary_ip_devices(missing):
    """``{device pk: reason}`` for the devices in ``missing`` (no primary IP).

    Every device lands in exactly one reason, in the order of ``REASONS``.
    """
    from dcim.models import Device
    from dcim.models import Interface
    from django.contrib.contenttypes.models import ContentType
    from django.db.models import Q
    from ipam.models import IPAddress

    from .scope_reconciliation import UNCOVERED_TAG_SLUG

    device_ids = set(missing.values_list("pk", flat=True))
    if not device_ids:
        return {}
    uncovered = set(
        missing.filter(tags__slug=UNCOVERED_TAG_SLUG).values_list("pk", flat=True)
    )
    with_interfaces = set(
        Interface.objects.filter(device_id__in=device_ids)
        .values_list("device_id", flat=True)
        .distinct()
    )
    interface_type = ContentType.objects.get_for_model(Interface)
    held_by_device = {}
    for device_id, address in IPAddress.objects.filter(
        assigned_object_type=interface_type,
        interface__device_id__in=device_ids,
    ).values_list("interface__device_id", "address"):
        held_by_device.setdefault(device_id, set()).add(_host(address))
    primary_hosts = {
        _host(address)
        for address in IPAddress.objects.filter(
            Q(
                pk__in=Device.objects.exclude(primary_ip4__isnull=True).values(
                    "primary_ip4_id"
                )
            )
            | Q(
                pk__in=Device.objects.exclude(primary_ip6__isnull=True).values(
                    "primary_ip6_id"
                )
            )
        ).values_list("address", flat=True)
    }
    # An address two of these devices both hold is shared too: NetBox lets only one
    # of them own it as a primary.
    devices_by_host = {}
    for device_id, hosts in held_by_device.items():
        for host in hosts:
            devices_by_host.setdefault(host, set()).add(device_id)
    colliding = primary_hosts | {
        host for host, owners in devices_by_host.items() if len(owners) > 1
    }

    classified = {}
    for device_id in device_ids:
        held = held_by_device.get(device_id, set())
        if device_id in uncovered:
            reason = "not_refreshed_by_forward"
        elif held and held <= colliding:
            reason = "shares_an_address_with_another_device"
        elif held:
            reason = "has_an_address_but_none_was_chosen"
        elif device_id in with_interfaces:
            reason = "interfaces_carry_no_address"
        else:
            reason = "no_interfaces_synced"
        classified[device_id] = reason
    return classified


def primary_ip_reasons(missing, *, settings_on=False):
    """Count the devices in ``missing`` by why, with what to do about each reason.

    Pass ``settings_on`` when the sync's primary-IP settings are already enabled,
    so the advice for an unchosen address does not tell the operator to turn on
    what is on.
    """
    counts = Counter(classify_primary_ip_devices(missing).values())
    return [
        {
            "reason": reason,
            "count": counts[reason],
            "fixable": fixable,
            "why": why,
            "what_to_do": (
                ALREADY_ON_ADVICE
                if settings_on and reason == "has_an_address_but_none_was_chosen"
                else advice
            ),
        }
        for reason, fixable, why, advice in REASONS
        if counts[reason]
    ]
