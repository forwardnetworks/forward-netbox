# A management interface for a management address no interface carries.
#
# Forward records a device's management addresses (`platform.managementIps`) apart
# from its interfaces. On some platforms none of the device's interfaces carries
# that address: a Palo Alto's out-of-band management port is not in its interface
# list, Forward models some FortiGates with no interfaces at all, and a Fortinet
# vdom has no `mgmt` interface while its chassis holds the address there. NetBox
# sets a primary IP only from an address on one of the device's own interfaces, so
# those devices end with no primary IP.
#
# With the opt-in parameter on, the primary-IP step gives such a device a virtual,
# management-only interface tagged `forward-management-interface` to carry the
# address. Nothing the sync applies reads or deletes these: no Forward query emits
# them, and every delete the sync stages comes from a Forward row. They go with
# their device. Turning the parameter off stops new ones; existing ones stay until
# deleted (filter interfaces by the tag).
import logging

logger = logging.getLogger("forward_netbox.management_interfaces")

MANAGEMENT_INTERFACE_PARAMETER = "add_management_interface"
MANAGEMENT_INTERFACE_TAG_SLUG = "forward-management-interface"
MANAGEMENT_INTERFACE_TAG_NAME = "Forward Management Interface"
MANAGEMENT_INTERFACE_TAG_COLOR = "9e9e9e"
MANAGEMENT_INTERFACE_TAG_DESCRIPTION = (
    "A management interface the Forward sync added to carry a management address "
    "Forward records on none of the device's interfaces."
)
MANAGEMENT_INTERFACE_NAME = "mgmt"
MANAGEMENT_INTERFACE_DESCRIPTION = (
    "Added by the Forward sync: Forward records this device's management address "
    "on none of its interfaces."
)


def management_interfaces_enabled(sync):
    return bool(
        (getattr(sync, "parameters", None) or {}).get(MANAGEMENT_INTERFACE_PARAMETER)
    )


def _tag():
    from extras.models import Tag

    tag, _created = Tag.objects.get_or_create(
        slug=MANAGEMENT_INTERFACE_TAG_SLUG,
        defaults={
            "name": MANAGEMENT_INTERFACE_TAG_NAME,
            "color": MANAGEMENT_INTERFACE_TAG_COLOR,
            "description": MANAGEMENT_INTERFACE_TAG_DESCRIPTION,
        },
    )
    return tag


def ensure_management_interface(device, name=MANAGEMENT_INTERFACE_NAME):
    """``(interface, reason)``: the device's added interface named ``name``.

    Reuses one this step added before. An interface of that name the sync or a
    person made is never taken over: the address is not on it, so using it would
    be a guess. Returns ``(None, "management-interface-name-taken")`` then.
    """
    from dcim.choices import InterfaceTypeChoices
    from dcim.models import Interface

    existing = list(Interface.objects.filter(device=device, name__iexact=name))
    if existing:
        ours = [
            row
            for row in existing
            if row.tags.filter(slug=MANAGEMENT_INTERFACE_TAG_SLUG).exists()
        ]
        if len(ours) == 1 and len(existing) == 1:
            return ours[0], None
        return None, "management-interface-name-taken"
    interface = Interface(
        device=device,
        name=name,
        type=InterfaceTypeChoices.TYPE_VIRTUAL,
        mgmt_only=True,
        description=MANAGEMENT_INTERFACE_DESCRIPTION,
    )
    interface.full_clean()
    interface.save()
    interface.tags.add(_tag())
    interface._created_here = True
    return interface, None


def give_device_a_management_address(device, host):
    """Put ``host`` on the device's added interface, as its primary IP.

    Two devices naming one address NetBox holds nowhere (a chassis and a context
    Forward models with no interfaces, HA peers) both end with a primary IP: the
    first by name gets this row, the next a per-context copy of it on its own
    added interface, and both rows are marked Anycast. Which one holds the
    original is not meaningful, Forward gives no basis to choose.

    ``(address, reason)``. Only for an address NetBox has no row for in the
    global table: a row elsewhere (another VRF's, an unassigned one, another
    device's) is not this step's to move, and creating a second would be a
    duplicate.
    """
    from django.db import router
    from django.db import transaction
    from ipam.models import IPAddress

    if IPAddress.objects.filter(address__net_host=str(host)).exists():
        return None, "management-interface-address-elsewhere"
    prefix_length = 32 if host.version == 4 else 128
    with transaction.atomic(using=router.db_for_write(IPAddress)):
        interface, reason = ensure_management_interface(device)
        if interface is None:
            return None, reason
        address = IPAddress(
            address=f"{host}/{prefix_length}",
            assigned_object=interface,
            description=MANAGEMENT_INTERFACE_DESCRIPTION,
        )
        address.full_clean()
        address.save()
        address.tags.add(_tag())
        attr = "primary_ip6" if host.version == 6 else "primary_ip4"
        device.snapshot()
        setattr(device, attr, address)
        device.save(update_fields=[attr])
    return address, None
