# Deleting a device that carries routing rows.
#
# netbox-routing makes an OSPF interface PROTECT the OSPF instance it belongs
# to, and a device delete cascades through that instance, so NetBox refuses to
# delete any device that has OSPF configured: "Unable to delete <interfaces>.
# 2 dependent objects were found". Django raises that before any delete signal
# fires, so nothing can unblock it from a signal; the rows have to be removed
# first, by the caller, on purpose.
#
# The rows this removes are routing rows (see `is_releasable_routing_row`), which
# the sync builds per device and rebuilds on the next run. The duplicate-device
# repair keeps its own narrower list. Anything else that protects the device
# still refuses the delete.
from .scope_reconciliation import _release_routing_rows_protecting
from .scope_reconciliation import SITE_RELABEL_RELEASABLE_ROUTING_MODELS

# What a manual device delete may remove to get past a PROTECT: a routing row
# (the `netbox_routing` app, plus the rows the duplicate repair already
# releases, which include a peering-manager session that PROTECTs a BGP peer)
# that hangs off one of the devices being deleted and no other. A BGP peer on a
# different device that merely uses a deleted device's address as its neighbour
# also protects that address, and is not this device's to remove: Django names
# it as a blocker all the same. Everything else, including any other plugin's
# rows, still refuses.
ROUTING_APPS = ("netbox_routing", "netbox_peering_manager")
_OWNER_WALK_DEPTH = 6


def is_releasable_routing_row(label):
    return label.startswith("netbox_routing.") or (
        label in SITE_RELABEL_RELEASABLE_ROUTING_MODELS
    )


def owning_device_ids(row, _depth=0, _cache=None):
    """The devices a routing row hangs off, found by following its own links.

    Empty means the owner could not be established, which the caller treats as
    "not this delete's to remove". Only links to a device, an interface, a
    virtual device context or another routing row are followed: an address or
    a VRF a row merely refers to says nothing about whose row it is. A link is
    checked against its target model before it is read, so an address or an
    ASN is never fetched, and a row reached twice is walked once.
    """
    from dcim.models import Device
    from dcim.models import Interface
    from dcim.models import VirtualDeviceContext
    from django.contrib.contenttypes.fields import GenericForeignKey

    if isinstance(row, Device):
        return {row.pk}
    if isinstance(row, (Interface, VirtualDeviceContext)):
        return {row.device_id} if row.device_id else set()
    if _depth >= _OWNER_WALK_DEPTH or row._meta.app_label not in ROUTING_APPS:
        return set()
    _cache = {} if _cache is None else _cache
    key = (row._meta.label_lower, row.pk)
    if key in _cache:
        return _cache[key]
    _cache[key] = set()  # a cycle contributes nothing the first visit won't
    owners = set()
    for field in row._meta.get_fields():
        if isinstance(field, GenericForeignKey):
            target = getattr(row, field.name, None)
        elif (
            field.concrete
            and (field.many_to_one or field.one_to_one)
            and not field.auto_created
            and _follows(field.related_model)
        ):
            target = getattr(row, field.name, None)
        else:
            continue
        if target is None:
            continue
        owners |= owning_device_ids(target, _depth + 1, _cache)
    _cache[key] = owners
    return owners


def _follows(model):
    from dcim.models import Device
    from dcim.models import Interface
    from dcim.models import VirtualDeviceContext

    return model in (Device, Interface, VirtualDeviceContext) or (
        model._meta.app_label in ROUTING_APPS
    )


class DeviceCascadeRefused(Exception):
    """Something outside the allowlist protects a device from deletion.

    ``refused`` maps a model label to how many rows of it protect the device,
    so the message can name what blocks the delete without naming a device.
    """

    def __init__(self, refused):
        super().__init__("a device is protected by rows outside the allowlist")
        self.refused = dict(refused)


def release_routing_rows(devices, using=None):
    """Delete the routing rows that block ``devices``' deletion.

    Only rows that hang off the devices being deleted are removed; a blocker
    that belongs to another device refuses. Returns ``{model_label: count}`` of
    what was removed. Raises `DeviceCascadeRefused` when anything else blocks a
    device. The caller runs this inside the transaction that deletes the
    devices, so a refusal, or a later failure, rolls the removals back.
    """
    devices = list(devices)
    selected = {device.pk for device in devices}

    cache = {}

    def releasable(row):
        if not is_releasable_routing_row(row._meta.label_lower):
            return False
        owners = owning_device_ids(row, _cache=cache)
        return bool(owners) and owners <= selected

    released = {}
    for device in devices:
        done, refused = _release_routing_rows_protecting(device, releasable=releasable)
        for label, count in done.items():
            released[label] = released.get(label, 0) + count
        if refused:
            raise DeviceCascadeRefused(refused)
    return released
