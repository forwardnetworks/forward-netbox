# Mark the IP addresses that exist only because a BGP peer needs them.
#
# A BGP peer's neighbour is a remote device, so the routing sync creates its
# address as an IP address with no interface. In IPAM those look like orphans: a
# customer's 14,988 unassigned addresses were almost all neighbours. The tag lets
# an operator filter them in or out (``/ipam/ip-addresses/?tag=...``).
#
# This is a review aid on the same footing as ``forward-delete-eligible``: the tag
# is reconciled to exactly the current set on every run, an address that later
# gets an interface loses it, and nothing keys a delete on it. It is run by an
# operator, never inside a sync, because tagging an existing estate writes one
# change per address and would trip the large-run guard on the first sync after an
# upgrade.
import uuid

from django.apps import apps
from django.db import transaction

BGP_NEIGHBOUR_TAG_SLUG = "forward-bgp-neighbour"
BGP_NEIGHBOUR_TAG_NAME = "Forward: BGP neighbour"
BGP_NEIGHBOUR_TAG_COLOR = "9e9e9e"
BGP_NEIGHBOUR_TAG_DESCRIPTION = (
    "Address held for a BGP peer, not assigned to an interface. Expected; "
    "reconciled on every run (removed once an interface carries the address)."
)


def routing_installed():
    return apps.is_installed("netbox_routing")


def unassigned_ip_addresses():
    from ipam.models import IPAddress

    return IPAddress.objects.filter(assigned_object_id__isnull=True)


def bgp_neighbour_ip_addresses(unassigned=None):
    """Addresses assigned to nothing that a BGP peer uses as its peer or source."""
    from django.db.models import Q

    if unassigned is None:
        unassigned = unassigned_ip_addresses()
    return unassigned.filter(
        Q(remote_peers__isnull=False) | Q(source_peers__isnull=False)
    ).distinct()


def tag_bgp_neighbour_ips(user=None):
    """Make the tag's address set exactly the current neighbour addresses.

    Returns ``{added, removed, total}``; ``None`` when netbox-routing is not
    installed, since there are no peers to need an address.

    A management command has no request, so NetBox would log none of these
    edits. Given a ``user`` the writes run under a request for that user and are
    change-logged and attributed like any other edit.
    """
    from ipam.models import IPAddress
    from netbox.context_managers import event_tracking
    from utilities.request import NetBoxFakeRequest

    from .scope_ipam_audit import _apply_maintained_ipam_tag

    if not routing_installed():
        return None
    want = bgp_neighbour_ip_addresses().values_list("pk", flat=True)

    def apply():
        return _apply_maintained_ipam_tag(
            IPAddress,
            want,
            slug=BGP_NEIGHBOUR_TAG_SLUG,
            name=BGP_NEIGHBOUR_TAG_NAME,
            color=BGP_NEIGHBOUR_TAG_COLOR,
            description=BGP_NEIGHBOUR_TAG_DESCRIPTION,
        )

    if user is None:
        with transaction.atomic():
            return apply()
    request = NetBoxFakeRequest(
        {
            "META": {},
            "POST": {},
            "GET": {},
            "FILES": {},
            "user": user,
            "path": "",
            "id": uuid.uuid4(),
        }
    )
    with transaction.atomic(), event_tracking(request):
        return apply()
