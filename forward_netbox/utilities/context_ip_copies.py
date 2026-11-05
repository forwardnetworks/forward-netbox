# A per-context copy of a shared management address.
#
# Forward models each firewall virtual context as its own device and gives every
# context its chassis's management address. NetBox allows one primary-IP owner per
# address object, so only one context can have it and the rest end with no primary
# IP. NetBox does allow duplicate address objects when every copy carries a shared
# role (Anycast, VIP, ...), so this gives each such context its own copy of the
# address on its own interface, marks the copies and the original Anycast, tags the
# copies, and makes the copy that context's primary IP.
#
# The sync's own address apply assumes one row per (address, vrf). Copies are
# invisible to it: every lookup it makes goes through `exclude_context_copies`.
# Copies are created only here, and the plugin never deletes one (they go with
# their interface or device). A copy is a snapshot: it does not follow later
# changes to the original's mask, status or VRF.
import logging

from rq.timeouts import JobTimeoutException

logger = logging.getLogger("forward_netbox.context_ip_copies")

CONTEXT_COPY_TAG_SLUG = "forward-context-copy"
CONTEXT_COPY_TAG_NAME = "Forward Context Copy"
CONTEXT_COPY_TAG_COLOR = "9e9e9e"
CONTEXT_COPY_TAG_DESCRIPTION = (
    "A per-context copy of a management address another device holds. Created "
    "and removed by the Forward sync; the sync's address apply ignores it."
)
CONTEXT_COPY_ROLE = "anycast"
CONTEXT_COPY_PARAMETER = "give_contexts_their_own_management_ip"


def context_copies_enabled(sync):
    return bool((getattr(sync, "parameters", None) or {}).get(CONTEXT_COPY_PARAMETER))


def _copy_keys(runner):
    """``{(host, vrf id)}`` that currently have a tagged copy, read once per run."""
    cached = getattr(runner, "_context_copy_keys", None)
    if isinstance(cached, set):
        return cached
    from ipam.models import IPAddress

    keys = set()
    for address, vrf_id in IPAddress.objects.filter(
        tags__slug=CONTEXT_COPY_TAG_SLUG
    ).values_list("address", "vrf_id"):
        keys.add((str(address.ip), vrf_id))
    try:
        runner._context_copy_keys = keys
    except AttributeError:
        pass
    return keys


def role_for_new_address(runner, address, vrf):
    """``anycast`` when copies of this host and VRF exist, else ``None``.

    NetBox refuses a duplicate address unless every copy has a shared role. If the
    synced original is deleted and later returns, creating it with no role would
    fail every run while its copies exist."""
    from ipaddress import ip_interface

    try:
        host = str(ip_interface(str(address)).ip)
    except ValueError:
        return None
    vrf_id = getattr(vrf, "pk", vrf)
    return CONTEXT_COPY_ROLE if (host, vrf_id) in _copy_keys(runner) else None


def exclude_context_copies(queryset):
    """The queryset without per-context copies, for lookups that expect one row."""
    return queryset.exclude(tags__slug=CONTEXT_COPY_TAG_SLUG)


def _copy_tag():
    from extras.models import Tag

    tag, _created = Tag.objects.get_or_create(
        slug=CONTEXT_COPY_TAG_SLUG,
        defaults={
            "name": CONTEXT_COPY_TAG_NAME,
            "color": CONTEXT_COPY_TAG_COLOR,
            "description": CONTEXT_COPY_TAG_DESCRIPTION,
        },
    )
    return tag


def give_context_a_copy(device, canonical, *, add_interface=False):
    """Make ``device`` a copy of ``canonical`` on its own same-named interface.

    Returns ``(copy, reason)``: the copy (existing or new) and ``None``, or
    ``None`` and a slug saying why no copy was made. Never guesses an interface:
    the context must carry one named like the interface that holds the address.
    With ``add_interface``, a context with no such interface is given a
    management interface of that name (`management_interfaces`) to carry it.
    """
    if not add_interface:
        return _give_context_a_copy(device, canonical)
    from django.db import router
    from django.db import transaction
    from ipam.models import IPAddress

    from .management_interfaces import ensure_management_interface

    copy, reason = _give_context_a_copy(device, canonical)
    if reason != "context-copy-no-matching-interface":
        return copy, reason
    # The interface and the copy land together or not at all: an added
    # interface with no address on it would answer nothing.
    with transaction.atomic(using=router.db_for_write(IPAddress)):
        interface, reason = ensure_management_interface(
            device, name=canonical.assigned_object.name
        )
        if interface is None:
            return None, reason
        return _give_context_a_copy(device, canonical)


def _give_context_a_copy(device, canonical):
    from dcim.models import Interface
    from django.contrib.contenttypes.models import ContentType
    from ipam.models import IPAddress

    holder = canonical.assigned_object
    if holder is None or not hasattr(holder, "name"):
        return None, "context-copy-original-unassigned"
    matches = list(Interface.objects.filter(device=device, name__iexact=holder.name))
    if len(matches) != 1:
        return None, "context-copy-no-matching-interface"
    interface = matches[0]
    interface_type = ContentType.objects.get_for_model(Interface)
    existing = IPAddress.objects.filter(
        assigned_object_type=interface_type,
        assigned_object_id=interface.pk,
        address=canonical.address,
        vrf=canonical.vrf,
        tags__slug=CONTEXT_COPY_TAG_SLUG,
    ).first()
    if existing is not None:
        return existing, None
    from django.db import router
    from django.db import transaction
    from ipam.constants import IPADDRESS_ROLES_NONUNIQUE

    # One device's copy is all or nothing: a failure between the role change and
    # the tag would otherwise leave an untagged duplicate the sync then trips on.
    with transaction.atomic(using=router.db_for_write(IPAddress)):
        if canonical.role not in IPADDRESS_ROLES_NONUNIQUE:
            # NetBox refuses a duplicate unless every copy has a shared role, the
            # original included. A role that already repeats (VIP, VRRP...) is
            # left as the operator set it; the copy takes the same one.
            canonical.snapshot()
            canonical.role = CONTEXT_COPY_ROLE
            canonical.save(update_fields=["role"])
        copy = IPAddress(
            address=canonical.address,
            vrf=canonical.vrf,
            status=canonical.status,
            role=canonical.role,
            tenant=canonical.tenant,
            assigned_object=interface,
        )
        copy.full_clean()
        copy.save()
        copy.tags.add(_copy_tag())
    copy._created_here = True
    return copy, None


def _canonical_address(owner_pk, host):
    """The synced address object for ``host`` held by device ``owner_pk``."""
    from dcim.models import Interface
    from django.contrib.contenttypes.models import ContentType
    from ipam.models import IPAddress

    interface_type = ContentType.objects.get_for_model(Interface)
    found = []
    for ip in exclude_context_copies(IPAddress.objects.filter(address__net_host=host)):
        if ip.assigned_object_type_id != interface_type.pk:
            continue
        holder = ip.assigned_object
        if holder is not None and getattr(holder, "device_id", None) == owner_pk:
            found.append(ip)
    # Exactly one: the same host in several VRFs on the owner is ambiguous, and
    # guessing would copy another VRF's row.
    return found[0] if len(found) == 1 else None


def apply_context_copies(
    devices, shared, device_management_ips, *, add_interfaces=False
):
    """Give each device in ``shared`` its own copy of its management address.

    ``shared`` is ``{device name: owner pk}``: devices whose one management
    address another device holds. Returns ``(created, updated, primaries,
    outcomes)``: new address copies, rows changed in place (the original's role,
    each device that took its copy as primary), the device pk -> copy mapping,
    and ``{device pk: (reason slug, owner pk)}`` for every device that could not
    get one. Never raises for one device: a device that cannot be given a copy
    keeps the reason it already had.
    """
    from .primary_ip import _host_ip

    created, updated, primaries, outcomes = [], [], {}, {}
    for name, owner_pk in shared.items():
        device = devices.get(name)
        if device is None:
            continue
        hosts = {
            host
            for host in (_host_ip(raw) for raw in device_management_ips.get(name) or ())
            if host is not None
        }
        if len(hosts) != 1:
            continue
        host = next(iter(hosts))
        canonical = _canonical_address(owner_pk, str(host))
        if canonical is None:
            outcomes[device.pk] = ("context-copy-original-not-found", owner_pk)
            continue
        # A primary IP an operator set by hand is theirs: never replace it.
        if getattr(device, "primary_ip6_id" if host.version == 6 else "primary_ip4_id"):
            continue
        role_before = canonical.role
        try:
            copy, reason = give_context_a_copy(
                device, canonical, add_interface=add_interfaces
            )
        except JobTimeoutException:
            raise
        except Exception as exc:  # noqa: BLE001 - one device must not sink the step
            logger.warning("context copy failed: %s", type(exc).__name__)
            copy, reason = None, "context-copy-failed"
        if copy is None:
            outcomes[device.pk] = (reason, owner_pk)
            continue
        if copy.pk not in {row.pk for row in created} and getattr(
            copy, "_created_here", False
        ):
            created.append(copy)
        if canonical.role != role_before and canonical.pk not in {
            row.pk for row in updated
        }:
            updated.append(canonical)
        primaries[device.pk] = (device, copy)
    return created, updated, primaries, outcomes
