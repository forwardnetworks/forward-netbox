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

# A short name for each reason, for lists and filters.
REASON_TITLES = {
    "not_refreshed_by_forward": "Uncovered: not refreshed by Forward",
    "shares_an_address_with_another_device": "Shares an address with another device",
    "has_an_address_but_none_was_chosen": "Has an address, none chosen",
    "interfaces_carry_no_address": "Interfaces carry no address",
    "no_interfaces_synced": "No interfaces synced",
}

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
    "The primary-IP settings are already on, so the management-address step has "
    "run. A Mgmt_<interface> device tag in Forward points the step at that "
    "interface (it takes the lowest address there)."
)

# The management-address summary's counts, in the words the operator reads.
_FALLBACK_PHRASES = (
    ("shared_with_another_device", "share the address with another device"),
    ("address_on_no_synced_interface", "have it on no synced interface"),
    ("several_management_addresses", "have several management addresses"),
    ("address_on_several_interfaces", "have it on several interfaces"),
)


# What the step recorded for one device, in the words the operator reads.
_OUTCOME_PHRASES = {
    "shared": "share the address with another device",
    "no-interface": "have it on no synced interface",
    "multiple-addresses": "have several management addresses",
    "several-interfaces": "have it on several interfaces",
    "holder": "lost the address to a device that already owned it",
    "tag-unresolved": "have a Mgmt_ tag that matched no interface address",
    "no-management-address": "have no usable management address from Forward",
}


def outcome_split(classified, outcomes):
    """``{reason: Counter({code or None: devices})}`` for the devices in ``classified``.

    ``outcomes`` is what the last management-address step recorded per device.
    A recorded device that is not in ``classified`` (it has a primary IP now, or
    is not this sync's) is left out, so every figure counts a bare device of this
    sync; ``None`` counts the bare devices the step recorded nothing for.
    """
    split = {}
    for pk, reason in classified.items():
        code = (outcomes.get(pk) or (None, None))[0]
        split.setdefault(reason, Counter())[code] += 1
    return split


def outcome_sentence(counts):
    """The step's recorded outcomes for one reason's devices, or "" if none recorded."""
    if not counts:
        return ""
    if not any(code for code in counts):
        return (
            f" The last management-address step recorded no reason for "
            f"{counts[None]} of these devices."
        )
    parts = [
        f"{counts[code]} {_OUTCOME_PHRASES.get(code, code)}"
        for code in _OUTCOME_PHRASES
        if counts.get(code)
    ]
    sentence = " Of these devices, the last management-address step recorded: " + (
        ", ".join(parts) if parts else "nothing it can name"
    )
    if counts.get(None):
        sentence += f"; it recorded no reason for {counts[None]}"
    return sentence + "."


def fallback_sentence(fallback_reasons):
    """What the last management-address step reported, across the whole sync.

    ``fallback_reasons`` is the parsed job-log summary (or ``None``). The
    sentence is a cross-reference, not a split of this reason's devices: the
    step reports on the devices in its own tag scope, and the classifier here
    counts this sync's devices by what NetBox holds.
    """
    if not fallback_reasons:
        return ""
    total = fallback_reasons.get("left_without_primary_ip", 0)
    parts = [
        f"{fallback_reasons.get(key, 0)} {phrase}"
        for key, phrase in _FALLBACK_PHRASES
        if fallback_reasons.get(key)
    ]
    return (
        f" The last management-address step reported {total} device(s) it could "
        "not give a primary IP from the management address"
        + (": " + ", ".join(parts) if parts else "")
        + "."
    )


# What the management-address step records when another device already owns
# the address it would have chosen.
SHARED_OUTCOMES = frozenset({"shared", "holder"})


def classify_primary_ip_devices(missing, outcomes=None):
    """``{device pk: reason}`` for the devices in ``missing`` (no primary IP).

    Every device lands in exactly one reason, in the order of ``REASONS``.
    ``outcomes`` is what the last management-address step recorded per device,
    ``{pk: (code, owner pk or None)}``. When it says another device already owns
    the address, the device shares it if every address it holds is that owner's
    too (or already collides), even where NetBox shows no second holder among
    the devices counted here: the owner is another sync's, or holds it on an
    interface this count never sees. A customer's bundle classed 346 such
    devices as fixable by a Mgmt_ tag. A device that also holds an address of
    its own stays fixable: a Mgmt_ tag can point the step at that one.
    """
    outcomes = outcomes or {}
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
    # The addresses each recorded owner holds, for the devices the step said lost
    # their address to it.
    owner_by_device = {
        device_id: outcome[1]
        for device_id, outcome in outcomes.items()
        if device_id in device_ids and outcome and outcome[0] in SHARED_OUTCOMES
    }
    owner_hosts = {}
    for owner_id, address in IPAddress.objects.filter(
        assigned_object_type=interface_type,
        interface__device_id__in={pk for pk in owner_by_device.values() if pk},
    ).values_list("interface__device_id", "address"):
        owner_hosts.setdefault(owner_id, set()).add(_host(address))

    def shared_with_recorded_owner(device_id, held):
        if device_id not in owner_by_device:
            return False
        owner = owner_by_device[device_id]
        if owner is None:
            # No owner named: only the one address the step chose can be it.
            return len(held) == 1
        return held <= colliding | owner_hosts.get(owner, set())

    classified = {}
    for device_id in device_ids:
        held = held_by_device.get(device_id, set())
        if device_id in uncovered:
            reason = "not_refreshed_by_forward"
        elif held and (
            held <= colliding or shared_with_recorded_owner(device_id, held)
        ):
            reason = "shares_an_address_with_another_device"
        elif held:
            reason = "has_an_address_but_none_was_chosen"
        elif device_id in with_interfaces:
            reason = "interfaces_carry_no_address"
        else:
            reason = "no_interfaces_synced"
        classified[device_id] = reason
    return classified


def _step_sentence(reason, split, fallback_reasons):
    recorded = outcome_sentence(split.get(reason))
    if recorded:
        return recorded
    if reason == "has_an_address_but_none_was_chosen":
        return fallback_sentence(fallback_reasons)
    return ""


def reason_rows(classified, *, settings_on=False, fallback_reasons=None, outcomes=None):
    """Rows (count, why, fixable, what to do) for a ``{device pk: reason}`` map.

    Pass ``settings_on`` when the sync's primary-IP settings are already enabled,
    so the advice for an unchosen address does not tell the operator to turn on
    what is on. ``fallback_reasons`` (the parsed management-address summary)
    adds what that step reported to the unchosen-address reason. ``outcomes`` (the
    step's per-device record) adds, to every reason, how that reason's devices
    split by what the step recorded; it takes the place of the sync-wide counts.
    """
    split = outcome_split(classified, outcomes) if outcomes is not None else {}
    counts = Counter(classified.values())
    return [
        {
            "reason": reason,
            "title": REASON_TITLES[reason],
            "count": counts[reason],
            "fixable": fixable,
            "why": why + _step_sentence(reason, split, fallback_reasons),
            "what_to_do": (
                ALREADY_ON_ADVICE
                if settings_on and reason == "has_an_address_but_none_was_chosen"
                else advice
            ),
        }
        for reason, fixable, why, advice in REASONS
        if counts[reason]
    ]


def primary_ip_reasons(
    missing, *, settings_on=False, fallback_reasons=None, outcomes=None
):
    """Count the devices in ``missing`` by why, with what to do about each reason."""
    return reason_rows(
        classify_primary_ip_devices(missing, outcomes),
        settings_on=settings_on,
        fallback_reasons=fallback_reasons,
        outcomes=outcomes,
    )
