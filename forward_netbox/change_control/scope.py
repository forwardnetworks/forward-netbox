# Which devices a change touches, and what Forward calls them.
#
# The names Forward scopes a workspace by are Forward's own, not NetBox's. A
# device is resolved against the source's latest processed snapshot with one NQE
# call for the whole list, and only an exact, unique, case-insensitive match is
# accepted: an ambiguous or missing name stays blank and says why. There is no
# fallback to the NetBox name, because a device scoped by one name and checked by
# another would be in the workspace and never checked.
from dataclasses import dataclass
from dataclasses import field

from django.db import transaction

from .actions import _journal
from .actions import _lock
from .workspace import _open_api
from .workspace import _source_network
from .workspace import SCOPABLE_STATES


@dataclass(frozen=True)
class ScopeOutcome:
    ok: bool
    message: str = ""
    problems: tuple = field(default_factory=tuple)

    def __bool__(self):
        return self.ok


def _refuse(message, problems=()):
    return ScopeOutcome(False, message, tuple(problems))


def _editable(locked):
    """The scope is fixed once a workspace was forked from it, or the change applied."""
    if locked.workspace_network_id:
        return (
            "This change already has a Forward workspace, forked from its device "
            "list. Devices cannot change now."
        )
    if locked.state not in SCOPABLE_STATES:
        return "Devices can only change before the change is applied."
    return ""


def parse_device_names(text):
    """Names from free text: one per line or comma separated, de-duplicated."""
    seen = {}
    for raw in (text or "").replace(",", "\n").splitlines():
        name = raw.strip()
        if name:
            seen.setdefault(name.lower(), name)
    return list(seen.values())


def add_devices(change, names, user):
    """Add NetBox devices by name. An unknown or duplicated name adds nothing."""
    from dcim.models import Device

    from ..models import ForwardChangeDevice

    names = list(names)
    if not names:
        return _refuse("Name at least one device.")
    problems = []
    found = []
    for name in names:
        matches = list(Device.objects.filter(name__iexact=name)[:3])
        if not matches:
            problems.append(f"{name}: no such device in NetBox.")
        elif len(matches) > 1:
            problems.append(
                f"{name}: more than one device in NetBox has this name (different "
                "sites), so it cannot be scoped by name."
            )
        else:
            found.append(matches[0])
    if problems:
        return _refuse("No devices were added.", problems)
    with transaction.atomic():
        locked = _lock(change)
        if locked is None:
            return _refuse("The change no longer exists.")
        reason = _editable(locked)
        if reason:
            return _refuse(reason)
        added = 0
        for device in found:
            _row, created = ForwardChangeDevice.objects.get_or_create(
                change=locked, device=device
            )
            added += int(created)
        if added:
            _journal(locked, user, f"Added {added} device(s) to the change's scope.")
    change.refresh_from_db()
    return ScopeOutcome(True, f"Added {added} device(s).")


def remove_device(change, row_pk, user):
    from ..models import ForwardChangeDevice

    with transaction.atomic():
        locked = _lock(change)
        if locked is None:
            return _refuse("The change no longer exists.")
        reason = _editable(locked)
        if reason:
            return _refuse(reason)
        row = ForwardChangeDevice.objects.filter(change=locked, pk=row_pk).first()
        if row is None:
            return _refuse("That device is not in this change.")
        label = str(row.device)
        row.delete()
        _journal(locked, user, f"Removed {label} from the change's scope.")
    return ScopeOutcome(True, f"Removed {label}.")


def match_forward_names(netbox_names, forward_names):
    """``{netbox name: (forward name or "", reason)}`` by exact unique match.

    Case-insensitive, like the sync's own device lookup. Two Forward devices that
    differ only in case, or two NetBox rows, make the match ambiguous.
    """
    by_lower = {}
    for name in forward_names:
        by_lower.setdefault(str(name).lower(), []).append(str(name))
    result = {}
    for name in netbox_names:
        hits = by_lower.get(name.lower(), [])
        if len(hits) == 1:
            result[name] = (hits[0], "")
        elif not hits:
            result[name] = ("", "Forward has no device with this name.")
        else:
            result[name] = ("", "Forward has several devices with this name.")
    return result


def resolve_forward_names(change, user, *, api=None):
    """Fill each device's Forward name from the source's latest processed snapshot.

    The Forward call is made with no lock held (it runs an NQE query over the whole
    network and may be slow); the change is then locked and checked again before
    anything is written.
    """
    from ..models import ForwardChange

    reason = _editable(change)
    if reason:
        return _refuse(reason)
    if not change.devices.exists():
        return _refuse("Add at least one device first.")
    network_id = _source_network(change)
    if not network_id:
        return _refuse("The source has no network selected.")
    api, error = _open_api(change, api)
    if error:
        return _refuse(error)
    try:
        forward_names = api.estate_device_names(network_id)
    except Exception as exc:  # noqa: BLE001 - reported on the page
        return _refuse(f"Forward could not list the network's devices: {exc}")
    with transaction.atomic():
        locked = ForwardChange.objects.select_for_update().filter(pk=change.pk).first()
        if locked is None:
            return _refuse("The change no longer exists.")
        reason = _editable(locked)
        if reason:
            return _refuse(reason)
        rows = list(locked.devices.select_related("device"))
        matched = match_forward_names([r.device.name for r in rows], forward_names)
        problems = []
        for row in rows:
            key, why = matched[row.device.name]
            if key != row.forward_device_key:
                row.forward_device_key = key
                row.save(update_fields=["forward_device_key"])
            if why:
                problems.append(f"{row.device.name}: {why}")
        resolved = len(rows) - len(problems)
        _journal(
            locked,
            user,
            f"Resolved Forward names: {resolved} of {len(rows)} device(s).",
        )
    if problems:
        return ScopeOutcome(
            True,
            f"Resolved {resolved} of {len(rows)} device(s); the rest cannot be "
            "scoped until Forward knows them.",
            tuple(problems),
        )
    return ScopeOutcome(True, f"Resolved all {len(rows)} device(s).")
