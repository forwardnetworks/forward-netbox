"""Device rows Forward stopped returning that the durable state forgot.

Before 3.0.14, any change in the set of in-scope devices made every
device-scoped child model take the `contract_reset` path in
`apply_durable_workload_deltas`: nothing missing was deleted that run, and the
stored state was REPLACED by the current target. A row Forward stopped
returning on such a run vanished from the state, so no later run could ever
see it go missing - it is stale in NetBox for good. 3.0.14 stops new ones;
this module names the ones already there, for an operator to remove.

A row is a candidate only when ALL of these hold (each is a test):

1. Its model is in `STALE_ROW_CLEANUP_MODELS`: a device-scoped child model the
   sync itself may delete (`DEVICE_SHARD_MODELS` & `BASELINE_REMOVAL_MODELS`)
   whose NetBox -> identity mapping can be built exactly. The models left out,
   and why, are listed under `STALE_ROW_CLEANUP_MODELS`.
2. Every device it belongs to is in the device list of the sync's latest
   finalized run (its current `dcim.device` state; a name two NetBox devices
   share, a device another sync identifies, and one marked out of scope,
   uncovered or backfilled are left out) AND has live rows in this model's
   state. Rows of devices that left scope are kept
   on purpose: see `out_of_scope_entries` in workload_state.py.
3. Its identity is absent from every current durable state for the model -
   this sync's and every other sync's. Matching is deliberately COARSER than
   the stored identity (an interface by name or abbreviation, an address by
   host, a MAC by value, a BGP peer by device and neighbour host), so a
   formatting difference can only make a row look present, never absent. A
   parent row (a BGP peer, a BGP address family, an OSPF instance) also
   counts as present while a child model's state still names it, because
   applying the child writes the parent.
4. NetBox's change log shows this sync's ingestions created it and nothing
   but this sync's ingestions ever changed it. A row added or edited by hand
   is never a candidate. A row whose create record was removed by changelog
   retention is held, not guessed at - unless the operator opted in with
   `include_older_than_change_log` AND the row's own `created` time is older
   than the OLDEST change NetBox still retains for anything (the log provably
   cannot speak for it) AND no retained change by anyone else touches it. The
   operator is then attesting that nobody created or edited those rows by hand. A row
   newer than that point without a create record stays held.
5. The model as a whole is refused - nothing deleted - when its durable state
   is missing, unreadable or older than the latest finalized run, when the
   state and NetBox disagree about most rows (the mapping cannot be trusted),
   or when the absent share is past `full_removal_reconciliation`'s
   MAX_REMOVAL_PERCENT brake.

Deletes go through the sync's own per-model delete adapters
(`runner._delete_<model>`, i.e. `delete_by_coalesce` and friends) inside a
savepoint, with `ForwardDependencySkipError` (the protected-delete path)
recorded as a hold - never a raw queryset delete. A delete that resolves to
any object other than the candidate is rolled back.

Every figure is a count; no device, interface or address name leaves here.
"""

from django.utils import timezone

from .full_removal_reconciliation import BASELINE_REMOVAL_MODELS
from .full_removal_reconciliation import MAX_REMOVAL_PERCENT
from .full_removal_reconciliation import MIN_REMOVAL_ROWS

# Delete order: what hangs off a row goes before the row, so a parent is only
# ever deleted bare. BGP first (a peer PROTECTs the address it points at, and an
# address family is held by the peer address families on it), then OSPF
# (interfaces before their instance), then everything that hangs off an
# interface, then the interface itself.
STALE_ROW_CLEANUP_MODELS = (
    "netbox_routing.bgppeeraddressfamily",
    "netbox_routing.bgpaddressfamily",
    "netbox_routing.bgppeer",
    "netbox_routing.ospfinterface",
    "netbox_routing.ospfinstance",
    "dcim.cable",
    "ipam.ipaddress",
    "dcim.macaddress",
    "dcim.inventoryitem",
    "dcim.module",
    "dcim.interface",
)

# Left out, and why (the plan file and the operations guide carry the same list):
#   ipam.fhrpgroup - the state's identity is a participant (protocol, group,
#     virtual address, VRF, device, interface), but the NetBox object that
#     carries it is split three ways: the assignment (device-scoped), the group
#     (shared by several devices) and the virtual IP (a separate address that
#     two groups may share, attached to only one of them). The delete adapter
#     finds the group by a name built from the address and VRF and, when the
#     last participant leaves, deletes the group AND the virtual IP. A group
#     without the shared IP cannot be mapped back to its row's address, and the
#     adapter's side effects land on objects no single device owns.
#   netbox_routing.ospfarea - a global catalogue row, not device-scoped (not in
#     DEVICE_SHARD_MODELS); shared by every device that uses the area.
#   netbox_peering_manager.peeringsession - its identity IS exact (it follows
#     its BGP peer), but it is not in BASELINE_REMOVAL_MODELS: the sync's own
#     durable-state path never removes a session by absence, only a Forward
#     diff or its peer's cascade does, and rule 1 will not widen past what the
#     sync itself may delete. A session on a stale peer holds that peer.
#   extras.taggeditem - a tag assignment, not a row; owned by the scope-tag job
#     (and not in BASELINE_REMOVAL_MODELS), whose claims decide what stays.

# One line per left-out device-shard model, kept next to the long reasons above
# so a test can insist every device-shard model is either covered or named here.
STALE_ROW_CLEANUP_LEFT_OUT = {
    "ipam.fhrpgroup": "group and virtual IP are shared across devices; row not mappable",
    "netbox_routing.ospfarea": "global catalogue row, not device-scoped",
    "netbox_peering_manager.peeringsession": "not removable by the sync's baseline path",
    "extras.taggeditem": "owned by the scope-tag job; not in the baseline allowlist",
}

# A parent row is written by applying its child models, so a child model's
# state that still names it keeps it. {parent: child models}.
PARENT_ASSERTED_BY = {
    "netbox_routing.bgppeer": (
        "netbox_routing.bgppeeraddressfamily",
        "netbox_peering_manager.peeringsession",
    ),
    "netbox_routing.bgpaddressfamily": ("netbox_routing.bgppeeraddressfamily",),
    "netbox_routing.ospfinstance": ("netbox_routing.ospfinterface",),
}

# The state must cover most of the rows it describes, or the reverse mapping
# is wrong for this estate and absence means nothing.
MIN_STATE_MATCH_FRACTION = 0.5

# Hold reasons: a row that met the absence rules but is kept.
HOLD_NO_CREATE_RECORD = "no_create_record_from_this_sync"
# Informational, option off: no create record, but the row is older than every
# change NetBox still retains, so "also remove rows older than the change log"
# would reach it. Counted before the later holds, so an upper bound.
HOLD_OLDER_THAN_CHANGE_LOG = "no_create_record_older_than_the_change_log"
HOLD_CHANGED_OUTSIDE_SYNC = "changed_outside_this_sync"
HOLD_TEMPLATE_COMPONENT = "device_type_template_component"
HOLD_MODULE_COMPONENT = "installed_by_a_module"
HOLD_ATTACHED_ROWS_STAY = "has_attached_rows_that_stay"
HOLD_PRIMARY_ADDRESS = "primary_or_oob_address_of_a_device"
HOLD_PRIMARY_MAC = "primary_mac_of_an_interface_that_stays"
HOLD_CONTEXT_COPY = "per_context_address_copy"
HOLD_NO_REVERSE_IDENTITY = "reverse_identity_unavailable"
HOLD_LOCALLY_REFERENCED = "locally_referenced"
HOLD_PROTECTED = "protected_by_another_object"
HOLD_AMBIGUOUS = "delete_path_lookup_ambiguous"
HOLD_NOT_RESOLVED = "delete_path_did_not_resolve_this_row"
HOLD_ALREADY_GONE = "already_deleted"
HOLD_FAILED = "delete_failed"
HOLD_MANAGEMENT_INTERFACE = "management_interface_step"
HOLD_CASCADE_REACHES_OTHER_ROWS = "delete_would_remove_or_blank_other_rows"
HOLD_SHARED_PARENT_NOT_OURS = "bgp_scope_or_router_not_created_by_this_sync"
HOLD_MODULE_HAS_COMPONENTS = "module_has_installed_components"

# What a delete may take with it beyond the candidate: NetBox's own cable and
# tag bookkeeping. Anything else - an OSPF interface, an FHRP assignment, an
# L2VPN termination, a journal entry, a blanked reference elsewhere - may be
# someone's hand work, and holds the row.
_CASCADE_BOOKKEEPING = frozenset(
    {"dcim.cabletermination", "dcim.cablepath", "extras.taggeditem"}
)
# The fields a cable delete blanks on its own interfaces.
_CABLE_FIELD_UPDATES = frozenset(
    {"cable", "cable_end", "_path", "_link_peer_type", "_link_peer_id"}
)

# Model refusals: nothing of this model is deleted.
REFUSED_NOT_ENABLED = "model_not_enabled_on_this_sync"
REFUSED_NOT_INSTALLED = "plugin_not_installed"
REFUSED_NO_STATE = "no_current_durable_state"
REFUSED_STATE_UNREADABLE = "durable_state_failed_its_checksum"
REFUSED_STATE_STALE = "durable_state_older_than_the_latest_run"
REFUSED_NO_DEVICE_LIST = "no_device_list_from_the_latest_run"
REFUSED_STATE_MISMATCH = "durable_state_does_not_match_netbox"
REFUSED_FRACTION = "too_large_a_share_of_the_model"
REFUSED_UNREPRESENTED_PEER = "another_sync_has_no_comparable_state"
REFUSED_STATE_ROW_WITHOUT_KEY = "durable_state_row_without_an_identity"
REFUSED_GREW_SINCE_PREVIEW = "more_removable_than_the_preview_showed"
REFUSED_CHILD_STATE_UNAVAILABLE = "a_child_model_has_no_current_durable_state"

# Whole-run refusals (the removal job).
REFUSED_NO_PREVIEW = "no_completed_preview"
REFUSED_PREVIEW_STALE = "preview_older_than_the_latest_run"
REFUSED_SYNC_RUNNING = "a_sync_run_is_active"
REFUSED_OPTION_WIDER_THAN_PREVIEW = "option_wider_than_the_reviewed_preview"

PREVIEW_JOB_SUFFIX = "preview stale device rows"

_CHUNK = 2000


class StaleRowCleanupRefused(RuntimeError):
    """The whole removal is refused before anything is deleted."""


def _chunks(values, size=_CHUNK):
    values = list(values)
    for start in range(0, len(values), size):
        yield values[start : start + size]


# --- identity keys -----------------------------------------------------------
#
# Each model maps a durable-state row and a NetBox object to a SET of keys. A
# NetBox object is "present" when any of its keys is any state row's key. Sets,
# because the interface lookup the sync uses matches either the exact name or
# its abbreviation, and the cleanup must be at least that generous.


def _host(value):
    import netaddr

    if value in (None, ""):
        return None
    try:
        return str(netaddr.IPNetwork(str(value).strip()).ip)
    except (netaddr.AddrFormatError, ValueError, TypeError):
        return None


def _mac(value):
    import netaddr

    if value in (None, ""):
        return None
    try:
        return str(netaddr.EUI(str(value).strip())).upper()
    except (netaddr.AddrFormatError, ValueError, TypeError):
        return None


def _device(value):
    return str(value or "").strip()


def interface_keys(device_name, interface_name):
    """Every key the sync's interface lookup would treat as this interface."""
    from .interface_naming import canonical_interface_key

    device_name = _device(device_name)
    name = str(interface_name or "").strip()
    if not device_name or not name:
        return set()
    keys = {("name", device_name, name.lower())}
    canonical = canonical_interface_key(name)
    if canonical is not None:
        keys.add(("canonical", device_name, canonical))
    return keys


def _vrf_key(value):
    return str(value or "").strip().lower()


def _afi_forms(value):
    """Every spelling of a BGP address family this row could be stored under.

    The raw value and the sync's own normalisation of it (`ipv4-any` is stored
    as `ipv4-unicast`): a set, so either spelling matching makes a row present.
    """
    from ..exceptions import ForwardQueryError
    from .sync import ForwardSyncRunner
    from .sync_routing_impl import normalize_bgp_address_family

    raw = str(value or "").strip().lower()
    if not raw:
        return set()
    forms = {raw}
    try:
        forms.add(
            normalize_bgp_address_family(
                value, aliases=ForwardSyncRunner.FORWARD_BGP_ADDRESS_FAMILY_ALIASES
            )
        )
    except ForwardQueryError:
        pass
    return forms


def _module_bay_forms(value):
    """A module bay's name as Forward reports it and as NetBox stores it.

    The apply path stores the cleaned name (stripped, cut to NetBox's 64
    characters); a bay is matched by either spelling, lower-cased.
    """
    raw = str(value or "").strip()
    if not raw:
        return set()
    return {raw.lower(), raw[:64].lower()}


def _ospf_process_id(row):
    from .sync_routing_impl import ospf_process_values

    return ospf_process_values(row)[0]


def state_row_keys(model_string, row):
    """The keys a durable-state row asserts."""
    if model_string == "dcim.inventoryitem":
        device_name = _device(row.get("device"))
        name = str(row.get("name") or "").strip().lower()
        if not (device_name and name):
            return set()
        return {("inventory", device_name, name)}
    if model_string == "dcim.module":
        device_name = _device(row.get("device"))
        if not device_name:
            return set()
        return {
            ("module", device_name, bay)
            for bay in _module_bay_forms(row.get("module_bay"))
        }
    if model_string == "netbox_routing.bgpaddressfamily":
        device_name = _device(row.get("device"))
        if not device_name:
            return set()
        vrf = _vrf_key(row.get("vrf"))
        return {
            ("bgpaf", device_name, vrf, form)
            for form in _afi_forms(row.get("afi_safi"))
        }
    if model_string == "netbox_routing.bgppeeraddressfamily":
        device_name = _device(row.get("device"))
        host = _host(row.get("neighbor_address"))
        if not (device_name and host):
            return set()
        return {
            ("peeraf", device_name, host, form)
            for form in _afi_forms(row.get("afi_safi"))
        }
    if model_string == "netbox_routing.ospfinstance":
        device_name = _device(row.get("device"))
        if not device_name:
            return set()
        vrf = _vrf_key(row.get("vrf"))
        return {("ospfinst", device_name, vrf, _ospf_process_id(row))}
    if model_string == "netbox_routing.ospfinterface":
        from .sync_routing_impl import routing_interface_lookup_candidates

        device_name = _device(row.get("device"))
        keys = set()
        for candidate in routing_interface_lookup_candidates(
            row.get("local_interface")
        ):
            keys |= interface_keys(device_name, candidate)
        return keys
    if model_string == "dcim.interface":
        return interface_keys(row.get("device"), row.get("name"))
    if model_string == "ipam.ipaddress":
        host = _host(row.get("address"))
        return {("host", host)} if host else set()
    if model_string == "dcim.macaddress":
        mac = _mac(row.get("mac_address") or row.get("mac"))
        return {("mac", mac)} if mac else set()
    if model_string == "netbox_routing.bgppeer":
        device_name = _device(row.get("device"))
        host = _host(row.get("neighbor_address"))
        return {("peer", device_name, host)} if device_name and host else set()
    if model_string == "dcim.cable":
        side_a = interface_keys(row.get("device"), row.get("interface"))
        side_b = interface_keys(row.get("remote_device"), row.get("remote_interface"))
        return {
            ("cable",) + tuple(sorted((a, b), key=repr)) for a in side_a for b in side_b
        }
    return set()


def state_row_devices(model_string, row):
    from .query_fetch_execution import _row_device_names

    return {_device(name) for name in _row_device_names(model_string, row)} - {""}


# --- what the state and the scope say ----------------------------------------


def _current_state(sync, model_string):
    from ..models import ForwardWorkloadState

    return (
        ForwardWorkloadState.objects.filter(
            sync=sync, model_string=model_string, is_current=True
        )
        .select_related("ingestion")
        .first()
    )


def _latest_finalized_ingestion_id(sync):
    from ..models import ForwardIngestion

    return (
        ForwardIngestion.objects.filter(sync=sync, merge_finalized_at__isnull=False)
        .order_by("-merge_finalized_at", "-pk")
        .values_list("pk", flat=True)
        .first()
    )


def _decode(state):
    from ..exceptions import ForwardQueryError
    from .workload_state import decode_state_entries

    try:
        return decode_state_entries(state.payload, state.payload_checksum)
    except ForwardQueryError:
        return None


def _other_syncs_present_keys(sync, model_string):
    """Keys every OTHER sync's current state still asserts for the model."""
    from ..models import ForwardWorkloadState

    keys = set()
    for state in ForwardWorkloadState.objects.filter(
        model_string=model_string, is_current=True
    ).exclude(sync=sync):
        entries = _decode(state)
        if entries is None:
            # Cannot tell what that sync asserts: treat the model as unsafe.
            return None
        for value in entries.values():
            if value.get("action") == "upsert":
                keys |= state_row_keys(model_string, value.get("row") or {})
    return keys


def in_scope_device_ids(sync):
    """Devices the sync's last run fetched, by NetBox pk, or None.

    None when there is no device list from the latest finalized run (missing,
    older, or unreadable): every model is then refused rather than judged on
    no devices, so the preview says why instead of showing a bare zero.

    The device list is the sync's current `dcim.device` state: the devices that
    run asked Forward for (rows carried for a device out of scope are not
    counted). Scope claims and device identity records are not used: the first
    exist only with "apply device scope tags" on, the second only for some
    devices. A name two NetBox devices share is left out, as is a device another
    sync identifies, or one marked out of scope, uncovered or backfilled.
    """
    from dcim.models import Device

    from ..models import ForwardDeviceIdentity
    from ..models import ForwardDeviceTagClaim
    from .workload_state import CARRIED_ROW_HASH

    state = _current_state(sync, "dcim.device")
    if state is None:
        return None
    latest_ingestion_id = _latest_finalized_ingestion_id(sync)
    if latest_ingestion_id is not None and state.ingestion_id != latest_ingestion_id:
        # An older run's device list: not what the child states were judged on.
        return None
    entries = _decode(state)
    if entries is None:
        return None
    names = {
        str((value.get("row") or {}).get("name") or "").strip()
        for value in entries.values()
        if value.get("action") == "upsert" and value.get("row_hash") != CARRIED_ROW_HASH
    } - {""}
    by_name = {}
    for chunk in _chunks(sorted(names)):
        for pk, name in Device.objects.filter(name__in=chunk).values_list("pk", "name"):
            by_name.setdefault(name, []).append(pk)
    device_ids = {pks[0] for pks in by_name.values() if len(pks) == 1}
    device_ids -= set(
        ForwardDeviceIdentity.objects.exclude(sync=sync).values_list(
            "device_id", flat=True
        )
    )
    device_ids -= set(
        ForwardDeviceTagClaim.objects.filter(
            sync=sync, claim_type__in=("out_of_scope", "uncovered", "backfilled")
        ).values_list("device_id", flat=True)
    )
    return device_ids


# --- NetBox side, per model --------------------------------------------------
#
# Each collector returns {pk: NetBoxRow} for rows whose devices are ALL in
# `device_name_by_id` (the eligible devices). `delete_row` is the row the sync's
# delete adapter takes, built from the object's own fields; None means the
# reverse identity cannot be built exactly and the row is held.


class NetBoxRow:
    __slots__ = ("pk", "keys", "delete_row", "hold", "interface_id", "by_option")

    def __init__(self, pk, keys, delete_row, *, hold=None, interface_id=None):
        self.pk = pk
        self.keys = keys
        self.delete_row = delete_row
        self.hold = hold
        self.interface_id = interface_id
        # Set when the row is a candidate ONLY because the operator opted in to
        # rows older than the change log.
        self.by_option = False


def _interface_rows(device_name_by_id):
    from dcim.models import Interface
    from dcim.models import InterfaceTemplate

    found = []
    for chunk in _chunks(device_name_by_id):
        found.extend(
            Interface.objects.filter(device_id__in=chunk).values_list(
                "pk", "device_id", "name", "module_id", "device__device_type_id"
            )
        )
    # A component NetBox instantiated from the device type is logged under the
    # same request as the device itself, so the change log cannot tell it from
    # one the sync wrote from a Forward row. Never a candidate.
    template_names = set(
        InterfaceTemplate.objects.filter(
            device_type_id__in={row[4] for row in found if row[4]}
        ).values_list("device_type_id", "name")
    )
    # Made by the management-interface step to carry an address Forward has
    # no interface for: never in an interface state, never this cleanup's.
    from .management_interfaces import MANAGEMENT_INTERFACE_TAG_SLUG

    management = set()
    for chunk in _chunks([row[0] for row in found]):
        management |= set(
            Interface.objects.filter(
                pk__in=chunk, tags__slug=MANAGEMENT_INTERFACE_TAG_SLUG
            ).values_list("pk", flat=True)
        )
    rows = {}
    for pk, device_id, name, module_id, device_type_id in found:
        device_name = device_name_by_id[device_id]
        if pk in management:
            hold = HOLD_MANAGEMENT_INTERFACE
        elif module_id:
            hold = HOLD_MODULE_COMPONENT
        elif (device_type_id, name) in template_names:
            hold = HOLD_TEMPLATE_COMPONENT
        else:
            hold = None
        rows[pk] = NetBoxRow(
            pk,
            interface_keys(device_name, name),
            {"device": device_name, "name": name},
            hold=hold,
            interface_id=pk,
        )
    return rows


def _assigned_rows(model, device_name_by_id, *, value_field):
    """(pk, value, interface pk, device name) for objects on eligible interfaces."""
    from dcim.models import Interface
    from django.contrib.contenttypes.models import ContentType

    interface_type = ContentType.objects.get_for_model(Interface)
    device_by_interface = {}
    for chunk in _chunks(device_name_by_id):
        for pk, device_id in Interface.objects.filter(device_id__in=chunk).values_list(
            "pk", "device_id"
        ):
            device_by_interface[pk] = device_name_by_id[device_id]
    found = []
    for chunk in _chunks(device_by_interface):
        for pk, value, interface_id in model.objects.filter(
            assigned_object_type=interface_type, assigned_object_id__in=chunk
        ).values_list("pk", value_field, "assigned_object_id"):
            found.append((pk, value, interface_id, device_by_interface[interface_id]))
    return found


def _ipaddress_rows(device_name_by_id):
    from ipam.models import IPAddress

    from .context_ip_copies import CONTEXT_COPY_TAG_SLUG

    found = _assigned_rows(IPAddress, device_name_by_id, value_field="address")
    pks = [pk for pk, _value, _iface, _device in found]
    vrf_by_pk = {}
    copies = set()
    for chunk in _chunks(pks):
        vrf_by_pk.update(
            IPAddress.objects.filter(pk__in=chunk).values_list("pk", "vrf__name")
        )
        copies |= set(
            IPAddress.objects.filter(
                pk__in=chunk, tags__slug=CONTEXT_COPY_TAG_SLUG
            ).values_list("pk", flat=True)
        )
    rows = {}
    for pk, address, interface_id, _device_name in found:
        host = _host(address)
        rows[pk] = NetBoxRow(
            pk,
            {("host", host)} if host else set(),
            {"address": str(address), "vrf": vrf_by_pk.get(pk)} if host else None,
            hold=(
                HOLD_CONTEXT_COPY
                if pk in copies
                else None if host else HOLD_NO_REVERSE_IDENTITY
            ),
            interface_id=interface_id,
        )
    return rows


def _macaddress_rows(device_name_by_id):
    from dcim.models import MACAddress

    rows = {}
    for pk, value, interface_id, _device_name in _assigned_rows(
        MACAddress, device_name_by_id, value_field="mac_address"
    ):
        mac = _mac(value)
        rows[pk] = NetBoxRow(
            pk,
            {("mac", mac)} if mac else set(),
            {"mac_address": str(value)} if mac else None,
            hold=None if mac else HOLD_NO_REVERSE_IDENTITY,
            interface_id=interface_id,
        )
    return rows


def _cable_rows(device_name_by_id):
    from dcim.models import CableTermination
    from dcim.models import Interface
    from django.contrib.contenttypes.models import ContentType

    interface_type = ContentType.objects.get_for_model(Interface)
    interface_by_pk = {}
    for chunk in _chunks(device_name_by_id):
        for pk, device_id, name in Interface.objects.filter(
            device_id__in=chunk
        ).values_list("pk", "device_id", "name"):
            interface_by_pk[pk] = (device_name_by_id[device_id], name)
    cable_ids = set()
    for chunk in _chunks(interface_by_pk):
        cable_ids |= set(
            CableTermination.objects.filter(
                termination_type=interface_type, termination_id__in=chunk
            ).values_list("cable_id", flat=True)
        )
    ends = {}
    for chunk in _chunks(cable_ids):
        for cable_id, end, type_id, termination_id in CableTermination.objects.filter(
            cable_id__in=chunk
        ).values_list("cable_id", "cable_end", "termination_type_id", "termination_id"):
            ends.setdefault(cable_id, []).append((end, type_id, termination_id))
    rows = {}
    for cable_id, terminations in ends.items():
        by_end = {}
        for end, type_id, termination_id in terminations:
            by_end.setdefault(end, []).append((type_id, termination_id))
        sides = [by_end.get("A") or [], by_end.get("B") or []]
        exact = all(
            len(side) == 1 and side[0][0] == interface_type.pk for side in sides
        )
        if exact and not all(side[0][1] in interface_by_pk for side in sides):
            # One end is on a device outside the eligible set: rule 2.
            continue
        if not exact:
            if any(
                type_id == interface_type.pk and termination_id in interface_by_pk
                for side in sides
                for type_id, termination_id in side
            ) and all(
                termination_id in interface_by_pk
                for side in sides
                for type_id, termination_id in side
                if type_id == interface_type.pk
            ):
                rows[cable_id] = NetBoxRow(
                    cable_id, set(), None, hold=HOLD_NO_REVERSE_IDENTITY
                )
            continue
        (a_device, a_name), (b_device, b_name) = (
            interface_by_pk[sides[0][0][1]],
            interface_by_pk[sides[1][0][1]],
        )
        keys = {
            ("cable",) + tuple(sorted((a, b), key=repr))
            for a in interface_keys(a_device, a_name)
            for b in interface_keys(b_device, b_name)
        }
        rows[cable_id] = NetBoxRow(
            cable_id,
            keys,
            {
                "device": a_device,
                "interface": a_name,
                "remote_device": b_device,
                "remote_interface": b_name,
            },
        )
    return rows


def _bgppeer_rows(device_name_by_id):
    from dcim.models import Device
    from django.apps import apps
    from django.contrib.contenttypes.models import ContentType

    BGPPeer = apps.get_model("netbox_routing", "BGPPeer")
    device_type = ContentType.objects.get_for_model(Device)
    rows = {}
    for chunk in _chunks(device_name_by_id):
        for pk, device_id, address, vrf_name, local_asn in BGPPeer.objects.filter(
            scope__router__assigned_object_type=device_type,
            scope__router__assigned_object_id__in=chunk,
        ).values_list(
            "pk",
            "scope__router__assigned_object_id",
            "peer__address",
            "scope__vrf__name",
            "scope__router__asn__asn",
        ):
            device_name = device_name_by_id[device_id]
            host = _host(address)
            rows[pk] = NetBoxRow(
                pk,
                {("peer", device_name, host)} if host else set(),
                (
                    {
                        "device": device_name,
                        "local_asn": local_asn,
                        "vrf": vrf_name,
                        "neighbor_address": host,
                    }
                    if host and local_asn
                    else None
                ),
                hold=None if host and local_asn else HOLD_NO_REVERSE_IDENTITY,
            )
    return rows


def _inventoryitem_rows(device_name_by_id):
    from dcim.models import InventoryItem
    from django.apps import apps

    found = []
    for chunk in _chunks(device_name_by_id):
        found.extend(
            InventoryItem.objects.filter(device_id__in=chunk).values_list(
                "pk", "device_id", "name", "part_id", "serial", "device__device_type_id"
            )
        )
    # Newer NetBox can instantiate inventory items from the device type. They
    # share their device's create request, so the change log cannot tell them
    # from rows the sync wrote: never a candidate.
    template_names = set()
    try:
        Template = apps.get_model("dcim", "InventoryItemTemplate")
    except LookupError:
        Template = None
    if Template is not None:
        template_names = set(
            Template.objects.filter(
                device_type_id__in={row[5] for row in found if row[5]}
            ).values_list("device_type_id", "name")
        )
    rows = {}
    for pk, device_id, name, part_id, serial, device_type_id in found:
        device_name = device_name_by_id[device_id]
        clean = str(name or "").strip().lower()
        if (device_type_id, name) in template_names:
            hold = HOLD_TEMPLATE_COMPONENT
        elif not clean:
            hold = HOLD_NO_REVERSE_IDENTITY
        else:
            hold = None
        rows[pk] = NetBoxRow(
            pk,
            {("inventory", device_name, clean)} if clean else set(),
            (
                {
                    "device": device_name,
                    "name": name,
                    "part_id": part_id or "",
                    "serial": serial or "",
                }
                if clean
                else None
            ),
            hold=hold,
        )
    return rows


def _modules_with_components(pks):
    """Which of these modules have any component installed in them.

    Read from the schema (every reverse foreign key named `module`), so a
    component type NetBox adds later is covered without anyone remembering.
    Deleting a module deletes its components, and a component may be a row
    Forward still returns.
    """
    from dcim.models import Module

    held = set()
    for rel in Module._meta.related_objects:
        if not rel.one_to_many or rel.field.name != "module":
            continue
        related = rel.related_model
        for chunk in _chunks(pks):
            held |= set(
                related._default_manager.filter(module_id__in=chunk).values_list(
                    "module_id", flat=True
                )
            )
    return held


def _module_rows(device_name_by_id):
    from dcim.models import Module

    found = []
    for chunk in _chunks(device_name_by_id):
        found.extend(
            Module.objects.filter(device_id__in=chunk).values_list(
                "pk", "device_id", "module_bay__name"
            )
        )
    with_components = _modules_with_components([row[0] for row in found])
    rows = {}
    for pk, device_id, bay_name in found:
        device_name = device_name_by_id[device_id]
        forms = _module_bay_forms(bay_name)
        if pk in with_components:
            hold = HOLD_MODULE_HAS_COMPONENTS
        elif not forms:
            hold = HOLD_NO_REVERSE_IDENTITY
        else:
            hold = None
        rows[pk] = NetBoxRow(
            pk,
            {("module", device_name, form) for form in forms},
            {"device": device_name, "module_bay": bay_name} if forms else None,
            hold=hold,
        )
    return rows


def _bgpaddressfamily_rows(device_name_by_id):
    from dcim.models import Device
    from django.apps import apps
    from django.contrib.contenttypes.models import ContentType

    AddressFamily = apps.get_model("netbox_routing", "BGPAddressFamily")
    device_type = ContentType.objects.get_for_model(Device)
    rows = {}
    for chunk in _chunks(device_name_by_id):
        found = AddressFamily.objects.filter(
            scope__router__assigned_object_type=device_type,
            scope__router__assigned_object_id__in=chunk,
        ).values_list(
            "pk",
            "scope__router__assigned_object_id",
            "address_family",
            "scope__vrf__name",
            "scope__router__asn__asn",
        )
        for pk, device_id, family, vrf_name, local_asn in found:
            device_name = device_name_by_id[device_id]
            forms = _afi_forms(family)
            exact = bool(forms and local_asn)
            rows[pk] = NetBoxRow(
                pk,
                {("bgpaf", device_name, _vrf_key(vrf_name), form) for form in forms},
                (
                    {
                        "device": device_name,
                        "local_asn": local_asn,
                        "vrf": vrf_name,
                        "afi_safi": family,
                    }
                    if exact
                    else None
                ),
                hold=None if exact else HOLD_NO_REVERSE_IDENTITY,
            )
    return rows


def _bgp_peers_on(device_name_by_id):
    """``{peer pk: (device name, host, vrf name, local asn)}`` on these devices."""
    from dcim.models import Device
    from django.apps import apps
    from django.contrib.contenttypes.models import ContentType

    BGPPeer = apps.get_model("netbox_routing", "BGPPeer")
    device_type = ContentType.objects.get_for_model(Device)
    peers = {}
    for chunk in _chunks(device_name_by_id):
        found = BGPPeer.objects.filter(
            scope__router__assigned_object_type=device_type,
            scope__router__assigned_object_id__in=chunk,
        ).values_list(
            "pk",
            "scope__router__assigned_object_id",
            "peer__address",
            "scope__vrf__name",
            "scope__router__asn__asn",
        )
        for pk, device_id, address, vrf_name, local_asn in found:
            peers[pk] = (
                device_name_by_id[device_id],
                _host(address),
                vrf_name,
                local_asn,
            )
    return peers


def _bgppeeraddressfamily_rows(device_name_by_id):
    from django.apps import apps
    from django.contrib.contenttypes.models import ContentType

    BGPPeer = apps.get_model("netbox_routing", "BGPPeer")
    PeerAddressFamily = apps.get_model("netbox_routing", "BGPPeerAddressFamily")
    peer_type = ContentType.objects.get_for_model(BGPPeer)
    peers = _bgp_peers_on(device_name_by_id)
    rows = {}
    for chunk in _chunks(peers):
        # Only an address family hung on a BGP peer is this model's row; one on
        # anything else has no device to judge it by and is simply not seen.
        found = PeerAddressFamily.objects.filter(
            assigned_object_type=peer_type, assigned_object_id__in=chunk
        ).values_list("pk", "assigned_object_id", "address_family__address_family")
        for pk, peer_id, family in found:
            device_name, host, vrf_name, local_asn = peers[peer_id]
            forms = _afi_forms(family)
            exact = bool(forms and host and local_asn)
            keys = (
                {("peeraf", device_name, host, form) for form in forms}
                if host
                else set()
            )
            rows[pk] = NetBoxRow(
                pk,
                keys,
                (
                    {
                        "device": device_name,
                        "local_asn": local_asn,
                        "vrf": vrf_name,
                        "neighbor_address": host,
                        "afi_safi": family,
                    }
                    if exact
                    else None
                ),
                hold=None if exact else HOLD_NO_REVERSE_IDENTITY,
            )
    return rows


def _ospfinstance_rows(device_name_by_id):
    from django.apps import apps

    OSPFInstance = apps.get_model("netbox_routing", "OSPFInstance")
    rows = {}
    for chunk in _chunks(device_name_by_id):
        found = OSPFInstance.objects.filter(device_id__in=chunk).values_list(
            "pk", "device_id", "vrf__name", "process_id"
        )
        for pk, device_id, vrf_name, process_id in found:
            device_name = device_name_by_id[device_id]
            known = process_id is not None
            rows[pk] = NetBoxRow(
                pk,
                (
                    {("ospfinst", device_name, _vrf_key(vrf_name), int(process_id))}
                    if known
                    else set()
                ),
                (
                    {
                        "device": device_name,
                        "vrf": vrf_name,
                        "process_id": str(process_id),
                    }
                    if known
                    else None
                ),
                hold=None if known else HOLD_NO_REVERSE_IDENTITY,
            )
    return rows


def _ospfinterface_rows(device_name_by_id):
    from django.apps import apps

    OSPFInterface = apps.get_model("netbox_routing", "OSPFInterface")
    rows = {}
    for chunk in _chunks(device_name_by_id):
        found = OSPFInterface.objects.filter(
            interface__device_id__in=chunk
        ).values_list("pk", "interface__device_id", "interface__name")
        for pk, device_id, interface_name in found:
            device_name = device_name_by_id[device_id]
            rows[pk] = NetBoxRow(
                pk,
                interface_keys(device_name, interface_name),
                {"device": device_name, "local_interface": interface_name},
            )
    return rows


_COLLECTORS = {
    "dcim.interface": _interface_rows,
    "ipam.ipaddress": _ipaddress_rows,
    "dcim.macaddress": _macaddress_rows,
    "dcim.cable": _cable_rows,
    "netbox_routing.bgppeer": _bgppeer_rows,
    "netbox_routing.bgpaddressfamily": _bgpaddressfamily_rows,
    "netbox_routing.bgppeeraddressfamily": _bgppeeraddressfamily_rows,
    "netbox_routing.ospfinstance": _ospfinstance_rows,
    "netbox_routing.ospfinterface": _ospfinterface_rows,
    "dcim.inventoryitem": _inventoryitem_rows,
    "dcim.module": _module_rows,
}


def _model_class(model_string):
    from django.apps import apps

    app_label, model_name = model_string.split(".")
    return apps.get_model(app_label, model_name)


# --- ownership (rule 4) ------------------------------------------------------


def _ownership(model_string, pks, request_ids):
    """``(created_by_sync, changed_outside_sync)`` pk sets from the change log."""
    from core.models import ObjectChange
    from core.models import ObjectType

    created, outside = set(), set()
    if not pks:
        return created, outside
    request_ids = {str(value) for value in request_ids}
    object_type = ObjectType.objects.get_for_model(_model_class(model_string))
    for chunk in _chunks(pks):
        for pk, action, request_id in ObjectChange.objects.filter(
            changed_object_type=object_type,
            changed_object_id__in=chunk,
        ).values_list("changed_object_id", "action", "request_id"):
            ours = str(request_id) in request_ids
            if action == "create" and ours:
                created.add(pk)
            elif not ours:
                outside.add(pk)
    return created, outside


def _locally_referenced(model_string, candidates):
    """The workload-state delete gate, asked about these rows."""
    from ..exceptions import ForwardQueryError
    from .sync_contracts import default_coalesce_fields_for_model
    from .workload_state import _locally_referenced_delete_identities
    from .workload_state import canonical_row_identity

    coalesce_fields = default_coalesce_fields_for_model(model_string)
    entries, pk_by_identity = {}, {}
    for row in candidates:
        try:
            identity = canonical_row_identity(
                model_string, row.delete_row, coalesce_fields
            )
        except ForwardQueryError:
            # No durable identity, so no gate to ask; the row is still
            # subject to the adapter's own protected-delete handling.
            continue
        entries[identity] = {"action": "delete", "row": row.delete_row}
        pk_by_identity.setdefault(identity, set()).add(row.pk)
    held = _locally_referenced_delete_identities(
        model_string,
        entries,
        association_protection={
            "device_software_authoritative": False,
            "device_software_versions": set(),
            "vulnerability_authoritative": False,
            "vulnerability_versions": set(),
            "vulnerability_cves": set(),
        },
    )
    return {pk for identity in held for pk in pk_by_identity.get(identity, ())}


# --- the plan ----------------------------------------------------------------


def sync_may_delete(model_string):
    """Rule 1: a device-scoped child model the sync's own delete path covers."""
    from .branch_budget import DEVICE_SHARD_MODELS

    return (
        model_string in DEVICE_SHARD_MODELS
        and model_string in BASELINE_REMOVAL_MODELS
        and model_string in _COLLECTORS
    )


def _hold(plan, reason, count=1):
    plan["held"][reason] = plan["held"].get(reason, 0) + count


def oldest_retained_change_time():
    """When NetBox's oldest retained change was made, anywhere, by anyone.

    The change log is pruned by age, so every change before this point is gone
    and the log cannot speak for a row older than it. None when the log is
    empty: then it proves nothing about any row.
    """
    from core.models import ObjectChange
    from django.db.models import Min

    return ObjectChange.objects.aggregate(oldest=Min("time"))["oldest"]


def first_sync_created_time():
    """When the first sync was set up: no row older than this is the plugin's.

    A row that predates every sync cannot have been written by one, whatever
    the change log no longer says, so the opt-in never reaches it. None when no
    sync exists.
    """
    from django.db.models import Min

    from ..models import ForwardSync

    return ForwardSync.objects.aggregate(first=Min("created"))["first"]


def _created_times(object_model_string, pks):
    """``{pk: created}`` for rows whose model records when it was created."""
    model = _model_class(object_model_string)
    if not any(field.name == "created" for field in model._meta.fields):
        return {}
    times = {}
    for chunk in _chunks(pks):
        times.update(
            model.objects.filter(pk__in=chunk, created__isnull=False).values_list(
                "pk", "created"
            )
        )
    return times


def _child_asserted_keys(sync, model_string):
    """Keys of this model that child models' states still name, or a refusal.

    Applying a child row writes its parent (a peer address family writes the
    peer and the address family; an OSPF interface writes its instance), so a
    parent absent from its own state is still wanted while a child names it.
    Every sync's current state counts. A child model enabled on this sync with
    no current state, or any child state that will not decode, means it cannot
    be told what the children want: the parent model is refused.
    """
    from django.apps import apps

    from ..models import ForwardWorkloadState

    keys = set()
    for child in PARENT_ASSERTED_BY.get(model_string, ()):
        if not apps.is_installed(child.split(".")[0]):
            continue
        if sync.is_model_enabled(child) and _current_state(sync, child) is None:
            return None, REFUSED_CHILD_STATE_UNAVAILABLE
        for state in ForwardWorkloadState.objects.filter(
            model_string=child, is_current=True
        ):
            entries = _decode(state)
            if entries is None:
                return None, REFUSED_STATE_UNREADABLE
            for value in entries.values():
                if value.get("action") == "upsert":
                    keys |= state_row_keys(model_string, value.get("row") or {})
    return keys, None


def _is_judged_state_row(modules_enabled, model_string, row):
    """Whether NetBox is expected to hold a row for this state row.

    The one exception: with `dcim.module` enabled, a module-native inventory
    row is never an inventory item (the sync deletes it instead), so it cannot
    be a NetBox row the state should match. It still keeps any item of that
    name present.
    """
    if model_string != "dcim.inventoryitem" or not modules_enabled:
        return True
    from .sync import ForwardSyncRunner

    return not (
        row.get("module_component") is True
        or row.get("part_type") in ForwardSyncRunner.MODULE_NATIVE_INVENTORY_PART_TYPES
    )


def _plan_model(
    sync,
    model_string,
    request_ids,
    scope_ids,
    latest_ingestion_id,
    *,
    include_older_than_change_log=False,
    change_log_cutoff=None,
):
    from dcim.models import Device
    from django.apps import apps

    from .workload_state import CARRIED_ROW_HASH

    plan = {
        "refused": None,
        "devices_judged": 0,
        "state_rows": 0,
        "netbox_rows": 0,
        "not_returned": 0,
        "removable": 0,
        "older_than_change_log": 0,
        "held": {},
        "deletable": {},
    }
    if not sync_may_delete(model_string):
        # Rule 1 as a guard, not only as the tuple above: a model added to the
        # tuple that the sync itself may not delete is refused here.
        raise StaleRowCleanupRefused(f"{model_string} is not a cleanup model")
    if not apps.is_installed(model_string.split(".")[0]):
        plan["refused"] = REFUSED_NOT_INSTALLED
        return plan
    if not sync.is_model_enabled(model_string):
        plan["refused"] = REFUSED_NOT_ENABLED
        return plan
    if scope_ids is None:
        plan["refused"] = REFUSED_NO_DEVICE_LIST
        return plan
    state = _current_state(sync, model_string)
    if state is None:
        plan["refused"] = REFUSED_NO_STATE
        return plan
    if latest_ingestion_id is not None and state.ingestion_id != latest_ingestion_id:
        plan["refused"] = REFUSED_STATE_STALE
        return plan
    entries = _decode(state)
    if entries is None:
        plan["refused"] = REFUSED_STATE_UNREADABLE
        return plan
    # The sync's own delete gate: no delete while another baseline-ready sync
    # with this model enabled has no comparable state - it may assert the row
    # without ever having written it, so rule 4 would not see it.
    from .workload_state import _peer_delete_protection

    _ids, unrepresented_peer, _rows = _peer_delete_protection(
        sync, model_string, state.identity_contract_hash
    )
    if unrepresented_peer:
        plan["refused"] = REFUSED_UNREPRESENTED_PEER
        return plan
    present = _other_syncs_present_keys(sync, model_string)
    if present is None:
        plan["refused"] = REFUSED_STATE_UNREADABLE
        return plan
    child_keys, child_refusal = _child_asserted_keys(sync, model_string)
    if child_refusal:
        plan["refused"] = child_refusal
        return plan
    present |= child_keys

    modules_enabled = sync.is_model_enabled("dcim.module")
    fetched_names = set()
    state_keys_by_row = []
    for value in entries.values():
        if value.get("action") != "upsert":
            continue
        row = value.get("row") or {}
        keys = state_row_keys(model_string, row)
        if not keys:
            # A row the cleanup cannot key would make the NetBox row it
            # describes look absent. Fail closed for the whole model.
            plan["refused"] = REFUSED_STATE_ROW_WITHOUT_KEY
            return plan
        present |= keys
        if value.get("row_hash") == CARRIED_ROW_HASH:
            continue
        devices = state_row_devices(model_string, row)
        fetched_names |= devices
        if _is_judged_state_row(modules_enabled, model_string, row):
            state_keys_by_row.append((devices, keys))

    device_name_by_id = {
        pk: _device(name)
        for chunk in _chunks(scope_ids)
        for pk, name in Device.objects.filter(pk__in=chunk).values_list("pk", "name")
        if _device(name) in fetched_names
    }
    eligible_names = set(device_name_by_id.values())
    plan["devices_judged"] = len(device_name_by_id)
    netbox_rows = _COLLECTORS[model_string](device_name_by_id)
    plan["netbox_rows"] = len(netbox_rows)

    # The state must describe NetBox: most state rows for these devices have to
    # find a NetBox row. If they do not, the reverse mapping is wrong here and
    # absence would be an artefact of it.
    netbox_keys = set()
    for row in netbox_rows.values():
        netbox_keys |= row.keys
    judged_state = [
        keys
        for devices, keys in state_keys_by_row
        if devices and devices <= eligible_names
    ]
    plan["state_rows"] = len(judged_state)
    matched = sum(1 for keys in judged_state if keys & netbox_keys)
    if (
        len(judged_state) >= MIN_REMOVAL_ROWS
        and matched < len(judged_state) * MIN_STATE_MATCH_FRACTION
    ):
        plan["refused"] = REFUSED_STATE_MISMATCH
        return plan

    absent = [row for row in netbox_rows.values() if not (row.keys & present)]
    plan["not_returned"] = len(absent)
    if (
        len(absent) >= MIN_REMOVAL_ROWS
        and netbox_rows
        and len(absent) / len(netbox_rows) * 100 > MAX_REMOVAL_PERCENT
    ):
        plan["refused"] = REFUSED_FRACTION
        return plan

    created, outside = _ownership(model_string, [row.pk for row in absent], request_ids)
    # The opt-in reach: no create record, nothing retained by anyone else, and
    # the row was created before the oldest change NetBox still holds - so the
    # log provably cannot say who made or touched it. A row newer than that
    # point lacks a create record for some other reason and is never in here.
    older_than_log = set()
    if change_log_cutoff is not None:
        unrecorded = [
            row.pk
            for row in absent
            if not row.hold and row.pk not in created and row.pk not in outside
        ]
        created_at = _created_times(model_string, unrecorded)
        first_sync = first_sync_created_time()
        older_than_log = {
            pk
            for pk in unrecorded
            if pk in created_at
            and created_at[pk] < change_log_cutoff
            and first_sync is not None
            and created_at[pk] >= first_sync
        }
    survivors = []
    for row in absent:
        if row.hold:
            _hold(plan, row.hold)
        elif row.pk not in created:
            if row.pk not in older_than_log:
                _hold(plan, HOLD_NO_CREATE_RECORD)
            elif include_older_than_change_log:
                row.by_option = True
                survivors.append(row)
            else:
                _hold(plan, HOLD_OLDER_THAN_CHANGE_LOG)
        elif row.pk in outside:
            _hold(plan, HOLD_CHANGED_OUTSIDE_SYNC)
        else:
            survivors.append(row)
    referenced = _locally_referenced(model_string, survivors)
    for row in survivors:
        if row.pk in referenced:
            _hold(plan, HOLD_LOCALLY_REFERENCED)
        else:
            plan["deletable"][row.pk] = row
    return plan


def _hold_primary_addresses(plan):
    from dcim.models import Device
    from django.db.models import Q

    pks = list(plan["deletable"])
    if not pks:
        return
    held = set()
    for chunk in _chunks(pks):
        for values in Device.objects.filter(
            Q(primary_ip4_id__in=chunk)
            | Q(primary_ip6_id__in=chunk)
            | Q(oob_ip_id__in=chunk)
        ).values_list("primary_ip4_id", "primary_ip6_id", "oob_ip_id"):
            held |= set(values) & set(chunk)
    for pk in held:
        plan["deletable"].pop(pk, None)
        _hold(plan, HOLD_PRIMARY_ADDRESS)


def _settle_interface_dependencies(plans):
    """An interface goes only bare; a MAC that stays primary on one stays.

    Deleting an interface cascades its addresses and MACs and strands half a
    cable, so an interface is deletable only when every address, MAC, cable,
    child and LAG member on it is itself leaving. And deleting a MAC that is the
    primary MAC of an interface that stays would quietly blank that field.
    Iterated to a fixed point: holding one side can hold the other.
    """
    from dcim.models import CableTermination
    from dcim.models import Interface
    from dcim.models import MACAddress
    from django.contrib.contenttypes.models import ContentType
    from ipam.models import IPAddress

    interfaces = plans.get("dcim.interface") or {}
    ips = plans.get("ipam.ipaddress") or {}
    macs = plans.get("dcim.macaddress") or {}
    cables = plans.get("dcim.cable") or {}
    ospf_interfaces = plans.get("netbox_routing.ospfinterface") or {}
    inventory_items = plans.get("dcim.inventoryitem") or {}
    interface_type = ContentType.objects.get_for_model(Interface)

    def deletable(plan):
        return set(plan.get("deletable") or ())

    attached = {}
    candidate_interfaces = deletable(interfaces)
    for chunk in _chunks(candidate_interfaces):
        for model, label in ((IPAddress, "ip"), (MACAddress, "mac")):
            for pk, interface_id in model.objects.filter(
                assigned_object_type=interface_type, assigned_object_id__in=chunk
            ).values_list("pk", "assigned_object_id"):
                attached.setdefault(interface_id, set()).add((label, pk))
        for cable_id, interface_id in CableTermination.objects.filter(
            termination_type=interface_type, termination_id__in=chunk
        ).values_list("cable_id", "termination_id"):
            attached.setdefault(interface_id, set()).add(("cable", cable_id))
        for pk, parent_id, lag_id in Interface.objects.filter(
            _parent_or_lag_q(chunk)
        ).values_list("pk", "parent_id", "lag_id"):
            for owner in (parent_id, lag_id):
                if owner in candidate_interfaces:
                    attached.setdefault(owner, set()).add(("interface", pk))
        for label, pk, interface_id in _interface_dependants(chunk):
            attached.setdefault(interface_id, set()).add((label, pk))
    primary_mac_owner = {}
    if any(f.name == "primary_mac_address" for f in Interface._meta.fields):
        for chunk in _chunks(deletable(macs)):
            primary_mac_owner.update(
                {
                    mac_pk: interface_pk
                    for interface_pk, mac_pk in Interface.objects.filter(
                        primary_mac_address_id__in=chunk
                    ).values_list("pk", "primary_mac_address_id")
                }
            )

    while True:
        leaving = {
            "ip": deletable(ips),
            "mac": deletable(macs),
            "cable": deletable(cables),
            "interface": deletable(interfaces),
            "ospf": deletable(ospf_interfaces),
            "inventory": deletable(inventory_items),
        }
        changed = False
        for pk in list(leaving["interface"]):
            if any(pk2 not in leaving[label] for label, pk2 in attached.get(pk, ())):
                interfaces["deletable"].pop(pk, None)
                _hold(interfaces, HOLD_ATTACHED_ROWS_STAY)
                changed = True
        leaving["interface"] = deletable(interfaces)
        for mac_pk, interface_pk in primary_mac_owner.items():
            if mac_pk in leaving["mac"] and interface_pk not in leaving["interface"]:
                macs["deletable"].pop(mac_pk, None)
                _hold(macs, HOLD_PRIMARY_MAC)
                changed = True
        if not changed:
            return


def _parent_or_lag_q(pks):
    """Interfaces whose parent or LAG is one of these."""
    from django.db.models import Q

    return Q(parent_id__in=pks) | Q(lag_id__in=pks)


def _interface_dependants(interface_pks):
    """``(label, pk, interface pk)`` for OSPF interfaces and inventory items on them.

    Deleting an interface cascades both, and either may be a row Forward still
    returns, so an interface goes only when each of them is itself leaving.
    """
    from dcim.models import Interface
    from dcim.models import InventoryItem
    from django.apps import apps
    from django.contrib.contenttypes.models import ContentType

    found = []
    if apps.is_installed("netbox_routing"):
        OSPFInterface = apps.get_model("netbox_routing", "OSPFInterface")
        found.extend(
            ("ospf", pk, interface_id)
            for pk, interface_id in OSPFInterface.objects.filter(
                interface_id__in=interface_pks
            ).values_list("pk", "interface_id")
        )
    interface_type = ContentType.objects.get_for_model(Interface)
    found.extend(
        ("inventory", pk, component_id)
        for pk, component_id in InventoryItem.objects.filter(
            component_type=interface_type, component_id__in=interface_pks
        ).values_list("pk", "component_id")
    )
    return found


def _settle_parent_dependencies(plans):
    """A parent goes only when every child on it is itself leaving.

    An OSPF instance with an OSPF interface that stays, or a BGP address family
    with a peer address family that stays, is still wanted: applying the child
    writes the parent, and deleting the parent would cascade the child.
    """
    from django.apps import apps

    if not apps.is_installed("netbox_routing"):
        return

    def settle(parent_model, child_model, related_model, parent_field):
        parent = plans.get(parent_model) or {}
        child = plans.get(child_model) or {}
        candidates = set(parent.get("deletable") or ())
        if not candidates:
            return
        leaving = set(child.get("deletable") or ())
        stays = set()
        for chunk in _chunks(candidates):
            for child_pk, parent_pk in related_model.objects.filter(
                **{f"{parent_field}__in": chunk}
            ).values_list("pk", parent_field):
                if child_pk not in leaving:
                    stays.add(parent_pk)
        for pk in stays:
            parent["deletable"].pop(pk, None)
            _hold(parent, HOLD_ATTACHED_ROWS_STAY)

    settle(
        "netbox_routing.ospfinstance",
        "netbox_routing.ospfinterface",
        apps.get_model("netbox_routing", "OSPFInterface"),
        "instance_id",
    )
    settle(
        "netbox_routing.bgpaddressfamily",
        "netbox_routing.bgppeeraddressfamily",
        apps.get_model("netbox_routing", "BGPPeerAddressFamily"),
        "address_family_id",
    )


def plan_stale_row_cleanup(
    sync, *, include_older_than_change_log=False, change_log_cutoff=None
):
    """What the cleanup would delete and hold, per model. Deletes nothing.

    Returns ``{model: plan}``; a plan's ``deletable`` maps pk -> NetBoxRow and
    never leaves this module (see `summarize_plan`).

    ``include_older_than_change_log`` is the operator's opt-in (see the module
    docstring, rule 4); ``change_log_cutoff`` is the oldest retained change it
    is measured against - read now when not given, and pinned by the removal to
    the one its preview recorded so it can never reach further than the preview.
    """
    from .routing_catalogue_cleanup import _sync_request_ids

    request_ids = _sync_request_ids(sync)
    scope_ids = in_scope_device_ids(sync)
    latest_ingestion_id = _latest_finalized_ingestion_id(sync)
    if change_log_cutoff is None:
        change_log_cutoff = oldest_retained_change_time()
    plans = {
        model_string: _plan_model(
            sync,
            model_string,
            request_ids,
            scope_ids,
            latest_ingestion_id,
            include_older_than_change_log=include_older_than_change_log,
            change_log_cutoff=change_log_cutoff,
        )
        for model_string in STALE_ROW_CLEANUP_MODELS
    }
    if not (plans["ipam.ipaddress"].get("refused")):
        _hold_primary_addresses(plans["ipam.ipaddress"])
    _settle_parent_dependencies(plans)
    _settle_interface_dependencies(plans)
    for plan in plans.values():
        plan["removable"] = len(plan["deletable"])
        plan["older_than_change_log"] = sum(
            1 for row in plan["deletable"].values() if row.by_option
        )
    return plans


def summarize_plan(plans, *, include_older_than_change_log=False, cutoff=None):
    """Counts only, in the shape the job stores and the page reads.

    Records the operator's choice and the change-log point it was measured
    against, so the removal can run on exactly that and no wider.
    """
    models = {}
    for model_string in STALE_ROW_CLEANUP_MODELS:
        plan = plans.get(model_string) or {}
        models[model_string] = {
            key: value for key, value in plan.items() if key != "deletable"
        }
        models[model_string]["held"] = dict(sorted(plan.get("held", {}).items()))
        # Primary keys of the rows only the option reached: the removal may act
        # on these and no others, whatever the change log does in between.
        models[model_string]["option_row_pks"] = sorted(
            pk for pk, row in (plan.get("deletable") or {}).items() if row.by_option
        )
    return {
        "generated_at": timezone.now().isoformat(),
        "options": {"older_than_change_log": bool(include_older_than_change_log)},
        "oldest_change_logged_at": cutoff.isoformat() if cutoff is not None else None,
        "models": models,
        "totals": {
            "not_returned": sum(m.get("not_returned", 0) for m in models.values()),
            "removable": sum(m.get("removable", 0) for m in models.values()),
            "older_than_change_log": sum(
                m.get("older_than_change_log", 0) for m in models.values()
            ),
            "held": sum(sum(m.get("held", {}).values()) for m in models.values()),
            "refused_models": sum(1 for m in models.values() if m.get("refused")),
        },
    }


def preview_stale_row_cleanup(sync, *, include_older_than_change_log=False):
    include = bool(include_older_than_change_log)
    cutoff = oldest_retained_change_time()
    plans = plan_stale_row_cleanup(
        sync, include_older_than_change_log=include, change_log_cutoff=cutoff
    )
    return summarize_plan(plans, include_older_than_change_log=include, cutoff=cutoff)


# --- the removal -------------------------------------------------------------


def latest_preview_job(sync):
    from core.choices import JobStatusChoices
    from core.models import Job
    from django.contrib.contenttypes.models import ContentType

    from ..models import ForwardSync

    return (
        Job.objects.filter(
            object_type=ContentType.objects.get_for_model(ForwardSync),
            object_id=sync.pk,
            name__icontains=PREVIEW_JOB_SUFFIX,
            status=JobStatusChoices.STATUS_COMPLETED,
        )
        .order_by("-created")
        .first()
    )


def _preview_counts(sync, preview_job_pk=None):
    """The preview the operator reviewed, or a refusal.

    Returns ``(job pk, {model: removable}, {model: removable only by the
    option}, options)``, where ``options`` is the choice the preview was run
    with and the change-log point it was measured against.
    """
    from datetime import datetime

    from core.choices import JobStatusChoices
    from core.models import Job

    from ..models import ForwardIngestion

    job = latest_preview_job(sync)
    if preview_job_pk:
        # The preview the operator clicked from, if it is still this sync's
        # completed preview; a newer one replaces it only by being reviewed.
        job = Job.objects.filter(
            pk=preview_job_pk,
            object_id=sync.pk,
            name__icontains=PREVIEW_JOB_SUFFIX,
            status=JobStatusChoices.STATUS_COMPLETED,
        ).first()
    data = job.data if job is not None and isinstance(job.data, dict) else {}
    if not data.get("models") or not data.get("generated_at"):
        raise StaleRowCleanupRefused(REFUSED_NO_PREVIEW)
    generated_at = datetime.fromisoformat(data["generated_at"])
    latest = (
        ForwardIngestion.objects.filter(sync=sync, merge_finalized_at__isnull=False)
        .order_by("-merge_finalized_at")
        .values_list("merge_finalized_at", flat=True)
        .first()
    )
    if latest is not None and generated_at < latest:
        raise StaleRowCleanupRefused(REFUSED_PREVIEW_STALE)
    options = {
        "older_than_change_log": bool(
            (data.get("options") or {}).get("older_than_change_log")
        ),
        "oldest_change_logged_at": data.get("oldest_change_logged_at"),
    }
    options["option_row_pks"] = {
        model: set((values or {}).get("option_row_pks") or ())
        for model, values in data["models"].items()
    }
    return (
        job.pk,
        {
            model: int((values or {}).get("removable") or 0)
            for model, values in data["models"].items()
        },
        {
            model: int((values or {}).get("older_than_change_log") or 0)
            for model, values in data["models"].items()
        },
        options,
    )


class _RolledBack(Exception):
    def __init__(self, reason):
        super().__init__(reason)
        self.reason = reason


def _interface_still_carries_rows(pk):
    from dcim.models import CableTermination
    from dcim.models import Interface
    from dcim.models import MACAddress
    from django.contrib.contenttypes.models import ContentType
    from ipam.models import IPAddress

    interface_type = ContentType.objects.get_for_model(Interface)
    return bool(_interface_dependants([pk])) or (
        IPAddress.objects.filter(
            assigned_object_type=interface_type, assigned_object_id=pk
        ).exists()
        or MACAddress.objects.filter(
            assigned_object_type=interface_type, assigned_object_id=pk
        ).exists()
        or CableTermination.objects.filter(
            termination_type=interface_type, termination_id=pk
        ).exists()
        or Interface.objects.filter(_parent_or_lag_q([pk])).exists()
    )


def _owned_by_sync(model_string, pks, request_ids):
    """Whether every pk was created by this sync and changed by nobody else."""
    pks = set(pks)
    created, outside = _ownership(model_string, pks, request_ids)
    return pks <= created and not (pks & outside)


def _has_rows(objs):
    exists = getattr(objs, "exists", None)
    return exists() if callable(exists) else bool(objs)


def _cascade_reaches_other_rows(model_string, obj, request_ids):
    """Would deleting ``obj`` remove or blank anything that may not be ours?

    Asked of Django's deletion collector - the code ``.delete()`` itself runs -
    so it follows cascades, generic relations and SET_NULL references by
    construction. A cascade may take NetBox's own cable/tag bookkeeping, and a
    row of a model the sync may delete that this sync created and nobody else
    changed (a BGP peer's address families). Anything else holds the row.
    Raises ProtectedError/RestrictedError as ``.delete()`` would.
    """
    from django.db import DEFAULT_DB_ALIAS
    from django.db.models.deletion import Collector

    collector = Collector(using=DEFAULT_DB_ALIAS)
    collector.collect([obj])
    for cascade_model, instances in collector.data.items():
        label = cascade_model._meta.label_lower
        pks = {instance.pk for instance in instances}
        if label == model_string and pks == {obj.pk}:
            continue
        if label in _CASCADE_BOOKKEEPING or not pks:
            continue
        if label == model_string or not sync_may_delete_cascade(label):
            return True
        if not _owned_by_sync(label, pks, request_ids):
            return True
    for queryset in collector.fast_deletes:
        label = queryset.model._meta.label_lower
        if label in _CASCADE_BOOKKEEPING:
            continue
        pks = set(queryset.values_list("pk", flat=True))
        if not pks:
            continue
        if not sync_may_delete_cascade(label) or not _owned_by_sync(
            label, pks, request_ids
        ):
            return True
    for key, batches in collector.field_updates.items():
        field = key[0] if isinstance(key, tuple) else key
        if model_string == "dcim.cable" and field.name in _CABLE_FIELD_UPDATES:
            continue
        if any(_has_rows(batch) for batch in batches):
            return True
    return False


def sync_may_delete_cascade(label):
    """A cascaded row's model is one the sync's own delete path removes."""
    from .branch_budget import DEVICE_SHARD_MODELS

    return label in DEVICE_SHARD_MODELS and label in BASELINE_REMOVAL_MODELS


def _bgp_parents(model, pk):
    """The scope and router a BGP peer delete may also remove."""
    values = model.objects.filter(pk=pk).values_list("scope_id", "scope__router_id")
    return next(iter(values), (None, None))


def _delete_one(runner, model_string, model, row, request_ids=()):
    """One row through the sync's own delete adapter. Returns a hold or None."""
    from django.db import transaction
    from django.db.models.deletion import ProtectedError
    from django.db.models.deletion import RestrictedError
    from rq.timeouts import JobTimeoutException

    from ..exceptions import ForwardDependencySkipError
    from ..exceptions import ForwardSearchError

    handler = getattr(runner, f"_delete_{model_string.replace('.', '_')}")
    try:
        with transaction.atomic():
            if not model.objects.filter(pk=row.pk).exists():
                return HOLD_ALREADY_GONE
            if model_string == "dcim.interface" and _interface_still_carries_rows(
                row.pk
            ):
                raise _RolledBack(HOLD_ATTACHED_ROWS_STAY)
            if _cascade_reaches_other_rows(
                model_string, model.objects.get(pk=row.pk), request_ids
            ):
                raise _RolledBack(HOLD_CASCADE_REACHES_OTHER_ROWS)
            parents = ()
            if model_string == "netbox_routing.bgppeer":
                parents = _bgp_parents(model, row.pk)
            if not handler(dict(row.delete_row)):
                raise _RolledBack(HOLD_NOT_RESOLVED)
            # The adapter resolves by identity, not pk; if that reached any
            # other object, undo it rather than delete the wrong row.
            if model.objects.filter(pk=row.pk).exists():
                raise _RolledBack(HOLD_NOT_RESOLVED)
            # The peer adapter also removes its scope and router once nothing
            # protects them. Only when this sync made them and nobody else
            # touched them.
            for label, parent_pk in zip(
                ("netbox_routing.bgpscope", "netbox_routing.bgprouter"), parents
            ):
                if parent_pk is None:
                    continue
                if not _model_class(label).objects.filter(pk=parent_pk).exists() and (
                    not _owned_by_sync(label, {parent_pk}, request_ids)
                ):
                    raise _RolledBack(HOLD_SHARED_PARENT_NOT_OURS)
    except _RolledBack as exc:
        return exc.reason
    except (ForwardDependencySkipError, ProtectedError, RestrictedError):
        return HOLD_PROTECTED
    except ForwardSearchError:
        return HOLD_AMBIGUOUS
    except JobTimeoutException:
        raise
    except Exception:  # noqa: BLE001 - one row's failure must not stop the rest
        return HOLD_FAILED
    return None


def remove_stale_rows(
    sync,
    *,
    preview_job_pk=None,
    runner_factory=None,
    include_older_than_change_log=None,
):
    """Delete what a fresh plan finds, within what the preview showed.

    Operator-initiated only. Refused outright without a completed preview newer
    than the latest finalized run; per model, refused when the fresh plan would
    remove more than the preview the operator saw. Returns value-free counts.

    The "older than the change log" opt-in is the PREVIEW's choice, measured
    against the change-log point the preview recorded. ``None`` takes the
    preview's choice; ``False`` narrows it; ``True`` against a preview that did
    not opt in is refused outright - the removal never widens what was reviewed.
    """
    from datetime import datetime

    from .logging import SyncLogging
    from .sync import ForwardSyncRunner

    from core.choices import JobStatusChoices

    from .routing_catalogue_cleanup import _sync_request_ids
    from .sync_facade import sync_run_job_names

    # The enqueue guard checked this when the job was queued; a sync may have
    # started since, and deleting under a running apply races it.
    if sync.jobs.filter(
        name__in=sync_run_job_names(sync),
        status__in=(JobStatusChoices.STATUS_PENDING, JobStatusChoices.STATUS_RUNNING),
    ).exists():
        raise StaleRowCleanupRefused(REFUSED_SYNC_RUNNING)
    preview_pk, previewed, previewed_by_option, preview_options = _preview_counts(
        sync, preview_job_pk
    )
    previewed_option = preview_options["older_than_change_log"]
    if include_older_than_change_log and not previewed_option:
        raise StaleRowCleanupRefused(REFUSED_OPTION_WIDER_THAN_PREVIEW)
    use_option = bool(previewed_option) and include_older_than_change_log is not False
    cutoff = None
    if use_option and preview_options["oldest_change_logged_at"]:
        cutoff = datetime.fromisoformat(preview_options["oldest_change_logged_at"])
    # A preview that recorded no change-log point reached nothing by the option.
    use_option = use_option and cutoff is not None
    plans = plan_stale_row_cleanup(
        sync, include_older_than_change_log=use_option, change_log_cutoff=cutoff
    )
    request_ids = _sync_request_ids(sync)

    def new_runner():
        if runner_factory is not None:
            return runner_factory()
        return ForwardSyncRunner(
            sync=sync, ingestion=None, client=None, logger_=SyncLogging()
        )

    runner = new_runner()
    results = {}
    for model_string in STALE_ROW_CLEANUP_MODELS:
        plan = plans[model_string]
        summary = {key: value for key, value in plan.items() if key != "deletable"}
        summary["held"] = dict(plan.get("held") or {})
        summary["deleted"] = 0
        summary["deleted_older_than_change_log"] = 0
        if not plan.get("refused") and (
            len(plan["deletable"]) > previewed.get(model_string, 0)
            or plan.get("older_than_change_log", 0)
            > previewed_by_option.get(model_string, 0)
            or not {pk for pk, row in plan["deletable"].items() if row.by_option}
            <= preview_options["option_row_pks"].get(model_string, set())
        ):
            summary["refused"] = REFUSED_GREW_SINCE_PREVIEW
        if summary.get("refused"):
            results[model_string] = summary
            continue
        model = _model_class(model_string)
        pending = sorted(plan["deletable"])
        while pending:
            # A parent interface waits for its leaving subinterfaces and LAG
            # members, which may sort after it: retry what was held only for
            # that, until a pass deletes nothing more.
            retry = []
            for pk in pending:
                reason = _delete_one(
                    runner, model_string, model, plan["deletable"][pk], request_ids
                )
                if reason is None:
                    summary["deleted"] += 1
                    if plan["deletable"][pk].by_option:
                        summary["deleted_older_than_change_log"] += 1
                    continue
                # A rolled-back delete may have left the runner's lookup caches
                # describing rows that came back; start clean.
                runner = new_runner()
                if reason == HOLD_ATTACHED_ROWS_STAY:
                    retry.append(pk)
                else:
                    summary["held"][reason] = summary["held"].get(reason, 0) + 1
            if len(retry) == len(pending):
                summary["held"][HOLD_ATTACHED_ROWS_STAY] = summary["held"].get(
                    HOLD_ATTACHED_ROWS_STAY, 0
                ) + len(retry)
                break
            pending = retry
        results[model_string] = summary
    return {
        "preview_job": preview_pk,
        "finished_at": timezone.now().isoformat(),
        "options": {"older_than_change_log": use_option},
        "models": results,
        "totals": {
            "deleted": sum(r["deleted"] for r in results.values()),
            "deleted_older_than_change_log": sum(
                r["deleted_older_than_change_log"] for r in results.values()
            ),
            "held": sum(sum(r["held"].values()) for r in results.values()),
            "refused_models": sum(1 for r in results.values() if r.get("refused")),
        },
    }
