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
   whose NetBox -> identity mapping can be built exactly.
2. Every device it belongs to is in the sync's CURRENT scope (owned by this
   sync, scope-claimed when the source is tag-scoped, not out of scope,
   uncovered or backfilled) AND was fetched on the last promoted run (it has
   live rows in this model's state). Rows of devices that left scope are kept
   on purpose: see `out_of_scope_entries` in workload_state.py.
3. Its identity is absent from every current durable state for the model -
   this sync's and every other sync's. Matching is deliberately COARSER than
   the stored identity (an interface by name or abbreviation, an address by
   host, a MAC by value, a BGP peer by device and neighbour host), so a
   formatting difference can only make a row look present, never absent.
4. NetBox's change log shows this sync's ingestions created it and nothing
   but this sync's ingestions ever changed it. A row added or edited by hand
   is never a candidate. A row whose create record was removed by changelog
   retention is held, not guessed at.
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

# Delete order: what hangs off an interface goes before the interface, so an
# interface is only ever deleted bare. BGP peers first: a peer PROTECTs the
# address it points at.
STALE_ROW_CLEANUP_MODELS = (
    "netbox_routing.bgppeer",
    "dcim.cable",
    "ipam.ipaddress",
    "dcim.macaddress",
    "dcim.interface",
)

# Left out, and why (the plan file carries the same list):
#   dcim.inventoryitem, dcim.module - identity is (device, name/part/serial)
#     variants that the module and inventory paths rewrite; no exact reverse.
#   ipam.fhrpgroup - one group is shared by several devices' participants.
#   extras.taggeditem - a tag assignment, not a row; the scope-tag job owns it.
#   netbox_routing.bgpaddressfamily / bgppeeraddressfamily / ospf* - removed
#     with their peer or instance by cascade, or reverse identity is several
#     hops through generic keys.
#   netbox_peering_manager.peeringsession - goes with its BGP peer.

# The state must cover most of the rows it describes, or the reverse mapping
# is wrong for this estate and absence means nothing.
MIN_STATE_MATCH_FRACTION = 0.5

# Hold reasons: a row that met the absence rules but is kept.
HOLD_NO_CREATE_RECORD = "no_create_record_from_this_sync"
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
REFUSED_STATE_MISMATCH = "durable_state_does_not_match_netbox"
REFUSED_FRACTION = "too_large_a_share_of_the_model"
REFUSED_UNREPRESENTED_PEER = "another_sync_has_no_comparable_state"
REFUSED_STATE_ROW_WITHOUT_KEY = "durable_state_row_without_an_identity"
REFUSED_GREW_SINCE_PREVIEW = "more_removable_than_the_preview_showed"

# Whole-run refusals (the removal job).
REFUSED_NO_PREVIEW = "no_completed_preview"
REFUSED_PREVIEW_STALE = "preview_older_than_the_latest_run"
REFUSED_SYNC_RUNNING = "a_sync_run_is_active"

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


def state_row_keys(model_string, row):
    """The keys a durable-state row asserts."""
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
    """Devices this sync owns and still has in scope, by NetBox pk."""
    from ..models import ForwardDeviceIdentity
    from ..models import ForwardDeviceTagClaim
    from .sync_facade import device_tag_scope

    device_ids = set(
        ForwardDeviceIdentity.objects.filter(sync=sync).values_list(
            "device_id", flat=True
        )
    )
    include_tags, _exclude, _match = device_tag_scope(sync)
    if include_tags:
        device_ids &= set(
            ForwardDeviceTagClaim.objects.filter(
                sync=sync, claim_type="scope"
            ).values_list("device_id", flat=True)
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
    __slots__ = ("pk", "keys", "delete_row", "hold", "interface_id")

    def __init__(self, pk, keys, delete_row, *, hold=None, interface_id=None):
        self.pk = pk
        self.keys = keys
        self.delete_row = delete_row
        self.hold = hold
        self.interface_id = interface_id


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


_COLLECTORS = {
    "dcim.interface": _interface_rows,
    "ipam.ipaddress": _ipaddress_rows,
    "dcim.macaddress": _macaddress_rows,
    "dcim.cable": _cable_rows,
    "netbox_routing.bgppeer": _bgppeer_rows,
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


def _plan_model(sync, model_string, request_ids, scope_ids, latest_ingestion_id):
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
    survivors = []
    for row in absent:
        if row.hold:
            _hold(plan, row.hold)
        elif row.pk not in created:
            _hold(plan, HOLD_NO_CREATE_RECORD)
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


def plan_stale_row_cleanup(sync):
    """What the cleanup would delete and hold, per model. Deletes nothing.

    Returns ``{model: plan}``; a plan's ``deletable`` maps pk -> NetBoxRow and
    never leaves this module (see `summarize_plan`).
    """
    from .routing_catalogue_cleanup import _sync_request_ids

    request_ids = _sync_request_ids(sync)
    scope_ids = in_scope_device_ids(sync)
    latest_ingestion_id = _latest_finalized_ingestion_id(sync)
    plans = {
        model_string: _plan_model(
            sync, model_string, request_ids, scope_ids, latest_ingestion_id
        )
        for model_string in STALE_ROW_CLEANUP_MODELS
    }
    if not (plans["ipam.ipaddress"].get("refused")):
        _hold_primary_addresses(plans["ipam.ipaddress"])
    _settle_interface_dependencies(plans)
    for plan in plans.values():
        plan["removable"] = len(plan["deletable"])
    return plans


def summarize_plan(plans):
    """Counts only, in the shape the job stores and the page reads."""
    models = {}
    for model_string in STALE_ROW_CLEANUP_MODELS:
        plan = plans.get(model_string) or {}
        models[model_string] = {
            key: value for key, value in plan.items() if key != "deletable"
        }
        models[model_string]["held"] = dict(sorted(plan.get("held", {}).items()))
    return {
        "generated_at": timezone.now().isoformat(),
        "models": models,
        "totals": {
            "not_returned": sum(m.get("not_returned", 0) for m in models.values()),
            "removable": sum(m.get("removable", 0) for m in models.values()),
            "held": sum(sum(m.get("held", {}).values()) for m in models.values()),
            "refused_models": sum(1 for m in models.values() if m.get("refused")),
        },
    }


def preview_stale_row_cleanup(sync):
    return summarize_plan(plan_stale_row_cleanup(sync))


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
    """The preview the operator reviewed, or a refusal."""
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
    return job.pk, {
        model: int((values or {}).get("removable") or 0)
        for model, values in data["models"].items()
    }


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
    return (
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


def remove_stale_rows(sync, *, preview_job_pk=None, runner_factory=None):
    """Delete what a fresh plan finds, within what the preview showed.

    Operator-initiated only. Refused outright without a completed preview newer
    than the latest finalized run; per model, refused when the fresh plan would
    remove more than the preview the operator saw. Returns value-free counts.
    """
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
    preview_pk, previewed = _preview_counts(sync, preview_job_pk)
    plans = plan_stale_row_cleanup(sync)
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
        if not plan.get("refused") and len(plan["deletable"]) > previewed.get(
            model_string, 0
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
        "models": results,
        "totals": {
            "deleted": sum(r["deleted"] for r in results.values()),
            "held": sum(sum(r["held"].values()) for r in results.values()),
            "refused_models": sum(1 for r in results.values() if r.get("refused")),
        },
    }
