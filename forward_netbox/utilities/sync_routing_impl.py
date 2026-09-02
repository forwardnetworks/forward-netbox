import hashlib

from django.core.exceptions import ObjectDoesNotExist
from django.db.models.deletion import ProtectedError

from ..exceptions import ForwardDependencySkipError
from ..exceptions import ForwardQueryError
from ..exceptions import ForwardSearchError
from .sync_primitives import forget_lookup_object
from .sync_primitives import remember_lookup_object


def delete_netbox_peering_manager_peeringsession(runner, row):
    PeeringSession = runner._optional_model(
        "netbox_peering_manager",
        "PeeringSession",
        "netbox_peering_manager.peeringsession",
    )
    peer = runner._resolve_bgp_peer_for_delete(row)
    if peer is None:
        return False
    return runner._delete_by_coalesce(PeeringSession, [{"bgp_peer": peer}])


def delete_netbox_routing_bgppeer(runner, row):
    peer = runner._resolve_bgp_peer_for_delete(row)
    if peer is None:
        return False
    scope = getattr(peer, "scope", None)
    router = getattr(scope, "router", None) if scope is not None else None
    forget_lookup_object(runner, peer)
    peer.delete()
    delete_bgp_scope_tree_if_unreferenced(runner=runner, scope=scope, router=router)
    return True


def delete_netbox_routing_bgpaddressfamily(runner, row):
    address_family = runner._resolve_bgp_address_family_for_delete(row)
    if address_family is None:
        return False
    forget_lookup_object(runner, address_family)
    address_family.delete()
    return True


def delete_netbox_routing_bgppeeraddressfamily(runner, row):
    BGPPeerAddressFamily = runner._optional_model(
        "netbox_routing",
        "BGPPeerAddressFamily",
        "netbox_routing.bgppeeraddressfamily",
    )
    peer = runner._resolve_bgp_peer_for_delete(row)
    if peer is None:
        return False
    address_family = runner._resolve_bgp_address_family_for_delete(row)
    if address_family is None:
        return False
    return runner._delete_by_coalesce(
        BGPPeerAddressFamily,
        [
            {
                "assigned_object_type": runner._content_type_for(peer.__class__),
                "assigned_object_id": peer.pk,
                "address_family": address_family,
            }
        ],
    )


def delete_bgp_scope_tree_if_unreferenced(*, runner, scope, router):
    for obj in (scope, router):
        if obj is None or getattr(obj, "pk", None) is None:
            continue
        try:
            forget_lookup_object(runner, obj)
            obj.delete()
        except ProtectedError:
            continue


def delete_netbox_routing_ospfinstance(runner, row):
    from ipam.models import VRF

    OSPFInstance = runner._optional_model(
        "netbox_routing", "OSPFInstance", "netbox_routing.ospfinstance"
    )
    device = runner._lookup_device_by_name(row.get("device"))
    if device is None:
        return False
    vrf = None
    if row.get("vrf"):
        vrf = runner._get_unique_or_raise(VRF, {"name": row["vrf"]})
        if vrf is None:
            return False
    process_id, _ = runner._ospf_process_values(row)
    return runner._delete_by_coalesce(
        OSPFInstance,
        [{"device": device, "vrf": vrf, "process_id": process_id}],
    )


def delete_netbox_routing_ospfarea(runner, row):
    OSPFArea = runner._optional_model(
        "netbox_routing", "OSPFArea", "netbox_routing.ospfarea"
    )
    return runner._delete_by_coalesce(
        OSPFArea,
        [{"area_id": str(row.get("area_id"))}],
    )


def delete_netbox_routing_ospfinterface(runner, row):
    OSPFInterface = runner._optional_model(
        "netbox_routing", "OSPFInterface", "netbox_routing.ospfinterface"
    )
    device = runner._lookup_device_by_name(row.get("device"))
    if device is None:
        return False
    interface = lookup_routing_interface_name(
        runner, device, row.get("local_interface")
    )
    if interface is None:
        return False
    return runner._delete_by_coalesce(OSPFInterface, [{"interface": interface}])


def preview_routing_outcome(runner, obj):
    """Classify one routing row from what the shimmed upserts reported.

    A routing row is not one object. A single BGP peer resolves - and the apply
    would write - up to five: two ASNs, the neighbour `IPAddress`, a
    `BGPRouter`, a `BGPScope`, and the peer itself. `netbox_routing.bgprouter`
    and `netbox_routing.bgpscope` have no Forward query of their own; they exist
    only as parents built while applying a peer. So a router this run would
    rewrite is drift that NO model would report if this function looked only at
    the leaf row, and the peer model would read `unchanged` while every run
    rewrote 360 routers - the confident zero this feature exists to prevent.

    Hence the verdict is the strongest outcome across every upsert the row
    performed, taken from the preview runner's own record of them, rather than
    from `last_upsert_would_change` alone the way the flat DLM rows are
    classified.

    ``obj is None`` means a parent was absent, so the leaf cannot already
    exist: the row is a create.
    """
    if obj is None:
        return "creates"
    outcomes = getattr(runner, "upsert_outcomes", ())
    if "creates" in outcomes:
        return "creates"
    if "updates" in outcomes:
        return "updates"
    return "unchanged"


def apply_netbox_routing_bgppeer(runner, row, *, preview=False):
    peer = runner._ensure_netbox_routing_bgppeer(row)
    if preview:
        return preview_routing_outcome(runner, peer)
    return peer


def apply_netbox_routing_bgpaddressfamily(runner, row, *, preview=False):
    address_family = runner._ensure_bgp_address_family(row)
    if preview:
        return preview_routing_outcome(runner, address_family)
    return address_family


def apply_netbox_routing_bgppeeraddressfamily(runner, row, *, preview=False):
    peer_address_family = runner._ensure_bgp_peer_address_family(row)
    if preview:
        return preview_routing_outcome(runner, peer_address_family)
    return peer_address_family


def preview_leaf_outcome(runner, obj):
    """Classify one OSPF row from its OWN upsert, not from its parents'.

    The deliberate difference from `preview_routing_outcome`. A BGP peer's
    `BGPRouter` and `BGPScope` have no Forward query of their own, so the peer
    is the only place their drift can be reported. Every OSPF parent is a
    SEPARATELY measured model - `netbox_routing.ospfinstance` and `ospfarea`
    each have their own query and their own row set - so folding a parent's
    create into the interface's verdict would count the same object twice, once
    under its own model and again under this one.

    So this reads the leaf's own upsert, exactly as the flat DLM rows do.
    """
    if obj is None:
        return "creates"
    return "updates" if runner.last_upsert_would_change else "unchanged"


def apply_netbox_routing_ospfinstance(runner, row, *, preview=False):
    instance = runner._ensure_ospf_instance(row)
    if preview:
        return preview_leaf_outcome(runner, instance)
    return instance


def apply_netbox_routing_ospfarea(runner, row, *, preview=False):
    area = runner._ensure_ospf_area(row)
    if preview:
        return preview_leaf_outcome(runner, area)
    return area


def apply_netbox_routing_ospfinterface(runner, row, *, preview=False):
    ospf_interface = runner._ensure_ospf_interface(row)
    if preview:
        # `False` is this path's own answer for an interface Forward reports
        # that NetBox never imported - a skip the apply records, not drift.
        if ospf_interface is False:
            return False
        return preview_leaf_outcome(runner, ospf_interface)
    return ospf_interface


def apply_netbox_peering_manager_peeringsession(runner, row, *, preview=False):
    """Bind the session to its peer, or - with ``preview`` - classify only.

    Every write in this path is behind a ``runner.`` call the preview overrides,
    including `_ensure_peering_relationship`, which upserts a `Relationship`.
    The one thing it needs of its own is the guard below: the session coalesces
    on `bgp_peer` alone, so an absent peer would look the session up on
    ``{"bgp_peer": None}`` and match whichever unrelated session has no peer.
    """
    PeeringSession = runner._optional_model(
        "netbox_peering_manager",
        "PeeringSession",
        "netbox_peering_manager.peeringsession",
    )
    bgp_peer = runner._ensure_netbox_routing_bgppeer(row)
    if preview and bgp_peer is None:
        # The peer is absent, so its session cannot already exist either.
        return "creates"
    values = runner._model_field_values(
        PeeringSession,
        {
            "bgp_peer": bgp_peer,
            "relationship": runner._ensure_peering_relationship(row),
            "service_reference": row.get("service_reference") or "",
        },
    )
    session, _ = runner._upsert_values_from_defaults(
        "netbox_peering_manager.peeringsession",
        PeeringSession,
        values=values,
        coalesce_sets=[("bgp_peer",)],
    )
    if preview:
        return preview_routing_outcome(runner, session)


#: Returned by `routing_vrf` under preview when the row NAMES a VRF that NetBox
#: does not have. Distinct from `None`, which means the row names no VRF at all
#: and the object genuinely belongs in the global table.
VRF_ABSENT = object()


def bgp_vrf(runner, row, *, preview=False):
    return routing_vrf(runner, row, preview=preview)


def routing_vrf(runner, row, *, preview=False):
    """Resolve the row's VRF; under preview, distinguish absent from global.

    The real `_ensure_vrf` CREATES a missing VRF. The preview runner's override
    resolves and returns `None` instead, which collides with the answer for a
    row that names no VRF at all - and the collision is not cosmetic. Every
    coalesce set below includes `vrf`, so a row whose VRF does not exist yet
    would look its object up on `vrf=None` and match the unrelated GLOBAL one:
    an OSPF instance or BGP scope that the apply would create would be reported
    as already present and unchanged.

    That is the confident-zero failure this feature exists to prevent, so the
    two answers are kept apart and the callers report a create.
    """
    if not row.get("vrf"):
        return None
    vrf = runner._ensure_vrf(
        {
            "name": row["vrf"],
            "rd": None,
            "description": "",
            "enforce_unique": False,
        },
        update_existing=False,
    )
    if preview and vrf is None:
        return VRF_ABSENT
    return vrf


def lookup_device_for_routing(runner, row, model_string, object_label):
    try:
        return runner._get_device_by_name(row["device"])
    except ObjectDoesNotExist as exc:
        key = (row["device"],)
        if runner._dependency_failed("dcim.device", key):
            raise ForwardDependencySkipError(
                f"Skipping {object_label} because dependency `dcim.device` failed for {key}.",
                model_string=model_string,
                dependency="dcim.device",
                context={"device": row["device"]},
                data=row,
            ) from exc
        raise ForwardSearchError(
            f"Unable to find device `{row['device']}` for {object_label}.",
            model_string=model_string,
            context={"device": row["device"]},
            data=row,
        ) from exc


def host_address(address):
    from ipaddress import ip_address

    parsed = ip_address(str(address))
    prefix_length = 32 if parsed.version == 4 else 128
    return f"{parsed}/{prefix_length}"


def lookup_ipaddress_by_host(runner, *, address, vrf):
    from ipaddress import ip_address

    from ipam.models import IPAddress

    host = str(ip_address(str(address)))
    lookup = {"address__net_host": host}
    if vrf is None:
        lookup["vrf__isnull"] = True
    else:
        lookup["vrf"] = vrf
    return runner._get_unique_or_raise(IPAddress, lookup)


def ensure_bgp_peer_ip(runner, row, vrf, *, preview=False):
    """Find the neighbour address, and - under ``preview`` - never create it.

    This save is DIRECT: it is not reached through any ``runner.`` call, so the
    preview runner's firewall does not see it. Same shape as the FHRP virtual
    IP and as cables, and the reason this path needs a ``preview`` argument
    rather than another shim.

    Returning ``None`` under preview says the address is absent. The caller
    treats that as a create for the whole row, which is the honest answer: a
    peer cannot already exist against a neighbour address NetBox does not have.
    """
    from ipam.models import IPAddress

    neighbor_address = row["neighbor_address"]
    existing = lookup_ipaddress_by_host(runner, address=neighbor_address, vrf=vrf)
    if existing is not None:
        return existing
    if preview:
        return None
    ip_obj = IPAddress(
        address=host_address(neighbor_address),
        vrf=vrf,
        status="active",
    )
    ip_obj.full_clean()
    ip_obj.save()
    return ip_obj


def ensure_bgp_router(runner, row, device, local_asn):
    BGPRouter = runner._optional_model(
        "netbox_routing", "BGPRouter", "netbox_routing.bgppeer"
    )
    values = runner._model_field_values(
        BGPRouter,
        {
            "name": f"{device.name} AS{local_asn.asn}"[:100],
            "assigned_object_type": runner._content_type_for(device.__class__),
            "assigned_object_id": device.pk,
            "asn": local_asn,
        },
    )
    router, _ = runner._upsert_values_from_defaults(
        "netbox_routing.bgppeer",
        BGPRouter,
        values=values,
        coalesce_sets=[("assigned_object_type", "assigned_object_id", "asn")],
    )
    return router


def ensure_bgp_scope(runner, row, router, vrf):
    BGPScope = runner._optional_model(
        "netbox_routing", "BGPScope", "netbox_routing.bgppeer"
    )
    values = runner._model_field_values(BGPScope, {"router": router, "vrf": vrf})
    scope, _ = runner._coalesce_upsert(
        "netbox_routing.bgpscope",
        BGPScope,
        coalesce_lookups=[{"router": router, "vrf": vrf}],
        create_values=values,
        update_values=values,
    )
    return scope


def bgp_peer_name(row):
    name = row.get("name") or f"AS{row['peer_asn']} {row['neighbor_address']}"
    return str(name)[:100]


def bgp_peer_comments(row):
    """Stable descriptive state only — never per-snapshot operational counters.

    `session_state`, `advertised_prefixes` and `received_prefixes` move between
    snapshots even when the configured peer is identical. Rendering them here put
    them inside the field that drives change detection, so every sync rewrote
    every peer: 360 UPDATEs, 360 ObjectChanges and 360 branch changes to stage
    and merge, on every run, for a peer whose configuration never changed.

    Forward remains authoritative for the live counters; NetBox carries the
    session's configured identity, and `status` already records active/offline.
    """
    lines = ["Observed by Forward from structured BGP neighbor state."]
    for label, key in (
        ("Router ID", "router_id"),
        ("Peer type", "peer_type"),
        ("Peer device", "peer_device"),
        ("Peer VRF", "peer_vrf"),
        ("Peer router ID", "peer_router_id"),
    ):
        value = row.get(key)
        if value not in ("", None):
            lines.append(f"{label}: {value}")
    return "\n".join(lines)


def rib_presence_label(value):
    if value in ("", None):
        return None
    if isinstance(value, str):
        return "present" if value.strip().lower() == "true" else "absent"
    return "present" if bool(value) else "absent"


def bgp_address_family_comments(row):
    lines = ["Observed by Forward from BGP RIB AFI/SAFI state."]
    if row.get("afi_safi") not in ("", None):
        lines.append(f"Forward AFI/SAFI: {row.get('afi_safi')}")
    return "\n".join(lines)


def bgp_peer_address_family_comments(row):
    lines = [bgp_address_family_comments(row)]
    for label, key in (
        ("Adj-RIB-In post-policy", "has_adj_rib_in"),
        ("Adj-RIB-Out post-policy", "has_adj_rib_out"),
    ):
        state = rib_presence_label(row.get(key))
        if state:
            lines.append(f"{label}: {state}")
    return "\n".join(lines)


def bgp_peer_values(runner, row, *, preview=False):
    """Resolve everything a peer hangs off, or ``None`` if a parent is absent.

    Under ``preview`` the ASNs and the neighbour address resolve rather than
    create, so any of them can come back ``None``. Building the peer's values
    against a missing parent would be worse than useless: ``ensure_bgp_router``
    reads ``local_asn.asn`` for the router name and would raise
    ``AttributeError``, which no caller catches, and a scope coalesced on
    ``router=None`` would match some unrelated row. So an absent parent short-
    circuits to ``None`` and the caller reports the row as a create.
    """
    device = lookup_device_for_routing(
        runner, row, "netbox_routing.bgppeer", "BGP peer"
    )

    vrf = bgp_vrf(runner, row, preview=preview)
    if vrf is VRF_ABSENT:
        # The peer's VRF does not exist yet, so neither can anything coalesced
        # inside it. Resolving on `vrf=None` would match the global scope.
        return None
    local_asn = runner._ensure_asn(row["local_asn"])
    remote_asn = runner._ensure_asn(row["peer_asn"])
    peer_ip = ensure_bgp_peer_ip(runner, row, vrf, preview=preview)
    if preview and (local_asn is None or remote_asn is None or peer_ip is None):
        return None
    router = ensure_bgp_router(runner, row, device, local_asn)
    if preview and router is None:
        return None
    scope = ensure_bgp_scope(runner, row, router, vrf)
    if preview and scope is None:
        return None
    status = row.get("status") or ("active" if row.get("enabled") else "offline")
    if status not in {"active", "planned", "offline", "failed"}:
        status = "active" if row.get("enabled") else "offline"
    return {
        "scope": scope,
        "peer": peer_ip,
        "name": bgp_peer_name(row),
        "remote_as": remote_asn,
        "local_as": local_asn,
        "enabled": bool(row.get("enabled")),
        "status": status,
        "description": str(row.get("description") or "")[:200],
        "comments": bgp_peer_comments(row),
    }


def ensure_netbox_routing_bgppeer(runner, row, *, preview=False):
    BGPPeer = runner._optional_model(
        "netbox_routing", "BGPPeer", "netbox_routing.bgppeer"
    )
    peer_values = bgp_peer_values(runner, row, preview=preview)
    if peer_values is None:
        # Preview only, and only when a parent is absent - see `bgp_peer_values`.
        return None
    values = runner._model_field_values(BGPPeer, peer_values)
    peer, _ = runner._upsert_values_from_defaults(
        "netbox_routing.bgppeer",
        BGPPeer,
        values=values,
        coalesce_sets=[("scope", "peer"), ("scope", "peer", "name")],
    )
    return peer


def normalize_bgp_address_family(afi_safi, *, aliases):
    value = str(afi_safi or "").strip()
    if "." in value:
        value = value.rsplit(".", 1)[-1]
    value = value.lower().replace("_", "-")
    if not value:
        raise ForwardQueryError("BGP address-family row did not include `afi_safi`.")
    return aliases.get(value, value)


def ensure_bgp_scope_for_row(runner, row, model_string, *, preview=False):
    device = lookup_device_for_routing(runner, row, model_string, "BGP scope")
    vrf = bgp_vrf(runner, row, preview=preview)
    if vrf is VRF_ABSENT:
        return None
    local_asn = runner._ensure_asn(row["local_asn"])
    if preview and local_asn is None:
        # The router name is built from `local_asn.asn`; an absent ASN means
        # no router, so no scope, so the row is a create.
        return None
    router = ensure_bgp_router(runner, row, device, local_asn)
    if preview and router is None:
        return None
    return ensure_bgp_scope(runner, row, router, vrf)


def ensure_bgp_address_family(runner, row, *, preview=False):
    BGPAddressFamily = runner._optional_model(
        "netbox_routing",
        "BGPAddressFamily",
        "netbox_routing.bgpaddressfamily",
    )
    scope = ensure_bgp_scope_for_row(
        runner, row, "netbox_routing.bgpaddressfamily", preview=preview
    )
    if preview and scope is None:
        return None
    address_family = normalize_bgp_address_family(
        row.get("afi_safi"), aliases=runner.FORWARD_BGP_ADDRESS_FAMILY_ALIASES
    )
    choices = {
        str(choice[0])
        for choice in BGPAddressFamily._meta.get_field("address_family").choices
    }
    if choices and address_family not in choices:
        raise ForwardQueryError(
            f"Unsupported BGP address family `{row.get('afi_safi')}`.",
            model_string="netbox_routing.bgpaddressfamily",
            context={"afi_safi": row.get("afi_safi")},
            data=row,
        )
    values = runner._model_field_values(
        BGPAddressFamily,
        {
            "scope": scope,
            "address_family": address_family,
            "description": "Observed by Forward from BGP RIB AFI/SAFI state.",
            "comments": bgp_address_family_comments(row),
        },
    )
    address_family_obj, _ = runner._upsert_values_from_defaults(
        "netbox_routing.bgpaddressfamily",
        BGPAddressFamily,
        values=values,
        coalesce_sets=[("scope", "address_family")],
    )
    return address_family_obj


def resolve_bgp_address_family_for_delete(runner, row):
    BGPAddressFamily = runner._optional_model(
        "netbox_routing",
        "BGPAddressFamily",
        "netbox_routing.bgpaddressfamily",
    )
    scope = resolve_bgp_scope_for_delete(runner, row)
    if scope is None:
        return None
    return runner._get_unique_or_raise(
        BGPAddressFamily,
        {
            "scope": scope,
            "address_family": normalize_bgp_address_family(
                row.get("afi_safi"), aliases=runner.FORWARD_BGP_ADDRESS_FAMILY_ALIASES
            ),
        },
    )


def ensure_bgp_peer_address_family(runner, row, *, preview=False):
    BGPPeerAddressFamily = runner._optional_model(
        "netbox_routing",
        "BGPPeerAddressFamily",
        "netbox_routing.bgppeeraddressfamily",
    )
    bgp_peer = ensure_netbox_routing_bgppeer(runner, row, preview=preview)
    if preview and bgp_peer is None:
        return None
    address_family = ensure_bgp_address_family(runner, row, preview=preview)
    if preview and address_family is None:
        return None
    values = runner._model_field_values(
        BGPPeerAddressFamily,
        {
            "assigned_object_type": runner._content_type_for(bgp_peer.__class__),
            "assigned_object_id": bgp_peer.pk,
            "address_family": address_family,
            "enabled": bool(row.get("enabled")),
            "description": "Observed by Forward from BGP RIB AFI/SAFI state.",
            "comments": bgp_peer_address_family_comments(row),
        },
    )
    peer_af, _ = runner._upsert_values_from_defaults(
        "netbox_routing.bgppeeraddressfamily",
        BGPPeerAddressFamily,
        values=values,
        coalesce_sets=[
            ("assigned_object_type", "assigned_object_id", "address_family")
        ],
    )
    return peer_af


def ospf_area_type(value):
    area_type = str(value or "").strip()
    if "." in area_type:
        area_type = area_type.rsplit(".", 1)[-1]
    area_type = area_type.lower().replace("_", "-")
    return {
        "backbone": "backbone",
        "stub": "stub",
        "nssa": "nssa",
        "standard": "standard",
    }.get(area_type, "standard")


def ospf_process_values(row):
    raw_process_id = str(row.get("process_id") or "0").strip() or "0"
    try:
        process_id = int(raw_process_id)
    except ValueError:
        digest_input = "|".join(
            str(value or "")
            for value in (
                row.get("device"),
                row.get("vrf"),
                raw_process_id,
                row.get("domain"),
            )
        )
        digest = hashlib.sha1(digest_input.encode("utf-8")).hexdigest()
        process_id = 1_000_000 + (int(digest[:8], 16) % 1_000_000_000)
    return process_id, raw_process_id


def ospf_instance_comments(row, process_label):
    lines = ["Observed by Forward from structured OSPF state."]
    for label, value in (
        ("Forward process ID", process_label),
        ("Forward domain", row.get("domain")),
    ):
        if value not in ("", None):
            lines.append(f"{label}: {value}")
    return "\n".join(lines)


def ospf_interface_comments(row):
    lines = ["Observed by Forward from structured OSPF neighbor state."]
    for label, key in (
        ("Cost", "cost"),
        ("Role", "role"),
        ("Remote device", "remote_device"),
        ("Remote interface", "remote_interface"),
        ("Remote interface IP", "remote_interface_ip"),
        ("Remote router ID", "remote_router_id"),
    ):
        value = row.get(key)
        if value not in ("", None):
            lines.append(f"{label}: {value}")
    return "\n".join(lines)


def ensure_ospf_instance(runner, row, *, preview=False):
    OSPFInstance = runner._optional_model(
        "netbox_routing", "OSPFInstance", "netbox_routing.ospfinstance"
    )
    device = lookup_device_for_routing(
        runner, row, "netbox_routing.ospfinstance", "OSPF instance"
    )
    vrf = routing_vrf(runner, row, preview=preview)
    if vrf is VRF_ABSENT:
        # Coalesced on ("device", "vrf", "process_id"); resolving on `vrf=None`
        # would match the device's GLOBAL instance instead. See `routing_vrf`.
        return None
    process_id, process_label = ospf_process_values(row)
    router_id = str(row.get("router_id") or "").strip()
    if not router_id:
        raise ForwardQueryError(
            "OSPF instance row did not include `router_id`.",
            model_string="netbox_routing.ospfinstance",
            context={"device": row.get("device"), "process_id": process_label},
            data=row,
        )
    values = runner._model_field_values(
        OSPFInstance,
        {
            "name": (row.get("name") or f"{device.name} OSPF {process_label}")[:100],
            "router_id": router_id,
            "process_id": process_id,
            "device": device,
            "vrf": vrf,
            "comments": row.get("comments")
            or ospf_instance_comments(row, process_label),
        },
    )
    instance, _ = runner._upsert_values_from_defaults(
        "netbox_routing.ospfinstance",
        OSPFInstance,
        values=values,
        coalesce_sets=[("device", "vrf", "process_id")],
    )
    return instance


def ensure_ospf_area(runner, row):
    OSPFArea = runner._optional_model(
        "netbox_routing", "OSPFArea", "netbox_routing.ospfarea"
    )
    values = runner._model_field_values(
        OSPFArea,
        {
            "area_id": str(row.get("area_id")),
            "area_type": ospf_area_type(row.get("area_type")),
            "description": "Observed by Forward from structured OSPF state.",
        },
    )
    area, _ = runner._upsert_values_from_defaults(
        "netbox_routing.ospfarea",
        OSPFArea,
        values=values,
        coalesce_sets=[("area_id",)],
    )
    return area


def ensure_ospf_interface(runner, row, *, preview=False):
    OSPFInterface = runner._optional_model(
        "netbox_routing", "OSPFInterface", "netbox_routing.ospfinterface"
    )
    device = lookup_device_for_routing(
        runner, row, "netbox_routing.ospfinterface", "OSPF interface"
    )
    interface = lookup_routing_interface_name(
        runner, device, row.get("local_interface")
    )
    if interface is None:
        runner._record_aggregated_skip_warning(
            model_string="netbox_routing.ospfinterface",
            reason="missing-interface",
            warning_message=(
                f"Skipping OSPF interface row on `{device.name}` because "
                f"local interface `{row.get('local_interface')}` was not imported."
            ),
        )
        return False
    instance = ensure_ospf_instance(runner, row, preview=preview)
    area = ensure_ospf_area(runner, row)
    values = runner._model_field_values(
        OSPFInterface,
        {
            "instance": instance,
            "area": area,
            "interface": interface,
            "priority": None,
            "comments": ospf_interface_comments(row),
        },
    )
    ospf_interface, _ = runner._upsert_values_from_defaults(
        "netbox_routing.ospfinterface",
        OSPFInterface,
        values=values,
        coalesce_sets=[("interface",)],
    )
    return ospf_interface


ROUTING_INTERFACE_PREFIX_ALIASES = (
    ("gigabitethernet", "GigabitEthernet"),
    ("gi", "GigabitEthernet"),
    ("tengigabitethernet", "TenGigabitEthernet"),
    ("te", "TenGigabitEthernet"),
    ("fastethernet", "FastEthernet"),
    ("fa", "FastEthernet"),
    ("hundredgige", "HundredGigE"),
    ("hu", "HundredGigE"),
    ("ethernet", "Ethernet"),
    ("eth", "Ethernet"),
    ("port-channel", "Port-channel"),
    ("portchannel", "Port-channel"),
    ("po", "Port-channel"),
    ("loopback", "Loopback"),
    ("lo", "Loopback"),
)


def routing_interface_lookup_candidates(interface_name):
    raw = str(interface_name or "").strip()
    if not raw:
        return []
    candidates = [raw]
    lowered = raw.lower()
    for alias, canonical in ROUTING_INTERFACE_PREFIX_ALIASES:
        if not lowered.startswith(alias):
            continue
        remainder = raw[len(alias) :]
        candidate = f"{canonical}{remainder}"
        if candidate not in candidates:
            candidates.append(candidate)
    return candidates


def lookup_routing_interface_name(runner, device, interface_name):
    from dcim.models import Interface

    if device is None:
        return None
    candidates = routing_interface_lookup_candidates(interface_name)
    for candidate in candidates:
        interface = runner._lookup_interface(device, candidate)
        if interface is not None:
            return interface
    for candidate in candidates:
        interface = (
            Interface.objects.filter(device=device, name__iexact=candidate)
            .order_by("pk")
            .first()
        )
        if interface is None:
            continue
        remember_lookup_object(runner, interface)
        return interface
    return None


def ensure_peering_relationship(runner, row):
    relationship_slug = row.get("relationship_slug") or ""
    relationship_name = row.get("relationship") or ""
    if not relationship_slug or not relationship_name:
        peer_type = str(row.get("peer_type") or "").upper()
        if "EXTERNAL" in peer_type:
            relationship_name = "External BGP"
            relationship_slug = "external-bgp"
        elif "INTERNAL" in peer_type:
            relationship_name = "Internal BGP"
            relationship_slug = "internal-bgp"
    if not relationship_slug or not relationship_name:
        return None
    Relationship = runner._optional_model(
        "netbox_peering_manager",
        "Relationship",
        "netbox_peering_manager.peeringsession",
    )
    relationship, _ = runner._upsert_values_from_defaults(
        "netbox_peering_manager.peeringsession",
        Relationship,
        values={
            "name": relationship_name,
            "slug": relationship_slug,
        },
        coalesce_sets=[("slug",), ("name",)],
    )
    return relationship


def resolve_bgp_peer_for_delete(runner, row):
    from dcim.models import Device
    from ipam.models import ASN
    from ipam.models import VRF

    BGPRouter = runner._optional_model(
        "netbox_routing", "BGPRouter", "netbox_routing.bgppeer"
    )
    BGPScope = runner._optional_model(
        "netbox_routing", "BGPScope", "netbox_routing.bgppeer"
    )
    BGPPeer = runner._optional_model(
        "netbox_routing", "BGPPeer", "netbox_routing.bgppeer"
    )

    device = runner._lookup_device_by_name(row.get("device"))
    if device is None:
        return None
    local_asn = runner._get_unique_or_raise(ASN, {"asn": row.get("local_asn")})
    if local_asn is None:
        return None
    vrf = None
    if row.get("vrf"):
        vrf = runner._get_unique_or_raise(VRF, {"name": row["vrf"]})
        if vrf is None:
            return None
    router = runner._get_unique_or_raise(
        BGPRouter,
        {
            "assigned_object_type": runner._content_type_for(Device),
            "assigned_object_id": device.pk,
            "asn": local_asn,
        },
    )
    if router is None:
        return None
    scope = runner._get_unique_or_raise(BGPScope, {"router": router, "vrf": vrf})
    if scope is None:
        return None
    peer_ip = runner._lookup_ipaddress_by_host(
        address=row.get("neighbor_address"), vrf=vrf
    )
    if peer_ip is None:
        return None
    return runner._get_unique_or_raise(
        BGPPeer,
        {
            "scope": scope,
            "peer": peer_ip,
        },
    )


def resolve_bgp_scope_for_delete(runner, row):
    from dcim.models import Device
    from ipam.models import ASN
    from ipam.models import VRF

    BGPRouter = runner._optional_model(
        "netbox_routing", "BGPRouter", "netbox_routing.bgppeer"
    )
    BGPScope = runner._optional_model(
        "netbox_routing", "BGPScope", "netbox_routing.bgppeer"
    )
    device = runner._lookup_device_by_name(row.get("device"))
    if device is None:
        return None
    local_asn = runner._get_unique_or_raise(ASN, {"asn": row.get("local_asn")})
    if local_asn is None:
        return None
    vrf = None
    if row.get("vrf"):
        vrf = runner._get_unique_or_raise(VRF, {"name": row["vrf"]})
        if vrf is None:
            return None
    router = runner._get_unique_or_raise(
        BGPRouter,
        {
            "assigned_object_type": runner._content_type_for(Device),
            "assigned_object_id": device.pk,
            "asn": local_asn,
        },
    )
    if router is None:
        return None
    return runner._get_unique_or_raise(BGPScope, {"router": router, "vrf": vrf})
