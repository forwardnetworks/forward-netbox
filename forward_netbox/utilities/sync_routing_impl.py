import hashlib

from django.core.exceptions import ObjectDoesNotExist

from ..exceptions import ForwardDependencySkipError
from ..exceptions import ForwardQueryError
from ..exceptions import ForwardSearchError


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
    peer.delete()
    return True


def delete_netbox_routing_bgpaddressfamily(runner, row):
    address_family = runner._resolve_bgp_address_family_for_delete(row)
    if address_family is None:
        return False
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


def delete_netbox_routing_ospfinstance(runner, row):
    from dcim.models import Device
    from ipam.models import VRF

    OSPFInstance = runner._optional_model(
        "netbox_routing", "OSPFInstance", "netbox_routing.ospfinstance"
    )
    device = Device.objects.filter(name=row.get("device")).order_by("pk").first()
    if device is None:
        return False
    vrf = None
    if row.get("vrf"):
        vrf = VRF.objects.filter(name=row["vrf"]).order_by("pk").first()
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
    from dcim.models import Device

    OSPFInterface = runner._optional_model(
        "netbox_routing", "OSPFInterface", "netbox_routing.ospfinterface"
    )
    device = Device.objects.filter(name=row.get("device")).order_by("pk").first()
    if device is None:
        return False
    interface = runner._lookup_interface(device, row.get("local_interface"))
    if interface is None:
        return False
    return runner._delete_by_coalesce(OSPFInterface, [{"interface": interface}])


def apply_netbox_routing_bgppeer(runner, row):
    return runner._ensure_netbox_routing_bgppeer(row)


def apply_netbox_routing_bgpaddressfamily(runner, row):
    return runner._ensure_bgp_address_family(row)


def apply_netbox_routing_bgppeeraddressfamily(runner, row):
    return runner._ensure_bgp_peer_address_family(row)


def apply_netbox_routing_ospfinstance(runner, row):
    return runner._ensure_ospf_instance(row)


def apply_netbox_routing_ospfarea(runner, row):
    return runner._ensure_ospf_area(row)


def apply_netbox_routing_ospfinterface(runner, row):
    return runner._ensure_ospf_interface(row)


def apply_netbox_peering_manager_peeringsession(runner, row):
    PeeringSession = runner._optional_model(
        "netbox_peering_manager",
        "PeeringSession",
        "netbox_peering_manager.peeringsession",
    )
    bgp_peer = runner._ensure_netbox_routing_bgppeer(row)
    values = runner._model_field_values(
        PeeringSession,
        {
            "bgp_peer": bgp_peer,
            "relationship": runner._ensure_peering_relationship(row),
            "service_reference": row.get("service_reference") or "",
        },
    )
    runner._upsert_values_from_defaults(
        "netbox_peering_manager.peeringsession",
        PeeringSession,
        values=values,
        coalesce_sets=[("bgp_peer",)],
    )


def bgp_vrf(runner, row):
    return routing_vrf(runner, row)


def routing_vrf(runner, row):
    if not row.get("vrf"):
        return None
    return runner._ensure_vrf(
        {
            "name": row["vrf"],
            "rd": None,
            "description": "",
            "enforce_unique": False,
        }
    )


def lookup_device_for_routing(runner, row, model_string, object_label):
    from dcim.models import Device

    try:
        return Device.objects.get(name=row["device"])
    except ObjectDoesNotExist as exc:
        key = (row["device"],)
        if runner._dependency_failed("dcim.device", key):
            raise ForwardDependencySkipError(
                f"Skipping {object_label} because dependency `dcim.device` failed for {key}.",
                model_string=model_string,
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


def ensure_bgp_peer_ip(runner, row, vrf):
    from ipam.models import IPAddress

    neighbor_address = row["neighbor_address"]
    existing = lookup_ipaddress_by_host(runner, address=neighbor_address, vrf=vrf)
    if existing is not None:
        return existing
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
    scope = BGPScope.objects.filter(router=router, vrf=vrf).order_by("pk").first()
    if scope is None:
        scope = BGPScope(**values)
        scope.full_clean()
        scope.save()
        return scope

    duplicate_count = BGPScope.objects.filter(router=router, vrf=vrf).count() - 1
    if duplicate_count > 0:
        runner._record_aggregated_skip_warning(
            model_string="netbox_routing.bgppeer",
            reason="duplicate-bgp-scope",
            warning_message=(
                "Reusing the oldest duplicate BGP scope for router "
                f"`{router}` and VRF `{vrf or 'global'}`; "
                f"{duplicate_count} duplicate scope(s) already exist."
            ),
        )
    return scope


def bgp_peer_name(row):
    name = row.get("name") or f"AS{row['peer_asn']} {row['neighbor_address']}"
    return str(name)[:100]


def bgp_peer_comments(row):
    lines = ["Observed by Forward from structured BGP neighbor state."]
    for label, key in (
        ("Router ID", "router_id"),
        ("Peer type", "peer_type"),
        ("Peer device", "peer_device"),
        ("Peer VRF", "peer_vrf"),
        ("Peer router ID", "peer_router_id"),
        ("Session state", "session_state"),
        ("Advertised prefixes", "advertised_prefixes"),
        ("Received prefixes", "received_prefixes"),
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


def bgp_peer_values(runner, row):
    device = lookup_device_for_routing(
        runner, row, "netbox_routing.bgppeer", "BGP peer"
    )

    vrf = bgp_vrf(runner, row)
    local_asn = runner._ensure_asn(row["local_asn"])
    remote_asn = runner._ensure_asn(row["peer_asn"])
    peer_ip = ensure_bgp_peer_ip(runner, row, vrf)
    router = ensure_bgp_router(runner, row, device, local_asn)
    scope = ensure_bgp_scope(runner, row, router, vrf)
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


def ensure_netbox_routing_bgppeer(runner, row):
    BGPPeer = runner._optional_model(
        "netbox_routing", "BGPPeer", "netbox_routing.bgppeer"
    )
    values = runner._model_field_values(BGPPeer, bgp_peer_values(runner, row))
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


def ensure_bgp_scope_for_row(runner, row, model_string):
    device = lookup_device_for_routing(runner, row, model_string, "BGP scope")
    vrf = bgp_vrf(runner, row)
    local_asn = runner._ensure_asn(row["local_asn"])
    router = ensure_bgp_router(runner, row, device, local_asn)
    return ensure_bgp_scope(runner, row, router, vrf)


def ensure_bgp_address_family(runner, row):
    BGPAddressFamily = runner._optional_model(
        "netbox_routing",
        "BGPAddressFamily",
        "netbox_routing.bgpaddressfamily",
    )
    scope = ensure_bgp_scope_for_row(runner, row, "netbox_routing.bgpaddressfamily")
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


def ensure_bgp_peer_address_family(runner, row):
    BGPPeerAddressFamily = runner._optional_model(
        "netbox_routing",
        "BGPPeerAddressFamily",
        "netbox_routing.bgppeeraddressfamily",
    )
    bgp_peer = ensure_netbox_routing_bgppeer(runner, row)
    address_family = ensure_bgp_address_family(runner, row)
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


def ensure_ospf_instance(runner, row):
    OSPFInstance = runner._optional_model(
        "netbox_routing", "OSPFInstance", "netbox_routing.ospfinstance"
    )
    device = lookup_device_for_routing(
        runner, row, "netbox_routing.ospfinstance", "OSPF instance"
    )
    vrf = routing_vrf(runner, row)
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


def ensure_ospf_interface(runner, row):
    OSPFInterface = runner._optional_model(
        "netbox_routing", "OSPFInterface", "netbox_routing.ospfinterface"
    )
    device = lookup_device_for_routing(
        runner, row, "netbox_routing.ospfinterface", "OSPF interface"
    )
    interface = runner._lookup_interface(device, row.get("local_interface"))
    if interface is None:
        raise ForwardSearchError(
            f"Unable to find interface `{row.get('local_interface')}` on `{device.name}` for OSPF interface.",
            model_string="netbox_routing.ospfinterface",
            context={
                "device": device.name,
                "local_interface": row.get("local_interface"),
            },
            data=row,
        )
    instance = ensure_ospf_instance(runner, row)
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

    device = Device.objects.filter(name=row.get("device")).order_by("pk").first()
    if device is None:
        return None
    local_asn = ASN.objects.filter(asn=row.get("local_asn")).order_by("pk").first()
    if local_asn is None:
        return None
    vrf = None
    if row.get("vrf"):
        vrf = VRF.objects.filter(name=row["vrf"]).order_by("pk").first()
        if vrf is None:
            return None
    router = BGPRouter.objects.filter(
        assigned_object_type=runner._content_type_for(Device),
        assigned_object_id=device.pk,
        asn=local_asn,
    ).first()
    if router is None:
        return None
    scope = BGPScope.objects.filter(router=router, vrf=vrf).first()
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
    device = Device.objects.filter(name=row.get("device")).order_by("pk").first()
    if device is None:
        return None
    local_asn = ASN.objects.filter(asn=row.get("local_asn")).order_by("pk").first()
    if local_asn is None:
        return None
    vrf = None
    if row.get("vrf"):
        vrf = VRF.objects.filter(name=row["vrf"]).order_by("pk").first()
        if vrf is None:
            return None
    router = BGPRouter.objects.filter(
        assigned_object_type=runner._content_type_for(Device),
        assigned_object_id=device.pk,
        asn=local_asn,
    ).first()
    if router is None:
        return None
    return BGPScope.objects.filter(router=router, vrf=vrf).first()
