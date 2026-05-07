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
