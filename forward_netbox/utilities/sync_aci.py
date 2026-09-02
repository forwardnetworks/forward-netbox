from django.core.exceptions import ValidationError

from ..exceptions import ForwardQueryError


ACI_APP_LABEL = "netbox_cisco_aci"
ACI_FABRIC_DESCRIPTION = "Forward observed ACI fabric"
ACI_POD_DESCRIPTION = "Forward observed ACI pod"
ACI_TENANT_DESCRIPTION = "Forward observed ACI tenant"
ACI_VRF_DESCRIPTION = "Forward observed ACI VRF"


def _aci_model(runner, model_name, model_string):
    return runner._optional_model(ACI_APP_LABEL, model_name, model_string)


def _aci_model_values(runner, model, values):
    return runner._model_field_values(model, values)


def _coerce_int(value, field_name):
    try:
        return int(value)
    except (TypeError, ValueError) as exc:
        raise ForwardQueryError(f"Invalid ACI `{field_name}` value `{value}`.") from exc


def _coerce_bool(value, default=False):
    if value in ("", None):
        return default
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "on", "enabled"}


def _ensure_aci_fabric(runner, row):
    ACIFabric = _aci_model(
        runner,
        "ACIFabric",
        "netbox_cisco_aci.acifabric",
    )
    values = _aci_model_values(
        runner,
        ACIFabric,
        {
            "name": row["name"],
            "fabric_id": int(row.get("fabric_id") or 1),
            "description": row.get("description") or ACI_FABRIC_DESCRIPTION,
        },
    )
    fabric, _ = runner._upsert_values_from_defaults(
        "netbox_cisco_aci.acifabric",
        ACIFabric,
        values=values,
        coalesce_sets=runner._coalesce_sets_for(
            "netbox_cisco_aci.acifabric",
            [("name",)],
        ),
    )
    return fabric


def _ensure_aci_tenant(runner, row):
    ACITenant = _aci_model(runner, "ACITenant", "netbox_cisco_aci.acitenant")
    fabric = _ensure_aci_fabric(
        runner,
        {
            "name": row["fabric_name"],
            "fabric_id": row.get("fabric_id") or 1,
        },
    )
    if _parent_absent(fabric):
        return None
    values = _aci_model_values(
        runner,
        ACITenant,
        {
            "aci_fabric": fabric,
            "name": row["name"],
            "description": row.get("description") or ACI_TENANT_DESCRIPTION,
        },
    )
    tenant, _ = runner._upsert_values_from_defaults(
        "netbox_cisco_aci.acitenant",
        ACITenant,
        values=values,
        coalesce_sets=[("aci_fabric", "name")],
    )
    return tenant


def _ensure_aci_vrf(runner, row):
    ACIVRF = _aci_model(runner, "ACIVRF", "netbox_cisco_aci.acivrf")
    tenant = _ensure_aci_tenant(
        runner,
        {
            "fabric_name": row["fabric_name"],
            "name": row["tenant_name"],
        },
    )
    if _parent_absent(tenant):
        return None
    values = _aci_model_values(
        runner,
        ACIVRF,
        {
            "aci_tenant": tenant,
            "name": row["name"],
            "policy_enforcement_preference": row.get("policy_enforcement_preference")
            or "enforced",
            "policy_enforcement_direction": row.get("policy_enforcement_direction")
            or "ingress",
            "bd_enforcement_enabled": _coerce_bool(
                row.get("bd_enforcement_enabled"),
                False,
            ),
            "preferred_group_enabled": _coerce_bool(
                row.get("preferred_group_enabled"),
                False,
            ),
            "description": row.get("description") or ACI_VRF_DESCRIPTION,
        },
    )
    vrf, _ = runner._upsert_values_from_defaults(
        "netbox_cisco_aci.acivrf",
        ACIVRF,
        values=values,
        coalesce_sets=[("aci_tenant", "name")],
    )
    return vrf


def _ensure_aci_bridge_domain(runner, row):
    ACIBridgeDomain = _aci_model(
        runner,
        "ACIBridgeDomain",
        "netbox_cisco_aci.acibridgedomain",
    )
    tenant = _ensure_aci_tenant(
        runner,
        {"fabric_name": row["fabric_name"], "name": row["tenant_name"]},
    )
    vrf = _ensure_aci_vrf(
        runner,
        {
            "fabric_name": row["fabric_name"],
            "tenant_name": row.get("vrf_tenant_name") or row["tenant_name"],
            "name": row["vrf_name"],
        },
    )
    if _parent_absent(tenant, vrf):
        return None
    values = _aci_model_values(
        runner,
        ACIBridgeDomain,
        {
            "aci_tenant": tenant,
            "aci_vrf": vrf,
            "name": row["name"],
            "unicast_routing_enabled": _coerce_bool(
                row.get("unicast_routing_enabled"),
                True,
            ),
            "arp_flooding_enabled": _coerce_bool(
                row.get("arp_flooding_enabled"),
                False,
            ),
            "limit_ip_learn_to_subnets": _coerce_bool(
                row.get("limit_ip_learn_to_subnets"),
                True,
            ),
            "l2_unknown_unicast": row.get("l2_unknown_unicast") or "proxy",
            "l3_unknown_multicast": row.get("l3_unknown_multicast") or "flood",
            "multi_destination_flooding": row.get("multi_destination_flooding")
            or "bd-flood",
            "mac_address": row.get("mac_address") or "",
            "description": row.get("description") or "",
        },
    )
    bd, _ = runner._upsert_values_from_defaults(
        "netbox_cisco_aci.acibridgedomain",
        ACIBridgeDomain,
        values=values,
        coalesce_sets=[("aci_tenant", "name")],
    )
    return bd


def _ensure_aci_filter(runner, row):
    ACIFilter = _aci_model(runner, "ACIFilter", "netbox_cisco_aci.acifilter")
    tenant = _ensure_aci_tenant(
        runner,
        {"fabric_name": row["fabric_name"], "name": row["tenant_name"]},
    )
    if _parent_absent(tenant):
        return None
    values = _aci_model_values(
        runner,
        ACIFilter,
        {
            "aci_tenant": tenant,
            "name": row["name"],
            "description": row.get("description") or "",
        },
    )
    aci_filter, _ = runner._upsert_values_from_defaults(
        "netbox_cisco_aci.acifilter",
        ACIFilter,
        values=values,
        coalesce_sets=[("aci_tenant", "name")],
    )
    return aci_filter


def _ensure_aci_l3out(runner, row):
    ACIL3Out = _aci_model(runner, "ACIL3Out", "netbox_cisco_aci.acil3out")
    tenant = _ensure_aci_tenant(
        runner,
        {"fabric_name": row["fabric_name"], "name": row["tenant_name"]},
    )
    vrf = _ensure_aci_vrf(
        runner,
        {
            "fabric_name": row["fabric_name"],
            "tenant_name": row.get("vrf_tenant_name") or row["tenant_name"],
            "name": row["vrf_name"],
        },
    )
    if _parent_absent(tenant, vrf):
        return None
    values = _aci_model_values(
        runner,
        ACIL3Out,
        {
            "aci_tenant": tenant,
            "aci_vrf": vrf,
            "name": row["name"],
            "protocol_bgp": _coerce_bool(row.get("protocol_bgp"), False),
            "protocol_ospf": _coerce_bool(row.get("protocol_ospf"), False),
            "protocol_eigrp": _coerce_bool(row.get("protocol_eigrp"), False),
            "protocol_static": _coerce_bool(row.get("protocol_static"), True),
            "target_dscp": row.get("target_dscp") or "",
            "description": row.get("description") or "",
        },
    )
    l3out, _ = runner._upsert_values_from_defaults(
        "netbox_cisco_aci.acil3out",
        ACIL3Out,
        values=values,
        coalesce_sets=[("aci_tenant", "name")],
    )
    return l3out


def _ensure_aci_pod(runner, row):
    ACIPod = _aci_model(runner, "ACIPod", "netbox_cisco_aci.acipod")
    fabric = _ensure_aci_fabric(
        runner,
        {
            "name": row["fabric_name"],
            "fabric_id": row.get("fabric_id") or 1,
        },
    )
    if _parent_absent(fabric):
        return None
    values = _aci_model_values(
        runner,
        ACIPod,
        {
            "aci_fabric": fabric,
            "name": row.get("name") or f"pod-{row['pod_id']}",
            "pod_id": int(row["pod_id"]),
            "description": row.get("description") or ACI_POD_DESCRIPTION,
        },
    )
    pod, _ = runner._upsert_values_from_defaults(
        "netbox_cisco_aci.acipod",
        ACIPod,
        values=values,
        coalesce_sets=[("aci_fabric", "pod_id"), ("aci_fabric", "name")],
    )
    return pod


def _resolve_aci_fabric(runner, fabric_name):
    ACIFabric = _aci_model(
        runner,
        "ACIFabric",
        "netbox_cisco_aci.acifabric",
    )
    if not fabric_name:
        return None
    return runner._get_unique_or_raise(ACIFabric, {"name": fabric_name})


def _resolve_aci_tenant(runner, row):
    ACITenant = _aci_model(runner, "ACITenant", "netbox_cisco_aci.acitenant")
    fabric = _resolve_aci_fabric(runner, row.get("fabric_name"))
    if fabric is None or not row.get("tenant_name"):
        return None
    return runner._get_unique_or_raise(
        ACITenant,
        {"aci_fabric": fabric, "name": row["tenant_name"]},
    )


def _resolve_aci_vrf(runner, row):
    ACIVRF = _aci_model(runner, "ACIVRF", "netbox_cisco_aci.acivrf")
    tenant = _resolve_aci_tenant(runner, row)
    if tenant is None or not row.get("vrf_name"):
        return None
    return runner._get_unique_or_raise(
        ACIVRF,
        {"aci_tenant": tenant, "name": row["vrf_name"]},
    )


def _resolve_aci_bridge_domain(runner, row):
    ACIBridgeDomain = _aci_model(
        runner,
        "ACIBridgeDomain",
        "netbox_cisco_aci.acibridgedomain",
    )
    tenant = _resolve_aci_tenant(runner, row)
    if tenant is None or not row.get("bridge_domain_name"):
        return None
    return runner._get_unique_or_raise(
        ACIBridgeDomain,
        {"aci_tenant": tenant, "name": row["bridge_domain_name"]},
    )


def _resolve_aci_pod(runner, row):
    ACIPod = _aci_model(runner, "ACIPod", "netbox_cisco_aci.acipod")
    fabric = _resolve_aci_fabric(runner, row.get("fabric_name"))
    if fabric is None:
        return None
    try:
        pod_id = int(row["pod_id"])
    except (TypeError, ValueError):
        return None
    return runner._get_unique_or_raise(
        ACIPod,
        {"aci_fabric": fabric, "pod_id": pod_id},
    )


def _preview_verdict(runner, obj):
    """Classify one ACI row from its OWN upsert - the leaf rule.

    Every ACI model has its own Forward query and its own row set, so a parent
    a row would create is drift reported under the PARENT's model, and folding
    it into the child's verdict would count one object twice. That is the OSPF
    rule, not the peering one; see `preview_leaf_outcome` for the distinction.
    """
    from .sync_routing_impl import preview_leaf_outcome

    return preview_leaf_outcome(runner, obj)


# Under a preview the firewalled upsert returns `None` for a parent it would
# CREATE, and `coalesce_lookup` drops `None` values. Left unguarded, a child
# under such a parent would be looked up by its remaining keys - `name` alone -
# and could match a sibling under a DIFFERENT parent, reading `unchanged` for
# a row the apply would create. That is the absent-VRF defect slice seven found
# in the routing chain, with the same confident-zero consequence. A parent the
# apply would create means the child cannot exist yet, so the child is a create
# and nothing below it needs resolving. The real apply never returns `None`
# from an ensure, so these guards are inert outside a preview.
def _parent_absent(*parents):
    return any(parent is None for parent in parents)


def _node_role(value):
    role = str(value or "").strip().lower()
    if role in {"spine", "leaf", "apic", "rleaf", "vleaf", "tier2"}:
        return role
    return "leaf"


def _node_type(value):
    node_type = str(value or "").strip().lower()
    if node_type in {"physical", "virtual", "remote", "unknown"}:
        return node_type
    return "physical"


def _lookup_aci_node_device(runner, row):
    device_name = row.get("node_object_name") or row.get("name")
    if not device_name:
        return None, None
    device = runner._lookup_device_by_name(device_name)
    if device is None:
        return None, None
    return runner._content_type_for(device.__class__), device.pk


def _aci_node_seen_keys(runner):
    seen_keys = getattr(runner, "_forward_aci_node_seen_keys", None)
    if seen_keys is None:
        seen_keys = set()
        setattr(runner, "_forward_aci_node_seen_keys", seen_keys)
    return seen_keys


def _aci_node_keys(pod, node_id, name):
    pod_pk = getattr(pod, "pk", pod)
    return (
        ("node_id", pod_pk, node_id),
        ("name", pod_pk, name),
    )


def _resolve_existing_aci_node(runner, model, pod, node_id, name):
    existing = runner._get_unique_or_raise(
        model,
        {"aci_pod": pod, "node_id": node_id},
    )
    if existing is not None:
        return existing
    return runner._get_unique_or_raise(
        model,
        {"aci_pod": pod, "name": name},
    )


def apply_netbox_cisco_aci_acifabric(runner, row, *, preview=False):
    fabric = _ensure_aci_fabric(runner, row)
    return _preview_verdict(runner, fabric) if preview else fabric


def apply_netbox_cisco_aci_acitenant(runner, row, *, preview=False):
    tenant = _ensure_aci_tenant(runner, row)
    return _preview_verdict(runner, tenant) if preview else tenant


def apply_netbox_cisco_aci_acivrf(runner, row, *, preview=False):
    vrf = _ensure_aci_vrf(runner, row)
    return _preview_verdict(runner, vrf) if preview else vrf


def apply_netbox_cisco_aci_acibridgedomain(runner, row, *, preview=False):
    bd = _ensure_aci_bridge_domain(runner, row)
    return _preview_verdict(runner, bd) if preview else bd


def apply_netbox_cisco_aci_acifilter(runner, row, *, preview=False):
    aci_filter = _ensure_aci_filter(runner, row)
    return _preview_verdict(runner, aci_filter) if preview else aci_filter


def apply_netbox_cisco_aci_acil3out(runner, row, *, preview=False):
    l3out = _ensure_aci_l3out(runner, row)
    return _preview_verdict(runner, l3out) if preview else l3out


def apply_netbox_cisco_aci_acipod(runner, row, *, preview=False):
    pod = _ensure_aci_pod(runner, row)
    return _preview_verdict(runner, pod) if preview else pod


def apply_netbox_cisco_aci_acinode(runner, row, *, preview=False):
    ACINode = _aci_model(runner, "ACINode", "netbox_cisco_aci.acinode")
    pod = _ensure_aci_pod(
        runner,
        {
            "fabric_name": row["fabric_name"],
            "name": row.get("pod_name") or f"pod-{row['pod_id']}",
            "pod_id": row["pod_id"],
        },
    )
    if _parent_absent(pod):
        # Preview only: the pod would be created, so the node cannot exist.
        # Reported under this model as a create; the pod's own create is the
        # pod model's to report.
        return "creates" if preview else None
    node_id = int(row["node_id"])
    name = row["name"]
    node_seen_keys = _aci_node_seen_keys(runner)
    node_key, name_key = _aci_node_keys(pod, node_id, name)
    if node_key in node_seen_keys or name_key in node_seen_keys:
        existing_node = _resolve_existing_aci_node(runner, ACINode, pod, node_id, name)
        if existing_node is not None:
            # A second observation of the same node in one run. The apply
            # writes nothing for it, so a preview reports nothing for it.
            return "unchanged" if preview else existing_node

    node_object_type, node_object_id = _lookup_aci_node_device(runner, row)
    values = _aci_model_values(
        runner,
        ACINode,
        {
            "aci_pod": pod,
            "node_id": node_id,
            "name": name,
            "role": _node_role(row.get("role")),
            "node_type": _node_type(row.get("node_type")),
            "serial_number": row.get("serial_number") or "",
            "pod_tep_pool": row.get("pod_tep_pool") or "",
            "firmware_version": row.get("firmware_version") or "",
            "node_object_type": node_object_type,
            "node_object_id": node_object_id,
            "description": row.get("description") or "",
        },
    )
    node, _ = runner._upsert_values_from_defaults(
        "netbox_cisco_aci.acinode",
        ACINode,
        values=values,
        coalesce_sets=[("aci_pod", "node_id"), ("aci_pod", "name")],
    )
    node_seen_keys.update((node_key, name_key))
    return _preview_verdict(runner, node) if preview else node


def delete_netbox_cisco_aci_acifabric(runner, row):
    ACIFabric = _aci_model(
        runner,
        "ACIFabric",
        "netbox_cisco_aci.acifabric",
    )
    return runner._delete_by_coalesce(ACIFabric, [{"name": row.get("name")}])


def delete_netbox_cisco_aci_acitenant(runner, row):
    ACITenant = _aci_model(runner, "ACITenant", "netbox_cisco_aci.acitenant")
    fabric = _resolve_aci_fabric(runner, row.get("fabric_name"))
    if fabric is None:
        return False
    return runner._delete_by_coalesce(
        ACITenant,
        [{"aci_fabric": fabric, "name": row.get("name")}],
    )


def delete_netbox_cisco_aci_acivrf(runner, row):
    ACIVRF = _aci_model(runner, "ACIVRF", "netbox_cisco_aci.acivrf")
    tenant = _resolve_aci_tenant(runner, row)
    if tenant is None:
        return False
    return runner._delete_by_coalesce(
        ACIVRF,
        [{"aci_tenant": tenant, "name": row.get("name")}],
    )


def delete_netbox_cisco_aci_acibridgedomain(runner, row):
    ACIBridgeDomain = _aci_model(
        runner,
        "ACIBridgeDomain",
        "netbox_cisco_aci.acibridgedomain",
    )
    tenant = _resolve_aci_tenant(runner, row)
    if tenant is None:
        return False
    return runner._delete_by_coalesce(
        ACIBridgeDomain,
        [{"aci_tenant": tenant, "name": row.get("name")}],
    )


def delete_netbox_cisco_aci_acifilter(runner, row):
    ACIFilter = _aci_model(runner, "ACIFilter", "netbox_cisco_aci.acifilter")
    tenant = _resolve_aci_tenant(runner, row)
    if tenant is None:
        return False
    return runner._delete_by_coalesce(
        ACIFilter,
        [{"aci_tenant": tenant, "name": row.get("name")}],
    )


def delete_netbox_cisco_aci_acil3out(runner, row):
    ACIL3Out = _aci_model(runner, "ACIL3Out", "netbox_cisco_aci.acil3out")
    tenant = _resolve_aci_tenant(runner, row)
    if tenant is None:
        return False
    return runner._delete_by_coalesce(
        ACIL3Out,
        [{"aci_tenant": tenant, "name": row.get("name")}],
    )


def delete_netbox_cisco_aci_acipod(runner, row):
    ACIPod = _aci_model(runner, "ACIPod", "netbox_cisco_aci.acipod")
    fabric = _resolve_aci_fabric(runner, row.get("fabric_name"))
    if fabric is None:
        return False
    try:
        pod_id = int(row["pod_id"])
    except (TypeError, ValueError):
        return False
    return runner._delete_by_coalesce(
        ACIPod,
        [{"aci_fabric": fabric, "pod_id": pod_id}],
    )


def delete_netbox_cisco_aci_acinode(runner, row):
    ACINode = _aci_model(runner, "ACINode", "netbox_cisco_aci.acinode")
    try:
        pod = _resolve_aci_pod(runner, row)
    except (ForwardQueryError, ValidationError, ValueError):
        return False
    if pod is None:
        return False
    try:
        node_id = int(row["node_id"])
    except (TypeError, ValueError):
        return False
    return runner._delete_by_coalesce(
        ACINode,
        [{"aci_pod": pod, "node_id": node_id}],
    )
