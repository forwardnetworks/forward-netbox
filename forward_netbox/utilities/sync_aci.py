from django.core.exceptions import ValidationError

from ..exceptions import ForwardQueryError
from .sync_reporting import ACI_NODE_DEVICE_AMBIGUOUS_REASON
from .sync_reporting import ACI_NODE_DEVICE_MISSING_REASON


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
    """The NetBox device an ACI node row describes, or ``(None, None)``.

    APIC output names nodes the way the fabric was configured
    (``DC01LEAF101``) while Forward - and so NetBox - carries the device name
    as collected (``dc01leaf101``); on a real fabric 0 of 667 nodes matched
    exactly and 614 matched case-insensitively. So the exact lookup is tried
    first and a case-insensitive one second, and only a UNIQUE match links.
    Two devices differing only by case is held, not guessed - a wrong link is
    worse than none. A miss is recorded once per run rather than silently
    leaving the link empty, which is how the unlinked nodes went unnoticed.
    """
    device_name = row.get("node_object_name") or row.get("name")
    if not device_name:
        return None, None
    warn = getattr(runner, "_record_aggregated_skip_warning", None)
    device = runner._lookup_device_by_name(device_name)
    if device is None:
        insensitive = getattr(runner, "_lookup_device_by_name_insensitive", None)
        outcome = insensitive(device_name) if insensitive is not None else None
        if outcome == "ambiguous":
            if warn is not None:
                warn(
                    model_string="netbox_cisco_aci.acinode",
                    reason=ACI_NODE_DEVICE_AMBIGUOUS_REASON,
                    warning_message=(
                        f"ACI node `{device_name}` matches more than one NetBox "
                        "device case-insensitively; the node is stored without a "
                        "device link."
                    ),
                    sample=str(device_name),
                )
            return None, None
        device = outcome
    if device is None:
        if warn is not None:
            warn(
                model_string="netbox_cisco_aci.acinode",
                reason=ACI_NODE_DEVICE_MISSING_REASON,
                warning_message=(
                    f"ACI node `{device_name}` has no NetBox device by that name "
                    "(exact or case-insensitive); the node is stored without a "
                    "device link."
                ),
                sample=str(device_name),
            )
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


# --- application profiles, endpoint groups, contracts, subjects, filter entries
#
# The tenant-policy half of the fabric, from the APIC `moquery` classes a
# real fabric was found to collect (fvAEPg, fvRsBd, vzBrCP, vzSubj, vzEntry).
# Every parent is a separately measured model, so each of these classifies
# from its own upsert (the leaf rule) and a parent the preview would create
# makes the row uncomparable rather than double-counted.

_QOS_CLASSES = {"level1", "level2", "level3", "level4", "level5", "level6", "unspecified"}
_CONTRACT_SCOPES = {"global", "tenant", "context", "application-profile"}
_ETHER_TYPES = {
    "unspecified", "ip", "ipv4", "ipv6", "arp", "fcoe", "mac-security", "mpls-ucast", "trill",
}
_IP_PROTOCOLS = {
    "unspecified", "tcp", "udp", "icmp", "icmpv6", "igmp", "eigrp", "ospfigp", "pim", "l2tp",
}


def _choice(value, allowed, default):
    text = str(value or "").strip().lower()
    return text if text in allowed else default


def _port(value):
    """An APIC port field: a number, a well-known name, or `unspecified`."""
    text = str(value or "").strip().lower()
    if not text or text == "unspecified":
        return None
    named = {"http": 80, "https": 443, "ssh": 22, "dns": 53, "smtp": 25, "pop3": 110, "ftpdata": 20, "rtsp": 554}
    if text in named:
        return named[text]
    try:
        return int(text)
    except ValueError:
        return None


def _dscp(value):
    text = str(value or "").strip()
    return "" if text.lower() == "unspecified" else text


def _ensure_aci_app_profile(runner, row):
    ACIAppProfile = _aci_model(runner, "ACIAppProfile", "netbox_cisco_aci.aciappprofile")
    tenant = _ensure_aci_tenant(
        runner, {"fabric_name": row["fabric_name"], "name": row["tenant_name"]}
    )
    if _parent_absent(tenant):
        return None
    values = _aci_model_values(
        runner,
        ACIAppProfile,
        {
            "aci_tenant": tenant,
            "name": row["name"],
            "description": row.get("description") or "",
        },
    )
    app_profile, _ = runner._upsert_values_from_defaults(
        "netbox_cisco_aci.aciappprofile",
        ACIAppProfile,
        values=values,
        coalesce_sets=[("aci_tenant", "name")],
    )
    return app_profile


def _ensure_aci_endpoint_group(runner, row):
    ACIEndpointGroup = _aci_model(
        runner, "ACIEndpointGroup", "netbox_cisco_aci.aciendpointgroup"
    )
    app_profile = _ensure_aci_app_profile(
        runner,
        {
            "fabric_name": row["fabric_name"],
            "tenant_name": row["tenant_name"],
            "name": row["app_profile_name"],
        },
    )
    if _parent_absent(app_profile):
        return None
    bridge_domain = None
    if row.get("bridge_domain_name"):
        bridge_domain = _resolve_aci_bridge_domain(
            runner,
            {
                "fabric_name": row["fabric_name"],
                "tenant_name": row.get("bridge_domain_tenant_name") or row["tenant_name"],
                "bridge_domain_name": row["bridge_domain_name"],
            },
        )
    values = _aci_model_values(
        runner,
        ACIEndpointGroup,
        {
            "aci_tenant": app_profile.aci_tenant,
            "aci_app_profile": app_profile,
            "aci_bridge_domain": bridge_domain,
            "name": row["name"],
            "admin_shutdown": _coerce_bool(row.get("admin_shutdown"), False),
            "is_useg": _coerce_bool(row.get("is_useg"), False),
            "intra_epg_isolation": _coerce_bool(row.get("intra_epg_isolation"), False),
            "preferred_group_member": _coerce_bool(
                row.get("preferred_group_member"), False
            ),
            "qos_class": _choice(row.get("qos_class"), _QOS_CLASSES, "unspecified"),
            "description": row.get("description") or "",
        },
    )
    epg, _ = runner._upsert_values_from_defaults(
        "netbox_cisco_aci.aciendpointgroup",
        ACIEndpointGroup,
        values=values,
        coalesce_sets=[("aci_app_profile", "name")],
    )
    return epg


def _ensure_aci_contract(runner, row):
    ACIContract = _aci_model(runner, "ACIContract", "netbox_cisco_aci.acicontract")
    tenant = _ensure_aci_tenant(
        runner, {"fabric_name": row["fabric_name"], "name": row["tenant_name"]}
    )
    if _parent_absent(tenant):
        return None
    values = _aci_model_values(
        runner,
        ACIContract,
        {
            "aci_tenant": tenant,
            "name": row["name"],
            "scope": _choice(row.get("scope"), _CONTRACT_SCOPES, "context"),
            "qos_class": _choice(row.get("qos_class"), _QOS_CLASSES, "unspecified"),
            "target_dscp": _dscp(row.get("target_dscp")),
            "description": row.get("description") or "",
        },
    )
    contract, _ = runner._upsert_values_from_defaults(
        "netbox_cisco_aci.acicontract",
        ACIContract,
        values=values,
        coalesce_sets=[("aci_tenant", "name")],
    )
    return contract


def _ensure_aci_subject(runner, row):
    ACISubject = _aci_model(runner, "ACISubject", "netbox_cisco_aci.acisubject")
    contract = _ensure_aci_contract(
        runner,
        {
            "fabric_name": row["fabric_name"],
            "tenant_name": row["tenant_name"],
            "name": row["contract_name"],
        },
    )
    if _parent_absent(contract):
        return None
    values = _aci_model_values(
        runner,
        ACISubject,
        {
            "aci_contract": contract,
            "name": row["name"],
            # vzInTerm/vzOutTerm are not collected; APIC's default is both.
            "apply_both_directions": _coerce_bool(row.get("apply_both_directions"), True),
            "reverse_filter_ports": _coerce_bool(row.get("reverse_filter_ports"), True),
            "qos_class": _choice(row.get("qos_class"), _QOS_CLASSES, "unspecified"),
            "target_dscp": _dscp(row.get("target_dscp")),
            "description": row.get("description") or "",
        },
    )
    subject, _ = runner._upsert_values_from_defaults(
        "netbox_cisco_aci.acisubject",
        ACISubject,
        values=values,
        coalesce_sets=[("aci_contract", "name")],
    )
    return subject


def _ensure_aci_filter_entry(runner, row):
    ACIFilterEntry = _aci_model(
        runner, "ACIFilterEntry", "netbox_cisco_aci.acifilterentry"
    )
    aci_filter = _ensure_aci_filter(
        runner,
        {
            "fabric_name": row["fabric_name"],
            "tenant_name": row["tenant_name"],
            "name": row["filter_name"],
        },
    )
    if _parent_absent(aci_filter):
        return None
    tcp_rules = str(row.get("tcp_rules") or "").strip()
    arp_opcode = str(row.get("arp_opcode") or "").strip().lower()
    values = _aci_model_values(
        runner,
        ACIFilterEntry,
        {
            "aci_filter": aci_filter,
            "name": row["name"],
            "ether_type": _choice(row.get("ether_type"), _ETHER_TYPES, "unspecified"),
            "ip_protocol": _choice(row.get("ip_protocol"), _IP_PROTOCOLS, "unspecified"),
            "source_port_from": _port(row.get("source_port_from")),
            "source_port_to": _port(row.get("source_port_to")),
            "destination_port_from": _port(row.get("destination_port_from")),
            "destination_port_to": _port(row.get("destination_port_to")),
            "tcp_rules": "" if tcp_rules.lower() == "unspecified" else tcp_rules[:64],
            "match_only_fragments": _coerce_bool(row.get("match_only_fragments"), False),
            "arp_opcode": "" if arp_opcode == "unspecified" else arp_opcode[:8],
            "stateful": _coerce_bool(row.get("stateful"), False),
            "description": row.get("description") or "",
        },
    )
    entry, _ = runner._upsert_values_from_defaults(
        "netbox_cisco_aci.acifilterentry",
        ACIFilterEntry,
        values=values,
        coalesce_sets=[("aci_filter", "name")],
    )
    return entry


def apply_netbox_cisco_aci_aciappprofile(runner, row, *, preview=False):
    app_profile = _ensure_aci_app_profile(runner, row)
    return _preview_verdict(runner, app_profile) if preview else app_profile


def apply_netbox_cisco_aci_aciendpointgroup(runner, row, *, preview=False):
    epg = _ensure_aci_endpoint_group(runner, row)
    return _preview_verdict(runner, epg) if preview else epg


def apply_netbox_cisco_aci_acicontract(runner, row, *, preview=False):
    contract = _ensure_aci_contract(runner, row)
    return _preview_verdict(runner, contract) if preview else contract


def apply_netbox_cisco_aci_acisubject(runner, row, *, preview=False):
    subject = _ensure_aci_subject(runner, row)
    return _preview_verdict(runner, subject) if preview else subject


def apply_netbox_cisco_aci_acifilterentry(runner, row, *, preview=False):
    entry = _ensure_aci_filter_entry(runner, row)
    return _preview_verdict(runner, entry) if preview else entry


def _resolve_aci_app_profile(runner, row):
    ACIAppProfile = _aci_model(runner, "ACIAppProfile", "netbox_cisco_aci.aciappprofile")
    tenant = _resolve_aci_tenant(runner, row)
    if tenant is None or not row.get("app_profile_name"):
        return None
    return runner._get_unique_or_raise(
        ACIAppProfile, {"aci_tenant": tenant, "name": row["app_profile_name"]}
    )


def _resolve_aci_contract(runner, row):
    ACIContract = _aci_model(runner, "ACIContract", "netbox_cisco_aci.acicontract")
    tenant = _resolve_aci_tenant(runner, row)
    if tenant is None or not row.get("contract_name"):
        return None
    return runner._get_unique_or_raise(
        ACIContract, {"aci_tenant": tenant, "name": row["contract_name"]}
    )


def _resolve_aci_filter(runner, row):
    ACIFilter = _aci_model(runner, "ACIFilter", "netbox_cisco_aci.acifilter")
    tenant = _resolve_aci_tenant(runner, row)
    if tenant is None or not row.get("filter_name"):
        return None
    return runner._get_unique_or_raise(
        ACIFilter, {"aci_tenant": tenant, "name": row["filter_name"]}
    )


def delete_netbox_cisco_aci_aciappprofile(runner, row):
    ACIAppProfile = _aci_model(runner, "ACIAppProfile", "netbox_cisco_aci.aciappprofile")
    tenant = _resolve_aci_tenant(runner, row)
    if tenant is None:
        return False
    return runner._delete_by_coalesce(
        ACIAppProfile, [{"aci_tenant": tenant, "name": row.get("name")}]
    )


def delete_netbox_cisco_aci_aciendpointgroup(runner, row):
    ACIEndpointGroup = _aci_model(
        runner, "ACIEndpointGroup", "netbox_cisco_aci.aciendpointgroup"
    )
    app_profile = _resolve_aci_app_profile(runner, row)
    if app_profile is None:
        return False
    return runner._delete_by_coalesce(
        ACIEndpointGroup, [{"aci_app_profile": app_profile, "name": row.get("name")}]
    )


def delete_netbox_cisco_aci_acicontract(runner, row):
    ACIContract = _aci_model(runner, "ACIContract", "netbox_cisco_aci.acicontract")
    tenant = _resolve_aci_tenant(runner, row)
    if tenant is None:
        return False
    return runner._delete_by_coalesce(
        ACIContract, [{"aci_tenant": tenant, "name": row.get("name")}]
    )


def delete_netbox_cisco_aci_acisubject(runner, row):
    ACISubject = _aci_model(runner, "ACISubject", "netbox_cisco_aci.acisubject")
    contract = _resolve_aci_contract(runner, row)
    if contract is None:
        return False
    return runner._delete_by_coalesce(
        ACISubject, [{"aci_contract": contract, "name": row.get("name")}]
    )


def delete_netbox_cisco_aci_acifilterentry(runner, row):
    ACIFilterEntry = _aci_model(
        runner, "ACIFilterEntry", "netbox_cisco_aci.acifilterentry"
    )
    aci_filter = _resolve_aci_filter(runner, row)
    if aci_filter is None:
        return False
    return runner._delete_by_coalesce(
        ACIFilterEntry, [{"aci_filter": aci_filter, "name": row.get("name")}]
    )
