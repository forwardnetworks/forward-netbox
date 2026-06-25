from dataclasses import dataclass

from ..exceptions import ForwardQueryError


@dataclass(frozen=True)
class ModelSyncContract:
    required_fields: tuple[str, ...]
    allowed_coalesce_fields: tuple[str, ...]
    default_coalesce_fields: tuple[tuple[str, ...], ...]
    preserve_existing_on_blank_fields: tuple[str, ...] = ()


MODEL_SYNC_CONTRACTS: dict[str, ModelSyncContract] = {
    "dcim.site": ModelSyncContract(
        required_fields=("name", "slug"),
        allowed_coalesce_fields=("name", "slug"),
        default_coalesce_fields=(("slug",), ("name",)),
    ),
    "dcim.manufacturer": ModelSyncContract(
        required_fields=("name", "slug"),
        allowed_coalesce_fields=("name", "slug"),
        default_coalesce_fields=(("slug",), ("name",)),
    ),
    "dcim.devicerole": ModelSyncContract(
        required_fields=("name", "slug", "color"),
        allowed_coalesce_fields=("name", "slug"),
        default_coalesce_fields=(("slug",), ("name",)),
    ),
    "dcim.platform": ModelSyncContract(
        # 2.0: platforms are global (manufacturer is forced None on apply so any
        # vendor's device can attach — see apply_engine_bulk). The query therefore
        # emits only name/slug; manufacturer is intentionally NOT required.
        required_fields=("name", "slug"),
        allowed_coalesce_fields=("name", "slug"),
        default_coalesce_fields=(("slug",), ("name",)),
    ),
    "dcim.devicetype": ModelSyncContract(
        required_fields=("manufacturer", "manufacturer_slug", "model", "slug"),
        allowed_coalesce_fields=("manufacturer_slug", "manufacturer", "slug", "model"),
        default_coalesce_fields=(
            ("manufacturer_slug", "slug"),
            ("manufacturer_slug", "model"),
        ),
    ),
    "dcim.device": ModelSyncContract(
        required_fields=(
            "name",
            "manufacturer",
            "manufacturer_slug",
            "device_type",
            "device_type_slug",
            "site",
            "site_slug",
            "role",
            "role_slug",
            "role_color",
            "status",
        ),
        allowed_coalesce_fields=("name",),
        default_coalesce_fields=(("name",),),
        preserve_existing_on_blank_fields=("serial",),
    ),
    "dcim.virtualchassis": ModelSyncContract(
        required_fields=("device", "vc_name", "vc_domain"),
        allowed_coalesce_fields=("name",),
        default_coalesce_fields=(("name",),),
    ),
    "extras.taggeditem": ModelSyncContract(
        required_fields=("device", "tag", "tag_slug", "tag_color"),
        allowed_coalesce_fields=("device", "tag_slug"),
        default_coalesce_fields=(("device", "tag_slug"),),
    ),
    "dcim.interface": ModelSyncContract(
        required_fields=("device", "name", "type", "enabled"),
        allowed_coalesce_fields=("device", "name"),
        default_coalesce_fields=(("device", "name"),),
        preserve_existing_on_blank_fields=("description", "mtu", "speed"),
    ),
    "dcim.cable": ModelSyncContract(
        required_fields=(
            "device",
            "interface",
            "remote_device",
            "remote_interface",
            "status",
        ),
        allowed_coalesce_fields=(
            "device",
            "interface",
            "remote_device",
            "remote_interface",
        ),
        default_coalesce_fields=(
            ("device", "interface", "remote_device", "remote_interface"),
        ),
    ),
    "dcim.macaddress": ModelSyncContract(
        required_fields=("device", "interface", "mac"),
        allowed_coalesce_fields=("mac_address",),
        default_coalesce_fields=(("mac_address",),),
    ),
    "ipam.vlan": ModelSyncContract(
        required_fields=("vid", "name", "status"),
        allowed_coalesce_fields=("site", "vid"),
        default_coalesce_fields=(("site", "vid"),),
    ),
    "ipam.vrf": ModelSyncContract(
        required_fields=("name", "rd", "description", "enforce_unique"),
        allowed_coalesce_fields=("name", "rd"),
        default_coalesce_fields=(("rd",), ("name",)),
    ),
    "ipam.prefix": ModelSyncContract(
        required_fields=("prefix", "vrf", "status"),
        allowed_coalesce_fields=("prefix", "vrf"),
        default_coalesce_fields=(("prefix", "vrf"),),
    ),
    "ipam.ipaddress": ModelSyncContract(
        required_fields=("device", "interface", "address", "status"),
        allowed_coalesce_fields=("address", "vrf"),
        default_coalesce_fields=(("address", "vrf"), ("address",)),
    ),
    "ipam.fhrpgroup": ModelSyncContract(
        required_fields=(
            "protocol",
            "group_id",
            "name",
            "device",
            "interface",
            "address",
            "status",
        ),
        allowed_coalesce_fields=("protocol", "group_id", "address", "vrf"),
        default_coalesce_fields=(
            ("protocol", "group_id", "address", "vrf"),
            ("protocol", "group_id", "address"),
        ),
        preserve_existing_on_blank_fields=("comments",),
    ),
    "dcim.inventoryitem": ModelSyncContract(
        required_fields=(
            "device",
            "name",
            "part_id",
            "serial",
            "status",
            "discovered",
        ),
        allowed_coalesce_fields=("device", "name", "part_id", "serial"),
        default_coalesce_fields=(
            ("device", "name", "part_id", "serial"),
            ("device", "name", "part_id"),
            ("device", "name"),
        ),
        preserve_existing_on_blank_fields=(
            "asset_tag",
            "description",
            "label",
            "part_id",
            "serial",
        ),
    ),
    "dcim.module": ModelSyncContract(
        required_fields=(
            "device",
            "module_bay",
            "manufacturer",
            "manufacturer_slug",
            "model",
            "part_number",
            "status",
        ),
        allowed_coalesce_fields=("device", "module_bay"),
        default_coalesce_fields=(("device", "module_bay"),),
        preserve_existing_on_blank_fields=("asset_tag", "serial"),
    ),
    "netbox_routing.bgprouter": ModelSyncContract(
        required_fields=(
            "name",
            "assigned_object_type",
            "assigned_object_id",
            "asn",
        ),
        allowed_coalesce_fields=(
            "assigned_object_type",
            "assigned_object_id",
            "asn",
        ),
        default_coalesce_fields=(
            ("assigned_object_type", "assigned_object_id", "asn"),
        ),
    ),
    "netbox_routing.bgpscope": ModelSyncContract(
        required_fields=("router", "vrf"),
        allowed_coalesce_fields=("router", "vrf"),
        default_coalesce_fields=(("router", "vrf"),),
    ),
    "netbox_routing.bgppeer": ModelSyncContract(
        required_fields=(
            "device",
            "local_asn",
            "neighbor_address",
            "peer_asn",
            "enabled",
            "status",
        ),
        allowed_coalesce_fields=("device", "vrf", "neighbor_address"),
        default_coalesce_fields=(
            ("device", "vrf", "neighbor_address"),
            ("device", "neighbor_address"),
        ),
    ),
    "netbox_routing.bgpaddressfamily": ModelSyncContract(
        required_fields=("device", "local_asn", "afi_safi"),
        allowed_coalesce_fields=("device", "vrf", "local_asn", "afi_safi"),
        default_coalesce_fields=(
            ("device", "vrf", "local_asn", "afi_safi"),
            ("device", "local_asn", "afi_safi"),
        ),
    ),
    "netbox_routing.bgppeeraddressfamily": ModelSyncContract(
        required_fields=(
            "device",
            "local_asn",
            "neighbor_address",
            "peer_asn",
            "afi_safi",
            "enabled",
        ),
        allowed_coalesce_fields=("device", "vrf", "neighbor_address", "afi_safi"),
        default_coalesce_fields=(
            ("device", "vrf", "neighbor_address", "afi_safi"),
            ("device", "neighbor_address", "afi_safi"),
        ),
    ),
    "netbox_routing.ospfinstance": ModelSyncContract(
        required_fields=("device", "process_id", "router_id"),
        allowed_coalesce_fields=("device", "vrf", "process_id"),
        default_coalesce_fields=(
            ("device", "vrf", "process_id"),
            ("device", "process_id"),
        ),
    ),
    "netbox_routing.ospfarea": ModelSyncContract(
        required_fields=("area_id", "area_type"),
        allowed_coalesce_fields=("area_id",),
        default_coalesce_fields=(("area_id",),),
    ),
    "netbox_routing.ospfinterface": ModelSyncContract(
        required_fields=(
            "device",
            "process_id",
            "router_id",
            "area_id",
            "area_type",
            "local_interface",
        ),
        allowed_coalesce_fields=("device", "process_id", "local_interface"),
        default_coalesce_fields=(("device", "process_id", "local_interface"),),
    ),
    "netbox_peering_manager.peeringsession": ModelSyncContract(
        required_fields=(
            "device",
            "local_asn",
            "neighbor_address",
            "peer_asn",
            "enabled",
            "status",
        ),
        allowed_coalesce_fields=("device", "vrf", "neighbor_address"),
        default_coalesce_fields=(
            ("device", "vrf", "neighbor_address"),
            ("device", "neighbor_address"),
        ),
    ),
    "netbox_cisco_aci.acifabric": ModelSyncContract(
        required_fields=("name", "fabric_id"),
        allowed_coalesce_fields=("name",),
        default_coalesce_fields=(("name",),),
    ),
    "netbox_cisco_aci.acipod": ModelSyncContract(
        required_fields=("fabric_name", "name", "pod_id"),
        allowed_coalesce_fields=("fabric_name", "pod_id", "name"),
        default_coalesce_fields=(("fabric_name", "pod_id"),),
    ),
    "netbox_cisco_aci.acinode": ModelSyncContract(
        required_fields=(
            "fabric_name",
            "pod_name",
            "pod_id",
            "node_id",
            "name",
            "role",
            "node_type",
        ),
        allowed_coalesce_fields=("fabric_name", "pod_id", "node_id", "name"),
        default_coalesce_fields=(("fabric_name", "pod_id", "node_id"),),
    ),
    "netbox_cisco_aci.acitenant": ModelSyncContract(
        required_fields=("fabric_name", "name"),
        allowed_coalesce_fields=("fabric_name", "name"),
        default_coalesce_fields=(("fabric_name", "name"),),
    ),
    "netbox_cisco_aci.acivrf": ModelSyncContract(
        required_fields=("fabric_name", "tenant_name", "name"),
        allowed_coalesce_fields=("fabric_name", "tenant_name", "name"),
        default_coalesce_fields=(("fabric_name", "tenant_name", "name"),),
    ),
    "netbox_cisco_aci.acibridgedomain": ModelSyncContract(
        required_fields=("fabric_name", "tenant_name", "vrf_name", "name"),
        allowed_coalesce_fields=("fabric_name", "tenant_name", "name"),
        default_coalesce_fields=(("fabric_name", "tenant_name", "name"),),
    ),
    "netbox_cisco_aci.aciappprofile": ModelSyncContract(
        required_fields=("fabric_name", "tenant_name", "name"),
        allowed_coalesce_fields=("fabric_name", "tenant_name", "name"),
        default_coalesce_fields=(("fabric_name", "tenant_name", "name"),),
    ),
    "netbox_cisco_aci.aciendpointgroup": ModelSyncContract(
        required_fields=(
            "fabric_name",
            "tenant_name",
            "app_profile_name",
            "bridge_domain_name",
            "name",
        ),
        allowed_coalesce_fields=(
            "fabric_name",
            "tenant_name",
            "app_profile_name",
            "name",
        ),
        default_coalesce_fields=(
            ("fabric_name", "tenant_name", "app_profile_name", "name"),
        ),
    ),
    "netbox_cisco_aci.acicontract": ModelSyncContract(
        required_fields=("fabric_name", "tenant_name", "name"),
        allowed_coalesce_fields=("fabric_name", "tenant_name", "name"),
        default_coalesce_fields=(("fabric_name", "tenant_name", "name"),),
    ),
    "netbox_cisco_aci.acifilter": ModelSyncContract(
        required_fields=("fabric_name", "tenant_name", "name"),
        allowed_coalesce_fields=("fabric_name", "tenant_name", "name"),
        default_coalesce_fields=(("fabric_name", "tenant_name", "name"),),
    ),
    "netbox_cisco_aci.acil3out": ModelSyncContract(
        required_fields=("fabric_name", "tenant_name", "vrf_name", "name"),
        allowed_coalesce_fields=("fabric_name", "tenant_name", "name"),
        default_coalesce_fields=(("fabric_name", "tenant_name", "name"),),
    ),
    "netbox_cisco_aci.acistaticportbinding": ModelSyncContract(
        required_fields=(
            "fabric_name",
            "tenant_name",
            "app_profile_name",
            "endpoint_group_name",
            "device",
            "interface",
            "encap_vlan",
        ),
        allowed_coalesce_fields=(
            "fabric_name",
            "tenant_name",
            "app_profile_name",
            "endpoint_group_name",
            "device",
            "interface",
            "encap_vlan",
        ),
        default_coalesce_fields=(
            (
                "fabric_name",
                "tenant_name",
                "app_profile_name",
                "endpoint_group_name",
                "device",
                "interface",
                "encap_vlan",
            ),
        ),
    ),
}


def canonical_cable_endpoint_identity(row: dict) -> tuple[tuple[str, str], ...] | None:
    endpoint_a = (row.get("device"), row.get("interface"))
    endpoint_b = (row.get("remote_device"), row.get("remote_interface"))
    if any(
        value in ("", None)
        for endpoint in (endpoint_a, endpoint_b)
        for value in endpoint
    ):
        return None
    return tuple(sorted((endpoint_a, endpoint_b)))


def contract_for_model(model_string: str) -> ModelSyncContract:
    try:
        return MODEL_SYNC_CONTRACTS[model_string]
    except KeyError as exc:
        raise ForwardQueryError(
            f"No sync contract is defined for `{model_string}`."
        ) from exc


def default_coalesce_fields_for_model(model_string: str) -> list[list[str]]:
    contract = contract_for_model(model_string)
    return [list(field_set) for field_set in contract.default_coalesce_fields]


def preserve_existing_on_blank_fields_for_model(model_string: str) -> set[str]:
    contract = MODEL_SYNC_CONTRACTS.get(model_string)
    if contract is None:
        return set()
    return set(contract.preserve_existing_on_blank_fields)


def field_ownership_for_model(model_string: str) -> dict:
    contract = contract_for_model(model_string)
    identity_fields = sorted(
        {
            field_name
            for field_set in contract.default_coalesce_fields
            for field_name in field_set
        }
    )
    preserve_fields = sorted(contract.preserve_existing_on_blank_fields)
    return {
        "model": model_string,
        "identity_fields": identity_fields,
        "required_fields": sorted(contract.required_fields),
        "preserve_existing_on_blank_fields": preserve_fields,
        "blank_update_policy": (
            "preserve_configured_fields"
            if preserve_fields
            else "authoritative_for_declared_fields"
        ),
    }


def normalize_coalesce_fields(
    model_string: str,
    raw_value,
    *,
    allow_default: bool = True,
) -> list[list[str]]:
    contract = contract_for_model(model_string)
    value = raw_value
    if (value is None or value == []) and allow_default:
        return default_coalesce_fields_for_model(model_string)
    if not isinstance(value, list) or not value:
        raise ValueError("`coalesce_fields` must be a non-empty list of field sets.")

    allowed = set(contract.allowed_coalesce_fields)
    normalized: list[list[str]] = []
    seen: set[tuple[str, ...]] = set()
    for field_set in value:
        if not isinstance(field_set, list) or not field_set:
            raise ValueError("Each coalesce field set must be a non-empty list.")
        normalized_set: list[str] = []
        for field_name in field_set:
            if not isinstance(field_name, str) or not field_name.strip():
                raise ValueError("Coalesce field names must be non-empty strings.")
            normalized_name = field_name.strip()
            if normalized_name not in allowed:
                raise ValueError(
                    f"`{normalized_name}` is not allowed for `{model_string}`."
                )
            if normalized_name not in normalized_set:
                normalized_set.append(normalized_name)
        normalized_tuple = tuple(normalized_set)
        if normalized_tuple in seen:
            raise ValueError("Duplicate coalesce field sets are not allowed.")
        seen.add(normalized_tuple)
        normalized.append(normalized_set)
    return normalized


def extract_declared_query_fields(query_text: str) -> set[str]:
    import re

    return set(re.findall(r"(?m)^\s*([A-Za-z_][A-Za-z0-9_]*)\s*:", query_text or ""))


def validate_query_shape_for_model(
    model_string: str,
    query_text: str,
    coalesce_fields: list[list[str]],
) -> None:
    contract = contract_for_model(model_string)
    declared = extract_declared_query_fields(query_text)
    missing_required = [
        field for field in contract.required_fields if field not in declared
    ]
    if missing_required:
        missing = ", ".join(sorted(missing_required))
        raise ValueError(
            f"Query for `{model_string}` is missing required fields: {missing}."
        )
    missing_coalesce = sorted(
        {
            field_name
            for field_set in coalesce_fields
            for field_name in field_set
            if field_name not in declared
        }
    )
    if missing_coalesce:
        missing = ", ".join(missing_coalesce)
        raise ValueError(
            f"Query for `{model_string}` is missing coalesce fields: {missing}."
        )


def validate_row_shape_for_model(
    model_string: str,
    row: dict,
    coalesce_fields: list[list[str]],
) -> None:
    contract = contract_for_model(model_string)
    missing_required = [field for field in contract.required_fields if field not in row]
    if missing_required:
        missing = ", ".join(sorted(missing_required))
        raise ForwardQueryError(
            f"Row for `{model_string}` is missing required fields: {missing}."
        )
    has_complete_coalesce = any(
        all(
            row_coalesce_field_is_complete(model_string, row, field_name)
            for field_name in field_set
        )
        for field_set in coalesce_fields
    )
    if not has_complete_coalesce:
        raise ForwardQueryError(
            f"Row for `{model_string}` does not satisfy any configured coalesce field set."
        )


def row_coalesce_field_is_complete(model_string: str, row: dict, field_name: str):
    if field_name not in row:
        return False
    value = row[field_name]
    if value == "":
        return model_string == "ipam.prefix" and field_name == "vrf"
    if value is None:
        return model_string == "ipam.prefix" and field_name == "vrf"
    return True
