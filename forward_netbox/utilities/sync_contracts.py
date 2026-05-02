from dataclasses import dataclass

from ..exceptions import ForwardQueryError


@dataclass(frozen=True)
class ModelSyncContract:
    required_fields: tuple[str, ...]
    allowed_coalesce_fields: tuple[str, ...]
    default_coalesce_fields: tuple[tuple[str, ...], ...]


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
        required_fields=("name", "slug", "manufacturer", "manufacturer_slug"),
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
        required_fields=("prefix", "status"),
        allowed_coalesce_fields=("prefix", "vrf"),
        default_coalesce_fields=(("prefix", "vrf"), ("prefix",)),
    ),
    "ipam.ipaddress": ModelSyncContract(
        required_fields=("device", "interface", "address", "status"),
        allowed_coalesce_fields=("address", "vrf"),
        default_coalesce_fields=(("address", "vrf"), ("address",)),
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
        default_coalesce_fields=(("device", "name", "part_id", "serial"),),
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
            field_name in row and row[field_name] not in ("", None)
            for field_name in field_set
        )
        for field_set in coalesce_fields
    )
    if not has_complete_coalesce:
        raise ForwardQueryError(
            f"Row for `{model_string}` does not satisfy any configured coalesce field set."
        )
