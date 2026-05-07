import re
from dataclasses import dataclass
from dataclasses import field
from pathlib import Path
from typing import Any

from ..choices import FORWARD_SUPPORTED_MODELS
from .sync_contracts import default_coalesce_fields_for_model
from .sync_contracts import normalize_coalesce_fields


@dataclass(frozen=True)
class QuerySpec:
    model_string: str
    query_name: str
    query: str | None = None
    query_id: str | None = None
    commit_id: str | None = None
    parameters: dict[str, Any] = field(default_factory=dict)
    coalesce_fields: tuple[tuple[str, ...], ...] = ()
    placeholder: bool = False

    def __post_init__(self):
        if bool(self.query) == bool(self.query_id):
            raise ValueError("Exactly one of `query` or `query_id` must be defined.")

    @property
    def execution_mode(self) -> str:
        return "query_id" if self.query_id else "query"

    @property
    def execution_value(self) -> str:
        return self.query_id or self.query_name

    def merged_parameters(
        self, extra_parameters: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        parameters = dict(self.parameters)
        if extra_parameters:
            parameters.update(extra_parameters)
        return parameters


QUERY_DIR = Path(__file__).resolve().parents[1] / "queries"
LOCAL_IMPORT_RE = re.compile(r'^\s*import\s+"([^"]+)"\s*;\s*$')
IPADDRESS_UNASSIGNABLE_DIAGNOSTIC_QUERY_NAME = (
    "Forward IP Address Assignment Diagnostics"
)
IPADDRESS_UNASSIGNABLE_DIAGNOSTIC_QUERY_FILE = (
    "forward_ip_addresses_unassignable_diagnostics.nqe"
)


def _read_query_source(filename: str) -> str:
    return (QUERY_DIR / filename).read_text(encoding="utf-8").strip()


def _resolve_local_import(base_path: Path, import_target: str) -> Path | None:
    if import_target.startswith("@"):
        return None

    candidates = [base_path.parent / import_target]
    if not import_target.endswith(".nqe"):
        candidates.append(base_path.parent / f"{import_target}.nqe")

    for candidate in candidates:
        resolved = candidate.resolve()
        try:
            resolved.relative_to(QUERY_DIR)
        except ValueError:
            continue
        if resolved.is_file():
            return resolved

    raise FileNotFoundError(
        f"Unable to resolve local NQE import '{import_target}' from '{base_path.name}'."
    )


def _compile_query_file(
    path: Path,
    *,
    seen: set[Path] | None = None,
    active: tuple[Path, ...] = (),
) -> str:
    if seen is None:
        seen = set()
    resolved_path = path.resolve()
    if resolved_path in active:
        cycle = " -> ".join(module.name for module in (*active, resolved_path))
        raise ValueError(f"Detected local NQE import cycle: {cycle}")
    if resolved_path in seen:
        return ""

    active = (*active, resolved_path)
    source = resolved_path.read_text(encoding="utf-8").strip()
    fragments: list[str] = []
    remaining_lines: list[str] = []

    for line in source.splitlines():
        match = LOCAL_IMPORT_RE.match(line)
        if not match:
            remaining_lines.append(line)
            continue

        import_path = _resolve_local_import(resolved_path, match.group(1))
        if import_path is None:
            remaining_lines.append(line)
            continue

        compiled_import = _compile_query_file(
            import_path,
            seen=seen,
            active=active,
        )
        if compiled_import:
            fragments.append(compiled_import)

    seen.add(resolved_path)
    remaining_source = "\n".join(remaining_lines).strip()
    if remaining_source:
        fragments.append(remaining_source)
    return "\n\n".join(fragment for fragment in fragments if fragment).strip()


def _read_query(filename: str) -> str:
    return _compile_query_file(QUERY_DIR / filename)


def ipaddress_unassignable_diagnostic_query() -> str:
    return _read_query(IPADDRESS_UNASSIGNABLE_DIAGNOSTIC_QUERY_FILE)


BUILTIN_QUERY_MAPS = [
    {
        "model_string": "dcim.site",
        "name": "Forward Locations",
        "filename": "forward_locations.nqe",
    },
    {
        "model_string": "dcim.manufacturer",
        "name": "Forward Device Vendors",
        "filename": "forward_device_vendors.nqe",
    },
    {
        "model_string": "dcim.devicerole",
        "name": "Forward Device Types",
        "filename": "forward_device_types.nqe",
    },
    {
        "model_string": "dcim.platform",
        "name": "Forward Platforms",
        "filename": "forward_platforms.nqe",
    },
    {
        "model_string": "dcim.devicetype",
        "name": "Forward Device Models",
        "filename": "forward_device_models.nqe",
    },
    {
        "model_string": "dcim.device",
        "name": "Forward Devices",
        "filename": "forward_devices.nqe",
    },
    {
        "model_string": "dcim.virtualchassis",
        "name": "Forward Virtual Chassis",
        "filename": "forward_virtual_chassis.nqe",
    },
    {
        "model_string": "extras.taggeditem",
        "name": "Forward Device Feature Tags",
        "filename": "forward_device_feature_tags.nqe",
    },
    {
        "model_string": "dcim.interface",
        "name": "Forward Interfaces",
        "filename": "forward_interfaces.nqe",
    },
    {
        "model_string": "dcim.cable",
        "name": "Forward Inferred Interface Cables",
        "filename": "forward_inferred_interface_cables.nqe",
    },
    {
        "model_string": "dcim.macaddress",
        "name": "Forward MAC Addresses",
        "filename": "forward_mac_addresses.nqe",
    },
    {
        "model_string": "ipam.vlan",
        "name": "Forward VLANs",
        "filename": "forward_vlans.nqe",
    },
    {
        "model_string": "ipam.vrf",
        "name": "Forward VRFs",
        "filename": "forward_vrfs.nqe",
    },
    {
        "model_string": "ipam.prefix",
        "name": "Forward IPv4 Prefixes",
        "filename": "forward_prefixes_ipv4.nqe",
    },
    {
        "model_string": "ipam.prefix",
        "name": "Forward IPv6 Prefixes",
        "filename": "forward_prefixes_ipv6.nqe",
    },
    {
        "model_string": "ipam.ipaddress",
        "name": "Forward IP Addresses",
        "filename": "forward_ip_addresses.nqe",
    },
    {
        "model_string": "dcim.inventoryitem",
        "name": "Forward Inventory Items",
        "filename": "forward_inventory_items.nqe",
    },
]

BUILTIN_OPTIONAL_QUERY_MAPS = [
    {
        "model_string": "dcim.devicetype",
        "name": "Forward Device Models with NetBox Device Type Aliases",
        "filename": "forward_device_models_with_netbox_aliases.nqe",
        "enabled": False,
    },
    {
        "model_string": "dcim.device",
        "name": "Forward Devices with NetBox Device Type Aliases",
        "filename": "forward_devices_with_netbox_aliases.nqe",
        "enabled": False,
    },
    {
        "model_string": "dcim.cablebundle",
        "name": "Forward Cable Bundles",
        "filename": "forward_cable_bundles.nqe",
        "enabled": False,
    },
    {
        "model_string": "extras.taggeditem",
        "name": "Forward Device Feature Tags with Rules",
        "filename": "forward_device_feature_tags_with_rules.nqe",
        "enabled": False,
    },
    {
        "model_string": "dcim.module",
        "name": "Forward Modules",
        "filename": "forward_modules.nqe",
        "enabled": False,
    },
]

BUILTIN_SEEDED_QUERY_MAPS = [
    *BUILTIN_QUERY_MAPS,
    *BUILTIN_OPTIONAL_QUERY_MAPS,
]

BUILTIN_QUERY_DEFAULTS = {
    (query_default["model_string"], query_default["name"]): query_default
    for query_default in BUILTIN_SEEDED_QUERY_MAPS
}


def builtin_nqe_map_rows() -> list[dict[str, Any]]:
    rows = []
    for index, query_default in enumerate(BUILTIN_SEEDED_QUERY_MAPS, start=1):
        rows.append(
            {
                "model_string": query_default["model_string"],
                "name": query_default["name"],
                "query_id": "",
                "query": _read_query_source(query_default["filename"]),
                "commit_id": "",
                "parameters": {},
                "coalesce_fields": default_coalesce_fields_for_model(
                    query_default["model_string"]
                ),
                "weight": index * 100,
                "enabled": query_default.get("enabled", True),
            }
        )
    return rows


def _build_builtin_query_spec(query_default: dict[str, Any]) -> QuerySpec:
    return QuerySpec(
        model_string=query_default["model_string"],
        query_name=query_default["name"],
        query=_read_query(query_default["filename"]),
        coalesce_fields=tuple(
            tuple(field_set)
            for field_set in default_coalesce_fields_for_model(
                query_default["model_string"]
            )
        ),
    )


def _build_query_spec_from_map(query_map) -> QuerySpec:
    normalized_coalesce = normalize_coalesce_fields(
        query_map.model_string,
        query_map.coalesce_fields,
        allow_default=True,
    )
    if query_map.built_in:
        query_default = BUILTIN_QUERY_DEFAULTS.get(
            (query_map.model_string, query_map.name)
        )
        if query_default is not None:
            return QuerySpec(
                model_string=query_map.model_string,
                query_name=query_map.name,
                query=_read_query(query_default["filename"]),
                parameters=query_map.parameters or {},
                coalesce_fields=tuple(
                    tuple(field_set) for field_set in normalized_coalesce
                ),
                placeholder=False,
            )
    return QuerySpec(
        model_string=query_map.model_string,
        query_name=query_map.name,
        query=query_map.query or None,
        query_id=query_map.query_id or None,
        commit_id=query_map.commit_id or None,
        parameters=query_map.parameters or {},
        coalesce_fields=tuple(tuple(field_set) for field_set in normalized_coalesce),
        placeholder=False,
    )


def _resolve_map_query_specs(model_string: str, maps) -> list[QuerySpec]:
    selected_maps = [
        query_map
        for query_map in maps or []
        if query_map.enabled and query_map.model_string == model_string
    ]
    custom_maps = [query_map for query_map in selected_maps if not query_map.built_in]
    builtin_maps = [query_map for query_map in selected_maps if query_map.built_in]
    chosen_maps = custom_maps or builtin_maps
    return [_build_query_spec_from_map(query_map) for query_map in chosen_maps]


def optional_builtin_query_names_for_model(model_string: str) -> list[str]:
    return [
        query_default["name"]
        for query_default in BUILTIN_OPTIONAL_QUERY_MAPS
        if query_default["model_string"] == model_string
    ]


def get_query_specs(
    model_string: str,
    maps=None,
) -> list[QuerySpec]:
    selected_specs = _resolve_map_query_specs(model_string, maps)
    if selected_specs:
        return selected_specs
    if maps:
        return []
    return BUILTIN_QUERY_SPECS[model_string]


def get_seeded_builtin_query_spec(model_string: str, query_name: str) -> QuerySpec:
    query_default = BUILTIN_QUERY_DEFAULTS.get((model_string, query_name))
    if query_default is None:
        raise KeyError(
            f"No seeded built-in query named `{query_name}` for {model_string}."
        )
    return _build_builtin_query_spec(query_default)


BUILTIN_QUERY_SPECS = {model_string: [] for model_string in FORWARD_SUPPORTED_MODELS}
for query_default in BUILTIN_QUERY_MAPS:
    BUILTIN_QUERY_SPECS[query_default["model_string"]].append(
        _build_builtin_query_spec(query_default)
    )


def describe_builtin_queries() -> list[str]:
    return [
        f"{model_string}: bundled raw query ({spec.query_name})"
        for model_string, specs in BUILTIN_QUERY_SPECS.items()
        for spec in specs
    ]
