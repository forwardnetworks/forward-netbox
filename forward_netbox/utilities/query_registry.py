import re
from dataclasses import dataclass
from dataclasses import field
from dataclasses import replace
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
    query_repository: str | None = None
    query_path: str | None = None
    commit_id: str | None = None
    resolved_query_id: str | None = None
    parameters: dict[str, Any] = field(default_factory=dict)
    coalesce_fields: tuple[tuple[str, ...], ...] = ()
    placeholder: bool = False

    def __post_init__(self):
        reference_count = sum(
            bool(value) for value in (self.query, self.query_id, self.query_path)
        )
        if reference_count != 1:
            raise ValueError(
                "Exactly one of `query`, `query_id`, or `query_path` must be defined."
            )
        if self.query_path and not self.query_repository:
            raise ValueError("`query_repository` must be defined with `query_path`.")

    @property
    def execution_mode(self) -> str:
        if self.query_path:
            return "query_path"
        return "query_id" if self.query_id else "query"

    @property
    def execution_value(self) -> str:
        if self.query_path:
            return f"{self.query_repository}:{self.query_path}"
        return self.query_id or self.query_name

    @property
    def run_query_id(self) -> str | None:
        return self.query_id or self.resolved_query_id

    @property
    def diff_query_id(self) -> str | None:
        return self.run_query_id

    def resolve(self, client) -> "QuerySpec":
        if not self.query_path:
            return self
        resolved = client.resolve_nqe_query_reference(
            repository=self.query_repository or "org",
            query_path=self.query_path,
            commit_id=self.commit_id,
        )
        resolved_query_id = str(resolved.get("queryId") or "").strip()
        resolved_commit_id = str(
            self.commit_id or resolved.get("commitId") or ""
        ).strip()
        return replace(
            self,
            resolved_query_id=resolved_query_id or None,
            commit_id=resolved_commit_id or None,
        )

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
ROUTING_IMPORT_DIAGNOSTIC_QUERY_NAME = "Forward Routing Import Diagnostics"
ROUTING_IMPORT_DIAGNOSTIC_QUERY_FILE = "forward_routing_import_diagnostics.nqe"


def _read_query_source(filename: str) -> str:
    return (QUERY_DIR / filename).read_text(encoding="utf-8").strip()


def read_builtin_query_source(filename: str) -> str:
    return _read_query_source(filename)


def builtin_query_source_filenames(filename: str) -> tuple[str, ...]:
    ordered_filenames = []
    seen_paths = set()

    def visit(path: Path):
        resolved_path = path.resolve()
        if resolved_path in seen_paths:
            return
        seen_paths.add(resolved_path)
        source = resolved_path.read_text(encoding="utf-8").strip()
        for line in source.splitlines():
            match = LOCAL_IMPORT_RE.match(line)
            if not match:
                continue
            import_path = _resolve_local_import(resolved_path, match.group(1))
            if import_path is not None:
                visit(import_path)
        ordered_filenames.append(resolved_path.name)

    visit(QUERY_DIR / filename)
    return tuple(ordered_filenames)


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


def routing_import_diagnostic_query() -> str:
    return _read_query(ROUTING_IMPORT_DIAGNOSTIC_QUERY_FILE)


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
        "model_string": "extras.taggeditem",
        "name": "Forward Device Feature Tags with Rules",
        "filename": "forward_device_feature_tags_with_rules.nqe",
        "enabled": False,
    },
    {
        "model_string": "dcim.module",
        "name": "Forward Modules",
        "filename": "forward_modules.nqe",
        "enabled": True,
    },
    {
        "model_string": "netbox_routing.bgppeer",
        "name": "Forward BGP Peers",
        "filename": "forward_bgp_peers.nqe",
        "enabled": True,
    },
    {
        "model_string": "netbox_routing.bgpaddressfamily",
        "name": "Forward BGP Address Families",
        "filename": "forward_bgp_address_families.nqe",
        "enabled": True,
    },
    {
        "model_string": "netbox_routing.bgppeeraddressfamily",
        "name": "Forward BGP Peer Address Families",
        "filename": "forward_bgp_peer_address_families.nqe",
        "enabled": True,
    },
    {
        "model_string": "netbox_routing.ospfinstance",
        "name": "Forward OSPF Instances",
        "filename": "forward_ospf_instances.nqe",
        "enabled": True,
    },
    {
        "model_string": "netbox_routing.ospfarea",
        "name": "Forward OSPF Areas",
        "filename": "forward_ospf_areas.nqe",
        "enabled": True,
    },
    {
        "model_string": "netbox_routing.ospfinterface",
        "name": "Forward OSPF Interfaces",
        "filename": "forward_ospf_interfaces.nqe",
        "enabled": True,
    },
    {
        "model_string": "netbox_peering_manager.peeringsession",
        "name": "Forward Peering Sessions",
        "filename": "forward_peering_sessions.nqe",
        "enabled": True,
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
                "query_repository": "",
                "query_path": "",
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
            if query_map.query_id:
                return QuerySpec(
                    model_string=query_map.model_string,
                    query_name=query_map.name,
                    query_id=query_map.query_id,
                    commit_id=query_map.commit_id or None,
                    parameters=query_map.parameters or {},
                    coalesce_fields=tuple(
                        tuple(field_set) for field_set in normalized_coalesce
                    ),
                    placeholder=False,
                )
            if getattr(query_map, "query_path", ""):
                return QuerySpec(
                    model_string=query_map.model_string,
                    query_name=query_map.name,
                    query_repository=query_map.query_repository or "org",
                    query_path=query_map.query_path,
                    commit_id=query_map.commit_id or None,
                    parameters=query_map.parameters or {},
                    coalesce_fields=tuple(
                        tuple(field_set) for field_set in normalized_coalesce
                    ),
                    placeholder=False,
                )
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
        query_repository=getattr(query_map, "query_repository", "") or None,
        query_path=getattr(query_map, "query_path", "") or None,
        commit_id=query_map.commit_id or None,
        parameters=query_map.parameters or {},
        coalesce_fields=tuple(tuple(field_set) for field_set in normalized_coalesce),
        placeholder=False,
    )


def resolve_query_specs_for_client(specs: list[QuerySpec], client) -> list[QuerySpec]:
    return [spec.resolve(client) for spec in specs]


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
