from dataclasses import dataclass
from dataclasses import field
from pathlib import Path
from typing import Any

from ..choices import FORWARD_SUPPORTED_MODELS


@dataclass(frozen=True)
class QuerySpec:
    model_string: str
    query_name: str
    query: str | None = None
    query_id: str | None = None
    commit_id: str | None = None
    parameters: dict[str, Any] = field(default_factory=dict)
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


def _read_query(filename: str) -> str:
    return (QUERY_DIR / filename).read_text(encoding="utf-8").strip()


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
        "model_string": "dcim.interface",
        "name": "Forward Interfaces",
        "filename": "forward_interfaces.nqe",
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


def builtin_nqe_map_rows() -> list[dict[str, Any]]:
    rows = []
    for index, query_default in enumerate(BUILTIN_QUERY_MAPS, start=1):
        rows.append(
            {
                "model_string": query_default["model_string"],
                "name": query_default["name"],
                "query_id": "",
                "query": _read_query(query_default["filename"]),
                "commit_id": "",
                "parameters": {},
                "weight": index * 100,
            }
        )
    return rows


def _build_builtin_query_spec(query_default: dict[str, Any]) -> QuerySpec:
    return QuerySpec(
        model_string=query_default["model_string"],
        query_name=query_default["name"],
        query=_read_query(query_default["filename"]),
    )


def _build_query_spec_from_map(query_map) -> QuerySpec:
    return QuerySpec(
        model_string=query_map.model_string,
        query_name=query_map.name,
        query=query_map.query or None,
        query_id=query_map.query_id or None,
        commit_id=query_map.commit_id or None,
        parameters=query_map.parameters or {},
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
