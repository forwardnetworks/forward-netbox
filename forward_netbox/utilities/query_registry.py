import json
import re
from dataclasses import dataclass
from dataclasses import field
from dataclasses import replace
from pathlib import Path
from typing import Any

from rq.timeouts import JobTimeoutException

from ..choices import FORWARD_SUPPORTED_MODELS
from ..exceptions import ForwardQueryError
from .model_contracts import architecture_default_coalesce_fields_for_model
from .model_contracts import architecture_fetch_contract_for_model
from .plugin_integrations.registry import OPTIONAL_PLUGIN_INTEGRATIONS
from .query_execution_contract import declared_query_parameters
from .query_execution_contract import query_source_sha256
from .sync_contracts import normalize_coalesce_fields


def _resolve_head_commit_for_query_id(
    client,
    *,
    query_id,
    repository,
    query_index,
) -> str:
    """Head commit for an ID-only binding, or "" when it cannot be resolved.

    Never raises: an unresolvable head leaves the spec unchanged so the existing
    contract reporting explains the refusal, rather than failing the whole fetch
    on a repository lookup.
    """
    resolver = getattr(client, "resolve_nqe_query_head_commit", None)
    if resolver is None:
        return ""
    try:
        return str(
            resolver(
                query_id=query_id,
                repository=repository,
                query_index=query_index,
            )
            or ""
        ).strip()
    except Exception:
        return ""


@dataclass(frozen=True)
class QuerySpec:
    model_string: str
    query_name: str
    query: str | None = None
    query_id: str | None = None
    query_repository: str | None = None
    query_path: str | None = None
    query_intent: str | None = None
    commit_id: str | None = None
    resolved_query_id: str | None = None
    resolved_query_path: str | None = None
    map_id: int | None = None
    map_weight: int = 100
    built_in: bool = False
    contract_key: str = ""
    full_query_source: str | None = None
    full_source_sha256: str = ""
    diff_commit_id: str | None = None
    diff_query_source: str | None = None
    diff_source_sha256: str = ""
    variant: str = "base"
    required_data_files: tuple[str, ...] = ()
    data_file_hashes: dict[str, str] = field(default_factory=dict)
    reducer_id: str = "direct_rows"
    reducer_version: int = 1
    diff_ownership_mode: str = "global"
    normalization_version: int = 1
    identity_version: int = 1
    query_contract_version: int = 1
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
        return self.run_query_id if self.diff_commit_id else None

    def resolve(self, client, query_index: dict | None = None) -> "QuerySpec":
        # A map bound by query ID keeps its path in resolved_query_path only, so
        # resolving on query_path alone left every ID-bound map without a commit.
        # The execution contract then rejected it as unresolved_full_commit and
        # skipped the model, which silently emptied a whole sync.
        lookup_query_path = self.query_path or self.resolved_query_path
        if not lookup_query_path:
            # Bound by query ID with no path at all. Resolve head from the
            # repository index instead, so an ID-only binding is still runnable.
            if self.commit_id or not self.query_id:
                return self
            head_commit_id = _resolve_head_commit_for_query_id(
                client,
                query_id=self.query_id,
                repository=self.query_repository or "org",
                query_index=query_index,
            )
            if not head_commit_id:
                return self
            return replace(self, commit_id=head_commit_id)
        resolved = client.get_committed_nqe_query(
            repository=self.query_repository or "org",
            query_path=lookup_query_path,
            commit_id=self.commit_id or "head",
            query_index=query_index,
        )
        resolved_query_id = str(resolved.get("queryId") or "").strip()
        # Only adopt a commit the bound query actually owns. If the path now
        # resolves to a different query, the head commit belongs to that other
        # query and grafting it here would execute the wrong revision.
        bound_query_id = str(self.query_id or "").strip()
        commit_belongs_to_bound_query = (
            not bound_query_id
            or not resolved_query_id
            or bound_query_id == resolved_query_id
        )
        resolved_commit_id = str(
            self.commit_id
            or (
                (
                    resolved.get("commitId")
                    or resolved.get("lastCommitId")
                    or (resolved.get("lastCommit") or {}).get("id")
                )
                if commit_belongs_to_bound_query
                else ""
            )
            or ""
        ).strip()
        resolved_query_path = str(
            resolved.get("path") or self.resolved_query_path or self.query_path or ""
        ).strip()
        return replace(
            self,
            resolved_query_id=resolved_query_id or None,
            resolved_query_path=resolved_query_path or None,
            commit_id=resolved_commit_id or None,
        )

    def merged_parameters(
        self, extra_parameters: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        parameters = dict(self.parameters)
        if extra_parameters and parameters:
            parameters.update(
                {
                    key: value
                    for key, value in extra_parameters.items()
                    if key in parameters
                }
            )
        return parameters


def ensure_unique_query_spec_executions(
    specs,
    *,
    extra_parameters: dict[str, Any] | None = None,
) -> list[QuerySpec]:
    """Reject map definitions that would execute the same logical NQE twice."""
    unique_specs = list(specs or [])
    seen = {}
    for spec in unique_specs:
        query_id = str(spec.run_query_id or "").strip()
        if query_id:
            reference = ("query_id", query_id)
        elif spec.query is not None:
            reference = ("query", spec.query)
        else:
            reference = (
                "query_path",
                str(spec.query_repository or "org"),
                str(spec.query_path or ""),
            )
        identity = (
            reference,
            str(spec.commit_id or ""),
            json.dumps(
                spec.merged_parameters(extra_parameters),
                sort_keys=True,
                separators=(",", ":"),
                default=str,
            ),
        )
        existing = seen.get(identity)
        if existing is not None:
            raise ForwardQueryError(
                "Duplicate logical NQE execution for "
                f"{spec.model_string}: maps `{existing.query_name}` and "
                f"`{spec.query_name}` resolve to the same query, commit, and "
                "parameters. Disable or consolidate one map."
            )
        seen[identity] = spec
    return unique_specs


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
SHARD_QUERY_PARAMETER_NAME = "forward_netbox_shard_keys"
SHARD_QUERY_PARAMETER_DEFAULT = {SHARD_QUERY_PARAMETER_NAME: []}
# Operator-selected Forward tags to sync as NetBox device tags. Declared by any
# query whose source references it (the device-feature-tags sync query); the
# resolved selection is injected in _prepare_query_parameters.
SYNC_DEVICE_TAGS_PARAMETER_NAME = "sync_device_tags"
SYNC_DEVICE_TAGS_PARAMETER_DEFAULT = {SYNC_DEVICE_TAGS_PARAMETER_NAME: []}
# Opt-in: also import Forward SNMP endpoints (generic SSH/SNMP devices Forward
# collects but does not model as first-class devices, e.g. Avocent console
# servers) as NetBox devices. Declared by the device query; default off.
SYNC_ENDPOINTS_PARAMETER_NAME = "sync_endpoints"
SYNC_ENDPOINTS_PARAMETER_DEFAULT = {SYNC_ENDPOINTS_PARAMETER_NAME: False}
# Generic endpoint import is deliberately separate from console-server import.
# Most SNMP endpoints expose only MIB-2 identity and otherwise create sparse,
# low-confidence NetBox devices. Keep the broad behavior behind an explicit
# opt-in while sync_endpoints continues to enable recognized console servers.
SYNC_GENERIC_ENDPOINTS_PARAMETER_NAME = "sync_generic_endpoints"
SYNC_GENERIC_ENDPOINTS_PARAMETER_DEFAULT = {
    SYNC_GENERIC_ENDPOINTS_PARAMETER_NAME: False
}
# Opt-in: endpoints must also carry the device include tags (by default the
# include scope narrows modeled devices only). Declared by the device query;
# default off (preserves the 2.4.4 endpoints-ignore-include-scope behavior).
SCOPE_ENDPOINTS_BY_INCLUDE_TAGS_PARAMETER_NAME = "scope_endpoints_by_include_tags"
SCOPE_ENDPOINTS_BY_INCLUDE_TAGS_PARAMETER_DEFAULT = {
    SCOPE_ENDPOINTS_BY_INCLUDE_TAGS_PARAMETER_NAME: False
}
DEVICE_TAG_QUERY_PARAMETER_DEFAULTS = {
    "device_tag_include_tags": [],
    "device_tag_include_match": "any",
    "device_tag_exclude_tags": [],
}
DEVICE_TAG_PARAMETER_QUERY_FILES = {
    "forward_devices.nqe",
    "forward_devices_with_netbox_aliases.nqe",
    "forward_hsrp_groups.nqe",
    "forward_locations.nqe",
    "forward_prefixes_ipv4.nqe",
    "forward_prefixes_ipv6.nqe",
    "forward_vlans.nqe",
    "forward_vrfs.nqe",
}
LEGACY_SAFE_PARAMETER_DEFAULTS = {
    **SHARD_QUERY_PARAMETER_DEFAULT,
    **SYNC_DEVICE_TAGS_PARAMETER_DEFAULT,
    **SYNC_ENDPOINTS_PARAMETER_DEFAULT,
    **SYNC_GENERIC_ENDPOINTS_PARAMETER_DEFAULT,
    **SCOPE_ENDPOINTS_BY_INCLUDE_TAGS_PARAMETER_DEFAULT,
    **DEVICE_TAG_QUERY_PARAMETER_DEFAULTS,
}


def only_legacy_safe_default_parameters(parameters: dict[str, Any]) -> bool:
    """Return whether every supplied parameter is a known unchanged default."""
    return bool(parameters) and all(
        name in LEGACY_SAFE_PARAMETER_DEFAULTS
        and value == LEGACY_SAFE_PARAMETER_DEFAULTS[name]
        for name, value in parameters.items()
    )


def _read_query_source(filename: str) -> str:
    return (QUERY_DIR / filename).read_text(encoding="utf-8").strip()


def _query_intent(filename: str) -> str:
    match = re.search(
        r"^[ \t]*\*[ \t]*@intent[ \t]+(.+?)[ \t]*$",
        _read_query_source(filename),
        flags=re.MULTILINE,
    )
    return match.group(1).strip() if match else ""


def _default_query_parameters(filename: str) -> dict[str, Any]:
    parameters = {}
    source = None
    if filename in DEVICE_TAG_PARAMETER_QUERY_FILES:
        source = _read_query(filename)
        if "device_tag_include_tags" in source:
            parameters.update(DEVICE_TAG_QUERY_PARAMETER_DEFAULTS)
    if source is None:
        source = _read_query(filename)
    if SHARD_QUERY_PARAMETER_NAME in source:
        parameters.update(SHARD_QUERY_PARAMETER_DEFAULT)
    if SYNC_DEVICE_TAGS_PARAMETER_NAME in source:
        parameters.update(SYNC_DEVICE_TAGS_PARAMETER_DEFAULT)
    if SYNC_ENDPOINTS_PARAMETER_NAME in source:
        parameters.update(SYNC_ENDPOINTS_PARAMETER_DEFAULT)
    if SYNC_GENERIC_ENDPOINTS_PARAMETER_NAME in source:
        parameters.update(SYNC_GENERIC_ENDPOINTS_PARAMETER_DEFAULT)
    if SCOPE_ENDPOINTS_BY_INCLUDE_TAGS_PARAMETER_NAME in source:
        parameters.update(SCOPE_ENDPOINTS_BY_INCLUDE_TAGS_PARAMETER_DEFAULT)
    return parameters


def _query_map_parameters(query_default: dict[str, Any], query_map) -> dict[str, Any]:
    parameters = _default_query_parameters(query_default["filename"])
    parameters.update(query_map.parameters or {})
    return parameters


def read_builtin_query_source(filename: str) -> str:
    return _read_query_source(filename)


def read_compiled_builtin_query_source(filename: str) -> str:
    return _read_query(filename)


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
        "name": "Forward IPv4 IP Addresses",
        "filename": "forward_ip_addresses_ipv4.nqe",
    },
    {
        "model_string": "ipam.ipaddress",
        "name": "Forward IPv6 IP Addresses",
        "filename": "forward_ip_addresses_ipv6.nqe",
    },
    {
        "model_string": "ipam.fhrpgroup",
        "name": "Forward HSRP Groups",
        "filename": "forward_hsrp_groups.nqe",
    },
    {
        "model_string": "dcim.inventoryitem",
        "name": "Forward Inventory Items",
        "filename": "forward_inventory_items.nqe",
    },
]

BUILTIN_OPTIONAL_QUERY_MAPS = [
    {
        "model_string": "dcim.virtualchassis",
        "name": "Forward Virtual Chassis",
        "filename": "forward_virtual_chassis.nqe",
        "enabled": False,
    },
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
        "model_string": "dcim.device",
        "name": "Forward ACI Command Inventory",
        "filename": "forward_aci_command_inventory.nqe",
        "enabled": False,
    },
    {
        "model_string": "extras.taggeditem",
        "name": "Forward Device Feature Tags with Rules",
        "filename": "forward_device_feature_tags_with_rules.nqe",
        "enabled": False,
    },
    {
        "model_string": "netbox_dlm.softwareversion",
        "name": "Forward DLM Software Versions",
        "filename": "forward_dlm_software_versions.nqe",
        "enabled": False,
    },
    {
        "model_string": "netbox_dlm.hardwarenotice",
        "name": "Forward DLM Hardware Notices",
        "filename": "forward_dlm_hardware_notices.nqe",
        "enabled": False,
    },
    {
        "model_string": "netbox_dlm.hardwarenotice",
        "name": "Forward DLM Hardware Notices with NetBox Aliases",
        "filename": "forward_dlm_hardware_notices_with_netbox_aliases.nqe",
        "enabled": False,
    },
    {
        "model_string": "netbox_dlm.devicesoftware",
        "name": "Forward DLM Device Software",
        "filename": "forward_dlm_device_software.nqe",
        "enabled": False,
    },
    {
        "model_string": "netbox_dlm.cve",
        "name": "Forward DLM CVEs",
        "filename": "forward_dlm_cves.nqe",
        "enabled": False,
    },
    {
        "model_string": "netbox_dlm.vulnerability",
        "name": "Forward DLM Vulnerabilities",
        "filename": "forward_dlm_vulnerabilities.nqe",
        "enabled": False,
    },
    {
        "model_string": "dcim.inventoryitem",
        "name": "Forward CIMC Endpoint Inventory",
        "filename": "forward_cimc_endpoint_inventory.nqe",
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
    {
        "model_string": "netbox_cisco_aci.acifabric",
        "name": "Forward ACI Fabrics",
        "filename": "forward_aci_fabrics.nqe",
        "enabled": False,
    },
    {
        "model_string": "netbox_cisco_aci.acipod",
        "name": "Forward ACI Pods",
        "filename": "forward_aci_pods.nqe",
        "enabled": False,
    },
    {
        "model_string": "netbox_cisco_aci.acinode",
        "name": "Forward ACI Nodes",
        "filename": "forward_aci_nodes.nqe",
        "enabled": False,
    },
    {
        "model_string": "netbox_cisco_aci.acinode",
        "name": "Forward ACI APIC Nodes",
        "filename": "forward_aci_apic_nodes.nqe",
        "enabled": False,
    },
    {
        "model_string": "dcim.inventoryitem",
        "name": "Forward ACI APIC CIMC Inventory",
        "filename": "forward_aci_apic_cimc_inventory.nqe",
        "enabled": False,
    },
    {
        "model_string": "netbox_cisco_aci.acitenant",
        "name": "Forward ACI Tenants",
        "filename": "forward_aci_tenants.nqe",
        "enabled": False,
    },
    {
        "model_string": "netbox_cisco_aci.acivrf",
        "name": "Forward ACI VRFs",
        "filename": "forward_aci_vrfs.nqe",
        "enabled": False,
    },
    {
        "model_string": "netbox_cisco_aci.acibridgedomain",
        "name": "Forward ACI Bridge Domains",
        "filename": "forward_aci_bridge_domains.nqe",
        "enabled": False,
    },
    {
        "model_string": "netbox_cisco_aci.acifilter",
        "name": "Forward ACI Filters",
        "filename": "forward_aci_filters.nqe",
        "enabled": False,
    },
    {
        "model_string": "netbox_cisco_aci.acil3out",
        "name": "Forward ACI L3Outs",
        "filename": "forward_aci_l3outs.nqe",
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
                "query_repository": "",
                "query_path": "",
                "query": _read_query_source(query_default["filename"]),
                "commit_id": "",
                "parameters": _default_query_parameters(query_default["filename"]),
                "coalesce_fields": architecture_default_coalesce_fields_for_model(
                    query_default["model_string"]
                ),
                "weight": index * 100,
                "enabled": query_default.get("enabled", True),
            }
        )
    return rows


def query_contract_summary_for_maps(
    query_defaults: list[dict[str, Any]],
    model_strings=None,
) -> dict[str, Any]:
    """Report whether shipped NQE maps satisfy model fetch contracts."""
    selected_models = tuple(model_strings or FORWARD_SUPPORTED_MODELS)
    contracts = {
        model_string: architecture_fetch_contract_for_model(model_string)
        for model_string in selected_models
    }
    query_defaults_by_model: dict[str, list[dict[str, Any]]] = {
        model_string: [] for model_string in selected_models
    }
    for query_default in query_defaults:
        model_string = query_default["model_string"]
        if model_string in query_defaults_by_model:
            query_defaults_by_model[model_string].append(query_default)

    model_reports = {}
    gaps = []
    for model_string in sorted(selected_models):
        contract = contracts.get(model_string) or {}
        query_defaults = query_defaults_by_model.get(model_string) or []
        query_reports = [
            _builtin_query_parameter_contract_report(model_string, query_default)
            for query_default in query_defaults
        ]

        if contract.get("fetch_mode") == "nqe_parameters" and not query_reports:
            gaps.append(
                _query_contract_gap(
                    model_string,
                    "",
                    "",
                    "missing_builtin_query_map",
                    "Model fetch contract is parameterized but no shipped query map exists.",
                )
            )

        for query_report in query_reports:
            if contract.get("fetch_mode") != "nqe_parameters":
                continue
            for check_key, code, message in (
                (
                    "declares_shard_parameter",
                    "missing_shard_parameter_declaration",
                    "Query does not declare forward_netbox_shard_keys.",
                ),
                (
                    "seeds_empty_shard_parameter",
                    "missing_shard_parameter_default",
                    "Query map does not seed an empty forward_netbox_shard_keys default.",
                ),
                (
                    "has_empty_shard_guard",
                    "missing_empty_shard_guard",
                    "Query does not keep no-parameter UI execution unfiltered.",
                ),
                (
                    "has_positive_shard_predicate",
                    "missing_positive_shard_predicate",
                    "Query does not use forward_netbox_shard_keys to constrain rows.",
                ),
            ):
                if query_report[check_key]:
                    continue
                gaps.append(
                    _query_contract_gap(
                        model_string,
                        query_report["query_name"],
                        query_report["filename"],
                        code,
                        message,
                    )
                )

        model_reports[model_string] = {
            "model": model_string,
            "fetch_mode": contract.get("fetch_mode") or "",
            "key_family": contract.get("key_family") or "",
            "query_count": len(query_reports),
            "queries": query_reports,
        }

    return {
        "status": "pass" if not gaps else "fail",
        "model_count": len(selected_models),
        "models": model_reports,
        "gaps": gaps,
    }


def builtin_query_contract_summary(model_strings=None) -> dict[str, Any]:
    return query_contract_summary_for_maps(BUILTIN_SEEDED_QUERY_MAPS, model_strings)


def optional_plugin_query_contract_summary(model_strings=None) -> dict[str, Any]:
    summary = {}
    for integration in OPTIONAL_PLUGIN_INTEGRATIONS:
        integration_query_defaults = [
            query_default
            for query_default in BUILTIN_OPTIONAL_QUERY_MAPS
            if query_default["name"] in integration.query_maps
        ]
        integration_model_strings = tuple(
            sorted(set(integration.supported_models).union(integration.native_models))
        )
        summary[integration.key] = query_contract_summary_for_maps(
            integration_query_defaults,
            model_strings or integration_model_strings,
        )
    return summary


def _builtin_query_parameter_contract_report(
    model_string: str,
    query_default: dict[str, Any],
) -> dict[str, Any]:
    filename = query_default["filename"]
    query = _read_query(filename)
    parameters = _default_query_parameters(filename)
    empty_guard_patterns = (
        f"isEmpty({SHARD_QUERY_PARAMETER_NAME})",
        f"length({SHARD_QUERY_PARAMETER_NAME}) == 0",
    )
    positive_predicate_patterns = (
        f"in {SHARD_QUERY_PARAMETER_NAME}",
        f"contains({SHARD_QUERY_PARAMETER_NAME}",
    )
    return {
        "model": model_string,
        "query_name": query_default["name"],
        "filename": filename,
        "enabled_by_default": bool(query_default.get("enabled", True)),
        "declares_shard_parameter": SHARD_QUERY_PARAMETER_NAME in query,
        "seeds_empty_shard_parameter": (
            parameters.get(SHARD_QUERY_PARAMETER_NAME) == []
        ),
        "has_empty_shard_guard": any(
            pattern in query for pattern in empty_guard_patterns
        ),
        "has_positive_shard_predicate": any(
            pattern in query for pattern in positive_predicate_patterns
        ),
    }


def _query_contract_gap(model_string, query_name, filename, code, message):
    return {
        "model": model_string,
        "query_name": query_name,
        "filename": filename,
        "code": code,
        "message": message,
        "remediation": _query_contract_gap_remediation(code),
    }


def _query_contract_gap_remediation(code: str) -> str:
    remediations = {
        "missing_builtin_query_map": (
            "Add a shipped query map for the model and publish it with "
            "forward_netbox_shard_keys support."
        ),
        "missing_shard_parameter_declaration": (
            "Declare `forward_netbox_shard_keys` in the query signature."
        ),
        "missing_shard_parameter_default": (
            "Seed `forward_netbox_shard_keys: []` in the query map parameters."
        ),
        "missing_empty_shard_guard": (
            "Keep no-parameter UI execution unfiltered with an empty-list guard."
        ),
        "missing_positive_shard_predicate": (
            "Use `forward_netbox_shard_keys` in a positive membership predicate."
        ),
    }
    return remediations.get(
        code,
        "Review the query contract and align the shipped map with the fetch contract.",
    )


def _build_builtin_query_spec(query_default: dict[str, Any]) -> QuerySpec:
    filename = query_default["filename"]
    return QuerySpec(
        model_string=query_default["model_string"],
        query_name=query_default["name"],
        query=_read_query(filename),
        built_in=True,
        contract_key=filename.removesuffix(".nqe"),
        full_query_source=_read_query(filename),
        variant=_query_contract_variant(filename),
        required_data_files=_query_contract_data_files(filename),
        reducer_id=_query_contract_reducer_id(filename),
        reducer_version=_query_contract_reducer_version(filename),
        diff_ownership_mode=_query_diff_ownership_mode(filename),
        parameters=_default_query_parameters(filename),
        coalesce_fields=tuple(
            tuple(field_set)
            for field_set in architecture_default_coalesce_fields_for_model(
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
            filename = query_default["filename"]
            full_query_source = _read_query(filename)
            contract_fields = {
                "map_id": query_map.pk,
                "map_weight": query_map.weight,
                "built_in": True,
                "contract_key": filename.removesuffix(".nqe"),
                "full_query_source": full_query_source,
                "resolved_query_path": query_map.query_path or None,
                "full_source_sha256": (
                    getattr(query_map, "full_source_sha256", "")
                    or query_source_sha256(full_query_source)
                ),
                "diff_commit_id": getattr(query_map, "diff_commit_id", "") or None,
                "diff_source_sha256": getattr(query_map, "diff_source_sha256", ""),
                "variant": _query_contract_variant(filename),
                "required_data_files": _query_contract_data_files(filename),
                "reducer_id": _query_contract_reducer_id(filename),
                "reducer_version": _query_contract_reducer_version(filename),
                "diff_ownership_mode": _query_diff_ownership_mode(filename),
            }
            if query_map.query_id:
                return QuerySpec(
                    model_string=query_map.model_string,
                    query_name=query_map.name,
                    query_id=query_map.query_id,
                    query_repository=query_map.query_repository or "org",
                    commit_id=query_map.commit_id or None,
                    parameters=_query_map_parameters(query_default, query_map),
                    coalesce_fields=tuple(
                        tuple(field_set) for field_set in normalized_coalesce
                    ),
                    placeholder=False,
                    **contract_fields,
                )
            if getattr(query_map, "query_path", ""):
                return QuerySpec(
                    model_string=query_map.model_string,
                    query_name=query_map.name,
                    query_repository=query_map.query_repository or "org",
                    query_path=query_map.query_path,
                    query_intent=_query_intent(query_default["filename"]) or None,
                    commit_id=query_map.commit_id or None,
                    parameters=_query_map_parameters(query_default, query_map),
                    coalesce_fields=tuple(
                        tuple(field_set) for field_set in normalized_coalesce
                    ),
                    placeholder=False,
                    **contract_fields,
                )
            return QuerySpec(
                model_string=query_map.model_string,
                query_name=query_map.name,
                query=_read_query(query_default["filename"]),
                parameters=_query_map_parameters(query_default, query_map),
                coalesce_fields=tuple(
                    tuple(field_set) for field_set in normalized_coalesce
                ),
                placeholder=False,
                **contract_fields,
            )
    query_id = query_map.query_id or None
    query_path = getattr(query_map, "query_path", "") or None
    resolved_query_path = query_path if query_id else None
    query_repository = getattr(query_map, "query_repository", "") or None
    if query_id:
        query_path = None
        query_repository = query_repository or "org"
    return QuerySpec(
        model_string=query_map.model_string,
        query_name=query_map.name,
        query=query_map.query or None,
        query_id=query_id,
        query_repository=query_repository,
        query_path=query_path,
        resolved_query_path=resolved_query_path,
        commit_id=query_map.commit_id or None,
        map_id=query_map.pk,
        map_weight=query_map.weight,
        built_in=False,
        full_query_source=query_map.query or None,
        full_source_sha256=getattr(query_map, "full_source_sha256", ""),
        diff_commit_id=getattr(query_map, "diff_commit_id", "") or None,
        diff_source_sha256=getattr(query_map, "diff_source_sha256", ""),
        parameters=query_map.parameters or {},
        coalesce_fields=tuple(tuple(field_set) for field_set in normalized_coalesce),
        placeholder=False,
    )


def _query_contract_variant(filename: str) -> str:
    if "with_netbox_aliases" in filename:
        return "aliases"
    if "with_rules" in filename:
        return "rules"
    return "base"


def _query_contract_data_files(filename: str) -> tuple[str, ...]:
    if "with_netbox_aliases" in filename:
        return ("netbox_device_type_aliases",)
    if "with_rules" in filename:
        return ("netbox_feature_tag_rules",)
    return ()


_TIER2_DEVICE_OWNED_QUERY_FILENAMES = {
    "forward_interfaces.nqe",
    "forward_inventory_items.nqe",
    "forward_modules.nqe",
    "forward_bgp_peers.nqe",
    "forward_bgp_address_families.nqe",
    "forward_bgp_peer_address_families.nqe",
    "forward_ospf_instances.nqe",
    "forward_ospf_interfaces.nqe",
}

_TIER3_REDUCERS_BY_QUERY_FILENAME = {
    "forward_locations.nqe": "tier3_locations",
    "forward_vlans.nqe": "tier3_vlans",
    "forward_vrfs.nqe": "tier3_vrfs",
    "forward_prefixes_ipv4.nqe": "tier3_prefixes",
    "forward_prefixes_ipv6.nqe": "tier3_prefixes",
    "forward_hsrp_groups.nqe": "tier3_hsrp_groups",
    "forward_mac_addresses.nqe": "tier3_mac_addresses",
    "forward_ip_addresses_ipv4.nqe": "tier3_ip_addresses",
    "forward_ip_addresses_ipv6.nqe": "tier3_ip_addresses",
    "forward_device_feature_tags.nqe": "tier3_device_feature_tags",
    "forward_device_feature_tags_with_rules.nqe": "tier3_device_feature_tags",
}

_FEATURE_STATE_FULL_ONLY_QUERY_FILENAMES = {
    "forward_devices.nqe",
    "forward_devices_with_netbox_aliases.nqe",
}


def _query_diff_ownership_mode(filename: str) -> str:
    if filename == "forward_inferred_interface_cables.nqe":
        return "cable_either_endpoint"
    if filename in _TIER2_DEVICE_OWNED_QUERY_FILENAMES:
        return "device"
    if filename in _TIER3_REDUCERS_BY_QUERY_FILENAME:
        return "contributor_relation"
    if filename in _FEATURE_STATE_FULL_ONLY_QUERY_FILENAMES:
        # Device revisions combine modeled devices with endpoint candidates.
        # Until exact collision and endpoint feature parity are live-proven,
        # neither the base nor aliases variant may enter the diff path.
        return "feature_state_full_only"
    if filename == "forward_aci_apic_cimc_inventory.nqe":
        # The parameterless draft exposes contributor selectors before a
        # select-distinct reduction. A changed contributor delta cannot prove
        # that an unchanged alternate controller does not preserve the final
        # inventory row, so this map must remain full-only.
        return "unsafe_contributor_reduction"
    return "global"


def _query_contract_reducer_id(filename: str) -> str:
    ownership_mode = _query_diff_ownership_mode(filename)
    if filename in _TIER3_REDUCERS_BY_QUERY_FILENAME:
        return _TIER3_REDUCERS_BY_QUERY_FILENAME[filename]
    if ownership_mode == "feature_state_full_only":
        return "full_only_feature_state"
    if ownership_mode == "device":
        return "tier2_side_local_device"
    if ownership_mode == "cable_either_endpoint":
        return "tier2_side_local_cable"
    if ownership_mode == "unsafe_contributor_reduction":
        return "full_only_contributor_reduction"
    return "direct_rows"


def _query_contract_reducer_version(filename: str) -> int:
    ownership_mode = _query_diff_ownership_mode(filename)
    if ownership_mode == "contributor_relation":
        return 1
    return 2 if ownership_mode != "global" else 1


def _unique_relocated_path_query(
    stored_path: str | None,
    candidates: list[tuple[str, dict]],
) -> dict | None:
    """Resolve one path preserved as a complete suffix under a new parent."""
    stored_parts = tuple(
        part for part in str(stored_path or "").strip("/").split("/") if part
    )
    if not stored_parts:
        return None
    relocated_matches = []
    for path, query in candidates:
        candidate_parts = tuple(
            part for part in str(path or "").strip("/").split("/") if part
        )
        if (
            len(candidate_parts) > len(stored_parts)
            and candidate_parts[-len(stored_parts) :] == stored_parts
        ):
            relocated_matches.append(query)
    return relocated_matches[0] if len(relocated_matches) == 1 else None


def indexed_query_for_spec(spec: QuerySpec, query_index: dict) -> dict | None:
    """Resolve a repository row using the fail-closed legacy binding order."""
    by_path = query_index.get("by_path") or {}
    indexed_query = by_path.get(spec.query_path)
    if not indexed_query:
        query_filename = str(spec.query_path or "").rstrip("/").rsplit("/", 1)[-1]
        moved_matches = [
            (path, query)
            for path, query in by_path.items()
            if str(path).rstrip("/").rsplit("/", 1)[-1] == query_filename
            and query.get("queryId")
        ]
        if len(moved_matches) == 1:
            indexed_query = moved_matches[0][1]
        elif moved_matches:
            indexed_query = _unique_relocated_path_query(
                spec.query_path,
                moved_matches,
            )
    if not indexed_query and spec.query_intent:
        normalized_intent = " ".join(spec.query_intent.split()).casefold()
        intent_matches = [
            (path, query)
            for path, query in by_path.items()
            if " ".join(str(query.get("intent") or "").split()).casefold()
            == normalized_intent
            and query.get("queryId")
        ]
        if len(intent_matches) == 1:
            indexed_query = intent_matches[0][1]
        elif intent_matches:
            indexed_query = _unique_relocated_path_query(
                spec.query_path,
                intent_matches,
            )
    return indexed_query


def _committed_source(query: dict[str, Any]) -> str | None:
    source = str(
        query.get("sourceCode") or query.get("source") or query.get("query") or ""
    )
    return source or None


def _history_commit_id(row: Any) -> str:
    if not isinstance(row, dict):
        return ""
    return str(
        row.get("id") or row.get("commitId") or row.get("lastCommitId") or ""
    ).strip()


def _resolve_unpinned_builtin_full_revision(
    spec: QuerySpec,
    client,
    *,
    preferred_commit_id: str = "",
) -> QuerySpec:
    """Resolve an unpinned built-in full contract to verified immutable source.

    Query heads can legitimately move to a parameterless diff/provenance
    revision. A full execution must therefore never inherit head merely from a
    query ID. Search immutable history and accept only exact shipped/persisted
    source with the declaration names the runtime contract can supply.
    """

    if not spec.built_in or not spec.run_query_id:
        return spec
    query_path = str(spec.resolved_query_path or spec.query_path or "").strip()
    if not query_path:
        return spec
    expected_source = spec.full_query_source or spec.query
    expected_source_hash = str(spec.full_source_sha256 or "").strip().lower()
    if not expected_source_hash:
        expected_source_hash = query_source_sha256(expected_source)
    expected_declarations = declared_query_parameters(expected_source)
    if not expected_source_hash or expected_declarations is None:
        return replace(
            spec,
            commit_id=(
                preferred_commit_id
                if preferred_commit_id and preferred_commit_id != "head"
                else spec.commit_id
            ),
        )
    expected_parameter_names = {parameter.name for parameter in expected_declarations}
    if expected_parameter_names != set(spec.parameters):
        return spec

    try:
        history = client.get_nqe_query_history(spec.run_query_id)
    except JobTimeoutException:
        raise
    except Exception:
        history = []
    if not isinstance(history, (list, tuple)):
        history = []

    candidate_commit_ids = []
    preferred_commit_id = str(preferred_commit_id or "").strip()
    if preferred_commit_id and preferred_commit_id != "head":
        candidate_commit_ids.append(preferred_commit_id)
    for row in reversed(history or []):
        commit_id = _history_commit_id(row)
        if commit_id and commit_id != "head" and commit_id not in candidate_commit_ids:
            candidate_commit_ids.append(commit_id)

    repository = str(spec.query_repository or "org").strip() or "org"
    expected_query_id = str(spec.run_query_id or "").strip()
    for commit_id in candidate_commit_ids:
        try:
            query = client.get_committed_nqe_query(
                repository=repository,
                query_path=query_path,
                commit_id=commit_id,
                require_source_code=True,
            )
        except JobTimeoutException:
            raise
        except Exception:
            continue
        if str(query.get("queryId") or "").strip() != expected_query_id:
            continue
        source = _committed_source(query)
        if not source or query_source_sha256(source) != expected_source_hash:
            continue
        declarations = declared_query_parameters(source)
        if declarations is None or {
            parameter.name for parameter in declarations
        } != set(spec.parameters):
            continue
        return replace(
            spec,
            commit_id=commit_id,
            query_repository=repository,
            resolved_query_path=query_path,
            full_query_source=source,
            full_source_sha256=expected_source_hash,
        )
    return replace(
        spec,
        commit_id=None,
        full_source_sha256=expected_source_hash,
    )


def _finalize_resolved_spec(
    spec: QuerySpec,
    client,
    *,
    full_was_unpinned: bool = False,
    preferred_commit_id: str = "",
) -> QuerySpec:
    if full_was_unpinned:
        spec = _resolve_unpinned_builtin_full_revision(
            spec,
            client,
            preferred_commit_id=preferred_commit_id,
        )
    return _hydrate_diff_contract_sources(spec, client)


def _hydrate_diff_contract_sources(spec: QuerySpec, client) -> QuerySpec:
    """Load immutable source evidence for persisted full and diff contracts.

    Persisted hashes are assertions, not substitutes for source verification.
    A lookup failure or query-identity mismatch leaves the sources unverified,
    which keeps that side of the resolved execution contract closed.
    """

    if not spec.run_query_id or not spec.commit_id or spec.commit_id == "head":
        return spec
    if spec.built_in and not spec.full_source_sha256:
        return spec
    if not spec.built_in and (not spec.query_id or not spec.resolved_query_path):
        return spec
    query_path = str(spec.resolved_query_path or spec.query_path or "").strip()
    if not query_path:
        try:
            history = client.get_nqe_query_history(spec.run_query_id)
        except JobTimeoutException:
            raise
        except Exception:
            history = []
        if not isinstance(history, (list, tuple)):
            history = []
        matching_paths = {
            str(row.get("path") or "").strip()
            for row in history or []
            if isinstance(row, dict)
            and _history_commit_id(row) == str(spec.commit_id)
            and str(row.get("path") or "").strip()
        }
        if len(matching_paths) == 1:
            query_path = matching_paths.pop()
    if not query_path:
        return spec
    repository = str(spec.query_repository or "org").strip() or "org"
    unverified = replace(
        spec,
        full_query_source=None,
        diff_query_source=None,
    )

    try:
        full_query = client.get_committed_nqe_query(
            repository=repository,
            query_path=query_path,
            commit_id=spec.commit_id,
            require_source_code=True,
        )
    except JobTimeoutException:
        raise
    except Exception:
        return unverified

    expected_query_id = str(spec.run_query_id or "").strip()
    if str(full_query.get("queryId") or "").strip() != expected_query_id:
        return unverified
    full_source = _committed_source(full_query)
    if not full_source:
        return unverified
    full_source_hash = str(spec.full_source_sha256 or "").strip().lower()
    if not full_source_hash and not spec.built_in:
        full_source_hash = query_source_sha256(full_source)
    if not full_source_hash:
        return unverified
    hydrated = replace(
        spec,
        query_repository=repository,
        resolved_query_path=query_path,
        full_query_source=full_source,
        full_source_sha256=full_source_hash,
    )
    if not spec.diff_commit_id:
        return hydrated

    try:
        diff_query = client.get_committed_nqe_query(
            repository=repository,
            query_path=query_path,
            commit_id=spec.diff_commit_id,
            require_source_code=True,
        )
    except JobTimeoutException:
        raise
    except Exception:
        return hydrated
    if str(diff_query.get("queryId") or "").strip() != expected_query_id:
        return hydrated
    diff_source = _committed_source(diff_query)
    if not diff_source:
        return hydrated
    diff_source_hash = str(spec.diff_source_sha256 or "").strip().lower()
    if not diff_source_hash and not spec.built_in:
        diff_source_hash = query_source_sha256(diff_source)
    if not diff_source_hash:
        return hydrated
    return replace(
        hydrated,
        diff_query_source=diff_source,
        diff_source_sha256=diff_source_hash,
    )


def resolve_query_specs_for_client(specs: list[QuerySpec], client) -> list[QuerySpec]:
    resolved_specs: list[QuerySpec] = []
    query_indexes: dict[str, dict] = {}
    resolved_query_cache: dict[tuple[str, str, str], tuple[str | None, str | None]] = {}
    for spec in specs:
        if not spec.query_path:
            resolved_specs.append(
                _finalize_resolved_spec(
                    spec,
                    client,
                    full_was_unpinned=(
                        bool(spec.run_query_id)
                        and str(spec.commit_id or "").strip() in ("", "head")
                    ),
                )
            )
            continue
        repository = spec.query_repository or "org"
        commit_id = str(spec.commit_id or "").strip()
        query_index = query_indexes.get(repository)
        if query_index is None:
            try:
                query_index = client.get_nqe_repository_query_index(
                    repository=repository,
                    directory="/",
                )
            except JobTimeoutException:
                raise
            except Exception:
                query_index = {}
            if not isinstance(query_index, dict):
                query_index = {"by_path": {}}
            query_indexes[repository] = query_index
        indexed_query = indexed_query_for_spec(spec, query_index)
        if indexed_query and indexed_query.get("queryId"):
            indexed_commit_id = str(
                indexed_query.get("commitId")
                or indexed_query.get("lastCommitId")
                or (indexed_query.get("lastCommit") or {}).get("id")
                or ""
            ).strip()
            resolved_commit_id = (
                commit_id
                if commit_id not in ("", "head")
                else indexed_commit_id or spec.commit_id
            )
            full_was_unpinned = spec.built_in and commit_id in ("", "head")
            resolved_specs.append(
                _finalize_resolved_spec(
                    replace(
                        spec,
                        resolved_query_id=str(
                            indexed_query.get("queryId") or ""
                        ).strip()
                        or None,
                        resolved_query_path=str(
                            indexed_query.get("path")
                            or spec.resolved_query_path
                            or spec.query_path
                            or ""
                        ).strip()
                        or None,
                        commit_id=(None if full_was_unpinned else resolved_commit_id),
                    ),
                    client,
                    full_was_unpinned=full_was_unpinned,
                    preferred_commit_id=indexed_commit_id,
                )
            )
            continue
        if commit_id in ("", "head"):
            cache_key = (repository, spec.query_path, "head")
            resolved_meta = resolved_query_cache.get(cache_key)
            if resolved_meta is None:
                resolved_query = client.get_committed_nqe_query(
                    repository=repository,
                    query_path=spec.query_path,
                    commit_id="head",
                    query_index=query_index,
                )
                resolved_meta = (
                    str(resolved_query.get("queryId") or "").strip() or None,
                    str(
                        resolved_query.get("commitId")
                        or resolved_query.get("lastCommitId")
                        or (resolved_query.get("lastCommit") or {}).get("id")
                        or ""
                    ).strip()
                    or None,
                )
                resolved_query_cache[cache_key] = resolved_meta
            resolved_query_id, resolved_commit_id = resolved_meta
            full_was_unpinned = spec.built_in
            resolved_specs.append(
                _finalize_resolved_spec(
                    replace(
                        spec,
                        resolved_query_id=resolved_query_id,
                        commit_id=(None if full_was_unpinned else resolved_commit_id),
                    ),
                    client,
                    full_was_unpinned=full_was_unpinned,
                    preferred_commit_id=resolved_commit_id or "",
                )
            )
            continue
        cache_key = (repository, spec.query_path, commit_id)
        resolved_meta = resolved_query_cache.get(cache_key)
        if resolved_meta is None:
            resolved_query = client.get_committed_nqe_query(
                repository=repository,
                query_path=spec.query_path,
                commit_id=commit_id,
            )
            resolved_meta = (
                str(resolved_query.get("queryId") or "").strip() or None,
                str(
                    resolved_query.get("commitId")
                    or resolved_query.get("lastCommitId")
                    or (resolved_query.get("lastCommit") or {}).get("id")
                    or ""
                ).strip()
                or None,
            )
            resolved_query_cache[cache_key] = resolved_meta
        resolved_query_id, resolved_commit_id = resolved_meta
        resolved_specs.append(
            _finalize_resolved_spec(
                replace(
                    spec,
                    resolved_query_id=resolved_query_id,
                    commit_id=resolved_commit_id or spec.commit_id,
                ),
                client,
            )
        )
    return ensure_unique_query_spec_executions(resolved_specs)


# Builtin map name suffix marking the "NetBox Device Type alias" variant of a
# base query (e.g. "Forward Devices" -> "Forward Devices with NetBox Device Type
# Aliases"). The alias variant maps device models through the NetBox Device Type
# Library; it is a drop-in replacement for the base query, not an addition.
_ALIAS_VARIANT_NAME_SUFFIX = " with NetBox Device Type Aliases"
_EXPLICIT_ALIAS_VARIANT_BASE_NAMES = {
    "Forward DLM Hardware Notices with NetBox Aliases": (
        "Forward DLM Hardware Notices"
    ),
}


def _collapse_alias_variant_duplicates(builtin_maps):
    """Run only one of each base/alias builtin pair.

    The base query and its ``*_with_netbox_aliases`` variant emit the SAME NetBox
    object from the same source rows but resolve ``device_type`` differently (raw
    PID vs Device-Type-Library alias). If both are enabled for a model, every sync
    reconciles the same device twice and flips its ``device_type`` FK between the
    two spellings — perpetual churn. When an alias variant is enabled, it
    supersedes its base counterpart; drop the base so exactly one runs.
    """
    superseded_base_names = {
        query_map.name[: -len(_ALIAS_VARIANT_NAME_SUFFIX)]
        for query_map in builtin_maps
        if query_map.name.endswith(_ALIAS_VARIANT_NAME_SUFFIX)
    }
    superseded_base_names.update(
        _EXPLICIT_ALIAS_VARIANT_BASE_NAMES[query_map.name]
        for query_map in builtin_maps
        if query_map.name in _EXPLICIT_ALIAS_VARIANT_BASE_NAMES
    )
    return [
        query_map
        for query_map in builtin_maps
        if query_map.name not in superseded_base_names
    ]


def _resolve_map_query_specs(model_string: str, maps) -> list[QuerySpec]:
    selected_maps = [
        query_map
        for query_map in maps or []
        if query_map.enabled and query_map.model_string == model_string
    ]
    custom_maps = [query_map for query_map in selected_maps if not query_map.built_in]
    builtin_maps = _collapse_alias_variant_duplicates(
        [query_map for query_map in selected_maps if query_map.built_in]
    )
    chosen_maps = custom_maps or builtin_maps
    return ensure_unique_query_spec_executions(
        [_build_query_spec_from_map(query_map) for query_map in chosen_maps]
    )


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
