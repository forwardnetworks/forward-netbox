from dataclasses import asdict
from dataclasses import dataclass
from dataclasses import field
from importlib import import_module
from importlib import metadata

from django.apps import apps
from django.contrib.contenttypes.models import ContentType
from packaging.version import InvalidVersion
from packaging.version import Version


@dataclass(frozen=True)
class OptionalPluginIntegration:
    key: str
    app_label: str
    display_name: str
    required_models: tuple[str, ...]
    supported_models: tuple[str, ...]
    native_models: tuple[str, ...] = ()
    discovery_models: tuple[str, ...] = ()
    future_models: tuple[str, ...] = ()
    query_maps: tuple[str, ...] = ()
    command_inventory: tuple[dict, ...] = ()
    package_names: tuple[str, ...] = ()
    adapter_module: str = ""
    minimum_package_version: str | None = None
    enabled_by_default: bool = False
    status: str = "candidate"
    notes: tuple[str, ...] = field(default_factory=tuple)

    def as_dict(self):
        payload = asdict(self)
        return _json_safe(payload)


ACI_INTEGRATION = OptionalPluginIntegration(
    key="aci.netbox_cisco_aci",
    app_label="netbox_cisco_aci",
    display_name="Cisco ACI",
    required_models=(
        "netbox_cisco_aci.acifabric",
        "netbox_cisco_aci.acipod",
        "netbox_cisco_aci.acinode",
        "netbox_cisco_aci.acitenant",
        "netbox_cisco_aci.acivrf",
        "netbox_cisco_aci.acibridgedomain",
        "netbox_cisco_aci.aciappprofile",
        "netbox_cisco_aci.aciendpointgroup",
        "netbox_cisco_aci.acicontract",
        "netbox_cisco_aci.acifilter",
        "netbox_cisco_aci.acil3out",
        "netbox_cisco_aci.acistaticportbinding",
    ),
    supported_models=(
        "netbox_cisco_aci.acifabric",
        "netbox_cisco_aci.acipod",
        "netbox_cisco_aci.acinode",
        "netbox_cisco_aci.acitenant",
        "netbox_cisco_aci.acivrf",
        "netbox_cisco_aci.acibridgedomain",
        "netbox_cisco_aci.aciappprofile",
        "netbox_cisco_aci.aciendpointgroup",
        "netbox_cisco_aci.acicontract",
        "netbox_cisco_aci.acifilter",
        "netbox_cisco_aci.acil3out",
        "netbox_cisco_aci.acistaticportbinding",
    ),
    native_models=("dcim.inventoryitem",),
    discovery_models=(
        "netbox_cisco_aci.acibridgedomainsubnet",
        "netbox_cisco_aci.aciendpointsecuritygroup",
        "netbox_cisco_aci.acifilterentry",
        "netbox_cisco_aci.acisubject",
        "netbox_cisco_aci.acisubjectfilter",
        "netbox_cisco_aci.acicontractrelation",
        "netbox_cisco_aci.acilogicalnodeprofile",
        "netbox_cisco_aci.acilogicalnode",
        "netbox_cisco_aci.acilogicalinterfaceprofile",
        "netbox_cisco_aci.acil3outinterface",
    ),
    future_models=(
        "netbox_cisco_aci.aciinterfacefabricmembership",
        "netbox_cisco_aci.acivpcbindingpair",
        "netbox_cisco_aci.acidomain",
        "netbox_cisco_aci.acivlanpool",
        "netbox_cisco_aci.aciaaep",
    ),
    query_maps=(
        "Forward ACI Fabrics",
        "Forward ACI Pods",
        "Forward ACI Nodes",
        "Forward ACI APIC Nodes",
        "Forward ACI APIC CIMC Inventory",
        "Forward ACI Tenants",
        "Forward ACI VRFs",
        "Forward ACI Bridge Domains",
        "Forward ACI Application Profiles",
        "Forward ACI Endpoint Groups",
        "Forward ACI Contracts",
        "Forward ACI Filters",
        "Forward ACI L3Outs",
        "Forward ACI Static Port Bindings",
    ),
    package_names=(
        "netbox-cisco-aci",
        "netbox_aci_plugin",
        "netbox-aci-plugin",
        "netbox-aci",
    ),
    adapter_module="forward_netbox.utilities.sync_aci",
    minimum_package_version="0.2.2",
    command_inventory=(
        {
            "command_type": "CISCO_APIC_SWITCH",
            "source": "APIC switch detail",
            "status": "current",
            "notes": ("Feeds APIC node discovery rows.",),
        },
        {
            "command_type": "CISCO_APIC_CONTROLLER_DETAIL",
            "source": "APIC controller detail",
            "status": "current",
            "notes": ("Feeds APIC controller discovery rows.",),
        },
        {
            "command_type": "CUSTOM",
            "source": "APIC custom command: moquery -c eqptCh -a all",
            "status": "current",
            "notes": ("Feeds APIC server CIMC inventory rows.",),
        },
        {
            "command_type": "CISCO_ACI_FABRIC_NODES",
            "source": "ACI fabric node detail",
            "status": "current",
            "notes": ("Feeds current ACI node and pod discovery maps.",),
        },
        {
            "command_type": "CISCO_ACI_FABRIC_VRFS",
            "source": "ACI fabric VRF detail",
            "status": "current",
            "notes": ("Feeds current ACI tenant and VRF discovery maps.",),
        },
        {
            "command_type": "CISCO_ACI_NODE_TYPE",
            "source": "ACI node type detail",
            "status": "future",
            "notes": ("Discovery candidate for leaf/spine role classification."),
        },
        {
            "command_type": "CISCO_ACI_SPINE_INTERFACE_MODE",
            "source": "ACI spine interface mode detail",
            "status": "future",
            "notes": ("Discovery candidate for spine interface role inference."),
        },
        {
            "command_type": "CISCO_ACI_SPINE_IP_ENDPOINTS",
            "source": "ACI spine IP endpoint detail",
            "status": "future",
            "notes": ("Discovery candidate for spine endpoint presence and addresses."),
        },
        {
            "command_type": "CISCO_ACI_SPINE_TUNNELS_NEXTHOP",
            "source": "ACI spine tunnel nexthop detail",
            "status": "future",
            "notes": (
                "Discovery candidate for tunnel nexthop presence and reachability."
            ),
        },
        {
            "command_type": "CISCO_ACI_TUNNELS_ENDPOINTS",
            "source": "ACI tunnel endpoint detail",
            "status": "future",
            "notes": (
                "Discovery candidate for tunnel endpoint inventory and presence."
            ),
        },
        {
            "command_type": "CISCO_ACI_ZONING_RULE",
            "source": "ACI zoning rule detail",
            "status": "future",
            "notes": ("Discovery candidate for policy observation and parser proof.",),
        },
        {
            "command_type": "CISCO_ACI_ZONING_FILTER",
            "source": "ACI zoning filter detail",
            "status": "current",
            "notes": (
                "Feeds current ACI filter discovery and anchors filter identity.",
            ),
        },
        {
            "command_type": "CISCO_ACI_EPM_ENDPOINTS",
            "source": "ACI endpoint table detail",
            "status": "future",
            "notes": ("Discovery candidate for endpoint and EPG-related inventory."),
        },
        {
            "command_type": "CISCO_APIC_VLAN_LEAF",
            "source": "APIC VLAN leaf detail",
            "status": "future",
            "notes": ("Discovery candidate for VLAN / bridge-domain / EPG inventory.",),
        },
        {
            "command_type": "CISCO_APIC_EXT_L3OUT_INTERFACES",
            "source": "APIC external L3Out interface detail",
            "status": "future",
            "notes": ("Discovery candidate for external L3Out interface inventory."),
        },
        {
            "command_type": "CISCO_APIC_VLAN_STATUS_LEAF",
            "source": "APIC VLAN status detail",
            "status": "future",
            "notes": ("Discovery candidate for VLAN state and presence reporting.",),
        },
        {
            "command_type": "CISCO_APIC_VPC_MAP",
            "source": "APIC vPC map detail",
            "status": "future",
            "notes": (
                "Discovery candidate for vPC topology presence and node mapping."
            ),
        },
    ),
    enabled_by_default=False,
    status="policy_write_path",
    notes=(
        "Writes proven fabric, pod, node, tenant, VRF, and filter rows in 1.3.2.",
        "Contract, BD, EPG, L3Out, and static binding maps are present but conservative until bounded source identity is proven.",
    ),
)


ROUTING_INTEGRATION = OptionalPluginIntegration(
    key="routing.netbox_routing",
    app_label="netbox_routing",
    display_name="NetBox Routing",
    required_models=(
        "netbox_routing.bgprouter",
        "netbox_routing.bgpscope",
        "netbox_routing.bgppeer",
        "netbox_routing.bgpaddressfamily",
        "netbox_routing.bgppeeraddressfamily",
        "netbox_routing.ospfinstance",
        "netbox_routing.ospfarea",
        "netbox_routing.ospfinterface",
    ),
    supported_models=(
        "netbox_routing.bgppeer",
        "netbox_routing.bgpaddressfamily",
        "netbox_routing.bgppeeraddressfamily",
        "netbox_routing.ospfinstance",
        "netbox_routing.ospfarea",
        "netbox_routing.ospfinterface",
    ),
    query_maps=(
        "Forward BGP Peers",
        "Forward BGP Address Families",
        "Forward BGP Peer Address Families",
        "Forward OSPF Instances",
        "Forward OSPF Areas",
        "Forward OSPF Interfaces",
    ),
    package_names=("netbox-routing", "netbox_routing"),
    adapter_module="forward_netbox.utilities.sync_routing_impl",
    enabled_by_default=False,
    status="beta_surface",
    notes=(
        "Beta routing import surface backed by optional NetBox routing models.",
        "The registry reports capability and query-contract coverage only; routing "
        "behavior continues to live in the dedicated sync adapters.",
    ),
)


PEERING_INTEGRATION = OptionalPluginIntegration(
    key="peering.netbox_peering_manager",
    app_label="netbox_peering_manager",
    display_name="NetBox Peering Manager",
    required_models=(
        "netbox_peering_manager.relationship",
        "netbox_peering_manager.peeringsession",
    ),
    supported_models=("netbox_peering_manager.peeringsession",),
    query_maps=("Forward Peering Sessions",),
    package_names=("netbox-peering-manager", "netbox_peering_manager"),
    adapter_module="forward_netbox.utilities.sync_routing_impl",
    enabled_by_default=False,
    status="beta_surface",
    notes=(
        "Optional peering overlay backed by netbox-routing.",
        "The registry reports capability and query-contract coverage only; the "
        "session adapter remains in the routing sync path.",
    ),
)


OPTIONAL_PLUGIN_INTEGRATIONS = (
    ROUTING_INTEGRATION,
    PEERING_INTEGRATION,
    ACI_INTEGRATION,
)


def iter_integrations():
    return iter(OPTIONAL_PLUGIN_INTEGRATIONS)


def optional_integration_for_model(model_string: str):
    model_string = str(model_string or "")
    for integration in OPTIONAL_PLUGIN_INTEGRATIONS:
        if model_string in integration.supported_models:
            return integration
    return None


def integration_summary():
    return {
        integration.key: integration.as_dict()
        for integration in OPTIONAL_PLUGIN_INTEGRATIONS
    }


def integration_capability_summary():
    return {
        integration.key: _integration_capability_summary(integration)
        for integration in OPTIONAL_PLUGIN_INTEGRATIONS
    }


def integration_adapter_contract_summary():
    return {
        integration.key: _integration_adapter_contract_summary(integration)
        for integration in OPTIONAL_PLUGIN_INTEGRATIONS
    }


def _integration_capability_summary(integration: OptionalPluginIntegration):
    detected_package_name, installed_version = _integration_package_version(
        integration.package_names
    )
    required_models_present = _present_models(integration.required_models)
    required_models_missing = _missing_models(integration.required_models)
    supported_models_present = _present_models(integration.supported_models)
    supported_models_missing = _missing_models(integration.supported_models)
    discovery_models_present = _present_models(integration.discovery_models)
    discovery_models_missing = _missing_models(integration.discovery_models)
    future_models_present = _present_models(integration.future_models)
    future_models_missing = _missing_models(integration.future_models)
    missing_optional = sorted(
        set(discovery_models_missing).union(future_models_missing)
    )
    availability_status = _integration_availability_status(
        installed=apps.is_installed(integration.app_label),
        missing_required=required_models_missing,
        unsupported_version=_is_unsupported_version(
            installed_version, integration.minimum_package_version
        ),
    )
    return {
        "app_label": integration.app_label,
        "display_name": integration.display_name,
        "installed": apps.is_installed(integration.app_label),
        "available": availability_status == "available",
        "availability_status": availability_status,
        "availability_reason": _integration_availability_reason(
            availability_status, integration.minimum_package_version
        ),
        "package_names": integration.package_names,
        "installed_package_name": detected_package_name,
        "version": installed_version,
        "minimum_version": integration.minimum_package_version,
        "unsupported_version": _is_unsupported_version(
            installed_version, integration.minimum_package_version
        ),
        "required_model_count": len(integration.required_models),
        "supported_model_count": len(integration.supported_models),
        "native_model_count": len(integration.native_models),
        "discovery_model_count": len(integration.discovery_models),
        "future_model_count": len(integration.future_models),
        "query_map_count": len(integration.query_maps),
        "command_inventory_count": len(integration.command_inventory),
        "command_inventory": list(integration.command_inventory),
        "adapter_contract": _integration_adapter_contract_summary(integration),
        "required_models_present": required_models_present,
        "required_models_missing": required_models_missing,
        "supported_models_present": supported_models_present,
        "supported_models_missing": supported_models_missing,
        "native_models": sorted(integration.native_models),
        "discovery_models_present": discovery_models_present,
        "discovery_models_missing": discovery_models_missing,
        "future_models_present": future_models_present,
        "future_models_missing": future_models_missing,
        "missing_required": required_models_missing,
        "missing_optional": missing_optional,
    }


def _integration_adapter_contract_summary(integration: OptionalPluginIntegration):
    models = list(integration.supported_models)
    if not integration.adapter_module:
        return {
            "available": False,
            "status": "missing_adapter_module",
            "adapter_module": "",
            "model_count": len(models),
            "models": [],
            "gaps": [
                {
                    "code": "missing_adapter_module",
                    "message": "Integration does not declare an adapter module.",
                }
            ],
        }
    try:
        module = import_module(integration.adapter_module)
    except Exception as exc:
        return {
            "available": False,
            "status": "adapter_module_import_failed",
            "adapter_module": integration.adapter_module,
            "model_count": len(models),
            "models": [],
            "gaps": [
                {
                    "code": "adapter_module_import_failed",
                    "message": f"Could not import adapter module: {exc}",
                }
            ],
        }
    entries = []
    gaps = []
    for model_string in models:
        function_suffix = model_string.replace(".", "_")
        apply_name = f"apply_{function_suffix}"
        delete_name = f"delete_{function_suffix}"
        has_apply = callable(getattr(module, apply_name, None))
        has_delete = callable(getattr(module, delete_name, None))
        entry = {
            "model": model_string,
            "adapter_module": integration.adapter_module,
            "apply_function": apply_name,
            "delete_function": delete_name,
            "has_apply": has_apply,
            "has_delete": has_delete,
            "status": "pass" if has_apply and has_delete else "fail",
        }
        entries.append(entry)
        if not has_apply:
            gaps.append(
                {
                    "code": "missing_apply_adapter",
                    "model": model_string,
                    "adapter_module": integration.adapter_module,
                    "function": apply_name,
                }
            )
        if not has_delete:
            gaps.append(
                {
                    "code": "missing_delete_adapter",
                    "model": model_string,
                    "adapter_module": integration.adapter_module,
                    "function": delete_name,
                }
            )
    return {
        "available": True,
        "status": "pass" if not gaps else "fail",
        "adapter_module": integration.adapter_module,
        "model_count": len(entries),
        "models": entries,
        "gaps": gaps,
    }


def _integration_package_version(
    package_names: tuple[str, ...],
) -> tuple[str | None, str | None]:
    for package_name in package_names:
        try:
            return package_name, metadata.version(package_name)
        except metadata.PackageNotFoundError:
            continue
    return None, None


def _is_unsupported_version(
    installed_version: str | None,
    minimum_version: str | None,
) -> bool:
    if not installed_version or not minimum_version:
        return False
    try:
        return Version(installed_version) < Version(minimum_version)
    except InvalidVersion:
        return False


def _integration_availability_status(
    *, installed: bool, missing_required: list[str], unsupported_version: bool
) -> str:
    if not installed:
        return "not_installed"
    if missing_required:
        return "missing_required_models"
    if unsupported_version:
        return "unsupported_version"
    return "available"


def _integration_availability_reason(
    availability_status: str, minimum_version: str | None
) -> str:
    if availability_status == "not_installed":
        return "Target plugin app is not installed."
    if availability_status == "missing_required_models":
        return "One or more required plugin models are missing."
    if availability_status == "unsupported_version":
        if minimum_version:
            return f"Installed plugin version is below the supported minimum {minimum_version}."
        return "Installed plugin version is below the supported minimum."
    return "Plugin capability is available."


def _present_models(model_strings):
    return sorted(
        model_string
        for model_string in model_strings
        if _content_type_exists(model_string)
    )


def _missing_models(model_strings):
    return sorted(
        model_string
        for model_string in model_strings
        if not _content_type_exists(model_string)
    )


def _content_type_exists(model_string: str) -> bool:
    try:
        app_label, model = model_string.split(".", 1)
    except ValueError:
        return False
    return ContentType.objects.filter(app_label=app_label, model=model).exists()


def _json_safe(value):
    if isinstance(value, tuple):
        return [_json_safe(item) for item in value]
    if isinstance(value, list):
        return [_json_safe(item) for item in value]
    if isinstance(value, dict):
        return {key: _json_safe(item) for key, item in value.items()}
    return value
