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
    query_maps: tuple[str, ...] = ()
    command_inventory: tuple[dict, ...] = ()
    package_name: str = ""
    adapter_module: str = ""
    required_package_version: str = ""
    enabled_by_default: bool = False
    status: str = "supported"
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
        "netbox_cisco_aci.acifilter",
        "netbox_cisco_aci.acil3out",
    ),
    supported_models=(
        "netbox_cisco_aci.acifabric",
        "netbox_cisco_aci.acipod",
        "netbox_cisco_aci.acinode",
        "netbox_cisco_aci.acitenant",
        "netbox_cisco_aci.acivrf",
        "netbox_cisco_aci.acibridgedomain",
        "netbox_cisco_aci.acifilter",
        "netbox_cisco_aci.acil3out",
    ),
    native_models=("dcim.inventoryitem",),
    query_maps=(
        "Forward ACI Fabrics",
        "Forward ACI Pods",
        "Forward ACI Nodes",
        "Forward ACI APIC Nodes",
        "Forward ACI APIC CIMC Inventory",
        "Forward ACI Tenants",
        "Forward ACI VRFs",
        "Forward ACI Bridge Domains",
        "Forward ACI Filters",
        "Forward ACI L3Outs",
    ),
    package_name="netbox-cisco-aci",
    adapter_module="forward_netbox.utilities.sync_aci",
    required_package_version="0.4.0",
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
            "notes": (
                "Feeds APIC server CIMC inventory rows and custom-command checks.",
            ),
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
            "command_type": "CISCO_ACI_ZONING_FILTER",
            "source": "ACI zoning filter detail",
            "status": "current",
            "notes": (
                "Feeds current ACI filter discovery and anchors filter identity.",
            ),
        },
    ),
    enabled_by_default=False,
    status="supported",
    notes=(
        "Writes the declared ACI model set through tested adapters and exact query contracts.",
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
    package_name="netbox-routing",
    adapter_module="forward_netbox.utilities.sync_routing_impl",
    required_package_version="0.4.3",
    enabled_by_default=False,
    status="supported",
    notes=("Supported routing import backed by optional NetBox routing models.",),
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
    package_name="netbox-peering-manager",
    adapter_module="forward_netbox.utilities.sync_routing_impl",
    required_package_version="0.3.0",
    enabled_by_default=False,
    status="supported",
    notes=("Supported peering-session import backed by NetBox Peering Manager.",),
)


DLM_INTEGRATION = OptionalPluginIntegration(
    key="lifecycle.netbox_dlm",
    app_label="netbox_dlm",
    display_name="NetBox Device Lifecycle Management",
    required_models=(
        "netbox_dlm.softwareversion",
        "netbox_dlm.hardwarenotice",
        "netbox_dlm.devicesoftware",
        "netbox_dlm.cve",
        "netbox_dlm.vulnerability",
    ),
    supported_models=(
        "netbox_dlm.softwareversion",
        "netbox_dlm.hardwarenotice",
        "netbox_dlm.devicesoftware",
        "netbox_dlm.cve",
        "netbox_dlm.vulnerability",
    ),
    query_maps=(
        "Forward DLM Software Versions",
        "Forward DLM Hardware Notices",
        "Forward DLM Hardware Notices with NetBox Aliases",
        "Forward DLM Device Software",
        "Forward DLM CVEs",
        "Forward DLM Vulnerabilities",
    ),
    package_name="netbox-dlm",
    adapter_module="forward_netbox.utilities.sync_dlm",
    required_package_version="0.4.1",
    enabled_by_default=False,
    status="supported",
    notes=(
        "Supported lifecycle import backed by the optional netbox-dlm "
        "plugin: OS end-of-life dates per (platform, version), hardware "
        "end-of-life notices per device type, each device's running "
        "software version, the CVE catalog, and per-device CVE "
        "vulnerabilities from Forward's support and security analysis.",
        "Run `manage.py migrate` after installing or upgrading netbox-dlm.",
    ),
)


OPTIONAL_PLUGIN_INTEGRATIONS = (
    ROUTING_INTEGRATION,
    PEERING_INTEGRATION,
    ACI_INTEGRATION,
    DLM_INTEGRATION,
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


def integration_capability(integration: OptionalPluginIntegration):
    """Return the authoritative runtime availability contract for one plugin."""
    return _integration_capability_summary(integration)


def integration_adapter_contract_summary():
    return {
        integration.key: _integration_adapter_contract_summary(integration)
        for integration in OPTIONAL_PLUGIN_INTEGRATIONS
    }


def _integration_capability_summary(integration: OptionalPluginIntegration):
    installed_version = _integration_package_version(integration.package_name)
    required_models_present = _present_models(integration.required_models)
    required_models_missing = _missing_models(integration.required_models)
    supported_models_present = _present_models(integration.supported_models)
    supported_models_missing = _missing_models(integration.supported_models)
    availability_status = _integration_availability_status(
        installed=apps.is_installed(integration.app_label),
        missing_required=required_models_missing,
        package_metadata_available=installed_version is not None,
        version_matches=_version_matches(
            installed_version,
            integration.required_package_version,
        ),
    )
    return {
        "app_label": integration.app_label,
        "display_name": integration.display_name,
        "installed": apps.is_installed(integration.app_label),
        "available": availability_status == "available",
        "availability_status": availability_status,
        "availability_reason": _integration_availability_reason(
            availability_status,
            integration.package_name,
            integration.required_package_version,
        ),
        "package_name": integration.package_name,
        "version": installed_version,
        "required_version": integration.required_package_version,
        "version_matches": _version_matches(
            installed_version,
            integration.required_package_version,
        ),
        "required_model_count": len(integration.required_models),
        "supported_model_count": len(integration.supported_models),
        "native_model_count": len(integration.native_models),
        "query_map_count": len(integration.query_maps),
        "command_inventory_count": len(integration.command_inventory),
        "command_inventory": list(integration.command_inventory),
        "adapter_contract": _integration_adapter_contract_summary(integration),
        "required_models_present": required_models_present,
        "required_models_missing": required_models_missing,
        "supported_models_present": supported_models_present,
        "supported_models_missing": supported_models_missing,
        "native_models": sorted(integration.native_models),
        "missing_required": required_models_missing,
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


def _integration_package_version(package_name: str) -> str | None:
    try:
        return metadata.version(package_name)
    except metadata.PackageNotFoundError:
        return None


def _version_matches(
    installed_version: str | None,
    required_version: str,
) -> bool:
    if not installed_version or not required_version:
        return False
    try:
        return Version(installed_version) == Version(required_version)
    except InvalidVersion:
        return False


def _integration_availability_status(
    *,
    installed: bool,
    missing_required: list[str],
    package_metadata_available: bool,
    version_matches: bool,
) -> str:
    if not installed:
        return "not_installed"
    if missing_required:
        return "missing_required_models"
    if not package_metadata_available:
        return "package_metadata_unavailable"
    if not version_matches:
        return "unsupported_version"
    return "available"


def _integration_availability_reason(
    availability_status: str,
    package_name: str,
    required_version: str,
) -> str:
    if availability_status == "not_installed":
        return "Target plugin app is not installed."
    if availability_status == "missing_required_models":
        return "One or more required plugin models are missing."
    if availability_status == "package_metadata_unavailable":
        return f"Canonical package metadata is unavailable for {package_name}."
    if availability_status == "unsupported_version":
        return f"Installed plugin version must equal {required_version}."
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
