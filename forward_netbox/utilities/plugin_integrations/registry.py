from dataclasses import asdict
from dataclasses import dataclass
from dataclasses import field


@dataclass(frozen=True)
class OptionalPluginIntegration:
    key: str
    app_label: str
    display_name: str
    required_models: tuple[str, ...]
    supported_models: tuple[str, ...]
    discovery_models: tuple[str, ...] = ()
    future_models: tuple[str, ...] = ()
    query_maps: tuple[str, ...] = ()
    enabled_by_default: bool = False
    status: str = "candidate"
    notes: tuple[str, ...] = field(default_factory=tuple)

    def as_dict(self):
        payload = asdict(self)
        for key, value in payload.items():
            if isinstance(value, tuple):
                payload[key] = list(value)
        return payload


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
    enabled_by_default=False,
    status="policy_write_path",
    notes=(
        "Writes proven fabric, pod, node, tenant, VRF, and filter rows in 1.3.2.",
        "Contract, BD, EPG, L3Out, and static binding maps are present but conservative until bounded source identity is proven.",
    ),
)


OPTIONAL_PLUGIN_INTEGRATIONS = (ACI_INTEGRATION,)


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
