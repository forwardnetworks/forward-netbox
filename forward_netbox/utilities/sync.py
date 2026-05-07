import logging

from ..exceptions import ForwardQueryError
from .module_readiness import module_bay_import_row
from .sync_cable import apply_dcim_cable
from .sync_cable import delete_dcim_cable
from .sync_cable import lookup_cable_between
from .sync_contracts import canonical_cable_endpoint_identity
from .sync_core_models import apply_dcim_devicerole
from .sync_core_models import apply_dcim_devicetype
from .sync_core_models import apply_dcim_manufacturer
from .sync_core_models import apply_dcim_platform
from .sync_core_models import apply_dcim_site
from .sync_core_models import delete_dcim_devicerole
from .sync_core_models import delete_dcim_devicetype
from .sync_core_models import delete_dcim_manufacturer
from .sync_core_models import delete_dcim_platform
from .sync_core_models import delete_dcim_site
from .sync_device import apply_dcim_device
from .sync_device import apply_dcim_virtualchassis
from .sync_device import delete_dcim_device
from .sync_device import delete_dcim_virtualchassis
from .sync_events import EventsClearer
from .sync_execution import run_sync_stage
from .sync_interface import apply_dcim_interface
from .sync_interface import apply_dcim_macaddress
from .sync_interface import apply_extras_taggeditem
from .sync_interface import delete_dcim_interface
from .sync_interface import delete_dcim_macaddress
from .sync_interface import delete_extras_taggeditem
from .sync_inventory_module import apply_dcim_inventoryitem
from .sync_inventory_module import apply_dcim_module
from .sync_inventory_module import delete_dcim_inventoryitem
from .sync_inventory_module import delete_dcim_module
from .sync_ipam import apply_ipam_ipaddress
from .sync_ipam import apply_ipam_prefix
from .sync_ipam import apply_ipam_vlan
from .sync_ipam import apply_ipam_vrf
from .sync_ipam import delete_ipam_ipaddress
from .sync_ipam import delete_ipam_prefix
from .sync_ipam import delete_ipam_vlan
from .sync_ipam import delete_ipam_vrf
from .sync_primitives import coalesce_lookup as sync_coalesce_lookup
from .sync_primitives import coalesce_sets_for as sync_coalesce_sets_for
from .sync_primitives import coalesce_update_or_create as sync_coalesce_update_or_create
from .sync_primitives import coalesce_upsert as sync_coalesce_upsert
from .sync_primitives import content_type_for as sync_content_type_for
from .sync_primitives import delete_by_coalesce as sync_delete_by_coalesce
from .sync_primitives import get_unique_or_raise as sync_get_unique_or_raise
from .sync_primitives import lookup_interface as sync_lookup_interface
from .sync_primitives import lookup_module_bay as sync_lookup_module_bay
from .sync_primitives import model_field_values as sync_model_field_values
from .sync_primitives import optional_model as sync_optional_model
from .sync_primitives import update_existing_or_create as sync_update_existing_or_create
from .sync_primitives import upsert_row as sync_upsert_row
from .sync_primitives import upsert_row_from_defaults as sync_upsert_row_from_defaults
from .sync_primitives import (
    upsert_values_from_defaults as sync_upsert_values_from_defaults,
)
from .sync_reporting import apply_model_rows as apply_sync_model_rows
from .sync_reporting import delete_model_rows as delete_sync_model_rows
from .sync_reporting import dependency_failed as sync_dependency_failed
from .sync_reporting import dependency_key as sync_dependency_key
from .sync_reporting import (
    emit_aggregated_conflict_warning_summaries as sync_emit_aggregated_conflict_warning_summaries,
)
from .sync_reporting import (
    emit_aggregated_skip_warning_summaries as sync_emit_aggregated_skip_warning_summaries,
)
from .sync_reporting import (
    ipaddress_assignment_skip_reason as sync_ipaddress_assignment_skip_reason,
)
from .sync_reporting import mark_dependency_failed as sync_mark_dependency_failed
from .sync_reporting import (
    record_aggregated_conflict_warning as sync_record_aggregated_conflict_warning,
)
from .sync_reporting import (
    record_aggregated_skip_warning as sync_record_aggregated_skip_warning,
)
from .sync_reporting import record_issue as sync_record_issue
from .sync_routing import apply_netbox_peering_manager_peeringsession
from .sync_routing import apply_netbox_routing_bgpaddressfamily
from .sync_routing import apply_netbox_routing_bgppeer
from .sync_routing import apply_netbox_routing_bgppeeraddressfamily
from .sync_routing import apply_netbox_routing_ospfarea
from .sync_routing import apply_netbox_routing_ospfinstance
from .sync_routing import apply_netbox_routing_ospfinterface
from .sync_routing import bgp_address_family_comments
from .sync_routing import bgp_peer_address_family_comments
from .sync_routing import bgp_peer_comments
from .sync_routing import bgp_peer_name
from .sync_routing import bgp_peer_values
from .sync_routing import bgp_vrf
from .sync_routing import delete_netbox_peering_manager_peeringsession
from .sync_routing import delete_netbox_routing_bgpaddressfamily
from .sync_routing import delete_netbox_routing_bgppeer
from .sync_routing import delete_netbox_routing_bgppeeraddressfamily
from .sync_routing import delete_netbox_routing_ospfarea
from .sync_routing import delete_netbox_routing_ospfinstance
from .sync_routing import delete_netbox_routing_ospfinterface
from .sync_routing import ensure_bgp_address_family
from .sync_routing import ensure_bgp_peer_address_family
from .sync_routing import ensure_bgp_peer_ip
from .sync_routing import ensure_bgp_router
from .sync_routing import ensure_bgp_scope
from .sync_routing import ensure_bgp_scope_for_row
from .sync_routing import ensure_netbox_routing_bgppeer
from .sync_routing import ensure_ospf_area
from .sync_routing import ensure_ospf_instance
from .sync_routing import ensure_ospf_interface
from .sync_routing import ensure_peering_relationship
from .sync_routing import host_address
from .sync_routing import lookup_device_for_routing
from .sync_routing import lookup_ipaddress_by_host
from .sync_routing import normalize_bgp_address_family
from .sync_routing import ospf_area_type
from .sync_routing import ospf_instance_comments
from .sync_routing import ospf_interface_comments
from .sync_routing import ospf_process_values
from .sync_routing import resolve_bgp_address_family_for_delete
from .sync_routing import resolve_bgp_peer_for_delete
from .sync_routing import resolve_bgp_scope_for_delete
from .sync_routing import rib_presence_label
from .sync_routing import routing_vrf
from .sync_runner_adapters import ForwardSyncRunnerAdapterMixin
from .sync_runner_contracts import ForwardSyncRunnerContractMixin


_SYNC_RUNNER_IMPORT_ANCHORS = (
    logging,
    ForwardQueryError,
    module_bay_import_row,
    apply_dcim_cable,
    delete_dcim_cable,
    lookup_cable_between,
    canonical_cable_endpoint_identity,
    apply_dcim_devicerole,
    apply_dcim_devicetype,
    apply_dcim_manufacturer,
    apply_dcim_platform,
    apply_dcim_site,
    delete_dcim_devicerole,
    delete_dcim_devicetype,
    delete_dcim_manufacturer,
    delete_dcim_platform,
    delete_dcim_site,
    apply_dcim_device,
    apply_dcim_virtualchassis,
    delete_dcim_device,
    delete_dcim_virtualchassis,
    EventsClearer,
    run_sync_stage,
    apply_dcim_interface,
    apply_dcim_macaddress,
    apply_extras_taggeditem,
    delete_dcim_interface,
    delete_dcim_macaddress,
    delete_extras_taggeditem,
    apply_dcim_inventoryitem,
    apply_dcim_module,
    delete_dcim_inventoryitem,
    delete_dcim_module,
    apply_ipam_ipaddress,
    apply_ipam_prefix,
    apply_ipam_vlan,
    apply_ipam_vrf,
    delete_ipam_ipaddress,
    delete_ipam_prefix,
    delete_ipam_vlan,
    delete_ipam_vrf,
    sync_coalesce_lookup,
    sync_coalesce_sets_for,
    sync_coalesce_update_or_create,
    sync_coalesce_upsert,
    sync_content_type_for,
    sync_delete_by_coalesce,
    sync_get_unique_or_raise,
    sync_lookup_interface,
    sync_lookup_module_bay,
    sync_model_field_values,
    sync_optional_model,
    sync_update_existing_or_create,
    sync_upsert_row,
    sync_upsert_row_from_defaults,
    sync_upsert_values_from_defaults,
    apply_sync_model_rows,
    delete_sync_model_rows,
    sync_dependency_failed,
    sync_dependency_key,
    sync_emit_aggregated_conflict_warning_summaries,
    sync_emit_aggregated_skip_warning_summaries,
    sync_ipaddress_assignment_skip_reason,
    sync_mark_dependency_failed,
    sync_record_aggregated_conflict_warning,
    sync_record_aggregated_skip_warning,
    sync_record_issue,
    apply_netbox_peering_manager_peeringsession,
    apply_netbox_routing_bgpaddressfamily,
    apply_netbox_routing_bgppeer,
    apply_netbox_routing_bgppeeraddressfamily,
    apply_netbox_routing_ospfarea,
    apply_netbox_routing_ospfinstance,
    apply_netbox_routing_ospfinterface,
    bgp_address_family_comments,
    bgp_peer_address_family_comments,
    bgp_peer_comments,
    bgp_peer_name,
    bgp_peer_values,
    bgp_vrf,
    delete_netbox_peering_manager_peeringsession,
    delete_netbox_routing_bgpaddressfamily,
    delete_netbox_routing_bgppeer,
    delete_netbox_routing_bgppeeraddressfamily,
    delete_netbox_routing_ospfarea,
    delete_netbox_routing_ospfinstance,
    delete_netbox_routing_ospfinterface,
    ensure_bgp_address_family,
    ensure_bgp_peer_address_family,
    ensure_bgp_peer_ip,
    ensure_bgp_router,
    ensure_bgp_scope,
    ensure_bgp_scope_for_row,
    ensure_netbox_routing_bgppeer,
    ensure_ospf_area,
    ensure_ospf_instance,
    ensure_ospf_interface,
    ensure_peering_relationship,
    host_address,
    lookup_device_for_routing,
    lookup_ipaddress_by_host,
    normalize_bgp_address_family,
    ospf_area_type,
    ospf_instance_comments,
    ospf_interface_comments,
    ospf_process_values,
    resolve_bgp_address_family_for_delete,
    resolve_bgp_peer_for_delete,
    resolve_bgp_scope_for_delete,
    rib_presence_label,
    routing_vrf,
)


class ForwardSyncRunner(ForwardSyncRunnerContractMixin, ForwardSyncRunnerAdapterMixin):
    CONFLICT_WARNING_DETAIL_LIMIT = 20
    MODULE_NATIVE_INVENTORY_PART_TYPES = {
        "FABRIC MODULE",
        "LINE CARD",
        "ROUTING ENGINE",
        "SUPERVISOR",
    }
    FORWARD_BGP_ADDRESS_FAMILY_ALIASES = {
        "ipv4-any": "ipv4-unicast",
        "ipv6-any": "ipv6-unicast",
        "l2vpn-evpn": "l2vpn-evpn",
        "l2vpn-vpls": "l2vpn-vpls",
        "l3vpn-ipv4-any": "vpnv4-unicast",
        "l3vpn-ipv4-unicast": "vpnv4-unicast",
        "l3vpn-ipv6-any": "vpnv6-unicast",
        "l3vpn-ipv6-unicast": "vpnv6-unicast",
        "l3vpn-ipv6-multicast": "vpnv6-multicast",
        "link-state": "link-state",
        "nsap-unicast": "nsap",
    }
    MODEL_CONFLICT_POLICIES = {
        "dcim.site": "reuse_on_unique_conflict",
        "dcim.manufacturer": "reuse_on_unique_conflict",
        "dcim.devicerole": "reuse_on_unique_conflict",
        "dcim.platform": "reuse_on_unique_conflict",
        "dcim.devicetype": "reuse_on_unique_conflict",
        "dcim.inventoryitemrole": "reuse_on_unique_conflict",
        "dcim.cable": "skip_warn_aggregate",
    }

    def __init__(self, sync, ingestion, client, logger_):
        self.sync = sync
        self.ingestion = ingestion
        self.client = client
        self.logger = logger_
        self._content_types = {}
        self._model_coalesce_fields: dict[str, list[list[str]]] = {}
        self._recorded_issue_ids: set[tuple] = set()
        self._failed_dependencies: dict[str, set[tuple]] = {}
        self._aggregated_conflict_warning_counts: dict[tuple[str, str], int] = {}
        self._aggregated_conflict_warning_suppressed: dict[tuple[str, str], int] = {}
        self._aggregated_skip_warning_counts: dict[tuple[str, str], int] = {}
        self._aggregated_skip_warning_suppressed: dict[tuple[str, str], int] = {}
        self.events_clearer = EventsClearer()

    def run(self):
        return run_sync_stage(self)
