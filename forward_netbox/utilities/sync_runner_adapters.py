import logging

from ..exceptions import ForwardQueryError
from .module_readiness import module_bay_import_row
from .sync_cable import apply_dcim_cable
from .sync_cable import delete_dcim_cable
from .sync_cable import lookup_cable_between
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
from .sync_reporting import mark_dependency_failed as sync_mark_dependency_failed
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

logger = logging.getLogger("forward_netbox.sync")


class ForwardSyncRunnerAdapterMixin:
    def _record_issue(
        self,
        model_string,
        message,
        row,
        *,
        exception=None,
        context=None,
        defaults=None,
    ):
        return sync_record_issue(
            self,
            model_string,
            message,
            row,
            exception=exception,
            context=context,
            defaults=defaults,
        )

    def _dependency_key(self, model_string, row):
        return sync_dependency_key(model_string, row)

    def _mark_dependency_failed(self, model_string, row):
        return sync_mark_dependency_failed(self, model_string, row)

    def _dependency_failed(self, model_string, key):
        return sync_dependency_failed(self, model_string, key)

    def _update_existing_or_create(
        self,
        model,
        *,
        lookup,
        defaults,
        fallback_lookups=None,
        conflict_policy="strict",
    ):
        return sync_update_existing_or_create(
            self,
            model,
            lookup=lookup,
            defaults=defaults,
            fallback_lookups=fallback_lookups,
            conflict_policy=conflict_policy,
        )

    def _coalesce_update_or_create(
        self,
        model,
        *,
        coalesce_lookups,
        create_values,
        update_values=None,
        conflict_policy="strict",
    ):
        return sync_coalesce_update_or_create(
            self,
            model,
            coalesce_lookups=coalesce_lookups,
            create_values=create_values,
            update_values=update_values,
            conflict_policy=conflict_policy,
        )

    def _get_unique_or_raise(self, model, lookup):
        return sync_get_unique_or_raise(self, model, lookup)

    def _coalesce_lookup(self, row, *fields):
        return sync_coalesce_lookup(row, *fields)

    def _coalesce_upsert(
        self,
        model_string,
        model,
        *,
        coalesce_lookups,
        create_values,
        update_values=None,
    ):
        return sync_coalesce_upsert(
            self,
            model_string,
            model,
            coalesce_lookups=coalesce_lookups,
            create_values=create_values,
            update_values=update_values,
        )

    def _coalesce_sets_for(self, model_string, default_sets):
        return sync_coalesce_sets_for(self, model_string, default_sets)

    def _upsert_row(
        self,
        model_string,
        model,
        *,
        row,
        coalesce_sets,
        create_values,
        update_values=None,
    ):
        return sync_upsert_row(
            self,
            model_string,
            model,
            row=row,
            coalesce_sets=coalesce_sets,
            create_values=create_values,
            update_values=update_values,
        )

    def _upsert_row_from_defaults(
        self,
        model_string,
        model,
        *,
        row,
        coalesce_sets,
        defaults,
    ):
        return sync_upsert_row_from_defaults(
            self,
            model_string,
            model,
            row=row,
            coalesce_sets=coalesce_sets,
            defaults=defaults,
        )

    def _upsert_values_from_defaults(
        self,
        model_string,
        model,
        *,
        values,
        coalesce_sets,
    ):
        return sync_upsert_values_from_defaults(
            self,
            model_string,
            model,
            values=values,
            coalesce_sets=coalesce_sets,
        )

    def _apply_model_rows(self, model_string, rows):
        return apply_sync_model_rows(self, model_string, rows)

    def _delete_model_rows(self, model_string, rows):
        return delete_sync_model_rows(self, model_string, rows)

    def _ensure_site(self, row):
        from dcim.models import Site

        site, _ = self._upsert_row_from_defaults(
            "dcim.site",
            Site,
            row=row,
            coalesce_sets=self._coalesce_sets_for(
                "dcim.site",
                [("slug",), ("name",)],
            ),
            defaults={"name": row["name"], "slug": row["slug"]},
        )
        return site

    def _ensure_manufacturer(self, row):
        from dcim.models import Manufacturer

        manufacturer, _ = self._upsert_row_from_defaults(
            "dcim.manufacturer",
            Manufacturer,
            row=row,
            coalesce_sets=self._coalesce_sets_for(
                "dcim.manufacturer",
                [("slug",), ("name",)],
            ),
            defaults={"name": row["name"], "slug": row["slug"]},
        )
        return manufacturer

    def _ensure_role(self, row):
        from dcim.models import DeviceRole

        role, _ = self._upsert_row_from_defaults(
            "dcim.devicerole",
            DeviceRole,
            row=row,
            coalesce_sets=self._coalesce_sets_for(
                "dcim.devicerole",
                [("slug",), ("name",)],
            ),
            defaults={
                "name": row["name"],
                "slug": row["slug"],
                "color": row["color"],
            },
        )
        return role

    def _ensure_platform(self, row):
        from dcim.models import Platform

        manufacturer = None
        if row.get("manufacturer"):
            manufacturer = self._ensure_manufacturer(
                {"name": row["manufacturer"], "slug": row["manufacturer_slug"]}
            )
        platform_values = {
            "name": row["name"],
            "slug": row["slug"],
            "manufacturer": manufacturer,
        }
        platform, _ = self._upsert_values_from_defaults(
            "dcim.platform",
            Platform,
            values=platform_values,
            coalesce_sets=self._coalesce_sets_for(
                "dcim.platform",
                [("slug",), ("name",)],
            ),
        )
        return platform

    def _ensure_device_type(self, row):
        from dcim.models import DeviceType

        manufacturer = self._ensure_manufacturer(
            {"name": row["manufacturer"], "slug": row["manufacturer_slug"]}
        )
        model = row["model"]
        slug = row["slug"]
        configured_sets = self._coalesce_sets_for(
            "dcim.devicetype",
            [("manufacturer_slug", "slug"), ("manufacturer_slug", "model")],
        )
        coalesce_lookups = []
        for field_set in configured_sets:
            lookup = {}
            for field_name in field_set:
                if field_name in {"manufacturer", "manufacturer_slug"}:
                    lookup["manufacturer"] = manufacturer
                elif field_name in {"slug", "model"}:
                    lookup[field_name] = row[field_name]
            if lookup:
                coalesce_lookups.append(lookup)
        if not coalesce_lookups:
            raise ForwardQueryError(
                "No usable coalesce lookups were configured for dcim.devicetype."
            )
        device_type_by_model = (
            DeviceType.objects.filter(**coalesce_lookups[1]).order_by("pk").first()
        )
        device_type_by_slug = (
            DeviceType.objects.filter(**coalesce_lookups[0]).order_by("pk").first()
        )

        if (
            device_type_by_model is not None
            and device_type_by_slug is not None
            and device_type_by_model.pk != device_type_by_slug.pk
        ):
            raise ForwardQueryError(
                "Conflicting NetBox device types already exist for "
                f"manufacturer `{manufacturer.name}`: model `{model}` and slug `{slug}` "
                "resolve to different rows."
            )

        create_values = {
            "manufacturer": manufacturer,
            "model": model,
            "slug": slug,
        }
        update_values = {
            "model": model,
            "slug": slug,
        }
        if "part_number" in row:
            part_number = row.get("part_number", "")
            create_values["part_number"] = part_number
            update_values["part_number"] = part_number

        device_type, _ = self._coalesce_upsert(
            "dcim.devicetype",
            DeviceType,
            coalesce_lookups=coalesce_lookups,
            create_values=create_values,
            update_values=update_values,
        )
        return device_type

    def _ensure_vrf(self, row):
        from ipam.models import VRF

        rd = row.get("rd") or None
        values = {
            "name": row["name"],
            "rd": rd,
            "description": row.get("description", ""),
            "enforce_unique": row.get("enforce_unique", False),
        }
        coalesce_sets = [("name",)]
        if rd:
            coalesce_sets.insert(0, ("rd",))

        vrf, _ = self._upsert_values_from_defaults(
            "ipam.vrf",
            VRF,
            values=values,
            coalesce_sets=self._coalesce_sets_for("ipam.vrf", coalesce_sets),
        )
        return vrf

    def _optional_model(self, app_label, model_name, model_string):
        return sync_optional_model(app_label, model_name, model_string)

    def _model_field_values(self, model, values):
        return sync_model_field_values(model, values)

    def _ensure_forward_observed_rir(self):
        from ipam.models import RIR

        values = {"name": "Forward Observed", "slug": "forward-observed"}
        if any(field.name == "is_private" for field in RIR._meta.fields):
            values["is_private"] = True
        rir, _ = self._upsert_values_from_defaults(
            "netbox_routing.bgppeer",
            RIR,
            values=values,
            coalesce_sets=[("slug",), ("name",)],
        )
        return rir

    def _ensure_asn(self, asn_value):
        from ipam.models import ASN

        try:
            asn_number = int(asn_value)
        except (TypeError, ValueError) as exc:
            raise ForwardQueryError(f"Invalid BGP ASN value `{asn_value}`.") from exc
        if asn_number < 1:
            raise ForwardQueryError(
                f"Invalid BGP ASN value `{asn_value}`; ASNs must be greater than or equal to 1."
            )
        existing = self._get_unique_or_raise(ASN, {"asn": asn_number})
        if existing is not None:
            return existing
        rir = self._ensure_forward_observed_rir()
        asn = ASN(asn=asn_number, rir=rir)
        asn.full_clean()
        asn.save()
        return asn

    def _bgp_vrf(self, row):
        return bgp_vrf(self, row)

    def _routing_vrf(self, row):
        return routing_vrf(self, row)

    def _lookup_device_for_routing(self, row, model_string, object_label):
        return lookup_device_for_routing(self, row, model_string, object_label)

    def _host_address(self, address):
        return host_address(address)

    def _lookup_ipaddress_by_host(self, *, address, vrf):
        return lookup_ipaddress_by_host(self, address=address, vrf=vrf)

    def _ensure_bgp_peer_ip(self, row, vrf):
        return ensure_bgp_peer_ip(self, row, vrf)

    def _ensure_bgp_router(self, row, device, local_asn):
        return ensure_bgp_router(self, row, device, local_asn)

    def _ensure_bgp_scope(self, row, router, vrf):
        return ensure_bgp_scope(self, row, router, vrf)

    def _bgp_peer_name(self, row):
        return bgp_peer_name(row)

    def _bgp_peer_comments(self, row):
        return bgp_peer_comments(row)

    def _rib_presence_label(self, value):
        return rib_presence_label(value)

    def _bgp_address_family_comments(self, row):
        return bgp_address_family_comments(row)

    def _bgp_peer_address_family_comments(self, row):
        return bgp_peer_address_family_comments(row)

    def _bgp_peer_values(self, row):
        return bgp_peer_values(self, row)

    def _ensure_netbox_routing_bgppeer(self, row):
        return ensure_netbox_routing_bgppeer(self, row)

    def _normalize_bgp_address_family(self, afi_safi):
        return normalize_bgp_address_family(
            afi_safi, aliases=self.FORWARD_BGP_ADDRESS_FAMILY_ALIASES
        )

    def _ensure_bgp_scope_for_row(self, row, model_string):
        return ensure_bgp_scope_for_row(self, row, model_string)

    def _ensure_bgp_address_family(self, row):
        return ensure_bgp_address_family(self, row)

    def _resolve_bgp_address_family_for_delete(self, row):
        return resolve_bgp_address_family_for_delete(self, row)

    def _ensure_bgp_peer_address_family(self, row):
        return ensure_bgp_peer_address_family(self, row)

    def _ospf_area_type(self, value):
        return ospf_area_type(value)

    def _ospf_process_values(self, row):
        return ospf_process_values(row)

    def _ospf_instance_comments(self, row, process_label):
        return ospf_instance_comments(row, process_label)

    def _ospf_interface_comments(self, row):
        return ospf_interface_comments(row)

    def _ensure_ospf_instance(self, row):
        return ensure_ospf_instance(self, row)

    def _ensure_ospf_area(self, row):
        return ensure_ospf_area(self, row)

    def _ensure_ospf_interface(self, row):
        return ensure_ospf_interface(self, row)

    def _ensure_peering_relationship(self, row):
        return ensure_peering_relationship(self, row)

    def _ensure_vlan(self, *, vid, name, status, site=None):
        from ipam.models import VLAN

        values = {
            "site": site,
            "vid": vid,
            "name": name,
            "status": status,
        }
        vlan, _ = self._upsert_values_from_defaults(
            "ipam.vlan",
            VLAN,
            values=values,
            coalesce_sets=self._coalesce_sets_for(
                "ipam.vlan",
                [("site", "vid")],
            ),
        )
        return vlan

    def _ensure_inventory_item_role(self, row):
        from dcim.models import InventoryItemRole

        role_name = row.get("role")
        if not role_name:
            return None
        role, _ = self._upsert_row_from_defaults(
            "dcim.inventoryitemrole",
            InventoryItemRole,
            row={"name": str(role_name), "slug": row["role_slug"]},
            coalesce_sets=[("slug",), ("name",)],
            defaults={
                "name": str(role_name),
                "slug": row["role_slug"],
                "color": row["role_color"],
            },
        )
        return role

    def _ensure_module_type(self, row):
        from dcim.models.modules import ModuleType

        manufacturer = self._ensure_manufacturer(
            {"name": row["manufacturer"], "slug": row["manufacturer_slug"]}
        )
        values = {
            "manufacturer": manufacturer,
            "model": row["model"],
            "part_number": row.get("part_number") or "",
            "description": row.get("description") or "",
            "comments": row.get("comments") or "",
        }
        module_type, _ = self._upsert_values_from_defaults(
            "dcim.module",
            ModuleType,
            values=values,
            coalesce_sets=[("manufacturer", "model")],
        )
        return module_type

    def _lookup_module_bay(self, device, module_bay_name):
        return sync_lookup_module_bay(device, module_bay_name)

    def _ensure_module_bay(self, device, row):
        module_bay = self._lookup_module_bay(device, row["module_bay"])
        if module_bay is not None:
            return module_bay
        import_row = module_bay_import_row(row)
        values = {
            "name": import_row["name"],
            "label": import_row["label"],
            "position": import_row["position"],
            "description": import_row["description"],
        }
        if any(
            field.name == "enabled" for field in device.modulebays.model._meta.fields
        ):
            values["enabled"] = True
        return device.modulebays.create(**values)

    def _content_type_for(self, model):
        return sync_content_type_for(self, model)

    def _lookup_interface(self, device, interface_name):
        return sync_lookup_interface(device, interface_name)

    def _delete_by_coalesce(self, model, lookups):
        return sync_delete_by_coalesce(self, model, lookups)

    def _delete_dcim_site(self, row):
        return delete_dcim_site(self, row)

    def _delete_dcim_manufacturer(self, row):
        return delete_dcim_manufacturer(self, row)

    def _delete_dcim_devicerole(self, row):
        return delete_dcim_devicerole(self, row)

    def _delete_dcim_platform(self, row):
        return delete_dcim_platform(self, row)

    def _delete_dcim_devicetype(self, row):
        return delete_dcim_devicetype(self, row)

    def _delete_dcim_device(self, row):
        return delete_dcim_device(self, row)

    def _delete_dcim_virtualchassis(self, row):
        return delete_dcim_virtualchassis(self, row)

    def _delete_extras_taggeditem(self, row):
        return delete_extras_taggeditem(self, row)

    def _delete_dcim_interface(self, row):
        return delete_dcim_interface(self, row)

    def _delete_dcim_cable(self, row):
        return delete_dcim_cable(self, row)

    def _delete_dcim_macaddress(self, row):
        return delete_dcim_macaddress(self, row)

    def _delete_ipam_vlan(self, row):
        return delete_ipam_vlan(self, row)

    def _delete_ipam_vrf(self, row):
        return delete_ipam_vrf(self, row)

    def _delete_ipam_prefix(self, row):
        return delete_ipam_prefix(self, row)

    def _delete_ipam_ipaddress(self, row):
        return delete_ipam_ipaddress(self, row)

    def _delete_dcim_inventoryitem(self, row):
        return delete_dcim_inventoryitem(self, row)

    def _delete_dcim_module(self, row):
        return delete_dcim_module(self, row)

    def _resolve_bgp_peer_for_delete(self, row):
        return resolve_bgp_peer_for_delete(self, row)

    def _resolve_bgp_scope_for_delete(self, row):
        return resolve_bgp_scope_for_delete(self, row)

    def _delete_netbox_peering_manager_peeringsession(self, row):
        return delete_netbox_peering_manager_peeringsession(self, row)

    def _delete_netbox_routing_bgppeer(self, row):
        return delete_netbox_routing_bgppeer(self, row)

    def _delete_netbox_routing_bgpaddressfamily(self, row):
        return delete_netbox_routing_bgpaddressfamily(self, row)

    def _delete_netbox_routing_bgppeeraddressfamily(self, row):
        return delete_netbox_routing_bgppeeraddressfamily(self, row)

    def _delete_netbox_routing_ospfinstance(self, row):
        return delete_netbox_routing_ospfinstance(self, row)

    def _delete_netbox_routing_ospfarea(self, row):
        return delete_netbox_routing_ospfarea(self, row)

    def _delete_netbox_routing_ospfinterface(self, row):
        return delete_netbox_routing_ospfinterface(self, row)

    def _apply_dcim_site(self, row):
        return apply_dcim_site(self, row)

    def _apply_dcim_manufacturer(self, row):
        return apply_dcim_manufacturer(self, row)

    def _apply_dcim_platform(self, row):
        return apply_dcim_platform(self, row)

    def _apply_dcim_devicerole(self, row):
        return apply_dcim_devicerole(self, row)

    def _apply_dcim_devicetype(self, row):
        return apply_dcim_devicetype(self, row)

    def _apply_dcim_virtualchassis(self, row):
        return apply_dcim_virtualchassis(self, row)

    def _apply_dcim_device(self, row):
        return apply_dcim_device(self, row)

    def _apply_dcim_interface(self, row):
        return apply_dcim_interface(self, row)

    def _apply_extras_taggeditem(self, row):
        return apply_extras_taggeditem(self, row)

    def _lookup_cable_between(self, interface, remote_interface):
        return lookup_cable_between(self, interface, remote_interface)

    def _apply_dcim_cable(self, row):
        return apply_dcim_cable(self, row)

    def _apply_dcim_macaddress(self, row):
        return apply_dcim_macaddress(self, row)

    def _apply_ipam_vlan(self, row):
        return apply_ipam_vlan(self, row)

    def _apply_ipam_vrf(self, row):
        return apply_ipam_vrf(self, row)

    def _apply_ipam_prefix(self, row):
        return apply_ipam_prefix(self, row)

    def _apply_ipam_ipaddress(self, row):
        return apply_ipam_ipaddress(self, row)

    def _apply_dcim_inventoryitem(self, row):
        return apply_dcim_inventoryitem(self, row)

    def _apply_dcim_module(self, row):
        return apply_dcim_module(self, row)

    def _apply_netbox_routing_bgppeer(self, row):
        return apply_netbox_routing_bgppeer(self, row)

    def _apply_netbox_routing_bgpaddressfamily(self, row):
        return apply_netbox_routing_bgpaddressfamily(self, row)

    def _apply_netbox_routing_bgppeeraddressfamily(self, row):
        return apply_netbox_routing_bgppeeraddressfamily(self, row)

    def _apply_netbox_routing_ospfinstance(self, row):
        return apply_netbox_routing_ospfinstance(self, row)

    def _apply_netbox_routing_ospfarea(self, row):
        return apply_netbox_routing_ospfarea(self, row)

    def _apply_netbox_routing_ospfinterface(self, row):
        return apply_netbox_routing_ospfinterface(self, row)

    def _apply_netbox_peering_manager_peeringsession(self, row):
        return apply_netbox_peering_manager_peeringsession(self, row)
