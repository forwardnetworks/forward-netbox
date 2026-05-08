import re

from django.contrib.contenttypes.models import ContentType
from django.test import TestCase

from forward_netbox.models import ForwardNQEMap
from forward_netbox.utilities.query_registry import builtin_nqe_map_rows
from forward_netbox.utilities.query_registry import BUILTIN_OPTIONAL_QUERY_MAPS
from forward_netbox.utilities.query_registry import BUILTIN_QUERY_MAPS
from forward_netbox.utilities.query_registry import BUILTIN_QUERY_SPECS
from forward_netbox.utilities.query_registry import get_query_specs
from forward_netbox.utilities.query_registry import get_seeded_builtin_query_spec
from forward_netbox.utilities.query_registry import (
    ipaddress_unassignable_diagnostic_query,
)
from forward_netbox.utilities.query_registry import (
    IPADDRESS_UNASSIGNABLE_DIAGNOSTIC_QUERY_NAME,
)
from forward_netbox.utilities.query_registry import routing_import_diagnostic_query
from forward_netbox.utilities.query_registry import ROUTING_IMPORT_DIAGNOSTIC_QUERY_NAME


REQUIRED_FIELDS_BY_QUERY_NAME = {
    "Forward Locations": {"name", "slug", "status", "physical_address", "comments"},
    "Forward Device Vendors": {"name", "slug"},
    "Forward Device Types": {"name", "slug", "color"},
    "Forward Platforms": {"name", "manufacturer", "manufacturer_slug", "slug"},
    "Forward Device Models": {
        "manufacturer",
        "manufacturer_slug",
        "model",
        "part_number",
        "slug",
    },
    "Forward Devices": {
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
        "platform",
        "platform_slug",
        "status",
    },
    "Forward Virtual Chassis": {"device", "vc_name", "name", "vc_domain"},
    "Forward Device Feature Tags": {"device", "tag", "tag_slug", "tag_color"},
    "Forward Interfaces": {
        "device",
        "name",
        "type",
        "enabled",
        "mtu",
        "description",
        "speed",
    },
    "Forward Inferred Interface Cables": {
        "device",
        "interface",
        "remote_device",
        "remote_interface",
        "status",
    },
    "Forward MAC Addresses": {"device", "interface", "mac", "mac_address"},
    "Forward VLANs": {"site", "site_slug", "vid", "name", "status"},
    "Forward VRFs": {"name", "rd", "description", "enforce_unique"},
    "Forward IPv4 Prefixes": {"vrf", "prefix", "status"},
    "Forward IPv6 Prefixes": {"vrf", "prefix", "status"},
    "Forward IP Addresses": {"device", "interface", "vrf", "address", "status"},
    "Forward Inventory Items": {
        "device",
        "manufacturer",
        "manufacturer_slug",
        "name",
        "part_id",
        "serial",
        "role",
        "role_slug",
        "role_color",
        "status",
        "discovered",
        "description",
    },
    "Forward Modules": {
        "device",
        "module_bay",
        "manufacturer",
        "manufacturer_slug",
        "model",
        "part_number",
        "status",
    },
}

SLUG_QUERY_NAMES = {
    "Forward Locations",
    "Forward Device Vendors",
    "Forward Device Types",
    "Forward Platforms",
    "Forward Device Models",
    "Forward Devices",
    "Forward VLANs",
    "Forward Inventory Items",
    "Forward Modules",
}

MANUFACTURER_QUERY_NAMES = {
    "Forward Device Vendors",
    "Forward Platforms",
    "Forward Device Models",
    "Forward Devices",
    "Forward Inventory Items",
    "Forward Modules",
}


def _field_pattern(field_name):
    return re.compile(rf"(?m)^\s*{re.escape(field_name)}\s*:")


class QueryRegistryTest(TestCase):
    def test_builtin_queries_expose_required_output_fields(self):
        for query_default in BUILTIN_QUERY_MAPS:
            model_specs = BUILTIN_QUERY_SPECS[query_default["model_string"]]
            spec = next(
                spec for spec in model_specs if spec.query_name == query_default["name"]
            )
            for field_name in REQUIRED_FIELDS_BY_QUERY_NAME[query_default["name"]]:
                self.assertRegex(
                    spec.query,
                    _field_pattern(field_name),
                    msg=f"{query_default['name']} is missing `{field_name}`.",
                )

    def test_slug_queries_keep_slug_shaping_in_nqe(self):
        for query_default in BUILTIN_QUERY_MAPS:
            if query_default["name"] not in SLUG_QUERY_NAMES:
                continue
            model_specs = BUILTIN_QUERY_SPECS[query_default["model_string"]]
            spec = next(
                spec for spec in model_specs if spec.query_name == query_default["name"]
            )
            self.assertIn(
                "replaceRegexMatches(",
                spec.query,
                msg=f"{query_default['name']} no longer shapes slugs in NQE.",
            )
            self.assertTrue(
                "slugify(" in spec.query
                or re.search(r"let\s+\w+_slug_1\s*=", spec.query),
                msg=f"{query_default['name']} no longer uses a reusable or staged slug pipeline.",
            )

    def test_manufacturer_queries_canonicalize_vendor_names_in_nqe(self):
        for query_default in BUILTIN_QUERY_MAPS:
            if query_default["name"] not in MANUFACTURER_QUERY_NAMES:
                continue
            model_specs = BUILTIN_QUERY_SPECS[query_default["model_string"]]
            spec = next(
                spec for spec in model_specs if spec.query_name == query_default["name"]
            )
            self.assertIn(
                "canonicalManufacturerName(",
                spec.query,
                msg=f"{query_default['name']} no longer canonicalizes manufacturers in NQE.",
            )
            self.assertIn(
                "manufacturer_name_overrides = [",
                spec.query,
                msg=f"{query_default['name']} no longer carries the shared manufacturer lookup table.",
            )
            self.assertIn(
                '{ vendor: Vendor.CISCO, name: "Cisco" }',
                spec.query,
                msg=f"{query_default['name']} lost the Cisco manufacturer mapping.",
            )
            self.assertIn(
                '{ vendor: Vendor.PALO_ALTO_NETWORKS, name: "Palo Alto Networks" }',
                spec.query,
                msg=f"{query_default['name']} lost the Palo Alto Networks mapping.",
            )
            self.assertIn(
                "where mapping.vendor == vendor",
                spec.query,
                msg=f"{query_default['name']} no longer uses the shared manufacturer lookup filter.",
            )
            self.assertIn(
                "let manufacturer_slug = slugify(manufacturer_name)",
                spec.query,
                msg=f"{query_default['name']} no longer derives manufacturer slugs from the canonical name.",
            )
            self.assertNotIn(
                'if vendor == Vendor.CISCO then "Cisco"',
                spec.query,
                msg=f"{query_default['name']} still uses the legacy vendor if/else chain.",
            )

        manufacturer_spec = next(
            spec
            for spec in BUILTIN_QUERY_SPECS["dcim.manufacturer"]
            if spec.query_name == "Forward Device Vendors"
        )
        self.assertIn("name: manufacturer_name", manufacturer_spec.query)
        self.assertNotIn("name: vendor", manufacturer_spec.query)

        for model_string in [
            "dcim.platform",
            "dcim.devicetype",
            "dcim.device",
            "dcim.inventoryitem",
        ]:
            spec = next(spec for spec in BUILTIN_QUERY_SPECS[model_string])
            self.assertIn("manufacturer: manufacturer_name", spec.query)
            self.assertNotIn("manufacturer: vendor", spec.query)
            self.assertNotIn("manufacturer: device.platform.vendor", spec.query)

    def test_interface_query_uses_lookup_record_for_speed_mapping(self):
        spec = next(
            spec
            for spec in BUILTIN_QUERY_SPECS["dcim.interface"]
            if spec.query_name == "Forward Interfaces"
        )

        self.assertIn("let ethernet_by_speed_mbps = [", spec.query)
        self.assertIn("where profile.mbps == speed_mbps", spec.query)
        self.assertIn(
            'type: if isPresent(interface_type) then interface_type else "other"',
            spec.query,
        )
        self.assertIn("interface.ethernet.aggregateId", spec.query)
        self.assertIn("IfaceType.IF_AGGREGATE", spec.query)
        self.assertIn('then "lag" else "other"', spec.query)
        self.assertIn(
            "speed: if isPresent(speed_mbps) then speed_mbps * 1000 else null : Integer",
            spec.query,
        )

    def test_virtual_chassis_query_supports_vpc_and_mlag_semantics(self):
        spec = next(
            spec
            for spec in BUILTIN_QUERY_SPECS["dcim.virtualchassis"]
            if spec.query_name == "Forward Virtual Chassis"
        )

        self.assertIn("let has_vpc =", spec.query)
        self.assertIn("let has_mlag_peer =", spec.query)
        self.assertIn("truncate(value: String, max_len: Integer)", spec.query)
        self.assertIn("compactMemberKey(value: String)", spec.query)
        self.assertIn("let member_a = if has_mlag_peer", spec.query)
        self.assertIn("let member_b = if has_mlag_peer", spec.query)
        self.assertIn(
            "let bounded_mlag_domain = if length(raw_mlag_domain) <= 30", spec.query
        )
        self.assertIn("&& isPresent(device.ha.vpc)", spec.query)
        self.assertIn("where has_vpc || has_mlag_peer", spec.query)
        self.assertIn("device.ha.vpc.domainId > 0", spec.query)
        self.assertIn("device.ha.mlagPeer", spec.query)
        self.assertIn(
            'join("-", [truncate(site_name, 28), "mlag", vc_domain])', spec.query
        )
        self.assertIn(
            'join("--", [compactMemberKey(member_a), compactMemberKey(member_b)])',
            spec.query,
        )
        self.assertNotIn("else []", spec.query)
        self.assertNotIn(" and isPresent", spec.query)
        self.assertNotIn("where has_vpc or has_mlag_peer", spec.query)
        self.assertNotIn("?.", spec.query)

    def test_wrapped_device_queries_keep_device_first_parallel_shape(self):
        rows = {row["name"]: row for row in builtin_nqe_map_rows()}

        for query_name in (
            "Forward Virtual Chassis",
            "Forward Inventory Items",
            "Forward Modules",
        ):
            query = rows[query_name]["query"]

            self.assertIn("foreach device in network.devices", query)
            self.assertNotIn(
                "foreach row in (",
                query,
                msg=f"{query_name} should not wrap the device iterator.",
            )
            self.assertNotIn(
                "select distinct row",
                query,
                msg=f"{query_name} should deduplicate the projected record directly.",
            )

    def test_custom_maps_win_over_built_in_maps_for_a_model(self):
        netbox_model = ContentType.objects.get(app_label="dcim", model="device")
        builtin_map = ForwardNQEMap.objects.create(
            name="Built-in Devices",
            netbox_model=netbox_model,
            query='select {name: "builtin"}',
            built_in=True,
            enabled=True,
            weight=100,
        )
        custom_map = ForwardNQEMap.objects.create(
            name="Custom Devices",
            netbox_model=netbox_model,
            query_id="FQ_custom_devices",
            built_in=False,
            enabled=True,
            weight=50,
        )

        specs = get_query_specs("dcim.device", maps=[builtin_map, custom_map])

        self.assertEqual(len(specs), 1)
        self.assertEqual(specs[0].query_id, "FQ_custom_devices")
        self.assertEqual(specs[0].query, None)

    def test_built_in_maps_use_current_bundled_query_text(self):
        netbox_model = ContentType.objects.get(app_label="dcim", model="virtualchassis")
        builtin_map = ForwardNQEMap.objects.create(
            name="Forward Virtual Chassis",
            netbox_model=netbox_model,
            query='select {stale: "query"}',
            built_in=True,
            enabled=True,
            weight=100,
        )

        specs = get_query_specs("dcim.virtualchassis", maps=[builtin_map])

        self.assertEqual(len(specs), 1)
        self.assertIn("where has_vpc || has_mlag_peer", specs[0].query)
        self.assertNotIn('select {stale: "query"}', specs[0].query)

    def test_builtin_map_rows_keep_authored_query_source(self):
        row = next(
            row
            for row in builtin_nqe_map_rows()
            if row["name"] == "Forward Device Vendors"
        )

        self.assertIn('import "netbox_utilities";', row["query"])
        self.assertNotIn("manufacturer_name_overrides = [", row["query"])
        self.assertEqual(row["coalesce_fields"], [["slug"], ["name"]])

    def test_builtin_query_specs_flatten_local_imports(self):
        spec = next(
            spec
            for spec in BUILTIN_QUERY_SPECS["dcim.manufacturer"]
            if spec.query_name == "Forward Device Vendors"
        )

        self.assertNotIn('import "netbox_utilities";', spec.query)
        self.assertIn("manufacturer_name_overrides = [", spec.query)
        self.assertEqual(spec.coalesce_fields, (("slug",), ("name",)))

    def test_optional_device_type_alias_maps_are_seeded_disabled(self):
        rows = {
            (row["model_string"], row["name"]): row for row in builtin_nqe_map_rows()
        }

        alias_query_defaults = [
            query_default
            for query_default in BUILTIN_OPTIONAL_QUERY_MAPS
            if query_default["model_string"] in {"dcim.devicetype", "dcim.device"}
        ]

        self.assertEqual(len(alias_query_defaults), 2)
        for query_default in alias_query_defaults:
            row = rows[(query_default["model_string"], query_default["name"])]
            self.assertFalse(row["enabled"])
            self.assertIn("netbox_device_type_aliases", row["query"])
            self.assertIn('alias.record_type == "device_type_alias"', row["query"])
            self.assertIn('alias.record_type == "manufacturer_override"', row["query"])
            self.assertNotIn("where isPresent(aliases.value)", row["query"])

        self.assertNotIn(
            "Forward Device Models with NetBox Device Type Aliases",
            {query_default["name"] for query_default in BUILTIN_QUERY_MAPS},
        )
        self.assertNotIn(
            "Forward Devices with NetBox Device Type Aliases",
            {query_default["name"] for query_default in BUILTIN_QUERY_MAPS},
        )
        self.assertIn(
            "Forward Device Feature Tags with Rules",
            {query_default["name"] for query_default in BUILTIN_OPTIONAL_QUERY_MAPS},
        )
        self.assertNotIn(
            "Forward Device Feature Tags with Rules",
            {query_default["name"] for query_default in BUILTIN_QUERY_MAPS},
        )

    def test_optional_module_maps_are_seeded_disabled(self):
        rows = {
            (row["model_string"], row["name"]): row for row in builtin_nqe_map_rows()
        }

        row = rows[("dcim.module", "Forward Modules")]
        self.assertFalse(row["enabled"])
        self.assertIn("device.platform.components", row["query"])
        self.assertIn("isNetBoxModuleComponent(component)", row["query"])
        self.assertIn("component.partType == DevicePartType.LINE_CARD", row["query"])
        self.assertIn("component.partType == DevicePartType.SUPERVISOR", row["query"])
        self.assertIn(
            "component.partType == DevicePartType.FABRIC_MODULE", row["query"]
        )
        self.assertIn(
            "component.partType == DevicePartType.ROUTING_ENGINE", row["query"]
        )
        self.assertNotIn("DevicePartType.TRANSCEIVER", row["query"])
        self.assertIn("canonicalManufacturerName(", row["query"])
        self.assertIn("manufacturer: manufacturer_name", row["query"])
        self.assertIn("module_bay:", row["query"])
        self.assertIn("part_number:", row["query"])
        self.assertIn("asset_tag: null", row["query"])
        self.assertNotIn("where isPresent(module_bay)", row["query"])
        self.assertNotIn(
            "Forward Modules",
            {query_default["name"] for query_default in BUILTIN_QUERY_MAPS},
        )
        self.assertIn(
            "Forward Modules",
            {query_default["name"] for query_default in BUILTIN_OPTIONAL_QUERY_MAPS},
        )

    def test_seeded_builtin_query_spec_resolves_optional_module_query(self):
        spec = get_seeded_builtin_query_spec("dcim.module", "Forward Modules")

        self.assertEqual(spec.model_string, "dcim.module")
        self.assertEqual(spec.query_name, "Forward Modules")
        self.assertIn("isNetBoxModuleComponent", spec.query)

    def test_optional_bgp_maps_are_seeded_disabled(self):
        rows = {
            (row["model_string"], row["name"]): row for row in builtin_nqe_map_rows()
        }

        bgp_row = rows[("netbox_routing.bgppeer", "Forward BGP Peers")]
        self.assertFalse(bgp_row["enabled"])
        self.assertIn("protocol.bgp.neighbors", bgp_row["query"])
        self.assertIn("neighbor.neighborAddress", bgp_row["query"])
        self.assertIn("neighbor.peerAS", bgp_row["query"])
        self.assertIn("local_asn:", bgp_row["query"])
        self.assertIn("reciprocal_local_asn", bgp_row["query"])
        self.assertIn("internal_peer_asn", bgp_row["query"])
        self.assertEqual(
            bgp_row["coalesce_fields"],
            [["device", "vrf", "neighbor_address"], ["device", "neighbor_address"]],
        )
        self.assertNotRegex(bgp_row["query"], r" : Int(?!eger)")

        bgp_af_row = rows[
            ("netbox_routing.bgpaddressfamily", "Forward BGP Address Families")
        ]
        self.assertFalse(bgp_af_row["enabled"])
        self.assertIn("device.bgpRib.afiSafis", bgp_af_row["query"])
        self.assertIn(
            'afi_safi == "AfiSafiType.L3VPN_IPV4_UNICAST"', bgp_af_row["query"]
        )
        self.assertIn("reciprocal_local_asn", bgp_af_row["query"])
        self.assertIn("internal_peer_asn", bgp_af_row["query"])
        self.assertNotIn('afi_safi == "AfiSafiType.IPV4_MDT"', bgp_af_row["query"])
        self.assertEqual(
            bgp_af_row["coalesce_fields"],
            [
                ["device", "vrf", "local_asn", "afi_safi"],
                ["device", "local_asn", "afi_safi"],
            ],
        )

        bgp_peer_af_row = rows[
            (
                "netbox_routing.bgppeeraddressfamily",
                "Forward BGP Peer Address Families",
            )
        ]
        self.assertFalse(bgp_peer_af_row["enabled"])
        self.assertIn("device.bgpRib.afiSafis", bgp_peer_af_row["query"])
        self.assertIn(
            'afi_safi == "AfiSafiType.L3VPN_IPV4_UNICAST"',
            bgp_peer_af_row["query"],
        )
        self.assertIn("reciprocal_local_asn", bgp_peer_af_row["query"])
        self.assertIn("internal_peer_asn", bgp_peer_af_row["query"])
        self.assertNotIn(
            'afi_safi == "AfiSafiType.IPV4_MDT"',
            bgp_peer_af_row["query"],
        )
        self.assertEqual(
            bgp_peer_af_row["coalesce_fields"],
            [
                ["device", "vrf", "neighbor_address", "afi_safi"],
                ["device", "neighbor_address", "afi_safi"],
            ],
        )

        ospf_instance_row = rows[
            ("netbox_routing.ospfinstance", "Forward OSPF Instances")
        ]
        self.assertFalse(ospf_instance_row["enabled"])
        self.assertIn("protocol.ospf", ospf_instance_row["query"])
        self.assertIn("inferred_router_id", ospf_instance_row["query"])
        self.assertIn("router_id:", ospf_instance_row["query"])

        ospf_interface_row = rows[
            ("netbox_routing.ospfinterface", "Forward OSPF Interfaces")
        ]
        self.assertFalse(ospf_interface_row["enabled"])
        self.assertIn("inferred_router_id", ospf_interface_row["query"])
        self.assertIn("local_interface:", ospf_interface_row["query"])

        peering_row = rows[
            ("netbox_peering_manager.peeringsession", "Forward Peering Sessions")
        ]
        self.assertFalse(peering_row["enabled"])
        self.assertIn("reciprocal_local_asn", peering_row["query"])
        self.assertIn("internal_peer_asn", peering_row["query"])
        self.assertIn("relationship_slug:", peering_row["query"])
        self.assertIn("service_reference:", peering_row["query"])
        self.assertEqual(
            peering_row["coalesce_fields"],
            [["device", "vrf", "neighbor_address"], ["device", "neighbor_address"]],
        )
        self.assertNotIn(
            "Forward BGP Peers",
            {query_default["name"] for query_default in BUILTIN_QUERY_MAPS},
        )
        self.assertIn(
            "Forward BGP Peers",
            {query_default["name"] for query_default in BUILTIN_OPTIONAL_QUERY_MAPS},
        )

    def test_builtin_map_query_id_overrides_bundled_query_for_diff_support(self):
        content_type = ContentType.objects.get(app_label="dcim", model="site")
        query_map = ForwardNQEMap.objects.create(
            name="Forward Locations",
            netbox_model=content_type,
            query_id="FQ_locations",
            built_in=True,
        )

        specs = get_query_specs("dcim.site", maps=[query_map])

        self.assertEqual(specs[0].query_id, "FQ_locations")
        self.assertIsNone(specs[0].query)

    def test_data_file_queries_keep_device_first_parallel_shape(self):
        rows = {row["name"]: row for row in builtin_nqe_map_rows()}

        for query_name in (
            "Forward Device Models with NetBox Device Type Aliases",
            "Forward Devices with NetBox Device Type Aliases",
            "Forward Device Feature Tags with Rules",
        ):
            query = re.sub(r"/\*.*?\*/", "", rows[query_name]["query"], flags=re.S)
            clauses = [
                line.strip()
                for line in query.splitlines()
                if line.strip() and not line.strip().startswith("import ")
            ]

            self.assertEqual(
                clauses[0],
                "foreach device in network.devices",
                msg=f"{query_name} no longer starts with the device iterator.",
            )
            self.assertEqual(
                query.count("network.devices"),
                1,
                msg=f"{query_name} should reference network.devices exactly once.",
            )
            self.assertNotIn(
                "foreach extensions in [network.extensions]",
                query,
                msg=f"{query_name} should not bind extensions before devices.",
            )

    def test_interface_query_includes_loopbacks_for_ip_bearing_logical_interfaces(self):
        spec = next(
            spec
            for spec in BUILTIN_QUERY_SPECS["dcim.interface"]
            if spec.query_name == "Forward Interfaces"
        )

        self.assertIn("loopback_interfaces =", spec.query)
        self.assertIn("interface.interfaceType == IfaceType.IF_LOOPBACK", spec.query)
        self.assertIn('type: "virtual"', spec.query)
        self.assertIn("ethernet_interfaces + loopback_interfaces", spec.query)

    def test_inferred_interface_cable_query_uses_resolved_interface_links(self):
        spec = next(
            spec
            for spec in BUILTIN_QUERY_SPECS["dcim.cable"]
            if spec.query_name == "Forward Inferred Interface Cables"
        )

        self.assertIn("foreach link in interface.links", spec.query)
        self.assertIn("link.deviceName", spec.query)
        self.assertIn("link.ifaceName", spec.query)
        self.assertIn("where link.deviceName in (", spec.query)
        self.assertIn("foreach snapshot_device in network.devices", spec.query)
        self.assertIn(
            "where interface.interfaceType != IfaceType.IF_AGGREGATE",
            spec.query,
        )
        self.assertIn("let remote_interface_type = max(", spec.query)
        self.assertIn(
            "remote_interface_type != IfaceType.IF_AGGREGATE",
            spec.query,
        )
        self.assertIn("select distinct", spec.query)
        self.assertEqual(
            spec.coalesce_fields,
            (("device", "interface", "remote_device", "remote_interface"),),
        )

    def test_device_feature_tag_query_emits_bgp_tag(self):
        spec = next(
            spec
            for spec in BUILTIN_QUERY_SPECS["extras.taggeditem"]
            if spec.query_name == "Forward Device Feature Tags"
        )

        self.assertIn("where isPresent(protocol.bgp)", spec.query)
        self.assertIn('tag: "Prot_BGP"', spec.query)
        self.assertIn('tag_slug: "prot-bgp"', spec.query)
        self.assertEqual(spec.coalesce_fields, (("device", "tag_slug"),))

    def test_optional_device_feature_tag_rules_query_uses_data_file(self):
        row = next(
            row
            for row in builtin_nqe_map_rows()
            if row["name"] == "Forward Device Feature Tags with Rules"
        )

        self.assertEqual(row["model_string"], "extras.taggeditem")
        self.assertFalse(row["enabled"])
        self.assertIn("netbox_feature_tag_rules", row["query"])
        self.assertIn(
            'rule.record_type == "structured_feature_tag_rule"',
            row["query"],
        )
        self.assertIn("let rule_rows = if isPresent(rules.value)", row["query"])
        self.assertIn("foreach rule in rule_rows", row["query"])
        self.assertIn('rule.feature == "bgp"', row["query"])
        self.assertIn("where isPresent(protocol.bgp)", row["query"])
        self.assertIn("tag: rule.tag", row["query"])
        self.assertIn("tag_slug: rule.tag_slug", row["query"])
        self.assertEqual(row["coalesce_fields"], [["device", "tag_slug"]])

    def test_inventory_query_treats_empty_strings_as_missing_identity_values(self):
        spec = next(
            spec
            for spec in BUILTIN_QUERY_SPECS["dcim.inventoryitem"]
            if spec.query_name == "Forward Inventory Items"
        )

        self.assertIn('component.partId) && component.partId != ""', spec.query)
        self.assertIn(
            'component.serialNumber) && component.serialNumber != ""',
            spec.query,
        )
        self.assertIn(
            'component.description) && component.description != ""', spec.query
        )
        self.assertIn("truncate(value: String, max_len: Integer)", spec.query)
        self.assertIn(
            'part_id: if isPresent(component_part_id) then truncate(component_part_id, 50) else ""',
            spec.query,
        )
        self.assertIn(
            'serial: if isPresent(component_serial) then truncate(component_serial, 50) else ""',
            spec.query,
        )
        self.assertIn(
            'role_name != "APPLICATION"',
            spec.query,
        )
        self.assertIn(
            "module_component: isNetBoxModuleRole(role_name)",
            spec.query,
        )
        self.assertIn(
            'label: if isPresent(component_name) then truncate(component_name, 64) else ""',
            spec.query,
        )
        self.assertIn(
            "component.versionId",
            spec.query,
        )

    def test_builtin_specs_use_vrf_optional_coalesce_fallbacks_for_ip_models(self):
        prefix_specs = BUILTIN_QUERY_SPECS["ipam.prefix"]
        self.assertEqual(
            prefix_specs[0].coalesce_fields,
            (("prefix", "vrf"), ("prefix",)),
        )
        self.assertEqual(
            prefix_specs[1].coalesce_fields,
            (("prefix", "vrf"), ("prefix",)),
        )

        ip_spec = next(spec for spec in BUILTIN_QUERY_SPECS["ipam.ipaddress"])
        self.assertEqual(
            ip_spec.coalesce_fields,
            (("address", "vrf"), ("address",)),
        )

    def test_prefix_queries_exclude_host_routes(self):
        ipv4_spec = next(
            spec
            for spec in BUILTIN_QUERY_SPECS["ipam.prefix"]
            if spec.query_name == "Forward IPv4 Prefixes"
        )
        ipv6_spec = next(
            spec
            for spec in BUILTIN_QUERY_SPECS["ipam.prefix"]
            if spec.query_name == "Forward IPv6 Prefixes"
        )

        self.assertIn("where length(entry.prefix) < 32", ipv4_spec.query)
        self.assertNotIn("length(entry.prefix) <= 32", ipv4_spec.query)
        self.assertIn("where length(entry.prefix) < 128", ipv6_spec.query)

    def test_ipaddress_query_excludes_unassignable_interface_addresses(self):
        ip_spec = next(spec for spec in BUILTIN_QUERY_SPECS["ipam.ipaddress"])

        self.assertEqual(
            ip_spec.query.count(
                "where address.prefixLength >= 31 || address.ip != networkAddress"
            ),
            4,
        )
        self.assertEqual(
            ip_spec.query.count(
                "where address.prefixLength >= 31 || address.ip != broadcastAddress"
            ),
            4,
        )
        self.assertIn("host_ip: address.ip", ip_spec.query)
        self.assertIn("prefix_length: address.prefixLength", ip_spec.query)
        self.assertIn(
            "group row as grouped_rows by row.host_ip as host_ip",
            ip_spec.query,
        )
        self.assertIn(
            "let chosen_prefix_length = max(foreach candidate in grouped_rows",
            ip_spec.query,
        )
        self.assertEqual(
            ip_spec.query.count(
                "where address.prefixLength >= 127 || address.ip != networkAddress"
            ),
            4,
        )

    def test_ipaddress_unassignable_diagnostic_query_is_not_seeded_as_import_map(
        self,
    ):
        seeded_names = {row["name"] for row in builtin_nqe_map_rows()}
        diagnostic_query = ipaddress_unassignable_diagnostic_query()

        self.assertNotIn(IPADDRESS_UNASSIGNABLE_DIAGNOSTIC_QUERY_NAME, seeded_names)
        self.assertIn('reason: "ipv4-subnet-network-id"', diagnostic_query)
        self.assertIn('reason: "ipv4-broadcast-address"', diagnostic_query)
        self.assertIn('reason: "ipv6-subnet-network-id"', diagnostic_query)
        self.assertIn("select distinct row", diagnostic_query)

    def test_routing_import_diagnostic_query_is_not_seeded_as_import_map(self):
        seeded_names = {row["name"] for row in builtin_nqe_map_rows()}
        diagnostic_query = routing_import_diagnostic_query()

        self.assertNotIn(ROUTING_IMPORT_DIAGNOSTIC_QUERY_NAME, seeded_names)
        self.assertIn('reason: "bgp-neighbor-without-local-as"', diagnostic_query)
        self.assertIn('reason: "bgp-unsupported-address-family"', diagnostic_query)
        self.assertIn('reason: "ospf-neighbor-without-remote-peer"', diagnostic_query)
        self.assertIn('reason: "ospf-neighbor-without-reverse-peer"', diagnostic_query)
        self.assertIn("select distinct row", diagnostic_query)
