import re

from django.contrib.contenttypes.models import ContentType
from django.test import TestCase

from forward_netbox.models import ForwardNQEMap
from forward_netbox.utilities.query_registry import builtin_nqe_map_rows
from forward_netbox.utilities.query_registry import BUILTIN_OPTIONAL_QUERY_MAPS
from forward_netbox.utilities.query_registry import BUILTIN_QUERY_MAPS
from forward_netbox.utilities.query_registry import BUILTIN_QUERY_SPECS
from forward_netbox.utilities.query_registry import get_query_specs


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
    "Forward Interfaces": {
        "device",
        "name",
        "type",
        "enabled",
        "mtu",
        "description",
        "speed",
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
}

MANUFACTURER_QUERY_NAMES = {
    "Forward Device Vendors",
    "Forward Platforms",
    "Forward Device Models",
    "Forward Devices",
    "Forward Inventory Items",
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

        self.assertEqual(len(BUILTIN_OPTIONAL_QUERY_MAPS), 2)
        for query_default in BUILTIN_OPTIONAL_QUERY_MAPS:
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
            "else if isPresent(component_part_id) then truncate(component_part_id, 50)",
            spec.query,
        )
        self.assertIn(
            "else if isPresent(component_name) then truncate(component_name, 50)",
            spec.query,
        )
        self.assertIn(
            "else truncate(role_name, 50)",
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
