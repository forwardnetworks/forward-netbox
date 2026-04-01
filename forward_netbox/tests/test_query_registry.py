import re

from django.contrib.contenttypes.models import ContentType
from django.test import TestCase

from forward_netbox.models import ForwardNQEMap
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
    "Forward Virtual Chassis": {"device", "vc_name", "vc_domain"},
    "Forward Interfaces": {
        "device",
        "name",
        "type",
        "enabled",
        "mtu",
        "description",
        "speed",
    },
    "Forward MAC Addresses": {"device", "interface", "mac"},
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
            self.assertRegex(
                spec.query,
                re.compile(r"let\s+\w+_slug_1\s*="),
                msg=f"{query_default['name']} no longer uses the staged slug pipeline.",
            )

    def test_interface_query_uses_lookup_record_for_speed_mapping(self):
        spec = next(
            spec
            for spec in BUILTIN_QUERY_SPECS["dcim.interface"]
            if spec.query_name == "Forward Interfaces"
        )

        self.assertIn("let ethernet_by_speed = [", spec.query)
        self.assertIn("where profile.key == speed_key", spec.query)
        self.assertIn(
            'type: if isPresent(interface_type) then interface_type else "other"',
            spec.query,
        )
        self.assertIn(
            "speed: if isPresent(interface_speed) then interface_speed else null : Integer",
            spec.query,
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
