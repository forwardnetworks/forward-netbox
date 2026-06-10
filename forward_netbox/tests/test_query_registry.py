import json
import re
from pathlib import Path
from unittest.mock import Mock

from django.contrib.contenttypes.models import ContentType
from django.test import TestCase

from forward_netbox.models import ForwardNQEMap
from forward_netbox.signals import seed_builtin_nqe_maps
from forward_netbox.utilities.query_registry import _query_contract_gap_remediation
from forward_netbox.utilities.query_registry import builtin_nqe_map_rows
from forward_netbox.utilities.query_registry import BUILTIN_OPTIONAL_QUERY_MAPS
from forward_netbox.utilities.query_registry import builtin_query_contract_summary
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
from forward_netbox.utilities.query_registry import (
    optional_plugin_query_contract_summary,
)
from forward_netbox.utilities.query_registry import QuerySpec
from forward_netbox.utilities.query_registry import read_builtin_query_source
from forward_netbox.utilities.query_registry import resolve_query_specs_for_client
from forward_netbox.utilities.query_registry import routing_import_diagnostic_query
from forward_netbox.utilities.query_registry import ROUTING_IMPORT_DIAGNOSTIC_QUERY_NAME


def _declared_query_parameter_names(query: str) -> set[str]:
    lines = query.splitlines()
    seen_query_marker = False
    for line in lines:
        stripped = line.strip()
        if stripped == "@query":
            seen_query_marker = True
            continue
        if not stripped or stripped.startswith("/*") or stripped.startswith("*"):
            continue
        match = re.match(r"^([A-Za-z_][\w]*)\((.*?)\)\s*=", stripped, flags=re.S)
        if match and (seen_query_marker or "@query" not in query):
            return {
                param_match.group(1)
                for param_match in re.finditer(r"([A-Za-z_][\w]*)\s*:", match.group(2))
            }
    return set()


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
    "Forward Virtual Chassis": {
        "device",
        "vc_name",
        "name",
        "vc_domain",
        "vc_position",
    },
    "Forward Device Feature Tags": {"device", "tag", "tag_slug", "tag_color"},
    "Forward Interfaces": {
        "device",
        "name",
        "type",
        "enabled",
        "mode",
        "untagged_vlan",
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
    "Forward HSRP Groups": {
        "protocol",
        "group_id",
        "name",
        "device",
        "interface",
        "vrf",
        "address",
        "state",
        "priority",
        "status",
    },
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


NETWORK_DEVICE_LOOP_RE = re.compile(
    r"(?m)^\s*foreach\s+(?P<variable>[A-Za-z_][A-Za-z0-9_]*)\s+in\s+network\.devices\s*$"
)


def _network_device_loop_blocks(query):
    matches = list(NETWORK_DEVICE_LOOP_RE.finditer(query))
    for index, match in enumerate(matches):
        block_start = match.end()
        next_start = matches[index + 1].start() if index + 1 < len(matches) else None
        yield match.group("variable"), query[block_start:next_start]


class QueryRegistryTest(TestCase):
    def test_aci_command_inventory_query_matches_fixture_contract(self):
        fixture_path = (
            Path(__file__).with_name("fixtures") / "aci_command_inventory_expected.json"
        )
        expected = json.loads(fixture_path.read_text(encoding="utf-8"))
        query = read_builtin_query_source(expected["filename"])

        self.assertEqual(expected["map_name"], "Forward ACI Command Inventory")
        self.assertEqual(expected["model_string"], "dcim.device")
        for command_type in expected["command_types"]:
            self.assertIn(command_type, query)
        for field_name in expected["required_fields"]:
            self.assertIn(field_name, query)
        self.assertIn("@intent Forward ACI Command Inventory", query)
        self.assertIn("where isEmpty(forward_netbox_shard_keys)", query)
        self.assertIn('description: "Forward observed ACI command inventory"', query)
        if expected["forbid_raw_response_projection"]:
            self.assertNotIn("response: command.response", query)
            self.assertNotIn("response = command.response", query)

    def test_aci_discovery_queries_match_fixture_contract(self):
        fixture_path = (
            Path(__file__).with_name("fixtures") / "aci_discovery_expected.json"
        )
        expected = json.loads(fixture_path.read_text(encoding="utf-8"))

        for query_expected in expected["queries"]:
            query = read_builtin_query_source(query_expected["filename"])
            self.assertIn(f'@intent {query_expected["map_name"]}', query)
            self.assertIn("where isEmpty(forward_netbox_shard_keys)", query)
            for marker in query_expected["command_markers"]:
                self.assertIn(marker, query)
            for field_name in query_expected["required_fields"]:
                self.assertIn(field_name, query)
            if query_expected["forbid_raw_response_projection"]:
                self.assertNotIn("response: command.response", query)
                self.assertNotIn("response = command.response", query)

    def test_query_spec_resolves_repository_path_to_runtime_query_id(self):
        class Client:
            def get_committed_nqe_query(
                self, *, repository, query_path, commit_id, query_index=None
            ):
                self.call = {
                    "repository": repository,
                    "query_path": query_path,
                    "commit_id": commit_id,
                    "query_index": query_index,
                }
                return {
                    "queryId": "Q_devices",
                    "commitId": "commit-1",
                }

        client = Client()
        spec = QuerySpec(
            model_string="dcim.device",
            query_name="Forward Devices",
            query_repository="org",
            query_path="/forward_netbox_validation/forward_devices",
        )

        resolved = spec.resolve(client)

        self.assertEqual(resolved.run_query_id, "Q_devices")
        self.assertEqual(resolved.diff_query_id, "Q_devices")
        self.assertEqual(resolved.commit_id, "commit-1")
        self.assertEqual(resolved.execution_mode, "query_path")
        self.assertEqual(
            resolved.execution_value,
            "org:/forward_netbox_validation/forward_devices",
        )
        self.assertEqual(
            client.call,
            {
                "repository": "org",
                "query_path": "/forward_netbox_validation/forward_devices",
                "commit_id": "head",
                "query_index": None,
            },
        )

    def test_resolve_query_specs_for_client_batches_head_path_queries_by_repository(
        self,
    ):
        client = Mock()
        client.get_nqe_repository_query_index.return_value = {
            "by_path": {
                "/forward_netbox_validation/forward_devices": {
                    "queryId": "Q_devices",
                    "path": "/forward_netbox_validation/forward_devices",
                    "lastCommitId": "commit-1",
                },
                "/forward_netbox_validation/forward_interfaces": {
                    "queryId": "Q_interfaces",
                    "path": "/forward_netbox_validation/forward_interfaces",
                    "lastCommitId": "commit-2",
                },
            }
        }

        specs = [
            QuerySpec(
                model_string="dcim.device",
                query_name="Forward Devices",
                query_repository="org",
                query_path="/forward_netbox_validation/forward_devices",
            ),
            QuerySpec(
                model_string="dcim.interface",
                query_name="Forward Interfaces",
                query_repository="org",
                query_path="/forward_netbox_validation/forward_interfaces",
            ),
        ]

        resolved = resolve_query_specs_for_client(specs, client)

        self.assertEqual(client.get_nqe_repository_query_index.call_count, 1)
        self.assertEqual(resolved[0].run_query_id, "Q_devices")
        self.assertEqual(resolved[1].run_query_id, "Q_interfaces")
        self.assertEqual(resolved[0].commit_id, "commit-1")
        self.assertEqual(resolved[1].commit_id, "commit-2")
        client.get_committed_nqe_query.assert_not_called()

    def test_resolve_query_specs_for_client_falls_back_for_pinned_commit(self):
        client = Mock()
        client.get_nqe_repository_query_index.return_value = {"by_path": {}}
        client.get_committed_nqe_query.return_value = {
            "queryId": "Q_devices",
            "commitId": "commit-1",
        }

        specs = [
            QuerySpec(
                model_string="dcim.device",
                query_name="Forward Devices",
                query_repository="org",
                query_path="/forward_netbox_validation/forward_devices",
                commit_id="commit-1",
            )
        ]

        resolved = resolve_query_specs_for_client(specs, client)

        self.assertEqual(resolved[0].run_query_id, "Q_devices")
        self.assertEqual(resolved[0].commit_id, "commit-1")
        client.get_nqe_repository_query_index.assert_not_called()
        client.get_committed_nqe_query.assert_called_once_with(
            repository="org",
            query_path="/forward_netbox_validation/forward_devices",
            commit_id="commit-1",
        )

    def test_resolve_query_specs_for_client_reuses_index_for_head_miss(self):
        client = Mock()
        client.get_nqe_repository_query_index.return_value = {"by_path": {}}
        client.get_committed_nqe_query.return_value = {
            "queryId": "Q_devices",
            "commitId": "commit-1",
        }

        specs = [
            QuerySpec(
                model_string="dcim.device",
                query_name="Forward Devices",
                query_repository="org",
                query_path="/forward_netbox_validation/forward_devices",
            )
        ]

        resolved = resolve_query_specs_for_client(specs, client)

        self.assertEqual(resolved[0].run_query_id, "Q_devices")
        self.assertEqual(resolved[0].commit_id, "commit-1")
        client.get_nqe_repository_query_index.assert_called_once_with(
            repository="org",
            directory="/",
        )
        client.get_committed_nqe_query.assert_called_once_with(
            repository="org",
            query_path="/forward_netbox_validation/forward_devices",
            commit_id="head",
            query_index={"by_path": {}},
        )

    def test_resolve_query_specs_for_client_dedupes_identical_head_misses(self):
        client = Mock()
        client.get_nqe_repository_query_index.return_value = {"by_path": {}}
        client.get_committed_nqe_query.return_value = {
            "queryId": "Q_devices",
            "commitId": "commit-1",
        }

        specs = [
            QuerySpec(
                model_string="dcim.device",
                query_name="Forward Devices",
                query_repository="org",
                query_path="/forward_netbox_validation/forward_devices",
            ),
            QuerySpec(
                model_string="dcim.device",
                query_name="Forward Devices Copy",
                query_repository="org",
                query_path="/forward_netbox_validation/forward_devices",
            ),
        ]

        resolved = resolve_query_specs_for_client(specs, client)

        self.assertEqual(resolved[0].run_query_id, "Q_devices")
        self.assertEqual(resolved[1].run_query_id, "Q_devices")
        self.assertEqual(client.get_nqe_repository_query_index.call_count, 1)
        self.assertEqual(client.get_committed_nqe_query.call_count, 1)

    def test_query_spec_requires_one_query_reference(self):
        with self.assertRaisesRegex(
            ValueError,
            "Exactly one of",
        ):
            QuerySpec(
                model_string="dcim.device",
                query_name="Forward Devices",
                query="select {}",
                query_id="Q_devices",
            )

    def test_query_spec_only_merges_extra_parameters_for_parameterized_queries(self):
        plain_spec = QuerySpec(
            model_string="dcim.device",
            query_name="Forward Devices",
            query="select {}",
        )
        parameterized_spec = QuerySpec(
            model_string="ipam.prefix",
            query_name="Forward IPv4 Prefixes",
            query="select {}",
            parameters={"forward_netbox_shard_keys": []},
        )

        self.assertEqual(
            plain_spec.merged_parameters({"device_tag_include_tags": ["N.Patel"]}),
            {},
        )
        self.assertEqual(
            parameterized_spec.merged_parameters(
                {"device_tag_include_tags": ["N.Patel"]}
            ),
            {
                "forward_netbox_shard_keys": [],
                "device_tag_include_tags": ["N.Patel"],
            },
        )

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

        self.assertIn("ethernet_by_speed_mbps = [", spec.query)
        self.assertIn("where profile.mbps == speed_mbps", spec.query)
        self.assertIn(
            'then if isPresent(interface_type) then interface_type else "other"',
            spec.query,
        )
        self.assertIn("interface.ethernet.aggregateId", spec.query)
        self.assertIn("IfaceType.IF_AGGREGATE", spec.query)
        self.assertIn('then "lag"', spec.query)
        self.assertIn("interface.ethernet?.switchedVlan?.interfaceMode", spec.query)
        self.assertIn("VlanModeType.ACCESS", spec.query)
        self.assertIn("VlanModeType.TRUNK", spec.query)
        self.assertIn("accessVlan", spec.query)
        self.assertIn("nativeVlan", spec.query)
        self.assertIn(
            "speed: if isPresent(speed_mbps) then speed_mbps * 1000 else null : Integer",
            spec.query,
        )

    def test_fhrp_query_includes_hsrp_and_vrrp_without_extra_query_maps(self):
        spec = next(
            spec
            for spec in BUILTIN_QUERY_SPECS["ipam.fhrpgroup"]
            if spec.query_name == "Forward HSRP Groups"
        )

        self.assertIn("subinterface.ipv4.fhrp.hsrp.fhrpGroups", spec.query)
        self.assertIn("subinterface.ipv6.fhrp.hsrp.fhrpGroups", spec.query)
        self.assertIn("interface.routedVlan.ipv4.fhrp.hsrp.fhrpGroups", spec.query)
        self.assertIn("interface.routedVlan.ipv6.fhrp.hsrp.fhrpGroups", spec.query)
        self.assertIn("subinterface.ipv4.fhrp.vrrp.fhrpGroups", spec.query)
        self.assertIn("subinterface.ipv6.fhrp.vrrp.fhrpGroups", spec.query)
        self.assertIn("interface.routedVlan.ipv4.fhrp.vrrp.fhrpGroups", spec.query)
        self.assertIn("interface.routedVlan.ipv6.fhrp.vrrp.fhrpGroups", spec.query)
        self.assertIn('protocol: "vrrp2"', spec.query)
        self.assertIn('protocol: "vrrp3"', spec.query)
        self.assertEqual(
            spec.parameters,
            {
                "device_tag_include_tags": [],
                "device_tag_include_match": "any",
                "device_tag_exclude_tags": [],
                "forward_netbox_shard_keys": [],
            },
        )

    def test_platform_queries_normalize_aci_vendor_platforms(self):
        normalized_query_names = [
            ("dcim.platform", "Forward Platforms"),
            ("dcim.device", "Forward Devices"),
            ("dcim.device", "Forward Devices with NetBox Device Type Aliases"),
        ]
        for model_string, query_name in normalized_query_names:
            spec = get_seeded_builtin_query_spec(model_string, query_name)
            self.assertIn(
                "normalizeDevicePlatformName(device)",
                spec.query,
                msg=f"{query_name} no longer normalizes forward platform OS values.",
            )
            self.assertNotIn(
                'replace(toString(device.platform.os), "OS.", "")',
                spec.query,
                msg=f"{query_name} still uses legacy direct platform normalization.",
            )

        platform_spec = get_seeded_builtin_query_spec(
            "dcim.platform", "Forward Platforms"
        )
        self.assertIn(
            'matches(toLowerCase(platformOsName(platform_os)), "*apic*")',
            platform_spec.query,
            msg="ACI alias normalization logic missing `apic` detection.",
        )
        self.assertIn(
            'matches(toLowerCase(platformOsName(platform_os)), "*nxos_aci*")',
            platform_spec.query,
            msg="ACI alias normalization logic missing `nxos_aci` detection.",
        )
        self.assertIn(
            'matches(platform_os_version, "15.*")',
            platform_spec.query,
            msg="ACI NX-OS release train detection missing 15.x versions.",
        )
        self.assertIn(
            'matches(platform_os_version, "16.*")',
            platform_spec.query,
            msg="ACI NX-OS release train detection missing 16.x versions.",
        )

    def test_netbox_utilities_aci_detection_uses_command_inventory(self):
        fixture_path = (
            Path(__file__).with_name("fixtures") / "aci_command_inventory_expected.json"
        )
        expected = json.loads(fixture_path.read_text(encoding="utf-8"))
        utilities = read_builtin_query_source("netbox_utilities.nqe")

        self.assertIn("export deviceHasAciCommandOutputs(device: Device)", utilities)
        for command_type in expected["command_types"]:
            self.assertIn(command_type, utilities)
        self.assertIn(
            "export normalizeDevicePlatformName(device: Device)",
            utilities,
        )
        self.assertNotIn(
            "VendorOs",
            utilities,
            msg="NQE helpers should avoid stale VendorOs type annotations.",
        )
        self.assertNotIn(
            "contains(",
            utilities,
            msg="NQE helpers should use SaaS-supported string matching.",
        )

    def test_virtual_chassis_query_does_not_map_ha_peers_by_default(self):
        spec = next(
            spec
            for spec in BUILTIN_QUERY_SPECS["dcim.virtualchassis"]
            if spec.query_name == "Forward Virtual Chassis"
        )

        self.assertIn("foreach device in network.devices", spec.query)
        self.assertIn("where false", spec.query)
        self.assertIn("device: device.name", spec.query)
        self.assertIn("vc_name:", spec.query)
        self.assertIn("vc_domain:", spec.query)
        self.assertIn("vc_position:", spec.query)
        self.assertNotIn("device.ha.vpc", spec.query)
        self.assertNotIn("device.ha.mlagPeer", spec.query)
        self.assertNotIn("clusterHa", spec.query)

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

    def test_device_scoped_builtin_queries_seed_empty_shard_parameter(self):
        rows = {row["name"]: row for row in builtin_nqe_map_rows()}

        for query_name in (
            "Forward Interfaces",
            "Forward IP Addresses",
            "Forward MAC Addresses",
            "Forward Modules",
            "Forward BGP Peers",
            "Forward Virtual Chassis",
        ):
            self.assertEqual(
                rows[query_name]["parameters"],
                {"forward_netbox_shard_keys": []},
            )

        self.assertEqual(
            rows["Forward Locations"]["parameters"],
            {
                "device_tag_include_tags": [],
                "device_tag_include_match": "any",
                "device_tag_exclude_tags": [],
                "forward_netbox_shard_keys": [],
            },
        )

    def test_prefix_builtin_queries_seed_empty_shard_parameter(self):
        rows = {row["name"]: row for row in builtin_nqe_map_rows()}

        self.assertEqual(
            rows["Forward IPv4 Prefixes"]["parameters"],
            {
                "device_tag_include_tags": [],
                "device_tag_include_match": "any",
                "device_tag_exclude_tags": [],
                "forward_netbox_shard_keys": [],
            },
        )
        self.assertEqual(
            rows["Forward IPv6 Prefixes"]["parameters"],
            {
                "device_tag_include_tags": [],
                "device_tag_include_match": "any",
                "device_tag_exclude_tags": [],
                "forward_netbox_shard_keys": [],
            },
        )

    def test_sync_builtin_queries_seed_empty_shard_parameter(self):
        rows = {row["name"]: row for row in builtin_nqe_map_rows()}
        excluded_names = {
            IPADDRESS_UNASSIGNABLE_DIAGNOSTIC_QUERY_NAME,
            ROUTING_IMPORT_DIAGNOSTIC_QUERY_NAME,
        }

        for query_name, row in rows.items():
            if query_name in excluded_names:
                continue
            self.assertIn(
                "forward_netbox_shard_keys",
                row["parameters"],
                msg=f"{query_name} does not seed the shard parameter.",
            )
            self.assertEqual(row["parameters"]["forward_netbox_shard_keys"], [])

    def test_builtin_query_maps_match_declared_parameter_contract(self):
        for row in builtin_nqe_map_rows():
            declared = _declared_query_parameter_names(row["query"])
            self.assertEqual(
                set(row["parameters"].keys()),
                declared,
                msg=(
                    f"{row['name']} should seed exactly the parameters declared "
                    "in its NQE signature."
                ),
            )

    def test_sync_builtin_queries_declare_shard_parameter(self):
        filenames = {
            query["filename"]
            for query in [*BUILTIN_QUERY_MAPS, *BUILTIN_OPTIONAL_QUERY_MAPS]
        }
        excluded_filenames = {
            "forward_ip_addresses_unassignable_diagnostics.nqe",
            "forward_routing_import_diagnostics.nqe",
        }
        for filename in sorted(filenames):
            if filename in excluded_filenames:
                continue
            query = read_builtin_query_source(filename)
            self.assertIn(
                "forward_netbox_shard_keys",
                query,
                msg=f"{filename} does not declare the shard parameter.",
            )

    def test_builtin_query_contract_summary_passes_for_parameterized_maps(self):
        summary = builtin_query_contract_summary()

        self.assertEqual(summary["status"], "pass")
        self.assertEqual(summary["gaps"], [])
        self.assertEqual(
            summary["models"]["ipam.prefix"]["fetch_mode"],
            "nqe_parameters",
        )
        prefix_query_names = {
            query["query_name"] for query in summary["models"]["ipam.prefix"]["queries"]
        }
        self.assertEqual(
            prefix_query_names,
            {"Forward IPv4 Prefixes", "Forward IPv6 Prefixes"},
        )
        for model_report in summary["models"].values():
            if model_report["fetch_mode"] != "nqe_parameters":
                continue
            self.assertGreater(
                model_report["query_count"],
                0,
                msg=f"{model_report['model']} has no shipped query maps.",
            )
            for query_report in model_report["queries"]:
                self.assertTrue(
                    query_report["declares_shard_parameter"],
                    msg=f"{query_report['filename']} missing shard parameter.",
                )
                self.assertTrue(
                    query_report["seeds_empty_shard_parameter"],
                    msg=f"{query_report['filename']} missing empty shard default.",
                )
                self.assertTrue(
                    query_report["has_empty_shard_guard"],
                    msg=f"{query_report['filename']} missing empty shard guard.",
                )
                self.assertTrue(
                    query_report["has_positive_shard_predicate"],
                    msg=f"{query_report['filename']} missing positive shard predicate.",
                )

    def test_optional_plugin_query_contract_summary_passes_for_aci_maps(self):
        summary = optional_plugin_query_contract_summary()

        self.assertIn("aci.netbox_cisco_aci", summary)
        aci_summary = summary["aci.netbox_cisco_aci"]
        self.assertEqual(aci_summary["status"], "pass")
        self.assertEqual(aci_summary["gaps"], [])
        self.assertGreater(aci_summary["model_count"], 0)
        self.assertEqual(
            aci_summary["models"]["netbox_cisco_aci.acifabric"]["fetch_mode"],
            "nqe_parameters",
        )
        self.assertEqual(
            aci_summary["models"]["netbox_cisco_aci.acicontract"]["query_count"],
            1,
        )
        self.assertIn("routing.netbox_routing", summary)
        routing_summary = summary["routing.netbox_routing"]
        self.assertEqual(routing_summary["status"], "pass")
        self.assertEqual(routing_summary["gaps"], [])
        self.assertGreater(routing_summary["model_count"], 0)
        self.assertIn("netbox_routing.bgppeer", routing_summary["models"])
        self.assertIn("netbox_routing.ospfinterface", routing_summary["models"])
        self.assertIn("peering.netbox_peering_manager", summary)
        peering_summary = summary["peering.netbox_peering_manager"]
        self.assertEqual(peering_summary["status"], "pass")
        self.assertEqual(peering_summary["gaps"], [])
        self.assertGreater(peering_summary["model_count"], 0)
        self.assertIn(
            "netbox_peering_manager.peeringsession", peering_summary["models"]
        )

    def test_query_contract_gap_remediation_messages_cover_known_gap_codes(self):
        self.assertIn(
            "shipped query map",
            _query_contract_gap_remediation("missing_builtin_query_map"),
        )
        self.assertIn(
            "forward_netbox_shard_keys",
            _query_contract_gap_remediation("missing_shard_parameter_declaration"),
        )
        self.assertIn(
            "forward_netbox_shard_keys: []",
            _query_contract_gap_remediation("missing_shard_parameter_default"),
        )
        self.assertIn(
            "empty-list guard",
            _query_contract_gap_remediation("missing_empty_shard_guard"),
        )
        self.assertIn(
            "positive membership predicate",
            _query_contract_gap_remediation("missing_positive_shard_predicate"),
        )

    def test_shard_parameter_queries_leave_peer_device_lookups_global(self):
        filenames = {
            query["filename"]
            for query in [*BUILTIN_QUERY_MAPS, *BUILTIN_OPTIONAL_QUERY_MAPS]
        }
        for filename in sorted(filenames):
            query = read_builtin_query_source(filename)
            if "forward_netbox_shard_keys" not in query:
                continue
            for variable, block in _network_device_loop_blocks(query):
                if variable != "peer_device":
                    continue
                self.assertNotRegex(
                    block,
                    (
                        r"where\s+isEmpty\(forward_netbox_shard_keys\)\s*\|\|\s*"
                        r"peer_device\.name\s+in\s+forward_netbox_shard_keys"
                    ),
                    msg=(
                        f"{filename} constrains a cross-device peer inference lookup "
                        "to the current shard."
                    ),
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
        self.assertIn("where false", specs[0].query)
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
            and "Aliases" in query_default["name"]
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

    def test_optional_module_maps_are_seeded_enabled(self):
        rows = {
            (row["model_string"], row["name"]): row for row in builtin_nqe_map_rows()
        }

        row = rows[("dcim.module", "Forward Modules")]
        self.assertTrue(row["enabled"])
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

    def test_optional_aci_maps_are_seeded_disabled(self):
        rows = {
            (row["model_string"], row["name"]): row for row in builtin_nqe_map_rows()
        }

        command_inventory_row = rows[("dcim.device", "Forward ACI Command Inventory")]
        self.assertFalse(command_inventory_row["enabled"])
        self.assertEqual(
            command_inventory_row["parameters"], {"forward_netbox_shard_keys": []}
        )
        self.assertIn("CISCO_APIC_SWITCH", command_inventory_row["query"])
        self.assertIn("response_length", command_inventory_row["query"])

        fabric_row = rows[("netbox_cisco_aci.acifabric", "Forward ACI Fabrics")]
        pod_row = rows[("netbox_cisco_aci.acipod", "Forward ACI Pods")]
        node_row = rows[("netbox_cisco_aci.acinode", "Forward ACI Nodes")]
        apic_node_row = rows[("netbox_cisco_aci.acinode", "Forward ACI APIC Nodes")]
        tenant_row = rows[("netbox_cisco_aci.acitenant", "Forward ACI Tenants")]
        vrf_row = rows[("netbox_cisco_aci.acivrf", "Forward ACI VRFs")]
        bd_row = rows[
            ("netbox_cisco_aci.acibridgedomain", "Forward ACI Bridge Domains")
        ]
        app_profile_row = rows[
            ("netbox_cisco_aci.aciappprofile", "Forward ACI Application Profiles")
        ]
        epg_row = rows[
            ("netbox_cisco_aci.aciendpointgroup", "Forward ACI Endpoint Groups")
        ]
        contract_row = rows[("netbox_cisco_aci.acicontract", "Forward ACI Contracts")]
        filter_row = rows[("netbox_cisco_aci.acifilter", "Forward ACI Filters")]
        l3out_row = rows[("netbox_cisco_aci.acil3out", "Forward ACI L3Outs")]
        static_binding_row = rows[
            (
                "netbox_cisco_aci.acistaticportbinding",
                "Forward ACI Static Port Bindings",
            )
        ]

        aci_rows = (
            fabric_row,
            pod_row,
            node_row,
            apic_node_row,
            tenant_row,
            vrf_row,
            bd_row,
            app_profile_row,
            epg_row,
            contract_row,
            filter_row,
            l3out_row,
            static_binding_row,
        )
        for row in aci_rows:
            self.assertFalse(row["enabled"])
            self.assertEqual(row["parameters"], {"forward_netbox_shard_keys": []})
            self.assertIn("forward_netbox_shard_keys", row["query"])

        self.assertIn("isAciDevice(device)", fabric_row["query"])
        self.assertIn("CISCO_ACI_FABRIC_NODES", pod_row["query"])
        self.assertIn("regexMatches(command.response, nodeRegex)", pod_row["query"])
        self.assertIn("node_id:", node_row["query"])
        self.assertIn("pod_id:", node_row["query"])
        self.assertIn("serial_number:", node_row["query"])
        self.assertIn("node_object_name:", node_row["query"])
        self.assertIn("CISCO_APIC_SWITCH", apic_node_row["query"])
        self.assertIn("CISCO_APIC_CONTROLLER_DETAIL", apic_node_row["query"])
        self.assertIn("apicNodeRegex", apic_node_row["query"])
        self.assertIn("In-Band IPv4 Address", apic_node_row["query"])
        self.assertIn("Pod I[Dd]", apic_node_row["query"])
        self.assertIn("CISCO_ACI_FABRIC_VRFS", tenant_row["query"])
        self.assertIn("tenant_name:", vrf_row["query"])
        self.assertIn("where false", bd_row["query"])
        self.assertIn("where false", app_profile_row["query"])
        self.assertIn("where false", epg_row["query"])
        self.assertIn("where false", contract_row["query"])
        self.assertIn("CISCO_ACI_ZONING_FILTER", filter_row["query"])
        self.assertIn("where false", l3out_row["query"])
        self.assertIn("where false", static_binding_row["query"])
        self.assertNotIn(
            "Forward ACI Nodes",
            {query_default["name"] for query_default in BUILTIN_QUERY_MAPS},
        )
        self.assertIn(
            "Forward ACI Static Port Bindings",
            {query_default["name"] for query_default in BUILTIN_OPTIONAL_QUERY_MAPS},
        )

    def test_seeded_builtin_query_spec_resolves_optional_module_query(self):
        spec = get_seeded_builtin_query_spec("dcim.module", "Forward Modules")

        self.assertEqual(spec.model_string, "dcim.module")
        self.assertEqual(spec.query_name, "Forward Modules")
        self.assertIn("isNetBoxModuleComponent", spec.query)

    def test_optional_bgp_maps_are_seeded_enabled(self):
        rows = {
            (row["model_string"], row["name"]): row for row in builtin_nqe_map_rows()
        }

        bgp_row = rows[("netbox_routing.bgppeer", "Forward BGP Peers")]
        self.assertTrue(bgp_row["enabled"])
        self.assertIn("protocol.bgp.neighbors", bgp_row["query"])
        self.assertIn("neighbor.neighborAddress", bgp_row["query"])
        self.assertIn("neighbor.peerAS", bgp_row["query"])
        self.assertIn("local_asn:", bgp_row["query"])
        self.assertIn("where local_asn >= 1", bgp_row["query"])
        self.assertIn("where neighbor.peerAS >= 1", bgp_row["query"])
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
        self.assertTrue(bgp_af_row["enabled"])
        self.assertIn("device.bgpRib.afiSafis", bgp_af_row["query"])
        self.assertIn(
            'afi_safi == "AfiSafiType.L3VPN_IPV4_UNICAST"', bgp_af_row["query"]
        )
        self.assertIn("reciprocal_local_asn", bgp_af_row["query"])
        self.assertIn("internal_peer_asn", bgp_af_row["query"])
        self.assertIn("where local_asn >= 1", bgp_af_row["query"])
        self.assertIn("where neighbor.peerAS >= 1", bgp_af_row["query"])
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
        self.assertTrue(bgp_peer_af_row["enabled"])
        self.assertIn("device.bgpRib.afiSafis", bgp_peer_af_row["query"])
        self.assertIn(
            'afi_safi == "AfiSafiType.L3VPN_IPV4_UNICAST"',
            bgp_peer_af_row["query"],
        )
        self.assertIn("reciprocal_local_asn", bgp_peer_af_row["query"])
        self.assertIn("internal_peer_asn", bgp_peer_af_row["query"])
        self.assertIn("where local_asn >= 1", bgp_peer_af_row["query"])
        self.assertIn("where neighbor.peerAS >= 1", bgp_peer_af_row["query"])
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
        self.assertTrue(ospf_instance_row["enabled"])
        self.assertIn("protocol.ospf", ospf_instance_row["query"])
        self.assertIn("inferred_router_id", ospf_instance_row["query"])
        self.assertIn("router_id:", ospf_instance_row["query"])
        self.assertEqual(
            ospf_instance_row["coalesce_fields"],
            [["device", "vrf", "process_id"], ["device", "process_id"]],
        )

        ospf_interface_row = rows[
            ("netbox_routing.ospfinterface", "Forward OSPF Interfaces")
        ]
        self.assertTrue(ospf_interface_row["enabled"])
        self.assertIn("inferred_router_id", ospf_interface_row["query"])
        self.assertIn("local_interface:", ospf_interface_row["query"])

        peering_row = rows[
            ("netbox_peering_manager.peeringsession", "Forward Peering Sessions")
        ]
        self.assertTrue(peering_row["enabled"])
        self.assertIn("reciprocal_local_asn", peering_row["query"])
        self.assertIn("internal_peer_asn", peering_row["query"])
        self.assertIn("where local_asn >= 1", peering_row["query"])
        self.assertIn("where neighbor.peerAS >= 1", peering_row["query"])
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

    def test_seed_builtin_maps_enables_existing_optional_routing_map_defaults(self):
        netbox_model, _ = ContentType.objects.get_or_create(
            app_label="netbox_routing", model="bgppeer"
        )
        seed_builtin_nqe_maps(type("Sender", (), {"label": "forward_netbox"}))
        query_map = ForwardNQEMap.objects.get(
            name="Forward BGP Peers",
            netbox_model=netbox_model,
            built_in=True,
        )
        query_map.enabled = False
        query_map.save(update_fields=["enabled"])

        seed_builtin_nqe_maps(type("Sender", (), {"label": "forward_netbox"}))

        query_map.refresh_from_db()
        self.assertTrue(query_map.enabled)

    def test_seed_builtin_maps_skips_aci_maps_when_plugin_contenttypes_are_absent(self):
        self.assertFalse(
            ContentType.objects.filter(app_label="netbox_cisco_aci").exists()
        )

        seed_builtin_nqe_maps(type("Sender", (), {"label": "forward_netbox"}))

        aci_maps = ForwardNQEMap.objects.filter(
            netbox_model__app_label="netbox_cisco_aci",
            built_in=True,
        )
        self.assertEqual(aci_maps.count(), 0)

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

            first_device_clause = next(
                (clause for clause in clauses if clause.startswith("foreach ")),
                "",
            )
            self.assertEqual(
                first_device_clause,
                "foreach device in network.devices",
                msg=f"{query_name} no longer starts execution with the device iterator.",
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

        self.assertIn("foreach interface in device.interfaces", spec.query)
        self.assertIn("interface.interfaceType == IfaceType.IF_LOOPBACK", spec.query)
        self.assertIn('then "virtual"', spec.query)
        self.assertNotIn("ethernet_interfaces + loopback_interfaces", spec.query)

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
        self.assertIn("where isPresent(remote_interface_type)", spec.query)
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

    def test_builtin_specs_use_exact_prefix_vrf_identity_for_prefix_maps(self):
        prefix_specs = BUILTIN_QUERY_SPECS["ipam.prefix"]
        self.assertEqual(
            prefix_specs[0].coalesce_fields,
            (("prefix", "vrf"),),
        )
        self.assertEqual(
            prefix_specs[1].coalesce_fields,
            (("prefix", "vrf"),),
        )

    def test_builtin_specs_use_vrf_optional_coalesce_fallbacks_for_ip_models(self):
        ip_spec = next(spec for spec in BUILTIN_QUERY_SPECS["ipam.ipaddress"])
        self.assertEqual(
            ip_spec.coalesce_fields,
            (("address", "vrf"), ("address",)),
        )
        self.assertEqual(
            ip_spec.parameters,
            {"forward_netbox_shard_keys": []},
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
        self.assertIn('ipAddress("0.0.0.0")', ipv4_spec.query)
        self.assertIn('ipAddress("127.0.0.0")', ipv4_spec.query)
        self.assertIn("where length(entry.prefix) < 128", ipv6_spec.query)
        for spec in (ipv4_spec, ipv6_spec):
            self.assertEqual(
                spec.parameters,
                {
                    "device_tag_include_tags": [],
                    "device_tag_include_match": "any",
                    "device_tag_exclude_tags": [],
                    "forward_netbox_shard_keys": [],
                },
            )
            self.assertIn(
                "f(forward_netbox_shard_keys: List<String>, device_tag_include_tags: List<String>, device_tag_include_match: String, device_tag_exclude_tags: List<String>)",
                spec.query,
            )
            self.assertIn(
                "toString(prefix) in forward_netbox_shard_keys",
                spec.query,
            )
            self.assertIn("tag in device_tag_include_tags", spec.query)

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
        self.assertIn(
            "foreach row in candidate_rows(forward_netbox_shard_keys)",
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
        self.assertIn('reason: "bgp-neighbor-invalid-asn"', diagnostic_query)
        self.assertIn('reason: "bgp-unsupported-address-family"', diagnostic_query)
        self.assertIn('reason: "ospf-neighbor-without-remote-peer"', diagnostic_query)
        self.assertIn('reason: "ospf-neighbor-without-reverse-peer"', diagnostic_query)
        self.assertIn("select distinct row", diagnostic_query)
