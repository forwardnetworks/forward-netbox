import json
import re
import unittest
from pathlib import Path
from unittest.mock import Mock

from django.apps import apps
from django.contrib.contenttypes.models import ContentType
from django.test import TestCase

from forward_netbox.exceptions import ForwardClientError
from forward_netbox.exceptions import ForwardQueryError
from forward_netbox.models import ForwardNQEMap
from forward_netbox.signals import seed_builtin_nqe_maps
from forward_netbox.utilities.query_execution_contract import query_source_sha256
from forward_netbox.utilities.query_execution_contract import resolve_execution_contract
from forward_netbox.utilities.query_fetch_execution import ForwardQueryFetcher
from forward_netbox.utilities.query_registry import _collapse_alias_variant_duplicates
from forward_netbox.utilities.query_registry import _query_contract_gap_remediation
from forward_netbox.utilities.query_registry import alias_variant_coverage_violations
from forward_netbox.utilities.query_registry import (
    ALIAS_VARIANT_EXEMPT_QUERY_FILENAMES,
)
from forward_netbox.utilities.query_registry import builtin_nqe_map_rows
from forward_netbox.utilities.query_registry import BUILTIN_OPTIONAL_QUERY_MAPS
from forward_netbox.utilities.query_registry import builtin_query_contract_summary
from forward_netbox.utilities.query_registry import BUILTIN_QUERY_MAPS
from forward_netbox.utilities.query_registry import BUILTIN_QUERY_SPECS
from forward_netbox.utilities.query_registry import ensure_unique_query_spec_executions
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
    "Forward Platforms": {"name", "slug"},
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
    "Forward IPv4 IP Addresses": {"device", "interface", "vrf", "address", "status"},
    "Forward IPv6 IP Addresses": {"device", "interface", "vrf", "address", "status"},
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
    "Forward ACI APIC CIMC Inventory": {
        "device",
        "manufacturer",
        "manufacturer_slug",
        "name",
        "label",
        "part_id",
        "serial",
        "asset_tag",
        "role",
        "role_slug",
        "role_color",
        "part_type",
        "module_component",
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
    def test_tier2_maps_declare_one_central_ownership_contract(self):
        specs_by_key = {}
        for query_default in [
            *BUILTIN_QUERY_MAPS,
            *BUILTIN_OPTIONAL_QUERY_MAPS,
        ]:
            spec = get_seeded_builtin_query_spec(
                query_default["model_string"],
                query_default["name"],
            )
            specs_by_key[spec.contract_key] = spec
        expected_modes = {
            "forward_inferred_interface_cables": "cable_either_endpoint",
            "forward_interfaces": "device",
            "forward_inventory_items": "device",
            "forward_modules": "device",
            "forward_bgp_peers": "device",
            "forward_bgp_address_families": "device",
            "forward_bgp_peer_address_families": "device",
            "forward_ospf_instances": "device",
            "forward_ospf_interfaces": "device",
        }

        self.assertEqual(
            {
                contract_key: specs_by_key[contract_key].diff_ownership_mode
                for contract_key in expected_modes
            },
            expected_modes,
        )
        for contract_key in expected_modes:
            self.assertTrue(
                specs_by_key[contract_key].reducer_id.startswith("tier2_side_local_")
            )
            self.assertEqual(specs_by_key[contract_key].reducer_version, 2)

        apic = specs_by_key["forward_aci_apic_cimc_inventory"]
        self.assertEqual(
            apic.diff_ownership_mode,
            "unsafe_contributor_reduction",
        )
        self.assertEqual(apic.reducer_id, "full_only_contributor_reduction")

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
                    "path": "/forward_netbox_validation/forward_devices_resolved",
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
        self.assertIsNone(resolved.diff_query_id)
        self.assertEqual(resolved.commit_id, "commit-1")
        self.assertEqual(
            resolved.resolved_query_path,
            "/forward_netbox_validation/forward_devices_resolved",
        )
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

    def test_path_bound_map_resolves_new_head_after_query_republish(self):
        content_type = ContentType.objects.get(app_label="dcim", model="device")
        query_map = ForwardNQEMap.objects.create(
            name="Forward Devices",
            netbox_model=content_type,
            query_repository="org",
            query_path="/forward_netbox_validation/forward_devices",
            query_id="",
            built_in=True,
        )
        specs = get_query_specs("dcim.device", maps=[query_map])
        full_source = specs[0].full_query_source
        client = Mock()
        client.get_nqe_repository_query_index.side_effect = [
            {
                "by_path": {
                    query_map.query_path: {
                        "queryId": "Q_devices_v1",
                        "lastCommitId": "commit-1",
                    }
                }
            },
            {
                "by_path": {
                    query_map.query_path: {
                        "queryId": "Q_devices_v2",
                        "lastCommitId": "commit-2",
                    }
                }
            },
        ]
        client.get_nqe_query_history.side_effect = [
            [{"id": "commit-1"}],
            [{"id": "commit-2"}],
        ]
        client.get_committed_nqe_query.side_effect = lambda **kwargs: {
            "queryId": (
                "Q_devices_v1" if kwargs["commit_id"] == "commit-1" else "Q_devices_v2"
            ),
            "path": query_map.query_path,
            "lastCommitId": kwargs["commit_id"],
            "sourceCode": full_source,
        }

        first = resolve_query_specs_for_client(specs, client)[0]
        second = resolve_query_specs_for_client(specs, client)[0]

        self.assertEqual(first.run_query_id, "Q_devices_v1")
        self.assertEqual(first.commit_id, "commit-1")
        self.assertEqual(second.run_query_id, "Q_devices_v2")
        self.assertEqual(second.commit_id, "commit-2")
        self.assertEqual(specs[0].execution_mode, "query_path")
        self.assertEqual(client.get_nqe_query_history.call_count, 2)

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

    def test_resolve_query_specs_for_client_follows_unique_moved_path(self):
        client = Mock()
        client.get_nqe_repository_query_index.return_value = {
            "by_path": {
                "/customer/netbox/forward_devices": {
                    "queryId": "Q_devices_moved",
                    "path": "/customer/netbox/forward_devices",
                    "lastCommitId": "commit-moved",
                }
            }
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

        self.assertEqual(resolved[0].run_query_id, "Q_devices_moved")
        self.assertEqual(resolved[0].commit_id, "commit-moved")
        client.get_committed_nqe_query.assert_not_called()

    def test_resolution_prefers_unique_filename_over_conflicting_intent(self):
        client = Mock()
        client.get_nqe_repository_query_index.return_value = {
            "by_path": {
                "/library/renamed_device_roles": {
                    "queryId": "Q_stale_intent",
                    "intent": "Device roles collected by Forward",
                },
                "/published/forward_device_roles": {
                    "queryId": "Q_matching_filename",
                    "intent": "Current device role contract",
                    "lastCommitId": "commit-current",
                },
            }
        }
        specs = [
            QuerySpec(
                model_string="dcim.devicerole",
                query_name="Forward Device Roles",
                query_repository="org",
                query_path="/legacy/forward_device_roles",
                query_intent="Device roles collected by Forward",
            )
        ]

        resolved = resolve_query_specs_for_client(specs, client)

        self.assertEqual(resolved[0].run_query_id, "Q_matching_filename")
        self.assertEqual(resolved[0].commit_id, "commit-current")
        client.get_committed_nqe_query.assert_not_called()

    def test_resolve_query_specs_for_client_does_not_guess_ambiguous_moved_path(self):
        client = Mock()
        client.get_nqe_repository_query_index.return_value = {
            "by_path": {
                "/folder-a/forward_devices": {
                    "queryId": "Q_devices_a",
                    "intent": "Devices collected by Forward",
                },
                "/folder-b/forward_devices": {
                    "queryId": "Q_devices_b",
                    "intent": "Devices collected by Forward",
                },
            }
        }
        client.get_committed_nqe_query.side_effect = RuntimeError("missing old path")
        specs = [
            QuerySpec(
                model_string="dcim.device",
                query_name="Forward Devices",
                query_repository="org",
                query_path="/forward_netbox_validation/forward_devices",
                query_intent="Devices collected by Forward",
            )
        ]

        with self.assertRaisesRegex(RuntimeError, "missing old path"):
            resolve_query_specs_for_client(specs, client)

        client.get_committed_nqe_query.assert_called_once()

    def test_resolve_query_specs_for_client_follows_unique_relocated_subtree(self):
        client = Mock()
        client.get_nqe_repository_query_index.return_value = {
            "by_path": {
                "/library/netbox/forward_devices": {
                    "queryId": "Q_devices_relocated",
                    "intent": "Devices collected by Forward",
                    "lastCommitId": "commit-relocated",
                },
                "/archive/forward_devices": {
                    "queryId": "Q_devices_archive",
                    "intent": "Devices collected by Forward",
                    "lastCommitId": "commit-archive",
                },
            }
        }
        specs = [
            QuerySpec(
                model_string="dcim.device",
                query_name="Forward Devices",
                query_repository="org",
                query_path="/netbox/forward_devices",
                query_intent="Devices collected by Forward",
            )
        ]

        resolved = resolve_query_specs_for_client(specs, client)

        self.assertEqual(resolved[0].run_query_id, "Q_devices_relocated")
        self.assertEqual(resolved[0].commit_id, "commit-relocated")
        client.get_committed_nqe_query.assert_not_called()

    def test_resolve_query_specs_for_client_uses_relocation_when_intent_changed(self):
        client = Mock()
        client.get_nqe_repository_query_index.return_value = {
            "by_path": {
                "/library/netbox/forward_devices": {
                    "queryId": "Q_devices_relocated",
                    "intent": "Updated device inventory contract",
                },
                "/archive/forward_devices": {
                    "queryId": "Q_devices_archive",
                    "intent": "Archived device inventory contract",
                },
            }
        }
        specs = [
            QuerySpec(
                model_string="dcim.device",
                query_name="Forward Devices",
                query_repository="org",
                query_path="/netbox/forward_devices",
                query_intent="Devices collected by Forward",
            )
        ]

        resolved = resolve_query_specs_for_client(specs, client)

        self.assertEqual(resolved[0].run_query_id, "Q_devices_relocated")
        client.get_committed_nqe_query.assert_not_called()

    def test_resolve_query_specs_for_client_does_not_guess_two_relocations(self):
        client = Mock()
        client.get_nqe_repository_query_index.return_value = {
            "by_path": {
                "/library/netbox/forward_devices": {
                    "queryId": "Q_devices_library",
                    "intent": "Devices collected by Forward",
                },
                "/archive/netbox/forward_devices": {
                    "queryId": "Q_devices_archive",
                    "intent": "Devices collected by Forward",
                },
            }
        }
        client.get_committed_nqe_query.side_effect = RuntimeError("missing old path")
        specs = [
            QuerySpec(
                model_string="dcim.device",
                query_name="Forward Devices",
                query_repository="org",
                query_path="/netbox/forward_devices",
                query_intent="Devices collected by Forward",
            )
        ]

        with self.assertRaisesRegex(RuntimeError, "missing old path"):
            resolve_query_specs_for_client(specs, client)

        client.get_committed_nqe_query.assert_called_once()

    def test_legacy_path_resolution_requires_no_repository_write_permission(self):
        class ReadOnlyClient:
            write_attempts = 0

            def get_nqe_repository_query_index(self, *, repository, directory):
                return {
                    "by_path": {
                        "/library/netbox/forward_devices": {
                            "queryId": "Q_devices_read_only",
                            "path": "/library/netbox/forward_devices",
                            "intent": "Devices collected by Forward",
                            "lastCommitId": "commit-read-only",
                        },
                        "/archive/forward_devices": {
                            "queryId": "Q_devices_archive",
                            "intent": "Devices collected by Forward",
                        },
                    }
                }

            def get_committed_nqe_query(
                self,
                *,
                repository,
                query_path,
                commit_id,
                query_index=None,
            ):
                raise AssertionError("obsolete path lookup must not be needed")

            def _deny_write(self, **kwargs):
                self.write_attempts += 1
                raise PermissionError("repository is read-only")

            add_org_nqe_query = _deny_write
            edit_org_nqe_query = _deny_write
            commit_org_nqe_queries = _deny_write

        client = ReadOnlyClient()
        specs = [
            QuerySpec(
                model_string="dcim.device",
                query_name="Forward Devices",
                query_repository="org",
                query_path="/netbox/forward_devices",
                query_intent="Devices collected by Forward",
                parameters={
                    "forward_netbox_shard_keys": [],
                    "sync_endpoints": False,
                },
            )
        ]

        resolved = resolve_query_specs_for_client(specs, client)

        self.assertEqual(resolved[0].run_query_id, "Q_devices_read_only")
        self.assertEqual(resolved[0].commit_id, "commit-read-only")
        self.assertEqual(
            resolved[0].parameters,
            {
                "forward_netbox_shard_keys": [],
                "sync_endpoints": False,
            },
        )
        self.assertEqual(client.write_attempts, 0)

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
        client.get_nqe_repository_query_index.assert_called_once_with(
            repository="org",
            directory="/",
        )
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

    def test_duplicate_head_paths_share_lookup_then_fail_before_execution(self):
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

        with self.assertRaisesRegex(
            ForwardQueryError,
            "Duplicate logical NQE execution.*Disable or consolidate one map",
        ):
            resolve_query_specs_for_client(specs, client)

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

    def test_query_spec_only_merges_declared_extra_parameters(self):
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
            plain_spec.merged_parameters({"device_tag_include_tags": ["Prod_Core"]}),
            {},
        )
        self.assertEqual(
            parameterized_spec.merged_parameters(
                {
                    "forward_netbox_shard_keys": ["core-1"],
                    "device_tag_include_tags": ["Prod_Core"],
                }
            ),
            {
                "forward_netbox_shard_keys": ["core-1"],
            },
        )

    def test_duplicate_detection_uses_effective_runtime_parameters(self):
        specs = [
            QuerySpec(
                model_string="dcim.device",
                query_name="Forward Devices A",
                query_id="Q_devices",
                parameters={"scope": "a"},
            ),
            QuerySpec(
                model_string="dcim.device",
                query_name="Forward Devices B",
                query_id="Q_devices",
                parameters={"scope": "b"},
            ),
        ]

        with self.assertRaisesRegex(
            ForwardQueryError,
            "Duplicate logical NQE execution",
        ):
            ensure_unique_query_spec_executions(
                specs,
                extra_parameters={"scope": "same"},
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

        platform_spec = next(spec for spec in BUILTIN_QUERY_SPECS["dcim.platform"])
        self.assertIn("group manufacturer_name as manufacturers", platform_spec.query)
        self.assertIn("distinct(manufacturers)", platform_spec.query)
        self.assertIn("length(distinct_manufacturers) == 1", platform_spec.query)

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

    def test_builtin_query_outputs_satisfy_contract_required_fields(self):
        # Regression: a ModelSyncContract must never require an output field the
        # builtin query does not emit. Platform manufacturer remains optional
        # because cross-vendor platform groups intentionally emit it blank.
        from forward_netbox.utilities.sync_contracts import MODEL_SYNC_CONTRACTS
        from forward_netbox.utilities.sync_contracts import (
            extract_declared_query_fields,
        )

        for model_string, contract in MODEL_SYNC_CONTRACTS.items():
            for spec in BUILTIN_QUERY_SPECS.get(model_string, ()):
                if not spec.query:
                    continue
                declared = extract_declared_query_fields(spec.query)
                missing = [
                    field for field in contract.required_fields if field not in declared
                ]
                self.assertEqual(
                    missing,
                    [],
                    f"{model_string} query `{spec.query_name}` omits "
                    f"contract-required output field(s): {missing}",
                )

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

    def test_platform_queries_normalize_aci_apic_and_cimc_platforms(self):
        """Producer and consumers derive the platform name the same way.

        This test used to require the opposite: that Forward Platforms call
        `normalizePlatformName(os, version)` directly while every consumer
        called `normalizeDevicePlatformName(device)`. The two disagree for
        exactly the devices ACI is about - a fabric switch whose `platform.os`
        is NXOS and whose ACI-ness is visible only in its command outputs got
        an "NXOS" platform row from the producer and an "ACI" reference from
        every consumer, so the consumers' rows skipped as missing a parent.
        `2026-08-03-alias-variant-coverage-guard.md` recorded that mismatch as
        the one code-verifiable lead behind a customer's softwareversion skips.
        One derivation, on both sides.
        """
        for model_string, query_name in (
            ("dcim.device", "Forward Devices"),
            ("dcim.device", "Forward Devices with NetBox Device Type Aliases"),
            ("dcim.platform", "Forward Platforms"),
        ):
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
        utilities = read_builtin_query_source("netbox_utilities.nqe")
        self.assertIn(
            "export isApicPlatform(platform_os: String)",
            utilities,
            msg="ACI/APIC platform split helper missing.",
        )
        self.assertIn(
            'then "APIC"',
            utilities,
            msg="APIC platforms should remain distinct from ACI switch platforms.",
        )
        self.assertIn(
            "export isCimcPlatform(platform_os: String)",
            utilities,
            msg="CIMC platform detection missing from the shared NQE helper.",
        )
        self.assertIn(
            'then "CIMC"',
            utilities,
            msg="CIMC platforms should remain distinct from APIC and ACI platforms.",
        )
        self.assertIn(
            'then "ACI"',
            utilities,
            msg="ACI switch platforms should still normalize to ACI.",
        )
        self.assertIn(
            'matches(toLowerCase(platformOsName(platform_os)), "*apic*")',
            platform_spec.query,
            msg="APIC platform detection missing from the shared NQE helper.",
        )
        self.assertIn(
            'matches(toLowerCase(platformOsName(platform_os)), "*cimc*")',
            platform_spec.query,
            msg="CIMC platform detection missing from the shared NQE helper.",
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
            # CommandType.CUSTOM is the generic catch-all collected by the ACI
            # command-inventory query, but it must NOT be an ACI signal in
            # deviceHasAciCommandOutputs — it is present on nearly every device,
            # so including it misclassified ~all devices as the "ACI" platform.
            if command_type == "CUSTOM":
                self.assertNotIn("CommandType.CUSTOM", utilities)
                continue
            self.assertIn(command_type, utilities)
        self.assertIn(
            "export normalizeDevicePlatformName(device: Device)",
            utilities,
        )
        # APIC controllers carry CISCO_APIC_CONTROLLER_DETAIL, which also trips
        # deviceHasAciCommandOutputs. The controller check must take precedence
        # so controllers land on "APIC" (distinct version) rather than "ACI".
        self.assertIn(
            "export deviceIsApicController(device: Device)",
            utilities,
            msg="APIC controller detection helper missing.",
        )
        self.assertIn(
            "CommandType.CISCO_APIC_CONTROLLER_DETAIL",
            utilities,
        )
        controller_branch = (
            "if isApicPlatform(toString(device.platform.os)) "
            "|| deviceIsApicController(device)"
        )
        # The controller check is the FIRST branch of normalizeDevicePlatformName
        # (paired with isApicPlatform → "APIC"), so it structurally precedes the
        # isAciDevice/command-output fallback that would otherwise win.
        self.assertIn(
            controller_branch,
            utilities,
            msg="normalizeDevicePlatformName must classify APIC controllers as "
            "APIC before the ACI command-output fallback.",
        )
        device_platform_body = utilities[
            utilities.index("export normalizeDevicePlatformName(device: Device)") :
        ]
        self.assertLess(
            device_platform_body.index(controller_branch),
            device_platform_body.index('then "ACI"'),
            msg="APIC controller branch must precede the ACI branch.",
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
        spec = get_seeded_builtin_query_spec(
            "dcim.virtualchassis", "Forward Virtual Chassis"
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

    def test_virtual_chassis_contract_map_is_optional_and_disabled(self):
        rows = {
            (row["model_string"], row["name"]): row for row in builtin_nqe_map_rows()
        }

        row = rows[("dcim.virtualchassis", "Forward Virtual Chassis")]
        self.assertFalse(row["enabled"])
        self.assertEqual(BUILTIN_QUERY_SPECS["dcim.virtualchassis"], [])
        self.assertNotIn(
            "Forward Virtual Chassis",
            {query_default["name"] for query_default in BUILTIN_QUERY_MAPS},
        )
        self.assertIn(
            "Forward Virtual Chassis",
            {query_default["name"] for query_default in BUILTIN_OPTIONAL_QUERY_MAPS},
        )

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
            "Forward IPv4 IP Addresses",
            "Forward IPv6 IP Addresses",
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

    def test_feature_tags_query_seeds_sync_device_tags_parameter(self):
        # The device-feature-tags query is operator-driven: its signature declares
        # sync_device_tags, so the seeded parameters must include it (empty) or the
        # param-injection guard never fires and the selected tags never reach the
        # fetch.
        rows = {row["name"]: row for row in builtin_nqe_map_rows()}
        self.assertEqual(
            rows["Forward Device Feature Tags"]["parameters"],
            {
                "forward_netbox_shard_keys": [],
                "sync_device_tags": [],
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
            aci_summary["models"]["dcim.inventoryitem"]["fetch_mode"],
            "nqe_parameters",
        )
        cimc_queries = aci_summary["models"]["dcim.inventoryitem"]["queries"]
        self.assertEqual(len(cimc_queries), 1)
        self.assertEqual(
            cimc_queries[0]["query_name"], "Forward ACI APIC CIMC Inventory"
        )
        self.assertFalse(cimc_queries[0]["enabled_by_default"])
        self.assertTrue(cimc_queries[0]["declares_shard_parameter"])
        self.assertTrue(cimc_queries[0]["seeds_empty_shard_parameter"])
        self.assertTrue(cimc_queries[0]["has_empty_shard_guard"])
        self.assertTrue(cimc_queries[0]["has_positive_shard_predicate"])
        self.assertNotIn("netbox_cisco_aci.acicontract", aci_summary["models"])
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

    def test_custom_map_executes_by_id_when_location_metadata_is_present(self):
        netbox_model = ContentType.objects.get(app_label="dcim", model="device")
        custom_map = ForwardNQEMap.objects.create(
            name="Custom Devices",
            netbox_model=netbox_model,
            query_id="OQ_custom_devices",
            query_repository="org",
            query_path="/old/folder/custom_devices",
            built_in=False,
            enabled=True,
        )

        specs = get_query_specs("dcim.device", maps=[custom_map])

        self.assertEqual(len(specs), 1)
        self.assertEqual(specs[0].query_id, "OQ_custom_devices")
        self.assertEqual(specs[0].query_repository, "org")
        self.assertIsNone(specs[0].query_path)
        self.assertEqual(
            specs[0].resolved_query_path,
            "/old/folder/custom_devices",
        )
        self.assertEqual(specs[0].execution_mode, "query_id")

    def test_pinned_custom_query_hydrates_source_and_checks_declared_parameters(self):
        full_source = """
@query
f(scope: List<String>) =
foreach value in scope
select {name: value}
"""
        netbox_model = ContentType.objects.get(app_label="dcim", model="device")
        custom_map = ForwardNQEMap.objects.create(
            name="Custom Devices",
            netbox_model=netbox_model,
            query_id="OQ_custom_devices",
            query_repository="org",
            query_path="/old/folder/custom_devices",
            commit_id="custom-full-commit",
            parameters={"scope": []},
            built_in=False,
            enabled=True,
        )
        client = Mock()
        client.get_committed_nqe_query.return_value = {
            "queryId": "OQ_custom_devices",
            "path": "/old/folder/custom_devices",
            "sourceCode": full_source,
        }

        (resolved,) = resolve_query_specs_for_client(
            get_query_specs("dcim.device", maps=[custom_map]),
            client,
        )
        contract = resolve_execution_contract(
            resolved,
            effective_parameters={"scope": ["device-a"]},
        )

        self.assertTrue(contract.full_eligible)
        self.assertEqual(contract.full_reason_code, "eligible")
        self.assertEqual(
            resolved.full_source_sha256,
            query_source_sha256(full_source),
        )
        client.get_committed_nqe_query.assert_called_once_with(
            repository="org",
            query_path="/old/folder/custom_devices",
            commit_id="custom-full-commit",
            require_source_code=True,
        )

    def test_duplicate_custom_map_execution_is_rejected_before_fetch(self):
        netbox_model = ContentType.objects.get(app_label="dcim", model="device")
        first = ForwardNQEMap.objects.create(
            name="Custom Devices First",
            netbox_model=netbox_model,
            query_id="FQ_duplicate_devices",
            parameters={"forward_netbox_shard_keys": []},
            built_in=False,
            enabled=True,
            weight=50,
        )
        second = ForwardNQEMap.objects.create(
            name="Custom Devices Second",
            netbox_model=netbox_model,
            query_id="FQ_duplicate_devices",
            parameters={"forward_netbox_shard_keys": []},
            built_in=False,
            enabled=True,
            weight=51,
        )

        with self.assertRaisesRegex(
            ForwardQueryError,
            "Duplicate logical NQE execution.*Disable or consolidate one map",
        ):
            get_query_specs("dcim.device", maps=[first, second])

    def test_distinct_paths_resolving_to_same_query_are_rejected(self):
        specs = [
            QuerySpec(
                model_string="dcim.device",
                query_name=name,
                query_repository="org",
                query_path=path,
                parameters={"forward_netbox_shard_keys": []},
            )
            for name, path in (
                ("Custom Devices First", "/queries/devices-first"),
                ("Custom Devices Second", "/queries/devices-second"),
            )
        ]
        client = Mock()
        client.get_nqe_repository_query_index.return_value = {
            "by_path": {
                spec.query_path: {
                    "queryId": "FQ_duplicate_devices",
                    "lastCommitId": "commit-1",
                }
                for spec in specs
            }
        }

        with self.assertRaisesRegex(
            ForwardQueryError,
            "Duplicate logical NQE execution.*Disable or consolidate one map",
        ):
            resolve_query_specs_for_client(specs, client)

    def test_resolve_map_specs_collapses_alias_variant_duplicates(self):
        # Base device query + its NetBox-alias variant both enabled for the same
        # model must collapse to ONE spec (the alias supersedes the base), else
        # each sync reconciles the device twice and flips its device_type FK.
        netbox_model = ContentType.objects.get(app_label="dcim", model="device")
        base = ForwardNQEMap.objects.create(
            name="Forward Devices",
            netbox_model=netbox_model,
            query_id="FQ_devices",
            built_in=True,
            enabled=True,
            weight=600,
        )
        alias = ForwardNQEMap.objects.create(
            name="Forward Devices with NetBox Device Type Aliases",
            netbox_model=netbox_model,
            query_id="FQ_devices_alias",
            built_in=True,
            enabled=True,
            weight=2100,
        )

        specs = get_query_specs("dcim.device", maps=[base, alias])

        self.assertEqual(len(specs), 1)
        self.assertEqual(specs[0].query_id, "FQ_devices_alias")

    def test_resolve_map_specs_collapses_dlm_hardware_alias_duplicate(self):
        base = Mock(name="base")
        base.name = "Forward DLM Hardware Notices"
        alias = Mock(name="alias")
        alias.name = "Forward DLM Hardware Notices with NetBox Aliases"

        collapsed = _collapse_alias_variant_duplicates([base, alias])

        self.assertEqual(collapsed, [alias])

    def test_resolve_map_specs_keeps_base_when_alias_disabled(self):
        # Only the base enabled -> keep the base (no alias to supersede it).
        netbox_model = ContentType.objects.get(app_label="dcim", model="device")
        base = ForwardNQEMap.objects.create(
            name="Forward Devices",
            netbox_model=netbox_model,
            query_id="FQ_devices",
            built_in=True,
            enabled=True,
            weight=600,
        )

        specs = get_query_specs("dcim.device", maps=[base])

        self.assertEqual(len(specs), 1)
        self.assertEqual(specs[0].query_id, "FQ_devices")

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

    def test_persisted_diff_contract_hydrates_both_pinned_sources(self):
        full_source = """
@query
f(forward_netbox_shard_keys: List<String>) =
foreach value in forward_netbox_shard_keys
select {name: value, slug: value}
"""
        diff_source = """
@query
f() =
foreach value in ["vendor"]
select {name: value, slug: value}
"""
        query_id = "OQ_vendor"
        query_path = "/validation/forward_device_vendors"
        netbox_model = ContentType.objects.get(app_label="dcim", model="manufacturer")
        query_map = ForwardNQEMap.objects.create(
            name="Forward Device Vendors",
            netbox_model=netbox_model,
            query_id=query_id,
            query_repository="org",
            query_path=query_path,
            commit_id="full-commit",
            diff_commit_id="diff-commit",
            full_source_sha256=query_source_sha256(full_source),
            diff_source_sha256=query_source_sha256(diff_source),
            built_in=True,
            enabled=True,
        )
        client = Mock()
        client.get_committed_nqe_query.side_effect = lambda **kwargs: {
            "queryId": query_id,
            "path": query_path,
            "lastCommitId": kwargs["commit_id"],
            "sourceCode": (
                full_source if kwargs["commit_id"] == "full-commit" else diff_source
            ),
        }

        specs = resolve_query_specs_for_client(
            get_query_specs("dcim.manufacturer", maps=[query_map]),
            client,
        )
        contract = resolve_execution_contract(
            specs[0],
            effective_parameters={"forward_netbox_shard_keys": []},
        )

        self.assertTrue(contract.diff_eligible)
        self.assertEqual(contract.reason_code, "eligible")
        self.assertEqual(contract.full_revision.commit_id, "full-commit")
        self.assertEqual(contract.diff_revision.commit_id, "diff-commit")
        self.assertEqual(
            contract.full_revision.declared_parameters[0].name,
            "forward_netbox_shard_keys",
        )
        self.assertEqual(contract.diff_revision.declared_parameters, ())
        self.assertEqual(client.get_committed_nqe_query.call_count, 2)
        for call in client.get_committed_nqe_query.call_args_list:
            self.assertTrue(call.kwargs["require_source_code"])

    def test_unpinned_query_id_reads_no_commit_and_runs_at_forward_latest(self):
        """A query ID with no commit is a complete binding, so nothing is read.

        This used to search history for a revision whose source matched the
        bundle, because a query head can legitimately move to a parameterless
        diff revision that would reject our parameters. That search cost more
        than it protected: any repository move, permissions gap, or lookup that
        did not answer refused every enabled map, and one refused map plans zero
        jobs for the whole sync - five dead syncs for one customer.

        The parameterless-head case now fails at execution with a Forward
        runtime error instead of at preflight, as one named per-model failure.
        That is the accepted trade; see
        `test_unpinned_query_id_with_a_parameterless_head_fails_at_execution`.
        """

        full_source = """
@query
f(scope: List<String>) =
foreach value in scope
select {name: value, slug: value}
"""
        parameterless_head = """
@query
f() =
foreach value in ["site"]
select {name: value, slug: value}
"""
        query_id = "Q_sites"
        query_path = "/validation/forward_locations"
        spec = QuerySpec(
            model_string="dcim.site",
            query_name="Forward Locations",
            query_id=query_id,
            query_repository="org",
            resolved_query_path=query_path,
            built_in=True,
            contract_key="forward_locations",
            full_query_source=full_source,
            full_source_sha256=query_source_sha256(full_source),
            parameters={"scope": []},
        )
        client = Mock()
        client.get_nqe_query_history.return_value = [
            {"id": "full-commit"},
            {"id": "parameterless-head"},
        ]

        def committed_query(**kwargs):
            return {
                "queryId": query_id,
                "path": query_path,
                "lastCommitId": kwargs["commit_id"],
                "sourceCode": (
                    full_source
                    if kwargs["commit_id"] == "full-commit"
                    else parameterless_head
                ),
            }

        client.get_committed_nqe_query.side_effect = committed_query

        (resolved,) = resolve_query_specs_for_client([spec], client)
        contract = resolve_execution_contract(
            resolved,
            effective_parameters={"scope": ["site-a"]},
        )

        self.assertIsNone(resolved.commit_id)
        self.assertTrue(contract.full_eligible)
        self.assertEqual(contract.full_reason_code, "eligible")
        self.assertTrue(contract.full_unpinned_head)
        # The declaration check survives without Forward: a built-in map ships
        # its `.nqe`, so the source is verified against its own bundled hash.
        self.assertTrue(contract.full_revision.source_verified)
        self.assertEqual(
            {
                parameter.name
                for parameter in contract.full_revision.declared_parameters
            },
            {"scope"},
        )
        client.get_nqe_query_history.assert_not_called()
        client.get_committed_nqe_query.assert_not_called()

    def test_unpinned_query_id_with_a_parameterless_head_fails_at_execution(self):
        full_source = """
@query
f(scope: List<String>) =
foreach value in scope
select {name: value, slug: value}
"""
        parameterless_head = """
@query
f() =
foreach value in ["site"]
select {name: value, slug: value}
"""
        query_id = "Q_sites"
        query_path = "/validation/forward_locations"
        spec = QuerySpec(
            model_string="dcim.site",
            query_name="Forward Locations",
            query_id=query_id,
            query_repository="org",
            resolved_query_path=query_path,
            built_in=True,
            contract_key="forward_locations",
            full_query_source=full_source,
            full_source_sha256=query_source_sha256(full_source),
            parameters={"scope": []},
        )
        client = Mock()
        client.get_nqe_query_history.return_value = [{"id": "parameterless-head"}]
        client.get_committed_nqe_query.return_value = {
            "queryId": query_id,
            "path": query_path,
            "lastCommitId": "parameterless-head",
            "sourceCode": parameterless_head,
        }

        (resolved,) = resolve_query_specs_for_client([spec], client)
        contract = resolve_execution_contract(
            resolved,
            effective_parameters={"scope": ["site-a"]},
        )

        self.assertIsNone(resolved.commit_id)
        # It plans and runs. Forward then refuses the parameters, and that
        # refusal is reported per model with the map named, rather than the
        # plugin quietly planning nothing at all.
        self.assertTrue(contract.full_eligible)
        self.assertTrue(contract.full_unpinned_head)
        client.get_nqe_query_history.assert_not_called()

        fetcher = ForwardQueryFetcher(sync=None, client=None, logger_=None)
        message = fetcher._failure_message(
            "dcim.site",
            resolved,
            ForwardClientError(
                "Forward API request failed with HTTP 400: NQE_RUNTIME_ERROR - "
                "Provided argument, 'scope' is not a parameter to the given query."
            ),
        )

        self.assertIn("Forward Locations", message)
        self.assertIn(query_id, message)
        self.assertIn("no longer declares them", message)

    def test_builtin_map_uses_shipped_full_hash_when_persisted_hash_is_empty(self):
        netbox_model = ContentType.objects.get(app_label="dcim", model="device")
        query_map = ForwardNQEMap.objects.create(
            name="Forward Devices",
            netbox_model=netbox_model,
            query_id="Q_devices",
            query_repository="org",
            query_path="/validation/forward_devices",
            built_in=True,
            enabled=True,
        )

        (spec,) = get_query_specs("dcim.device", maps=[query_map])

        self.assertEqual(
            spec.full_source_sha256,
            query_source_sha256(spec.full_query_source),
        )

    def test_persisted_full_only_contract_hydrates_its_pinned_source(self):
        bundled_source = """
@query
f(forward_netbox_shard_keys: List<String>) =
foreach value in forward_netbox_shard_keys
select {name: value, slug: value}
"""
        parameterless_live_source = """
@query
f() =
foreach value in ["vendor"]
select {name: value, slug: value}
"""
        query_id = "OQ_vendor"
        query_path = "/validation/forward_device_vendors"
        netbox_model = ContentType.objects.get(app_label="dcim", model="manufacturer")
        query_map = ForwardNQEMap.objects.create(
            name="Forward Device Vendors",
            netbox_model=netbox_model,
            query_id=query_id,
            query_repository="org",
            query_path=query_path,
            commit_id="full-commit",
            full_source_sha256=query_source_sha256(bundled_source),
            built_in=True,
            enabled=True,
        )
        client = Mock()
        client.get_committed_nqe_query.return_value = {
            "queryId": query_id,
            "path": query_path,
            "lastCommitId": "full-commit",
            "sourceCode": parameterless_live_source,
        }

        specs = resolve_query_specs_for_client(
            get_query_specs("dcim.manufacturer", maps=[query_map]),
            client,
        )
        contract = resolve_execution_contract(
            specs[0],
            effective_parameters={"forward_netbox_shard_keys": []},
        )

        self.assertFalse(contract.full_eligible)
        self.assertEqual(contract.full_reason_code, "unverified_full_source")
        client.get_committed_nqe_query.assert_called_once_with(
            repository="org",
            query_path=query_path,
            commit_id="full-commit",
            require_source_code=True,
        )

    def test_persisted_diff_contract_source_mismatch_falls_back_closed(self):
        full_source = """
@query
f(forward_netbox_shard_keys: List<String>) =
select {name: "vendor", slug: "vendor"}
"""
        diff_source = """
@query
f() =
select {name: "vendor", slug: "vendor"}
"""
        query_id = "OQ_vendor"
        query_path = "/validation/forward_device_vendors"
        netbox_model = ContentType.objects.get(app_label="dcim", model="manufacturer")
        query_map = ForwardNQEMap.objects.create(
            name="Forward Device Vendors",
            netbox_model=netbox_model,
            query_id=query_id,
            query_repository="org",
            query_path=query_path,
            commit_id="full-commit",
            diff_commit_id="diff-commit",
            full_source_sha256=query_source_sha256(full_source),
            diff_source_sha256="0" * 64,
            built_in=True,
            enabled=True,
        )
        client = Mock()
        client.get_committed_nqe_query.side_effect = lambda **kwargs: {
            "queryId": query_id,
            "path": query_path,
            "lastCommitId": kwargs["commit_id"],
            "sourceCode": (
                full_source if kwargs["commit_id"] == "full-commit" else diff_source
            ),
        }

        specs = resolve_query_specs_for_client(
            get_query_specs("dcim.manufacturer", maps=[query_map]),
            client,
        )
        contract = resolve_execution_contract(
            specs[0],
            effective_parameters={"forward_netbox_shard_keys": []},
        )

        self.assertFalse(contract.diff_eligible)
        self.assertEqual(contract.reason_code, "unverified_diff_source")

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

    def test_alias_variant_coverage_is_complete_and_intentional(self):
        """No alias-sensitive base map may ship without a variant or a reason."""
        self.assertEqual(alias_variant_coverage_violations(), [])

    def test_alias_variant_coverage_flags_a_missing_variant(self):
        """The check fails when a base map that needs a variant lacks one.

        Registering the alias-sensitive hardware-notice base map on its own --
        exactly the state the catalogue was in before 2.5.3 -- must be reported,
        not tolerated.
        """
        violations = alias_variant_coverage_violations(
            [
                {
                    "model_string": "netbox_dlm.hardwarenotice",
                    "name": "Forward DLM Hardware Notices",
                    "filename": "forward_dlm_hardware_notices.nqe",
                }
            ]
        )

        self.assertTrue(
            any(
                "forward_dlm_hardware_notices.nqe" in violation
                and "ALIAS_VARIANT_EXEMPT_QUERY_FILENAMES" in violation
                for violation in violations
            ),
            violations,
        )

    def test_alias_variant_coverage_flags_an_unresolvable_variant_name(self):
        """A variant whose display name has no base map would double-apply.

        ``_collapse_alias_variant_duplicates`` supersedes a base map by name. A
        variant named outside the " with NetBox Device Type Aliases" convention
        and missing from _EXPLICIT_ALIAS_VARIANT_BASE_NAMES silently runs
        alongside its base and flips the shared object every sync.
        """
        violations = alias_variant_coverage_violations(
            [
                {
                    "model_string": "netbox_dlm.hardwarenotice",
                    "name": "Forward DLM Hardware Notices",
                    "filename": "forward_dlm_hardware_notices.nqe",
                },
                {
                    "model_string": "netbox_dlm.hardwarenotice",
                    "name": "Forward DLM Hardware Notices (aliased)",
                    "filename": (
                        "forward_dlm_hardware_notices_with_netbox_aliases.nqe"
                    ),
                },
            ]
        )

        self.assertTrue(
            any(
                "does not resolve to a registered base map name" in violation
                for violation in violations
            ),
            violations,
        )

    def test_alias_variant_exemptions_are_live_and_reasoned(self):
        registered = {
            query_default["filename"]
            for query_default in BUILTIN_QUERY_MAPS + BUILTIN_OPTIONAL_QUERY_MAPS
        }

        self.assertTrue(ALIAS_VARIANT_EXEMPT_QUERY_FILENAMES)
        for filename, reason in ALIAS_VARIANT_EXEMPT_QUERY_FILENAMES.items():
            self.assertIn(filename, registered)
            self.assertNotIn(
                filename.removesuffix(".nqe") + "_with_netbox_aliases.nqe",
                registered,
            )
            # A reason has to explain, not label.
            self.assertGreater(len(reason.split()), 12, filename)

        # The CIMC firmware map is exempt by design, not by oversight: it emits
        # a hardcoded Platform its own adapter creates.
        self.assertIn(
            "forward_dlm_inventory_item_software.nqe",
            ALIAS_VARIANT_EXEMPT_QUERY_FILENAMES,
        )
        self.assertIn(
            "CIMC",
            ALIAS_VARIANT_EXEMPT_QUERY_FILENAMES[
                "forward_dlm_inventory_item_software.nqe"
            ],
        )

    def test_dlm_platform_maps_match_the_alias_device_query_verbatim(self):
        """Why the DLM platform maps need no alias variant, asserted not assumed.

        The alias-aware device query creates the Platform rows these maps look
        up, and it derives the platform name with the same shared helper as the
        base device query. If that ever diverges, the exemptions above stop
        being true and this test says so.
        """
        alias_device_query = read_builtin_query_source(
            "forward_devices_with_netbox_aliases.nqe"
        )
        base_device_query = read_builtin_query_source("forward_devices.nqe")
        platform_expression = "let platform_name = normalizeDevicePlatformName(device)"

        self.assertIn(platform_expression, alias_device_query)
        self.assertIn(platform_expression, base_device_query)
        # The alias data file has no platform record type to map through.
        self.assertNotIn("platform_alias", alias_device_query)

        for filename in (
            "forward_dlm_software_versions.nqe",
            "forward_dlm_device_software.nqe",
            "forward_dlm_vulnerabilities.nqe",
        ):
            self.assertIn(
                platform_expression,
                read_builtin_query_source(filename),
                filename,
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

    def test_optional_cimc_endpoint_inventory_map_is_seeded_disabled(self):
        rows = {
            (row["model_string"], row["name"]): row for row in builtin_nqe_map_rows()
        }

        row = rows[("dcim.inventoryitem", "Forward CIMC Endpoint Inventory")]

        self.assertFalse(row["enabled"])
        self.assertEqual(row["parameters"], {"forward_netbox_shard_keys": []})
        self.assertIn('matches(endpointNameLower, "*-cimc")', row["query"])
        self.assertIn("where device.name == parentName", row["query"])
        self.assertIn("device: device.name", row["query"])
        self.assertIn('name: "CIMC"', row["query"])

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
        cimc_row = rows[("dcim.inventoryitem", "Forward ACI APIC CIMC Inventory")]
        tenant_row = rows[("netbox_cisco_aci.acitenant", "Forward ACI Tenants")]
        vrf_row = rows[("netbox_cisco_aci.acivrf", "Forward ACI VRFs")]
        bd_row = rows[
            ("netbox_cisco_aci.acibridgedomain", "Forward ACI Bridge Domains")
        ]
        filter_row = rows[("netbox_cisco_aci.acifilter", "Forward ACI Filters")]
        l3out_row = rows[("netbox_cisco_aci.acil3out", "Forward ACI L3Outs")]

        aci_rows = (
            fabric_row,
            pod_row,
            node_row,
            apic_node_row,
            cimc_row,
            tenant_row,
            vrf_row,
            bd_row,
            filter_row,
            l3out_row,
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
        self.assertIn("CommandType.CUSTOM", cimc_row["query"])
        self.assertIn(
            'matches(toLowerCase(chassis_command.commandText), "moquery -c eqptch*")',
            cimc_row["query"],
        )
        self.assertIn("CISCO_APIC_CONTROLLER_DETAIL", cimc_row["query"])
        self.assertIn(
            "regexMatches(chassis_command.response, chassisRegex)", cimc_row["query"]
        )
        self.assertIn("cimcVersion", cimc_row["query"])
        self.assertIn("device: node.node_name", cimc_row["query"])
        self.assertIn(
            'matches(toLowerCase(command.commandText), "moquery -c fvctx*")',
            tenant_row["query"],
        )
        self.assertIn(
            'matches(toLowerCase(command.commandText), "moquery -c fvctx*")',
            vrf_row["query"],
        )
        self.assertIn("policy_enforcement_preference:", vrf_row["query"])
        self.assertIn("policy_enforcement_direction:", vrf_row["query"])
        self.assertIn("bd_enforcement_enabled:", vrf_row["query"])
        self.assertIn(
            'matches(toLowerCase(command.commandText), "moquery -c fvbd*")',
            bd_row["query"],
        )
        self.assertIn("arp_flooding_enabled:", bd_row["query"])
        self.assertIn("unicast_routing_enabled:", bd_row["query"])
        self.assertIn("limit_ip_learn_to_subnets:", bd_row["query"])
        self.assertIn("mac_address:", bd_row["query"])
        self.assertIn("CISCO_ACI_ZONING_FILTER", filter_row["query"])
        self.assertIn(
            'matches(toLowerCase(command.commandText), "moquery -c l3extinstp*")',
            l3out_row["query"],
        )
        self.assertIn("(?<matchT>", l3out_row["query"])
        self.assertIn("(?<pcEnfPref>", l3out_row["query"])
        self.assertIn("(?<prefGrMemb>", l3out_row["query"])
        self.assertIn("(?<target_dscp>", l3out_row["query"])
        self.assertNotIn(
            "Forward ACI Nodes",
            {query_default["name"] for query_default in BUILTIN_QUERY_MAPS},
        )
        self.assertTrue(
            {
                "Forward ACI Application Profiles",
                "Forward ACI Endpoint Groups",
                "Forward ACI Contracts",
                "Forward ACI Static Port Bindings",
            }.isdisjoint(
                {query_default["name"] for query_default in BUILTIN_OPTIONAL_QUERY_MAPS}
            )
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

    def test_dlm_post_migrate_seeds_optional_maps_after_late_install(self):
        for model in (
            "softwareversion",
            "hardwarenotice",
            "devicesoftware",
            "inventoryitemsoftware",
            "cve",
            "vulnerability",
        ):
            ContentType.objects.get_or_create(app_label="netbox_dlm", model=model)
        ForwardNQEMap.objects.filter(netbox_model__app_label="netbox_dlm").delete()

        seed_builtin_nqe_maps(type("Sender", (), {"label": "netbox_dlm"}))

        dlm_maps = ForwardNQEMap.objects.filter(
            netbox_model__app_label="netbox_dlm",
            built_in=True,
        )
        self.assertSetEqual(
            set(dlm_maps.values_list("name", flat=True)),
            {
                "Forward DLM Software Versions",
                "Forward DLM Hardware Notices",
                "Forward DLM Hardware Notices with NetBox Aliases",
                "Forward DLM Device Software",
                "Forward CIMC Inventory Item Software",
                "Forward ACI APIC CIMC Inventory Item Software",
                "Forward DLM CVEs",
                "Forward DLM Vulnerabilities",
            },
        )
        self.assertFalse(dlm_maps.filter(enabled=True).exists())

    @unittest.skipUnless(
        apps.is_installed("netbox_cisco_aci"), "netbox-cisco-aci is not installed"
    )
    def test_seed_builtin_maps_includes_installed_aci_models(self):
        seed_builtin_nqe_maps(type("Sender", (), {"label": "forward_netbox"}))

        aci_maps = ForwardNQEMap.objects.filter(
            netbox_model__app_label="netbox_cisco_aci",
            built_in=True,
        )
        self.assertGreater(aci_maps.count(), 0)
        self.assertFalse(aci_maps.filter(enabled=True).exists())

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

    def test_alias_device_query_wraps_endpoint_union_with_one_device_scan(self):
        # The alias-aware device query gains the SNMP-endpoint branch, so (like
        # the base forward_devices query) it wraps a `foreach row in ((devices)
        # + (endpoints))` union. One network.devices reference proves a bounded
        # source shape, not optimizer parallelism; only Forward Query Debug can
        # establish whether the compiled plan contains parallel_foreach.
        rows = {row["name"]: row for row in builtin_nqe_map_rows()}
        query = re.sub(
            r"/\*.*?\*/",
            "",
            rows["Forward Devices with NetBox Device Type Aliases"]["query"],
            flags=re.S,
        )
        self.assertIn("foreach row in (", query)
        self.assertIn("network.endpoints", query)
        self.assertEqual(
            query.count("network.devices"),
            1,
            msg="alias device query must keep one modeled-device scan.",
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
        self.assertIn(
            'description:\n      if isPresent(interface.description) && interface.description != ""',
            spec.query,
        )

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

    def test_device_feature_tag_query_syncs_operator_selected_tags(self):
        # 2.2.5: the default feature-tags map is param-driven — it emits exactly the
        # Forward device tags the operator selected in sync_device_tags, not a
        # hardcoded Prot_BGP. (BGP tagging moved to the routing plugin.)
        spec = next(
            spec
            for spec in BUILTIN_QUERY_SPECS["extras.taggeditem"]
            if spec.query_name == "Forward Device Feature Tags"
        )

        self.assertIn("sync_device_tags: List<String>", spec.query)
        self.assertIn("foreach tag in device.tagNames", spec.query)
        self.assertIn("where tag in sync_device_tags", spec.query)
        self.assertIn("tag: tag", spec.query)
        self.assertNotIn('tag: "Prot_BGP"', spec.query)
        self.assertNotIn("protocol.bgp", spec.query)
        self.assertIn("sync_device_tags", spec.parameters)
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

    def test_prefix_queries_derive_connected_subnets(self):
        # Prefixes derive from connected interface subnets (not routing tables),
        # exclude host addresses, and still respect the device tag + shard scope.
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

        # Connected derivation, not routing-table entries.
        self.assertIn("subinterface.ipv4.addresses", ipv4_spec.query)
        self.assertNotIn("ipv4Unicast", ipv4_spec.query)
        self.assertIn("where address.prefixLength < 32", ipv4_spec.query)
        self.assertIn("subinterface.ipv6.addresses", ipv6_spec.query)
        self.assertNotIn("ipv6Unicast", ipv6_spec.query)
        self.assertIn("where address.prefixLength < 128", ipv6_spec.query)
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
                "toString(row.prefix) in forward_netbox_shard_keys",
                spec.query,
            )
            self.assertIn("tag in device_tag_include_tags", spec.query)

    def test_ipaddress_query_excludes_unassignable_interface_addresses(self):
        specs = {
            spec.query_name: spec for spec in BUILTIN_QUERY_SPECS["ipam.ipaddress"]
        }
        ipv4_spec = specs["Forward IPv4 IP Addresses"]
        ipv6_spec = specs["Forward IPv6 IP Addresses"]

        # IPv4: four address sources (subinterface, bridge, tunnel, routed VLAN),
        # each excluding network and broadcast addresses; no IPv6 boundary checks.
        self.assertEqual(
            ipv4_spec.query.count(
                "where address.prefixLength >= 31 || address.ip != networkAddress"
            ),
            4,
        )
        self.assertEqual(
            ipv4_spec.query.count(
                "where address.prefixLength >= 31 || address.ip != broadcastAddress"
            ),
            4,
        )
        self.assertEqual(
            ipv4_spec.query.count(
                "where address.prefixLength >= 127 || address.ip != networkAddress"
            ),
            0,
        )

        # IPv6: four address sources excluding the subnet-router anycast address;
        # IPv6 has no broadcast, so no broadcast checks.
        self.assertEqual(
            ipv6_spec.query.count(
                "where address.prefixLength >= 127 || address.ip != networkAddress"
            ),
            4,
        )
        self.assertEqual(
            ipv6_spec.query.count("broadcastAddress"),
            0,
        )

        # Both families keep the host-IP dedup/aggregation pipeline.
        for spec in (ipv4_spec, ipv6_spec):
            self.assertIn("host_ip: address.ip", spec.query)
            self.assertIn("prefix_length: address.prefixLength", spec.query)
            self.assertIn(
                "group row as grouped_rows by row.host_ip as host_ip",
                spec.query,
            )
            self.assertIn(
                "let chosen_prefix_length = max(foreach candidate in grouped_rows",
                spec.query,
            )
            self.assertIn(
                "foreach row in candidate_rows(forward_netbox_shard_keys)",
                spec.query,
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
