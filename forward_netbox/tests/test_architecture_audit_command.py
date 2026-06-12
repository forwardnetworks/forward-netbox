import json
import os
from io import StringIO
from pathlib import Path
from unittest.mock import Mock
from unittest.mock import patch

from django.apps import apps
from django.core.management import call_command
from django.core.management.base import CommandError
from django.test import TestCase

from forward_netbox.models import ForwardSource
from forward_netbox.models import ForwardSync
from forward_netbox.utilities.plugin_integrations import integration_capability_summary
from forward_netbox.utilities.plugin_integrations.registry import (
    _is_unsupported_version,
)


class ForwardArchitectureAuditCommandTest(TestCase):
    def setUp(self):
        self.source = ForwardSource.objects.create(
            name="audit-source",
            type="saas",
            url="https://fwd.app",
            status="ready",
            parameters={
                "username": "user@example.com",
                "password": "secret",
                "verify": True,
                "network_id": "test-network",
            },
        )
        self.sync = ForwardSync.objects.create(
            name="audit-sync",
            source=self.source,
            parameters={"snapshot_id": "latestProcessed"},
        )

    def test_architecture_audit_outputs_apply_engine_matrix(self):
        stream = StringIO()
        call_command("forward_architecture_audit", stdout=stream)
        payload = json.loads(stream.getvalue())
        matrix = payload["apply_engine_matrix"]
        validation_org_query_sync = payload["validation_org_query_sync"]
        self.assertIn("dcim.site", matrix["bulk_orm_safe_models"])
        self.assertIn("dcim.devicerole", matrix["bulk_orm_safe_models"])
        self.assertIn("dcim.platform", matrix["bulk_orm_safe_models"])
        self.assertIn("dcim.virtualchassis", matrix["bulk_orm_safe_models"])
        self.assertIn("dcim.interface", matrix["adapter_required_models"])
        self.assertIn("netbox_cisco_aci.acinode", matrix["adapter_required_models"])
        self.assertNotIn("dcim.virtualchassis", matrix["adapter_required_models"])
        self.assertIn("aci.netbox_cisco_aci", matrix["optional_plugin_integrations"])
        aci_integration = matrix["optional_plugin_integrations"]["aci.netbox_cisco_aci"]
        self.assertIn("routing.netbox_routing", matrix["optional_plugin_integrations"])
        routing_integration = matrix["optional_plugin_integrations"][
            "routing.netbox_routing"
        ]
        self.assertIn(
            "peering.netbox_peering_manager",
            matrix["optional_plugin_integrations"],
        )
        peering_integration = matrix["optional_plugin_integrations"][
            "peering.netbox_peering_manager"
        ]
        self.assertIn("aci.netbox_cisco_aci", matrix["optional_plugin_capabilities"])
        self.assertIn(
            "aci.netbox_cisco_aci",
            matrix["optional_plugin_adapter_contracts"],
        )
        aci_capabilities = matrix["optional_plugin_capabilities"][
            "aci.netbox_cisco_aci"
        ]
        aci_adapter_contract = matrix["optional_plugin_adapter_contracts"][
            "aci.netbox_cisco_aci"
        ]
        self.assertIn("routing.netbox_routing", matrix["optional_plugin_capabilities"])
        routing_capabilities = matrix["optional_plugin_capabilities"][
            "routing.netbox_routing"
        ]
        self.assertIn(
            "peering.netbox_peering_manager",
            matrix["optional_plugin_capabilities"],
        )
        peering_capabilities = matrix["optional_plugin_capabilities"][
            "peering.netbox_peering_manager"
        ]
        self.assertEqual(aci_integration["app_label"], "netbox_cisco_aci")
        self.assertFalse(aci_integration["enabled_by_default"])
        self.assertIn(
            "netbox_cisco_aci.acinode",
            aci_integration["supported_models"],
        )
        self.assertNotIn(
            "dcim.inventoryitem",
            aci_integration["supported_models"],
        )
        self.assertEqual(aci_integration["native_models"], ["dcim.inventoryitem"])
        self.assertIn(
            "netbox_cisco_aci.aciendpointgroup",
            aci_integration["supported_models"],
        )
        self.assertIn(
            "netbox_cisco_aci.acil3outinterface",
            aci_integration["discovery_models"],
        )
        self.assertEqual(
            [entry["command_type"] for entry in aci_integration["command_inventory"]],
            [
                "CISCO_APIC_SWITCH",
                "CISCO_APIC_CONTROLLER_DETAIL",
                "CUSTOM",
                "CISCO_ACI_FABRIC_NODES",
                "CISCO_ACI_FABRIC_VRFS",
                "CISCO_ACI_NODE_TYPE",
                "CISCO_ACI_SPINE_INTERFACE_MODE",
                "CISCO_ACI_SPINE_IP_ENDPOINTS",
                "CISCO_ACI_SPINE_TUNNELS_NEXTHOP",
                "CISCO_ACI_TUNNELS_ENDPOINTS",
                "CISCO_ACI_ZONING_RULE",
                "CISCO_ACI_ZONING_FILTER",
                "CISCO_ACI_EPM_ENDPOINTS",
                "CISCO_APIC_VLAN_LEAF",
                "CISCO_APIC_EXT_L3OUT_INTERFACES",
                "CISCO_APIC_VLAN_STATUS_LEAF",
                "CISCO_APIC_VPC_MAP",
            ],
        )
        command_inventory_by_type = {
            entry["command_type"]: entry
            for entry in aci_integration["command_inventory"]
        }
        self.assertEqual(
            command_inventory_by_type["CISCO_ACI_FABRIC_NODES"]["status"],
            "current",
        )
        self.assertEqual(
            command_inventory_by_type["CISCO_ACI_ZONING_FILTER"]["status"],
            "current",
        )
        self.assertEqual(
            command_inventory_by_type["CISCO_APIC_CONTROLLER_DETAIL"]["status"],
            "current",
        )
        self.assertEqual(aci_capabilities["app_label"], "netbox_cisco_aci")
        self.assertEqual(
            aci_capabilities["installed"],
            apps.is_installed("netbox_cisco_aci"),
        )
        self.assertEqual(
            aci_capabilities["available"],
            bool(
                aci_capabilities["installed"]
                and not aci_capabilities["missing_required"]
            ),
        )
        self.assertEqual(aci_capabilities["availability_status"], "not_installed")
        self.assertEqual(
            aci_capabilities["availability_reason"],
            "Target plugin app is not installed.",
        )
        self.assertEqual(
            aci_capabilities["package_names"],
            [
                "netbox-cisco-aci",
                "netbox_aci_plugin",
                "netbox-aci-plugin",
                "netbox-aci",
            ],
        )
        self.assertIsNone(aci_capabilities["installed_package_name"])
        self.assertEqual(aci_capabilities["minimum_version"], "0.2.2")
        self.assertIsNone(aci_capabilities["version"])
        self.assertFalse(aci_capabilities["unsupported_version"])
        self.assertEqual(aci_capabilities["required_model_count"], 12)
        self.assertEqual(aci_capabilities["supported_model_count"], 12)
        self.assertEqual(aci_capabilities["native_model_count"], 1)
        self.assertEqual(aci_capabilities["native_models"], ["dcim.inventoryitem"])
        self.assertEqual(aci_capabilities["discovery_model_count"], 10)
        self.assertEqual(aci_capabilities["future_model_count"], 5)
        self.assertEqual(aci_capabilities["query_map_count"], 14)
        self.assertEqual(aci_capabilities["command_inventory_count"], 17)
        self.assertEqual(aci_adapter_contract["status"], "pass")
        self.assertEqual(aci_adapter_contract["gaps"], [])
        self.assertEqual(
            aci_adapter_contract["adapter_module"],
            "forward_netbox.utilities.sync_aci",
        )
        self.assertEqual(
            matrix["optional_plugin_adapter_contracts"]["routing.netbox_routing"][
                "status"
            ],
            "pass",
        )
        self.assertEqual(
            matrix["optional_plugin_adapter_contracts"][
                "peering.netbox_peering_manager"
            ]["status"],
            "pass",
        )
        self.assertEqual(routing_integration["display_name"], "NetBox Routing")
        self.assertEqual(peering_integration["display_name"], "NetBox Peering Manager")
        self.assertEqual(routing_capabilities["display_name"], "NetBox Routing")
        self.assertEqual(peering_capabilities["display_name"], "NetBox Peering Manager")
        self.assertEqual(
            routing_capabilities["installed"], apps.is_installed("netbox_routing")
        )
        self.assertEqual(
            routing_capabilities["available"],
            bool(
                routing_capabilities["installed"]
                and not routing_capabilities["missing_required"]
            ),
        )
        self.assertEqual(
            peering_capabilities["installed"],
            apps.is_installed("netbox_peering_manager"),
        )
        self.assertEqual(validation_org_query_sync["status"], "skipped")
        self.assertIn(
            "Validation-org credentials are not configured.",
            validation_org_query_sync["reason"],
        )
        self.assertEqual(
            peering_capabilities["available"],
            bool(
                peering_capabilities["installed"]
                and not peering_capabilities["missing_required"]
            ),
        )
        if aci_capabilities["installed"]:
            self.assertEqual(aci_capabilities["required_models_missing"], [])
            self.assertEqual(aci_capabilities["supported_models_missing"], [])
            self.assertEqual(aci_capabilities["discovery_models_missing"], [])
            self.assertEqual(aci_capabilities["missing_required"], [])
            self.assertEqual(aci_capabilities["missing_optional"], [])
        else:
            self.assertEqual(
                sorted(aci_capabilities["required_models_missing"]),
                sorted(aci_integration["required_models"]),
            )
            self.assertEqual(
                sorted(aci_capabilities["supported_models_missing"]),
                sorted(aci_integration["supported_models"]),
            )
            self.assertEqual(
                sorted(aci_capabilities["discovery_models_missing"]),
                sorted(aci_integration["discovery_models"]),
            )
            self.assertEqual(
                sorted(aci_capabilities["missing_required"]),
                sorted(aci_integration["required_models"]),
            )
            self.assertEqual(
                sorted(aci_capabilities["missing_optional"]),
                sorted(
                    list(aci_integration["discovery_models"])
                    + list(aci_integration["future_models"])
                ),
            )

    @patch(
        "forward_netbox.management.commands.forward_architecture_audit.builtin_query_repository_sync_summary"
    )
    @patch(
        "forward_netbox.management.commands.forward_architecture_audit.build_validation_org_query_source"
    )
    def test_architecture_audit_includes_validation_org_query_sync_when_configured(
        self,
        build_validation_org_query_source,
        builtin_query_repository_sync_summary,
    ):
        source = Mock()
        source.validate_connection.return_value = None
        source.get_client.return_value = Mock()
        build_validation_org_query_source.return_value = source
        builtin_query_repository_sync_summary.return_value = {
            "status": "pass",
            "repository": "org",
            "directory": "/forward_netbox_validation/",
            "query_count": 1,
            "published_count": 1,
            "matched_count": 1,
            "missing_count": 0,
            "stale_count": 0,
            "lookup_error_count": 0,
            "query_contract_summary": {"status": "pass", "gaps": []},
            "matched": [],
            "missing": [],
            "stale": [],
            "lookup_errors": [],
            "gaps": [],
        }

        with patch.dict(
            os.environ,
            {
                "FORWARD_VALIDATION_USERNAME": "user@example.com",
                "FORWARD_VALIDATION_PASSWORD": "secret",
                "FORWARD_VALIDATION_NETWORK_ID": "network-1",
                "FORWARD_VALIDATION_SOURCE_NAME": "validation-source",
                "FORWARD_VALIDATION_URL": "https://fwd.app",
                "FORWARD_VALIDATION_REPOSITORY": "org",
                "FORWARD_VALIDATION_DIRECTORY": "/forward_netbox_validation/",
            },
            clear=False,
        ):
            stream = StringIO()
            call_command("forward_architecture_audit", stdout=stream)

        payload = json.loads(stream.getvalue())
        matrix = payload["apply_engine_matrix"]
        self.assertEqual(payload["validation_org_query_sync"]["status"], "pass")
        build_validation_org_query_source.assert_called_once()
        builtin_query_repository_sync_summary.assert_called_once()
        self.assertIn(
            "aci.netbox_cisco_aci",
            matrix["optional_plugin_query_contract_registry"],
        )
        optional_query_contract_registry = matrix[
            "optional_plugin_query_contract_registry"
        ]["aci.netbox_cisco_aci"]
        self.assertEqual(optional_query_contract_registry["status"], "pass")
        self.assertEqual(optional_query_contract_registry["gaps"], [])
        self.assertEqual(
            optional_query_contract_registry["models"]["netbox_cisco_aci.acinode"][
                "fetch_mode"
            ],
            "nqe_parameters",
        )
        cimc_query_contract = optional_query_contract_registry["models"][
            "dcim.inventoryitem"
        ]
        self.assertEqual(cimc_query_contract["fetch_mode"], "nqe_parameters")
        self.assertEqual(cimc_query_contract["query_count"], 1)
        self.assertEqual(
            cimc_query_contract["queries"][0]["query_name"],
            "Forward ACI APIC CIMC Inventory",
        )
        self.assertFalse(cimc_query_contract["queries"][0]["enabled_by_default"])
        self.assertTrue(cimc_query_contract["queries"][0]["declares_shard_parameter"])
        self.assertTrue(
            cimc_query_contract["queries"][0]["seeds_empty_shard_parameter"]
        )
        self.assertEqual(
            matrix["adapter_blockers"]["dcim.interface"],
            "relationship_side_effects",
        )
        self.assertEqual(
            matrix["classification_gaps"]["unclassified_supported_models"],
            [],
        )
        self.assertEqual(
            matrix["classification_gaps"]["adapter_models_without_blocker"],
            [],
        )
        self.assertEqual(
            matrix["classification_gaps"]["bulk_orm_enabled_models_without_specs"],
            [],
        )
        self.assertEqual(
            matrix["classification_gaps"]["decision_unclassified_fallback_models"],
            [],
        )
        self.assertEqual(
            matrix["classification_gaps"]["fetch_contract_coverage_gaps"],
            [],
        )
        self.assertEqual(
            matrix["classification_gaps"]["model_contract_registry_gaps"],
            [],
        )
        self.assertEqual(
            matrix["classification_gaps"]["query_contract_registry_gaps"],
            [],
        )
        self.assertEqual(
            matrix["classification_gaps"][
                "optional_plugin_query_contract_registry_gaps"
            ]["aci.netbox_cisco_aci"],
            [],
        )
        self.assertEqual(
            matrix["classification_gaps"]["optional_plugin_adapter_contract_gaps"][
                "aci.netbox_cisco_aci"
            ],
            [],
        )
        self.assertEqual(matrix["model_contract_registry"]["status"], "pass")
        self.assertEqual(matrix["query_contract_registry"]["status"], "pass")
        self.assertEqual(
            matrix["query_contract_registry"]["models"]["ipam.prefix"]["fetch_mode"],
            "nqe_parameters",
        )
        self.assertIn("dcim.interface", matrix["model_contract_registry"]["contracts"])
        interface_contract = matrix["model_contract_registry"]["contracts"][
            "dcim.interface"
        ]
        for model_string, contract in matrix["model_contract_registry"][
            "contracts"
        ].items():
            self.assertEqual(contract["field_ownership"]["model"], model_string)
            self.assertIn(
                contract["field_ownership"]["blank_update_policy"],
                {
                    "authoritative_for_declared_fields",
                    "preserve_configured_fields",
                },
            )
        self.assertEqual(interface_contract["model"], "dcim.interface")
        self.assertEqual(
            interface_contract["fetch_contract"]["fetch_mode"],
            "nqe_parameters",
        )
        self.assertEqual(
            interface_contract["apply_engine_classification"],
            "adapter_required",
        )
        self.assertEqual(
            interface_contract["apply_engine_blocker_code"],
            "relationship_side_effects",
        )
        self.assertEqual(
            interface_contract["preserve_existing_on_blank_fields"],
            ["description", "mtu", "speed"],
        )
        self.assertEqual(
            interface_contract["field_ownership"]["preserve_existing_on_blank_fields"],
            ["description", "mtu", "speed"],
        )
        self.assertEqual(
            matrix["model_contract_registry"]["contracts"]["dcim.device"][
                "preserve_existing_on_blank_fields"
            ],
            ["serial"],
        )
        self.assertEqual(
            matrix["model_contract_registry"]["contracts"]["dcim.inventoryitem"][
                "preserve_existing_on_blank_fields"
            ],
            ["asset_tag", "description", "label", "part_id", "serial"],
        )
        self.assertIsNotNone(interface_contract["delete_dependency_rank"])
        self.assertIn("device", interface_contract["support_diagnostic_fields"])
        self.assertIn("name", interface_contract["support_diagnostic_fields"])
        self.assertEqual(
            matrix["bulk_orm_expansion"]["status"],
            "blocked_pending_parity",
        )
        self.assertGreaterEqual(
            len(matrix["bulk_orm_expansion"]["parity_gates"]),
            5,
        )
        self.assertGreater(
            matrix["bulk_orm_expansion"]["blocked_model_count"],
            0,
        )
        self.assertEqual(
            matrix["bulk_orm_expansion"]["promotion_lanes"][0]["lane"],
            "dependency_anchored_models",
        )
        self.assertEqual(
            matrix["bulk_orm_expansion"]["recommended_next_models"][0]["model"],
            "dcim.device",
        )
        self.assertEqual(
            matrix["bulk_orm_expansion"]["parity_plan"]["status"],
            "pending_candidate_parity",
        )
        self.assertEqual(
            matrix["bulk_orm_expansion"]["parity_plan"]["candidates"][0]["model"],
            "dcim.device",
        )
        self.assertIn(
            "ForwardApplyEngineParityTest.test_dcim_device_create_parity",
            matrix["bulk_orm_expansion"]["parity_plan"]["candidates"][0][
                "required_test_ids"
            ],
        )
        self.assertEqual(
            matrix["bulk_orm_expansion"]["high_impact_blocked_models"][0]["model"],
            "dcim.device",
        )
        self.assertEqual(
            matrix["fetch_contracts"]["dcim.interface"]["fetch_mode"],
            "nqe_parameters",
        )
        self.assertTrue(matrix["fetch_contracts"]["dcim.interface"]["shard_safe"])
        self.assertEqual(
            matrix["model_eligibility"]["dcim.site"]["default"]["selected_engine"],
            "adapter",
        )
        self.assertEqual(
            matrix["model_eligibility"]["dcim.site"]["bulk_enabled"]["selected_engine"],
            "bulk_orm",
        )
        self.assertEqual(
            matrix["model_eligibility"]["dcim.devicerole"]["bulk_enabled"][
                "selected_engine"
            ],
            "bulk_orm",
        )
        self.assertEqual(
            matrix["model_eligibility"]["dcim.devicerole"]["bulk_enabled"][
                "reason_code"
            ],
            "bulk_orm_enabled_safe_model_set",
        )
        self.assertIsNone(payload["sync_evidence"])

    def test_aci_version_gate_compares_versions(self):
        self.assertTrue(_is_unsupported_version("0.2.1", "0.2.2"))
        self.assertFalse(_is_unsupported_version("0.2.2", "0.2.2"))
        self.assertFalse(_is_unsupported_version("0.2.3", "0.2.2"))
        self.assertFalse(_is_unsupported_version(None, "0.2.2"))
        self.assertFalse(_is_unsupported_version("not-a-version", "0.2.2"))

    def test_aci_capability_summary_marks_old_plugin_as_unsupported(self):
        with patch(
            "forward_netbox.utilities.plugin_integrations.registry.apps.is_installed",
            return_value=True,
        ), patch(
            "forward_netbox.utilities.plugin_integrations.registry._present_models",
            side_effect=lambda models: sorted(models),
        ), patch(
            "forward_netbox.utilities.plugin_integrations.registry._missing_models",
            return_value=[],
        ), patch(
            "forward_netbox.utilities.plugin_integrations.registry.metadata.version",
            return_value="0.2.1",
        ):
            summary = integration_capability_summary()

        aci_capabilities = summary["aci.netbox_cisco_aci"]
        self.assertEqual(aci_capabilities["version"], "0.2.1")
        self.assertEqual(aci_capabilities["minimum_version"], "0.2.2")
        self.assertEqual(aci_capabilities["availability_status"], "unsupported_version")
        self.assertEqual(
            aci_capabilities["availability_reason"],
            "Installed plugin version is below the supported minimum 0.2.2.",
        )
        self.assertEqual(
            aci_capabilities["installed_package_name"],
            "netbox-cisco-aci",
        )
        self.assertTrue(aci_capabilities["unsupported_version"])
        self.assertFalse(aci_capabilities["available"])

    def test_architecture_audit_includes_sync_evidence(self):
        stream = StringIO()
        call_command(
            "forward_architecture_audit",
            "--sync-name",
            self.sync.name,
            stdout=stream,
        )
        payload = json.loads(stream.getvalue())
        self.assertEqual(payload["sync_evidence"]["sync_name"], self.sync.name)
        self.assertIn(
            "sync_health_summary",
            payload["sync_evidence"],
        )

    def test_architecture_audit_writes_relative_output_under_repo_root(self):
        repo_root = Path(__file__).resolve().parents[2]
        rel_path = "docs/03_Plans/evidence/test-architecture-audit-output.json"
        output_path = repo_root / rel_path
        output_path.unlink(missing_ok=True)
        try:
            call_command("forward_architecture_audit", "--output-json", rel_path)
            payload = json.loads(output_path.read_text(encoding="utf-8"))
        finally:
            output_path.unlink(missing_ok=True)

        self.assertIn("apply_engine_matrix", payload)
        self.assertIsNone(payload["sync_evidence"])

    def test_architecture_audit_fail_on_gap_raises_command_error(self):
        with patch(
            "forward_netbox.management.commands.forward_architecture_audit.architecture_unclassified_supported_models",
            return_value=["dcim.site"],
        ):
            with self.assertRaises(CommandError):
                call_command("forward_architecture_audit", "--fail-on-gap")

    def test_architecture_audit_fail_on_gap_raises_on_decision_fallback(self):
        with patch(
            "forward_netbox.management.commands.forward_architecture_audit.apply_engine_decision_for"
        ) as decision:

            def _decision(*, sync, model_string, backend):
                if model_string == "dcim.site":
                    return type(
                        "Decision",
                        (),
                        {
                            "selected_engine": "adapter",
                            "reason_code": "adapter_default_unclassified_model",
                        },
                    )()
                return type(
                    "Decision",
                    (),
                    {
                        "selected_engine": "adapter",
                        "reason_code": "adapter_required_model_contract",
                    },
                )()

            decision.side_effect = _decision
            with self.assertRaises(CommandError):
                call_command("forward_architecture_audit", "--fail-on-gap")

    def test_architecture_audit_fail_on_gap_raises_on_fetch_contract_gap(self):
        with patch(
            "forward_netbox.management.commands.forward_architecture_audit.architecture_fetch_contracts",
            return_value={"dcim.site": {}},
        ):
            with self.assertRaises(CommandError):
                call_command("forward_architecture_audit", "--fail-on-gap")

    def test_architecture_audit_fail_on_gap_raises_on_model_contract_gap(self):
        with patch(
            "forward_netbox.management.commands.forward_architecture_audit.architecture_contract_summary",
            return_value={
                "status": "fail",
                "contract_count": 1,
                "models": ["dcim.site"],
                "contracts": {},
                "gaps": [
                    {
                        "model": "dcim.site",
                        "code": "missing_delete_dependency_rank",
                        "message": "missing rank",
                    }
                ],
            },
        ):
            with self.assertRaises(CommandError):
                call_command("forward_architecture_audit", "--fail-on-gap")

    def test_architecture_audit_fail_on_gap_raises_on_query_contract_gap(self):
        with patch(
            "forward_netbox.management.commands.forward_architecture_audit.builtin_query_contract_summary",
            return_value={
                "status": "fail",
                "model_count": 1,
                "models": {},
                "gaps": [
                    {
                        "model": "ipam.prefix",
                        "query_name": "Forward IPv4 Prefixes",
                        "filename": "forward_prefixes_ipv4.nqe",
                        "code": "missing_positive_shard_predicate",
                        "message": "missing predicate",
                    }
                ],
            },
        ):
            with self.assertRaises(CommandError):
                call_command("forward_architecture_audit", "--fail-on-gap")
