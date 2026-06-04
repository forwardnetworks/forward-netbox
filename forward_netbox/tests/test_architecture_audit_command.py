import json
from io import StringIO
from pathlib import Path
from unittest.mock import patch

from django.core.management import call_command
from django.core.management.base import CommandError
from django.test import TestCase

from forward_netbox.models import ForwardSource
from forward_netbox.models import ForwardSync


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
        self.assertIn("dcim.site", matrix["bulk_orm_safe_models"])
        self.assertIn("dcim.devicerole", matrix["bulk_orm_safe_models"])
        self.assertIn("dcim.platform", matrix["bulk_orm_safe_models"])
        self.assertIn("dcim.virtualchassis", matrix["bulk_orm_safe_models"])
        self.assertIn("dcim.interface", matrix["adapter_required_models"])
        self.assertNotIn("dcim.virtualchassis", matrix["adapter_required_models"])
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
        self.assertEqual(matrix["model_contract_registry"]["status"], "pass")
        self.assertIn("dcim.interface", matrix["model_contract_registry"]["contracts"])
        interface_contract = matrix["model_contract_registry"]["contracts"][
            "dcim.interface"
        ]
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
