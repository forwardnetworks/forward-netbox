import json
from io import StringIO

from django.core.management import call_command
from django.core.management.base import CommandError
from django.test import TestCase
from unittest.mock import patch

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
            matrix["fetch_contracts"]["dcim.interface"]["fetch_mode"],
            "nqe_column_filter",
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
            matrix["model_eligibility"]["dcim.devicerole"]["bulk_enabled"]["selected_engine"],
            "bulk_orm",
        )
        self.assertEqual(
            matrix["model_eligibility"]["dcim.devicerole"]["bulk_enabled"]["reason_code"],
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

    def test_architecture_audit_fail_on_gap_raises_command_error(self):
        with patch(
            "forward_netbox.management.commands.forward_architecture_audit.UNCLASSIFIED_SUPPORTED_MODELS",
            ("dcim.site",),
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
            "forward_netbox.management.commands.forward_architecture_audit.shard_fetch_capability_for_model",
            return_value={},
        ):
            with self.assertRaises(CommandError):
                call_command("forward_architecture_audit", "--fail-on-gap")
