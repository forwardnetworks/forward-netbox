import json
from io import StringIO
from unittest.mock import Mock
from unittest.mock import patch

from django.core.management import call_command
from django.test import TestCase

from forward_netbox.models import ForwardSource
from forward_netbox.models import ForwardSync


class ForwardApicCimcReadinessAuditCommandTest(TestCase):
    def setUp(self):
        self.source = ForwardSource.objects.create(
            name="apic-readiness-source",
            type="saas",
            url="https://fwd.app",
            status="ready",
            parameters={
                "username": "user@example.com",
                "password": "secret",
                "verify": True,
                "network_id": "net-1",
            },
        )
        self.sync = ForwardSync.objects.create(
            name="apic-readiness-sync",
            source=self.source,
            parameters={"snapshot_id": "latestProcessed"},
        )

    def _run(self, rows, **kwargs):
        client = Mock()
        client.run_nqe_query = Mock(return_value=rows)
        out = StringIO()
        with (
            patch.object(ForwardSource, "get_client", return_value=client),
            patch.object(ForwardSync, "resolve_snapshot_id", return_value="snap-1"),
        ):
            call_command(
                "forward_apic_cimc_readiness_audit",
                "--sync-name",
                "apic-readiness-sync",
                stdout=out,
                stderr=StringIO(),
                **kwargs,
            )
        return json.loads(out.getvalue())

    def test_reports_ready_when_completed_apic_has_eqptch(self):
        rows = [
            {"has_controller_detail": True, "has_eqptch": True, "completed": True},
            {"has_controller_detail": True, "has_eqptch": False, "completed": True},
        ]
        payload = self._run(rows)
        self.assertEqual(payload["apic_device_count"], 2)
        self.assertEqual(payload["with_controller_detail"], 2)
        self.assertEqual(payload["with_eqptch_command"], 1)
        self.assertEqual(payload["completed_with_eqptch"], 1)
        self.assertTrue(payload["cimc_inventory_ready"])
        self.assertEqual(payload["remediation"], "")

    def test_reports_not_ready_when_eqptch_only_on_backfilled_apic(self):
        rows = [
            {"has_controller_detail": True, "has_eqptch": True, "completed": False},
            {"has_controller_detail": True, "has_eqptch": False, "completed": True},
        ]
        payload = self._run(rows)
        self.assertFalse(payload["cimc_inventory_ready"])
        self.assertIn("eqptCh", payload["remediation"])
        self.assertIn("recurring custom command", payload["remediation"])

    def test_fail_on_missing_exits_nonzero(self):
        rows = [
            {"has_controller_detail": True, "has_eqptch": False, "completed": True},
        ]
        with self.assertRaises(SystemExit):
            self._run(rows, **{"fail_on_missing": True})
