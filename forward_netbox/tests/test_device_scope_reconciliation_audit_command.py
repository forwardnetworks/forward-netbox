import json
from io import StringIO
from unittest.mock import Mock
from unittest.mock import patch

from dcim.models import Device
from dcim.models import DeviceRole
from dcim.models import DeviceType
from dcim.models import Manufacturer
from dcim.models import Site
from django.core.management import call_command
from django.test import TestCase

from forward_netbox.models import ForwardSource
from forward_netbox.models import ForwardSync


class ForwardDeviceScopeReconciliationAuditCommandTest(TestCase):
    def setUp(self):
        self.source = ForwardSource.objects.create(
            name="recon-source",
            type="saas",
            url="https://fwd.app",
            status="ready",
            parameters={
                "username": "user@example.com",
                "password": "secret",
                "verify": True,
                "network_id": "net-1",
                "device_tag_include_tags": ["N.Patel"],
                "device_tag_include_match": "any",
            },
        )
        self.sync = ForwardSync.objects.create(
            name="recon-sync",
            source=self.source,
            parameters={"snapshot_id": "latestProcessed"},
        )
        mfr = Manufacturer.objects.create(name="MfrR", slug="mfr-r")
        self.dt = DeviceType.objects.create(manufacturer=mfr, model="dt-r", slug="dt-r")
        self.role = DeviceRole.objects.create(name="RoleR", slug="role-r")
        self.site = Site.objects.create(name="SiteR", slug="site-r")

    def _make_devices(self, *names):
        for name in names:
            Device.objects.create(
                name=name, device_type=self.dt, role=self.role, site=self.site
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
                "forward_device_scope_reconciliation_audit",
                "--sync-name",
                "recon-sync",
                stdout=out,
                stderr=StringIO(),
                **kwargs,
            )
        return json.loads(out.getvalue())

    def test_clean_when_netbox_matches_scope(self):
        self._make_devices("dev-a", "dev-b")
        rows = [
            {"name": "dev-a", "completed": True},
            {"name": "dev-b", "completed": True},
        ]
        payload = self._run(rows)
        self.assertEqual(payload["netbox_device_count"], 2)
        self.assertEqual(payload["forward_in_scope_completed"], 2)
        self.assertEqual(payload["netbox_out_of_scope"], 0)
        self.assertEqual(payload["remediation"], "")

    def test_reports_out_of_scope_and_backfilled(self):
        # dev-a/dev-b completed in scope, dev-c tagged but backfilled, dev-d stale.
        self._make_devices("dev-a", "dev-b", "dev-c", "dev-d")
        rows = [
            {"name": "dev-a", "completed": True},
            {"name": "dev-b", "completed": True},
            {"name": "dev-c", "completed": False},
        ]
        payload = self._run(rows)
        self.assertEqual(payload["forward_in_scope_completed"], 2)
        self.assertEqual(payload["forward_tagged_backfilled"], 1)
        self.assertEqual(payload["netbox_present_backfilled"], 1)
        self.assertEqual(payload["netbox_out_of_scope"], 1)
        self.assertEqual(payload["out_of_scope_sample"], ["dev-d"])
        self.assertEqual(payload["present_backfilled_sample"], ["dev-c"])
        self.assertIn("device_tag_prune_out_of_scope", payload["remediation"])

    def test_fail_on_drift_exits_nonzero(self):
        self._make_devices("dev-a", "dev-stale")
        rows = [{"name": "dev-a", "completed": True}]
        with self.assertRaises(SystemExit):
            self._run(rows, **{"fail_on_drift": True})
