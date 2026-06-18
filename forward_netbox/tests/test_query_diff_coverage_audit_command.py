import json
from io import StringIO

from django.contrib.contenttypes.models import ContentType
from django.core.management import call_command
from django.test import TestCase

from forward_netbox.models import ForwardNQEMap


class ForwardQueryDiffCoverageAuditCommandTest(TestCase):
    def setUp(self):
        # Start from a clean map set so the audit reflects only this test's maps
        # (the install seeds built-in maps via data migration).
        ForwardNQEMap.objects.all().delete()
        self.device_ct = ContentType.objects.get(app_label="dcim", model="device")
        self.site_ct = ContentType.objects.get(app_label="dcim", model="site")
        self.platform_ct = ContentType.objects.get(app_label="dcim", model="platform")

    def _run(self, **kwargs):
        out = StringIO()
        call_command(
            "forward_query_diff_coverage_audit", stdout=out, stderr=StringIO(), **kwargs
        )
        return json.loads(out.getvalue())

    def test_classifies_maps_by_diff_eligibility(self):
        ForwardNQEMap.objects.create(
            name="Diff via query_id",
            netbox_model=self.device_ct,
            query_id="Q_devices",
            enabled=True,
        )
        ForwardNQEMap.objects.create(
            name="Diff via query_path",
            netbox_model=self.site_ct,
            query_repository="org",
            query_path="/forward_netbox_validation/forward_locations",
            enabled=True,
        )
        ForwardNQEMap.objects.create(
            name="Full fetch only",
            netbox_model=self.platform_ct,
            query="foreach d in network.devices select {name: d.name};",
            enabled=True,
        )

        payload = self._run()

        self.assertEqual(payload["counts"]["diff_eligible"], 2)
        self.assertEqual(payload["counts"]["full_fetch_only"], 1)
        self.assertEqual(
            [m["name"] for m in payload["full_fetch_only"]], ["Full fetch only"]
        )
        self.assertIn("Org Repository query_path", payload["remediation"])

    def test_enabled_only_by_default(self):
        ForwardNQEMap.objects.create(
            name="Disabled raw map",
            netbox_model=self.platform_ct,
            query="foreach d in network.devices select {name: d.name};",
            enabled=False,
        )

        payload = self._run()
        self.assertEqual(payload["scope"], "enabled")
        self.assertEqual(payload["counts"]["total"], 0)

        payload_all = self._run(include_disabled=True)
        self.assertEqual(payload_all["scope"], "all")
        self.assertEqual(payload_all["counts"]["full_fetch_only"], 1)

    def test_fail_on_full_exits_nonzero(self):
        ForwardNQEMap.objects.create(
            name="Full fetch only",
            netbox_model=self.platform_ct,
            query="foreach d in network.devices select {name: d.name};",
            enabled=True,
        )

        with self.assertRaises(SystemExit):
            call_command(
                "forward_query_diff_coverage_audit",
                "--fail-on-full",
                stdout=StringIO(),
                stderr=StringIO(),
            )
