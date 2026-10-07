# The support bundle answers the questions a customer report actually asks.
#
# A customer's report this cycle could not be matched to its bundle: a three-day-old
# scope report sat beside a same-day sync, the primary-IP count had no split to
# reconcile against the device list, 147 issues had to be tallied by hand to find
# that 130 were one rule, and a dependency release that stopped NetBox starting left
# no version in the file. These pin the figures that close each gap, and that the
# export still carries counts rather than names.
import json

from dcim.models import Device
from dcim.models import DeviceRole
from dcim.models import DeviceType
from dcim.models import Manufacturer
from dcim.models import Site
from django.test import TestCase
from extras.models import Tag

from forward_netbox.models import ForwardDeviceIdentity
from forward_netbox.models import ForwardIngestion
from forward_netbox.models import ForwardIngestionIssue
from forward_netbox.models import ForwardSource
from forward_netbox.models import ForwardSync
from forward_netbox.utilities.bundle_diagnostics import bundle_diagnostics
from forward_netbox.utilities.export_redaction import export_safe_payload
from forward_netbox.views import _environment_bundle_payload
from forward_netbox.views import _ingestion_issue_bundle_payload
from forward_netbox.views import _primary_ip_bundle_payload
from forward_netbox.views import _scope_reconciliation_bundle_payload


class BundleTriageDiagnosticsTest(TestCase):
    def setUp(self):
        self.site = Site.objects.create(name="Secret Street Site", slug="secret")
        mfr = Manufacturer.objects.create(name="Cisco", slug="cisco")
        self.device_type = DeviceType.objects.create(
            manufacturer=mfr, model="M1", slug="m1"
        )
        self.role = DeviceRole.objects.create(name="firewall", slug="firewall")
        source = ForwardSource.objects.create(
            name="src", url="https://fwd.example.invalid"
        )
        self.sync = ForwardSync.objects.create(name="sync", source=source)

    def _device(self, name, *, managed=True, tag=None):
        device = Device.objects.create(
            name=name,
            site=self.site,
            device_type=self.device_type,
            role=self.role,
            status="active",
        )
        if managed:
            ForwardDeviceIdentity.objects.create(
                sync=self.sync, device=device, source_device_key=name
            )
        if tag:
            device.tags.add(Tag.objects.get_or_create(name=tag, slug=tag.lower())[0])
        return device

    def test_the_primary_ip_count_is_split_so_it_can_be_reconciled(self):
        self._device("fw-uncovered", tag="forward-uncovered")
        self._device("fw-mgmt", tag="Mgmt_Vl211")
        self._device("fw-plain")
        self._device("unmanaged", managed=False)

        payload = _primary_ip_bundle_payload(self.sync)

        self.assertEqual(payload["sync_devices"], 3)
        self.assertEqual(payload["without_primary_ip"], 3)
        split = payload["without_primary_ip_split"]
        self.assertEqual(split["tagged_uncovered"], 1)
        self.assertEqual(split["with_a_mgmt_tag"], 1)
        self.assertEqual(split["with_no_ip_at_all"], 3)
        self.assertEqual(split["with_an_interface_ip"], 0)
        # The figure on the device list covers devices this sync does not own.
        self.assertEqual(payload["netbox_devices_without_primary_ip"], 4)

    def test_primary_addresses_are_counted_by_mask_and_host_masks_by_role(self):
        from ipam.models import IPAddress

        loopback = self._device("fw-loopback")
        subnet = self._device("fw-subnet")
        for device, address in ((loopback, "10.0.0.1/32"), (subnet, "10.0.1.1/24")):
            ip = IPAddress.objects.create(address=address)
            Device.objects.filter(pk=device.pk).update(primary_ip4=ip)

        masks = _primary_ip_bundle_payload(self.sync)["primary_ip4_prefix_lengths"]

        self.assertEqual(
            {row["prefix_length"]: row["devices"] for row in masks["by_prefix_length"]},
            {32: 1, 24: 1},
        )
        self.assertEqual(
            masks["host_mask_by_role"], [{"role": "firewall", "devices": 1}]
        )

    def test_interface_addresses_are_counted_by_mask_and_the_default_site_is_listed(
        self,
    ):
        from dcim.models import Interface
        from ipam.models import IPAddress

        default = Site.objects.create(name="Default", slug="default")
        endpoint = self._device("endpoint-1")
        Device.objects.filter(pk=endpoint.pk).update(site=default)
        switch = self._device("switch-1")
        for device, address in ((endpoint, "10.0.0.9/32"), (switch, "10.0.1.1/24")):
            interface = Interface.objects.create(
                device=device, name="Gi0/1", type="1000base-t"
            )
            IPAddress.objects.create(address=address, assigned_object=interface)

        payload = _primary_ip_bundle_payload(self.sync)

        masks = payload["interface_ip4_prefix_lengths"]
        self.assertEqual(
            {
                row["prefix_length"]: row["addresses"]
                for row in masks["by_prefix_length"]
            },
            {32: 1, 24: 1},
        )
        self.assertEqual(
            masks["host_mask_by_role"], [{"role": "firewall", "addresses": 1}]
        )
        placement = payload["site_placement"]
        self.assertEqual(placement["at_default_site"], 1)
        self.assertEqual(placement["at_default_site_by_role"], {"firewall": 1})
        self.assertEqual(placement["at_default_site_without_primary_ip"], 1)
        self.assertNotIn("endpoint-1", json.dumps(payload))

    def test_issues_are_tallied_by_model_exception_and_rule_over_every_row(self):
        ingestion = ForwardIngestion.objects.create(sync=self.sync)
        for _ in range(3):
            ForwardIngestionIssue.objects.create(
                ingestion=ingestion,
                model="dcim.interface",
                exception="ValidationError",
                message="Merge failed",
                raw_data={"validation_rules": ["untagged-vlan-outside-device-site"]},
            )
        ForwardIngestionIssue.objects.create(
            ingestion=ingestion,
            model="dcim.site",
            exception="ForwardDependencySkipError",
            message="skipped",
            raw_data={},
        )

        summary = _ingestion_issue_bundle_payload(ingestion)["summary"]

        self.assertEqual(summary["counted"], 4)
        self.assertEqual(
            summary["by_model_and_exception"][0],
            {"model": "dcim.interface", "exception": "ValidationError", "count": 3},
        )
        self.assertEqual(
            summary["by_validation_rule"],
            [
                {
                    "model": "dcim.interface",
                    "rule": "untagged-vlan-outside-device-site",
                    "count": 3,
                }
            ],
        )

    def test_the_environment_names_the_dependency_versions(self):
        deps = _environment_bundle_payload()["dependency_versions"]

        for name in ("scrapli", "scrapli-netconf", "forward-sdk", "django"):
            self.assertIn(name, deps)
        # A package that is not installed reads None, never an error.
        self.assertTrue(all(v is None or isinstance(v, str) for v in deps.values()))
        self.assertTrue(deps["django"])

    def test_a_missing_scope_report_has_no_age_rather_than_a_wrong_one(self):
        payload = _scope_reconciliation_bundle_payload(self.sync)

        self.assertIsNone(payload["generated_at"])
        self.assertIsNone(payload["age_hours"])
        self.assertIsNone(payload["older_than_last_sync"])

    def test_routing_name_collisions_reports_whether_the_plugin_is_there(self):
        out = bundle_diagnostics(self.sync)

        self.assertEqual(out["errors"], {})
        collisions = out["routing_name_collisions"]
        self.assertIn("installed", collisions)
        if collisions["installed"]:
            self.assertEqual(collisions["duplicate_name_groups"], 0)

    def test_the_new_figures_export_without_names(self):
        self._device("customer-firewall-01", tag="forward-uncovered")
        text = json.dumps(
            export_safe_payload(
                {
                    "primary_ip": _primary_ip_bundle_payload(self.sync),
                    "diagnostics": bundle_diagnostics(self.sync),
                    "environment": _environment_bundle_payload(),
                }
            ),
            default=str,
        )

        self.assertNotIn("customer-firewall-01", text)
        self.assertNotIn("Secret Street", text)
