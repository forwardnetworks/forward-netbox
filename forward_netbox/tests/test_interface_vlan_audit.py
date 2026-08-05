# An operator has to be able to find the interface a sync refused.
#
# A customer sync recorded `untagged-vlan-outside-device-site` against
# `dcim.interface` and nothing else: the message is composed by the recorder and
# the context is reduced to key names, because persisted diagnostics carry schema
# identifiers and never customer data. Correct policy, and it left one bad
# interface among tens of thousands with no way to locate it.
from dcim.models import Device
from dcim.models import DeviceRole
from dcim.models import DeviceType
from dcim.models import Interface
from dcim.models import Manufacturer
from dcim.models import Site
from django.test import TestCase
from ipam.models import VLAN

from forward_netbox.utilities.interface_vlan_audit import audit_interface_untagged_vlans


class InterfaceUntaggedVlanAuditTest(TestCase):
    def setUp(self):
        self.site = Site.objects.create(name="site-1", slug="site-1")
        self.other_site = Site.objects.create(name="site-2", slug="site-2")
        manufacturer = Manufacturer.objects.create(name="vendor-1", slug="vendor-1")
        self.device_type = DeviceType.objects.create(
            manufacturer=manufacturer, model="model-1", slug="model-1"
        )
        self.role = DeviceRole.objects.create(
            name="role-1", slug="role-1", color="9e9e9e"
        )

    def _device(self, name, site=None):
        return Device.objects.create(
            name=name,
            site=site or self.site,
            role=self.role,
            device_type=self.device_type,
            status="active",
        )

    def _interface(self, device, name, *, vlan=None, mode="access"):
        """Build the state directly, because the model refuses to build it.

        `save()` nulls an untagged VLAN when mode is unset and `full_clean()`
        rejects a cross-site one, so a queryset update is the only way to
        reproduce what a writer bypassing validation leaves behind — which is
        exactly how these rows arise in the field.
        """
        interface = Interface.objects.create(
            device=device, name=name, type="1000base-t", mode=mode
        )
        if vlan is not None:
            Interface.objects.filter(pk=interface.pk).update(untagged_vlan=vlan)
        return interface

    def test_a_clean_deployment_reports_nothing(self):
        device = self._device("device-1")
        vlan = VLAN.objects.create(
            site=self.site, vid=10, name="local", status="active"
        )
        self._interface(device, "eth1", vlan=vlan)

        payload = audit_interface_untagged_vlans()

        self.assertEqual(0, payload["cross_site_count"])
        self.assertEqual(0, payload["no_mode_count"])
        self.assertNotIn("cross_site_remediation", payload)

    def test_a_global_vlan_is_valid_on_any_device(self):
        # `untagged_vlan.site not in [device.site, None]` — a VLAN with no site
        # is explicitly allowed, so it must not be reported.
        device = self._device("device-1")
        vlan = VLAN.objects.create(site=None, vid=11, name="global", status="active")
        self._interface(device, "eth1", vlan=vlan)

        self.assertEqual(0, audit_interface_untagged_vlans()["cross_site_count"])

    def test_a_cross_site_vlan_is_reported_with_both_sites_named(self):
        device = self._device("device-1")
        vlan = VLAN.objects.create(
            site=self.other_site, vid=30, name="elsewhere", status="active"
        )
        self._interface(device, "eth1", vlan=vlan)

        payload = audit_interface_untagged_vlans()

        self.assertEqual(1, payload["cross_site_count"])
        (row,) = payload["cross_site"]
        # Naming the interface is the entire point; naming both sites is what
        # makes it actionable without a second lookup.
        self.assertEqual("device-1", row["device"])
        self.assertEqual("eth1", row["interface"])
        self.assertEqual("site-1", row["device_site"])
        self.assertEqual("site-2", row["vlan_site"])
        self.assertEqual(30, row["vlan_vid"])
        self.assertIn("cross_site_remediation", payload)

    def test_an_untagged_vlan_without_mode_is_reported_separately(self):
        device = self._device("device-1")
        vlan = VLAN.objects.create(
            site=self.site, vid=12, name="local", status="active"
        )
        self._interface(device, "eth1", vlan=vlan, mode="")

        payload = audit_interface_untagged_vlans()

        # Same site, so it is not a cross-site violation - but NetBox still
        # refuses it, and the two rules have different fixes.
        self.assertEqual(0, payload["cross_site_count"])
        self.assertEqual(1, payload["no_mode_count"])
        self.assertEqual("eth1", payload["no_mode"][0]["interface"])
        self.assertIn("no_mode_remediation", payload)

    def test_counts_are_exact_while_the_listing_is_bounded(self):
        # The command has to be runnable on a deployment with tens of thousands
        # of interfaces, so the sample is capped and the count is not.
        device = self._device("device-1")
        vlan = VLAN.objects.create(
            site=self.other_site, vid=31, name="elsewhere", status="active"
        )
        for index in range(5):
            self._interface(device, f"eth{index}", vlan=vlan)

        payload = audit_interface_untagged_vlans(sample_limit=2)

        self.assertEqual(5, payload["cross_site_count"])
        self.assertEqual(2, len(payload["cross_site"]))

    def test_a_zero_limit_still_counts(self):
        device = self._device("device-1")
        vlan = VLAN.objects.create(
            site=self.other_site, vid=32, name="elsewhere", status="active"
        )
        self._interface(device, "eth1", vlan=vlan)

        payload = audit_interface_untagged_vlans(sample_limit=0)

        self.assertEqual(1, payload["cross_site_count"])
        self.assertEqual([], payload["cross_site"])

    def test_an_interface_with_no_untagged_vlan_is_never_reported(self):
        device = self._device("device-1")
        self._interface(device, "eth1", mode="")

        payload = audit_interface_untagged_vlans()

        self.assertEqual(0, payload["cross_site_count"])
        self.assertEqual(0, payload["no_mode_count"])
