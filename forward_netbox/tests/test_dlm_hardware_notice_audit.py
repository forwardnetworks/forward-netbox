# A customer reported "duplicate entries in the hardware notices": the same
# hardware under Forward's part number and under the NetBox Device Type Library
# name, each with a notice, neither ever collected. Removals reach NetBox only
# from a Forward diff of the query now in use, so nothing revisits rows the
# previous query wrote.
#
# The first version of this audit asked "does the device type hold any
# devices?" and was WRONG, in a way worth pinning so it cannot come back. At
# that customer it flagged 33 notices of which only 5 were stale, because:
#
#   1. A Device Type Library import leaves thousands of legitimately empty
#      device types - 5879 there - so emptiness is the ordinary state.
#   2. Notices are written network-wide while devices are imported TAG-SCOPED.
#      Hardware present in Forward but outside the include tags permanently has
#      zero devices in NetBox, and its notice is correct, not stale.
#
# Applying that rule would have deleted 20 notices Forward re-creates on the
# next sync, plus any comments or journal entries on them.
#
# The signal that works: does Forward still emit a notice for that device type?
from dcim.models import Device
from dcim.models import DeviceRole
from dcim.models import DeviceType
from dcim.models import Manufacturer
from dcim.models import Site
from django.apps import apps
from django.test import tag
from django.test import TestCase

from forward_netbox.utilities.dlm_notice_audit import delete_stale_hardware_notices
from forward_netbox.utilities.dlm_notice_audit import emitted_device_type_slugs
from forward_netbox.utilities.dlm_notice_audit import stale_hardware_notices


@tag("dlm")
class DlmHardwareNoticeAuditTest(TestCase):
    def setUp(self):
        if not apps.is_installed("netbox_dlm"):
            self.skipTest("netbox_dlm is not installed")
        self.HardwareNotice = apps.get_model("netbox_dlm", "HardwareNotice")
        self.manufacturer = Manufacturer.objects.create(
            name="Cisco HN", slug="cisco-hn"
        )
        self.site = Site.objects.create(name="HN Site", slug="hn-site")
        self.role = DeviceRole.objects.create(name="HN Role", slug="hn-role")
        # The customer's pair: Forward's part number, and the Device Type
        # Library name for the same hardware.
        self.legacy = self._type("N9K-C93180YC-FX", "n9k-c93180yc-fx")
        self.aliased = self._type("Nexus 93180YC-FX", "cisco-n9k-c93180yc-fx")
        # In-Forward but out of the include tags: no devices, notice is CORRECT.
        self.unscoped = self._type("Catalyst 2960X-24TD-L", "cisco-ws-c2960x-24td-l")
        Device.objects.create(
            name="hn-device", device_type=self.aliased, role=self.role, site=self.site
        )

    def _type(self, model, slug):
        return DeviceType.objects.create(
            manufacturer=self.manufacturer, model=model, slug=slug
        )

    def _notice(self, device_type):
        return self.HardwareNotice.objects.create(device_type=device_type)

    def _emitted(self, *slugs):
        return emitted_device_type_slugs([{"device_type_slug": s} for s in slugs])

    def test_a_notice_forward_no_longer_emits_is_stale(self):
        self._notice(self.legacy)
        self._notice(self.aliased)

        report = stale_hardware_notices(self._emitted("cisco-n9k-c93180yc-fx"))

        self.assertEqual(report["stale_notice_count"], 1)
        self.assertEqual(report["stale_notices"][0]["slug"], "n9k-c93180yc-fx")

    def test_an_unscoped_notice_with_no_devices_is_not_stale(self):
        # THE regression. This device type holds no devices and never will,
        # because its hardware sits outside the include tags - but Forward
        # emits a notice for it on every run.
        self._notice(self.unscoped)

        report = stale_hardware_notices(self._emitted("cisco-ws-c2960x-24td-l"))

        self.assertEqual(report["stale_notice_count"], 0)

    def test_a_notice_on_a_device_type_holding_devices_can_still_be_stale(self):
        # The inverse of the old rule, and equally real: Forward stopped
        # emitting it while NetBox devices remain on that type.
        self._notice(self.aliased)

        report = stale_hardware_notices(self._emitted("something-else"))

        self.assertEqual(report["stale_notice_count"], 1)
        self.assertEqual(report["stale_notices"][0]["slug"], "cisco-n9k-c93180yc-fx")

    def test_an_empty_result_is_refused_rather_than_deleting_everything(self):
        # Indistinguishable from a failed fetch. The same reasoning refuses an
        # empty scope result before an orphan prune.
        self._notice(self.legacy)
        self._notice(self.aliased)

        report = stale_hardware_notices(set())

        self.assertFalse(report["available"])
        self.assertEqual(report["stale_notice_count"], 0)
        self.assertIn("failed fetch", report["reason"])

    def test_deleting_removes_only_the_stale_notice(self):
        kept = self._notice(self.aliased)
        self._notice(self.legacy)

        report = stale_hardware_notices(self._emitted("cisco-n9k-c93180yc-fx"))
        delete_stale_hardware_notices(report["stale_notice_ids"])

        self.assertTrue(self.HardwareNotice.objects.filter(pk=kept.pk).exists())
        self.assertFalse(
            self.HardwareNotice.objects.filter(device_type=self.legacy).exists()
        )

    def test_the_device_type_itself_is_never_deleted(self):
        self._notice(self.legacy)

        report = stale_hardware_notices(self._emitted("cisco-n9k-c93180yc-fx"))
        delete_stale_hardware_notices(report["stale_notice_ids"])

        self.assertTrue(DeviceType.objects.filter(pk=self.legacy.pk).exists())

    def test_the_full_set_is_returned_when_no_sample_limit_is_given(self):
        # A caller that deletes must not act on a page while reporting a total.
        for index in range(30):
            self._notice(self._type(f"Retired {index}", f"retired-{index}"))

        sampled = stale_hardware_notices(self._emitted("cisco-n9k-c93180yc-fx"))
        full = stale_hardware_notices(
            self._emitted("cisco-n9k-c93180yc-fx"), sample_limit=None
        )

        self.assertEqual(len(sampled["stale_notices"]), 25)
        self.assertEqual(len(full["stale_notices"]), full["stale_notice_count"])
        self.assertGreater(full["stale_notice_count"], 25)
