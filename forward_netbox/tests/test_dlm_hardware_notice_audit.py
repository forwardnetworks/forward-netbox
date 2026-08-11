# A customer reported "duplicate entries in the hardware notices": the same
# hardware listed twice under Forward's model string and under the NetBox Device
# Type Library name, with identical dates.
#
# It is not a duplicate write. Removals reach NetBox only through a Forward NQE
# diff, which reports what the CURRENT query stopped returning; a full run
# computes no removals at all. So re-pointing the device-type maps at their
# alias-aware variants orphans every row the base query wrote, permanently.
import json
from io import StringIO

from dcim.models import Device
from dcim.models import DeviceRole
from dcim.models import DeviceType
from dcim.models import Manufacturer
from dcim.models import Site
from django.apps import apps
from django.core.management import call_command
from django.test import tag
from django.test import TestCase


def _dlm_installed():
    return apps.is_installed("netbox_dlm")


@tag("dlm")
class DlmHardwareNoticeAuditTest(TestCase):
    def setUp(self):
        if not _dlm_installed():
            self.skipTest("netbox_dlm is not installed")
        self.HardwareNotice = apps.get_model("netbox_dlm", "HardwareNotice")
        self.manufacturer = Manufacturer.objects.create(
            name="Cisco HN", slug="cisco-hn"
        )
        self.site = Site.objects.create(name="HN Site", slug="hn-site")
        self.role = DeviceRole.objects.create(name="HN Role", slug="hn-role")
        # The pair a customer sees: Forward's part number and the Device Type
        # Library name for the same hardware.
        self.legacy = DeviceType.objects.create(
            manufacturer=self.manufacturer, model="N9K-C93180YC-FX", slug="n9k-93180-hn"
        )
        self.aliased = DeviceType.objects.create(
            manufacturer=self.manufacturer,
            model="Nexus 93180YC-FX",
            slug="nexus-93180-hn",
        )
        Device.objects.create(
            name="hn-device", device_type=self.aliased, role=self.role, site=self.site
        )

    def _notice(self, device_type):
        return self.HardwareNotice.objects.create(device_type=device_type)

    def _run(self, *args):
        out = StringIO()
        call_command(
            "forward_dlm_hardware_notice_audit", *args, stdout=out, stderr=StringIO()
        )
        return json.loads(out.getvalue())

    def test_a_notice_on_a_device_type_with_no_devices_is_reported(self):
        self._notice(self.legacy)
        self._notice(self.aliased)

        payload = self._run()

        self.assertEqual(payload["stale_notice_count"], 1)
        self.assertEqual(payload["stale_notices"][0]["model"], "N9K-C93180YC-FX")
        self.assertTrue(payload["remediation"])

    def test_the_notice_that_describes_real_hardware_is_left_alone(self):
        kept = self._notice(self.aliased)
        self._notice(self.legacy)

        self._run("--prune", "--apply")

        self.assertTrue(self.HardwareNotice.objects.filter(pk=kept.pk).exists())
        self.assertFalse(
            self.HardwareNotice.objects.filter(device_type=self.legacy).exists()
        )

    def test_a_dry_run_deletes_nothing(self):
        self._notice(self.legacy)

        payload = self._run("--prune")

        self.assertFalse(payload["prune_applied"])
        self.assertEqual(payload["prune_candidate_count"], 1)
        self.assertTrue(
            self.HardwareNotice.objects.filter(device_type=self.legacy).exists()
        )

    def test_the_device_type_itself_is_never_deleted(self):
        # An empty device type may have come from a Device Type Library import.
        # The notice is derived data the sync owns; the device type is not.
        self._notice(self.legacy)

        self._run("--prune", "--apply")

        self.assertTrue(DeviceType.objects.filter(pk=self.legacy.pk).exists())

    def test_a_clean_estate_reports_nothing_to_do(self):
        self._notice(self.aliased)

        payload = self._run()

        self.assertEqual(payload["stale_notice_count"], 0)
        self.assertEqual(payload["remediation"], "")
