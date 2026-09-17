# dcim.platform and dcim.devicerole reported "Not measured" on every drift
# page: the simple-model bulk path declines to preview them (their tree save
# path has no preview mode), and nothing else answered. Their adapters resolve
# and upsert through runner methods the preview overrides, so the adapter
# loop can measure them exactly - once `_ensure_platform` stops calling the
# module upsert the preview cannot intercept.
from dcim.models import DeviceRole
from dcim.models import Manufacturer
from dcim.models import Platform
from django.test import TestCase

from forward_netbox.utilities.drift_comparison import compare_model_rows


class PlatformAndRoleAreMeasuredTest(TestCase):
    def setUp(self):
        self.mfr = Manufacturer.objects.create(name="Cisco", slug="cisco")
        Platform.objects.create(name="IOS-XE", slug="ios-xe", manufacturer=self.mfr)
        DeviceRole.objects.create(name="Leaf", slug="leaf", color="ff0000")

    def test_a_converged_platform_compares_unchanged(self):
        rows = [
            {
                "name": "IOS-XE",
                "slug": "ios-xe",
                "manufacturer": "Cisco",
                "manufacturer_slug": "cisco",
            }
        ]
        result = compare_model_rows(None, "dcim.platform", rows)
        self.assertIsNotNone(result, "dcim.platform must be measured, not estimated")
        self.assertEqual((result["creates"], result["updates"]), (0, 0))
        # Measuring never writes.
        self.assertEqual(Platform.objects.count(), 1)

    def test_a_new_platform_is_a_create_and_a_cleared_manufacturer_an_update(self):
        rows = [
            {
                "name": "NX-OS",
                "slug": "nx-os",
                "manufacturer": "",
                "manufacturer_slug": "",
            },
            {
                "name": "IOS-XE",
                "slug": "ios-xe",
                "manufacturer": "",
                "manufacturer_slug": "",
            },
        ]
        result = compare_model_rows(None, "dcim.platform", rows)
        self.assertEqual((result["creates"], result["updates"]), (1, 1))
        self.assertEqual(Platform.objects.count(), 1)

    def test_a_converged_role_compares_unchanged_and_a_recolour_is_an_update(self):
        result = compare_model_rows(
            None,
            "dcim.devicerole",
            [{"name": "Leaf", "slug": "leaf", "color": "ff0000"}],
        )
        self.assertIsNotNone(result)
        self.assertEqual((result["creates"], result["updates"]), (0, 0))

        result = compare_model_rows(
            None,
            "dcim.devicerole",
            [{"name": "Leaf", "slug": "leaf", "color": "00ff00"}],
        )
        self.assertEqual((result["creates"], result["updates"]), (0, 1))
        self.assertEqual(DeviceRole.objects.get(slug="leaf").color, "ff0000")
