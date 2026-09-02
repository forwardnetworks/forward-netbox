# The two netbox-dlm models slice six left unmeasured: `inventoryitemsoftware`
# and `inventoryitemroleplatform`. Deferred because their chains had not been
# audited for the writes-behind-a-runner-call trap; the audit found two reads
# that raise a dependency skip, a platform ensure and an upsert the preview
# overrides, and the same software-version upsert already previewed - and one
# guard to add, in the ensure, for the absent-platform case.
from dcim.models import Device
from dcim.models import DeviceRole
from dcim.models import DeviceType
from dcim.models import InventoryItem
from dcim.models import InventoryItemRole
from dcim.models import Manufacturer
from dcim.models import Platform
from dcim.models import Site
from django.apps import apps
from django.test import TestCase

from forward_netbox.utilities.drift_comparison import compare_model_rows

SOFTWARE = "netbox_dlm.inventoryitemsoftware"
MAPPING = "netbox_dlm.inventoryitemroleplatform"


def _dlm(name):
    return apps.get_model("netbox_dlm", name)


class InventoryItemSoftwarePreviewTest(TestCase):
    def setUp(self):
        site = Site.objects.create(name="IIS Site", slug="iis-site")
        mfr = Manufacturer.objects.create(name="IIS Mfr", slug="iis-mfr")
        dtype = DeviceType.objects.create(
            manufacturer=mfr, model="IIS DT", slug="iis-dt"
        )
        role = DeviceRole.objects.create(name="IIS Role", slug="iis-role")
        self.device = Device.objects.create(
            name="iis-dev", site=site, device_type=dtype, role=role, status="active"
        )
        self.item_role = InventoryItemRole.objects.create(
            name="MANAGEMENT CONTROLLER", slug="management-controller"
        )
        self.item = InventoryItem.objects.create(
            device=self.device, name="CIMC", role=self.item_role
        )

    def _row(self, **extra):
        return {
            "device": "iis-dev",
            "inventory_item": "CIMC",
            "role": "MANAGEMENT CONTROLLER",
            "role_slug": "management-controller",
            "platform": "CIMC",
            "platform_slug": "cimc",
            "version": "4.3(2.230270)",
            **extra,
        }

    def _existing(self, version="4.3(2.230270)"):
        platform = Platform.objects.create(name="CIMC", slug="cimc")
        _dlm("InventoryItemRolePlatform").objects.create(
            role=self.item_role, platform=platform
        )
        software_version = _dlm("SoftwareVersion").objects.create(
            platform=platform, version=version
        )
        return _dlm("InventoryItemSoftware").objects.create(
            inventory_item=self.item, software_version=software_version
        )

    def _counts(self):
        return {
            name: _dlm(name).objects.count()
            for name in (
                "InventoryItemSoftware",
                "InventoryItemRolePlatform",
                "SoftwareVersion",
            )
        }

    def test_a_preview_writes_nothing(self):
        before = (self._counts(), Platform.objects.count())
        counts = compare_model_rows(None, SOFTWARE, [self._row()])
        self.assertEqual(counts["creates"], 1)
        self.assertEqual((self._counts(), Platform.objects.count()), before)

    def test_everything_absent_is_one_create_under_this_model(self):
        # Platform, mapping and software version would all be created; each is
        # another model's drift. This row is one create here.
        counts = compare_model_rows(None, SOFTWARE, [self._row()])
        self.assertEqual(
            counts, {"creates": 1, "updates": 0, "unchanged": 0, "rejected": 0}
        )

    def test_an_existing_row_on_the_same_version_is_unchanged(self):
        self._existing()
        counts = compare_model_rows(None, SOFTWARE, [self._row()])
        self.assertEqual(counts["unchanged"], 1)

    def test_a_version_change_is_an_update(self):
        self._existing(version="4.1(3f)")
        counts = compare_model_rows(None, SOFTWARE, [self._row()])
        self.assertEqual(counts["updates"], 1)

    def test_a_new_platform_on_an_existing_row_is_an_update_not_a_sibling_match(self):
        """The guard. The mapping is keyed on the role alone, so an absent
        platform dropped from the lookup would match the role's mapping to
        SOME OTHER platform and the row could read unchanged."""
        self._existing()
        counts = compare_model_rows(
            None, SOFTWARE, [self._row(platform="APIC", platform_slug="apic")]
        )
        self.assertEqual(counts["updates"], 1)
        self.assertEqual(counts["unchanged"], 0)

    def test_a_missing_inventory_item_is_rejected_not_drift(self):
        counts = compare_model_rows(None, SOFTWARE, [self._row(inventory_item="NOPE")])
        self.assertEqual(counts["rejected"], 1)
        self.assertEqual(counts["creates"], 0)

    def test_a_missing_device_is_rejected(self):
        counts = compare_model_rows(None, SOFTWARE, [self._row(device="nope")])
        self.assertEqual(counts["rejected"], 1)


class InventoryItemRolePlatformPreviewTest(InventoryItemSoftwarePreviewTest):
    def test_an_absent_mapping_is_a_create(self):
        Platform.objects.create(name="CIMC", slug="cimc")
        counts = compare_model_rows(None, MAPPING, [self._row()])
        self.assertEqual(counts["creates"], 1)

    def test_an_absent_platform_is_a_create_too(self):
        counts = compare_model_rows(None, MAPPING, [self._row()])
        self.assertEqual(counts["creates"], 1)

    def test_an_existing_mapping_is_unchanged(self):
        self._existing()
        counts = compare_model_rows(None, MAPPING, [self._row()])
        self.assertEqual(counts["unchanged"], 1)

    def test_a_repeated_row_is_unchanged_because_the_apply_writes_it_once(self):
        # The ensure caches by (role, platform); the apply performs one upsert
        # for the first row and none for the rest. The preview says the same.
        # The platform exists, so the ensure reaches its upsert and its cache.
        Platform.objects.create(name="CIMC", slug="cimc")
        counts = compare_model_rows(None, MAPPING, [self._row(), self._row()])
        self.assertEqual(counts["creates"], 1)
        self.assertEqual(counts["unchanged"], 1)

    def test_a_platform_change_on_the_role_is_an_update(self):
        self._existing()
        Platform.objects.create(name="APIC", slug="apic")
        counts = compare_model_rows(
            None, MAPPING, [self._row(platform="APIC", platform_slug="apic")]
        )
        self.assertEqual(counts["updates"], 1)
