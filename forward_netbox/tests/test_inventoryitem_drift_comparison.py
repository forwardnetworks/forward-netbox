# Slice three of the adapter-only drift comparison: `dcim.inventoryitem`.
#
# Every write on this path is behind a `runner.` call - `_ensure_manufacturer`,
# `_ensure_inventory_item_role`, `_upsert_values_from_defaults` - so the
# preview runner's firewall covers them, unlike cables where both writes were
# direct. `_ensure_inventory_item_role` is the new one, and it is the same trap
# as `_ensure_vrf`: an upsert reached during classification that a grep for ORM
# calls cannot see.
#
# The branch that needed real thought is not a write at all. When `dcim.module`
# is enabled a module-native row is DELETED rather than upserted, and this
# comparison's contract has no slot for a delete - the report reads drift as
# `creates + updates` and accounts for deletes separately. Such a row declines
# the whole model rather than being folded into a bucket that would double-count
# it or zero it.
from dcim.models import Device
from dcim.models import DeviceRole
from dcim.models import DeviceType
from dcim.models import InventoryItem
from dcim.models import InventoryItemRole
from dcim.models import Manufacturer
from dcim.models import Site
from django.test import TestCase

from forward_netbox.utilities.drift_comparison import compare_model_rows


class _ModuleEnabledSync:
    """A sync that says `dcim.module` is on, so the delete branch is live."""

    class _Source:
        parameters = {}

    source = _Source()
    pk = None
    name = ""

    def is_model_enabled(self, model_string):
        return model_string == "dcim.module"


class InventoryItemPreviewTest(TestCase):
    def setUp(self):
        site = Site.objects.create(name="I Site", slug="i-site")
        self.mfr = Manufacturer.objects.create(name="I Mfr", slug="i-mfr")
        dtype = DeviceType.objects.create(
            manufacturer=self.mfr, model="I DT", slug="i-dt"
        )
        role = DeviceRole.objects.create(name="I Role", slug="i-role")
        self.device = Device.objects.create(
            name="inv-dev", site=site, device_type=dtype, role=role, status="active"
        )

    def _row(self, **extra):
        row = {
            "device": "inv-dev",
            "name": "Slot 1",
            "part_id": "PN-1",
            "serial": "SN-1",
            "status": "active",
            "discovered": True,
            "role": "Transceiver",
            "role_slug": "transceiver",
            "role_color": "9e9e9e",
            "manufacturer": "I Mfr",
            "manufacturer_slug": "i-mfr",
        }
        row.update(extra)
        return row

    # --- the negative space -------------------------------------------------

    def test_a_preview_creates_no_inventory_item(self):
        before = InventoryItem.objects.count()

        result = compare_model_rows(None, "dcim.inventoryitem", [self._row()])

        self.assertEqual(InventoryItem.objects.count(), before)
        self.assertEqual(result["creates"], 1)

    def test_a_preview_creates_no_inventory_item_role(self):
        # `_ensure_inventory_item_role` upserts on the real runner, and it is
        # reached during classification - the `_ensure_vrf` trap again.
        before = InventoryItemRole.objects.count()

        compare_model_rows(None, "dcim.inventoryitem", [self._row()])

        self.assertEqual(InventoryItemRole.objects.count(), before)
        self.assertFalse(InventoryItemRole.objects.filter(slug="transceiver").exists())

    def test_a_preview_creates_no_manufacturer(self):
        before = Manufacturer.objects.count()

        compare_model_rows(
            None,
            "dcim.inventoryitem",
            [
                self._row(
                    manufacturer="Mfr That Does Not Exist",
                    manufacturer_slug="mfr-that-does-not-exist",
                )
            ],
        )

        self.assertEqual(Manufacturer.objects.count(), before)

    def test_a_preview_does_not_rewrite_a_drifted_item(self):
        role = InventoryItemRole.objects.create(
            name="Transceiver", slug="transceiver", color="9e9e9e"
        )
        item = InventoryItem.objects.create(
            device=self.device,
            name="Slot 1",
            part_id="PN-1",
            serial="SN-1",
            status="active",
            discovered=True,
            role=role,
            manufacturer=self.mfr,
            description="original",
        )

        compare_model_rows(
            None, "dcim.inventoryitem", [self._row(description="changed")]
        )

        item.refresh_from_db()
        self.assertEqual(item.description, "original")

    # --- classification -----------------------------------------------------

    def test_an_absent_item_is_a_create(self):
        result = compare_model_rows(None, "dcim.inventoryitem", [self._row()])

        self.assertEqual(
            result, {"creates": 1, "updates": 0, "unchanged": 0, "rejected": 0}
        )

    def test_a_matching_item_is_unchanged(self):
        role = InventoryItemRole.objects.create(
            name="Transceiver", slug="transceiver", color="9e9e9e"
        )
        InventoryItem.objects.create(
            device=self.device,
            name="Slot 1",
            part_id="PN-1",
            serial="SN-1",
            status="active",
            discovered=True,
            role=role,
            manufacturer=self.mfr,
        )

        result = compare_model_rows(None, "dcim.inventoryitem", [self._row()])

        self.assertEqual(
            result, {"creates": 0, "updates": 0, "unchanged": 1, "rejected": 0}
        )

    def test_a_drifted_item_is_an_update(self):
        role = InventoryItemRole.objects.create(
            name="Transceiver", slug="transceiver", color="9e9e9e"
        )
        InventoryItem.objects.create(
            device=self.device,
            name="Slot 1",
            part_id="PN-1",
            serial="SN-1",
            status="active",
            discovered=True,
            role=role,
            manufacturer=self.mfr,
            description="original",
        )

        result = compare_model_rows(
            None, "dcim.inventoryitem", [self._row(description="changed")]
        )

        self.assertEqual(
            result, {"creates": 0, "updates": 1, "unchanged": 0, "rejected": 0}
        )

    def test_an_unknown_device_is_rejected(self):
        result = compare_model_rows(
            None, "dcim.inventoryitem", [self._row(device="no-such-device")]
        )

        self.assertEqual(result["rejected"], 1)
        self.assertEqual(result["creates"], 0)

    # --- the delete branch: declines rather than guesses ---------------------

    def test_a_module_native_row_declines_the_whole_model(self):
        # `dcim.module` enabled + a module-native part type means the apply
        # DELETES this row. The contract has no slot for that, so the honest
        # answer is no comparison at all.
        result = compare_model_rows(
            _ModuleEnabledSync(),
            "dcim.inventoryitem",
            [self._row(part_type="LINE CARD")],
        )

        self.assertIsNone(result)

    def test_a_module_native_row_deletes_nothing_while_declining(self):
        role = InventoryItemRole.objects.create(
            name="Transceiver", slug="transceiver", color="9e9e9e"
        )
        InventoryItem.objects.create(
            device=self.device,
            name="Slot 1",
            part_id="PN-1",
            serial="SN-1",
            status="active",
            discovered=True,
            role=role,
        )
        before = InventoryItem.objects.count()

        compare_model_rows(
            _ModuleEnabledSync(),
            "dcim.inventoryitem",
            [self._row(part_type="LINE CARD")],
        )

        self.assertEqual(InventoryItem.objects.count(), before)

    def test_one_module_native_row_declines_the_batch_it_is_in(self):
        # Not merely the row: the whole model, because a partial count would
        # understate drift by however many rows were dropped.
        result = compare_model_rows(
            _ModuleEnabledSync(),
            "dcim.inventoryitem",
            [self._row(), self._row(name="Slot 2", part_type="LINE CARD")],
        )

        self.assertIsNone(result)

    def test_a_module_native_row_is_compared_normally_when_modules_are_off(self):
        # The delete branch is gated on `dcim.module` being enabled. With it
        # off the row is an ordinary inventory item and must be measured, not
        # declined - otherwise every deployment without modules loses the model.
        result = compare_model_rows(
            None, "dcim.inventoryitem", [self._row(part_type="LINE CARD")]
        )

        self.assertIsNotNone(result)
        self.assertEqual(result["creates"], 1)
