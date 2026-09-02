# Slice five of the adapter-only drift comparison: `dcim.module`.
#
# Every write here is behind a `runner.` call, so the firewall covers them:
# `_ensure_module_bay` (upserts a ModuleBay), `_ensure_module_type` (upserts a
# ModuleType and a Manufacturer beneath it), `_upsert_values_from_defaults`.
#
# That last one is the one that matters. It passes
# `create_instance_attrs={"_adopt_components": True}`, and NetBox core's
# `Module.save()` then instantiates the module type's component templates -
# interfaces, console ports, power ports - as real rows on the device. A
# preview that saved would not create one row, it would create a dozen, on
# hardware the operator only asked about.
from dcim.models import Device
from dcim.models import DeviceRole
from dcim.models import DeviceType
from dcim.models import Interface
from dcim.models import Manufacturer
from dcim.models import Module
from dcim.models import ModuleBay
from dcim.models import ModuleType
from dcim.models import Site
from django.test import TestCase

from forward_netbox.utilities.drift_comparison import compare_model_rows


class ModulePreviewTest(TestCase):
    def setUp(self):
        site = Site.objects.create(name="M Site", slug="m-site")
        self.mfr = Manufacturer.objects.create(name="M Mfr", slug="m-mfr")
        dtype = DeviceType.objects.create(
            manufacturer=self.mfr, model="M DT", slug="m-dt"
        )
        role = DeviceRole.objects.create(name="M Role", slug="m-role")
        self.device = Device.objects.create(
            name="mod-dev", site=site, device_type=dtype, role=role, status="active"
        )

    def _row(self, **extra):
        row = {
            "device": "mod-dev",
            "module_bay": "Slot 1",
            "manufacturer": "M Mfr",
            "manufacturer_slug": "m-mfr",
            "model": "MT-1",
            "status": "active",
            "serial": "SN-1",
        }
        row.update(extra)
        return row

    def _module_type(self):
        return ModuleType.objects.create(
            manufacturer=self.mfr, model="MT-1", part_number="", description=""
        )

    def _bay(self):
        return ModuleBay.objects.create(device=self.device, name="Slot 1")

    # --- the negative space -------------------------------------------------

    def test_a_preview_creates_no_module_bay_or_type(self):
        bays = ModuleBay.objects.count()
        types = ModuleType.objects.count()
        modules = Module.objects.count()

        result = compare_model_rows(None, "dcim.module", [self._row()])

        self.assertEqual(ModuleBay.objects.count(), bays)
        self.assertEqual(ModuleType.objects.count(), types)
        self.assertEqual(Module.objects.count(), modules)
        self.assertEqual(result["creates"], 1)

    def test_a_preview_creates_no_manufacturer(self):
        before = Manufacturer.objects.count()

        compare_model_rows(
            None,
            "dcim.module",
            [
                self._row(
                    manufacturer="Mfr That Does Not Exist",
                    manufacturer_slug="mfr-that-does-not-exist",
                )
            ],
        )

        self.assertEqual(Manufacturer.objects.count(), before)

    def test_a_preview_instantiates_no_components(self):
        """The assertion this slice exists for.

        With the bay and type both present, the apply would reach
        `Module.save()` with `_adopt_components`, and NetBox core would
        instantiate every component template on the module type as a real row
        on the device. Counting interfaces before and after is what catches a
        preview that got as far as the save.
        """
        self._bay()
        module_type = self._module_type()
        from dcim.models import InterfaceTemplate

        InterfaceTemplate.objects.create(
            module_type=module_type, name="Ethernet1/1", type="1000base-t"
        )
        interfaces_before = Interface.objects.count()
        modules_before = Module.objects.count()

        result = compare_model_rows(None, "dcim.module", [self._row()])

        self.assertEqual(Interface.objects.count(), interfaces_before)
        self.assertEqual(Module.objects.count(), modules_before)
        self.assertEqual(result["creates"], 1)

    def test_a_preview_does_not_rewrite_a_drifted_module(self):
        bay = self._bay()
        module_type = self._module_type()
        module = Module.objects.create(
            device=self.device,
            module_bay=bay,
            module_type=module_type,
            status="active",
            serial="OLD-SERIAL",
        )

        result = compare_model_rows(None, "dcim.module", [self._row()])

        module.refresh_from_db()
        self.assertEqual(module.serial, "OLD-SERIAL")
        self.assertEqual(result["updates"], 1)

    # --- classification -----------------------------------------------------

    def test_an_absent_bay_is_a_create(self):
        self._module_type()

        result = compare_model_rows(None, "dcim.module", [self._row()])

        self.assertEqual(
            result, {"creates": 1, "updates": 0, "unchanged": 0, "rejected": 0}
        )

    def test_an_absent_module_type_is_a_create(self):
        self._bay()

        result = compare_model_rows(None, "dcim.module", [self._row()])

        self.assertEqual(result["creates"], 1)

    def test_a_matching_module_is_unchanged(self):
        bay = self._bay()
        module_type = self._module_type()
        Module.objects.create(
            device=self.device,
            module_bay=bay,
            module_type=module_type,
            status="active",
            serial="SN-1",
        )

        result = compare_model_rows(None, "dcim.module", [self._row()])

        self.assertEqual(
            result, {"creates": 0, "updates": 0, "unchanged": 1, "rejected": 0}
        )

    def test_a_drifted_module_is_an_update(self):
        bay = self._bay()
        module_type = self._module_type()
        Module.objects.create(
            device=self.device,
            module_bay=bay,
            module_type=module_type,
            status="active",
            serial="OLD-SERIAL",
        )

        result = compare_model_rows(None, "dcim.module", [self._row()])

        self.assertEqual(
            result, {"creates": 0, "updates": 1, "unchanged": 0, "rejected": 0}
        )

    def test_a_row_missing_its_manufacturer_key_is_rejected_not_a_create(self):
        """Preview and apply must agree that a malformed row is malformed.

        The real `_ensure_module_type` indexes `row["manufacturer"]` and raises
        KeyError, which is counted as a rejected row. The override used
        `.get()`, returned None, and classified the same row as a create.
        """
        row = self._row()
        del row["manufacturer"]

        result = compare_model_rows(None, "dcim.module", [row])

        self.assertEqual(result["rejected"], 1)
        self.assertEqual(result["creates"], 0)

    def test_the_claimed_component_scan_is_not_repeated_per_row(self):
        """Eight queries a row, for a scan whose answer cannot change.

        A preview writes nothing, so a device's claimed component names are
        fixed for the whole run. Without the cache this cost eight queries per
        module row on top of the resolution ones.
        """
        module_type = self._module_type()
        from dcim.models import InterfaceTemplate

        InterfaceTemplate.objects.create(
            module_type=module_type, name="Ethernet1/1", type="1000base-t"
        )
        other_bay = ModuleBay.objects.create(device=self.device, name="Slot 9")
        other_module = Module.objects.create(
            device=self.device,
            module_bay=other_bay,
            module_type=module_type,
            status="active",
        )
        Interface.objects.filter(device=self.device, name="Ethernet1/1").update(
            module=other_module
        )
        rows = [self._row(module_bay=f"Slot {index}") for index in range(2, 12)]

        from django.test.utils import CaptureQueriesContext
        from django.db import connection

        with CaptureQueriesContext(connection) as captured:
            compare_model_rows(None, "dcim.module", rows)
        repeated = [
            query
            for query in captured.captured_queries
            if "dcim_interface" in query["sql"] and "module_id" in query["sql"]
        ]

        # One scan for the device, not one per row.
        self.assertLessEqual(len(repeated), 1, f"{len(repeated)} repeated scans")

    def test_an_unknown_device_is_rejected(self):
        result = compare_model_rows(
            None, "dcim.module", [self._row(device="no-such-device")]
        )

        self.assertEqual(result["rejected"], 1)
        self.assertEqual(result["creates"], 0)

    def test_a_row_without_a_module_bay_is_rejected(self):
        result = compare_model_rows(None, "dcim.module", [self._row(module_bay="")])

        self.assertEqual(result["rejected"], 1)
        self.assertEqual(result["creates"], 0)

    def test_a_claimed_component_is_rejected_even_when_the_bay_is_absent(self):
        """The permanent-skip verdict does not depend on the bay existing.

        The apply creates the missing bay and THEN hits the collision and skips,
        so a preview that short-circuited on the absent bay reported a create
        for a row no run will ever apply. Found by review; the sibling test
        creates the bay first and so never covered this ordering.
        """
        module_type = self._module_type()
        from dcim.models import InterfaceTemplate

        InterfaceTemplate.objects.create(
            module_type=module_type, name="Ethernet1/1", type="1000base-t"
        )
        other_bay = ModuleBay.objects.create(device=self.device, name="Slot 9")
        other_module = Module.objects.create(
            device=self.device,
            module_bay=other_bay,
            module_type=module_type,
            status="active",
        )
        Interface.objects.filter(device=self.device, name="Ethernet1/1").update(
            module=other_module
        )
        # Deliberately NO "Slot 1" bay.
        self.assertFalse(
            ModuleBay.objects.filter(device=self.device, name="Slot 1").exists()
        )

        result = compare_model_rows(None, "dcim.module", [self._row()])

        self.assertEqual(result["creates"], 0)
        self.assertEqual(result["rejected"], 1)

    def test_a_swapped_card_in_an_existing_bay_is_an_update_not_a_create(self):
        """`module_type` is not part of the module's identity.

        The coalesce set is ("device", "module_bay"), so a bay that already
        holds a module gets an UPDATE when the card is swapped for a type
        NetBox has not seen yet. Treating an absent type as a create split
        creates and updates wrongly for every hardware replacement.
        """
        bay = self._bay()
        old_type = ModuleType.objects.create(
            manufacturer=self.mfr, model="MT-OLD", part_number="", description=""
        )
        Module.objects.create(
            device=self.device,
            module_bay=bay,
            module_type=old_type,
            status="active",
            serial="SN-1",
        )
        # The row names MT-1, which NetBox does not have.
        self.assertFalse(ModuleType.objects.filter(model="MT-1").exists())

        result = compare_model_rows(None, "dcim.module", [self._row()])

        self.assertEqual(result["creates"], 0)
        self.assertEqual(result["updates"], 1)

    def test_a_component_claimed_by_another_module_is_rejected_not_a_create(self):
        # NetBox can only adopt a component belonging to NO module. One already
        # claimed by a different module makes the template instantiation fail
        # the unique-name constraint, so the apply skips the row - and a skip
        # is not drift.
        self._bay()
        module_type = self._module_type()
        from dcim.models import InterfaceTemplate

        InterfaceTemplate.objects.create(
            module_type=module_type, name="Ethernet1/1", type="1000base-t"
        )
        other_bay = ModuleBay.objects.create(device=self.device, name="Slot 9")
        other_module = Module.objects.create(
            device=self.device,
            module_bay=other_bay,
            module_type=module_type,
            status="active",
        )
        Interface.objects.filter(device=self.device, name="Ethernet1/1").update(
            module=other_module
        )
        if not Interface.objects.filter(
            device=self.device, name="Ethernet1/1"
        ).exists():
            Interface.objects.create(
                device=self.device,
                name="Ethernet1/1",
                type="1000base-t",
                module=other_module,
            )

        result = compare_model_rows(None, "dcim.module", [self._row()])

        self.assertEqual(result["creates"], 0)
        self.assertEqual(result["rejected"], 1)
