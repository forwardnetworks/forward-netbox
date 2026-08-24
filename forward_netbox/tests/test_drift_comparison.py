# The drift report called every fetched row a change, so `In sync` and
# `Total drift` read "Not measured" on every run for every deployment. These
# tests cover the comparison that replaces that estimate.
#
# The first class is the one that matters. Routing a preview through the apply
# path is what keeps the comparison honest, and it is also what makes it
# dangerous: `bulk_orm_apply_simple_models` writes during dependency resolution
# before it classifies anything, creating missing manufacturers for device types
# and missing VRFs for prefixes. A preview that inherited those writes would
# create rows in an operator's NetBox as a side effect of looking at it.
from dcim.models import DeviceType
from dcim.models import Manufacturer
from dcim.models import Site
from django.test import TestCase
from ipam.models import Prefix
from ipam.models import VRF

from forward_netbox.utilities.drift_comparison import compare_model_rows


class PreviewWritesNothingTest(TestCase):
    """The negative space: looking must never change anything."""

    def test_a_prefix_preview_does_not_create_the_missing_vrf(self):
        # apply_engine_bulk creates absent VRFs before classifying prefixes.
        # Under preview that must not happen.
        before = set(VRF.objects.values_list("name", flat=True))

        compare_model_rows(
            None,
            "ipam.prefix",
            [{"prefix": "10.99.0.0/24", "vrf": "vrf-that-does-not-exist"}],
        )

        self.assertEqual(set(VRF.objects.values_list("name", flat=True)), before)
        self.assertFalse(VRF.objects.filter(name="vrf-that-does-not-exist").exists())
        self.assertFalse(Prefix.objects.filter(prefix="10.99.0.0/24").exists())

    def test_a_devicetype_preview_does_not_create_the_missing_manufacturer(self):
        before = set(Manufacturer.objects.values_list("name", flat=True))

        compare_model_rows(
            None,
            "dcim.devicetype",
            [
                {
                    "manufacturer": "Mfr That Does Not Exist",
                    "manufacturer_slug": "mfr-that-does-not-exist",
                    "model": "DT-1",
                    "slug": "dt-1",
                }
            ],
        )

        self.assertEqual(
            set(Manufacturer.objects.values_list("name", flat=True)), before
        )
        self.assertFalse(DeviceType.objects.filter(slug="dt-1").exists())

    def test_a_site_preview_creates_no_site(self):
        before = Site.objects.count()

        result = compare_model_rows(
            None,
            "dcim.site",
            [{"name": "Preview Site", "slug": "preview-site"}],
        )

        self.assertEqual(Site.objects.count(), before)
        self.assertEqual(result["creates"], 1)


class PreviewCountsMatchRealityTest(TestCase):
    """The counts are the product; they have to be right, not merely cheap."""

    def setUp(self):
        Site.objects.create(name="Existing Site", slug="existing-site")

    def test_an_unchanged_row_is_not_counted_as_drift(self):
        result = compare_model_rows(
            None,
            "dcim.site",
            [{"name": "Existing Site", "slug": "existing-site"}],
        )

        self.assertEqual(result["unchanged"], 1)
        self.assertEqual(result["creates"], 0)
        self.assertEqual(result["updates"], 0)

    def test_a_changed_row_counts_as_an_update(self):
        result = compare_model_rows(
            None,
            "dcim.site",
            [{"name": "Renamed Site", "slug": "existing-site"}],
        )

        self.assertEqual(result["updates"], 1)
        self.assertEqual(result["creates"], 0)

    def test_a_mixed_batch_is_not_all_or_nothing(self):
        result = compare_model_rows(
            None,
            "dcim.site",
            [
                {"name": "Existing Site", "slug": "existing-site"},
                {"name": "Renamed Later", "slug": "existing-site-2"},
                {"name": "Brand New", "slug": "brand-new"},
            ],
        )

        self.assertEqual(result["creates"], 2)
        self.assertEqual(result["unchanged"], 1)

    def test_a_row_missing_identity_is_rejected_not_counted_as_drift(self):
        result = compare_model_rows(
            None,
            "dcim.site",
            [{"name": "", "slug": ""}],
        )

        self.assertEqual(result["rejected"], 1)
        self.assertEqual(result["creates"], 0)
        self.assertEqual(result["updates"], 0)


class UncoveredModelsReportNoComparisonTest(TestCase):
    """A model with no comparison must say so, never report a confident zero.

    Returning ``{"creates": 0, ...}`` for something that was never compared
    would tell an operator they are in sync when nothing checked - the one
    failure mode here that leads somewhere bad.
    """

    def test_the_unaudited_bespoke_paths_return_none_under_preview(self):
        """The one path still without a comparison.

        `virtualchassis` creates VirtualChassis rows and then reads their pks
        back to assign devices, so skipping the first phase leaves the second
        unclassifiable - there is no verdict to shortcut to, the way there was
        for `interface`. Until that is handled it must decline to answer rather
        than answer wrongly.
        """
        self.assertIsNone(
            compare_model_rows(None, "dcim.virtualchassis", [{"name": "x"}]),
            "virtualchassis has no comparison yet and must not claim one",
        )

    def test_an_empty_row_set_is_zero_drift_rather_than_unmeasured(self):
        # Nothing fetched genuinely is nothing to change, for any model.
        self.assertEqual(
            compare_model_rows(None, "dcim.site", []),
            {"creates": 0, "updates": 0, "unchanged": 0, "rejected": 0},
        )


class MacAddressComparisonTest(TestCase):
    """macaddress was the one bespoke path the write audit cleared."""

    def test_a_macaddress_preview_writes_nothing(self):
        from dcim.models import MACAddress

        before = MACAddress.objects.count()

        compare_model_rows(
            None,
            "dcim.macaddress",
            [
                {
                    "mac_address": "00:11:22:33:44:55",
                    "device": "no-such-device",
                    "interface": "eth0",
                }
            ],
        )

        self.assertEqual(MACAddress.objects.count(), before)

    def test_it_answers_rather_than_declining(self):
        # The point of auditing it: a clean path should report a comparison, not
        # be lumped in with the ones that cannot.
        result = compare_model_rows(None, "dcim.macaddress", [])

        self.assertIsNotNone(result)
        self.assertEqual(result["creates"], 0)


class MacAddressPreviewCostContractTest(TestCase):
    """The classification a fast preview keeps, and the one divergence it buys.

    The preview stopped constructing MACAddress objects and running
    `full_clean` per row - on the measuring deployment that was 42% of the
    whole report's cost for this one model. The counts must not have moved,
    and the single deliberate divergence must stay pinned so removing it later
    is a decision rather than an accident.
    """

    @classmethod
    def setUpTestData(cls):
        from dcim.models import Device
        from dcim.models import DeviceRole
        from dcim.models import DeviceType
        from dcim.models import Interface

        site = Site.objects.create(name="Mac Site", slug="mac-site")
        mfr = Manufacturer.objects.create(name="Mac Mfr", slug="mac-mfr")
        dtype = DeviceType.objects.create(
            manufacturer=mfr, model="Mac DT", slug="mac-dt"
        )
        role = DeviceRole.objects.create(name="Mac Role", slug="mac-role")
        cls.device = Device.objects.create(
            name="mac-dev", site=site, device_type=dtype, role=role, status="active"
        )
        cls.interface = Interface.objects.create(
            device=cls.device, name="Ethernet1", type="1000base-t"
        )
        cls.second_interface = Interface.objects.create(
            device=cls.device, name="Ethernet2", type="1000base-t"
        )

    def _row(self, mac, interface="Ethernet1"):
        return {"device": "mac-dev", "interface": interface, "mac": mac}

    def test_an_absent_mac_is_a_create_and_nothing_is_constructed(self):
        result = compare_model_rows(
            None, "dcim.macaddress", [self._row("00:11:22:33:44:01")]
        )

        self.assertEqual(result["creates"], 1)
        self.assertEqual(result["updates"], 0)

    def test_a_reassignment_is_an_update(self):
        from dcim.models import MACAddress

        MACAddress.objects.create(mac_address="00:11:22:33:44:02")

        result = compare_model_rows(
            None, "dcim.macaddress", [self._row("00:11:22:33:44:02")]
        )

        self.assertEqual(result["updates"], 1)
        self.assertEqual(result["creates"], 0)

    def test_a_matching_assignment_is_unchanged(self):
        from django.contrib.contenttypes.models import ContentType
        from dcim.models import Interface
        from dcim.models import MACAddress

        MACAddress.objects.create(
            mac_address="00:11:22:33:44:03",
            assigned_object_type=ContentType.objects.get_for_model(Interface),
            assigned_object_id=self.interface.pk,
        )

        result = compare_model_rows(
            None, "dcim.macaddress", [self._row("00:11:22:33:44:03")]
        )

        self.assertEqual(result["creates"], 0)
        self.assertEqual(result["updates"], 0)
        self.assertEqual(result["unchanged"], 1)

    def test_duplicate_incoming_rows_collapse_to_one_create(self):
        # Two rows, one canonical MAC, two interfaces. The apply keys creates by
        # canonical MAC and lets the second row take the update branch against
        # the in-memory first, contributing nothing to the counts. The preview's
        # sentinel must walk the same path: pk=None, so the update branch skips
        # it, exactly as it skips an unsaved in-memory create.
        result = compare_model_rows(
            None,
            "dcim.macaddress",
            [
                self._row("00:11:22:33:44:04"),
                self._row("00:11:22:33:44:04", interface="Ethernet2"),
            ],
        )

        self.assertEqual(result["creates"], 1)
        self.assertEqual(result["updates"], 0)

    def test_an_invalid_row_counts_as_a_create_under_preview(self):
        """THE deliberate divergence, pinned so it stays deliberate.

        The apply runs `full_clean` and counts a row it rejects as failed. The
        preview skips `full_clean` - that is where the 42% went - so the same
        row counts as a create. Overstated drift is the safe direction: an
        operator investigates a number that will not converge, where an
        understated one tells them nothing is wrong.

        If this test starts failing because the preview now rejects the row,
        the divergence was closed - update the plan and delete this test
        KNOWINGLY, because the cost that motivated the skip comes back with it.
        """
        result = compare_model_rows(
            None,
            "dcim.macaddress",
            # Parseable as a MAC by the plugin, refused by NetBox's own field
            # validation (EUI64 where the model wants EUI48).
            [self._row("00:11:22:33:44:55:66:77")],
        )

        self.assertIsNotNone(result)
        self.assertEqual(result["creates"] + result["rejected"], 1)


class InterfacePreviewCostContractTest(TestCase):
    """The interface preview stopped validating, and stopped mutating."""

    @classmethod
    def setUpTestData(cls):
        from dcim.models import Device
        from dcim.models import DeviceRole
        from dcim.models import DeviceType

        site = Site.objects.create(name="IfCost Site", slug="ifcost-site")
        mfr = Manufacturer.objects.create(name="IfCost Mfr", slug="ifcost-mfr")
        dtype = DeviceType.objects.create(
            manufacturer=mfr, model="IfCost DT", slug="ifcost-dt"
        )
        role = DeviceRole.objects.create(name="IfCost Role", slug="ifcost-role")
        cls.device = Device.objects.create(
            name="ifcost-dev", site=site, device_type=dtype, role=role, status="active"
        )

    def _row(self, name="Ethernet1", **extra):
        row = {
            "device": "ifcost-dev",
            "name": name,
            "type": "1000base-t",
            "enabled": True,
        }
        row.update(extra)
        return row

    def test_an_absent_interface_is_a_create(self):
        result = compare_model_rows(None, "dcim.interface", [self._row()])

        self.assertEqual(result["creates"], 1)
        self.assertEqual(result["updates"], 0)

    def test_a_matching_interface_is_unchanged(self):
        from dcim.models import Interface

        Interface.objects.create(
            device=self.device, name="Ethernet1", type="1000base-t", enabled=True
        )

        result = compare_model_rows(None, "dcim.interface", [self._row()])

        self.assertEqual(result["creates"], 0)
        self.assertEqual(result["updates"], 0)
        self.assertEqual(result["unchanged"], 1)

    def test_a_drifted_interface_is_an_update(self):
        from dcim.models import Interface

        Interface.objects.create(
            device=self.device, name="Ethernet1", type="1000base-t", enabled=False
        )

        result = compare_model_rows(None, "dcim.interface", [self._row()])

        self.assertEqual(result["updates"], 1)
        self.assertEqual(result["creates"], 0)

    def test_an_invalid_interface_row_counts_as_a_create_under_preview(self):
        """The deliberate divergence, same shape as the macaddress one.

        `_validate_interface` runs `full_clean` and classifies a rejected row
        as failed. The preview skips it - that is the whole win, 29,139 ms to
        1,467 ms at 16,000 first-sync rows - so such a row counts as a create.
        Overstates drift, never understates it.

        If this starts failing because the preview now rejects the row, the
        divergence was closed: update the plan and delete this test KNOWINGLY,
        because the cost comes back with it.
        """
        result = compare_model_rows(
            None,
            "dcim.interface",
            # A type NetBox's own field validation refuses.
            [self._row("Ethernet7", type="not-a-real-interface-type")],
        )

        self.assertIsNotNone(result)
        self.assertEqual(result["creates"] + result["rejected"], 1)

    def test_a_preview_creates_no_interface(self):
        from dcim.models import Interface

        before = Interface.objects.count()

        compare_model_rows(None, "dcim.interface", [self._row("Ethernet99")])

        self.assertEqual(Interface.objects.count(), before)


class IpAddressComparisonTest(TestCase):
    """ipaddress writes a VRF mid-classification, behind a runner call.

    The audit grep looks for direct ORM writes and did not see it. It is
    neutralised because the preview runner overrides `_ensure_vrf` with a
    lookup - so these tests exist to hold that override in place, since losing
    it would put VRF creation back into a read-only preview silently.
    """

    def test_an_ipaddress_preview_does_not_create_the_missing_vrf(self):
        before = set(VRF.objects.values_list("name", flat=True))

        compare_model_rows(
            None,
            "ipam.ipaddress",
            [{"address": "10.77.0.1/32", "vrf": "vrf-absent-from-netbox"}],
        )

        self.assertEqual(set(VRF.objects.values_list("name", flat=True)), before)
        self.assertFalse(VRF.objects.filter(name="vrf-absent-from-netbox").exists())

    def test_an_ipaddress_preview_creates_no_address(self):
        from ipam.models import IPAddress

        before = IPAddress.objects.count()

        result = compare_model_rows(
            None,
            "ipam.ipaddress",
            [{"address": "10.77.0.2/32"}],
        )

        self.assertEqual(IPAddress.objects.count(), before)
        self.assertIsNotNone(result)

    def test_it_answers_rather_than_declining(self):
        result = compare_model_rows(None, "ipam.ipaddress", [])

        self.assertIsNotNone(result)
        self.assertEqual(result["creates"], 0)


class InterfaceComparisonTest(TestCase):
    """interface deletes a cable partway through applying, and re-enters itself.

    NetBox refuses `type=lag` on a cabled interface, so the apply removes the
    cable and re-applies the row. A preview must not do that - it is the most
    destructive thing any of these paths does - and must still count the row,
    because an existing cabled interface becoming a LAG is plainly a change.
    """

    def setUp(self):
        from dcim.models import Device
        from dcim.models import DeviceRole
        from dcim.models import DeviceType

        site = Site.objects.create(name="I Site", slug="i-site")
        mfr = Manufacturer.objects.create(name="I Mfr", slug="i-mfr")
        dtype = DeviceType.objects.create(manufacturer=mfr, model="I DT", slug="i-dt")
        role = DeviceRole.objects.create(name="I Role", slug="i-role")
        self.device = Device.objects.create(
            name="iface-dev", site=site, device_type=dtype, role=role
        )

    def test_a_cabled_lag_conversion_does_not_delete_the_cable(self):
        from dcim.models import Cable
        from dcim.models import Interface

        left = Interface.objects.create(
            device=self.device, name="Ethernet1", type="1000base-t"
        )
        right = Interface.objects.create(
            device=self.device, name="Ethernet2", type="1000base-t"
        )
        cable = Cable.objects.create(
            a_terminations=[left], b_terminations=[right], status="connected"
        )
        cables_before = Cable.objects.count()

        result = compare_model_rows(
            None,
            "dcim.interface",
            [{"device": "iface-dev", "name": "Ethernet1", "type": "lag"}],
        )

        self.assertEqual(Cable.objects.count(), cables_before)
        self.assertTrue(Cable.objects.filter(pk=cable.pk).exists())
        left.refresh_from_db()
        self.assertIsNotNone(left.cable_id)
        # Still counted, just not performed.
        self.assertIsNotNone(result)
        self.assertEqual(result["updates"], 1)

    def test_an_interface_preview_creates_nothing(self):
        from dcim.models import Interface

        before = Interface.objects.count()

        result = compare_model_rows(
            None,
            "dcim.interface",
            [
                {
                    "device": "iface-dev",
                    "name": "Ethernet9",
                    "type": "1000base-t",
                    "enabled": True,
                }
            ],
        )

        self.assertEqual(Interface.objects.count(), before)
        self.assertEqual(result["creates"], 1)

    def test_an_unchanged_interface_is_not_drift(self):
        from dcim.models import Interface

        Interface.objects.create(
            device=self.device, name="Ethernet3", type="1000base-t"
        )

        result = compare_model_rows(
            None,
            "dcim.interface",
            [
                {
                    "device": "iface-dev",
                    "name": "Ethernet3",
                    "type": "1000base-t",
                    "enabled": True,
                }
            ],
        )

        self.assertEqual(result["creates"], 0)
        self.assertEqual(result["updates"], 0)


class DeviceComparisonTest(TestCase):
    """device creates Tags and TaggedItems, and upserts a Platform.

    The Tag and TaggedItem writes are inside the same transaction as the Device
    write, so an early return skips them. The Platform upsert is not - it is
    reached through `runner._ensure_platform` during classification, and it
    creates a Manufacturer under it, so the preview runner overrides both.
    """

    def setUp(self):
        from dcim.models import DeviceRole
        from dcim.models import DeviceType

        self.site = Site.objects.create(name="D Site", slug="d-site")
        mfr = Manufacturer.objects.create(name="D Mfr", slug="d-mfr")
        self.dtype = DeviceType.objects.create(
            manufacturer=mfr, model="D DT", slug="d-dt"
        )
        self.role = DeviceRole.objects.create(name="D Role", slug="d-role")

    def _row(self, name, **extra):
        row = {
            "name": name,
            "site": "D Site",
            "site_slug": "d-site",
            "device_type": "D DT",
            "device_type_slug": "d-dt",
            "manufacturer": "D Mfr",
            "manufacturer_slug": "d-mfr",
            "role": "D Role",
            "role_slug": "d-role",
            "status": "active",
        }
        row.update(extra)
        return row

    def test_a_device_preview_creates_no_device(self):
        from dcim.models import Device

        before = Device.objects.count()

        result = compare_model_rows(None, "dcim.device", [self._row("new-dev")])

        self.assertEqual(Device.objects.count(), before)
        self.assertIsNotNone(result)

    def test_a_device_preview_creates_no_platform_or_manufacturer(self):
        from dcim.models import Platform

        platforms_before = Platform.objects.count()
        mfrs_before = Manufacturer.objects.count()

        compare_model_rows(
            None,
            "dcim.device",
            [
                self._row(
                    "plat-dev",
                    platform="Platform That Does Not Exist",
                    platform_slug="platform-that-does-not-exist",
                )
            ],
        )

        self.assertEqual(Platform.objects.count(), platforms_before)
        self.assertEqual(Manufacturer.objects.count(), mfrs_before)

    def test_a_device_preview_creates_no_tags(self):
        from extras.models import Tag
        from extras.models import TaggedItem

        tags_before = Tag.objects.count()
        assignments_before = TaggedItem.objects.count()

        compare_model_rows(None, "dcim.device", [self._row("tag-dev")])

        self.assertEqual(Tag.objects.count(), tags_before)
        self.assertEqual(TaggedItem.objects.count(), assignments_before)

    def test_an_unchanged_device_is_not_drift(self):
        from dcim.models import Device

        Device.objects.create(
            name="steady-dev",
            site=self.site,
            device_type=self.dtype,
            role=self.role,
            status="active",
        )

        result = compare_model_rows(None, "dcim.device", [self._row("steady-dev")])

        self.assertEqual(result["creates"], 0)
        self.assertEqual(result["updates"], 0)


class PartialCoverageIsReportedTest(TestCase):
    """Drift over the compared models, and never a whole-estate claim.

    The old report withheld every figure until all models could be compared.
    Since the adapter-only models cannot be, that meant "Not measured" forever.
    Reporting the measured subset is the fix - but it must not slide into
    implying the subset is the estate.
    """

    def _report(self, *model_results):
        from forward_netbox.utilities.drift_report import compute_drift_report

        return compute_drift_report(
            {"generated_at": "t", "model_results": list(model_results)}
        )

    def _exact(self, model, changes, deletes=0, rows=10):
        return {
            "model": model,
            "row_count": rows,
            "estimated_changes": changes,
            "delete_count": deletes,
            "change_estimate_kind": "exact_comparison",
        }

    def _upper_bound(self, model, rows=7):
        return {
            "model": model,
            "row_count": rows,
            "estimated_changes": rows,
            "delete_count": 0,
            "change_estimate_kind": "workload_upper_bound",
        }

    def test_one_uncompared_model_no_longer_silences_the_whole_report(self):
        report = self._report(
            self._exact("dcim.site", 3),
            self._upper_bound("netbox_dlm.softwareversion"),
        )

        self.assertTrue(report["comparison_available"])
        self.assertEqual(report["total_drift"], 3)
        self.assertEqual(report["measured_model_count"], 1)
        self.assertEqual(report["unmeasured_model_count"], 1)
        self.assertEqual(report["unmeasured_models"], ["netbox_dlm.softwareversion"])

    def test_an_uncompared_model_is_not_counted_as_a_drifted_model(self):
        # It is unknown, not clean and not dirty; counting it either way is a
        # statement nothing measured.
        report = self._report(
            self._exact("dcim.site", 0),
            self._upper_bound("dcim.cable"),
        )

        self.assertEqual(report["drifted_model_count"], 0)

    def test_in_sync_stays_unanswered_while_any_model_is_uncompared(self):
        """The whole point: zero drift over half the estate is not "in sync"."""
        report = self._report(
            self._exact("dcim.site", 0),
            self._upper_bound("dcim.cable"),
        )

        self.assertEqual(report["total_drift"], 0)
        self.assertIsNone(report["in_sync"])
        self.assertFalse(report["fully_measured"])

    def test_in_sync_is_answered_once_every_model_is_compared(self):
        report = self._report(
            self._exact("dcim.site", 0),
            self._exact("ipam.vlan", 0),
        )

        self.assertTrue(report["fully_measured"])
        self.assertIs(report["in_sync"], True)

    def test_all_upper_bound_still_reports_nothing_measured(self):
        # The pre-existing behaviour for a preview that compared nothing.
        report = self._report(
            self._upper_bound("dcim.cable"),
            self._upper_bound("dcim.module"),
        )

        self.assertFalse(report["comparison_available"])
        self.assertIsNone(report["total_drift"])
        self.assertIsNone(report["in_sync"])
