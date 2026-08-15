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
        """The two paths still without a comparison.

        `device` creates Tag and TaggedItem rows. `virtualchassis` creates
        VirtualChassis rows and then reads their pks back to assign devices, so
        skipping the first phase leaves the second unclassifiable. Returning
        before their final write would not make either read-only, so until each
        is handled they must decline to answer rather than answer wrongly.
        """
        for model_string in (
            "dcim.device",
            "dcim.virtualchassis",
        ):
            with self.subTest(model=model_string):
                self.assertIsNone(
                    compare_model_rows(None, model_string, [{"name": "x"}]),
                    f"{model_string} has no comparison yet and must not claim one",
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
