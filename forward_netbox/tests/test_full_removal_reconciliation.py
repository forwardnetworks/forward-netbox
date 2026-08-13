# A full execution used to compute no removals at all. Removals reached NetBox
# only through a Forward NQE diff, which reports what the CURRENT query stopped
# returning - so every row a map wrote before it was re-pointed at a different
# query stayed forever, for every model. A customer saw it as duplicate DLM
# hardware notices: the same hardware under Forward's part number and under the
# NetBox Device Type Library name, each with a notice, neither ever collected.
#
# These tests are deliberately weighted toward what must NOT be removed. The
# comparison is destructive and a narrowed query looks identical to real churn,
# which is the failure orphan prune already taught this codebase to fear.
from django.test import SimpleTestCase

from forward_netbox.utilities.full_removal_reconciliation import compute_full_removals
from forward_netbox.utilities.full_removal_reconciliation import (
    RemovalReconciliationRefused,
)

COALESCE = (("device_type_slug",),)


def _notice(slug):
    return {"device_type": slug.upper(), "device_type_slug": slug}


class FullRemovalReconciliationTest(SimpleTestCase):
    def _removals(self, current, previous, **kwargs):
        return compute_full_removals(
            "netbox_dlm.hardwarenotice",
            current_rows=current,
            previous_rows=previous,
            coalesce_fields=COALESCE,
            **kwargs,
        )

    def test_a_row_the_current_result_dropped_is_removed(self):
        removals = self._removals([_notice("kept")], [_notice("kept"), _notice("gone")])

        self.assertEqual([row["device_type_slug"] for row in removals], ["gone"])

    def test_the_customer_case_a_re_pointed_map_collects_its_old_rows(self):
        # The base query emitted Forward's part number; the alias variant emits
        # the Device Type Library slug for the same hardware. Both notices exist
        # and only the second is now written, so the first must be collected.
        previous = [_notice("n9k-c93180yc-fx"), _notice("c9500-40x")]
        current = [_notice("cisco-n9k-c93180yc-fx"), _notice("cisco-c9500-40x")]

        removals = self._removals(current, previous)

        self.assertEqual(
            sorted(row["device_type_slug"] for row in removals),
            ["c9500-40x", "n9k-c93180yc-fx"],
        )

    def test_an_unchanged_result_removes_nothing(self):
        rows = [_notice("a"), _notice("b")]

        self.assertEqual(self._removals(rows, list(rows)), [])

    def test_an_empty_current_result_removes_nothing(self):
        # The most dangerous input there is: a query that failed open, a
        # permission change, an emptied collection region. It would otherwise
        # remove the entire model.
        previous = [_notice(f"row-{index}") for index in range(50)]

        self.assertEqual(self._removals([], previous), [])

    def test_no_baseline_removes_nothing(self):
        # `None` means "cannot prove what was written", including a payload that
        # failed its own checksum. That is a reason to delete less, never more.
        self.assertEqual(self._removals([_notice("a")], None), [])

    def test_a_large_removal_share_is_refused_rather_than_applied(self):
        previous = [_notice(f"row-{index}") for index in range(100)]
        current = previous[:50]

        with self.assertRaises(RemovalReconciliationRefused) as caught:
            self._removals(current, previous)

        message = str(caught.exception)
        self.assertIn("50 of 100", message)
        self.assertIn("narrowed query", message)

    def test_a_small_absolute_drop_is_allowed_through_the_percentage(self):
        # Percentage alone trips constantly on small reference models: 3 rows of
        # 5 is 60% and means nothing. The row floor is what keeps that usable.
        previous = [_notice(f"row-{index}") for index in range(5)]
        current = previous[:2]

        removals = self._removals(current, previous)

        self.assertEqual(len(removals), 3)

    def test_a_row_with_no_computable_identity_is_never_removed(self):
        # It cannot be matched against the current result either, so treating it
        # as absent would delete on the strength of a missing field.
        previous = [_notice("a"), {"device_type": "no slug here"}]

        removals = self._removals([_notice("a")], previous)

        self.assertEqual(removals, [])

    def test_the_refusal_limit_cannot_be_widened_past_the_default(self):
        # The caller may tighten but the guard exists precisely because the
        # validation row-shrink guard skips when scope configuration changed.
        previous = [_notice(f"row-{index}") for index in range(100)]
        current = previous[:79]

        with self.assertRaises(RemovalReconciliationRefused):
            self._removals(current, previous, max_removal_percent=10)


class BaselineRemovalAllowlistTest(SimpleTestCase):
    """Devices are never removed by a baseline comparison.

    2.7.11 applied this to every model, which made a full sync delete devices
    absent from the current result. Device removal is gated behind Scope
    Reconciliation -> Prune orphans, with a shrink guard and a "confirm in
    Forward before deleting anything" warning, precisely because absence from a
    query result is not evidence a device is gone. Reconciling devices here
    bypassed that gate and did it unattended, on every full run.

    A deployment on 2.7.12 showed it in one run: a `dcim.device` ProtectedError
    and five `netbox_dlm.softwareversion` protected-delete skips.
    """

    def _removals(self, model_string, current, previous, coalesce):
        return compute_full_removals(
            model_string,
            current_rows=current,
            previous_rows=previous,
            coalesce_fields=coalesce,
        )

    def test_a_device_absent_from_the_result_is_never_removed(self):
        coalesce = (("name",),)
        previous = [{"name": "kept"}, {"name": "gone"}]

        removals = self._removals("dcim.device", [{"name": "kept"}], previous, coalesce)

        self.assertEqual(removals, [])

    def test_a_site_is_never_removed(self):
        coalesce = (("slug",),)
        previous = [{"slug": "kept"}, {"slug": "gone"}]

        removals = self._removals("dcim.site", [{"slug": "kept"}], previous, coalesce)

        self.assertEqual(removals, [])

    def test_a_shared_catalogue_with_children_is_never_removed(self):
        # The five protected-delete skips: a software version still referenced
        # by inventory-item software cannot be deleted, and should not be tried.
        coalesce = (("platform_slug", "version"),)
        previous = [
            {"platform_slug": "ios", "version": "15.2"},
            {"platform_slug": "ios", "version": "12.4"},
        ]

        removals = self._removals(
            "netbox_dlm.softwareversion",
            [{"platform_slug": "ios", "version": "15.2"}],
            previous,
            coalesce,
        )

        self.assertEqual(removals, [])

    def test_global_ipam_is_never_removed(self):
        coalesce = (("prefix",),)
        previous = [{"prefix": "10.0.0.0/8"}, {"prefix": "10.1.0.0/16"}]

        removals = self._removals(
            "ipam.prefix", [{"prefix": "10.0.0.0/8"}], previous, coalesce
        )

        self.assertEqual(removals, [])

    def test_a_derived_row_is_still_removed(self):
        # The behaviour the reconciliation exists for must survive the fix.
        coalesce = (("device", "name"),)
        previous = [
            {"device": "sw1", "name": "Gi0/1"},
            {"device": "sw1", "name": "Gi0/2"},
        ]

        removals = self._removals(
            "dcim.interface",
            [{"device": "sw1", "name": "Gi0/1"}],
            previous,
            coalesce,
        )

        self.assertEqual([row["name"] for row in removals], ["Gi0/2"])
