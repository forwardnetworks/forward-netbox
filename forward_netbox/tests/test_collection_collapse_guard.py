# A customer lost 41795 objects because a Forward collection was CANCELLED and
# the resulting snapshot still processed green with a normal device count.
# `latestProcessed` selected it, every bundled query filters on
# `snapshotInfo.result == completed` and so returned nothing, the whole estate
# read as departed, and validation reported PASSED.
#
# It passed because the row-count floor compares only models executed in FULL -
# correctly, since a diff run's row_count counts changes, not rows - which
# leaves a diff run with no collapse protection whatsoever. Collection health is
# a property of the snapshot, so it is decidable before anything is staged and
# it holds for both execution modes.
#
# The real numbers from that incident are used deliberately.
from django.test import SimpleTestCase

from forward_netbox.utilities.validation import collection_collapse_finding
from forward_netbox.utilities.validation import collection_collapse_reason
from forward_netbox.utilities.validation import DEFAULT_MAX_COLLECTION_DROP_PERCENT


class CollectionCollapseGuardTest(SimpleTestCase):
    def _finding(self, current, baseline, **kwargs):
        return collection_collapse_finding(
            current_metrics={"numSuccessfulDevices": current},
            baseline_metrics={"numSuccessfulDevices": baseline},
            **kwargs,
        )

    def test_the_customer_incident_is_refused(self):
        # The incident's shape: 154 collected against ~5000 the day before.
        finding = self._finding(154, 4969)

        self.assertIsNotNone(finding)
        self.assertEqual(finding["current_collected"], 154)
        self.assertEqual(finding["dropped_devices"], 4815)
        self.assertGreater(finding["dropped_percent"], 95)

    def test_the_reason_names_the_counts_and_why_the_snapshot_looked_fine(self):
        reason = collection_collapse_reason(
            self._finding(154, 4969),
            max_drop_percent=DEFAULT_MAX_COLLECTION_DROP_PERCENT,
        )

        self.assertIn("154", reason)
        self.assertIn("4969", reason)
        # The trap that made this invisible must be stated where it is read.
        self.assertIn("green", reason)

    def test_a_healthy_snapshot_passes(self):
        # The recovery snapshot: the same count collected as the baseline.
        self.assertIsNone(self._finding(4969, 4969))

    def test_ordinary_collection_churn_passes(self):
        # A rack decommissioned and a handful of devices failing auth. A guard
        # that fires on ordinary weeks gets turned off.
        self.assertIsNone(self._finding(4700, 4969))

    def test_growth_passes(self):
        self.assertIsNone(self._finding(6000, 4969))

    def test_absent_metrics_never_read_as_zero(self):
        # An older ingestion predating the field, or a Forward response that
        # omitted it, would otherwise present as a total collapse.
        self.assertIsNone(
            collection_collapse_finding(
                current_metrics={}, baseline_metrics={"numSuccessfulDevices": 4969}
            )
        )
        self.assertIsNone(
            collection_collapse_finding(
                current_metrics={"numSuccessfulDevices": 154}, baseline_metrics={}
            )
        )
        self.assertIsNone(
            collection_collapse_finding(current_metrics=None, baseline_metrics=None)
        )

    def test_a_non_numeric_metric_is_ignored_rather_than_guessed(self):
        self.assertIsNone(
            collection_collapse_finding(
                current_metrics={"numSuccessfulDevices": "unknown"},
                baseline_metrics={"numSuccessfulDevices": 4969},
            )
        )

    def test_a_tiny_baseline_is_not_compared(self):
        # A lab moving 20 -> 2 is a 90% drop and means nothing.
        self.assertIsNone(self._finding(2, 20))

    def test_a_total_collapse_on_a_real_estate_is_refused(self):
        finding = self._finding(0, 4969)

        self.assertIsNotNone(finding)
        self.assertEqual(finding["dropped_percent"], 100.0)
