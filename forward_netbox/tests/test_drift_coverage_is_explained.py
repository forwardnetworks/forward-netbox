"""When nothing was measured, the report must say WHY.

A deployment on 2.8.2 read "Not measured" in every cell of the drift report.
The same three words cover three unrelated situations:

  1. the preview predates drift measurement, and re-running it fixes everything
  2. the preview ran and compared nothing, which is a defect
  3. some models are compared and some are not, which is normal and fine

Only the first is fixed by re-running the preview, and the operator had no way
to tell it from the second. The support exchange that followed was two rounds
of guessing, because the report already computed `measured_model_count` and
`unmeasured_models` in 2.8.1 and then displayed neither.

The rule this encodes: a summary that can be produced from unmeasured data must
say how much data it measured.
"""

from django.test import SimpleTestCase

from forward_netbox.utilities.drift_report import compute_drift_report


def _model(name, *, exact, row_count=10, changes=10, deletes=0):
    return {
        "model": name,
        "row_count": row_count,
        "estimated_changes": changes,
        "delete_count": deletes,
        "change_estimate_kind": "exact_comparison" if exact else "workload_upper_bound",
    }


class CoverageIsReportedTest(SimpleTestCase):
    def test_a_modern_payload_is_not_flagged_as_predating_measurement(self):
        report = compute_drift_report(
            {
                "model_results": [_model("dcim.site", exact=True)],
                "comparison_coverage": {"measured_models": 1, "total_models": 1},
            }
        )
        self.assertFalse(report["payload_predates_measurement"])

    def test_a_payload_without_coverage_predates_measurement(self):
        # The signal is the ABSENCE of a key every preview has written since
        # 2.8.1, so it dates the payload rather than describing it.
        report = compute_drift_report(
            {"model_results": [_model("dcim.site", exact=False)]}
        )
        self.assertTrue(report["payload_predates_measurement"])

    def test_a_modern_payload_that_measured_nothing_is_a_defect_not_staleness(self):
        # The distinction the customer needed. Re-running the preview fixes the
        # stale case and does nothing for this one, so they must not read alike.
        report = compute_drift_report(
            {
                "model_results": [
                    _model("dcim.site", exact=False),
                    _model("dcim.device", exact=False),
                ],
                "comparison_coverage": {"measured_models": 0, "total_models": 2},
            }
        )
        self.assertEqual(0, report["measured_model_count"])
        self.assertFalse(report["payload_predates_measurement"])

    def test_an_empty_payload_is_not_flagged(self):
        # Nothing to be stale about; the report has no rows at all.
        report = compute_drift_report({"model_results": []})
        self.assertFalse(report["payload_predates_measurement"])

    def test_partial_coverage_names_the_unmeasured_models(self):
        report = compute_drift_report(
            {
                "model_results": [
                    _model("dcim.site", exact=True, changes=0),
                    _model("dcim.cable", exact=False),
                ],
                "comparison_coverage": {"measured_models": 1, "total_models": 2},
            }
        )
        self.assertEqual(1, report["measured_model_count"])
        self.assertEqual(1, report["unmeasured_model_count"])
        self.assertEqual(["dcim.cable"], report["unmeasured_models"])
        self.assertTrue(report["comparison_available"])

    def test_drifted_models_is_reported_over_what_was_measured(self):
        # "1 / 5" when only two models were compared invites reading the other
        # three as in sync. The denominator is the measured set.
        report = compute_drift_report(
            {
                "model_results": [
                    _model("dcim.site", exact=True, changes=4),
                    _model("dcim.device", exact=True, changes=0, row_count=3),
                    _model("dcim.cable", exact=False),
                ],
                "comparison_coverage": {"measured_models": 2, "total_models": 3},
            }
        )
        self.assertEqual(2, report["measured_model_count"])
        self.assertEqual(1, report["drifted_model_count"])
        self.assertIsNone(report["in_sync"])
