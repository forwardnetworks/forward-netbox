"""The preview measures its own cost, so nobody has to estimate it.

Measuring drift on a converged estate is work the preview did not previously
do: every model now classifies its full row set where the whole thing used to
be skipped and reported as unmeasured. The job cannot time out over it - every
Forward job is floored at 7200s and the entire 1M-row merge projection is under
1800s - but "cannot fail" is not "costs nothing", and the runtime on a large
fabric has never been observed.

Rather than estimate it, the preview records it. These tests pin what the
report does with that number, including the two ways it can be missing: a
payload written before the cost was recorded, and a model that was never
compared and therefore has no time to report.
"""

from django.test import SimpleTestCase

from forward_netbox.utilities.drift_report import compute_drift_report


def _payload(models, coverage=None):
    payload = {"model_results": models}
    if coverage is not None:
        payload["comparison_coverage"] = coverage
    return payload


class TheReportCarriesTheCostTest(SimpleTestCase):
    def test_runtime_and_rows_come_from_the_preview(self):
        report = compute_drift_report(
            _payload(
                [
                    {
                        "model": "dcim.site",
                        "row_count": 10,
                        "estimated_changes": 0,
                        "change_estimate_kind": "exact_comparison",
                        "comparison_runtime_ms": 25.0,
                    }
                ],
                coverage={
                    "measured_models": 1,
                    "total_models": 1,
                    "runtime_ms": 25.0,
                    "rows_compared": 10,
                },
            )
        )
        self.assertEqual(report["comparison_runtime_ms"], 25.0)
        self.assertEqual(report["comparison_rows_compared"], 10)

    def test_a_payload_without_the_cost_reports_none_not_zero(self):
        # Zero would read as "instant" for a preview that never reported.
        report = compute_drift_report(
            _payload(
                [
                    {
                        "model": "dcim.site",
                        "row_count": 10,
                        "estimated_changes": 0,
                        "change_estimate_kind": "exact_comparison",
                    }
                ],
                coverage={"measured_models": 1, "total_models": 1},
            )
        )
        self.assertIsNone(report["comparison_runtime_ms"])
        self.assertIsNone(report["comparison_rows_compared"])


class TheSlowestModelIsNamedTest(SimpleTestCase):
    """A total hides one outlier, and the two need different responses."""

    def test_the_slowest_compared_model_is_identified(self):
        report = compute_drift_report(
            _payload(
                [
                    {
                        "model": "dcim.site",
                        "row_count": 10,
                        "estimated_changes": 0,
                        "change_estimate_kind": "exact_comparison",
                        "comparison_runtime_ms": 12.0,
                    },
                    {
                        "model": "dcim.interface",
                        "row_count": 100000,
                        "estimated_changes": 0,
                        "change_estimate_kind": "exact_comparison",
                        "comparison_runtime_ms": 9000.0,
                    },
                ],
                coverage={"measured_models": 2, "total_models": 2},
            )
        )
        self.assertEqual(report["slowest_compared_model"]["model"], "dcim.interface")
        self.assertEqual(report["slowest_compared_model"]["runtime_ms"], 9000.0)

    def test_an_uncompared_model_is_never_named_as_slowest(self):
        # It has no comparison, so any time against it is not a measurement.
        report = compute_drift_report(
            _payload(
                [
                    {
                        "model": "netbox_dlm.softwareversion",
                        "row_count": 45,
                        "estimated_changes": 45,
                        "change_estimate_kind": "workload_upper_bound",
                        "comparison_runtime_ms": 5000.0,
                    },
                    {
                        "model": "dcim.site",
                        "row_count": 10,
                        "estimated_changes": 0,
                        "change_estimate_kind": "exact_comparison",
                        "comparison_runtime_ms": 12.0,
                    },
                ],
                coverage={"measured_models": 1, "total_models": 2},
            )
        )
        self.assertEqual(report["slowest_compared_model"]["model"], "dcim.site")

    def test_no_timings_at_all_reports_none(self):
        report = compute_drift_report(
            _payload(
                [
                    {
                        "model": "dcim.site",
                        "row_count": 10,
                        "estimated_changes": 0,
                        "change_estimate_kind": "exact_comparison",
                    }
                ],
                coverage={"measured_models": 1, "total_models": 1},
            )
        )
        self.assertIsNone(report["slowest_compared_model"])
