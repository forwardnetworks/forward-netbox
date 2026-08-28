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

from dcim.models import Site
from django.test import SimpleTestCase
from django.test import TestCase

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


class TheReportCarriesTheQueryCountTest(SimpleTestCase):
    """Milliseconds alone cannot say WHY a model was slow.

    A deployment reported `dcim.macaddress` at 276,377 ms for 122,478 fully
    converged rows - 2.26 ms/row, against 0.065 ms/row measured locally for the
    same converged shape. Every hypothesis testable locally was eliminated, and
    the two that remain want opposite fixes: a high query count is chatter to
    batch, a low count against a high runtime is work inside Python. The report
    could not tell them apart, so it now carries both.
    """

    def test_the_query_count_comes_from_the_preview(self):
        report = compute_drift_report(
            _payload(
                [
                    {
                        "model": "dcim.macaddress",
                        "row_count": 122478,
                        "estimated_changes": 0,
                        "change_estimate_kind": "exact_comparison",
                        "comparison_runtime_ms": 276377.0,
                        "comparison_queries": 245,
                    }
                ],
                coverage={
                    "measured_models": 1,
                    "total_models": 1,
                    "runtime_ms": 276377.0,
                    "queries": 245,
                    "rows_compared": 122478,
                },
            )
        )
        self.assertEqual(report["comparison_queries"], 245)

    def test_the_slowest_model_carries_its_query_count(self):
        # Naming the slowest model is half an answer; the count is the half
        # that says which kind of slow it is.
        report = compute_drift_report(
            _payload(
                [
                    {
                        "model": "dcim.macaddress",
                        "row_count": 122478,
                        "estimated_changes": 0,
                        "change_estimate_kind": "exact_comparison",
                        "comparison_runtime_ms": 276377.0,
                        "comparison_queries": 245,
                    },
                    {
                        "model": "dcim.site",
                        "row_count": 93,
                        "estimated_changes": 0,
                        "change_estimate_kind": "exact_comparison",
                        "comparison_runtime_ms": 12.0,
                        "comparison_queries": 4,
                    },
                ],
                coverage={"measured_models": 2, "total_models": 2},
            )
        )
        self.assertEqual(report["slowest_compared_model"]["model"], "dcim.macaddress")
        self.assertEqual(report["slowest_compared_model"]["queries"], 245)

    def test_a_payload_without_a_query_count_reports_none_not_zero(self):
        # A preview written before this existed reported no count. Zero would
        # read as "issued no queries", which is a measurement it never made -
        # the same reason the runtime reports None rather than 0.
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
        self.assertIsNone(report["comparison_queries"])
        self.assertIsNone(report["slowest_compared_model"]["queries"])


class TheQueryCounterActuallyCountsTest(TestCase):
    """The tests above feed a synthetic payload; this one tests the meter.

    Without this, everything above could pass while the preview reported a
    count it never took - the plumbing verified and the measurement absent.
    """

    def test_it_counts_queries_without_debug(self):
        """The property that ruled out `connection.queries`.

        `connection.queries` only populates when DEBUG is on, and a release
        deployment never runs with it - so the number that matters most would
        have been exactly the one that is missing. This asserts the count is
        taken with DEBUG off, which is how it will run in production.
        """
        from django.db import connection
        from django.test import override_settings

        from forward_netbox.views import _QueryCounter

        counter = _QueryCounter()
        with override_settings(DEBUG=False):
            with connection.execute_wrapper(counter):
                Site.objects.count()
                Site.objects.count()

        self.assertEqual(counter.count, 2)

    def test_it_counts_nothing_when_nothing_runs(self):
        from django.db import connection

        from forward_netbox.views import _QueryCounter

        counter = _QueryCounter()
        with connection.execute_wrapper(counter):
            pass

        self.assertEqual(counter.count, 0)

    def test_the_wrapper_returns_the_query_result_unchanged(self):
        # A counter that swallowed or altered results would corrupt every
        # comparison it measured.
        from django.db import connection

        from forward_netbox.views import _QueryCounter

        Site.objects.create(name="Counter Site", slug="counter-site")
        counter = _QueryCounter()
        with connection.execute_wrapper(counter):
            names = list(
                Site.objects.filter(slug="counter-site").values_list("name", flat=True)
            )

        self.assertEqual(names, ["Counter Site"])
        self.assertGreater(counter.count, 0)
