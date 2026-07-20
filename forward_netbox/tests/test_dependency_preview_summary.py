from django.test import SimpleTestCase

from forward_netbox.utilities.query_fetch_execution import ForwardModelResult
from forward_netbox.views import _dependency_model_result_summary


class DependencyModelResultSummaryTest(SimpleTestCase):
    """Regression: fetcher.model_results are ForwardModelResult dataclasses, but
    _dependency_model_result_summary called result.get(...) — AttributeError that
    errored the whole dependency preview (hidden as null-data until 2.2.4 surfaced
    job errors). The summary must accept a ForwardModelResult.
    """

    def _result(self, **kw):
        base = dict(
            model_string="ipam.prefix",
            query_name="Forward Prefixes",
            execution_mode="query_path",
            execution_value="",
            sync_mode="branching",
            row_count=12,
            delete_count=3,
        )
        base.update(kw)
        return ForwardModelResult(**base)

    def test_summary_accepts_dataclass(self):
        summary = _dependency_model_result_summary(self._result())
        self.assertEqual(summary["model"], "ipam.prefix")
        self.assertEqual(summary["row_count"], 12)
        self.assertEqual(summary["delete_count"], 3)
        # estimated_changes is derived (as_dict has no such field).
        self.assertEqual(summary["estimated_changes"], 15)
        self.assertEqual(summary["change_estimate_kind"], "workload_upper_bound")

    def test_summary_handles_none_runtime(self):
        # runtime_ms defaults to None on the dataclass; must not blow up.
        summary = _dependency_model_result_summary(self._result(runtime_ms=None))
        self.assertEqual(summary["runtime_ms"], 0.0)

    def test_summary_rejects_noncanonical_plain_dict(self):
        with self.assertRaisesRegex(TypeError, "must be ForwardModelResult"):
            _dependency_model_result_summary(
                {"model": "dcim.device", "row_count": 5, "delete_count": 1}
            )
