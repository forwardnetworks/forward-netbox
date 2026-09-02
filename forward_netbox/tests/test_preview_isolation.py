# One model's comparison must not take the whole dependency preview down.
#
# 2.8.8's regression: one missing attribute in one model's classification
# raised out of the preview loop and failed the job - every model "Not
# measured", the report empty. Its plan recorded per-model isolation as the fix
# that would have turned the outage into a single cell.
from unittest.mock import patch

from django.test import TestCase
from rq.timeouts import JobTimeoutException

from forward_netbox.views import _compare_rows_by_model


def _boom(sync, model_string, rows):
    if model_string == "broken.model":
        raise AttributeError(
            "'PreviewRunner' object has no attribute '_optional_model'"
        )
    return {"creates": len(rows), "updates": 0, "unchanged": 0, "rejected": 0}


class PreviewIsolationTest(TestCase):
    def test_one_failing_model_is_a_single_cell_not_an_outage(self):
        rows = {
            "dcim.site": [{"name": "a"}],
            "broken.model": [{}],
            "dcim.vrf": [{}, {}],
        }
        with patch(
            "forward_netbox.utilities.drift_comparison.compare_model_rows",
            side_effect=_boom,
        ):
            result = _compare_rows_by_model(None, rows)

        self.assertEqual(result["comparison"]["dcim.site"]["creates"], 1)
        self.assertEqual(result["comparison"]["dcim.vrf"]["creates"], 2)
        self.assertIsNone(result["comparison"]["broken.model"])
        self.assertEqual(result["errors"], {"broken.model": "AttributeError"})
        # Cost is still recorded for the model that failed, so a slow failure
        # is visible as such.
        self.assertIn("broken.model", result["runtime_ms"])

    def test_the_error_is_a_name_not_a_message(self):
        with patch(
            "forward_netbox.utilities.drift_comparison.compare_model_rows",
            side_effect=_boom,
        ):
            result = _compare_rows_by_model(None, {"broken.model": [{}]})
        self.assertNotIn("_optional_model", result["errors"]["broken.model"])

    def test_a_job_timeout_is_not_swallowed(self):
        def timeout(sync, model_string, rows):
            raise JobTimeoutException("torn down")

        with patch(
            "forward_netbox.utilities.drift_comparison.compare_model_rows",
            side_effect=timeout,
        ):
            with self.assertRaises(JobTimeoutException):
                _compare_rows_by_model(None, {"dcim.site": [{}]})
