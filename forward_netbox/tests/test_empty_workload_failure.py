"""An empty workload set must not be reported as a converged sync.

Reproduces the customer-reported 2.6.3 failure: every model fetch was rejected
by the execution-contract preflight, no workloads were produced, and the run
recorded a completed, validation-passed, branchless ingestion with zero changes.
That is indistinguishable from a genuinely converged sync, so the failure was
invisible and the Drift report stayed "not measured".
"""

from dataclasses import dataclass

from django.test import SimpleTestCase

from forward_netbox.utilities.single_branch_executor import failed_model_strings


@dataclass
class _Result:
    model_string: str
    failure_count: int = 0
    failure_reason: str = ""


class FailedModelStringsTest(SimpleTestCase):
    def test_convergence_reports_no_failures(self):
        results = [
            _Result("dcim.device"),
            _Result("dcim.interface"),
        ]
        self.assertEqual(failed_model_strings(results), [])

    def test_single_failed_model_is_reported(self):
        results = [
            _Result("dcim.device"),
            _Result("dcim.virtualchassis", failure_count=1),
        ]
        self.assertEqual(failed_model_strings(results), ["dcim.virtualchassis"])

    def test_wholesale_fetch_failure_is_reported(self):
        # The customer case: nothing fetched, so nothing to stage.
        models = ["dcim.device", "dcim.interface", "ipam.ipaddress"]
        results = [_Result(model, failure_count=1) for model in models]
        self.assertEqual(failed_model_strings(results), sorted(models))

    def test_results_are_deduplicated_and_ordered(self):
        results = [
            _Result("dcim.interface", failure_count=1),
            _Result("dcim.interface", failure_count=2),
            _Result("dcim.cable", failure_count=1),
        ]
        self.assertEqual(
            failed_model_strings(results), ["dcim.cable", "dcim.interface"]
        )

    def test_unnamed_models_are_dropped_rather_than_reported_blank(self):
        results = [_Result("", failure_count=1), _Result("dcim.site", failure_count=1)]
        self.assertEqual(failed_model_strings(results), ["dcim.site"])

    def test_empty_and_none_inputs_are_safe(self):
        self.assertEqual(failed_model_strings([]), [])
        self.assertEqual(failed_model_strings(None), [])

    def test_a_model_with_no_enabled_map_is_configuration_not_a_failure(self):
        # The sync enables a model whose NQE Map is off: a run with Forward
        # changes skips it and goes on, so an unchanged re-run must too.
        results = [
            _Result("dcim.device"),
            _Result("netbox_dlm.cve", 1, "no-enabled-query-maps"),
            _Result("dcim.cable", 1, "ForwardQueryError"),
        ]
        self.assertEqual(failed_model_strings(results), ["dcim.cable"])

    def test_with_nothing_fetched_even_unmapped_models_count(self):
        results = [
            _Result("dcim.device", 1, "no-enabled-query-maps"),
            _Result("dcim.interface", 1, "no-enabled-query-maps"),
        ]
        self.assertEqual(
            failed_model_strings(results), ["dcim.device", "dcim.interface"]
        )

    def test_one_fetched_model_and_one_unmapped_model_is_convergence(self):
        results = [
            _Result("dcim.device"),
            _Result("netbox_dlm.cve", 1, "no-enabled-query-maps"),
        ]
        self.assertEqual(failed_model_strings(results), [])
