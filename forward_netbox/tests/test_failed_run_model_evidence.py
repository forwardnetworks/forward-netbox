"""A failed run must name WHICH models failed and WHY.

On a successful dependency preview the support bundle carries `model_results`
with per-model `failure_count`, `fetch_mode` and `query_name`. On the run that
actually failed it carried none of that: a customer's sync failed identically
across five ingestions, thirty models failed within two seconds, and every one
of them recorded the same sentence with no way to tell them apart.

`ForwardModelResult` already recorded *that* a model failed. These pin the two
missing halves - the exception class and the safe reason slug - and the paths
that carry them to the ingestion issue and the support bundle.

The redaction rule is unchanged in kind: only the model string, exception class
and an allowlisted slug travel. No device name, address, hostname, interface
name or tenant label may appear in any of them.
"""

from django.test import SimpleTestCase

from forward_netbox.exceptions import ForwardQueryError
from forward_netbox.utilities.diagnostics import model_failure_summary
from forward_netbox.utilities.query_fetch_execution import ForwardModelResult
from forward_netbox.utilities.query_fetch_execution import ForwardQueryFetcher

CUSTOMER_TOKENS = ("dc11-edge-01", "core-sw1.corp.example", "10.11.12.13")


class _Context:
    snapshot_id = "snapshot-1"


class _Spec:
    query_name = "Forward Devices"
    execution_mode = "query_id"
    execution_value = "Q-1"


class _Logger:
    def __init__(self):
        self.warnings = []

    def log_warning(self, message, obj=None):
        self.warnings.append(message)

    def log_info(self, message, obj=None):
        pass


class _Sync:
    pk = 1
    parameters = {}
    source = None


def _fetcher():
    return ForwardQueryFetcher(_Sync(), None, _Logger())


class ModelResultCarriesItsFailureReasonTest(SimpleTestCase):
    def test_a_recorded_failure_names_the_model_and_a_non_empty_reason(self):
        fetcher = _fetcher()
        fetcher._record_model_failure(
            _Context(),
            "dcim.device",
            _Spec(),
            ForwardQueryError(
                "No enabled NQE maps were resolved for dcim.device. "
                "Enable at least one NQE Map."
            ),
            sync_mode="planning",
        )
        result = fetcher._failed_model_results["dcim.device"]
        self.assertEqual(result.model_string, "dcim.device")
        self.assertEqual(result.failure_count, 1)
        self.assertEqual(result.failure_exception, "ForwardQueryError")
        self.assertEqual(result.failure_reason, "no-enabled-query-maps")

    def test_an_uncatalogued_failure_records_that_it_was_uncatalogued(self):
        # Never silent: an unmatched reason is a prompt to extend the catalogue,
        # not a reason to fall back to an empty field.
        fetcher = _fetcher()
        fetcher._record_model_failure(
            _Context(),
            "dcim.interface",
            _Spec(),
            ForwardQueryError("something nobody has catalogued yet"),
            sync_mode="planning",
        )
        result = fetcher._failed_model_results["dcim.interface"]
        self.assertEqual(result.failure_reason, "unrecognized-fetch-failure")

    def test_two_models_failing_differently_record_differently(self):
        # The customer case inverted: thirty identical sentences told nobody
        # anything, because the reason never reached the record.
        fetcher = _fetcher()
        fetcher._record_model_failure(
            _Context(),
            "dcim.device",
            _Spec(),
            ForwardQueryError("Forward sync requires a network ID on the sync."),
            sync_mode="planning",
        )
        fetcher._record_model_failure(
            _Context(),
            "dcim.interface",
            _Spec(),
            ForwardQueryError("No enabled NQE maps were resolved for dcim.interface."),
            sync_mode="planning",
        )
        reasons = {
            result.model_string: result.failure_reason
            for result in fetcher._failed_model_results.values()
        }
        self.assertEqual(
            reasons,
            {
                "dcim.device": "missing-network-id",
                "dcim.interface": "no-enabled-query-maps",
            },
        )

    def test_the_skip_warning_no_longer_reduces_to_a_class_name(self):
        fetcher = _fetcher()
        fetcher._record_model_failure(
            _Context(),
            "dcim.device",
            _Spec(),
            ForwardQueryError("No enabled NQE maps were resolved for dcim.device."),
            sync_mode="planning",
        )
        self.assertIn("no-enabled-query-maps", fetcher.logger.warnings[0])

    def test_the_failure_fields_reach_the_serialized_result(self):
        # `as_dict` is what the ingestion persists and the bundle reads.
        fetcher = _fetcher()
        fetcher._record_model_failure(
            _Context(),
            "dcim.device",
            _Spec(),
            ForwardQueryError("No enabled NQE maps were resolved for dcim.device."),
            sync_mode="planning",
        )
        data = fetcher._failed_model_results["dcim.device"].as_dict()
        self.assertEqual(data["failure_exception"], "ForwardQueryError")
        self.assertEqual(data["failure_reason"], "no-enabled-query-maps")

    def test_a_customer_bearing_message_leaves_nothing_behind(self):
        fetcher = _fetcher()
        fetcher._record_model_failure(
            _Context(),
            "dcim.device",
            _Spec(),
            ForwardQueryError(
                "Device dc11-edge-01 (core-sw1.corp.example) at 10.11.12.13 failed."
            ),
            sync_mode="planning",
        )
        result = fetcher._failed_model_results["dcim.device"]
        persisted = str(result.as_dict()) + " " + " ".join(fetcher.logger.warnings)
        for token in CUSTOMER_TOKENS:
            with self.subTest(token=token):
                self.assertNotIn(token, persisted)

    def test_a_successful_result_carries_no_failure_fields(self):
        result = ForwardModelResult(
            model_string="dcim.device",
            query_name="Forward Devices",
            execution_mode="query_id",
            execution_value="Q-1",
            sync_mode="full",
            row_count=10,
        )
        self.assertEqual(result.failure_exception, "")
        self.assertEqual(result.failure_reason, "")
        self.assertEqual(result.as_dict()["failure_reason"], "")


class BundleNamesTheFailingModelsTest(SimpleTestCase):
    """The bundle summary built from the persisted `model_results`."""

    def test_a_wholesale_fetch_failure_names_every_failing_model(self):
        model_results = [
            ForwardModelResult(
                model_string=model,
                query_name="q",
                execution_mode="query_id",
                execution_value="Q-1",
                sync_mode="planning",
                row_count=0,
                failure_count=1,
                failure_exception="ForwardQueryError",
                failure_reason=reason,
            ).as_dict()
            for model, reason in (
                ("dcim.device", "no-enabled-query-maps"),
                ("dcim.interface", "diff-required-no-baseline"),
                ("ipam.ipaddress", "no-enabled-query-maps"),
            )
        ]
        summary = model_failure_summary(model_results)
        self.assertEqual(
            [item["model"] for item in summary],
            ["dcim.device", "dcim.interface", "ipam.ipaddress"],
        )
        self.assertEqual(
            {item["reason"] for item in summary},
            {"no-enabled-query-maps", "diff-required-no-baseline"},
        )
