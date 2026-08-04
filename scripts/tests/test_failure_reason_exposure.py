"""A failure must say what it was, without saying whose it was.

A customer's sync failed identically across five ingestions and could not be
diagnosed for days. Auth, the Forward snapshot and scope resolution were all
ruled out; thirty models then failed within two seconds, and every one of them
recorded the single word `ForwardQueryError.` - the whole reason destroyed at
capture, before the logger and before the database, so no downstream tooling
could recover it at any price.

These pins run without Django or the NetBox stack: `diagnostics` imports only
the standard library, which is deliberate - it is the module every failure path
in the plugin formats through, and it should stay cheap enough to test directly.
"""

from __future__ import annotations

import importlib.util
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
DIAGNOSTICS_PATH = REPO_ROOT / "forward_netbox" / "utilities" / "diagnostics.py"


def _load_diagnostics():
    spec = importlib.util.spec_from_file_location(
        "forward_netbox_diagnostics_under_test",
        DIAGNOSTICS_PATH,
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


diagnostics = _load_diagnostics()


class _OwnershipConflictError(Exception):
    pass


# The names matter: `failure_reason` dispatches ownership handling on the class
# name so that `diagnostics` need not import `ownership` and create a cycle.
_OwnershipConflictError.__name__ = "OwnershipConflictError"


class ForwardQueryError(Exception):
    pass


class ForwardClientError(Exception):
    pass


class SyncError(Exception):
    pass


# Value-bearing tokens of every shape the plugin actually handles. If any of
# these survives into a summary, a reason slug or a rendered log row, the
# redaction has been widened rather than fixed.
CUSTOMER_TOKENS = (
    "dc11-edge-01",
    "core-sw1.corp.example",
    "10.11.12.13",
    "2001:db8::1",
    "GigabitEthernet0/0/2",
    "Mgmt_Vl211",
)
CUSTOMER_MESSAGE = (
    "Device dc11-edge-01 (core-sw1.corp.example) at 10.11.12.13 / 2001:db8::1 "
    "on GigabitEthernet0/0/2 tagged Mgmt_Vl211 was rejected."
)


class CaptureLevelTest(unittest.TestCase):
    """`_safe_exception_summary` used to return the bare class name."""

    def test_a_classified_failure_names_its_reason(self):
        summary = diagnostics.safe_exception_summary(
            ForwardQueryError(
                "Diff execution is required, but the resolved execution "
                "contract for dcim.device is full-only (map_set_incompatible)."
            )
        )
        self.assertEqual(
            summary,
            "ForwardQueryError: diff-required-full-only-contract.",
        )

    def test_an_http_status_survives_as_a_slug(self):
        # The status is the single most actionable token in a client error and
        # the wording masker drops it, because it carries digits.
        summary = diagnostics.safe_exception_summary(
            ForwardClientError(
                'Forward API request failed with HTTP 403: {"device":"dc11-edge-01"}'
            )
        )
        self.assertEqual(summary, "ForwardClientError: http-403.")
        self.assertNotIn("dc11-edge-01", summary)

    def test_a_structured_http_status_is_preferred_over_the_message(self):
        class _Response:
            status_code = 503

        class _Cause(Exception):
            response = _Response()

        exc = ForwardClientError("Forward API request failed.")
        exc.__cause__ = _Cause()
        self.assertEqual(
            diagnostics.safe_exception_summary(exc),
            "ForwardClientError: http-503.",
        )

    def test_an_uncatalogued_failure_keeps_wording_not_values(self):
        summary = diagnostics.safe_exception_summary(
            ForwardClientError(CUSTOMER_MESSAGE)
        )
        for token in CUSTOMER_TOKENS:
            self.assertNotIn(token, summary)
        self.assertEqual(summary, "ForwardClientError: Device.")

    def test_a_message_that_opens_with_customer_data_yields_the_class_alone(self):
        summary = diagnostics.safe_exception_summary(
            ForwardClientError("dc11-edge-01 refused the request")
        )
        self.assertEqual(summary, "ForwardClientError.")

    def test_every_summary_is_at_least_the_classifier(self):
        # Strictly more informative than the old behaviour, never less.
        for exc in (
            ForwardQueryError("No enabled NQE maps were resolved for x.y."),
            ForwardClientError(CUSTOMER_MESSAGE),
            SyncError(""),
        ):
            with self.subTest(exc=exc):
                self.assertTrue(
                    diagnostics.safe_exception_summary(exc).startswith(
                        exc.__class__.__name__
                    )
                )


class FailureReasonTest(unittest.TestCase):
    def test_reasons_are_allowlisted_slugs_only(self):
        # Nothing derived from message text may reach a reason slug.
        reason = diagnostics.failure_reason(ForwardClientError(CUSTOMER_MESSAGE))
        self.assertEqual(reason, "")
        for token in CUSTOMER_TOKENS:
            self.assertNotIn(token, reason)

    def test_known_conditions_resolve(self):
        cases = {
            "Forward API request timed out while connecting to Forward.": "timeout",
            "No enabled NQE maps were resolved for netbox_dlm.vulnerability.": (
                "no-enabled-query-maps"
            ),
            "Forward NQE fetch exceeded its wall-clock budget": (
                "fetch-budget-exceeded"
            ),
            "Execution contract preflight rejected an unsafe full contract "
            "for dcim.site: x.": "unsafe-full-contract",
            "Forward sync requires a network ID on the sync or its source.": (
                "missing-network-id"
            ),
        }
        for message, slug in cases.items():
            with self.subTest(slug=slug):
                self.assertEqual(
                    diagnostics.failure_reason(ForwardQueryError(message)),
                    slug,
                )


class OwnershipConflictStillWorksTest(unittest.TestCase):
    """The exemption this work replaced must not regress while replacing it."""

    def test_the_slug_still_resolves(self):
        self.assertEqual(
            diagnostics.ownership_conflict_reason(
                _OwnershipConflictError(
                    "Device dc11-edge-01 is already mapped to Forward source key SRC-77"
                )
            ),
            "device-already-mapped",
        )

    def test_the_shared_formatter_still_names_the_rule(self):
        message = diagnostics.safe_operation_failure(
            "Forward scope reconciliation",
            _OwnershipConflictError(
                "Forward device identity is ambiguous for source key abc"
            ),
        )
        self.assertEqual(
            message,
            "Forward scope reconciliation failed "
            "(OwnershipConflictError: identity-ambiguous).",
        )

    def test_an_uncatalogued_ownership_message_is_still_recorded(self):
        self.assertEqual(
            diagnostics.ownership_conflict_reason(
                _OwnershipConflictError("brand new condition")
            ),
            "unrecognized-ownership-conflict",
        )

    def test_the_device_key_never_reaches_the_message(self):
        message = diagnostics.safe_operation_failure(
            "Forward scope reconciliation",
            _OwnershipConflictError(
                "Device dc11-edge-01 is already mapped to Forward source key SRC-77"
            ),
        )
        self.assertNotIn("dc11-edge-01", message)
        self.assertNotIn("SRC-77", message)

    def test_an_unclassifiable_exception_is_unchanged(self):
        self.assertEqual(
            diagnostics.safe_operation_failure("Forward sync", ValueError("boom")),
            "Forward sync failed (ValueError).",
        )


class RenderLevelTest(unittest.TestCase):
    """`_sanitize_log_rows` flattened every failure row to one fixed sentence."""

    def _row(self, message, level="error"):
        return ["2026-08-04T00:00:00", level, "obj", "url", message]

    def test_the_classifier_survives_sanitization(self):
        rows = diagnostics._sanitize_log_rows(
            [
                self._row(
                    "Skipping dcim.device because Forward query validation "
                    "failed: ForwardQueryError: no-enabled-query-maps."
                )
            ]
        )
        self.assertEqual(
            rows[0][4],
            "The operation failed (ForwardQueryError: no-enabled-query-maps).",
        )

    def test_distinct_failures_no_longer_render_identically(self):
        rows = diagnostics._sanitize_log_rows(
            [
                self._row("Forward sync failed (ForwardQueryError: timeout)."),
                self._row("Forward sync failed (ForwardClientError: http-403)."),
            ]
        )
        self.assertNotEqual(rows[0][4], rows[1][4])

    def test_message_bodies_are_still_redacted(self):
        rows = diagnostics._sanitize_log_rows([self._row(CUSTOMER_MESSAGE)])
        for token in CUSTOMER_TOKENS:
            self.assertNotIn(token, rows[0][4])
        self.assertEqual(rows[0][4], diagnostics.SAFE_FAILURE_LOG_MESSAGE)

    def test_a_classifier_beside_customer_data_keeps_only_the_classifier(self):
        rows = diagnostics._sanitize_log_rows(
            [
                self._row(
                    f"Forward sync failed (ForwardClientError: http-403). "
                    f"{CUSTOMER_MESSAGE}"
                )
            ]
        )
        for token in CUSTOMER_TOKENS:
            self.assertNotIn(token, rows[0][4])
        self.assertEqual(
            rows[0][4],
            "The operation failed (ForwardClientError: http-403).",
        )

    def test_dict_rows_are_treated_the_same_as_tuple_rows(self):
        rows = diagnostics._sanitize_log_rows(
            [
                {
                    "level": "failure",
                    "message": "Forward merge failed (IntegrityError: shape-error).",
                }
            ]
        )
        self.assertEqual(
            rows[0]["message"],
            "The operation failed (IntegrityError: shape-error).",
        )

    def test_non_failure_rows_are_untouched(self):
        rows = diagnostics._sanitize_log_rows([self._row("all good", level="info")])
        self.assertEqual(rows[0][4], "all good")

    def test_the_message_no_longer_promises_a_record_that_is_never_written(self):
        # The plugin writes no `logger.exception` and passes `exc_info` nowhere,
        # so "use the job identifier and exception type for server-side
        # investigation" pointed at a log that never held the answer.
        self.assertNotIn("server-side", diagnostics.SAFE_FAILURE_LOG_MESSAGE)
        self.assertIn("The operation failed", diagnostics.SAFE_FAILURE_LOG_MESSAGE)


class PersistedJobErrorTest(unittest.TestCase):
    """Level 2: text survives to the database, and must survive readback."""

    def test_a_reason_bearing_job_error_is_not_read_back_as_unparseable(self):
        self.assertEqual(
            diagnostics.safe_job_error_summary(
                "Forward sync failed (ForwardQueryError: no-enabled-query-maps)."
            ),
            "Job failed (ForwardQueryError: no-enabled-query-maps).",
        )

    def test_a_bare_classifier_still_reads_back(self):
        self.assertEqual(
            diagnostics.safe_job_error_summary("Forward sync failed (ValueError)."),
            "Job failed (ValueError).",
        )

    def test_anything_else_is_still_redacted(self):
        self.assertEqual(
            diagnostics.safe_job_error_summary(CUSTOMER_MESSAGE),
            diagnostics.REDACTED_DIAGNOSTIC,
        )


class PerModelFailureEvidenceTest(unittest.TestCase):
    """Level B: which models failed, and why - not N copies of one sentence."""

    RESULTS = [
        {
            "model": "dcim.device",
            "failure_count": 1,
            "failure_exception": "ForwardQueryError",
            "failure_reason": "diff-required-no-baseline",
        },
        {
            "model": "dcim.interface",
            "failure_count": 1,
            "failure_exception": "ForwardQueryError",
            "failure_reason": "no-enabled-query-maps",
        },
        {"model": "ipam.prefix", "row_count": 12, "failure_count": 0},
    ]

    def test_only_failing_models_are_reported(self):
        summary = diagnostics.model_failure_summary(self.RESULTS)
        self.assertEqual(
            [item["model"] for item in summary],
            ["dcim.device", "dcim.interface"],
        )

    def test_each_failing_model_carries_a_non_empty_reason(self):
        for item in diagnostics.model_failure_summary(self.RESULTS):
            with self.subTest(model=item["model"]):
                self.assertTrue(item["reason"])
                self.assertTrue(item["exception"])

    def test_distinct_reasons_are_not_collapsed(self):
        reasons = {
            item["reason"] for item in diagnostics.model_failure_summary(self.RESULTS)
        }
        self.assertEqual(
            reasons,
            {"diff-required-no-baseline", "no-enabled-query-maps"},
        )

    def test_an_unsafe_token_is_redacted_rather_than_emitted(self):
        summary = diagnostics.model_failure_summary(
            [
                {
                    "model": "dc11-edge-01 leaked in as a model",
                    "failure_count": 1,
                    "failure_exception": "ForwardQueryError",
                    "failure_reason": "10.11.12.13 leaked in as a reason",
                }
            ]
        )
        self.assertEqual(summary[0]["model"], "redacted")
        self.assertEqual(summary[0]["reason"], "redacted")

    def test_empty_and_malformed_input_is_safe(self):
        self.assertEqual(diagnostics.model_failure_summary(None), [])
        self.assertEqual(diagnostics.model_failure_summary(["not a dict"]), [])


class StructuredDiagnosisTest(unittest.TestCase):
    """A raiser may attach safe facts; free text is still never persisted."""

    def test_a_supplied_safe_diagnosis_is_merged(self):
        exc = SyncError("No Forward changes were returned because 2 model(s) failed.")
        exc.safe_diagnosis = {
            "failed_models": ["dcim.device", "dcim.interface"],
            "failed_model_reasons": ["no-enabled-query-maps"],
        }
        diagnosis = diagnostics.structured_failure_diagnosis(exc)
        self.assertEqual(diagnosis["failed_models"], ["dcim.device", "dcim.interface"])
        self.assertEqual(diagnosis["exception_type"], "SyncError")

    def test_unsafe_supplied_values_are_dropped(self):
        exc = SyncError("boom")
        exc.safe_diagnosis = {
            "failed_models": ["dcim.device", "dc11-edge-01 is not a model string"],
            "note": "Device dc11-edge-01 at 10.11.12.13 failed",
        }
        diagnosis = diagnostics.structured_failure_diagnosis(exc)
        self.assertEqual(diagnosis["failed_models"], ["dcim.device"])
        self.assertNotIn("note", diagnosis)
        self.assertNotIn("dc11-edge-01", str(diagnosis))

    def test_a_supplied_diagnosis_cannot_overwrite_a_derived_one(self):
        exc = SyncError("boom")
        exc.safe_diagnosis = {"exception_type": "SomethingElse"}
        self.assertEqual(
            diagnostics.structured_failure_diagnosis(exc)["exception_type"],
            "SyncError",
        )

    def test_the_failure_message_names_the_failing_models(self):
        exc = SyncError("boom")
        exc.safe_diagnosis = {"failed_models": ["dcim.device", "dcim.interface"]}
        diagnosis = diagnostics.structured_failure_diagnosis(exc)
        message = diagnostics.describe_failure(
            diagnostics.safe_operation_failure("Forward ingestion", exc),
            diagnosis,
        )
        self.assertIn("dcim.device", message)
        self.assertIn("dcim.interface", message)
        self.assertIn("2 model(s)", message)

    def test_a_long_model_list_is_summarised_not_truncated_silently(self):
        models = [f"app.model{index}" for index in range(30)]
        exc = SyncError("boom")
        exc.safe_diagnosis = {"failed_models": models}
        message = diagnostics.describe_failure(
            diagnostics.safe_operation_failure("Forward ingestion", exc),
            diagnostics.structured_failure_diagnosis(exc),
        )
        self.assertIn("30 model(s)", message)
        self.assertIn("+22 more", message)

    def test_an_exception_without_a_supplied_diagnosis_is_unchanged(self):
        self.assertEqual(
            diagnostics.structured_failure_diagnosis(ValueError("boom")),
            {"exception_type": "ValueError"},
        )


class NoCustomerDataAnywhereTest(unittest.TestCase):
    """One sweep across every public entry point, with one hostile message."""

    def test_no_entry_point_emits_a_value_bearing_token(self):
        exc = ForwardClientError(CUSTOMER_MESSAGE)
        outputs = [
            diagnostics.safe_exception_summary(exc),
            diagnostics.safe_operation_failure("Forward sync", exc),
            diagnostics.failure_reason(exc),
            diagnostics.redacted_message_prefix(CUSTOMER_MESSAGE),
            diagnostics.safe_failure_log_message(CUSTOMER_MESSAGE),
            str(diagnostics.structured_failure_diagnosis(exc)),
            str(
                diagnostics._sanitize_log_rows(
                    [["t", "error", "", "", CUSTOMER_MESSAGE]]
                )
            ),
        ]
        for output in outputs:
            for token in CUSTOMER_TOKENS:
                with self.subTest(output=output, token=token):
                    self.assertNotIn(token, output)


if __name__ == "__main__":
    unittest.main()
