"""A redacted warning must not claim that an operation failed.

Warning bodies are redacted for the same reason failure bodies are: a query
name is customer-chosen and a warning can quote one. But the replacement
sentence asserted `The operation failed.`, which is a different claim from
`this text was redacted`.

A deployment's support bundle showed thirty of those seconds into a run, one per
model, each directing the reader to ingestion issues and per-model failure
evidence that were empty - because nothing had failed. They were the routine
preflight notice that a model cannot run a diff and *still syncs*. The run did
have a real defect, much later and somewhere else, and thirty invented failures
is what had to be read past to find it.

The redaction is not relaxed here. Only the claim is made to match the level.
"""

from django.test import SimpleTestCase

from forward_netbox.utilities.diagnostics import SAFE_FAILURE_LOG_MESSAGE
from forward_netbox.utilities.diagnostics import SAFE_FAILURE_LOG_PREFIX
from forward_netbox.utilities.diagnostics import SAFE_WARNING_LOG_MESSAGE
from forward_netbox.utilities.diagnostics import sanitize_job_diagnostics


class AWarningSaysItIsAWarningTest(SimpleTestCase):
    def _rows(self, level, message):
        data = {"logs": [["2026-08-19T15:01:00Z", level, "sync", "/url/", message]]}
        return sanitize_job_diagnostics(data)["logs"][0][4]

    def test_a_warning_without_a_classifier_does_not_claim_failure(self):
        rendered = self._rows(
            "warning",
            "Execution contract preflight found 1 map(s) for ipam.vlan that "
            "cannot run a diff; this model still syncs.",
        )
        self.assertEqual(rendered, SAFE_WARNING_LOG_MESSAGE)
        # Not merely a different sentence: it must not contain the phrase a
        # support engineer greps a bundle for, or it is counted as a failure
        # again by the search rather than by the reader.
        self.assertNotIn(SAFE_FAILURE_LOG_PREFIX, rendered)

    def test_an_error_without_a_classifier_still_claims_failure(self):
        rendered = self._rows("error", "something went wrong with 10.1.2.3")
        self.assertEqual(rendered, SAFE_FAILURE_LOG_MESSAGE)

    def test_the_warning_body_is_still_redacted(self):
        # The customer-named query must not survive, warning or not.
        rendered = self._rows(
            "warning", "Map `DC11 Core Interfaces` for sw01.example.net is stale."
        )
        for leaked in ("DC11", "sw01.example.net", "Core Interfaces"):
            self.assertNotIn(leaked, rendered)

    def test_a_classified_warning_keeps_its_classifier(self):
        rendered = self._rows(
            "warning", "Forward query spec resolution failed (ForwardQueryError)."
        )
        self.assertIn("ForwardQueryError", rendered)


class DictShapedRowsBehaveTheSameTest(SimpleTestCase):
    """Both row shapes reach the operator; both were making the same claim."""

    def _row(self, level, message):
        data = {"logs": [{"level": level, "message": message}]}
        return sanitize_job_diagnostics(data)["logs"][0]["message"]

    def test_a_dict_warning_does_not_claim_failure(self):
        self.assertEqual(
            self._row("warning", "a redacted notice about sw01"),
            SAFE_WARNING_LOG_MESSAGE,
        )

    def test_a_dict_error_still_claims_failure(self):
        self.assertEqual(
            self._row("error", "a redacted problem with sw01"),
            SAFE_FAILURE_LOG_MESSAGE,
        )
