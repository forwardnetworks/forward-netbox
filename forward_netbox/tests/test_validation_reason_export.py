from types import SimpleNamespace

from django.test import SimpleTestCase

from forward_netbox.utilities.export_redaction import export_safe_payload
from forward_netbox.utilities.health_summary_blocks import blocking_reason_texts
from forward_netbox.utilities.health_summary_blocks import validation_summary

REASON = (
    "Row shrink: `dcim.device` returned 10 row(s), down from 100 in the last "
    "successful ingestion."
)


def _run(reasons):
    return SimpleNamespace(
        pk=7,
        status="blocked",
        allowed=False,
        snapshot_selector="latestProcessed",
        snapshot_id="snap-1",
        blocking_reasons=reasons,
        created=None,
        completed=None,
    )


class ValidationReasonExportTest(SimpleTestCase):
    def test_summary_carries_the_reason_sentences_not_only_their_count(self):
        summary = validation_summary(_run([REASON]))
        self.assertEqual(summary["blocking_reason_count"], 1)
        self.assertEqual(summary["blocking_reasons"], [REASON])

    def test_the_export_filter_keeps_the_sentences(self):
        exported = export_safe_payload(
            {"latest_validation": validation_summary(_run([REASON]))}
        )
        self.assertEqual(exported["latest_validation"]["blocking_reasons"], [REASON])

    def test_an_unblocked_run_has_no_reasons_and_the_list_is_capped(self):
        self.assertEqual(blocking_reason_texts(_run(None)), [])
        self.assertEqual(len(blocking_reason_texts(_run(["r"] * 50))), 20)
