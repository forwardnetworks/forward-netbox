# A blocked row-count run used to say only that rows fell. When the cause is
# devices Forward did not collect in this snapshot, the run now says so first.
# Synthetic counts only.
from unittest.mock import Mock
from unittest.mock import patch

from django.test import SimpleTestCase
from django.test import TestCase

from forward_netbox.models import ForwardIngestion
from forward_netbox.models import ForwardSource
from forward_netbox.models import ForwardSync
from forward_netbox.utilities.snapshot_collection_health import scope_collection_counts
from forward_netbox.utilities.snapshot_collection_health import scope_collection_finding
from forward_netbox.utilities.snapshot_collection_health import scope_collection_reason
from forward_netbox.utilities.snapshot_collection_health import scope_query
from forward_netbox.utilities.snapshot_collection_health import (
    SNAPSHOT_COLLECTION_REASON_PREFIX,
)
from forward_netbox.utilities.snapshot_collection_health import summarize_results
from forward_netbox.utilities.validation import ForwardValidationRunner
from forward_netbox.utilities.validation import ROW_SHRINK_REASON_PREFIX
from forward_netbox.utilities.validation_guidance import guidance_for


def _rows(completed, **failures):
    rows = [{"completed": True, "reason": "DeviceSnapshotResult.completed"}] * completed
    for value, count in failures.items():
        rows += [
            {
                "completed": False,
                "reason": (
                    "DeviceSnapshotResult.collectionFailed("
                    f"DeviceCollectionError.{value})"
                ),
            }
        ] * count
    return rows


class SummarizeTest(SimpleTestCase):
    def test_counts_total_completed_and_failures_by_type(self):
        summary = summarize_results(
            _rows(7, AUTHENTICATION_FAILED=2, CONNECTION_TIMEOUT=1)
        )
        self.assertEqual(summary["total"], 10)
        self.assertEqual(summary["completed"], 7)
        self.assertEqual(summary["not_completed"], 3)
        self.assertEqual(
            summary["by_failure"],
            {
                "AUTHENTICATION_FAILED": 2,
                "CONNECTION_TIMEOUT": 1,
            },
        )

    def test_a_processing_failure_is_named_without_the_closing_bracket(self):
        rows = [
            {
                "completed": False,
                "reason": "DeviceSnapshotResult.processingFailed("
                "DeviceProcessingError.PARSER_EXCEPTION)",
            },
            {"completed": False, "reason": ""},
        ]
        self.assertEqual(
            summarize_results(rows)["by_failure"],
            {"PARSER_EXCEPTION": 1, "UNKNOWN": 1},
        )

    def test_empty_is_zero_not_unknown(self):
        self.assertEqual(summarize_results([])["total"], 0)


class QueryTest(SimpleTestCase):
    def test_scope_uses_the_syncs_own_tags(self):
        query = scope_query(["a", "b"], ["x"], "any")
        self.assertIn('"a" in device.tagNames', query)
        self.assertIn('"b" in device.tagNames', query)
        self.assertIn("||", query)
        self.assertIn('!("x" in device.tagNames)', query)

    def test_match_all_requires_every_tag(self):
        query = scope_query(["a", "b"], [], "all")
        self.assertNotIn("||", query)
        self.assertEqual(query.count('where "'), 2)

    def test_returns_every_device_with_its_result_rather_than_only_completed(self):
        # The point is to see the devices that did not complete.
        query = scope_query(["a"], [], "any")
        self.assertNotIn("\nwhere device.snapshotInfo", query)
        self.assertIn("toString(device.snapshotInfo.result)", query)


class FindingTest(SimpleTestCase):
    def summary(self, completed, **failures):
        return summarize_results(_rows(completed, **failures))

    def test_a_fall_in_completed_with_more_failures_is_a_finding(self):
        finding = scope_collection_finding(
            self.summary(2834, AUTHENTICATION_FAILED=600),
            self.summary(3400, AUTHENTICATION_FAILED=2),
        )
        self.assertEqual(finding["dropped_devices"], 566)
        self.assertEqual(finding["newly_failed_devices"], 598)
        reason = scope_collection_reason(finding)
        self.assertTrue(reason.startswith(SNAPSHOT_COLLECTION_REASON_PREFIX))
        self.assertIn("2834 of the 3434", reason)
        self.assertIn("600 AUTHENTICATION_FAILED", reason)

    def test_no_fall_is_silent(self):
        self.assertIsNone(
            scope_collection_finding(self.summary(3400), self.summary(3400))
        )

    def test_a_small_fall_is_not_a_snapshot_problem(self):
        self.assertIsNone(
            scope_collection_finding(
                self.summary(3397, CONNECTION_TIMEOUT=3), self.summary(3400)
            )
        )

    def test_a_retired_site_with_a_few_failures_is_not_a_snapshot_problem(self):
        # 400 devices left the scope while 3 failed: the fall is real, not collection.
        self.assertIsNone(
            scope_collection_finding(
                self.summary(2997, AUTHENTICATION_FAILED=3), self.summary(3400)
            )
        )

    def test_a_fall_with_no_new_failures_is_silent(self):
        # Devices left the scope; they did not fail collection.
        self.assertIsNone(
            scope_collection_finding(self.summary(3000), self.summary(3400))
        )

    def test_an_unknown_side_is_never_read_as_zero(self):
        ok = self.summary(3400)
        self.assertIsNone(scope_collection_finding({"error": "X"}, ok))
        self.assertIsNone(scope_collection_finding(ok, {"error": "X"}))
        self.assertIsNone(scope_collection_finding(None, ok))


class ProbeTest(SimpleTestCase):
    def test_a_failed_probe_is_an_error_not_zero(self):
        with patch(
            "forward_netbox.utilities.snapshot_collection_health.run_nqe_query",
            side_effect=RuntimeError("boom"),
        ):
            counts = scope_collection_counts(
                Mock(),
                network_id="n",
                snapshot_id="s",
                include_tags=["a"],
                exclude_tags=[],
                include_match="any",
            )
        self.assertEqual(counts, {"error": "RuntimeError"})

    def test_missing_snapshot_is_an_error(self):
        counts = scope_collection_counts(
            Mock(),
            network_id="n",
            snapshot_id="",
            include_tags=[],
            exclude_tags=[],
            include_match="any",
        )
        self.assertIn("error", counts)


class GuidanceTest(SimpleTestCase):
    def test_the_reason_has_a_next_step_that_warns_against_force_allow(self):
        text = guidance_for(f"{SNAPSHOT_COLLECTION_REASON_PREFIX} anything")
        self.assertIn("next snapshot", text)
        self.assertIn("Force-Allow", text)


def _shrink_model_results(rows):
    return [
        {
            "model": "dcim.module",
            "sync_mode": "full",
            "row_count": rows,
            "failure_count": 0,
            "delete_count": 0,
        }
    ]


class RunnerTest(TestCase):
    def setUp(self):
        source = ForwardSource.objects.create(
            name="scope-health-source",
            type="saas",
            url="https://fwd.app",
            parameters={
                "username": "u@example.com",
                "password": "secret",
                "verify": True,
                "network_id": "net-1",
            },
        )
        self.sync = ForwardSync.objects.create(
            name="scope-health-sync",
            source=source,
            parameters={"snapshot_id": "latestProcessed", "dcim.module": True},
        )
        ForwardIngestion.objects.create(
            sync=self.sync,
            baseline_ready=True,
            snapshot_id="snap-base",
            model_results=_shrink_model_results(1000),
        )
        self.context = {
            "network_id": "net-1",
            "snapshot_selector": "latestProcessed",
            "snapshot_id": "snap-now",
            "snapshot_info": {"state": "PROCESSED"},
            "snapshot_metrics": {},
            "device_tag_include_tags": ["a"],
            "device_tag_exclude_tags": [],
            "device_tag_include_match": "any",
        }

    def reasons(self, rows, counts):
        runner = ForwardValidationRunner(self.sync, Mock(), Mock())
        with patch(
            "forward_netbox.utilities.validation.scope_collection_counts",
            side_effect=counts,
        ) as probe:
            reasons = runner._blocking_reasons(
                self.context,
                plan=[],
                model_results=_shrink_model_results(rows),
                policy=None,
            )
        return runner, reasons, probe

    def counts(self, now, base):
        return [summarize_results(now), summarize_results(base)]

    def test_the_snapshot_cause_leads_a_shrink_block(self):
        runner, reasons, _ = self.reasons(
            400,
            self.counts(_rows(2834, AUTHENTICATION_FAILED=600), _rows(3400)),
        )
        self.assertEqual(len(reasons), 2)
        self.assertTrue(reasons[0].startswith(SNAPSHOT_COLLECTION_REASON_PREFIX))
        self.assertTrue(reasons[1].startswith(ROW_SHRINK_REASON_PREFIX))
        self.assertEqual(runner._scope_collection["current"]["completed"], 2834)

    def test_a_passing_run_makes_no_probe(self):
        _, reasons, probe = self.reasons(1000, [])
        self.assertEqual(reasons, [])
        probe.assert_not_called()

    def test_a_shrink_with_healthy_collection_reports_only_the_shrink(self):
        _, reasons, _ = self.reasons(400, self.counts(_rows(3400), _rows(3400)))
        self.assertEqual(len(reasons), 1)
        self.assertTrue(reasons[0].startswith(ROW_SHRINK_REASON_PREFIX))

    def test_a_probe_error_leaves_the_shrink_reasons_as_they_were(self):
        _, reasons, _ = self.reasons(
            400, [{"error": "RuntimeError"}, {"error": "RuntimeError"}]
        )
        self.assertEqual(len(reasons), 1)
        self.assertTrue(reasons[0].startswith(ROW_SHRINK_REASON_PREFIX))

    def test_the_counts_are_stored_on_the_run(self):
        runner = ForwardValidationRunner(self.sync, Mock(), Mock())
        with patch(
            "forward_netbox.utilities.validation.scope_collection_counts",
            side_effect=self.counts(
                _rows(2834, AUTHENTICATION_FAILED=600), _rows(3400)
            ),
        ):
            run = runner.record_plan_validation(
                self.context,
                [],
                _shrink_model_results(400),
                raise_on_block=False,
            )
        stored = run.snapshot_metrics["scope_collection"]
        self.assertEqual(stored["current"]["not_completed"], 600)
        self.assertEqual(stored["baseline"]["completed"], 3400)
