# A blocked validation run used to leave its ingestion with nothing to click:
# the executor assigned `last_validation_run` from the return value, and a
# blocked run raises instead of returning. The ingestion issue also named only
# the line that raised, so an operator could not tell what to do.
from unittest.mock import Mock
from unittest.mock import patch

from django.contrib.auth import get_user_model
from django.test import SimpleTestCase
from django.test import TestCase

from forward_netbox.choices import ForwardValidationStatusChoices
from forward_netbox.exceptions import ForwardSyncError
from forward_netbox.models import ForwardDriftPolicy
from forward_netbox.models import ForwardIngestion
from forward_netbox.models import ForwardSource
from forward_netbox.models import ForwardSync
from forward_netbox.utilities.query_fetch import ForwardQueryFetcher
from forward_netbox.utilities.query_fetch_execution import ForwardQueryContext
from forward_netbox.utilities.single_branch_executor import ForwardSingleBranchExecutor
from forward_netbox.utilities.sync_orchestration import _with_validation_pointer
from forward_netbox.utilities.validation_guidance import guidance_for
from forward_netbox.utilities.validation_guidance import guidance_for_reasons


class BlockedRunIsHeldTest(TestCase):
    def setUp(self):
        self.user = get_user_model().objects.create_user(username="blocked-user")
        source = ForwardSource.objects.create(
            name="blocked-source",
            type="saas",
            url="https://fwd.app",
            parameters={
                "username": "u@example.com",
                "password": "secret",
                "verify": True,
                "network_id": "net-1",
            },
        )
        policy = ForwardDriftPolicy.objects.create(
            name="blocked-policy", enabled=True, require_processed_snapshot=True
        )
        self.sync = ForwardSync.objects.create(
            name="blocked-sync",
            source=source,
            user=self.user,
            drift_policy=policy,
            parameters={"snapshot_id": "latestProcessed"},
        )

    def _run_blocked_executor(self):
        context = ForwardQueryContext(
            network_id="net-1",
            snapshot_selector="latestProcessed",
            snapshot_id="snap-1",
            snapshot_info={"state": "UNPROCESSED"},
        )
        logger = Mock()
        logger.log_data = {}
        self.sync.logger = logger
        executor = ForwardSingleBranchExecutor(
            self.sync, Mock(), logger, user=self.user
        )
        with (
            patch.object(ForwardQueryFetcher, "resolve_context", return_value=context),
            patch.object(ForwardQueryFetcher, "fetch_workloads", return_value=[]),
        ):
            with self.assertRaises(ForwardSyncError):
                executor.run()
        return executor

    def test_the_executor_holds_the_run_that_blocked_it(self):
        executor = self._run_blocked_executor()
        run = executor.last_validation_run
        self.assertIsNotNone(run)
        run.refresh_from_db()
        self.assertEqual(run.status, ForwardValidationStatusChoices.BLOCKED)
        self.assertTrue(run.blocking_reasons)

    def test_the_failed_ingestion_links_the_blocked_run(self):
        from forward_netbox.utilities.sync_orchestration import (
            _build_forward_ingestion,
        )

        executor = self._run_blocked_executor()
        ingestion = _build_forward_ingestion(self.sync, None, executor)
        self.assertEqual(ingestion.validation_run_id, executor.last_validation_run.pk)

    def test_a_failure_that_is_not_a_block_marks_the_run_failed_not_running(self):
        context = ForwardQueryContext(
            network_id="net-1",
            snapshot_selector="latestProcessed",
            snapshot_id="snap-1",
            snapshot_info={"state": "PROCESSED"},
        )
        logger = Mock()
        logger.log_data = {}
        self.sync.logger = logger
        executor = ForwardSingleBranchExecutor(
            self.sync, Mock(), logger, user=self.user
        )
        from forward_netbox.utilities.validation import ForwardValidationRunner

        with (
            patch.object(ForwardQueryFetcher, "resolve_context", return_value=context),
            patch.object(ForwardQueryFetcher, "fetch_workloads", return_value=[]),
            patch.object(
                ForwardValidationRunner,
                "record_plan_validation",
                side_effect=RuntimeError("boom"),
            ),
        ):
            with self.assertRaises(RuntimeError):
                executor.run()
        run = executor.last_validation_run
        run.refresh_from_db()
        self.assertEqual(run.status, ForwardValidationStatusChoices.FAILED)

    def test_the_issue_message_points_at_the_blocked_run(self):
        executor = self._run_blocked_executor()
        ingestion = ForwardIngestion(validation_run=executor.last_validation_run)
        message = _with_validation_pointer("Forward ingestion failed.", ingestion)
        self.assertIn("open the validation run", message)

    def test_the_ingestion_summary_carries_the_blocking_sentences(self):
        from forward_netbox.utilities.ingestion_presentation import (
            get_analysis_summary,
        )

        executor = self._run_blocked_executor()
        ingestion = ForwardIngestion.objects.create(
            sync=self.sync, validation_run=executor.last_validation_run
        )
        summary = get_analysis_summary(ingestion)
        self.assertEqual(
            summary["validation_blocking_reasons"],
            executor.last_validation_run.blocking_reasons,
        )
        self.assertTrue(summary["validation_blocking_reasons"])

    def test_an_ingestion_without_a_blocked_run_gets_no_pointer(self):
        self.assertEqual(
            _with_validation_pointer("Forward ingestion failed.", ForwardIngestion()),
            "Forward ingestion failed.",
        )


class RealReasonsHaveNextStepsTest(TestCase):
    """Reasons produced by the validation code itself, not copied sentences."""

    def test_the_query_failure_reasons_validation_writes_have_a_next_step(self):
        from forward_netbox.models import ForwardValidationRun
        from forward_netbox.utilities.validation import ForwardValidationRunner

        source = ForwardSource.objects.create(
            name="guide-src",
            type="saas",
            url="https://fwd.app",
            parameters={
                "username": "u@example.com",
                "password": "p",
                "verify": True,
                "network_id": "n",
            },
        )
        policy = ForwardDriftPolicy.objects.create(
            name="guide-policy",
            enabled=True,
            require_processed_snapshot=True,
            block_on_query_errors=True,
        )
        sync = ForwardSync.objects.create(
            name="guide-sync",
            source=source,
            drift_policy=policy,
            parameters={"snapshot_id": "latestProcessed"},
        )
        runner = ForwardValidationRunner(sync, Mock(), Mock())
        results = [
            {"model": "dcim.device", "failure_count": 1, "row_count": 0},
            {"model": "dcim.devicetype", "failure_count": 1, "row_count": 0},
        ]
        context = {
            "snapshot_selector": "latestProcessed",
            "snapshot_id": "s",
            "snapshot_info": {"state": "UNPROCESSED"},
            "snapshot_metrics": {},
        }
        run = ForwardValidationRun.objects.create(sync=sync, policy=policy)
        reasons = runner._blocking_reasons(
            context, [], results, policy, validation_run=run
        )
        self.assertTrue(reasons)
        for reason in reasons:
            with self.subTest(reason=reason[:60]):
                # The diff-fallback advice carries its own instruction.
                if reason.startswith("These query failures blocked the sync"):
                    continue
                self.assertTrue(guidance_for(reason), reason)


class GuidanceTest(SimpleTestCase):
    def test_every_reason_validation_writes_has_a_next_step(self):
        from forward_netbox.utilities.validation import collection_collapse_reason
        from forward_netbox.utilities.validation import row_shrink_reason

        reasons = [
            "Foundational device metadata query failed before `dcim.device`: dcim.devicetype.",
            "`dcim.device` query failed while enabled child models depend on device coverage: dcim.interface.",
            "Target snapshot is not processed.",
            "3 query failures were reported.",
            "No rows were returned for enabled models: dcim.cable.",
            "Delete count 9 exceeds policy limit 5.",
            "Delete percentage exceeds policy limit 10%.",
            row_shrink_reason(
                {
                    "model": "dcim.device",
                    "current_rows": 1,
                    "baseline_rows": 10,
                    "dropped_rows": 9,
                    "dropped_percent": 90.0,
                },
                max_shrink_percent=50,
            ),
            collection_collapse_reason(
                {
                    "current_collected": 1,
                    "baseline_collected": 10,
                    "dropped_devices": 9,
                    "dropped_percent": 90.0,
                },
                max_drop_percent=50,
            ),
        ]
        for reason in reasons:
            with self.subTest(reason=reason[:50]):
                self.assertTrue(guidance_for(reason), reason)

    def test_a_failed_query_says_to_run_again_and_what_to_send_if_it_recurs(self):
        step = guidance_for(
            "Foundational device metadata query failed before `dcim.device`: dcim.devicetype."
        )
        self.assertIn("run the sync again", step)
        self.assertIn("support bundle", step)

    def test_duplicate_steps_are_listed_once_and_unknown_reasons_add_none(self):
        failed = "Foundational device metadata query failed before `dcim.device`: x."
        steps = guidance_for_reasons([failed, failed, "something unrecognised"])
        self.assertEqual(len(steps), 1)
