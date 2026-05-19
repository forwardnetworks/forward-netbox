from datetime import timedelta
from threading import Barrier
from threading import Lock
from threading import Thread
from unittest.mock import ANY
from unittest.mock import Mock
from unittest.mock import patch
from uuid import uuid4

from core.choices import JobStatusChoices
from core.exceptions import SyncError
from core.models import Job
from django.contrib.contenttypes.models import ContentType
from django.db import close_old_connections
from django.test import TestCase
from django.test import TransactionTestCase
from django.utils import timezone
from netbox_branching.models import Branch

from forward_netbox.choices import ForwardExecutionStepKindChoices
from forward_netbox.choices import ForwardExecutionStepStatusChoices
from forward_netbox.choices import ForwardIngestionPhaseChoices
from forward_netbox.choices import ForwardValidationStatusChoices
from forward_netbox.models import ForwardExecutionRun
from forward_netbox.models import ForwardExecutionStep
from forward_netbox.models import ForwardIngestion
from forward_netbox.models import ForwardSource
from forward_netbox.models import ForwardSync
from forward_netbox.models import ForwardValidationRun
from forward_netbox.tests import scenarios
from forward_netbox.utilities.branch_budget import build_branch_plan
from forward_netbox.utilities.execution_ledger import active_execution_run
from forward_netbox.utilities.execution_ledger import claim_ingestion_merge_step
from forward_netbox.utilities.execution_ledger import claim_stage_step
from forward_netbox.utilities.execution_ledger import current_discardable_step
from forward_netbox.utilities.execution_ledger import current_mergeable_step
from forward_netbox.utilities.execution_ledger import current_retryable_step
from forward_netbox.utilities.execution_ledger import discard_stage_branch_for_retry
from forward_netbox.utilities.execution_ledger import execution_run_support_bundle
from forward_netbox.utilities.execution_ledger import mark_run_completed
from forward_netbox.utilities.execution_ledger import prepare_stage_step_retry
from forward_netbox.utilities.execution_ledger import reconcile_execution_run
from forward_netbox.utilities.forward_api import LATEST_PROCESSED_SNAPSHOT
from forward_netbox.utilities.job_compat import ensure_core_job_compat_defaults
from forward_netbox.utilities.multi_branch import BranchBudgetExceeded
from forward_netbox.utilities.multi_branch import DEFAULT_PREFLIGHT_ROW_LIMIT
from forward_netbox.utilities.multi_branch import ForwardMultiBranchExecutor
from forward_netbox.utilities.multi_branch import ForwardMultiBranchPlanner
from forward_netbox.utilities.query_registry import QuerySpec
from forward_netbox.utilities.sync import ForwardSyncRunner
from forward_netbox.utilities.sync_execution import ForwardExecutionBackendChoices
from forward_netbox.utilities.sync_state import STALE_BRANCH_PROGRESS_SECONDS


class SyntheticSyncScenarioHarnessTest(TestCase):
    def setUp(self):
        self.source = ForwardSource.objects.create(
            name="source-synthetic-scenarios",
            type="saas",
            url="https://fwd.app",
            parameters=scenarios.source_parameters(),
        )
        self.sync = ForwardSync.objects.create(
            name="sync-synthetic-scenarios",
            source=self.source,
            auto_merge=True,
            parameters={
                "snapshot_id": LATEST_PROCESSED_SNAPSHOT,
                "dcim.site": True,
            },
        )

    def _job(self, *, instance, status=JobStatusChoices.STATUS_PENDING, completed=None):
        ensure_core_job_compat_defaults()
        started = (
            completed - timedelta(seconds=5)
            if completed is not None
            else timezone.now()
        )
        return Job.objects.create(
            object_type=ContentType.objects.get_for_model(instance),
            object_id=instance.pk,
            name=f"synthetic {instance._meta.model_name} job",
            status=status,
            job_id=uuid4(),
            created=timezone.now(),
            started=started,
            completed=completed,
            data={},
        )

    def _execution_run(self, *, status="running", next_step_index=1):
        run = ForwardExecutionRun.objects.create(
            sync=self.sync,
            source=self.source,
            backend="branching",
            status=status,
            snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
            snapshot_id=scenarios.SNAPSHOT_AFTER,
            total_steps=1,
            next_step_index=next_step_index,
        )
        self.sync.set_branch_run_state(
            {
                "execution_run_id": run.pk,
                "snapshot_selector": LATEST_PROCESSED_SNAPSHOT,
                "snapshot_id": scenarios.SNAPSHOT_AFTER,
                "next_plan_index": next_step_index,
                "total_plan_items": 1,
            }
        )
        return run

    def test_large_interface_import_splits_without_live_data(self):
        workload = scenarios.branch_workload(
            "dcim.interface",
            scenarios.interface_rows(device_count=3, interfaces_per_device=4),
            coalesce_fields=[["device", "name"]],
        )

        plan = build_branch_plan([workload], max_changes_per_branch=5)

        self.assertEqual(sum(item.estimated_changes for item in plan), 12)
        self.assertGreater(len(plan), 1)
        self.assertTrue(all(item.estimated_changes <= 5 for item in plan))

    def test_run_rejects_when_waiting_for_merge(self):
        executor = ForwardMultiBranchExecutor(
            sync=self.sync,
            client=Mock(),
            logger_=Mock(),
        )
        executor.plan = Mock()
        run = self._execution_run(next_step_index=2)
        ForwardExecutionStep.objects.create(
            run=run,
            index=1,
            status=ForwardExecutionStepStatusChoices.STAGED,
            model_string="dcim.site",
            label="dcim.site staged shard",
        )

        with self.assertRaisesRegex(
            SyncError,
            "waiting for the current shard branch to be merged",
        ):
            executor.run(max_changes_per_branch=10)

        executor.plan.assert_not_called()

    @patch("forward_netbox.utilities.query_fetch_execution.get_query_specs")
    def test_bad_model_rows_are_isolated_during_preflight(self, mock_specs):
        client = Mock()
        client.get_snapshots.return_value = [scenarios.snapshot()]
        client.get_snapshot_metrics.return_value = {}
        client.run_nqe_query.return_value = scenarios.invalid_site_rows()
        self.sync.resolve_snapshot_id = lambda client=None: scenarios.SNAPSHOT_AFTER
        self.sync.get_model_strings = lambda: ["dcim.site"]
        self.sync.incremental_diff_baseline = Mock(return_value=None)
        mock_specs.return_value = [
            QuerySpec(
                model_string="dcim.site",
                query_name="Forward Sites",
                query='select {name: "site-without-slug"}',
            )
        ]
        planner = ForwardMultiBranchPlanner(
            sync=self.sync,
            client=client,
            logger_=Mock(),
        )

        _context, plan = planner.build_plan(
            max_changes_per_branch=10, run_preflight=True
        )

        client.run_nqe_query.assert_called_once()
        self.assertEqual(plan, [])
        self.assertEqual(planner.model_results[0]["model"], "dcim.site")
        self.assertEqual(planner.model_results[0]["failure_count"], 1)
        self.assertIn(
            "missing required fields: slug",
            planner.model_results[0]["diagnostics"][0]["message"],
        )
        self.assertEqual(
            client.run_nqe_query.call_args.kwargs["limit"],
            DEFAULT_PREFLIGHT_ROW_LIMIT,
        )

    @patch("forward_netbox.utilities.sync_execution.select_apply_engine")
    @patch("forward_netbox.utilities.sync_execution.get_query_specs")
    def test_diff_scenario_routes_upserts_and_deletes(
        self, mock_specs, mock_select_apply_engine
    ):
        baseline = ForwardIngestion.objects.create(
            sync=self.sync,
            snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
            snapshot_id=scenarios.SNAPSHOT_BEFORE,
            baseline_ready=True,
        )
        ingestion = ForwardIngestion.objects.create(sync=self.sync)
        client = Mock()
        client.get_latest_processed_snapshot.return_value = scenarios.snapshot(
            scenarios.SNAPSHOT_AFTER
        )
        client.get_snapshot_metrics.return_value = {}
        client.run_nqe_diff.return_value = scenarios.diff_rows()
        mock_specs.return_value = [
            QuerySpec(
                model_string="dcim.site",
                query_name="Forward Sites",
                query_id="Q_sites",
            )
        ]
        runner = ForwardSyncRunner(
            sync=self.sync,
            ingestion=ingestion,
            client=client,
            logger_=Mock(),
        )
        mock_select_apply_engine.return_value = Mock(
            apply_upserts=lambda runner, model_string, rows: runner._apply_model_rows(
                model_string, rows
            ),
            apply_deletes=lambda runner, model_string, rows: runner._delete_model_rows(
                model_string, rows
            ),
        )
        runner._apply_model_rows = Mock()
        runner._delete_model_rows = Mock()
        self.sync.get_model_strings = lambda: ["dcim.site"]
        self.sync.resolve_snapshot_id = lambda client=None: scenarios.SNAPSHOT_AFTER

        runner.run()

        client.run_nqe_diff.assert_called_once_with(
            query_id="Q_sites",
            commit_id=None,
            parameters={},
            before_snapshot_id=baseline.snapshot_id,
            after_snapshot_id=scenarios.SNAPSHOT_AFTER,
            fetch_all=True,
        )
        runner._apply_model_rows.assert_called_once_with(
            "dcim.site",
            [
                {"name": "site-added", "slug": "site-added"},
                {"name": "site-new", "slug": "site-modified"},
            ],
        )
        runner._delete_model_rows.assert_called_once_with(
            "dcim.site",
            [{"name": "site-deleted", "slug": "site-deleted"}],
        )

    def test_branch_overflow_scenario_splits_and_retries(self):
        workload = scenarios.branch_workload(
            "dcim.device",
            [{"name": f"device-{index}"} for index in range(8)],
            coalesce_fields=[["name"]],
        )
        oversized_item = build_branch_plan([workload], max_changes_per_branch=10)[0]
        split_items = build_branch_plan([workload], max_changes_per_branch=4)
        executor = ForwardMultiBranchExecutor(
            sync=self.sync,
            client=Mock(),
            logger_=Mock(),
        )
        executor.plan = Mock(
            return_value=(
                {
                    "snapshot_selector": LATEST_PROCESSED_SNAPSHOT,
                    "snapshot_id": scenarios.SNAPSHOT_AFTER,
                    "snapshot_info": {},
                    "snapshot_metrics": {},
                },
                [oversized_item],
            )
        )
        executor._record_model_density = Mock()
        executor._cleanup_overflow_branch = Mock()
        executor._split_overflow_item = Mock(return_value=split_items)
        executor._run_plan_item = Mock(
            side_effect=[
                BranchBudgetExceeded(
                    item=oversized_item,
                    actual_changes=25,
                    budget=10,
                    branch=None,
                    ingestion=None,
                ),
                Mock(name="ingestion-1"),
                Mock(name="ingestion-2"),
            ]
        )

        ingestions = executor.run(max_changes_per_branch=10)

        self.assertEqual(len(ingestions), 2)
        self.assertEqual(executor._run_plan_item.call_count, 3)
        executor._split_overflow_item.assert_called_once_with(oversized_item)

    def test_cleanup_overflow_branch_detaches_ingestion_and_deletes_branch(self):
        executor = ForwardMultiBranchExecutor(
            sync=self.sync,
            client=Mock(),
            logger_=Mock(),
        )
        ingestion = Mock()
        ingestion.branch = Mock(name="branch")
        ingestion.issues = Mock()
        branch = Mock()
        exc = BranchBudgetExceeded(
            item=Mock(),
            actual_changes=25,
            budget=10,
            branch=branch,
            ingestion=ingestion,
        )

        executor._cleanup_overflow_branch(exc)

        ingestion.issues.create.assert_called_once()
        self.assertIsNone(ingestion.branch)
        ingestion.save.assert_called_once_with(update_fields=["branch"])
        branch.delete.assert_called_once()

    def test_resume_skips_preflight_and_reuses_validation_run(self):
        workload = scenarios.branch_workload(
            "dcim.device",
            [{"name": f"device-{index}"} for index in range(8)],
            coalesce_fields=[["name"]],
        )
        plan = build_branch_plan([workload], max_changes_per_branch=4)
        validation_run = ForwardValidationRun.objects.create(
            sync=self.sync,
            status=ForwardValidationStatusChoices.PASSED,
            allowed=True,
            snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
            snapshot_id=scenarios.SNAPSHOT_AFTER,
        )
        executor = ForwardMultiBranchExecutor(
            sync=self.sync,
            client=Mock(),
            logger_=Mock(),
        )
        executor.plan = Mock(
            return_value=(
                {
                    "snapshot_selector": LATEST_PROCESSED_SNAPSHOT,
                    "snapshot_id": scenarios.SNAPSHOT_AFTER,
                    "snapshot_info": {},
                    "snapshot_metrics": {},
                },
                plan,
            )
        )
        executor._run_plan_item = Mock(return_value=Mock(name="ingestion"))
        run = ForwardExecutionRun.objects.create(
            sync=self.sync,
            source=self.source,
            backend="branching",
            status="running",
            snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
            snapshot_id=scenarios.SNAPSHOT_AFTER,
            total_steps=len(plan),
            next_step_index=2,
            validation_run=validation_run,
        )

        ingestions = executor.run(max_changes_per_branch=10)

        self.assertEqual(len(ingestions), 1)
        executor.plan.assert_called_once_with(
            max_changes_per_branch=10,
            run_preflight=False,
            model_change_density={},
            model_strings=None,
            shard_scope=None,
            branch_run_state=ANY,
        )
        self.assertEqual(executor.last_validation_run, validation_run)
        executor._run_plan_item.assert_called_once()
        self.assertEqual(executor._run_plan_item.call_args.args[0].index, 2)
        self.assertEqual(run.pk, active_execution_run(self.sync).pk)

    def test_merge_timeout_reconcile_records_support_bundle_detail(self):
        ingestion = ForwardIngestion.objects.create(
            sync=self.sync,
            snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
            snapshot_id=scenarios.SNAPSHOT_AFTER,
        )
        merge_job = self._job(
            instance=ingestion,
            status=JobStatusChoices.STATUS_ERRORED,
            completed=timezone.now(),
        )
        run = self._execution_run()
        step = ForwardExecutionStep.objects.create(
            run=run,
            index=1,
            status=ForwardExecutionStepStatusChoices.MERGE_QUEUED,
            model_string="dcim.site",
            label="dcim.site synthetic shard",
            ingestion=ingestion,
            merge_job=merge_job,
        )

        result = reconcile_execution_run(run)

        step.refresh_from_db()
        self.assertEqual(result["updated_steps"], 1)
        self.assertEqual(
            step.status,
            ForwardExecutionStepStatusChoices.MERGE_TIMEOUT,
        )
        self.assertIn(str(merge_job.pk), step.last_error)
        bundle = execution_run_support_bundle(run)
        self.assertEqual(bundle["steps"][0]["status"], "merge_timeout")
        self.assertEqual(bundle["steps"][0]["merge_job_detail"]["pk"], merge_job.pk)
        self.assertEqual(
            bundle["run"]["reconciliation_events"][0]["reason"],
            "associated_job_errored",
        )
        self.assertEqual(
            bundle["run"]["reconciliation_events"][0]["old_status"],
            "merge_queued",
        )
        self.assertEqual(
            bundle["run"]["reconciliation_events"][0]["new_status"],
            "merge_timeout",
        )
        self.assertEqual(
            bundle["recovery_recommendation"]["action"],
            "retry_current_step",
        )
        self.assertEqual(
            bundle["recovery_recommendation"]["step_index"],
            1,
        )
        self.assertEqual(bundle["metrics"]["step_count"], 1)
        self.assertEqual(bundle["metrics"]["retry_count"], 0)

    def test_stale_running_stage_without_branch_reconciles_to_retryable_step(self):
        run = self._execution_run()
        stale_started = timezone.now() - timedelta(
            seconds=STALE_BRANCH_PROGRESS_SECONDS + 5
        )
        step = ForwardExecutionStep.objects.create(
            run=run,
            index=1,
            kind=ForwardExecutionStepKindChoices.STAGE,
            status=ForwardExecutionStepStatusChoices.RUNNING,
            model_string="dcim.site",
            label="dcim.site stale shard",
            query_name="Forward Sites",
            execution_mode="query_id",
            execution_value="query-sites",
            started=stale_started,
            heartbeat=stale_started,
        )

        result = reconcile_execution_run(run)

        step.refresh_from_db()
        run.refresh_from_db()
        bundle = execution_run_support_bundle(run)

        self.assertEqual(result["updated_steps"], 1)
        self.assertEqual(
            step.status,
            ForwardExecutionStepStatusChoices.FAILED,
        )
        self.assertIn("stale", step.last_error)
        self.assertEqual(bundle["steps"][0]["status"], "failed")
        self.assertEqual(
            bundle["recovery_recommendation"]["action"],
            "retry_current_step",
        )
        self.assertEqual(
            bundle["recovery_recommendation"]["step_index"],
            1,
        )
        self.assertEqual(
            run.reconciliation_events[0]["reason"],
            "stale_stage_without_branch",
        )
        self.assertEqual(
            bundle["metrics"]["steps"][0]["status"],
            "failed",
        )

    def test_reconcile_stale_running_stage_without_branch_marks_retryable(self):
        run = self._execution_run()
        stale_started = timezone.now() - timedelta(
            seconds=STALE_BRANCH_PROGRESS_SECONDS + 5
        )
        step = ForwardExecutionStep.objects.create(
            run=run,
            index=1,
            kind=ForwardExecutionStepKindChoices.STAGE,
            status=ForwardExecutionStepStatusChoices.RUNNING,
            model_string="dcim.site",
            label="dcim.site stale shard",
            query_name="Forward Sites",
            execution_mode="query_id",
            execution_value="query-sites",
            started=stale_started,
            heartbeat=stale_started,
        )

        result = reconcile_execution_run(run)

        step.refresh_from_db()
        bundle = execution_run_support_bundle(run)

        self.assertEqual(result["updated_steps"], 1)
        self.assertEqual(
            step.status,
            ForwardExecutionStepStatusChoices.FAILED,
        )
        self.assertIn("heartbeat is stale", step.last_error)
        self.assertEqual(
            run.reconciliation_events[0]["reason"],
            "stale_stage_without_branch",
        )
        self.assertEqual(
            current_retryable_step(run),
            step,
        )
        self.assertEqual(
            bundle["recovery_recommendation"]["action"],
            "retry_current_step",
        )
        self.assertEqual(
            bundle["recovery_recommendation"]["step_index"],
            1,
        )
        self.assertEqual(bundle["steps"][0]["status"], "failed")

    def test_hard_kill_after_branch_creation_reconciles_to_discardable_step(self):
        branch = Branch.objects.create(
            name="synthetic-stale-branch-stage",
            schema_id=f"synthetic_stage_{uuid4().hex[:12]}",
        )
        ingestion = ForwardIngestion.objects.create(sync=self.sync, branch=branch)
        job = self._job(instance=self.sync, status=JobStatusChoices.STATUS_RUNNING)
        stale_started = timezone.now() - timedelta(
            seconds=STALE_BRANCH_PROGRESS_SECONDS + 5
        )
        run = self._execution_run()
        step = ForwardExecutionStep.objects.create(
            run=run,
            index=1,
            kind=ForwardExecutionStepKindChoices.STAGE,
            status=ForwardExecutionStepStatusChoices.RUNNING,
            model_string="dcim.site",
            label="dcim.site stale branch shard",
            ingestion=ingestion,
            branch=branch,
            job=job,
            started=stale_started,
            heartbeat=stale_started,
        )

        result = reconcile_execution_run(run)

        step.refresh_from_db()
        bundle = execution_run_support_bundle(run)
        self.assertEqual(result["updated_steps"], 1)
        self.assertEqual(step.status, ForwardExecutionStepStatusChoices.FAILED)
        self.assertEqual(step.branch_id, branch.pk)
        self.assertEqual(current_discardable_step(run), step)
        self.assertEqual(
            run.reconciliation_events[0]["reason"],
            "stale_stage_with_branch",
        )
        self.assertEqual(
            bundle["recovery_recommendation"]["action"],
            "discard_branch_retry",
        )

    def test_hard_kill_during_merge_reconciles_to_requeueable_merge(self):
        branch = Branch.objects.create(
            name="synthetic-stale-merge-branch",
            schema_id=f"synthetic_merge_{uuid4().hex[:12]}",
        )
        ingestion = ForwardIngestion.objects.create(sync=self.sync, branch=branch)
        merge_job = self._job(
            instance=ingestion,
            status=JobStatusChoices.STATUS_RUNNING,
        )
        ingestion.merge_job = merge_job
        ingestion.save(update_fields=["merge_job"])
        stale_started = timezone.now() - timedelta(
            seconds=STALE_BRANCH_PROGRESS_SECONDS + 5
        )
        merge_job.started = stale_started
        merge_job.save(update_fields=["started"])
        run = self._execution_run()
        step = ForwardExecutionStep.objects.create(
            run=run,
            index=1,
            kind=ForwardExecutionStepKindChoices.STAGE,
            status=ForwardExecutionStepStatusChoices.MERGE_QUEUED,
            model_string="dcim.site",
            label="dcim.site stale merge shard",
            ingestion=ingestion,
            branch=branch,
            merge_job=merge_job,
            heartbeat=stale_started,
        )

        result = reconcile_execution_run(run)

        step.refresh_from_db()
        bundle = execution_run_support_bundle(run)
        self.assertEqual(result["updated_steps"], 1)
        self.assertEqual(step.status, ForwardExecutionStepStatusChoices.MERGE_TIMEOUT)
        self.assertEqual(current_mergeable_step(run), step)
        self.assertTrue(ingestion.can_queue_merge)
        self.assertEqual(
            run.reconciliation_events[0]["reason"],
            "stale_merge_job",
        )
        self.assertEqual(
            bundle["recovery_recommendation"]["action"],
            "requeue_merge",
        )

        requeue_job = self._job(instance=ingestion)
        self.assertTrue(claim_ingestion_merge_step(ingestion, requeue_job))
        step.refresh_from_db()
        self.assertEqual(step.status, ForwardExecutionStepStatusChoices.MERGE_QUEUED)
        self.assertEqual(step.merge_job_id, requeue_job.pk)

    def test_reconcile_queued_step_with_applied_rows_and_no_branch_marks_merged(self):
        ingestion = ForwardIngestion.objects.create(sync=self.sync, branch=None)
        run = self._execution_run()
        step = ForwardExecutionStep.objects.create(
            run=run,
            index=1,
            kind=ForwardExecutionStepKindChoices.STAGE,
            status=ForwardExecutionStepStatusChoices.QUEUED,
            model_string="dcim.devicetype",
            label="dcim.devicetype stale queued shard",
            ingestion=ingestion,
            attempted_row_count=138,
            applied_row_count=138,
            failed_row_count=0,
            skipped_row_count=0,
        )

        result = reconcile_execution_run(run)

        step.refresh_from_db()
        self.assertEqual(result["updated_steps"], 1)
        self.assertEqual(step.status, ForwardExecutionStepStatusChoices.MERGED)
        self.assertEqual(
            run.reconciliation_events[0]["reason"],
            "queued_step_applied_without_merge_path",
        )

    def test_reconcile_stage_job_completed_without_ingestion_resets_to_pending(self):
        run = self._execution_run()
        stage_job = self._job(
            instance=self.sync,
            status=JobStatusChoices.STATUS_COMPLETED,
            completed=timezone.now(),
        )
        step = ForwardExecutionStep.objects.create(
            run=run,
            index=1,
            kind=ForwardExecutionStepKindChoices.STAGE,
            status=ForwardExecutionStepStatusChoices.QUEUED,
            model_string="dcim.devicetype",
            label="dcim.devicetype orphan queued shard",
            job=stage_job,
        )

        result = reconcile_execution_run(run)

        step.refresh_from_db()
        self.assertEqual(result["updated_steps"], 1)
        self.assertEqual(step.status, ForwardExecutionStepStatusChoices.PENDING)
        self.assertIsNone(step.job_id)
        self.assertEqual(
            run.reconciliation_events[0]["reason"],
            "stage_job_completed_without_ingestion",
        )

    def test_reconcile_duplicate_inflight_steps_keeps_single_inflight(self):
        run = self._execution_run(next_step_index=10)
        merged = ForwardExecutionStep.objects.create(
            run=run,
            index=11,
            kind=ForwardExecutionStepKindChoices.STAGE,
            status=ForwardExecutionStepStatusChoices.MERGED,
            model_string="ipam.vlan",
        )
        queued_10 = ForwardExecutionStep.objects.create(
            run=run,
            index=10,
            kind=ForwardExecutionStepKindChoices.STAGE,
            status=ForwardExecutionStepStatusChoices.QUEUED,
            model_string="ipam.vlan",
        )
        queued_12 = ForwardExecutionStep.objects.create(
            run=run,
            index=12,
            kind=ForwardExecutionStepKindChoices.STAGE,
            status=ForwardExecutionStepStatusChoices.QUEUED,
            model_string="ipam.vlan",
        )

        result = reconcile_execution_run(run)

        run.refresh_from_db()
        queued_10.refresh_from_db()
        queued_12.refresh_from_db()
        merged.refresh_from_db()
        self.assertGreaterEqual(result["updated_steps"], 1)
        self.assertEqual(run.next_step_index, 12)
        self.assertEqual(queued_12.status, ForwardExecutionStepStatusChoices.QUEUED)
        self.assertEqual(queued_10.status, ForwardExecutionStepStatusChoices.PENDING)
        reasons = [event.get("reason") for event in run.reconciliation_events]
        self.assertIn("duplicate_inflight_step", reasons)

    def test_reconcile_clears_stale_pending_job_binding(self):
        run = self._execution_run()
        stale_job = self._job(
            instance=self.sync, status=JobStatusChoices.STATUS_COMPLETED
        )
        step = ForwardExecutionStep.objects.create(
            run=run,
            index=1,
            kind=ForwardExecutionStepKindChoices.STAGE,
            status=ForwardExecutionStepStatusChoices.PENDING,
            model_string="dcim.site",
            label="dcim.site pending shard",
            job=stale_job,
        )

        result = reconcile_execution_run(run)

        step.refresh_from_db()
        self.assertGreaterEqual(result["updated_steps"], 1)
        self.assertIsNone(step.job_id)
        reasons = [event.get("reason") for event in run.reconciliation_events]
        self.assertIn("cleared_stale_pending_job_binding", reasons)

    def test_duplicate_stage_job_cannot_reclaim_terminal_step(self):
        run = self._execution_run()
        ForwardExecutionStep.objects.create(
            run=run,
            index=1,
            status=ForwardExecutionStepStatusChoices.MERGED,
            model_string="dcim.site",
            label="dcim.site synthetic shard",
            estimated_changes=12,
            actual_changes=10,
            fetched_row_count=12,
            query_runtime_ms=123.4,
            attempted_row_count=11,
            applied_row_count=8,
            skipped_row_count=2,
            failed_row_count=1,
            retry_count=1,
            fetch_mode="model",
            apply_engine="adapter",
        )
        duplicate_job = self._job(instance=self.sync)

        claimed = claim_stage_step(self.sync, 1, duplicate_job)

        self.assertIsNone(claimed)
        bundle = execution_run_support_bundle(run)
        self.assertEqual(bundle["steps"][0]["status"], "merged")
        self.assertEqual(bundle["steps"][0]["actual_changes"], 10)
        self.assertEqual(bundle["steps"][0]["fetch_mode"], "model")
        self.assertIn(
            "persisted shard locally", bundle["steps"][0]["fetch_explanation"]
        )
        self.assertEqual(bundle["steps"][0]["apply_engine"], "adapter")
        self.assertEqual(
            bundle["steps"][0]["apply_engine_decision"]["reason_code"],
            "bulk_orm_disabled_by_default",
        )
        self.assertEqual(bundle["metrics"]["estimated_changes"], 12)
        self.assertEqual(bundle["metrics"]["actual_changes"], 10)
        self.assertEqual(bundle["metrics"]["fetched_row_count"], 12)
        self.assertEqual(bundle["metrics"]["query_runtime_ms"], 123.4)
        self.assertEqual(bundle["metrics"]["attempted_row_count"], 11)
        self.assertEqual(bundle["metrics"]["applied_row_count"], 8)
        self.assertEqual(bundle["metrics"]["skipped_row_count"], 2)
        self.assertEqual(bundle["metrics"]["failed_row_count"], 1)
        self.assertEqual(bundle["metrics"]["retry_count"], 1)
        self.assertEqual(bundle["metrics"]["fetch_modes"], ["model"])
        self.assertEqual(bundle["metrics"]["apply_engines"], ["adapter"])
        self.assertEqual(
            bundle["metrics"]["steps"][0]["apply_engine_decision"]["selected_engine"],
            "adapter",
        )
        self.assertEqual(bundle["metrics"]["steps"][0]["fetched_row_count"], 12)
        self.assertEqual(bundle["metrics"]["steps"][0]["query_runtime_ms"], 123.4)
        self.assertIn(
            "persisted shard locally",
            bundle["metrics"]["steps"][0]["fetch_explanation"],
        )
        self.assertEqual(bundle["metrics"]["steps"][0]["attempted_row_count"], 11)
        self.assertEqual(bundle["metrics"]["steps"][0]["applied_row_count"], 8)
        self.assertEqual(bundle["metrics"]["steps"][0]["skipped_row_count"], 2)
        self.assertEqual(bundle["metrics"]["steps"][0]["failed_row_count"], 1)
        self.assertEqual(bundle["metrics"]["bottleneck"]["phase"], "forward_query")

    def test_duplicate_stage_job_cannot_reclaim_running_step(self):
        run = self._execution_run()
        original_job = self._job(instance=self.sync)
        step = ForwardExecutionStep.objects.create(
            run=run,
            index=1,
            status=ForwardExecutionStepStatusChoices.QUEUED,
            model_string="dcim.site",
            label="dcim.site synthetic shard",
            job=original_job,
        )
        first_claim = claim_stage_step(self.sync, 1, original_job)
        duplicate_job = self._job(instance=self.sync)

        second_claim = claim_stage_step(self.sync, 1, duplicate_job)

        step.refresh_from_db()
        self.assertEqual(first_claim.pk, step.pk)
        self.assertIsNone(second_claim)
        self.assertEqual(step.status, ForwardExecutionStepStatusChoices.RUNNING)
        self.assertEqual(step.job_id, original_job.pk)

    def test_claim_stage_step_blocks_when_another_index_is_running(self):
        run = self._execution_run(next_step_index=11)
        running_job = self._job(instance=self.sync)
        ForwardExecutionStep.objects.create(
            run=run,
            index=11,
            status=ForwardExecutionStepStatusChoices.RUNNING,
            model_string="ipam.vlan",
            job=running_job,
        )
        step_12 = ForwardExecutionStep.objects.create(
            run=run,
            index=12,
            status=ForwardExecutionStepStatusChoices.QUEUED,
            model_string="ipam.vlan",
        )
        new_job = self._job(instance=self.sync)

        claimed = claim_stage_step(self.sync, 12, new_job)

        step_12.refresh_from_db()
        self.assertIsNone(claimed)
        self.assertEqual(step_12.status, ForwardExecutionStepStatusChoices.QUEUED)

    def test_support_bundle_keeps_branch_evidence_after_cleanup(self):
        branch = Branch.objects.create(
            name="synthetic-cleaned-branch",
            schema_id=f"synthetic_clean_{uuid4().hex[:12]}",
        )
        ingestion = ForwardIngestion.objects.create(
            sync=self.sync,
            snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
            snapshot_id=scenarios.SNAPSHOT_AFTER,
            branch=branch,
        )
        run = self._execution_run(status="completed")
        ForwardExecutionStep.objects.create(
            run=run,
            index=1,
            status=ForwardExecutionStepStatusChoices.MERGED,
            model_string="dcim.site",
            label="dcim.site synthetic shard",
            ingestion=ingestion,
            branch=branch,
            branch_name=branch.name,
        )
        self.sync.clear_branch_run_state()
        branch.delete()

        bundle = execution_run_support_bundle(run)

        self.assertEqual(bundle["steps"][0]["branch"], None)
        self.assertEqual(bundle["steps"][0]["branch_name"], "synthetic-cleaned-branch")
        self.assertEqual(bundle["steps"][0]["ingestion"], ingestion.pk)
        self.assertEqual(bundle["recovery_recommendation"]["action"], "complete")

    def test_support_bundle_survives_upgrade_cleanup_after_old_branch_state(self):
        branch = Branch.objects.create(
            name="synthetic-upgrade-cleanup-branch",
            schema_id=f"synthetic_upgrade_{uuid4().hex[:12]}",
        )
        run = ForwardExecutionRun.objects.create(
            sync=self.sync,
            source=self.source,
            backend=ForwardExecutionBackendChoices.BRANCHING,
            status="running",
            snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
            snapshot_id=scenarios.SNAPSHOT_AFTER,
            total_steps=1,
            next_step_index=1,
            phase="executing",
            phase_message="Applying planned shard changes.",
        )
        ForwardExecutionStep.objects.create(
            run=run,
            index=1,
            model_string="dcim.site",
            label="dcim.site upgrade shard",
            estimated_changes=1,
            status=ForwardExecutionStepStatusChoices.MERGED,
            sync_mode="diff",
            query_name="Forward Sites",
            execution_mode="query_id",
            execution_value="query-site",
            baseline_snapshot_id="snapshot-before",
            apply_engine="adapter",
        )
        ingestion = ForwardIngestion.objects.create(
            sync=self.sync,
            snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
            snapshot_id=scenarios.SNAPSHOT_AFTER,
        )
        step = run.steps.first()
        step.ingestion = ingestion
        step.branch_name = branch.name
        step.save(update_fields=["ingestion", "branch_name"])
        self.sync.clear_branch_run_state()
        branch.delete()
        run = mark_run_completed(self.sync, baseline_ready=True)

        bundle = execution_run_support_bundle(run)

        self.assertEqual(bundle["run"]["id"], run.pk)
        self.assertEqual(bundle["steps"][0]["branch"], None)
        self.assertEqual(
            bundle["steps"][0]["branch_name"], "synthetic-upgrade-cleanup-branch"
        )
        self.assertEqual(bundle["steps"][0]["ingestion"], ingestion.pk)
        self.assertEqual(bundle["recovery_recommendation"]["action"], "complete")

    def test_execution_run_bundle_remains_actionable_after_later_run_starts(self):
        old_branch = Branch.objects.create(
            name="synthetic-old-run-branch",
            schema_id=f"synthetic_old_{uuid4().hex[:12]}",
        )
        old_ingestion = ForwardIngestion.objects.create(
            sync=self.sync,
            snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
            snapshot_id=scenarios.SNAPSHOT_AFTER,
            branch=old_branch,
            applied_change_count=7,
            failed_change_count=1,
        )
        old_ingestion.issues.create(
            phase=ForwardIngestionPhaseChoices.SYNC,
            model="ipam.ipaddress",
            message="Skipped invalid row during synthetic test.",
            raw_data={"address": "not exported"},
            defaults={"description": "not exported"},
            exception="ValidationError: invalid address",
        )
        old_run = self._execution_run(status="completed")
        ForwardExecutionStep.objects.create(
            run=old_run,
            index=1,
            status=ForwardExecutionStepStatusChoices.MERGED,
            model_string="dcim.site",
            label="dcim.site completed shard",
            ingestion=old_ingestion,
            branch=old_branch,
            branch_name=old_branch.name,
            actual_changes=7,
        )
        old_branch.delete()
        new_run = ForwardExecutionRun.objects.create(
            sync=self.sync,
            source=self.source,
            backend="branching",
            status="running",
            snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
            snapshot_id="snapshot-later",
            total_steps=1,
            next_step_index=1,
        )
        self.sync.set_branch_run_state(
            {
                "execution_run_id": new_run.pk,
                "snapshot_selector": LATEST_PROCESSED_SNAPSHOT,
                "snapshot_id": "snapshot-later",
                "next_plan_index": 1,
                "total_plan_items": 1,
            }
        )

        old_bundle = execution_run_support_bundle(old_run)

        self.assertEqual(old_bundle["run"]["id"], old_run.pk)
        self.assertEqual(old_bundle["steps"][0]["branch"], None)
        self.assertEqual(
            old_bundle["steps"][0]["branch_name"], "synthetic-old-run-branch"
        )
        self.assertEqual(old_bundle["steps"][0]["ingestion"], old_ingestion.pk)
        self.assertEqual(old_bundle["steps"][0]["actual_changes"], 7)
        ingestion_detail = old_bundle["steps"][0]["ingestion_detail"]
        self.assertEqual(ingestion_detail["id"], old_ingestion.pk)
        self.assertEqual(ingestion_detail["applied_change_count"], 7)
        self.assertEqual(ingestion_detail["failed_change_count"], 1)
        self.assertEqual(ingestion_detail["issue_count"], 1)
        self.assertEqual(ingestion_detail["issues"][0]["model"], "ipam.ipaddress")
        self.assertEqual(
            ingestion_detail["issues"][0]["message"],
            "Skipped invalid row during synthetic test.",
        )
        self.assertNotIn("raw_data", ingestion_detail["issues"][0])
        self.assertNotIn("defaults", ingestion_detail["issues"][0])
        self.assertEqual(old_bundle["recovery_recommendation"]["action"], "complete")

    def test_support_bundle_includes_sanitized_model_issue_samples(self):
        ingestion = ForwardIngestion.objects.create(
            sync=self.sync,
            snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
            snapshot_id=scenarios.SNAPSHOT_AFTER,
            failed_change_count=6,
        )
        issue_models = [
            "dcim.cable",
            "dcim.module",
            "dcim.virtualchassis",
            "ipam.ipaddress",
            "ipam.prefix",
            "netbox_routing.bgprouter",
        ]
        for model_string in issue_models:
            ingestion.issues.create(
                phase=ForwardIngestionPhaseChoices.SYNC,
                model=model_string,
                message=f"Synthetic skipped row for {model_string}.",
                exception="ValidationError",
                raw_data={"customer_row": "not exported"},
                defaults={"private_default": "not exported"},
            )
        run = self._execution_run(status="failed")
        ForwardExecutionStep.objects.create(
            run=run,
            index=1,
            status=ForwardExecutionStepStatusChoices.FAILED,
            model_string="dcim.site",
            label="dcim.site failed shard",
            ingestion=ingestion,
            failed_row_count=len(issue_models),
        )

        bundle = execution_run_support_bundle(run)

        ingestion_detail = bundle["steps"][0]["ingestion_detail"]
        self.assertEqual(ingestion_detail["issue_count"], len(issue_models))
        self.assertEqual(
            [issue["model"] for issue in ingestion_detail["issues"]],
            issue_models,
        )
        for issue in ingestion_detail["issues"]:
            self.assertIn("Synthetic skipped row", issue["message"])
            self.assertEqual(issue["exception"], "ValidationError")
            self.assertNotIn("raw_data", issue)
            self.assertNotIn("defaults", issue)
            self.assertNotIn("customer_row", issue)
            self.assertNotIn("private_default", issue)

    def test_failed_run_bundle_stays_actionable_without_branch_run_state(self):
        ingestion = ForwardIngestion.objects.create(
            sync=self.sync,
            snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
            snapshot_id=scenarios.SNAPSHOT_AFTER,
            failed_change_count=1,
        )
        ingestion.issues.create(
            phase=ForwardIngestionPhaseChoices.SYNC,
            model="dcim.virtualchassis",
            message="Synthetic failed row after worker failure.",
            exception="RuntimeError",
            raw_data={"customer_row": "not exported"},
            defaults={"private_default": "not exported"},
        )
        job = self._job(
            instance=self.sync,
            status=JobStatusChoices.STATUS_ERRORED,
            completed=timezone.now(),
        )
        run = self._execution_run(status="failed")
        ForwardExecutionStep.objects.create(
            run=run,
            index=1,
            status=ForwardExecutionStepStatusChoices.FAILED,
            model_string="dcim.virtualchassis",
            label="dcim.virtualchassis failed shard",
            ingestion=ingestion,
            job=job,
            last_error="Synthetic worker failure.",
            failed_row_count=1,
        )
        self.sync.clear_branch_run_state()

        bundle = execution_run_support_bundle(run)

        self.assertEqual(bundle["run"]["id"], run.pk)
        self.assertEqual(bundle["steps"][0]["status"], "failed")
        self.assertEqual(bundle["steps"][0]["job_detail"]["pk"], job.pk)
        self.assertEqual(bundle["steps"][0]["job_detail"]["status"], "errored")
        self.assertEqual(
            bundle["steps"][0]["ingestion_detail"]["issues"][0]["model"],
            "dcim.virtualchassis",
        )
        self.assertNotIn(
            "raw_data",
            bundle["steps"][0]["ingestion_detail"]["issues"][0],
        )
        self.assertEqual(
            bundle["recovery_recommendation"]["action"],
            "retry_current_step",
        )
        self.assertEqual(
            bundle["recovery_recommendation"]["step_index"],
            1,
        )

    def test_failed_partial_branch_requires_explicit_discard_before_retry(self):
        branch = Branch.objects.create(
            name="synthetic-discardable-branch",
            schema_id=f"synthetic_discard_{uuid4().hex[:12]}",
        )
        ingestion = ForwardIngestion.objects.create(
            sync=self.sync,
            snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
            snapshot_id=scenarios.SNAPSHOT_AFTER,
            branch=branch,
        )
        run = self._execution_run(status="failed")
        step = ForwardExecutionStep.objects.create(
            run=run,
            index=1,
            status=ForwardExecutionStepStatusChoices.FAILED,
            model_string="dcim.site",
            label="dcim.site synthetic shard",
            ingestion=ingestion,
            branch=branch,
            retry_count=2,
        )

        self.assertIsNone(current_retryable_step(run))
        self.assertEqual(current_discardable_step(run), step)

        discarded = discard_stage_branch_for_retry(step)

        self.assertEqual(discarded, step)
        ingestion.refresh_from_db()
        step.refresh_from_db()
        self.assertFalse(Branch.objects.filter(pk=branch.pk).exists())
        self.assertIsNone(ingestion.branch)
        self.assertIsNone(step.branch)
        self.assertEqual(step.status, ForwardExecutionStepStatusChoices.QUEUED)
        self.assertEqual(step.retry_count, 3)
        issue = ingestion.issues.get()
        self.assertEqual(issue.phase, ForwardIngestionPhaseChoices.SYNC)
        self.assertIn("Discarded failed shard branch", issue.message)

        second_discard = discard_stage_branch_for_retry(step)

        step.refresh_from_db()
        self.assertIsNone(second_discard)
        self.assertEqual(step.retry_count, 3)
        self.assertEqual(ingestion.issues.count(), 1)

    def test_retry_current_step_rebuilds_state_without_branch_run_json(self):
        run = self._execution_run(status="failed", next_step_index=2)
        step = ForwardExecutionStep.objects.create(
            run=run,
            index=2,
            status=ForwardExecutionStepStatusChoices.FAILED,
            model_string="dcim.device",
            label="dcim.device synthetic shard",
            query_name="Forward Devices",
            execution_mode="query_id",
            execution_value="query-device",
            retry_count=1,
            last_error="synthetic failure",
        )
        self.sync.clear_branch_run_state()

        self.assertEqual(current_retryable_step(run), step)

        retried = prepare_stage_step_retry(step)

        self.assertEqual(retried, step)
        step.refresh_from_db()
        self.sync.refresh_from_db()
        state = self.sync.get_branch_run_state()
        refreshed_run = active_execution_run(self.sync)
        self.assertEqual(step.status, ForwardExecutionStepStatusChoices.QUEUED)
        self.assertEqual(step.retry_count, 2)
        self.assertEqual(step.last_error, "")
        self.assertEqual(state, {})
        self.assertEqual(refreshed_run.pk, run.pk)
        self.assertEqual(refreshed_run.next_step_index, 2)
        self.assertEqual(refreshed_run.phase, "queued")

    def test_retry_current_step_is_idempotent_once_queued(self):
        run = self._execution_run(status="failed", next_step_index=1)
        step = ForwardExecutionStep.objects.create(
            run=run,
            index=1,
            status=ForwardExecutionStepStatusChoices.FAILED,
            model_string="dcim.device",
            label="dcim.device synthetic shard",
            retry_count=1,
            last_error="synthetic failure",
        )

        first_retry = prepare_stage_step_retry(step)
        second_retry = prepare_stage_step_retry(step)

        self.assertEqual(first_retry.pk, step.pk)
        self.assertIsNone(second_retry)
        step.refresh_from_db()
        self.sync.refresh_from_db()
        self.assertEqual(step.status, ForwardExecutionStepStatusChoices.QUEUED)
        self.assertEqual(step.retry_count, 2)
        self.assertEqual(self.sync.status, "queued")


class ExecutionLedgerConcurrencyTest(TransactionTestCase):
    def setUp(self):
        self.source = ForwardSource.objects.create(
            name="source-ledger-concurrency",
            type="saas",
            url="https://fwd.app",
            parameters=scenarios.source_parameters(),
        )
        self.sync = ForwardSync.objects.create(
            name="sync-ledger-concurrency",
            source=self.source,
            auto_merge=True,
            parameters={
                "snapshot_id": LATEST_PROCESSED_SNAPSHOT,
                "dcim.site": True,
            },
        )
        self.run = ForwardExecutionRun.objects.create(
            sync=self.sync,
            source=self.source,
            backend="branching",
            status="running",
            snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
            snapshot_id=scenarios.SNAPSHOT_AFTER,
            total_steps=1,
            next_step_index=1,
        )
        self.sync.set_branch_run_state(
            {
                "execution_run_id": self.run.pk,
                "snapshot_selector": LATEST_PROCESSED_SNAPSHOT,
                "snapshot_id": scenarios.SNAPSHOT_AFTER,
                "next_plan_index": 1,
                "total_plan_items": 1,
            }
        )
        self.step = ForwardExecutionStep.objects.create(
            run=self.run,
            index=1,
            status=ForwardExecutionStepStatusChoices.QUEUED,
            model_string="dcim.site",
            label="dcim.site concurrent shard",
        )

    def _job(self):
        return Job.objects.create(
            object_type=ContentType.objects.get_for_model(self.sync),
            object_id=self.sync.pk,
            name="synthetic concurrent stage job",
            status=JobStatusChoices.STATUS_PENDING,
            job_id=uuid4(),
            created=timezone.now(),
            started=timezone.now(),
            data={},
        )

    def _run_concurrently(self, worker):
        barrier = Barrier(2)
        lock = Lock()
        results = []
        errors = []

        def run_worker(arg):
            close_old_connections()
            try:
                barrier.wait(timeout=5)
                result = worker(arg)
                with lock:
                    results.append((arg, result))
            except Exception as exc:  # pragma: no cover - assertion path reports it
                with lock:
                    errors.append(exc)
            finally:
                close_old_connections()

        args = [self._job().pk, self._job().pk]
        threads = [Thread(target=run_worker, args=(arg,)) for arg in args]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=10)

        self.assertEqual(errors, [])
        self.assertEqual(len(results), 2)
        return results

    def test_simultaneous_stage_claim_allows_only_one_owner(self):
        def claim(job_id):
            sync = ForwardSync.objects.get(pk=self.sync.pk)
            job = Job.objects.get(pk=job_id)
            claimed = claim_stage_step(sync, 1, job)
            return claimed.pk if claimed else None

        results = self._run_concurrently(claim)
        claimed = [job_id for job_id, step_id in results if step_id == self.step.pk]
        rejected = [job_id for job_id, step_id in results if step_id is None]
        self.assertEqual(len(claimed), 1)
        self.assertEqual(len(rejected), 1)

        self.step.refresh_from_db()
        self.assertEqual(self.step.status, ForwardExecutionStepStatusChoices.RUNNING)
        self.assertEqual(self.step.job_id, claimed[0])

    def test_simultaneous_merge_claim_allows_only_one_owner(self):
        branch = Branch.objects.create(
            name="synthetic-concurrent-merge-branch",
            schema_id=f"synthetic_merge_{uuid4().hex[:12]}",
        )
        ingestion = ForwardIngestion.objects.create(sync=self.sync, branch=branch)
        self.step.status = ForwardExecutionStepStatusChoices.STAGED
        self.step.ingestion = ingestion
        self.step.branch = branch
        self.step.save()

        def claim(job_id):
            job = Job.objects.get(pk=job_id)
            current_ingestion = ForwardIngestion.objects.get(pk=ingestion.pk)
            return claim_ingestion_merge_step(current_ingestion, job)

        results = self._run_concurrently(claim)
        claimed = [job_id for job_id, success in results if success]
        rejected = [job_id for job_id, success in results if not success]
        self.assertEqual(len(claimed), 1)
        self.assertEqual(len(rejected), 1)

        self.step.refresh_from_db()
        self.assertEqual(
            self.step.status,
            ForwardExecutionStepStatusChoices.MERGE_QUEUED,
        )
        self.assertEqual(self.step.merge_job_id, claimed[0])

    def test_simultaneous_retry_preparation_increments_once(self):
        self.step.status = ForwardExecutionStepStatusChoices.FAILED
        self.step.retry_count = 2
        self.step.last_error = "synthetic failure"
        self.step.save()
        self.run.status = "failed"
        self.run.save()

        def retry(_job_id):
            step = ForwardExecutionStep.objects.get(pk=self.step.pk)
            retried = prepare_stage_step_retry(step)
            return retried.pk if retried else None

        results = self._run_concurrently(retry)
        retried = [step_id for _job_id, step_id in results if step_id == self.step.pk]
        skipped = [step_id for _job_id, step_id in results if step_id is None]
        self.assertEqual(len(retried), 1)
        self.assertEqual(len(skipped), 1)

        self.step.refresh_from_db()
        self.assertEqual(self.step.status, ForwardExecutionStepStatusChoices.QUEUED)
        self.assertEqual(self.step.retry_count, 3)

    def test_simultaneous_discard_retry_records_one_issue(self):
        branch = Branch.objects.create(
            name="synthetic-concurrent-discard-branch",
            schema_id=f"synthetic_discard_{uuid4().hex[:12]}",
        )
        ingestion = ForwardIngestion.objects.create(sync=self.sync, branch=branch)
        self.step.status = ForwardExecutionStepStatusChoices.FAILED
        self.step.ingestion = ingestion
        self.step.branch = branch
        self.step.retry_count = 1
        self.step.save()
        self.run.status = "failed"
        self.run.save()

        def discard(_job_id):
            step = ForwardExecutionStep.objects.get(pk=self.step.pk)
            discarded = discard_stage_branch_for_retry(step)
            return discarded.pk if discarded else None

        results = self._run_concurrently(discard)
        discarded = [step_id for _job_id, step_id in results if step_id == self.step.pk]
        skipped = [step_id for _job_id, step_id in results if step_id is None]
        self.assertEqual(len(discarded), 1)
        self.assertEqual(len(skipped), 1)

        self.step.refresh_from_db()
        ingestion.refresh_from_db()
        self.assertEqual(self.step.status, ForwardExecutionStepStatusChoices.QUEUED)
        self.assertEqual(self.step.retry_count, 2)
        self.assertIsNone(self.step.branch)
        self.assertIsNone(ingestion.branch)
        self.assertEqual(ingestion.issues.count(), 1)
        self.assertFalse(Branch.objects.filter(pk=branch.pk).exists())

    def test_simultaneous_finalize_leaves_one_completed_state(self):
        self.step.status = ForwardExecutionStepStatusChoices.MERGED
        self.step.save()

        def complete(_job_id):
            sync = ForwardSync.objects.get(pk=self.sync.pk)
            run = mark_run_completed(sync, baseline_ready=True)
            return run.pk if run else None

        results = self._run_concurrently(complete)
        completed = [run_id for _job_id, run_id in results if run_id == self.run.pk]
        self.assertEqual(len(completed), 2)

        self.run.refresh_from_db()
        self.assertEqual(self.run.status, "completed")
        self.assertTrue(self.run.baseline_ready)
        self.assertIsNotNone(self.run.completed)
