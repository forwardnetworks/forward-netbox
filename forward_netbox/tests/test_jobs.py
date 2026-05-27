from datetime import datetime
from types import SimpleNamespace
from unittest.mock import ANY
from unittest.mock import patch
from uuid import uuid4

from core.choices import JobStatusChoices
from core.exceptions import SyncError
from core.models import Job
from dcim.models import Site
from django.contrib.auth import get_user_model
from django.contrib.contenttypes.models import ContentType
from django.test import TestCase
from django.utils import timezone
from netbox.context import current_request
from netbox_branching.choices import BranchStatusChoices
from netbox_branching.models import Branch
from rq.timeouts import JobTimeoutException

from forward_netbox.choices import ForwardExecutionRunStatusChoices
from forward_netbox.choices import ForwardExecutionStepStatusChoices
from forward_netbox.choices import ForwardIngestionPhaseChoices
from forward_netbox.choices import ForwardSyncStatusChoices
from forward_netbox.jobs import merge_forwardingestion
from forward_netbox.jobs import record_timeout_issue
from forward_netbox.jobs import safe_save_job_data
from forward_netbox.jobs import stage_forward_branch_item
from forward_netbox.models import ForwardExecutionRun
from forward_netbox.models import ForwardExecutionStep
from forward_netbox.models import ForwardIngestion
from forward_netbox.models import ForwardIngestionIssue
from forward_netbox.models import ForwardSource
from forward_netbox.models import ForwardSync
from forward_netbox.utilities.branch_budget import BranchWorkload
from forward_netbox.utilities.branch_budget import build_branch_plan
from forward_netbox.utilities.execution_ledger import ensure_branch_execution_run
from forward_netbox.utilities.resumable_branching import enqueue_branch_stage_job
from forward_netbox.utilities.resumable_branching import update_plan_item_state
from forward_netbox.utilities.sync_state import clear_branch_run_state
from forward_netbox.utilities.sync_state import get_branch_run_display_state


class ForwardJobsTest(TestCase):
    def setUp(self):
        self.addCleanup(current_request.set, None)
        self.source = ForwardSource.objects.create(
            name="source-jobs",
            type="saas",
            url="https://fwd.app",
            parameters={
                "username": "user@example.com",
                "password": "secret",
                "verify": True,
                "timeout": 1200,
                "network_id": "test-network",
            },
        )
        self.sync = ForwardSync.objects.create(
            name="sync-jobs",
            source=self.source,
            auto_merge=False,
            parameters={"snapshot_id": "latestProcessed"},
        )
        self.ingestion = ForwardIngestion.objects.create(sync=self.sync)

    def test_record_timeout_issue_creates_single_issue_per_ingestion_phase(self):
        issue_1 = record_timeout_issue(
            self.ingestion,
            ForwardIngestionPhaseChoices.SYNC,
            "timeout",
        )
        issue_2 = record_timeout_issue(
            self.ingestion,
            ForwardIngestionPhaseChoices.SYNC,
            "timeout again",
        )

        self.assertEqual(issue_1.pk, issue_2.pk)
        self.assertEqual(
            ForwardIngestionIssue.objects.filter(
                ingestion=self.ingestion,
                phase=ForwardIngestionPhaseChoices.SYNC,
                exception=JobTimeoutException.__name__,
            ).count(),
            1,
        )

    def test_safe_save_job_data_persists_job_log_entries(self):
        class DummyJob:
            pk = 52

            def __init__(self):
                self.data = None
                self.log_entries = []
                self.saved_update_fields = None

            def save(self, update_fields=None):
                self.saved_update_fields = update_fields

        job = DummyJob()
        obj_with_logger = SimpleNamespace(
            logger=SimpleNamespace(
                log_data={
                    "logs": [
                        [
                            "2026-05-03T14:34:00+00:00",
                            "success",
                            "ui-harness-sync",
                            "/plugins/forward/sync/2/",
                            "Synthetic UI harness ingestion completed.",
                        ]
                    ],
                    "statistics": {},
                }
            )
        )

        safe_save_job_data(job, obj_with_logger)

        self.assertEqual(
            job.data["logs"][0][4],
            "Synthetic UI harness ingestion completed.",
        )
        self.assertEqual(len(job.log_entries), 1)
        self.assertEqual(job.log_entries[0]["level"], "info")
        self.assertEqual(
            job.log_entries[0]["message"],
            "Synthetic UI harness ingestion completed.",
        )
        self.assertEqual(job.saved_update_fields, ["data", "log_entries"])

    def test_safe_save_job_data_serializes_nested_model_values(self):
        class DummyJob:
            pk = 53

            def __init__(self):
                self.data = None
                self.log_entries = []
                self.saved_update_fields = None

            def save(self, update_fields=None):
                self.saved_update_fields = update_fields

        site = Site.objects.create(name="site-1", slug="site-1")
        job = DummyJob()
        obj_with_logger = SimpleNamespace(
            logger=SimpleNamespace(
                log_data={
                    "logs": [
                        [
                            datetime.fromisoformat(
                                "2026-05-04T14:00:00+00:00"
                            ).isoformat(),
                            "success",
                            site,
                            "/plugins/forward/sync/2/",
                            "Synthetic UI harness ingestion completed.",
                        ]
                    ],
                    "statistics": {"dcim.site": {"last_object": site}},
                }
            )
        )

        safe_save_job_data(job, obj_with_logger)

        self.assertEqual(job.data["logs"][0][2]["model"], "dcim.site")
        self.assertEqual(
            job.data["statistics"]["dcim.site"]["last_object"]["pk"], site.pk
        )
        self.assertEqual(job.saved_update_fields, ["data", "log_entries"])

    def test_ensure_branch_execution_run_reuses_active_ledger_run_without_branch_state(
        self,
    ):
        workload = BranchWorkload(
            model_string="dcim.site",
            label="sites",
            upsert_rows=[{"name": "site-1"}],
            coalesce_fields=[["name"]],
        )
        plan = build_branch_plan([workload], max_changes_per_branch=10)
        context = {
            "snapshot_selector": "latestProcessed",
            "snapshot_id": "snapshot-1",
        }
        existing_run = ForwardExecutionRun.objects.create(
            sync=self.sync,
            source=self.source,
            backend="branching",
            status="running",
            phase="executing",
            snapshot_selector="old",
            snapshot_id="old",
            total_steps=1,
            next_step_index=1,
        )

        run = ensure_branch_execution_run(
            sync=self.sync,
            context=context,
            plan=plan,
            plan_preview={"planned_shards": len(plan)},
            validation_run=None,
            job=None,
            max_changes_per_branch=10,
            auto_merge=False,
            model_change_density={},
            next_plan_index=1,
        )

        self.assertEqual(run.pk, existing_run.pk)
        self.assertEqual(ForwardExecutionRun.objects.filter(sync=self.sync).count(), 1)
        run.refresh_from_db()
        self.assertEqual(run.snapshot_id, "snapshot-1")
        self.assertEqual(run.next_step_index, 1)
        self.assertEqual(run.total_steps, len(plan))
        self.assertEqual(run.plan_preview["planned_shards"], len(plan))

    def test_ensure_branch_execution_run_ignores_terminal_run_id_from_stale_compatibility_state(
        self,
    ):
        workload = BranchWorkload(
            model_string="dcim.site",
            label="sites",
            upsert_rows=[{"name": "site-1"}],
            coalesce_fields=[["name"]],
        )
        plan = build_branch_plan([workload], max_changes_per_branch=10)
        context = {
            "snapshot_selector": "latestProcessed",
            "snapshot_id": "snapshot-1",
        }
        completed_run = ForwardExecutionRun.objects.create(
            sync=self.sync,
            source=self.source,
            backend="branching",
            status=ForwardExecutionRunStatusChoices.COMPLETED,
            phase="completed",
            snapshot_selector="latestProcessed",
            snapshot_id="snapshot-old",
            total_steps=1,
            next_step_index=2,
        )
        self.sync.set_branch_run_state(
            {
                "execution_run_id": completed_run.pk,
                "next_plan_index": 1,
                "total_plan_items": 1,
                "phase": "planning",
            }
        )

        run = ensure_branch_execution_run(
            sync=self.sync,
            context=context,
            plan=plan,
            plan_preview={"planned_shards": len(plan)},
            validation_run=None,
            job=None,
            max_changes_per_branch=10,
            auto_merge=False,
            model_change_density={},
            next_plan_index=1,
        )

        self.assertNotEqual(run.pk, completed_run.pk)
        self.assertEqual(run.status, ForwardExecutionRunStatusChoices.RUNNING)
        self.assertEqual(run.snapshot_id, "snapshot-1")

    def test_ensure_branch_execution_run_prefers_active_run_when_compatibility_id_is_terminal(
        self,
    ):
        workload = BranchWorkload(
            model_string="dcim.site",
            label="sites",
            upsert_rows=[{"name": "site-1"}],
            coalesce_fields=[["name"]],
        )
        plan = build_branch_plan([workload], max_changes_per_branch=10)
        context = {
            "snapshot_selector": "latestProcessed",
            "snapshot_id": "snapshot-1",
        }
        completed_run = ForwardExecutionRun.objects.create(
            sync=self.sync,
            source=self.source,
            backend="branching",
            status=ForwardExecutionRunStatusChoices.COMPLETED,
            phase="completed",
            snapshot_selector="latestProcessed",
            snapshot_id="snapshot-old",
            total_steps=1,
            next_step_index=2,
        )
        active_run = ForwardExecutionRun.objects.create(
            sync=self.sync,
            source=self.source,
            backend="branching",
            status=ForwardExecutionRunStatusChoices.RUNNING,
            phase="executing",
            snapshot_selector="latestProcessed",
            snapshot_id="snapshot-active",
            total_steps=1,
            next_step_index=1,
        )
        self.sync.set_branch_run_state(
            {
                "execution_run_id": completed_run.pk,
                "next_plan_index": 1,
                "total_plan_items": 1,
                "phase": "planning",
            }
        )

        run = ensure_branch_execution_run(
            sync=self.sync,
            context=context,
            plan=plan,
            plan_preview={"planned_shards": len(plan)},
            validation_run=None,
            job=None,
            max_changes_per_branch=10,
            auto_merge=False,
            model_change_density={},
            next_plan_index=1,
        )

        self.assertEqual(run.pk, active_run.pk)
        self.assertEqual(run.status, ForwardExecutionRunStatusChoices.RUNNING)
        self.assertEqual(run.snapshot_id, "snapshot-1")
        self.assertEqual(ForwardExecutionRun.objects.filter(sync=self.sync).count(), 2)

    @patch("forward_netbox.utilities.multi_branch.ForwardMultiBranchExecutor")
    def test_stage_forward_branch_item_timeout_marks_current_shard_retryable(
        self,
        mock_executor,
    ):
        class DummyJob:
            pk = 54
            object_id = self.sync.pk
            user = None
            job_id = "stage-timeout-job"

            def __init__(self):
                self.data = None
                self.log_entries = []
                self.started = None
                self.terminated_status = None

            def start(self):
                self.started = True

            def terminate(self, status=None):
                self.terminated_status = status

            def save(self, update_fields=None):
                self.saved_update_fields = update_fields

        self.sync.set_branch_run_state(
            {
                "next_plan_index": 2,
                "total_plan_items": 3,
                "awaiting_merge": False,
                "plan_items": [
                    {"index": 1, "status": "merged"},
                    {"index": 2, "status": "staging", "retry_count": 1},
                ],
            }
        )
        execution_run = ForwardExecutionRun.objects.create(
            sync=self.sync,
            source=self.source,
            snapshot_selector="latestProcessed",
            snapshot_id="snapshot-1",
            total_steps=3,
            next_step_index=2,
        )
        ForwardExecutionStep.objects.create(
            run=execution_run,
            index=2,
            status="running",
            model_string="dcim.site",
            retry_count=1,
        )
        clear_branch_run_state(self.sync)
        mock_executor.return_value.run_next_plan_item.side_effect = JobTimeoutException(
            "timeout"
        )
        job = DummyJob()

        stage_forward_branch_item(job)

        self.sync.refresh_from_db()
        state = get_branch_run_display_state(self.sync)
        self.assertEqual(state, {})
        execution_step = ForwardExecutionStep.objects.get(run=execution_run, index=2)
        self.assertEqual(execution_step.status, "timeout")
        self.assertEqual(execution_step.retry_count, 2)
        self.assertEqual(
            ForwardIngestionIssue.objects.filter(
                ingestion=self.ingestion,
                phase=ForwardIngestionPhaseChoices.SYNC,
                exception=JobTimeoutException.__name__,
            ).count(),
            1,
        )

    @patch("forward_netbox.utilities.resumable_branching.Job.enqueue")
    @patch("forward_netbox.utilities.multi_branch.ForwardMultiBranchExecutor")
    def test_stage_forward_branch_item_retries_transient_db_connection_failure(
        self,
        mock_executor,
        mock_enqueue,
    ):
        class DummyJob:
            pk = 62
            object_id = self.sync.pk
            user = None
            job_id = "stage-db-pressure-job"

            def __init__(self):
                self.data = None
                self.log_entries = []
                self.started = None
                self.terminated_status = None

            def start(self):
                self.started = True

            def terminate(self, status=None):
                self.terminated_status = status

            def save(self, update_fields=None):
                self.saved_update_fields = update_fields

        self.sync.set_branch_run_state(
            {
                "next_plan_index": 2,
                "total_plan_items": 3,
                "awaiting_merge": False,
                "plan_items": [
                    {"index": 1, "status": "merged"},
                    {"index": 2, "status": "staging", "retry_count": 1},
                ],
            }
        )
        execution_run = ForwardExecutionRun.objects.create(
            sync=self.sync,
            source=self.source,
            snapshot_selector="latestProcessed",
            snapshot_id="snapshot-1",
            total_steps=3,
            next_step_index=2,
            status=ForwardExecutionRunStatusChoices.RUNNING,
        )
        execution_step = ForwardExecutionStep.objects.create(
            run=execution_run,
            index=2,
            status=ForwardExecutionStepStatusChoices.RUNNING,
            model_string="dcim.site",
            retry_count=1,
        )
        clear_branch_run_state(self.sync)

        queued_job = Job.objects.create(
            object_type=ContentType.objects.get_for_model(ForwardSync),
            object_id=self.sync.pk,
            name="retry shard job",
            user=None,
            status=JobStatusChoices.STATUS_PENDING,
            job_id=uuid4(),
            created=timezone.now(),
            data={},
        )
        mock_enqueue.return_value = queued_job
        mock_executor.return_value.run_next_plan_item.side_effect = SyncError(
            'connection failed: connection to server at "172.19.0.3", port 5432 failed: FATAL:  sorry, too many clients already'
        )
        job = DummyJob()

        stage_forward_branch_item(job)

        execution_run.refresh_from_db()
        execution_step.refresh_from_db()
        self.sync.refresh_from_db()
        self.assertEqual(job.terminated_status, JobStatusChoices.STATUS_ERRORED)
        self.assertEqual(execution_run.status, ForwardExecutionRunStatusChoices.RUNNING)
        self.assertEqual(
            execution_step.status, ForwardExecutionStepStatusChoices.QUEUED
        )
        self.assertEqual(execution_step.job_id, queued_job.pk)
        self.assertNotEqual(self.sync.status, ForwardSyncStatusChoices.FAILED)

    @patch("forward_netbox.utilities.multi_branch.ForwardMultiBranchExecutor")
    def test_stage_forward_branch_item_skips_already_staged_ledger_step(
        self,
        mock_executor,
    ):
        class DummyJob:
            pk = 55
            object_id = self.sync.pk
            user = None
            job_id = "duplicate-stage-job"

            def __init__(self):
                self.data = None
                self.log_entries = []
                self.started = None
                self.terminated_status = None

            def start(self):
                self.started = True

            def terminate(self, status=None):
                self.terminated_status = status

            def save(self, update_fields=None):
                self.saved_update_fields = update_fields

        execution_run = ForwardExecutionRun.objects.create(
            sync=self.sync,
            source=self.source,
            snapshot_selector="latestProcessed",
            snapshot_id="snapshot-1",
            total_steps=1,
            next_step_index=1,
        )
        ForwardExecutionStep.objects.create(
            run=execution_run,
            index=1,
            status="staged",
            model_string="dcim.site",
            estimated_changes=1,
        )
        self.sync.set_branch_run_state(
            {
                "execution_run_id": execution_run.pk,
                "next_plan_index": 1,
                "total_plan_items": 1,
                "plan_items": [{"index": 1, "status": "staged"}],
            }
        )
        job = DummyJob()

        stage_forward_branch_item(job)

        mock_executor.return_value.run_next_plan_item.assert_not_called()
        self.assertIsNone(job.terminated_status)

    @patch("forward_netbox.utilities.multi_branch.ForwardMultiBranchExecutor")
    def test_stage_forward_branch_item_uses_ledger_without_branch_run_json(
        self,
        mock_executor,
    ):
        class DummyJob:
            pk = 58
            object_id = self.sync.pk
            user = None
            job_id = "ledger-stage-job"

            def __init__(self):
                self.data = None
                self.log_entries = []
                self.started = None
                self.terminated_status = None

            def start(self):
                self.started = True

            def terminate(self, status=None):
                self.terminated_status = status

            def save(self, update_fields=None):
                self.saved_update_fields = update_fields

        execution_run = ForwardExecutionRun.objects.create(
            sync=self.sync,
            source=self.source,
            snapshot_selector="latestProcessed",
            snapshot_id="snapshot-1",
            total_steps=2,
            next_step_index=2,
            status="running",
        )
        ForwardExecutionStep.objects.create(
            run=execution_run,
            index=1,
            status="merged",
            model_string="dcim.site",
            estimated_changes=1,
        )
        step = ForwardExecutionStep.objects.create(
            run=execution_run,
            index=2,
            status="pending",
            model_string="dcim.device",
            label="dcim.device shard",
            query_name="Forward Devices",
            execution_mode="query_id",
            execution_value="query-device",
            shard_keys=["device:one"],
            estimated_changes=2,
        )
        self.sync.clear_branch_run_state()
        job = DummyJob()

        stage_forward_branch_item(job)

        mock_executor.return_value.run_next_plan_item.assert_called_once()
        step.refresh_from_db()
        self.assertEqual(step.status, "running")
        self.sync.refresh_from_db()
        execution_run.refresh_from_db()
        self.assertEqual(self.sync.get_branch_run_state(), {})
        self.assertEqual(execution_run.next_step_index, 2)
        self.assertIsNone(job.terminated_status)

    @patch("forward_netbox.jobs.maybe_enqueue_next_branch_stage")
    @patch("forward_netbox.utilities.multi_branch.ForwardMultiBranchExecutor")
    def test_stage_forward_branch_item_auto_queues_merge_for_staged_step(
        self,
        mock_executor,
        mock_enqueue_merge,
    ):
        class DummyJob:
            pk = 59
            object_id = self.sync.pk
            user = None
            job_id = "ledger-stage-auto-merge"

            def __init__(self):
                self.data = None
                self.log_entries = []
                self.started = None
                self.terminated_status = None

            def start(self):
                self.started = True

            def terminate(self, status=None):
                self.terminated_status = status

            def save(self, update_fields=None):
                self.saved_update_fields = update_fields

        execution_run = ForwardExecutionRun.objects.create(
            sync=self.sync,
            source=self.source,
            snapshot_selector="latestProcessed",
            snapshot_id="snapshot-1",
            total_steps=2,
            next_step_index=2,
            status="running",
            auto_merge=True,
        )
        ForwardExecutionStep.objects.create(
            run=execution_run,
            index=1,
            status="merged",
            model_string="dcim.site",
            estimated_changes=1,
            ingestion=self.ingestion,
        )
        step = ForwardExecutionStep.objects.create(
            run=execution_run,
            index=2,
            status="pending",
            model_string="dcim.device",
            label="dcim.device shard",
            query_name="Forward Devices",
            execution_mode="query_id",
            execution_value="query-device",
            shard_keys=["device:one"],
            estimated_changes=2,
            ingestion=self.ingestion,
        )
        self.sync.clear_branch_run_state()

        def _mark_staged(*args, **kwargs):
            step.status = ForwardExecutionStepStatusChoices.STAGED
            step.save(update_fields=["status"])

        mock_executor.return_value.run_next_plan_item.side_effect = _mark_staged
        merge_job = Job.objects.create(
            object_type=ContentType.objects.get_for_model(ForwardIngestion),
            object_id=self.ingestion.pk,
            name="auto merge queue job",
            user=None,
            status=JobStatusChoices.STATUS_PENDING,
            job_id=uuid4(),
            created=timezone.now(),
            data={},
        )
        mock_enqueue_merge.return_value = merge_job
        job = DummyJob()

        stage_forward_branch_item(job)

        mock_enqueue_merge.assert_called_once_with(self.ingestion, job.user)
        self.assertIsNone(job.terminated_status)

    @patch("forward_netbox.jobs._stop_stage_liveness_monitor")
    @patch("forward_netbox.jobs._stage_liveness_monitor")
    @patch("forward_netbox.utilities.multi_branch.ForwardMultiBranchExecutor")
    def test_stage_forward_branch_item_passes_overlap_stage_flag(
        self,
        mock_executor,
        mock_stage_liveness_monitor,
        mock_stop_stage_liveness_monitor,
    ):
        mock_stage_liveness_monitor.return_value = (object(), object())
        queued_job = Job.objects.create(
            object_type=ContentType.objects.get_for_model(ForwardSync),
            object_id=self.sync.pk,
            name="ledger overlap stage job",
            user=None,
            status=JobStatusChoices.STATUS_PENDING,
            job_id=uuid4(),
            created=timezone.now(),
            data={},
        )

        execution_run = ForwardExecutionRun.objects.create(
            sync=self.sync,
            source=self.source,
            snapshot_selector="latestProcessed",
            snapshot_id="snapshot-1",
            total_steps=2,
            next_step_index=2,
            status="running",
            auto_merge=True,
        )
        current_merge_job = Job.objects.create(
            object_type=ContentType.objects.get_for_model(ForwardIngestion),
            object_id=self.ingestion.pk,
            name="current merge job",
            user=None,
            status=JobStatusChoices.STATUS_PENDING,
            job_id=uuid4(),
            created=timezone.now(),
            data={},
        )
        ForwardExecutionStep.objects.create(
            run=execution_run,
            index=1,
            status="merge_queued",
            model_string="dcim.site",
            estimated_changes=1,
            ingestion=self.ingestion,
            merge_job=current_merge_job,
        )
        ForwardExecutionStep.objects.create(
            run=execution_run,
            index=2,
            status="queued",
            model_string="dcim.device",
            label="dcim.device shard",
            query_name="Forward Devices",
            execution_mode="query_id",
            execution_value="query-device",
            shard_keys=["device:one"],
            estimated_changes=2,
            job=queued_job,
        )
        self.sync.clear_branch_run_state()

        stage_forward_branch_item(queued_job, overlap_stage=True)

        mock_executor.return_value.run_next_plan_item.assert_called_once_with(
            max_changes_per_branch=self.sync.get_max_changes_per_branch(),
            expected_plan_index=2,
            overlap_stage=True,
        )
        mock_stage_liveness_monitor.assert_called_once_with(
            sync=self.sync,
            logger_=ANY,
            shard_index=2,
            model_string="dcim.device",
        )
        mock_stop_stage_liveness_monitor.assert_called_once()

    @patch("forward_netbox.utilities.multi_branch.ForwardMultiBranchExecutor")
    def test_stage_forward_branch_item_prefers_ledger_state_over_stale_branch_json(
        self,
        mock_executor,
    ):
        class DummyJob:
            pk = 60
            object_id = self.sync.pk
            user = None
            job_id = "ledger-stage-stale-json"

            def __init__(self):
                self.data = None
                self.log_entries = []
                self.started = None
                self.terminated_status = None

            def start(self):
                self.started = True

            def terminate(self, status=None):
                self.terminated_status = status

            def save(self, update_fields=None):
                self.saved_update_fields = update_fields

        execution_run = ForwardExecutionRun.objects.create(
            sync=self.sync,
            source=self.source,
            snapshot_selector="latestProcessed",
            snapshot_id="snapshot-1",
            total_steps=2,
            next_step_index=2,
            status="running",
        )
        ForwardExecutionStep.objects.create(
            run=execution_run,
            index=1,
            status="merged",
            model_string="dcim.site",
            estimated_changes=1,
        )
        step = ForwardExecutionStep.objects.create(
            run=execution_run,
            index=2,
            status="pending",
            model_string="dcim.device",
            label="dcim.device shard",
            query_name="Forward Devices",
            execution_mode="query_id",
            execution_value="query-device",
            shard_keys=["device:one"],
            estimated_changes=2,
        )
        parameters = dict(self.sync.parameters or {})
        parameters["_branch_run"] = {
            "next_plan_index": 1,
            "total_plan_items": 99,
            "awaiting_merge": True,
        }
        self.sync.parameters = parameters
        self.sync.save(update_fields=["parameters"])
        job = DummyJob()

        stage_forward_branch_item(job)

        mock_executor.return_value.run_next_plan_item.assert_called_once()
        self.assertEqual(
            mock_executor.return_value.run_next_plan_item.call_args.kwargs[
                "expected_plan_index"
            ],
            2,
        )
        step.refresh_from_db()
        self.assertEqual(step.status, "running")
        self.assertIsNone(job.terminated_status)

    @patch("forward_netbox.utilities.multi_branch.ForwardMultiBranchExecutor")
    @patch("forward_netbox.jobs.reconcile_execution_run")
    @patch("forward_netbox.jobs.claim_stage_step")
    def test_stage_forward_branch_item_retries_claim_after_reconcile(
        self,
        mock_claim_stage_step,
        mock_reconcile,
        mock_executor,
    ):
        class DummyJob:
            pk = 59
            object_id = self.sync.pk
            user = None
            job_id = "ledger-stage-retry-job"

            def __init__(self):
                self.data = None
                self.log_entries = []
                self.started = None
                self.terminated_status = None

            def start(self):
                self.started = True

            def terminate(self, status=None):
                self.terminated_status = status

            def save(self, update_fields=None):
                self.saved_update_fields = update_fields

        execution_run = ForwardExecutionRun.objects.create(
            sync=self.sync,
            source=self.source,
            snapshot_selector="latestProcessed",
            snapshot_id="snapshot-1",
            total_steps=2,
            next_step_index=2,
            status="running",
        )
        step = ForwardExecutionStep.objects.create(
            run=execution_run,
            index=2,
            status="pending",
            model_string="dcim.device",
            estimated_changes=2,
        )
        self.sync.clear_branch_run_state()
        job = DummyJob()
        mock_claim_stage_step.side_effect = [None, step]

        stage_forward_branch_item(job)

        self.assertEqual(mock_claim_stage_step.call_count, 2)
        self.assertGreaterEqual(mock_reconcile.call_count, 2)
        mock_executor.return_value.run_next_plan_item.assert_called_once()
        self.assertIsNone(job.terminated_status)

    @patch("forward_netbox.utilities.multi_branch.ForwardMultiBranchExecutor")
    @patch("forward_netbox.jobs.claim_stage_step")
    def test_stage_forward_branch_item_stale_claim_failure_does_not_fail_current_step(
        self,
        mock_claim_stage_step,
        mock_executor,
    ):
        class DummyJob:
            pk = 61
            object_id = self.sync.pk
            user = None
            job_id = "stale-claimed-stage-job"

            def __init__(self):
                self.data = None
                self.log_entries = []
                self.started = None
                self.terminated_status = None

            def start(self):
                self.started = True

            def terminate(self, status=None):
                self.terminated_status = status

            def save(self, update_fields=None):
                self.saved_update_fields = update_fields

        execution_run = ForwardExecutionRun.objects.create(
            sync=self.sync,
            source=self.source,
            snapshot_selector="latestProcessed",
            snapshot_id="snapshot-1",
            total_steps=60,
            next_step_index=46,
            status=ForwardExecutionRunStatusChoices.RUNNING,
        )
        stale_step = ForwardExecutionStep.objects.create(
            run=execution_run,
            index=46,
            status=ForwardExecutionStepStatusChoices.RUNNING,
            model_string="ipam.prefix",
            estimated_changes=1,
        )
        current_step = ForwardExecutionStep.objects.create(
            run=execution_run,
            index=54,
            status=ForwardExecutionStepStatusChoices.RUNNING,
            model_string="dcim.inventoryitem",
            estimated_changes=1,
        )
        self.sync.clear_branch_run_state()
        job = DummyJob()
        mock_claim_stage_step.return_value = stale_step

        def stale_job_fails_after_run_advances(**kwargs):
            execution_run.next_step_index = 54
            execution_run.save(update_fields=["next_step_index"])
            stale_step.status = ForwardExecutionStepStatusChoices.MERGED
            stale_step.save(update_fields=["status"])
            raise SyncError("Unable to resolve execution shard for claimed index 46.")

        mock_executor.return_value.run_next_plan_item.side_effect = (
            stale_job_fails_after_run_advances
        )

        stage_forward_branch_item(job)

        execution_run.refresh_from_db()
        stale_step.refresh_from_db()
        current_step.refresh_from_db()
        self.sync.refresh_from_db()
        self.assertNotIn(
            execution_run.status,
            (
                ForwardExecutionRunStatusChoices.FAILED,
                ForwardExecutionRunStatusChoices.TIMEOUT,
            ),
        )
        self.assertEqual(stale_step.status, ForwardExecutionStepStatusChoices.MERGED)
        self.assertNotEqual(
            current_step.status,
            ForwardExecutionStepStatusChoices.FAILED,
        )
        self.assertNotIn("Unable to resolve", current_step.last_error)
        self.assertNotEqual(self.sync.status, ForwardSyncStatusChoices.FAILED)
        self.assertIsNone(job.terminated_status)

    @patch("forward_netbox.utilities.multi_branch.ForwardMultiBranchExecutor")
    def test_stage_forward_branch_item_skips_when_no_active_run_and_history_exists(
        self,
        mock_executor,
    ):
        class DummyJob:
            pk = 60
            object_id = self.sync.pk
            user = None
            job_id = "stale-stage-job"

            def __init__(self):
                self.data = None
                self.log_entries = []
                self.started = None
                self.terminated_status = None

            def start(self):
                self.started = True

            def terminate(self, status=None):
                self.terminated_status = status

            def save(self, update_fields=None):
                self.saved_update_fields = update_fields

        ForwardExecutionRun.objects.create(
            sync=self.sync,
            source=self.source,
            snapshot_selector="latestProcessed",
            snapshot_id="snapshot-complete",
            total_steps=1,
            next_step_index=2,
            status=ForwardExecutionRunStatusChoices.COMPLETED,
        )
        self.sync.set_branch_run_state(
            {
                "next_plan_index": 1,
                "total_plan_items": 1,
                "plan_items": [{"index": 1, "status": "queued"}],
            }
        )
        job = DummyJob()

        stage_forward_branch_item(job)

        mock_executor.assert_not_called()
        self.assertIsNone(job.terminated_status)
        self.sync.refresh_from_db()
        self.assertEqual(self.sync.get_branch_run_state(), {})

    @patch("forward_netbox.utilities.resumable_branching.Job.enqueue")
    def test_enqueue_branch_stage_job_uses_ledger_without_branch_run_json(
        self,
        mock_enqueue,
    ):
        queued_job = Job.objects.create(
            object_type=ContentType.objects.get_for_model(ForwardSync),
            object_id=self.sync.pk,
            name="queued shard job",
            user=None,
            status=JobStatusChoices.STATUS_PENDING,
            job_id=uuid4(),
            created=timezone.now(),
            data={},
        )
        mock_enqueue.return_value = queued_job
        execution_run = ForwardExecutionRun.objects.create(
            sync=self.sync,
            source=self.source,
            snapshot_selector="latestProcessed",
            snapshot_id="snapshot-1",
            total_steps=2,
            next_step_index=2,
            status="running",
        )
        ForwardExecutionStep.objects.create(
            run=execution_run,
            index=1,
            status="merged",
            model_string="dcim.site",
            estimated_changes=1,
        )
        step = ForwardExecutionStep.objects.create(
            run=execution_run,
            index=2,
            status="pending",
            model_string="dcim.device",
            label="dcim.device shard",
            query_name="Forward Devices",
            execution_mode="query_id",
            execution_value="query-device",
            shard_keys=["device:one"],
            estimated_changes=2,
        )
        self.sync.clear_branch_run_state()
        initial_parameters = dict(self.sync.parameters or {})

        job = enqueue_branch_stage_job(self.sync, user=None, adhoc=True)

        self.assertEqual(job, queued_job)
        mock_enqueue.assert_called_once()
        self.sync.refresh_from_db()
        step.refresh_from_db()
        execution_run.refresh_from_db()
        self.assertEqual(self.sync.get_branch_run_state(), {})
        self.assertEqual(step.status, ForwardExecutionStepStatusChoices.QUEUED)
        self.assertEqual(step.job_id, queued_job.pk)
        self.assertEqual(execution_run.phase, "queued")
        self.assertEqual(execution_run.next_step_index, 2)
        self.assertEqual(self.sync.parameters, initial_parameters)

    @patch("forward_netbox.utilities.resumable_branching.Job.enqueue")
    def test_enqueue_branch_stage_job_marks_overlap_stage_job(
        self,
        mock_enqueue,
    ):
        queued_job = Job.objects.create(
            object_type=ContentType.objects.get_for_model(ForwardSync),
            object_id=self.sync.pk,
            name="queued overlap shard job",
            user=None,
            status=JobStatusChoices.STATUS_PENDING,
            job_id=uuid4(),
            created=timezone.now(),
            data={},
        )
        mock_enqueue.return_value = queued_job
        execution_run = ForwardExecutionRun.objects.create(
            sync=self.sync,
            source=self.source,
            snapshot_selector="latestProcessed",
            snapshot_id="snapshot-1",
            total_steps=2,
            next_step_index=2,
            status="running",
            auto_merge=True,
        )
        current_merge_job = Job.objects.create(
            object_type=ContentType.objects.get_for_model(ForwardIngestion),
            object_id=self.ingestion.pk,
            name="current merge job",
            user=None,
            status=JobStatusChoices.STATUS_PENDING,
            job_id=uuid4(),
            created=timezone.now(),
            data={},
        )
        ForwardExecutionStep.objects.create(
            run=execution_run,
            index=1,
            status="merge_queued",
            model_string="dcim.site",
            estimated_changes=1,
            ingestion=self.ingestion,
            merge_job=current_merge_job,
        )
        ForwardExecutionStep.objects.create(
            run=execution_run,
            index=2,
            status="pending",
            model_string="dcim.device",
            estimated_changes=2,
        )
        self.sync.clear_branch_run_state()

        job = enqueue_branch_stage_job(
            self.sync,
            user=None,
            adhoc=True,
            overlap_stage=True,
        )

        self.assertEqual(job, queued_job)
        self.assertTrue(mock_enqueue.call_args.kwargs["overlap_stage"])

    def test_update_plan_item_state_updates_ledger_without_branch_run_json(self):
        queued_job = Job.objects.create(
            object_type=ContentType.objects.get_for_model(ForwardSync),
            object_id=self.sync.pk,
            name="queued shard job",
            user=None,
            status=JobStatusChoices.STATUS_PENDING,
            job_id=uuid4(),
            created=timezone.now(),
            data={},
        )
        execution_run = ForwardExecutionRun.objects.create(
            sync=self.sync,
            source=self.source,
            snapshot_selector="latestProcessed",
            snapshot_id="snapshot-1",
            total_steps=1,
            next_step_index=1,
        )
        step = ForwardExecutionStep.objects.create(
            run=execution_run,
            index=1,
            status="pending",
            model_string="dcim.site",
            estimated_changes=1,
        )
        self.sync.clear_branch_run_state()

        updated = update_plan_item_state(
            self.sync,
            1,
            status="queued",
            stage_job_id=queued_job.pk,
        )

        self.assertTrue(updated)
        self.sync.refresh_from_db()
        step.refresh_from_db()
        self.assertEqual(self.sync.get_branch_run_state(), {})
        self.assertEqual(step.status, ForwardExecutionStepStatusChoices.QUEUED)
        self.assertEqual(step.job_id, queued_job.pk)

    @patch("forward_netbox.utilities.resumable_branching.Job.enqueue")
    def test_enqueue_branch_stage_job_uses_latest_failed_ledger_without_branch_run_json(
        self,
        mock_enqueue,
    ):
        queued_job = Job.objects.create(
            object_type=ContentType.objects.get_for_model(ForwardSync),
            object_id=self.sync.pk,
            name="queued shard job",
            user=None,
            status=JobStatusChoices.STATUS_PENDING,
            job_id=uuid4(),
            created=timezone.now(),
            data={},
        )
        mock_enqueue.return_value = queued_job
        execution_run = ForwardExecutionRun.objects.create(
            sync=self.sync,
            source=self.source,
            snapshot_selector="latestProcessed",
            snapshot_id="snapshot-1",
            total_steps=2,
            next_step_index=2,
            status="failed",
        )
        ForwardExecutionStep.objects.create(
            run=execution_run,
            index=1,
            status="merged",
            model_string="dcim.site",
            estimated_changes=1,
        )
        step = ForwardExecutionStep.objects.create(
            run=execution_run,
            index=2,
            status="pending",
            model_string="dcim.device",
            estimated_changes=2,
        )
        self.sync.clear_branch_run_state()

        job = enqueue_branch_stage_job(self.sync, user=None, adhoc=True)

        self.assertEqual(job, queued_job)
        step.refresh_from_db()
        execution_run.refresh_from_db()
        self.assertEqual(step.status, ForwardExecutionStepStatusChoices.QUEUED)
        self.assertEqual(step.job_id, queued_job.pk)
        self.assertEqual(execution_run.status, "running")

    @patch("forward_netbox.utilities.resumable_branching.Job.enqueue")
    def test_enqueue_branch_stage_job_skips_when_another_stage_is_running(
        self, mock_enqueue
    ):
        execution_run = ForwardExecutionRun.objects.create(
            sync=self.sync,
            source=self.source,
            snapshot_selector="latestProcessed",
            snapshot_id="snapshot-1",
            total_steps=3,
            next_step_index=2,
            status="running",
        )
        ForwardExecutionStep.objects.create(
            run=execution_run,
            index=1,
            status="merged",
            model_string="dcim.site",
            estimated_changes=1,
        )
        ForwardExecutionStep.objects.create(
            run=execution_run,
            index=2,
            status="running",
            model_string="dcim.device",
            estimated_changes=2,
        )
        self.sync.clear_branch_run_state()

        job = enqueue_branch_stage_job(self.sync, user=None, adhoc=True)

        self.assertIsNone(job)
        mock_enqueue.assert_not_called()

    @patch("forward_netbox.utilities.resumable_branching.Job.enqueue")
    def test_enqueue_branch_stage_job_reuses_existing_queued_step_job(
        self, mock_enqueue
    ):
        execution_run = ForwardExecutionRun.objects.create(
            sync=self.sync,
            source=self.source,
            snapshot_selector="latestProcessed",
            snapshot_id="snapshot-1",
            total_steps=2,
            next_step_index=2,
            status="running",
        )
        existing_job = Job.objects.create(
            object_type=ContentType.objects.get_for_model(ForwardSync),
            object_id=self.sync.pk,
            name="existing queued stage job",
            user=None,
            status=JobStatusChoices.STATUS_PENDING,
            job_id=uuid4(),
            created=timezone.now(),
            data={},
        )
        ForwardExecutionStep.objects.create(
            run=execution_run,
            index=2,
            status="queued",
            model_string="dcim.device",
            estimated_changes=2,
            job=existing_job,
        )
        self.sync.clear_branch_run_state()

        job = enqueue_branch_stage_job(self.sync, user=None, adhoc=True)

        self.assertEqual(job, existing_job)
        mock_enqueue.assert_not_called()

    @patch("forward_netbox.utilities.resumable_branching.Job.enqueue")
    def test_enqueue_branch_stage_job_does_not_restage_staged_step(self, mock_enqueue):
        execution_run = ForwardExecutionRun.objects.create(
            sync=self.sync,
            source=self.source,
            snapshot_selector="latestProcessed",
            snapshot_id="snapshot-1",
            total_steps=2,
            next_step_index=2,
            status="running",
            auto_merge=True,
        )
        ForwardExecutionStep.objects.create(
            run=execution_run,
            index=2,
            status="staged",
            model_string="dcim.device",
            estimated_changes=2,
        )
        self.sync.clear_branch_run_state()

        job = enqueue_branch_stage_job(self.sync, user=None, adhoc=True)

        self.assertIsNone(job)
        mock_enqueue.assert_not_called()

    @patch("forward_netbox.utilities.resumable_branching.Job.enqueue")
    def test_enqueue_branch_stage_job_requeues_orphaned_queued_step(self, mock_enqueue):
        queued_job = Job.objects.create(
            object_type=ContentType.objects.get_for_model(ForwardSync),
            object_id=self.sync.pk,
            name="queued shard job",
            user=None,
            status=JobStatusChoices.STATUS_PENDING,
            job_id=uuid4(),
            created=timezone.now(),
            data={},
        )
        mock_enqueue.return_value = queued_job
        execution_run = ForwardExecutionRun.objects.create(
            sync=self.sync,
            source=self.source,
            snapshot_selector="latestProcessed",
            snapshot_id="snapshot-1",
            total_steps=2,
            next_step_index=2,
            status="running",
        )
        ForwardExecutionStep.objects.create(
            run=execution_run,
            index=1,
            status="merged",
            model_string="dcim.site",
            estimated_changes=1,
        )
        step = ForwardExecutionStep.objects.create(
            run=execution_run,
            index=2,
            status="queued",
            model_string="dcim.device",
            estimated_changes=2,
        )
        self.sync.clear_branch_run_state()

        job = enqueue_branch_stage_job(self.sync, user=None, adhoc=True)

        self.assertEqual(job, queued_job)
        mock_enqueue.assert_called_once()
        step.refresh_from_db()
        self.assertEqual(step.status, ForwardExecutionStepStatusChoices.QUEUED)
        self.assertEqual(step.job_id, queued_job.pk)

    @patch("forward_netbox.utilities.resumable_branching.reconcile_execution_run")
    @patch("forward_netbox.utilities.resumable_branching.Job.enqueue")
    def test_enqueue_branch_stage_job_reconciles_run_before_queue(
        self,
        mock_enqueue,
        mock_reconcile,
    ):
        queued_job = Job.objects.create(
            object_type=ContentType.objects.get_for_model(ForwardSync),
            object_id=self.sync.pk,
            name="queued shard job",
            user=None,
            status=JobStatusChoices.STATUS_PENDING,
            job_id=uuid4(),
            created=timezone.now(),
            data={},
        )
        mock_enqueue.return_value = queued_job
        execution_run = ForwardExecutionRun.objects.create(
            sync=self.sync,
            source=self.source,
            snapshot_selector="latestProcessed",
            snapshot_id="snapshot-1",
            total_steps=2,
            next_step_index=2,
            status="running",
        )
        step = ForwardExecutionStep.objects.create(
            run=execution_run,
            index=2,
            status="pending",
            model_string="dcim.device",
            estimated_changes=2,
        )
        self.sync.clear_branch_run_state()

        job = enqueue_branch_stage_job(self.sync, user=None, adhoc=True)

        self.assertEqual(job, queued_job)
        mock_reconcile.assert_called_once_with(execution_run)
        step.refresh_from_db()
        self.assertEqual(step.status, ForwardExecutionStepStatusChoices.QUEUED)

    @patch("forward_netbox.utilities.resumable_branching.Job.enqueue")
    def test_enqueue_branch_stage_job_upgrades_legacy_state_to_ledger(
        self,
        mock_enqueue,
    ):
        queued_job = Job.objects.create(
            object_type=ContentType.objects.get_for_model(ForwardSync),
            object_id=self.sync.pk,
            name="queued shard job",
            user=None,
            status=JobStatusChoices.STATUS_PENDING,
            job_id=uuid4(),
            created=timezone.now(),
            data={},
        )
        mock_enqueue.return_value = queued_job
        self.sync.set_branch_run_state(
            {
                "snapshot_selector": "latestProcessed",
                "snapshot_id": "snapshot-legacy",
                "next_plan_index": 1,
                "total_plan_items": 1,
                "plan_items": [
                    {
                        "index": 1,
                        "model": "dcim.site",
                        "label": "dcim.site shard 1",
                        "estimated_changes": 1,
                        "sync_mode": "full",
                        "status": "pending",
                        "query_name": "Forward Sites",
                        "execution_mode": "query_id",
                        "execution_value": "query-site",
                    }
                ],
            }
        )
        self.assertEqual(self.sync.execution_runs.count(), 0)

        job = enqueue_branch_stage_job(self.sync, user=None, adhoc=True)

        self.assertEqual(job, queued_job)
        self.sync.refresh_from_db()
        run = self.sync.execution_runs.order_by("-pk").first()
        self.assertIsNotNone(run)
        self.assertEqual(run.snapshot_id, "snapshot-legacy")
        step = run.steps.get(index=1, kind="stage")
        self.assertEqual(step.status, ForwardExecutionStepStatusChoices.QUEUED)
        self.assertEqual(step.job_id, queued_job.pk)

    @patch("forward_netbox.utilities.resumable_branching.Job.enqueue")
    def test_enqueue_branch_stage_job_skips_completed_history_and_stale_compat(
        self, mock_enqueue
    ):
        ForwardExecutionRun.objects.create(
            sync=self.sync,
            source=self.source,
            snapshot_selector="latestProcessed",
            snapshot_id="snapshot-complete",
            total_steps=1,
            next_step_index=2,
            status=ForwardExecutionRunStatusChoices.COMPLETED,
        )
        self.sync.set_branch_run_state(
            {
                "next_plan_index": 1,
                "total_plan_items": 1,
                "plan_items": [{"index": 1, "status": "queued"}],
            }
        )

        job = enqueue_branch_stage_job(self.sync, user=None, adhoc=True)

        self.assertIsNone(job)
        mock_enqueue.assert_not_called()
        self.sync.refresh_from_db()
        self.assertEqual(self.sync.get_branch_run_state(), {})

    def test_ensure_branch_execution_run_prunes_stale_compatibility_branch_state(
        self,
    ):
        workload = BranchWorkload(
            model_string="dcim.site",
            label="sites",
            upsert_rows=[{"name": "site-1"}],
            coalesce_fields=[["name"]],
        )
        plan = build_branch_plan([workload], max_changes_per_branch=10)
        context = {
            "snapshot_selector": "latestProcessed",
            "snapshot_id": "snapshot-1",
        }
        ForwardExecutionRun.objects.create(
            sync=self.sync,
            source=self.source,
            backend="branching",
            status=ForwardExecutionRunStatusChoices.COMPLETED,
            phase="completed",
            snapshot_selector="latestProcessed",
            snapshot_id="snapshot-old",
            total_steps=1,
            next_step_index=2,
        )
        parameters = dict(self.sync.parameters or {})
        parameters["_branch_run"] = {
            "next_plan_index": 77,
            "total_plan_items": 99,
            "phase": "stale",
        }
        self.sync.parameters = parameters
        self.sync.save(update_fields=["parameters"])

        run = ensure_branch_execution_run(
            sync=self.sync,
            context=context,
            plan=plan,
            plan_preview={"planned_shards": len(plan)},
            validation_run=None,
            job=None,
            max_changes_per_branch=10,
            auto_merge=False,
            model_change_density={},
            next_plan_index=1,
        )

        self.sync.refresh_from_db()
        self.assertEqual(run.status, ForwardExecutionRunStatusChoices.RUNNING)
        self.assertEqual(self.sync.get_branch_run_state(), {})

    def test_merge_forwardingestion_skips_duplicate_merge_without_branch(self):
        class DummyJob:
            pk = 56
            object_id = self.ingestion.pk
            user = None
            job_id = "duplicate-merge-job"

            def __init__(self):
                self.data = None
                self.log_entries = []
                self.started = None
                self.terminated_status = None

            def start(self):
                self.started = True

            def terminate(self, status=None):
                self.terminated_status = status

            def save(self, update_fields=None):
                self.saved_update_fields = update_fields

        self.ingestion.baseline_ready = True
        self.ingestion.save(update_fields=["baseline_ready"])
        job = DummyJob()

        with patch("forward_netbox.utilities.merge.merge_branch") as mock_merge:
            merge_forwardingestion(job)

        mock_merge.assert_not_called()
        self.ingestion.refresh_from_db()
        self.assertIsNone(self.ingestion.merge_job)
        self.assertIsNone(job.terminated_status)
        self.assertEqual(
            job.data["logs"][0][4],
            "Forward ingestion branch is already merged or no longer present; skipping duplicate merge job.",
        )

    @patch("forward_netbox.utilities.ingestion_merge.enqueue_branch_stage_job")
    def test_merge_forwardingestion_reconciles_completed_merge_queued_step_without_branch(
        self, mock_enqueue_stage
    ):
        class DummyJob:
            pk = 156
            object_id = self.ingestion.pk
            user = None
            job_id = "duplicate-merge-finish-job"

            def __init__(self):
                self.data = None
                self.log_entries = []
                self.started = None
                self.terminated_status = None

            def start(self):
                self.started = True

            def terminate(self, status=None):
                self.terminated_status = status

            def save(self, update_fields=None):
                self.saved_update_fields = update_fields

        self.sync.auto_merge = True
        self.sync.save(update_fields=["auto_merge"])
        execution_run = ForwardExecutionRun.objects.create(
            sync=self.sync,
            source=self.source,
            snapshot_selector="latestProcessed",
            snapshot_id="snapshot-1",
            total_steps=2,
            next_step_index=1,
            auto_merge=True,
            status="running",
        )
        prior_merge_job = Job.objects.create(
            object_type=ContentType.objects.get_for_model(ForwardIngestion),
            object_id=self.ingestion.pk,
            name="prior merge",
            user=None,
            status=JobStatusChoices.STATUS_COMPLETED,
            job_id=uuid4(),
            created=timezone.now(),
            completed=timezone.now(),
            data={},
        )
        step = ForwardExecutionStep.objects.create(
            run=execution_run,
            index=1,
            status=ForwardExecutionStepStatusChoices.MERGE_QUEUED,
            model_string="dcim.site",
            ingestion=self.ingestion,
            merge_job=prior_merge_job,
        )
        job = DummyJob()
        queued_job = SimpleNamespace(pk=999)
        mock_enqueue_stage.return_value = queued_job

        with patch("forward_netbox.utilities.merge.merge_branch") as mock_merge:
            merge_forwardingestion(job)

        mock_merge.assert_not_called()
        step.refresh_from_db()
        execution_run.refresh_from_db()
        self.assertEqual(step.status, ForwardExecutionStepStatusChoices.MERGED)
        self.assertEqual(execution_run.next_step_index, 2)
        mock_enqueue_stage.assert_called_once()
        messages = [entry[4] for entry in job.data["logs"]]
        self.assertTrue(
            any("merge_queued -> merged" in message for message in messages)
        )
        self.assertTrue(
            any("Queued next stage step job 999" in message for message in messages)
        )

    def test_merge_forwardingestion_skips_merge_claimed_by_unfinished_step(self):
        class DummyJob:
            pk = 57
            object_id = self.ingestion.pk
            user = None
            job_id = "duplicate-claimed-merge-job"

            def __init__(self):
                self.data = None
                self.log_entries = []
                self.started = None
                self.terminated_status = None

            def start(self):
                self.started = True

            def terminate(self, status=None):
                self.terminated_status = status

            def save(self, update_fields=None):
                self.saved_update_fields = update_fields

        branch = Branch.objects.create(
            name=f"claimed-merge-{uuid4().hex[:12]}",
            schema_id=f"claimed_merge_{uuid4().hex[:12]}",
        )
        self.ingestion.branch = branch
        self.ingestion.save(update_fields=["branch"])
        existing_merge_job = Job.objects.create(
            object_type=ContentType.objects.get_for_model(ForwardIngestion),
            object_id=self.ingestion.pk,
            name="existing merge",
            user=None,
            status=JobStatusChoices.STATUS_RUNNING,
            job_id=uuid4(),
            created=timezone.now(),
            started=timezone.now(),
            data={},
        )
        execution_run = ForwardExecutionRun.objects.create(
            sync=self.sync,
            source=self.source,
            snapshot_selector="latestProcessed",
            snapshot_id="snapshot-1",
            total_steps=1,
            next_step_index=1,
        )
        ForwardExecutionStep.objects.create(
            run=execution_run,
            index=1,
            status=ForwardExecutionStepStatusChoices.MERGE_QUEUED,
            model_string="dcim.site",
            ingestion=self.ingestion,
            branch=branch,
            merge_job=existing_merge_job,
        )
        job = DummyJob()

        with patch.object(ForwardIngestion, "sync_merge") as mock_sync_merge:
            merge_forwardingestion(job)

        mock_sync_merge.assert_not_called()
        self.assertIsNone(job.terminated_status)
        self.assertEqual(
            job.data["logs"][0][4],
            "Forward ingestion merge is already claimed or completed; skipping duplicate merge job.",
        )

    def test_merge_forwardingestion_timeout_uses_ledger_without_branch_run_json(
        self,
    ):
        execution_run = ForwardExecutionRun.objects.create(
            sync=self.sync,
            source=self.source,
            snapshot_selector="latestProcessed",
            snapshot_id="snapshot-1",
            total_steps=2,
            next_step_index=2,
        )
        ForwardExecutionStep.objects.create(
            run=execution_run,
            index=1,
            status=ForwardExecutionStepStatusChoices.MERGED,
            model_string="dcim.site",
            estimated_changes=1,
            ingestion=self.ingestion,
        )
        ForwardExecutionStep.objects.create(
            run=execution_run,
            index=2,
            status=ForwardExecutionStepStatusChoices.STAGED,
            model_string="dcim.device",
            estimated_changes=2,
            ingestion=self.ingestion,
        )
        branch = Branch.objects.create(
            name=f"merge-timeout-{uuid4().hex[:12]}",
            schema_id=f"merge_timeout_{uuid4().hex[:12]}",
        )
        user = get_user_model().objects.create_user(
            username=f"merge-timeout-{uuid4().hex[:12]}"
        )
        self.ingestion.branch = branch
        self.ingestion.save(update_fields=["branch"])
        branch.status = BranchStatusChoices.MERGING
        branch.save(update_fields=["status", "last_updated"])
        clear_branch_run_state(self.sync)
        initial_parameters = dict(self.sync.parameters or {})
        job = Job.objects.create(
            object_type=ContentType.objects.get_for_model(ForwardIngestion),
            object_id=self.ingestion.pk,
            name="merge timeout job",
            user=user,
            status=JobStatusChoices.STATUS_PENDING,
            job_id=uuid4(),
            created=timezone.now(),
            data={},
        )

        with patch.object(
            ForwardIngestion,
            "sync_merge",
            side_effect=JobTimeoutException("timeout"),
        ):
            merge_forwardingestion(job)

        self.sync.refresh_from_db()
        execution_run.refresh_from_db()
        step = ForwardExecutionStep.objects.get(run=execution_run, index=2)
        state = get_branch_run_display_state(self.sync)
        self.assertEqual(step.status, "merge_timeout")
        self.assertEqual(step.retry_count, 1)
        self.assertEqual(state, {})
        self.assertEqual(self.sync.parameters, initial_parameters)
        self.assertEqual(
            ForwardIngestionIssue.objects.filter(
                ingestion=self.ingestion,
                phase=ForwardIngestionPhaseChoices.MERGE,
                exception=JobTimeoutException.__name__,
            ).count(),
            1,
        )

    def test_merge_forwardingestion_timeout_auto_requeues_merge_within_budget(self):
        execution_run = ForwardExecutionRun.objects.create(
            sync=self.sync,
            source=self.source,
            snapshot_selector="latestProcessed",
            snapshot_id="snapshot-1",
            total_steps=2,
            next_step_index=2,
            auto_merge=True,
        )
        ForwardExecutionStep.objects.create(
            run=execution_run,
            index=1,
            status=ForwardExecutionStepStatusChoices.MERGED,
            model_string="dcim.site",
            estimated_changes=1,
            ingestion=self.ingestion,
        )
        step = ForwardExecutionStep.objects.create(
            run=execution_run,
            index=2,
            status=ForwardExecutionStepStatusChoices.STAGED,
            model_string="dcim.device",
            estimated_changes=2,
            ingestion=self.ingestion,
        )
        branch = Branch.objects.create(
            name=f"merge-timeout-auto-{uuid4().hex[:12]}",
            schema_id=f"merge_timeout_auto_{uuid4().hex[:12]}",
        )
        user = get_user_model().objects.create_user(
            username=f"merge-timeout-auto-{uuid4().hex[:12]}"
        )
        self.ingestion.branch = branch
        self.ingestion.save(update_fields=["branch"])
        clear_branch_run_state(self.sync)
        current_job = Job.objects.create(
            object_type=ContentType.objects.get_for_model(ForwardIngestion),
            object_id=self.ingestion.pk,
            name="merge timeout current job",
            user=user,
            status=JobStatusChoices.STATUS_PENDING,
            job_id=uuid4(),
            created=timezone.now(),
            data={},
        )
        retry_job = Job.objects.create(
            object_type=ContentType.objects.get_for_model(ForwardIngestion),
            object_id=self.ingestion.pk,
            name="merge timeout retry job",
            user=user,
            status=JobStatusChoices.STATUS_PENDING,
            job_id=uuid4(),
            created=timezone.now(),
            data={},
        )

        with (
            patch.object(
                ForwardIngestion,
                "sync_merge",
                side_effect=JobTimeoutException("timeout"),
            ),
            patch(
                "forward_netbox.utilities.ingestion_merge.Job.enqueue",
                return_value=retry_job,
            ) as mock_enqueue,
        ):
            merge_forwardingestion(current_job)

        self.sync.refresh_from_db()
        execution_run.refresh_from_db()
        self.ingestion.refresh_from_db()
        step.refresh_from_db()
        self.assertEqual(step.status, ForwardExecutionStepStatusChoices.MERGE_TIMEOUT)
        self.assertEqual(self.ingestion.merge_job_id, retry_job.pk)
        mock_enqueue.assert_called_once()

    def test_merge_forwardingestion_not_ready_auto_requeues_without_failing_sync(self):
        execution_run = ForwardExecutionRun.objects.create(
            sync=self.sync,
            source=self.source,
            snapshot_selector="latestProcessed",
            snapshot_id="snapshot-1",
            total_steps=2,
            next_step_index=2,
            auto_merge=True,
        )
        ForwardExecutionStep.objects.create(
            run=execution_run,
            index=1,
            status=ForwardExecutionStepStatusChoices.MERGED,
            model_string="dcim.site",
            estimated_changes=1,
            ingestion=self.ingestion,
        )
        step = ForwardExecutionStep.objects.create(
            run=execution_run,
            index=2,
            status=ForwardExecutionStepStatusChoices.STAGED,
            model_string="dcim.device",
            estimated_changes=2,
            ingestion=self.ingestion,
        )
        branch = Branch.objects.create(
            name=f"merge-not-ready-{uuid4().hex[:12]}",
            schema_id=f"merge_not_ready_{uuid4().hex[:12]}",
        )
        user = get_user_model().objects.create_user(
            username=f"merge-not-ready-{uuid4().hex[:12]}"
        )
        self.ingestion.branch = branch
        self.ingestion.save(update_fields=["branch"])
        clear_branch_run_state(self.sync)
        current_job = Job.objects.create(
            object_type=ContentType.objects.get_for_model(ForwardIngestion),
            object_id=self.ingestion.pk,
            name="merge not ready current job",
            user=user,
            status=JobStatusChoices.STATUS_PENDING,
            job_id=uuid4(),
            created=timezone.now(),
            data={},
        )
        retry_job = Job.objects.create(
            object_type=ContentType.objects.get_for_model(ForwardIngestion),
            object_id=self.ingestion.pk,
            name="merge not ready retry job",
            user=user,
            status=JobStatusChoices.STATUS_PENDING,
            job_id=uuid4(),
            created=timezone.now(),
            data={},
        )

        with (
            patch.object(
                ForwardIngestion,
                "sync_merge",
                side_effect=SyncError(f"Branch {branch.name} is not ready to merge"),
            ),
            patch(
                "forward_netbox.utilities.ingestion_merge.Job.enqueue",
                return_value=retry_job,
            ) as mock_enqueue,
        ):
            merge_forwardingestion(current_job)

        self.sync.refresh_from_db()
        execution_run.refresh_from_db()
        self.ingestion.refresh_from_db()
        step.refresh_from_db()
        self.assertEqual(step.status, ForwardExecutionStepStatusChoices.MERGE_TIMEOUT)
        self.assertEqual(self.ingestion.merge_job_id, retry_job.pk)
        self.assertEqual(self.sync.status, ForwardSyncStatusChoices.QUEUED)
        self.assertNotEqual(
            Branch.objects.get(pk=branch.pk).status, BranchStatusChoices.MERGING
        )
        mock_enqueue.assert_called_once()

    def test_merge_forwardingestion_uses_job_user_when_sync_user_missing(self):
        branch = Branch.objects.create(
            name=f"merge-user-{uuid4().hex[:12]}",
            schema_id=f"merge_user_{uuid4().hex[:12]}",
        )
        self.ingestion.branch = branch
        self.ingestion.save(update_fields=["branch"])
        self.sync.user = None
        self.sync.save(update_fields=["user"])
        user = get_user_model().objects.create_user(
            username=f"merge-user-{uuid4().hex[:12]}"
        )
        job = Job.objects.create(
            object_type=ContentType.objects.get_for_model(ForwardIngestion),
            object_id=self.ingestion.pk,
            name="merge user fallback job",
            user=user,
            status=JobStatusChoices.STATUS_PENDING,
            job_id=uuid4(),
            created=timezone.now(),
            data={},
        )

        def _assert_request_user(*args, **kwargs):
            request = current_request.get()
            assert request is not None
            assert request.user == user

        with patch.object(
            ForwardIngestion, "sync_merge", side_effect=_assert_request_user
        ):
            merge_forwardingestion(job)
