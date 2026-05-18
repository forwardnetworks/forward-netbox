from datetime import datetime
from types import SimpleNamespace
from unittest.mock import patch
from uuid import uuid4

from core.choices import JobStatusChoices
from core.models import Job
from dcim.models import Site
from django.contrib.auth import get_user_model
from django.contrib.contenttypes.models import ContentType
from django.test import TestCase
from django.utils import timezone
from netbox.context import current_request
from netbox_branching.models import Branch
from rq.timeouts import JobTimeoutException

from forward_netbox.choices import ForwardExecutionStepStatusChoices
from forward_netbox.choices import ForwardIngestionPhaseChoices
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
        self.assertEqual(state["plan_items"][0]["status"], "timeout")
        self.assertEqual(state["plan_items"][0]["retry_count"], 2)
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
        self.assertEqual(state["plan_items"][1]["status"], "merge_timeout")
        self.assertEqual(state["plan_items"][1]["retry_count"], 1)
        self.assertEqual(self.sync.parameters, initial_parameters)
        self.assertEqual(
            ForwardIngestionIssue.objects.filter(
                ingestion=self.ingestion,
                phase=ForwardIngestionPhaseChoices.MERGE,
                exception=JobTimeoutException.__name__,
            ).count(),
            1,
        )
