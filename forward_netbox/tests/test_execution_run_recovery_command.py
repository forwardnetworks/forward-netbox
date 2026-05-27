import json
from io import StringIO
from unittest.mock import patch
from uuid import uuid4

from core.choices import JobStatusChoices
from core.models import Job
from django.contrib.contenttypes.models import ContentType
from django.core.management import call_command
from django.test import TestCase
from django.utils import timezone

from forward_netbox.choices import ForwardExecutionRunStatusChoices
from forward_netbox.choices import ForwardExecutionStepKindChoices
from forward_netbox.choices import ForwardExecutionStepStatusChoices
from forward_netbox.management.commands.forward_execution_run_recovery import (
    _enqueue_recovery_job,
)
from forward_netbox.models import ForwardExecutionRun
from forward_netbox.models import ForwardExecutionStep
from forward_netbox.models import ForwardIngestion
from forward_netbox.models import ForwardSource
from forward_netbox.models import ForwardSync


class ForwardExecutionRunRecoveryCommandTest(TestCase):
    def setUp(self):
        self.source = ForwardSource.objects.create(
            name="recovery-command-source",
            type="saas",
            url="https://fwd.app",
            parameters={
                "username": "user@example.com",
                "password": "secret",
                "network_id": "network-1",
            },
        )
        self.sync = ForwardSync.objects.create(
            name="recovery-command-sync",
            source=self.source,
            parameters={"snapshot_id": "latestProcessed"},
        )

    @patch("forward_netbox.utilities.resumable_branching.Job.enqueue")
    def test_reconcile_and_enqueue_next_uses_native_stage_queue(self, mock_enqueue):
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
        run = ForwardExecutionRun.objects.create(
            sync=self.sync,
            source=self.source,
            status=ForwardExecutionRunStatusChoices.RUNNING,
            phase="queued",
            snapshot_selector="latestProcessed",
            snapshot_id="snapshot-1",
            total_steps=2,
            next_step_index=2,
        )
        ForwardExecutionStep.objects.create(
            run=run,
            index=1,
            kind=ForwardExecutionStepKindChoices.STAGE,
            status=ForwardExecutionStepStatusChoices.MERGED,
            model_string="dcim.site",
            estimated_changes=1,
        )
        step = ForwardExecutionStep.objects.create(
            run=run,
            index=2,
            kind=ForwardExecutionStepKindChoices.STAGE,
            status=ForwardExecutionStepStatusChoices.QUEUED,
            model_string="dcim.device",
            estimated_changes=2,
            shard_keys=["device:private"],
            fetch_column_filters=[
                {"columnName": "name", "operator": "EQUALS_ANY", "values": ["private"]}
            ],
        )

        stream = StringIO()
        call_command(
            "forward_execution_run_recovery",
            "--run-id",
            str(run.pk),
            "--enqueue-next",
            stdout=stream,
        )

        payload = json.loads(stream.getvalue())
        step.refresh_from_db()
        self.assertEqual(payload["reconcile"]["updated_steps"], 1)
        self.assertEqual(payload["enqueued_job"]["id"], queued_job.pk)
        self.assertEqual(payload["next_step"]["status"], "queued")
        self.assertEqual(payload["next_step"]["shard_key_count"], 1)
        self.assertEqual(payload["next_step"]["fetch_column_filter_count"], 1)
        self.assertNotIn("shard_keys", payload["next_step"])
        self.assertNotIn("fetch_column_filters", payload["next_step"])
        self.assertEqual(step.status, ForwardExecutionStepStatusChoices.QUEUED)
        self.assertEqual(step.job_id, queued_job.pk)

    def test_skip_reconcile_reports_orphaned_queued_recommendation(self):
        run = ForwardExecutionRun.objects.create(
            sync=self.sync,
            source=self.source,
            status=ForwardExecutionRunStatusChoices.RUNNING,
            phase="queued",
            snapshot_selector="latestProcessed",
            snapshot_id="snapshot-2",
            total_steps=1,
            next_step_index=1,
        )
        ForwardExecutionStep.objects.create(
            run=run,
            index=1,
            kind=ForwardExecutionStepKindChoices.STAGE,
            status=ForwardExecutionStepStatusChoices.QUEUED,
            model_string="dcim.site",
            estimated_changes=1,
        )

        stream = StringIO()
        call_command(
            "forward_execution_run_recovery",
            "--sync-name",
            self.sync.name,
            "--skip-reconcile",
            stdout=stream,
        )

        payload = json.loads(stream.getvalue())
        self.assertTrue(payload["reconcile"]["skipped"])
        self.assertEqual(payload["recovery_recommendation"]["action"], "reconcile")
        self.assertEqual(payload["next_step"]["status"], "queued")

    @patch(
        "forward_netbox.management.commands.forward_execution_run_recovery.enqueue_branch_stage_job"
    )
    @patch(
        "forward_netbox.management.commands.forward_execution_run_recovery.maybe_enqueue_next_branch_stage"
    )
    def test_enqueue_next_requeues_merge_for_staged_step(
        self, mock_maybe_enqueue_merge, mock_enqueue_stage
    ):
        merge_job = Job.objects.create(
            object_type=ContentType.objects.get_for_model(ForwardSync),
            object_id=self.sync.pk,
            name="merge job",
            user=None,
            status=JobStatusChoices.STATUS_PENDING,
            job_id=uuid4(),
            created=timezone.now(),
            data={},
        )
        mock_maybe_enqueue_merge.return_value = merge_job
        run = ForwardExecutionRun.objects.create(
            sync=self.sync,
            source=self.source,
            status=ForwardExecutionRunStatusChoices.RUNNING,
            phase="staging",
            snapshot_selector="latestProcessed",
            snapshot_id="snapshot-3",
            total_steps=1,
            next_step_index=1,
            auto_merge=True,
        )
        ingestion = ForwardIngestion.objects.create(sync=self.sync)
        step = ForwardExecutionStep.objects.create(
            run=run,
            index=1,
            kind=ForwardExecutionStepKindChoices.STAGE,
            status=ForwardExecutionStepStatusChoices.STAGED,
            model_string="ipam.prefix",
            estimated_changes=10,
            ingestion=ingestion,
        )

        stream = StringIO()
        call_command(
            "forward_execution_run_recovery",
            "--run-id",
            str(run.pk),
            "--enqueue-next",
            stdout=stream,
        )

        payload = json.loads(stream.getvalue())
        step.refresh_from_db()
        self.assertIn(
            payload["recovery_recommendation"]["action"],
            {"requeue_merge", "wait_for_review"},
        )
        self.assertEqual(payload["enqueued_job"]["id"], merge_job.pk)
        mock_maybe_enqueue_merge.assert_called_once_with(
            ingestion,
            user=None,
            allow_failed_recovery=True,
        )
        mock_enqueue_stage.assert_not_called()

    def test_reconcile_reopens_failed_run_with_active_step(self):
        merge_job = Job.objects.create(
            object_type=ContentType.objects.get_for_model(ForwardIngestion),
            object_id=self.sync.pk,
            name="active merge job",
            user=None,
            status=JobStatusChoices.STATUS_PENDING,
            job_id=uuid4(),
            created=timezone.now(),
            data={},
        )
        run = ForwardExecutionRun.objects.create(
            sync=self.sync,
            source=self.source,
            status=ForwardExecutionRunStatusChoices.FAILED,
            phase="failed",
            snapshot_selector="latestProcessed",
            snapshot_id="snapshot-4",
            total_steps=1,
            next_step_index=1,
            auto_merge=True,
        )
        ingestion = ForwardIngestion.objects.create(sync=self.sync)
        ForwardExecutionStep.objects.create(
            run=run,
            index=1,
            kind=ForwardExecutionStepKindChoices.STAGE,
            status=ForwardExecutionStepStatusChoices.MERGE_QUEUED,
            model_string="ipam.prefix",
            ingestion=ingestion,
            merge_job=merge_job,
        )

        stream = StringIO()
        call_command(
            "forward_execution_run_recovery",
            "--run-id",
            str(run.pk),
            stdout=stream,
        )

        payload = json.loads(stream.getvalue())
        run.refresh_from_db()
        self.assertEqual(run.status, ForwardExecutionRunStatusChoices.RUNNING)
        self.assertEqual(
            payload["run"]["status"], ForwardExecutionRunStatusChoices.RUNNING
        )
        self.assertEqual(payload["run"]["phase"], "queued_merge")

    @patch(
        "forward_netbox.management.commands.forward_execution_run_recovery.enqueue_branch_stage_job"
    )
    @patch(
        "forward_netbox.management.commands.forward_execution_run_recovery.maybe_enqueue_next_branch_stage"
    )
    def test_enqueue_recovery_job_prefers_auto_merge_path_even_for_manual_recommendation(
        self, mock_maybe_enqueue_merge, mock_enqueue_stage
    ):
        merge_job = Job.objects.create(
            object_type=ContentType.objects.get_for_model(ForwardIngestion),
            object_id=self.sync.pk,
            name="merge job manual recommendation",
            user=None,
            status=JobStatusChoices.STATUS_PENDING,
            job_id=uuid4(),
            created=timezone.now(),
            data={},
        )
        mock_maybe_enqueue_merge.return_value = merge_job
        run = ForwardExecutionRun.objects.create(
            sync=self.sync,
            source=self.source,
            status=ForwardExecutionRunStatusChoices.WAITING,
            phase="waiting_merge",
            snapshot_selector="latestProcessed",
            snapshot_id="snapshot-5",
            total_steps=1,
            next_step_index=1,
            auto_merge=True,
        )
        ingestion = ForwardIngestion.objects.create(sync=self.sync)
        ForwardExecutionStep.objects.create(
            run=run,
            index=1,
            kind=ForwardExecutionStepKindChoices.STAGE,
            status=ForwardExecutionStepStatusChoices.STAGED,
            model_string="ipam.prefix",
            ingestion=ingestion,
        )

        queued = _enqueue_recovery_job(run, {"action": "manual_intervention"})
        self.assertEqual(queued.pk, merge_job.pk)
        mock_maybe_enqueue_merge.assert_called_once_with(
            ingestion,
            user=None,
            allow_failed_recovery=True,
        )
        mock_enqueue_stage.assert_not_called()
