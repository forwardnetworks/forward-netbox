import json
from io import StringIO
from unittest.mock import patch
from uuid import uuid4

from core.choices import JobStatusChoices
from core.models import Job
from django.contrib.contenttypes.models import ContentType
from django.core.management import call_command
from django.core.management.base import CommandError
from django.test import TestCase
from django.utils import timezone

from forward_netbox.models import ForwardExecutionRun
from forward_netbox.models import ForwardExecutionStep
from forward_netbox.models import ForwardIngestion
from forward_netbox.models import ForwardSource
from forward_netbox.models import ForwardSync


class ForwardWatchSyncCommandTest(TestCase):
    def setUp(self):
        self.source = ForwardSource.objects.create(
            name="watch-sync-source",
            type="saas",
            url="https://fwd.app",
            status="ready",
            parameters={
                "username": "user@example.com",
                "password": "secret",
                "verify": True,
                "network_id": "test-network",
            },
        )
        self.sync = ForwardSync.objects.create(
            name="watch-sync",
            source=self.source,
            status="completed",
            parameters={"snapshot_id": "latestProcessed"},
        )

    def _create_ingestion(self):
        return ForwardIngestion.objects.create(
            sync=self.sync,
            snapshot_selector="latestProcessed",
            snapshot_id="snapshot-1",
            sync_mode="full",
            baseline_ready=True,
            applied_change_count=10,
            created_change_count=1,
            updated_change_count=9,
            deleted_change_count=0,
            failed_change_count=0,
        )

    def test_outputs_summary_for_terminal_sync(self):
        ingestion = self._create_ingestion()
        stream = StringIO()
        call_command(
            "forward_watch_sync",
            "--sync-id",
            self.sync.pk,
            "--max-polls",
            "1",
            stdout=stream,
        )
        payload = json.loads(stream.getvalue())
        self.assertEqual(payload["sync_status"], "completed")
        self.assertEqual(payload["ingestion"]["id"], ingestion.pk)
        self.assertEqual(payload["ingestion"]["blocking_issue_count"], 0)
        self.assertIn("job", payload)
        self.assertIn("latest_log_age_seconds", payload["job"])
        self.assertIn("execution_run", payload)

    def test_fail_on_blocking_issues(self):
        ingestion = self._create_ingestion()
        ingestion.issues.create(
            model="dcim.device",
            message="blocking row",
            exception="ForwardSyncDataError",
        )
        with self.assertRaises(CommandError):
            call_command(
                "forward_watch_sync",
                "--sync-id",
                self.sync.pk,
                "--max-polls",
                "1",
                "--fail-on-blocking",
            )

    def test_max_polls_raises_when_sync_not_terminal(self):
        self.sync.status = "syncing"
        self.sync.save(update_fields=["status"])
        self._create_ingestion()
        with patch("time.sleep", return_value=None):
            with self.assertRaises(CommandError):
                call_command(
                    "forward_watch_sync",
                    "--sync-id",
                    self.sync.pk,
                    "--max-polls",
                    "1",
                    "--interval-seconds",
                    "1",
                )

    def test_allow_nonterminal_exits_cleanly_on_max_polls(self):
        self.sync.status = "syncing"
        self.sync.save(update_fields=["status"])
        self._create_ingestion()
        stream = StringIO()
        with patch("time.sleep", return_value=None):
            call_command(
                "forward_watch_sync",
                "--sync-id",
                self.sync.pk,
                "--max-polls",
                "1",
                "--interval-seconds",
                "1",
                "--allow-nonterminal",
                stdout=stream,
            )
        payload = json.loads(stream.getvalue())
        self.assertEqual(payload["sync_status"], "syncing")

    def test_no_ingestion_still_reports_latest_sync_job(self):
        self.sync.status = "syncing"
        self.sync.save(update_fields=["status"])
        now = timezone.now()
        job = Job.objects.create(
            object_type=ContentType.objects.get_for_model(ForwardSync),
            object_id=self.sync.pk,
            name="watch-sync-active-job",
            user=None,
            status=JobStatusChoices.STATUS_RUNNING,
            job_id=uuid4(),
            created=now,
            started=now,
            data={},
            log_entries=[
                {
                    "timestamp": now,
                    "level": "info",
                    "message": "planning",
                }
            ],
        )
        stream = StringIO()
        with patch("time.sleep", return_value=None):
            call_command(
                "forward_watch_sync",
                "--sync-id",
                self.sync.pk,
                "--max-polls",
                "1",
                "--allow-nonterminal",
                stdout=stream,
            )
        payload = json.loads(stream.getvalue())
        self.assertIsNone(payload["ingestion"])
        self.assertEqual(payload["job"]["id"], job.pk)
        self.assertEqual(payload["job"]["status"], "running")

    def test_running_execution_run_prevents_terminal_exit_when_sync_is_queued(self):
        self.sync.status = "queued"
        self.sync.save(update_fields=["status"])
        self._create_ingestion()
        run = ForwardExecutionRun.objects.create(
            sync=self.sync,
            source=self.source,
            snapshot_selector="latestProcessed",
            snapshot_id="snapshot-1",
            status="running",
            phase="staging",
            next_step_index=1,
            total_steps=3,
        )
        ForwardExecutionStep.objects.create(
            run=run,
            index=1,
            kind="stage",
            status="running",
            model_string="ipam.prefix",
        )
        with patch("time.sleep", return_value=None):
            with self.assertRaises(CommandError):
                call_command(
                    "forward_watch_sync",
                    "--sync-id",
                    self.sync.pk,
                    "--max-polls",
                    "1",
                    "--interval-seconds",
                    "1",
                )

    def test_execution_run_summary_includes_active_step(self):
        self.sync.status = "queued"
        self.sync.save(update_fields=["status"])
        ingestion = self._create_ingestion()
        run = ForwardExecutionRun.objects.create(
            sync=self.sync,
            source=self.source,
            snapshot_selector="latestProcessed",
            snapshot_id="snapshot-1",
            status="running",
            phase="staging",
            next_step_index=1,
            total_steps=3,
        )
        step = ForwardExecutionStep.objects.create(
            run=run,
            index=1,
            kind="stage",
            status="running",
            model_string="ipam.prefix",
        )
        stream = StringIO()
        with patch("time.sleep", return_value=None):
            call_command(
                "forward_watch_sync",
                "--sync-id",
                self.sync.pk,
                "--max-polls",
                "1",
                "--allow-nonterminal",
                stdout=stream,
            )
        payload = json.loads(stream.getvalue())
        self.assertEqual(payload["ingestion"]["id"], ingestion.pk)
        self.assertEqual(payload["execution_run"]["status"], "running")
        self.assertEqual(payload["execution_run"]["active_step"]["id"], step.pk)
        self.assertEqual(
            payload["execution_run"]["active_step"]["model"], "ipam.prefix"
        )
        self.assertIn("job_live", payload["execution_run"]["active_step"])
        self.assertIn("attempted_row_count", payload["execution_run"]["active_step"])
        self.assertIn("applied_row_count", payload["execution_run"]["active_step"])
        self.assertIn("fetched_row_count", payload["execution_run"]["active_step"])

    def test_prefers_active_execution_step_job_for_runtime_summary(self):
        self.sync.status = "syncing"
        self.sync.save(update_fields=["status"])
        ingestion = self._create_ingestion()
        stale_ingestion_job = Job.objects.create(
            object_type=ContentType.objects.get_for_model(ForwardIngestion),
            object_id=ingestion.pk,
            name="stale-ingestion-job",
            user=None,
            status=JobStatusChoices.STATUS_COMPLETED,
            job_id=uuid4(),
            created=timezone.now(),
            started=timezone.now(),
            completed=timezone.now(),
            data={},
            log_entries=[],
        )
        ingestion.job = stale_ingestion_job
        ingestion.save(update_fields=["job"])
        run = ForwardExecutionRun.objects.create(
            sync=self.sync,
            source=self.source,
            snapshot_selector="latestProcessed",
            snapshot_id="snapshot-1",
            status="running",
            phase="staging",
            next_step_index=1,
            total_steps=3,
        )
        active_job = Job.objects.create(
            object_type=ContentType.objects.get_for_model(ForwardSync),
            object_id=self.sync.pk,
            name="active-shard-job",
            user=None,
            status=JobStatusChoices.STATUS_RUNNING,
            job_id=uuid4(),
            created=timezone.now(),
            started=timezone.now(),
            data={},
            log_entries=[
                {
                    "timestamp": timezone.now(),
                    "level": "info",
                    "message": "active shard running",
                }
            ],
        )
        ForwardExecutionStep.objects.create(
            run=run,
            index=1,
            kind="stage",
            status="running",
            model_string="ipam.prefix",
            job=active_job,
        )

        stream = StringIO()
        with patch("time.sleep", return_value=None):
            call_command(
                "forward_watch_sync",
                "--sync-id",
                self.sync.pk,
                "--max-polls",
                "1",
                "--allow-nonterminal",
                stdout=stream,
            )
        payload = json.loads(stream.getvalue())
        self.assertEqual(payload["job"]["id"], active_job.pk)
        self.assertEqual(payload["job"]["status"], "running")

    def test_reconciles_when_active_step_job_is_not_live(self):
        self.sync.status = "syncing"
        self.sync.save(update_fields=["status"])
        self._create_ingestion()
        run = ForwardExecutionRun.objects.create(
            sync=self.sync,
            source=self.source,
            snapshot_selector="latestProcessed",
            snapshot_id="snapshot-1",
            status="running",
            phase="staging",
            next_step_index=1,
            total_steps=1,
        )
        dead_job = Job.objects.create(
            object_type=ContentType.objects.get_for_model(ForwardSync),
            object_id=self.sync.pk,
            name="dead-active-job",
            user=None,
            status=JobStatusChoices.STATUS_RUNNING,
            job_id=uuid4(),
            created=timezone.now(),
            started=timezone.now(),
            completed=None,
            data={},
        )
        ForwardExecutionStep.objects.create(
            run=run,
            index=1,
            kind="stage",
            status="running",
            model_string="ipam.prefix",
            job=dead_job,
        )

        with (
            patch(
                "forward_netbox.management.commands.forward_watch_sync.job_has_live_execution",
                return_value=False,
            ),
            patch(
                "forward_netbox.management.commands.forward_watch_sync.reconcile_execution_run"
            ) as reconcile,
            patch("time.sleep", return_value=None),
        ):
            call_command(
                "forward_watch_sync",
                "--sync-id",
                self.sync.pk,
                "--max-polls",
                "1",
                "--allow-nonterminal",
            )

        reconcile.assert_called_once()

    def test_auto_enqueues_stage_when_reconciled_step_is_pending_without_job(self):
        self.sync.status = "syncing"
        self.sync.save(update_fields=["status"])
        self._create_ingestion()
        run = ForwardExecutionRun.objects.create(
            sync=self.sync,
            source=self.source,
            snapshot_selector="latestProcessed",
            snapshot_id="snapshot-1",
            status="running",
            phase="staging",
            next_step_index=1,
            total_steps=1,
        )
        ForwardExecutionStep.objects.create(
            run=run,
            index=1,
            kind="stage",
            status="pending",
            model_string="ipam.prefix",
            job=None,
            merge_job=None,
        )

        with (
            patch(
                "forward_netbox.management.commands.forward_watch_sync.enqueue_branch_stage_job"
            ) as enqueue,
            patch("time.sleep", return_value=None),
        ):
            call_command(
                "forward_watch_sync",
                "--sync-id",
                self.sync.pk,
                "--max-polls",
                "1",
                "--allow-nonterminal",
            )

        enqueue.assert_called_once_with(self.sync, user=None, adhoc=True)

    def test_does_not_auto_enqueue_when_another_stage_is_inflight(self):
        self.sync.status = "syncing"
        self.sync.save(update_fields=["status"])
        self._create_ingestion()
        run = ForwardExecutionRun.objects.create(
            sync=self.sync,
            source=self.source,
            snapshot_selector="latestProcessed",
            snapshot_id="snapshot-1",
            status="running",
            phase="staging",
            next_step_index=1,
            total_steps=2,
        )
        ForwardExecutionStep.objects.create(
            run=run,
            index=1,
            kind="stage",
            status="pending",
            model_string="ipam.prefix",
            job=None,
            merge_job=None,
        )
        ForwardExecutionStep.objects.create(
            run=run,
            index=2,
            kind="stage",
            status="running",
            model_string="ipam.prefix",
            job=None,
            merge_job=None,
        )

        with (
            patch(
                "forward_netbox.management.commands.forward_watch_sync.enqueue_branch_stage_job"
            ) as enqueue,
            patch("time.sleep", return_value=None),
        ):
            call_command(
                "forward_watch_sync",
                "--sync-id",
                self.sync.pk,
                "--max-polls",
                "1",
                "--allow-nonterminal",
            )

        enqueue.assert_not_called()
