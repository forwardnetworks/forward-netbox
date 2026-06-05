import json
from io import StringIO
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


class ForwardWarningAuditCommandTest(TestCase):
    def setUp(self):
        self.source = ForwardSource.objects.create(
            name="warning-audit-source",
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
            name="warning-audit-sync",
            source=self.source,
            parameters={"snapshot_id": "latestProcessed"},
        )

    def _job(self, *, ingestion, name, logs):
        now = timezone.now()
        return Job.objects.create(
            object_type=ContentType.objects.get_for_model(ForwardIngestion),
            object_id=ingestion.pk,
            name=name,
            user=None,
            status=JobStatusChoices.STATUS_COMPLETED,
            job_id=uuid4(),
            created=now,
            started=now,
            completed=now,
            data={"logs": logs, "statistics": {}},
        )

    def test_reports_warning_and_error_counts(self):
        ingestion = ForwardIngestion.objects.create(
            sync=self.sync,
            snapshot_selector="latestProcessed",
            snapshot_id="snapshot-1",
        )
        ingestion.job = self._job(
            ingestion=ingestion,
            name="warning-audit-sync-job",
            logs=[
                [
                    timezone.now().isoformat(),
                    "warning",
                    self.sync.name,
                    "/plugins/forward/sync/1/",
                    "query fallback warning",
                ],
                [
                    timezone.now().isoformat(),
                    "failure",
                    self.sync.name,
                    "/plugins/forward/sync/1/",
                    "hard failure",
                ],
            ],
        )
        ingestion.merge_job = self._job(
            ingestion=ingestion,
            name="warning-audit-merge-job",
            logs=[
                [
                    timezone.now().isoformat(),
                    "info",
                    self.sync.name,
                    "/plugins/forward/sync/1/",
                    "merge completed",
                ]
            ],
        )
        ingestion.save(update_fields=["job", "merge_job"])

        stream = StringIO()
        call_command("forward_warning_audit", "--sync-id", self.sync.pk, stdout=stream)
        payload = json.loads(stream.getvalue())

        self.assertEqual(payload["warning_count"], 1)
        self.assertEqual(payload["error_count"], 1)
        self.assertEqual(payload["levels"]["warning"], 1)
        self.assertEqual(payload["levels"]["failure"], 1)
        self.assertEqual(payload["levels"]["info"], 1)

    def test_fail_on_warning_raises(self):
        ingestion = ForwardIngestion.objects.create(
            sync=self.sync,
            snapshot_selector="latestProcessed",
            snapshot_id="snapshot-2",
        )
        ingestion.job = self._job(
            ingestion=ingestion,
            name="warning-audit-warning-job",
            logs=[
                [
                    timezone.now().isoformat(),
                    "warning",
                    self.sync.name,
                    "/plugins/forward/sync/2/",
                    "warn only",
                ]
            ],
        )
        ingestion.save(update_fields=["job"])

        with self.assertRaises(CommandError):
            call_command(
                "forward_warning_audit",
                "--sync-id",
                self.sync.pk,
                "--fail-on-warning",
                stdout=StringIO(),
            )

    def test_suppresses_non_actionable_diff_fallback_warning(self):
        ingestion = ForwardIngestion.objects.create(
            sync=self.sync,
            snapshot_selector="latestProcessed",
            snapshot_id="snapshot-3",
        )
        ingestion.job = self._job(
            ingestion=ingestion,
            name="warning-audit-benign-fallback-job",
            logs=[
                [
                    timezone.now().isoformat(),
                    "warning",
                    self.sync.name,
                    "/plugins/forward/sync/3/",
                    (
                        "Forward diffs require a newer processed snapshot than the latest baseline; "
                        "baseline ingestion `706` already matches snapshot `1296934`, "
                        "so running full query execution for ipam.prefix instead."
                    ),
                ]
            ],
        )
        ingestion.save(update_fields=["job"])

        stream = StringIO()
        call_command("forward_warning_audit", "--sync-id", self.sync.pk, stdout=stream)
        payload = json.loads(stream.getvalue())

        self.assertEqual(payload["warning_count"], 0)
        self.assertEqual(payload["raw_warning_count"], 1)
        self.assertEqual(payload["suppressed_warning_count"], 1)
        self.assertEqual(len(payload["top_suppressed_warnings"]), 1)

        # fail-on-warning should not trip for suppressed informational fallback warnings
        call_command(
            "forward_warning_audit",
            "--sync-id",
            self.sync.pk,
            "--fail-on-warning",
            stdout=StringIO(),
        )

    def test_suppresses_non_actionable_merge_requeue_warning(self):
        ingestion = ForwardIngestion.objects.create(
            sync=self.sync,
            snapshot_selector="latestProcessed",
            snapshot_id="snapshot-4",
        )
        ingestion.job = self._job(
            ingestion=ingestion,
            name="warning-audit-benign-merge-requeue-job",
            logs=[
                [
                    timezone.now().isoformat(),
                    "warning",
                    self.sync.name,
                    "/plugins/forward/sync/4/",
                    "Merge job hit a transient Branching readiness guard; attempting automatic requeue.",
                ]
            ],
        )
        ingestion.save(update_fields=["job"])

        stream = StringIO()
        call_command("forward_warning_audit", "--sync-id", self.sync.pk, stdout=stream)
        payload = json.loads(stream.getvalue())

        self.assertEqual(payload["warning_count"], 0)
        self.assertEqual(payload["raw_warning_count"], 1)
        self.assertEqual(payload["suppressed_warning_count"], 1)
        self.assertEqual(len(payload["top_suppressed_warnings"]), 1)

        # fail-on-warning should not trip for suppressed recovery warnings
        call_command(
            "forward_warning_audit",
            "--sync-id",
            self.sync.pk,
            "--fail-on-warning",
            stdout=StringIO(),
        )

    def test_suppresses_non_actionable_claimed_shard_reconciliation_warning(self):
        ingestion = ForwardIngestion.objects.create(
            sync=self.sync,
            snapshot_selector="latestProcessed",
            snapshot_id="snapshot-5",
        )
        ingestion.job = self._job(
            ingestion=ingestion,
            name="warning-audit-benign-claimed-shard-warning-job",
            logs=[
                [
                    timezone.now().isoformat(),
                    "warning",
                    self.sync.name,
                    "/plugins/forward/sync/5/",
                    (
                        "Execution context returned a different shard index than claimed; "
                        "executing claimed shard 15 instead of loaded index 14."
                    ),
                ]
            ],
        )
        ingestion.save(update_fields=["job"])

        stream = StringIO()
        call_command("forward_warning_audit", "--sync-id", self.sync.pk, stdout=stream)
        payload = json.loads(stream.getvalue())

        self.assertEqual(payload["warning_count"], 0)
        self.assertEqual(payload["raw_warning_count"], 1)
        self.assertEqual(payload["suppressed_warning_count"], 1)
        self.assertEqual(len(payload["top_suppressed_warnings"]), 1)

        call_command(
            "forward_warning_audit",
            "--sync-id",
            self.sync.pk,
            "--fail-on-warning",
            stdout=StringIO(),
        )

    def test_reclassifies_transient_partition_retry_warnings_as_info(self):
        ingestion = ForwardIngestion.objects.create(
            sync=self.sync,
            snapshot_selector="latestProcessed",
            snapshot_id="snapshot-7",
        )
        ingestion.job = self._job(
            ingestion=ingestion,
            name="warning-audit-transient-partition-retry-job",
            logs=[
                [
                    timezone.now().isoformat(),
                    "warning",
                    self.sync.name,
                    "/plugins/forward/sync/7/",
                    (
                        "ipam.prefix full partition fetch failed; retrying as 2 smaller "
                        "partition(s): Forward API request returned transient HTTP 503; "
                        "retry attempts were exhausted."
                    ),
                ],
                [
                    timezone.now().isoformat(),
                    "warning",
                    self.sync.name,
                    "/plugins/forward/sync/7/",
                    (
                        "ipam.prefix full single-value partition fetch failed; retrying "
                        "with alternate column-filter operator before full fallback: "
                        "Forward API request returned transient HTTP 504; retry attempts "
                        "were exhausted."
                    ),
                ],
            ],
        )
        ingestion.save(update_fields=["job"])

        stream = StringIO()
        call_command("forward_warning_audit", "--sync-id", self.sync.pk, stdout=stream)
        payload = json.loads(stream.getvalue())

        self.assertEqual(payload["warning_count"], 0)
        self.assertEqual(payload["raw_warning_count"], 0)
        self.assertEqual(payload["suppressed_warning_count"], 0)
        self.assertEqual(payload["levels"]["info"], 2)
        self.assertEqual(payload["top_warnings"], [])
        self.assertEqual(payload["top_suppressed_warnings"], [])

    def test_requires_exactly_one_sync_selector(self):
        with self.assertRaises(CommandError):
            call_command(
                "forward_warning_audit",
                "--sync-id",
                self.sync.pk,
                "--sync-name",
                self.sync.name,
                stdout=StringIO(),
            )

    def test_includes_execution_run_step_jobs(self):
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
        now = timezone.now()
        stage_job = Job.objects.create(
            object_type=ContentType.objects.get_for_model(ForwardSync),
            object_id=self.sync.pk,
            name="warning-audit-stage-job",
            user=None,
            status=JobStatusChoices.STATUS_RUNNING,
            job_id=uuid4(),
            created=now,
            started=now,
            data={
                "logs": [
                    [
                        timezone.now().isoformat(),
                        "warning",
                        self.sync.name,
                        "/plugins/forward/sync/1/",
                        "execution-stage warning",
                    ],
                    [
                        timezone.now().isoformat(),
                        "info",
                        self.sync.name,
                        "/plugins/forward/sync/1/",
                        "execution-stage info",
                    ],
                ],
                "statistics": {},
            },
        )
        ForwardExecutionStep.objects.create(
            run=run,
            index=1,
            kind="stage",
            status="running",
            model_string="ipam.prefix",
            job=stage_job,
        )

        stream = StringIO()
        call_command("forward_warning_audit", "--sync-id", self.sync.pk, stdout=stream)
        payload = json.loads(stream.getvalue())

        self.assertEqual(payload["warning_count"], 1)
        self.assertEqual(payload["levels"]["warning"], 1)
        self.assertEqual(payload["levels"]["info"], 1)
        self.assertIn(stage_job.pk, payload["execution_run_job_ids"])

    def test_uses_live_log_entries_when_job_data_logs_absent(self):
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
        now = timezone.now()
        stage_job = Job.objects.create(
            object_type=ContentType.objects.get_for_model(ForwardSync),
            object_id=self.sync.pk,
            name="warning-audit-live-log-entries-job",
            user=None,
            status=JobStatusChoices.STATUS_RUNNING,
            job_id=uuid4(),
            created=now,
            started=now,
            data={},
            log_entries=[
                {
                    "timestamp": timezone.now(),
                    "level": "warning",
                    "message": "live warning",
                },
                {
                    "timestamp": timezone.now(),
                    "level": "info",
                    "message": "live info",
                },
            ],
        )
        ForwardExecutionStep.objects.create(
            run=run,
            index=1,
            kind="stage",
            status="running",
            model_string="ipam.prefix",
            job=stage_job,
        )

        stream = StringIO()
        call_command("forward_warning_audit", "--sync-id", self.sync.pk, stdout=stream)
        payload = json.loads(stream.getvalue())

        self.assertEqual(payload["warning_count"], 1)
        self.assertEqual(payload["levels"]["warning"], 1)
        self.assertEqual(payload["levels"]["info"], 1)
        self.assertIn(stage_job.pk, payload["execution_run_job_ids"])

    def test_includes_sync_jobs_without_ingestions(self):
        now = timezone.now()
        sync_job = Job.objects.create(
            object_type=ContentType.objects.get_for_model(ForwardSync),
            object_id=self.sync.pk,
            name="warning-audit-sync-job",
            user=None,
            status=JobStatusChoices.STATUS_RUNNING,
            job_id=uuid4(),
            created=now,
            started=now,
            data={},
            log_entries=[
                {
                    "timestamp": timezone.now(),
                    "level": "warning",
                    "message": "sync-level warning",
                }
            ],
        )

        stream = StringIO()
        call_command("forward_warning_audit", "--sync-id", self.sync.pk, stdout=stream)
        payload = json.loads(stream.getvalue())

        self.assertEqual(payload["warning_count"], 1)
        self.assertIn(sync_job.pk, payload["sync_job_ids"])

    def test_default_scope_excludes_stale_execution_step_warnings(self):
        older_ingestion = ForwardIngestion.objects.create(
            sync=self.sync,
            snapshot_selector="latestProcessed",
            snapshot_id="snapshot-old",
        )
        latest_ingestion = ForwardIngestion.objects.create(
            sync=self.sync,
            snapshot_selector="latestProcessed",
            snapshot_id="snapshot-latest",
        )
        run = ForwardExecutionRun.objects.create(
            sync=self.sync,
            source=self.source,
            snapshot_selector="latestProcessed",
            snapshot_id="snapshot-latest",
            status="running",
            phase="staging",
            next_step_index=2,
            total_steps=2,
        )
        now = timezone.now()
        stale_warning_job = Job.objects.create(
            object_type=ContentType.objects.get_for_model(ForwardSync),
            object_id=self.sync.pk,
            name="warning-audit-stale-step",
            user=None,
            status=JobStatusChoices.STATUS_COMPLETED,
            job_id=uuid4(),
            created=now,
            started=now,
            completed=now,
            data={
                "logs": [
                    [
                        timezone.now().isoformat(),
                        "warning",
                        self.sync.name,
                        "/plugins/forward/sync/1/",
                        "stale execution-step warning",
                    ]
                ],
                "statistics": {},
            },
        )
        clean_latest_job = Job.objects.create(
            object_type=ContentType.objects.get_for_model(ForwardSync),
            object_id=self.sync.pk,
            name="warning-audit-latest-step",
            user=None,
            status=JobStatusChoices.STATUS_RUNNING,
            job_id=uuid4(),
            created=now,
            started=now,
            data={
                "logs": [
                    [
                        timezone.now().isoformat(),
                        "info",
                        self.sync.name,
                        "/plugins/forward/sync/1/",
                        "latest execution-step info",
                    ]
                ],
                "statistics": {},
            },
        )
        ForwardExecutionStep.objects.create(
            run=run,
            index=1,
            kind="stage",
            status="completed",
            model_string="dcim.device",
            ingestion=older_ingestion,
            job=stale_warning_job,
        )
        ForwardExecutionStep.objects.create(
            run=run,
            index=2,
            kind="stage",
            status="running",
            model_string="dcim.interface",
            ingestion=latest_ingestion,
            job=clean_latest_job,
        )

        stream = StringIO()
        call_command("forward_warning_audit", "--sync-id", self.sync.pk, stdout=stream)
        payload = json.loads(stream.getvalue())

        self.assertEqual(payload["ingestion_ids"], [latest_ingestion.pk])
        self.assertEqual(payload["warning_count"], 0)
        self.assertNotIn(stale_warning_job.pk, payload["execution_run_job_ids"])
        self.assertIn(clean_latest_job.pk, payload["execution_run_job_ids"])

    def test_all_ingestions_scope_includes_stale_execution_step_warnings(self):
        older_ingestion = ForwardIngestion.objects.create(
            sync=self.sync,
            snapshot_selector="latestProcessed",
            snapshot_id="snapshot-old",
        )
        latest_ingestion = ForwardIngestion.objects.create(
            sync=self.sync,
            snapshot_selector="latestProcessed",
            snapshot_id="snapshot-latest",
        )
        run = ForwardExecutionRun.objects.create(
            sync=self.sync,
            source=self.source,
            snapshot_selector="latestProcessed",
            snapshot_id="snapshot-latest",
            status="running",
            phase="staging",
            next_step_index=2,
            total_steps=2,
        )
        now = timezone.now()
        stale_warning_job = Job.objects.create(
            object_type=ContentType.objects.get_for_model(ForwardSync),
            object_id=self.sync.pk,
            name="warning-audit-stale-step-all-ingestions",
            user=None,
            status=JobStatusChoices.STATUS_COMPLETED,
            job_id=uuid4(),
            created=now,
            started=now,
            completed=now,
            data={
                "logs": [
                    [
                        timezone.now().isoformat(),
                        "warning",
                        self.sync.name,
                        "/plugins/forward/sync/1/",
                        "stale execution-step warning",
                    ]
                ],
                "statistics": {},
            },
        )
        ForwardExecutionStep.objects.create(
            run=run,
            index=1,
            kind="stage",
            status="completed",
            model_string="dcim.device",
            ingestion=older_ingestion,
            job=stale_warning_job,
        )
        ForwardExecutionStep.objects.create(
            run=run,
            index=2,
            kind="stage",
            status="running",
            model_string="dcim.interface",
            ingestion=latest_ingestion,
        )

        stream = StringIO()
        call_command(
            "forward_warning_audit",
            "--sync-id",
            self.sync.pk,
            "--all-ingestions",
            stdout=stream,
        )
        payload = json.loads(stream.getvalue())

        self.assertEqual(payload["warning_count"], 1)
        self.assertIn(stale_warning_job.pk, payload["execution_run_job_ids"])
