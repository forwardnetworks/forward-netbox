import json
from io import StringIO

from django.core.management import call_command
from django.core.management.base import CommandError
from django.test import TestCase
from netbox_branching.models import Branch

from forward_netbox.choices import ForwardExecutionRunStatusChoices
from forward_netbox.choices import ForwardExecutionStepStatusChoices
from forward_netbox.models import ForwardExecutionRun
from forward_netbox.models import ForwardExecutionStep
from forward_netbox.models import ForwardIngestion
from forward_netbox.models import ForwardSource
from forward_netbox.models import ForwardSync


class ForwardBlockerAuditCommandTest(TestCase):
    def setUp(self):
        self.source = ForwardSource.objects.create(
            name="blocker-audit-source",
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
            name="blocker-audit-sync",
            source=self.source,
            parameters={"snapshot_id": "latestProcessed"},
        )

    def test_reports_blocking_and_non_blocking_issue_counts(self):
        ingestion = ForwardIngestion.objects.create(
            sync=self.sync,
            snapshot_selector="latestProcessed",
            snapshot_id="snapshot-1",
            baseline_ready=True,
        )
        ingestion.issues.create(
            model="ipam.ipaddress",
            message="Skipping delete for `ipam.ipaddress` due to protected dependencies.",
            exception="ForwardDependencySkipError",
        )
        ingestion.issues.create(
            model="dcim.device",
            message="Unable to apply device row.",
            exception="ForwardSyncDataError",
        )

        stream = StringIO()
        call_command(
            "forward_blocker_audit", "--ingestion-id", ingestion.pk, stdout=stream
        )
        payload = json.loads(stream.getvalue())

        self.assertEqual(payload["counts"]["total"], 2)
        self.assertEqual(payload["counts"]["blocking"], 1)
        self.assertEqual(payload["counts"]["non_blocking"], 1)

    def test_fail_on_blocking_exits_non_zero(self):
        ingestion = ForwardIngestion.objects.create(
            sync=self.sync,
            snapshot_selector="latestProcessed",
            snapshot_id="snapshot-2",
            baseline_ready=False,
        )
        ingestion.issues.create(
            model="dcim.device",
            message="Unable to apply device row.",
            exception="ForwardSyncDataError",
        )

        with self.assertRaises(CommandError):
            stream = StringIO()
            call_command(
                "forward_blocker_audit",
                "--ingestion-id",
                ingestion.pk,
                "--fail-on-blocking",
                stdout=stream,
            )

    def test_sync_name_selects_latest_ingestion(self):
        older = ForwardIngestion.objects.create(
            sync=self.sync,
            snapshot_selector="latestProcessed",
            snapshot_id="snapshot-3",
        )
        older.issues.create(
            model="dcim.device",
            message="blocking",
            exception="ForwardSyncDataError",
        )
        latest = ForwardIngestion.objects.create(
            sync=self.sync,
            snapshot_selector="latestProcessed",
            snapshot_id="snapshot-4",
        )
        latest.issues.create(
            model="ipam.ipaddress",
            message="Skipping delete for protected dependency",
            exception="ForwardDependencySkipError",
        )

        stream = StringIO()
        call_command(
            "forward_blocker_audit", "--sync-name", self.sync.name, stdout=stream
        )
        payload = json.loads(stream.getvalue())

        self.assertEqual(payload["ingestion_id"], latest.pk)
        self.assertEqual(payload["counts"]["blocking"], 0)

    def test_recoverable_operational_issue_is_non_blocking(self):
        branch = Branch.objects.create(
            name="blocker-audit-recoverable-branch",
            schema_id="blocker_audit_recoverable_branch",
        )
        ingestion = ForwardIngestion.objects.create(
            sync=self.sync,
            snapshot_selector="latestProcessed",
            snapshot_id="snapshot-5",
            branch=branch,
        )
        run = ForwardExecutionRun.objects.create(
            sync=self.sync,
            source=self.source,
            status=ForwardExecutionRunStatusChoices.FAILED,
            phase="failed",
            total_steps=1,
            next_step_index=1,
            auto_merge=True,
        )
        ForwardExecutionStep.objects.create(
            run=run,
            index=1,
            status=ForwardExecutionStepStatusChoices.STAGED,
            model_string="ipam.prefix",
            ingestion=ingestion,
            branch=branch,
        )
        ingestion.issues.create(
            model="ipam.prefix",
            message="terminating connection due to administrator command",
            exception="OperationalError",
        )

        stream = StringIO()
        call_command(
            "forward_blocker_audit",
            "--ingestion-id",
            ingestion.pk,
            stdout=stream,
        )
        payload = json.loads(stream.getvalue())

        self.assertEqual(payload["counts"]["total"], 1)
        self.assertEqual(payload["counts"]["blocking"], 0)
        self.assertEqual(payload["counts"]["non_blocking"], 1)

    def test_transient_operational_issue_stays_non_blocking_after_step_advances(self):
        branch = Branch.objects.create(
            name="blocker-audit-transient-branch",
            schema_id="blocker_audit_transient_branch",
        )
        ingestion = ForwardIngestion.objects.create(
            sync=self.sync,
            snapshot_selector="latestProcessed",
            snapshot_id="snapshot-6",
            branch=branch,
        )
        run = ForwardExecutionRun.objects.create(
            sync=self.sync,
            source=self.source,
            status=ForwardExecutionRunStatusChoices.RUNNING,
            phase="planning",
            total_steps=2,
            next_step_index=2,
            auto_merge=True,
        )
        ForwardExecutionStep.objects.create(
            run=run,
            index=1,
            status=ForwardExecutionStepStatusChoices.MERGED,
            model_string="ipam.prefix",
            ingestion=ingestion,
            branch=branch,
        )
        ingestion.issues.create(
            model="ipam.prefix",
            message="the connection is closed",
            exception="OperationalError",
        )

        stream = StringIO()
        call_command(
            "forward_blocker_audit",
            "--ingestion-id",
            ingestion.pk,
            stdout=stream,
        )
        payload = json.loads(stream.getvalue())

        self.assertEqual(payload["counts"]["blocking"], 0)
        self.assertEqual(payload["counts"]["non_blocking"], 1)
