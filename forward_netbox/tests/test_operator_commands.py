import json
from io import StringIO

from core.choices import JobStatusChoices
from core.models import Job
from django.contrib.contenttypes.models import ContentType
from django.core.management import call_command
from django.core.management.base import CommandError
from django.test import TestCase
from django.utils import timezone
from netbox_branching.choices import BranchStatusChoices
from netbox_branching.models import Branch

from forward_netbox.choices import ForwardSyncStatusChoices
from forward_netbox.models import ForwardIngestion
from forward_netbox.models import ForwardOwnershipReconciliation
from forward_netbox.models import ForwardSource
from forward_netbox.models import ForwardSync
from forward_netbox.utilities.drift_report import build_latest_sync_evidence


class SingleBranchOperatorCommandTest(TestCase):
    @classmethod
    def setUpTestData(cls):
        cls.source = ForwardSource.objects.create(
            name="operator-command-source",
            type="saas",
            url="https://fwd.app",
            parameters={
                "username": "operator@example.com",
                "password": "secret",
                "network_id": "network-1",
            },
        )
        cls.sync = ForwardSync.objects.create(
            name="operator-command-sync",
            source=cls.source,
            status=ForwardSyncStatusChoices.COMPLETED,
            parameters={"snapshot_id": "latestProcessed"},
        )
        cls.branch = Branch(name="operator-command-branch")
        cls.branch.save(provision=False)
        Branch.objects.filter(pk=cls.branch.pk).update(
            status=BranchStatusChoices.MERGED
        )
        cls.branch.refresh_from_db()
        cls.ingestion = ForwardIngestion.objects.create(
            sync=cls.sync,
            branch=cls.branch,
            snapshot_id="snapshot-1",
            baseline_ready=True,
            applied_change_count=3,
        )
        now = timezone.now()
        cls.stage_job = Job.objects.create(
            object_type=ContentType.objects.get_for_model(ForwardIngestion),
            object_id=cls.ingestion.pk,
            name="operator-stage-job",
            status=JobStatusChoices.STATUS_COMPLETED,
            job_id="123e4567-e89b-12d3-a456-426614174010",
            created=now,
            started=now,
            completed=now,
            data={
                "logs": [
                    [
                        now.isoformat(),
                        "warning",
                        "operator-command-sync",
                        "",
                        "Synthetic warning.",
                    ]
                ]
            },
        )
        cls.merge_job = Job.objects.create(
            object_type=ContentType.objects.get_for_model(ForwardIngestion),
            object_id=cls.ingestion.pk,
            name="operator-merge-job",
            status=JobStatusChoices.STATUS_COMPLETED,
            job_id="123e4567-e89b-12d3-a456-426614174011",
            created=now,
            started=now,
            completed=now,
            data={
                "logs": [
                    [
                        now.isoformat(),
                        "error",
                        "operator-command-sync",
                        "",
                        "Synthetic error.",
                    ]
                ]
            },
        )
        cls.ingestion.job = cls.stage_job
        cls.ingestion.merge_job = cls.merge_job
        cls.ingestion.save(update_fields=["job", "merge_job"])
        ForwardOwnershipReconciliation.objects.create(
            sync=cls.sync,
            domain=ForwardOwnershipReconciliation.Domain.VIRTUAL_PARENTS,
            generation=cls.ingestion.pk,
            snapshot_id=cls.ingestion.snapshot_id,
            status=ForwardOwnershipReconciliation.Status.COMPLETED,
            completed_at=now,
        )
        ForwardOwnershipReconciliation.objects.create(
            sync=cls.sync,
            domain=ForwardOwnershipReconciliation.Domain.STATUS_TAGS,
            generation=cls.ingestion.pk,
            snapshot_id=cls.ingestion.snapshot_id,
            status=ForwardOwnershipReconciliation.Status.COMPLETED,
            completed_at=now,
        )

    def test_watch_reports_single_branch_and_jobs(self):
        output = StringIO()

        call_command(
            "forward_watch_sync",
            "--sync-id",
            str(self.sync.pk),
            "--max-polls",
            "1",
            stdout=output,
        )

        report = json.loads(output.getvalue())
        self.assertEqual(report["branch"]["id"], self.branch.pk)
        self.assertEqual(report["job"]["id"], self.stage_job.pk)
        self.assertEqual(report["merge_job"]["id"], self.merge_job.pk)
        self.assertTrue(report["ingestion"]["baseline_ready"])
        self.assertTrue(report["ownership"]["complete"])
        self.assertNotIn("execution_run", report)

    def test_watch_fails_for_failed_or_nonterminal_sync(self):
        self.sync.status = ForwardSyncStatusChoices.FAILED
        self.sync.save(update_fields=["status"])
        with self.assertRaises(CommandError):
            call_command(
                "forward_watch_sync",
                "--sync-id",
                str(self.sync.pk),
                stdout=StringIO(),
            )

        self.sync.status = ForwardSyncStatusChoices.SYNCING
        self.sync.save(update_fields=["status"])
        with self.assertRaises(CommandError):
            call_command(
                "forward_watch_sync",
                "--sync-id",
                str(self.sync.pk),
                "--max-polls",
                "1",
                stdout=StringIO(),
            )

    def test_watch_does_not_complete_before_ownership_converges(self):
        ForwardOwnershipReconciliation.objects.filter(sync=self.sync).update(
            status=ForwardOwnershipReconciliation.Status.PENDING,
            completed_at=None,
        )

        with self.assertRaises(CommandError):
            call_command(
                "forward_watch_sync",
                "--sync-id",
                str(self.sync.pk),
                "--max-polls",
                "1",
                stdout=StringIO(),
            )

    def test_warning_audit_scans_stage_and_merge_jobs(self):
        overlay_job = Job.objects.create(
            object_type=ContentType.objects.get_for_model(ForwardSync),
            object_id=self.sync.pk,
            name=f"{self.sync.name} - link vsys/vdom parents (auto)",
            status=JobStatusChoices.STATUS_COMPLETED,
            job_id="123e4567-e89b-12d3-a456-426614174012",
            created=timezone.now(),
            data={"logs": []},
        )
        output = StringIO()

        call_command(
            "forward_warning_audit",
            "--sync-id",
            str(self.sync.pk),
            stdout=output,
        )

        report = json.loads(output.getvalue())
        self.assertEqual(report["warning_count"], 1)
        self.assertEqual(report["error_count"], 1)
        self.assertEqual(
            report["job_ids"],
            sorted([self.stage_job.pk, self.merge_job.pk, overlay_job.pk]),
        )
        with self.assertRaises(CommandError):
            call_command(
                "forward_warning_audit",
                "--sync-id",
                str(self.sync.pk),
                "--fail-on-error",
                stdout=StringIO(),
            )


class UIHarnessCommandTest(TestCase):
    def test_seed_is_idempotent_and_creates_completed_ownership_evidence(self):
        options = {
            "username": "ui-command-admin",
            "password": "test-password",
            "source_name": "ui-command-source",
            "sync_name": "ui-command-sync",
            "snapshot_id": "ui-command-snapshot",
            "network_id": "ui-command-network",
            "stdout": StringIO(),
        }

        call_command("forward_seed_ui_harness", **options)
        call_command("forward_seed_ui_harness", **options)

        sync = ForwardSync.objects.get(name="ui-command-sync")
        ingestion = ForwardIngestion.objects.get(sync=sync)
        preview_job = Job.objects.get(
            object_type=ContentType.objects.get_for_model(ForwardSync),
            object_id=sync.pk,
            name__icontains="dependency preview",
        )
        reconciliations = ForwardOwnershipReconciliation.objects.filter(sync=sync)

        self.assertEqual(
            set(reconciliations.values_list("domain", flat=True)),
            {
                ForwardOwnershipReconciliation.Domain.STATUS_TAGS,
                ForwardOwnershipReconciliation.Domain.VIRTUAL_PARENTS,
            },
        )
        self.assertFalse(
            reconciliations.exclude(
                ingestion=ingestion,
                status=ForwardOwnershipReconciliation.Status.COMPLETED,
            ).exists()
        )
        self.assertEqual(
            build_latest_sync_evidence(ingestion, preview_job.data)["status"],
            "confirmation_required",
        )
