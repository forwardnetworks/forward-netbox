import json
from io import StringIO

from django.core.management import call_command
from django.core.management.base import CommandError
from django.test import TestCase
from netbox_branching.choices import BranchStatusChoices
from netbox_branching.models import Branch

from forward_netbox.choices import ForwardSyncStatusChoices
from forward_netbox.models import ForwardIngestion
from forward_netbox.models import ForwardSource
from forward_netbox.models import ForwardSync


class OwnershipAuditCommandTest(TestCase):
    def test_empty_database_is_consistent(self):
        output = StringIO()

        call_command(
            "forward_ownership_audit",
            "--fail-on-inconsistent",
            stdout=output,
        )

        report = json.loads(output.getvalue())
        self.assertTrue(report["consistent"])
        self.assertTrue(report["release_ready"])
        self.assertEqual(report["device_tag_claims"], 0)

    def test_open_ready_branch_only_fails_strict_release_audit(self):
        branch = Branch(name="Open Ready Branch")
        branch.save(provision=False)
        Branch.objects.filter(pk=branch.pk).update(status=BranchStatusChoices.READY)

        output = StringIO()
        call_command(
            "forward_ownership_audit",
            "--fail-on-inconsistent",
            stdout=output,
        )
        report = json.loads(output.getvalue())
        self.assertTrue(report["consistent"])
        self.assertFalse(report["release_ready"])

        with self.assertRaises(CommandError):
            call_command(
                "forward_ownership_audit",
                "--fail-on-inconsistent",
                "--require-no-open-branches",
                stdout=StringIO(),
            )

    def test_pending_migration_branch_fails_release_audit(self):
        branch = Branch(name="Pending Migration Branch")
        branch.save(provision=False)
        Branch.objects.filter(pk=branch.pk).update(
            status=BranchStatusChoices.PENDING_MIGRATIONS
        )

        with self.assertRaises(CommandError):
            call_command(
                "forward_ownership_audit",
                "--fail-on-inconsistent",
                stdout=StringIO(),
            )

    def test_required_domains_without_reconciliation_fail_audit(self):
        source = ForwardSource.objects.create(
            name="ownership-audit-source",
            type="saas",
            url="https://fwd.app",
            parameters={"network_id": "n-1"},
        )
        sync = ForwardSync.objects.create(
            name="ownership-audit-sync",
            source=source,
            status=ForwardSyncStatusChoices.COMPLETED,
            parameters={"snapshot_id": "latestProcessed"},
        )
        ForwardIngestion.objects.create(
            sync=sync,
            snapshot_id="snapshot-1",
            baseline_ready=True,
        )

        output = StringIO()
        call_command("forward_ownership_audit", stdout=output)
        report = json.loads(output.getvalue())
        self.assertEqual(report["missing_required_reconciliations"], 2)
        self.assertFalse(report["consistent"])
        with self.assertRaises(CommandError):
            call_command(
                "forward_ownership_audit",
                "--fail-on-inconsistent",
                stdout=StringIO(),
            )
