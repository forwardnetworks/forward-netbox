# Recovery for syncs wedged by a dead worker (backlog: a hard-killed worker
# left ForwardSync/Branch MERGING forever and never auto-recovered).
from datetime import timedelta
from unittest.mock import patch

from core.choices import JobStatusChoices
from core.models import Job
from django.contrib.contenttypes.models import ContentType
from django.test import TestCase
from django.utils import timezone
from netbox_branching.choices import BranchStatusChoices
from netbox_branching.models import Branch

from forward_netbox.choices import ForwardSyncStatusChoices
from forward_netbox.models import ForwardIngestion
from forward_netbox.models import ForwardSource
from forward_netbox.models import ForwardSync
from forward_netbox.utilities.stuck_recovery import classify_stuck_sync
from forward_netbox.utilities.stuck_recovery import FORWARD_STUCK_MERGE_REQUEUE_LIMIT
from forward_netbox.utilities.stuck_recovery import recover_stuck_sync


class StuckRecoveryTest(TestCase):
    @classmethod
    def setUpTestData(cls):
        from django.contrib.auth import get_user_model

        cls.user = get_user_model().objects.create_user(username="stuck-user")
        cls.source = ForwardSource.objects.create(
            name="stuck-src",
            type="saas",
            url="https://fwd.app",
            parameters={
                "username": "u@example.com",
                "password": "x",
                "verify": True,
                "network_id": "n",
            },
        )

    def _sync(self, name, status, *, updated_ago=timedelta(hours=1)):
        sync = ForwardSync.objects.create(
            name=name,
            source=self.source,
            user=self.user,
            parameters={"snapshot_id": "latestProcessed"},
        )
        ForwardSync.objects.filter(pk=sync.pk).update(
            status=status, last_updated=timezone.now() - updated_ago
        )
        sync.refresh_from_db()
        return sync

    def _merge_job(self, sync, *, status=JobStatusChoices.STATUS_RUNNING, suffix="1"):
        return Job.objects.create(
            object_type=ContentType.objects.get_for_model(ForwardIngestion),
            object_id=0,
            name=f"{sync.name} Merge",
            status=status,
            started=timezone.now() - timedelta(hours=1),
            job_id=f"123e4567-e89b-12d3-a456-42661418000{suffix}",
        )

    def test_live_merge_is_not_touched(self):
        sync = self._sync("live", ForwardSyncStatusChoices.MERGING)
        ingestion = ForwardIngestion.objects.create(
            sync=sync, merge_job=self._merge_job(sync)
        )
        ingestion  # noqa: B018
        with patch(
            "forward_netbox.utilities.stuck_recovery.job_has_live_execution",
            return_value=True,
        ):
            self.assertIsNone(classify_stuck_sync(sync))

    def test_ready_to_merge_is_never_recovered(self):
        # Operator review lane — out of scope entirely.
        sync = self._sync("review", ForwardSyncStatusChoices.READY_TO_MERGE)
        self.assertIsNone(classify_stuck_sync(sync))

    def test_grace_window_respected(self):
        sync = self._sync(
            "young",
            ForwardSyncStatusChoices.MERGING,
            updated_ago=timedelta(seconds=30),
        )
        job = self._merge_job(sync)
        # A recent wedge (job started within the grace window) is not eligible.
        Job.objects.filter(pk=job.pk).update(
            started=timezone.now() - timedelta(seconds=20)
        )
        ForwardIngestion.objects.create(sync=sync, merge_job=job)
        with patch(
            "forward_netbox.utilities.stuck_recovery.job_has_live_execution",
            return_value=False,
        ):
            self.assertIsNone(classify_stuck_sync(sync))

    def test_dead_merge_requeues_and_resets_branch(self):
        sync = self._sync("wedged", ForwardSyncStatusChoices.MERGING)
        branch = Branch.objects.create(name="wedged-branch")
        Branch.objects.filter(pk=branch.pk).update(status=BranchStatusChoices.MERGING)
        branch.refresh_from_db()
        dead = self._merge_job(sync)
        ingestion = ForwardIngestion.objects.create(
            sync=sync, merge_job=dead, branch=branch
        )
        with patch(
            "forward_netbox.utilities.stuck_recovery.job_has_live_execution",
            return_value=False,
        ):
            verdict = classify_stuck_sync(sync)
            self.assertEqual(verdict["action"], "requeue_merge")
            with patch.object(ForwardIngestion, "enqueue_merge_job") as enqueue:
                enqueue.return_value = Job(pk=999, name="x")
                result = recover_stuck_sync(sync, verdict, user=self.user)
        self.assertEqual(result["action"], "requeued_merge")
        branch.refresh_from_db()
        self.assertEqual(str(branch.status), BranchStatusChoices.READY)
        dead.refresh_from_db()
        self.assertEqual(dead.status, JobStatusChoices.STATUS_FAILED)
        sync.refresh_from_db()
        self.assertEqual(sync.parameters["stuck_recovery"]["attempts"], 1)
        ingestion  # noqa: B018

    def test_budget_exhaustion_fails_and_records_issue(self):
        from forward_netbox.models import ForwardIngestionIssue

        sync = self._sync("exhausted", ForwardSyncStatusChoices.MERGING)
        branch = Branch.objects.create(name="exhausted-branch")
        Branch.objects.filter(pk=branch.pk).update(status=BranchStatusChoices.MERGING)
        branch.refresh_from_db()
        ingestion = ForwardIngestion.objects.create(
            sync=sync, merge_job=self._merge_job(sync), branch=branch
        )
        ForwardSync.objects.filter(pk=sync.pk).update(
            parameters={
                **sync.parameters,
                "stuck_recovery": {
                    "ingestion_id": ingestion.pk,
                    "attempts": FORWARD_STUCK_MERGE_REQUEUE_LIMIT,
                },
            }
        )
        sync.refresh_from_db()
        with patch(
            "forward_netbox.utilities.stuck_recovery.job_has_live_execution",
            return_value=False,
        ):
            verdict = classify_stuck_sync(sync)
            self.assertEqual(verdict["action"], "give_up")
            recover_stuck_sync(sync, verdict, user=self.user)
        sync.refresh_from_db()
        self.assertEqual(sync.status, ForwardSyncStatusChoices.FAILED)
        self.assertTrue(
            ForwardIngestionIssue.objects.filter(
                ingestion=ingestion, exception="StuckMergeRecoveryExhausted"
            ).exists()
        )

    def test_dead_sync_run_is_failed(self):
        sync = self._sync("dead-sync", ForwardSyncStatusChoices.SYNCING)
        Job.objects.create(
            object_type=ContentType.objects.get_for_model(ForwardSync),
            object_id=sync.pk,
            name=f"{sync.name} - adhoc",
            status=JobStatusChoices.STATUS_RUNNING,
            started=timezone.now() - timedelta(hours=1),
            job_id="123e4567-e89b-12d3-a456-426614180099",
        )
        with patch(
            "forward_netbox.utilities.stuck_recovery.job_has_live_execution",
            return_value=False,
        ):
            verdict = classify_stuck_sync(sync)
            self.assertEqual(verdict["action"], "fail_sync")
            recover_stuck_sync(sync, verdict, user=self.user)
        sync.refresh_from_db()
        self.assertEqual(sync.status, ForwardSyncStatusChoices.FAILED)

    def test_recover_reclassifies_as_live_under_lock(self):
        sync = self._sync("racy", ForwardSyncStatusChoices.MERGING)
        branch = Branch.objects.create(name="racy-branch")
        Branch.objects.filter(pk=branch.pk).update(status=BranchStatusChoices.MERGING)
        ingestion = ForwardIngestion.objects.create(
            sync=sync, merge_job=self._merge_job(sync), branch=branch
        )
        ingestion  # noqa: B018
        verdict = {
            "action": "requeue_merge",
            "ingestion_id": ingestion.pk,
            "attempts": 0,
            "dead_job_pks": [],
        }
        # Worker came back alive between classify and recover.
        with patch(
            "forward_netbox.utilities.stuck_recovery.job_has_live_execution",
            return_value=True,
        ):
            result = recover_stuck_sync(sync, verdict, user=self.user)
        self.assertEqual(result["action"], "skipped")
