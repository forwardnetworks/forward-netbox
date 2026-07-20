# Recovery contract for syncs wedged by a dead worker while the ForwardSync or
# Branch remains in a nonterminal state.
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
from forward_netbox.models import ForwardOwnershipReconciliation
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

    def test_future_scheduled_first_sync_is_not_recovered_as_dead(self):
        sync = self._sync("future-first", ForwardSyncStatusChoices.QUEUED)
        scheduled = Job.objects.create(
            object_type=ContentType.objects.get_for_model(ForwardSync),
            object_id=sync.pk,
            name=f"{sync.name} - scheduled",
            status=JobStatusChoices.STATUS_SCHEDULED,
            scheduled=timezone.now() + timedelta(hours=2),
            job_id="123e4567-e89b-12d3-a456-426614180095",
        )

        with patch(
            "forward_netbox.utilities.stuck_recovery.job_has_live_execution",
            return_value=False,
        ):
            verdict = classify_stuck_sync(sync)

        self.assertIsNone(verdict)
        scheduled.refresh_from_db()
        self.assertEqual(scheduled.status, JobStatusChoices.STATUS_SCHEDULED)

    def test_dead_standing_occurrence_is_reconciled_without_failing_sync(self):
        sync = self._sync("dead-standing", ForwardSyncStatusChoices.COMPLETED)
        parameters = dict(sync.parameters or {})
        parameters["validation_schedule_interval"] = 30
        ForwardSync.objects.filter(pk=sync.pk).update(parameters=parameters)
        sync.refresh_from_db()
        standing = Job.objects.create(
            object_type=ContentType.objects.get_for_model(ForwardSync),
            object_id=sync.pk,
            name="validation",
            status=JobStatusChoices.STATUS_RUNNING,
            started=timezone.now() - timedelta(hours=1),
            interval=30,
            job_id="123e4567-e89b-12d3-a456-426614180096",
        )

        with patch(
            "forward_netbox.utilities.stuck_recovery.job_has_live_execution",
            return_value=False,
        ), patch(
            "forward_netbox.utilities.sync_facade.reconcile_standing_schedules"
        ) as reconcile:
            verdict = classify_stuck_sync(sync, grace_seconds=0)
            self.assertEqual(verdict["action"], "reconcile_standing_schedules")
            result = recover_stuck_sync(sync, verdict, user=self.user)

        self.assertEqual(result["action"], "reconciled_standing_schedules")
        standing.refresh_from_db()
        sync.refresh_from_db()
        self.assertEqual(standing.status, JobStatusChoices.STATUS_FAILED)
        self.assertEqual(sync.status, ForwardSyncStatusChoices.COMPLETED)
        reconcile.assert_called_once()

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
        producer = Job.objects.create(
            object_type=ContentType.objects.get_for_model(ForwardIngestion),
            object_id=ingestion.pk,
            name=f"{sync.name} - scheduled",
            status=JobStatusChoices.STATUS_RUNNING,
            started=timezone.now() - timedelta(hours=1),
            job_id="123e4567-e89b-12d3-a456-426614180097",
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
        enqueue.assert_called_once_with(
            self.user,
            remove_branch=True,
            recovery_sync_job_pks=[producer.pk],
        )
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
        ingestion = ForwardIngestion.objects.create(sync=sync)
        job = Job.objects.create(
            object_type=ContentType.objects.get_for_model(ForwardIngestion),
            object_id=ingestion.pk,
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
            self.assertEqual(verdict["dead_job_pks"], [job.pk])
            recover_stuck_sync(sync, verdict, user=self.user)
        sync.refresh_from_db()
        job.refresh_from_db()
        self.assertEqual(sync.status, ForwardSyncStatusChoices.FAILED)
        self.assertEqual(job.status, JobStatusChoices.STATUS_FAILED)
        self.assertIsNotNone(job.completed)

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

    def test_recover_does_not_apply_stale_verdict_after_state_transition(self):
        sync = self._sync("state-race", ForwardSyncStatusChoices.MERGING)
        branch = Branch.objects.create(name="state-race-branch")
        Branch.objects.filter(pk=branch.pk).update(status=BranchStatusChoices.READY)
        branch.refresh_from_db()
        dead = self._merge_job(sync)
        ForwardIngestion.objects.create(sync=sync, merge_job=dead, branch=branch)

        with patch(
            "forward_netbox.utilities.stuck_recovery.job_has_live_execution",
            return_value=False,
        ):
            verdict = classify_stuck_sync(sync, grace_seconds=0)
            self.assertEqual(verdict["action"], "requeue_merge")
            ForwardSync.objects.filter(pk=sync.pk).update(
                status=ForwardSyncStatusChoices.READY_TO_MERGE
            )
            result = recover_stuck_sync(sync, verdict, user=self.user)

        self.assertEqual(result["action"], "skipped")
        self.assertEqual(result["reason"], "state changed under lock")
        dead.refresh_from_db()
        self.assertEqual(dead.status, JobStatusChoices.STATUS_RUNNING)

    def test_merged_branch_recovery_completes_bookkeeping_and_dispatches(self):
        sync = self._sync("merged-bookkeeping", ForwardSyncStatusChoices.MERGING)
        branch = Branch.objects.create(name="merged-bookkeeping-branch")
        Branch.objects.filter(pk=branch.pk).update(
            status=BranchStatusChoices.MERGED,
            merged_time=timezone.now(),
        )
        branch.refresh_from_db()
        dead = self._merge_job(sync)
        ingestion = ForwardIngestion.objects.create(
            sync=sync,
            merge_job=dead,
            branch=branch,
            snapshot_id="snapshot-merged-bookkeeping",
            merge_applied_at=branch.merged_time,
        )

        with patch(
            "forward_netbox.utilities.stuck_recovery.job_has_live_execution",
            return_value=False,
        ), patch(
            "forward_netbox.utilities.ingestion_merge.latest_processed_catchup_decision",
            return_value={"should_queue": False},
        ), patch(
            "forward_netbox.jobs._enqueue_post_sync_overlays",
            return_value={"scheduled": True},
        ) as enqueue:
            verdict = classify_stuck_sync(sync, grace_seconds=0)
            self.assertEqual(verdict["action"], "finalize_merged_bookkeeping")
            result = recover_stuck_sync(sync, verdict, user=self.user)

        self.assertEqual(result["action"], "finalized_merged_bookkeeping")
        enqueue.assert_called_once_with(
            sync,
            snapshot_id="snapshot-merged-bookkeeping",
            ingestion_id=ingestion.pk,
        )
        ingestion.refresh_from_db()
        sync.refresh_from_db()
        dead.refresh_from_db()
        self.assertTrue(ingestion.baseline_ready)
        self.assertIsNone(ingestion.branch)
        self.assertEqual(sync.status, ForwardSyncStatusChoices.COMPLETED)
        self.assertEqual(dead.status, JobStatusChoices.STATUS_COMPLETED)

    def test_completed_sync_with_pending_ownership_is_redispatched(self):
        sync = self._sync("ownership-pending", ForwardSyncStatusChoices.COMPLETED)
        ingestion = ForwardIngestion.objects.create(
            sync=sync,
            snapshot_id="snapshot-ownership",
            baseline_ready=True,
        )
        ForwardOwnershipReconciliation.objects.create(
            sync=sync,
            domain=ForwardOwnershipReconciliation.Domain.VIRTUAL_PARENTS,
            generation=ingestion.pk,
            snapshot_id=ingestion.snapshot_id,
            status=ForwardOwnershipReconciliation.Status.PENDING,
        )

        with patch(
            "forward_netbox.utilities.stuck_recovery.job_has_live_execution",
            return_value=False,
        ):
            verdict = classify_stuck_sync(sync, grace_seconds=0)
        self.assertEqual(verdict["action"], "redispatch_ownership")
        with patch(
            "forward_netbox.jobs._enqueue_post_sync_overlays",
            return_value={"scheduled": True},
        ) as enqueue:
            result = recover_stuck_sync(sync, verdict, user=self.user)

        self.assertEqual(result["action"], "redispatched_ownership")
        enqueue.assert_called_once_with(
            sync,
            snapshot_id="snapshot-ownership",
            ingestion_id=ingestion.pk,
        )

    def test_completed_sync_finalizes_dead_producer_after_ownership_converges(self):
        sync = self._sync("producer-pending", ForwardSyncStatusChoices.COMPLETED)
        ingestion = ForwardIngestion.objects.create(
            sync=sync,
            snapshot_id="snapshot-complete",
            baseline_ready=True,
        )
        ForwardOwnershipReconciliation.objects.create(
            sync=sync,
            domain=ForwardOwnershipReconciliation.Domain.VIRTUAL_PARENTS,
            generation=ingestion.pk,
            snapshot_id=ingestion.snapshot_id,
            status=ForwardOwnershipReconciliation.Status.COMPLETED,
            completed_at=timezone.now(),
        )
        ForwardOwnershipReconciliation.objects.create(
            sync=sync,
            domain=ForwardOwnershipReconciliation.Domain.STATUS_TAGS,
            generation=ingestion.pk,
            snapshot_id=ingestion.snapshot_id,
            status=ForwardOwnershipReconciliation.Status.COMPLETED,
            completed_at=timezone.now(),
        )
        producer = Job.objects.create(
            object_type=ContentType.objects.get_for_model(ForwardSync),
            object_id=sync.pk,
            name=f"{sync.name} - adhoc",
            status=JobStatusChoices.STATUS_RUNNING,
            started=timezone.now() - timedelta(hours=1),
            job_id="123e4567-e89b-12d3-a456-426614180088",
        )

        with patch(
            "forward_netbox.utilities.stuck_recovery.job_has_live_execution",
            return_value=False,
        ):
            verdict = classify_stuck_sync(sync, grace_seconds=0)
            self.assertEqual(verdict["action"], "finalize_completed_jobs")
            result = recover_stuck_sync(sync, verdict, user=self.user)

        self.assertEqual(result["action"], "finalized_completed_jobs")
        producer.refresh_from_db()
        self.assertEqual(producer.status, JobStatusChoices.STATUS_COMPLETED)

    def test_completed_recovery_never_terminates_unrelated_operator_job(self):
        sync = self._sync("producer-only", ForwardSyncStatusChoices.COMPLETED)
        ingestion = ForwardIngestion.objects.create(
            sync=sync,
            snapshot_id="snapshot-producer-only",
            baseline_ready=True,
        )
        for domain in (
            ForwardOwnershipReconciliation.Domain.VIRTUAL_PARENTS,
            ForwardOwnershipReconciliation.Domain.STATUS_TAGS,
        ):
            ForwardOwnershipReconciliation.objects.create(
                sync=sync,
                domain=domain,
                generation=ingestion.pk,
                snapshot_id=ingestion.snapshot_id,
                status=ForwardOwnershipReconciliation.Status.COMPLETED,
                completed_at=timezone.now(),
            )
        producer = Job.objects.create(
            object_type=ContentType.objects.get_for_model(ForwardSync),
            object_id=sync.pk,
            name=f"{sync.name} - adhoc",
            status=JobStatusChoices.STATUS_RUNNING,
            started=timezone.now() - timedelta(hours=1),
            job_id="123e4567-e89b-12d3-a456-426614180089",
        )
        validation = Job.objects.create(
            object_type=ContentType.objects.get_for_model(ForwardSync),
            object_id=sync.pk,
            name=f"{sync.name} - validation",
            status=JobStatusChoices.STATUS_RUNNING,
            started=timezone.now() - timedelta(hours=1),
            job_id="123e4567-e89b-12d3-a456-426614180090",
        )

        with patch(
            "forward_netbox.utilities.stuck_recovery.job_has_live_execution",
            return_value=False,
        ):
            verdict = classify_stuck_sync(sync, grace_seconds=0)
            self.assertEqual(verdict["dead_job_pks"], [producer.pk])
            recover_stuck_sync(sync, verdict, user=self.user)

        producer.refresh_from_db()
        validation.refresh_from_db()
        self.assertEqual(producer.status, JobStatusChoices.STATUS_COMPLETED)
        self.assertEqual(validation.status, JobStatusChoices.STATUS_RUNNING)

    def test_producer_rebound_to_older_ingestion_remains_visible(self):
        sync = self._sync("older-producer", ForwardSyncStatusChoices.SYNCING)
        older = ForwardIngestion.objects.create(sync=sync, snapshot_id="older")
        ForwardIngestion.objects.create(sync=sync, snapshot_id="newer")
        producer = Job.objects.create(
            object_type=ContentType.objects.get_for_model(ForwardIngestion),
            object_id=older.pk,
            name=f"{sync.name} - adhoc",
            status=JobStatusChoices.STATUS_RUNNING,
            started=timezone.now() - timedelta(hours=1),
            job_id="123e4567-e89b-12d3-a456-426614180091",
        )

        with patch(
            "forward_netbox.utilities.stuck_recovery.job_has_live_execution",
            return_value=False,
        ):
            verdict = classify_stuck_sync(sync, grace_seconds=0)

        self.assertEqual(verdict["action"], "fail_sync")
        self.assertEqual(verdict["producer_job_pks"], [producer.pk])

    def test_recovery_skips_when_pending_replacement_job_commits(self):
        sync = self._sync("replacement-race", ForwardSyncStatusChoices.SYNCING)
        ingestion = ForwardIngestion.objects.create(sync=sync)
        original = Job.objects.create(
            object_type=ContentType.objects.get_for_model(ForwardIngestion),
            object_id=ingestion.pk,
            name=f"{sync.name} - adhoc",
            status=JobStatusChoices.STATUS_RUNNING,
            started=timezone.now() - timedelta(hours=1),
            job_id="123e4567-e89b-12d3-a456-426614180092",
        )
        with patch(
            "forward_netbox.utilities.stuck_recovery.job_has_live_execution",
            return_value=False,
        ):
            verdict = classify_stuck_sync(sync, grace_seconds=0)

        Job.objects.filter(pk=original.pk).update(
            status=JobStatusChoices.STATUS_FAILED,
            completed=timezone.now(),
        )
        replacement = Job.objects.create(
            object_type=ContentType.objects.get_for_model(ForwardSync),
            object_id=sync.pk,
            name=f"{sync.name} - adhoc",
            status=JobStatusChoices.STATUS_PENDING,
            job_id="123e4567-e89b-12d3-a456-426614180093",
        )
        with patch(
            "forward_netbox.utilities.stuck_recovery.job_has_live_execution",
            return_value=False,
        ):
            result = recover_stuck_sync(sync, verdict, user=self.user)

        self.assertEqual(result["action"], "skipped")
        self.assertEqual(result["reason"], "candidate jobs changed under lock")
        replacement.refresh_from_db()
        self.assertEqual(replacement.status, JobStatusChoices.STATUS_PENDING)

    def test_completed_scheduled_producer_restores_recurrence(self):
        sync = self._sync("scheduled-producer", ForwardSyncStatusChoices.COMPLETED)
        ForwardSync.objects.filter(pk=sync.pk).update(
            interval=30,
            scheduled=timezone.now() - timedelta(hours=1),
        )
        sync.refresh_from_db()
        ingestion = ForwardIngestion.objects.create(
            sync=sync,
            snapshot_id="snapshot-scheduled-producer",
            baseline_ready=True,
        )
        for domain in (
            ForwardOwnershipReconciliation.Domain.VIRTUAL_PARENTS,
            ForwardOwnershipReconciliation.Domain.STATUS_TAGS,
        ):
            ForwardOwnershipReconciliation.objects.create(
                sync=sync,
                domain=domain,
                generation=ingestion.pk,
                snapshot_id=ingestion.snapshot_id,
                status=ForwardOwnershipReconciliation.Status.COMPLETED,
                completed_at=timezone.now(),
            )
        producer = Job.objects.create(
            object_type=ContentType.objects.get_for_model(ForwardIngestion),
            object_id=ingestion.pk,
            name=f"{sync.name} - scheduled",
            status=JobStatusChoices.STATUS_RUNNING,
            started=timezone.now() - timedelta(hours=1),
            job_id="123e4567-e89b-12d3-a456-426614180094",
        )

        with patch(
            "forward_netbox.utilities.stuck_recovery.job_has_live_execution",
            return_value=False,
        ), patch("forward_netbox.jobs._reconcile_sync_run_schedules") as reconcile:
            verdict = classify_stuck_sync(sync, grace_seconds=0)
            recover_stuck_sync(sync, verdict, user=self.user)

        reconcile.assert_called_once()
        args, kwargs = reconcile.call_args
        self.assertEqual(args[0].pk, sync.pk)
        self.assertEqual(args[1].pk, producer.pk)
        self.assertEqual(kwargs, {"adhoc": False})

    def test_recovery_without_producer_self_heals_standing_schedules(self):
        sync = self._sync("schedule-only", ForwardSyncStatusChoices.COMPLETED)

        with patch(
            "forward_netbox.utilities.sync_facade.reconcile_standing_schedules"
        ) as reconcile:
            from forward_netbox.jobs import _complete_recovered_sync_producers

            _complete_recovered_sync_producers(sync, [])

        reconcile.assert_called_once()
        self.assertEqual(reconcile.call_args.args[0].pk, sync.pk)
