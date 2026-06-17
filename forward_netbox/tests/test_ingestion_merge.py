from contextlib import nullcontext
from unittest.mock import MagicMock
from unittest.mock import patch
from uuid import uuid4

from core.choices import JobStatusChoices
from core.models import Job
from django.contrib.contenttypes.models import ContentType
from django.test import TestCase
from django.utils import timezone
from netbox_branching.choices import BranchStatusChoices
from netbox_branching.models import Branch

from forward_netbox.choices import ForwardExecutionRunStatusChoices
from forward_netbox.choices import ForwardExecutionStepStatusChoices
from forward_netbox.choices import ForwardSyncStatusChoices
from forward_netbox.models import ForwardExecutionRun
from forward_netbox.models import ForwardExecutionStep
from forward_netbox.models import ForwardIngestion
from forward_netbox.models import ForwardSource
from forward_netbox.models import ForwardSync
from forward_netbox.utilities.forward_api import LATEST_PROCESSED_SNAPSHOT
from forward_netbox.utilities.ingestion_merge import (
    AUTO_MERGE_STALE_MERGE_REQUEUE_LIMIT,
)
from forward_netbox.utilities.ingestion_merge import cleanup_merged_branch
from forward_netbox.utilities.ingestion_merge import enqueue_merge_job
from forward_netbox.utilities.ingestion_merge import maybe_enqueue_next_branch_stage
from forward_netbox.utilities.ingestion_merge import record_change_totals
from forward_netbox.utilities.ingestion_merge import sync_merge_ingestion
from forward_netbox.utilities.sync_state import has_pending_branch_run


class ForwardIngestionMergeHelperTest(TestCase):
    def setUp(self):
        self.source = ForwardSource.objects.create(
            name="source-ingestion-merge",
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
            name="sync-ingestion-merge",
            source=self.source,
            auto_merge=False,
            parameters={
                "snapshot_id": LATEST_PROCESSED_SNAPSHOT,
                "dcim.device": True,
            },
        )

    def test_sync_merge_ingestion_advances_pending_branch_state(self):
        ingestion = ForwardIngestion.objects.create(
            sync=self.sync,
            snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
            snapshot_id="snapshot-before",
        )
        self.sync.set_branch_run_state(
            {
                "snapshot_selector": LATEST_PROCESSED_SNAPSHOT,
                "snapshot_id": "snapshot-before",
                "max_changes_per_branch": 10000,
                "next_plan_index": 2,
                "total_plan_items": 3,
                "auto_merge": False,
                "awaiting_merge": True,
                "pending_ingestion_id": ingestion.pk,
                "pending_plan_index": 1,
                "pending_is_final": True,
            }
        )

        with (
            patch("forward_netbox.utilities.merge.merge_branch"),
            patch(
                "forward_netbox.utilities.ingestion_merge.suppress_branch_merge_side_effect_signals",
                return_value=nullcontext(),
            ),
            patch(
                "forward_netbox.utilities.execution_ledger.update_run_from_branch_state"
            ) as mock_update_run_from_branch_state,
        ):
            sync_merge_ingestion(ingestion)

        self.sync.refresh_from_db()
        ingestion.refresh_from_db()

        self.assertEqual(self.sync.status, ForwardSyncStatusChoices.COMPLETED)
        self.assertTrue(ingestion.baseline_ready)
        self.assertEqual(self.sync.get_branch_run_state(), {})
        mock_update_run_from_branch_state.assert_not_called()

    def test_cleanup_merged_branch_refreshes_stale_status_before_delete(self):
        branch = Branch.objects.create(
            name=f"merged-branch-{uuid4().hex[:12]}",
            schema_id=f"merged_branch_{uuid4().hex[:12]}",
            status=BranchStatusChoices.MERGING,
        )
        ingestion = ForwardIngestion.objects.create(
            sync=self.sync,
            snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
            snapshot_id="snapshot-merged-branch",
            branch=branch,
        )
        Branch.objects.filter(pk=branch.pk).update(status=BranchStatusChoices.MERGED)
        branch.status = BranchStatusChoices.MERGING

        cleanup_merged_branch(ingestion)

        self.assertFalse(Branch.objects.filter(pk=branch.pk).exists())
        ingestion.refresh_from_db()
        self.assertIsNone(ingestion.branch)

    def test_sync_merge_ingestion_deletes_stale_merged_branch(self):
        branch = Branch.objects.create(
            name=f"stale-merged-branch-{uuid4().hex[:12]}",
            schema_id=f"stale_merged_branch_{uuid4().hex[:12]}",
            status=BranchStatusChoices.MERGING,
        )
        ingestion = ForwardIngestion.objects.create(
            sync=self.sync,
            snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
            snapshot_id="snapshot-stale-merged-branch",
            branch=branch,
        )
        Branch.objects.filter(pk=branch.pk).update(status=BranchStatusChoices.MERGED)
        branch.status = BranchStatusChoices.MERGING

        with (
            patch("forward_netbox.utilities.merge.merge_branch"),
            patch(
                "forward_netbox.utilities.ingestion_merge.suppress_branch_merge_side_effect_signals",
                return_value=nullcontext(),
            ),
        ):
            sync_merge_ingestion(ingestion)

        self.assertFalse(Branch.objects.filter(pk=branch.pk).exists())
        ingestion.refresh_from_db()
        self.assertIsNone(ingestion.branch)
        self.sync.refresh_from_db()
        self.assertEqual(self.sync.status, ForwardSyncStatusChoices.COMPLETED)

    def test_can_queue_merge_falls_back_to_ledger_step(self):
        branch = Branch.objects.create(
            name=f"ledger-mergeable-{uuid4().hex[:12]}",
            schema_id=f"ledger_mergeable_{uuid4().hex[:12]}",
        )
        ingestion = ForwardIngestion.objects.create(
            sync=self.sync,
            snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
            snapshot_id="snapshot-ledger-mergeable",
            branch=branch,
        )
        run = ForwardExecutionRun.objects.create(
            sync=self.sync,
            source=self.source,
            backend="branching",
            status=ForwardExecutionRunStatusChoices.WAITING,
            snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
            snapshot_id="snapshot-ledger-mergeable",
            total_steps=1,
            next_step_index=1,
        )
        ForwardExecutionStep.objects.create(
            run=run,
            index=1,
            status=ForwardExecutionStepStatusChoices.STAGED,
            model_string="dcim.site",
            ingestion=ingestion,
            branch=branch,
        )

        self.sync.clear_branch_run_state()
        self.sync.status = ForwardSyncStatusChoices.COMPLETED
        self.sync.save(update_fields=["status", "parameters"])

        self.assertTrue(ingestion.can_queue_merge)

    def test_can_queue_merge_ignores_stale_compatibility_state_when_ledger_history_exists(
        self,
    ):
        branch = Branch.objects.create(
            name=f"ledger-stale-merge-{uuid4().hex[:12]}",
            schema_id=f"ledger_stale_merge_{uuid4().hex[:12]}",
        )
        ingestion = ForwardIngestion.objects.create(
            sync=self.sync,
            snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
            snapshot_id="snapshot-ledger-stale-merge",
            branch=branch,
        )
        ForwardExecutionRun.objects.create(
            sync=self.sync,
            source=self.source,
            backend="branching",
            status=ForwardExecutionRunStatusChoices.COMPLETED,
            snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
            snapshot_id="snapshot-ledger-stale-merge",
            total_steps=1,
            next_step_index=2,
        )
        self.sync.set_branch_run_state(
            {
                "awaiting_merge": True,
                "pending_ingestion_id": ingestion.pk,
                "pending_plan_index": 1,
            }
        )
        self.sync.status = ForwardSyncStatusChoices.COMPLETED
        self.sync.save(update_fields=["status"])

        self.assertFalse(ingestion.can_queue_merge)

    def test_sync_merge_ingestion_updates_ledger_without_branch_state(self):
        branch = Branch.objects.create(
            name=f"ledger-merge-{uuid4().hex[:12]}",
            schema_id=f"ledger_merge_{uuid4().hex[:12]}",
        )
        ingestion = ForwardIngestion.objects.create(
            sync=self.sync,
            snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
            snapshot_id="snapshot-ledger",
            branch=branch,
        )
        run = ForwardExecutionRun.objects.create(
            sync=self.sync,
            source=self.source,
            backend="branching",
            status=ForwardExecutionRunStatusChoices.WAITING,
            snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
            snapshot_id="snapshot-ledger",
            total_steps=2,
            next_step_index=1,
            auto_merge=True,
        )
        step = ForwardExecutionStep.objects.create(
            run=run,
            index=1,
            status=ForwardExecutionStepStatusChoices.STAGED,
            model_string="dcim.site",
            ingestion=ingestion,
            branch=branch,
        )
        self.sync.clear_branch_run_state()

        with (
            patch("forward_netbox.utilities.merge.merge_branch"),
            patch(
                "forward_netbox.utilities.ingestion_merge.suppress_branch_merge_side_effect_signals",
                return_value=nullcontext(),
            ),
        ):
            sync_merge_ingestion(ingestion, remove_branch=False)

        step.refresh_from_db()
        run.refresh_from_db()
        ingestion.refresh_from_db()

        self.assertEqual(step.status, ForwardExecutionStepStatusChoices.MERGED)
        self.assertEqual(run.status, ForwardExecutionRunStatusChoices.RUNNING)
        self.assertEqual(run.phase, "merged")
        self.assertEqual(run.next_step_index, 2)
        self.assertFalse(run.baseline_ready)
        self.assertFalse(ingestion.baseline_ready)
        self.sync.refresh_from_db()
        self.assertEqual(self.sync.status, ForwardSyncStatusChoices.SYNCING)
        self.assertEqual(self.sync.get_branch_run_state(), {})

    def test_sync_merge_ingestion_marks_final_ledger_step_baseline_ready(self):
        branch = Branch.objects.create(
            name=f"ledger-final-{uuid4().hex[:12]}",
            schema_id=f"ledger_final_{uuid4().hex[:12]}",
        )
        ingestion = ForwardIngestion.objects.create(
            sync=self.sync,
            snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
            snapshot_id="snapshot-ledger-final",
            branch=branch,
        )
        run = ForwardExecutionRun.objects.create(
            sync=self.sync,
            source=self.source,
            backend="branching",
            status=ForwardExecutionRunStatusChoices.WAITING,
            snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
            snapshot_id="snapshot-ledger-final",
            total_steps=1,
            next_step_index=1,
        )
        step = ForwardExecutionStep.objects.create(
            run=run,
            index=1,
            status=ForwardExecutionStepStatusChoices.STAGED,
            model_string="dcim.site",
            ingestion=ingestion,
            branch=branch,
        )
        self.sync.clear_branch_run_state()

        with (
            patch("forward_netbox.utilities.merge.merge_branch"),
            patch(
                "forward_netbox.utilities.ingestion_merge.suppress_branch_merge_side_effect_signals",
                return_value=nullcontext(),
            ),
        ):
            sync_merge_ingestion(ingestion, remove_branch=False)

        step.refresh_from_db()
        run.refresh_from_db()
        ingestion.refresh_from_db()

        self.assertEqual(step.status, ForwardExecutionStepStatusChoices.MERGED)
        self.assertEqual(run.status, ForwardExecutionRunStatusChoices.COMPLETED)
        self.assertTrue(run.baseline_ready)
        self.assertTrue(ingestion.baseline_ready)
        self.assertEqual(self.sync.get_branch_run_state(), {})

    def test_sync_merge_ingestion_does_not_complete_out_of_order_final_step(self):
        branch = Branch.objects.create(
            name=f"ledger-final-gap-{uuid4().hex[:12]}",
            schema_id=f"ledger_final_gap_{uuid4().hex[:12]}",
        )
        ingestion = ForwardIngestion.objects.create(
            sync=self.sync,
            snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
            snapshot_id="snapshot-ledger-final-gap",
            branch=branch,
        )
        run = ForwardExecutionRun.objects.create(
            sync=self.sync,
            source=self.source,
            backend="branching",
            status=ForwardExecutionRunStatusChoices.WAITING,
            snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
            snapshot_id="snapshot-ledger-final-gap",
            total_steps=3,
            next_step_index=3,
            auto_merge=True,
        )
        ForwardExecutionStep.objects.create(
            run=run,
            index=1,
            status=ForwardExecutionStepStatusChoices.MERGED,
            model_string="dcim.site",
        )
        pending_step = ForwardExecutionStep.objects.create(
            run=run,
            index=2,
            status=ForwardExecutionStepStatusChoices.PENDING,
            model_string="dcim.device",
        )
        final_step = ForwardExecutionStep.objects.create(
            run=run,
            index=3,
            status=ForwardExecutionStepStatusChoices.STAGED,
            model_string="dcim.interface",
            ingestion=ingestion,
            branch=branch,
        )
        self.sync.clear_branch_run_state()

        with (
            patch("forward_netbox.utilities.merge.merge_branch"),
            patch(
                "forward_netbox.utilities.ingestion_merge.suppress_branch_merge_side_effect_signals",
                return_value=nullcontext(),
            ),
        ):
            sync_merge_ingestion(ingestion, remove_branch=False)

        final_step.refresh_from_db()
        pending_step.refresh_from_db()
        run.refresh_from_db()
        ingestion.refresh_from_db()
        self.sync.refresh_from_db()

        self.assertEqual(final_step.status, ForwardExecutionStepStatusChoices.MERGED)
        self.assertEqual(pending_step.status, ForwardExecutionStepStatusChoices.PENDING)
        self.assertEqual(run.status, ForwardExecutionRunStatusChoices.RUNNING)
        self.assertEqual(run.next_step_index, 2)
        self.assertFalse(run.baseline_ready)
        self.assertFalse(ingestion.baseline_ready)
        self.assertEqual(self.sync.status, ForwardSyncStatusChoices.SYNCING)

    def test_maybe_enqueue_next_branch_stage_uses_ledger_without_branch_run_json(self):
        branch = Branch.objects.create(
            name=f"ledger-next-{uuid4().hex[:12]}",
            schema_id=f"ledger_next_{uuid4().hex[:12]}",
        )
        ingestion = ForwardIngestion.objects.create(
            sync=self.sync,
            snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
            snapshot_id="snapshot-ledger-next",
            branch=branch,
        )
        run = ForwardExecutionRun.objects.create(
            sync=self.sync,
            source=self.source,
            backend="branching",
            status=ForwardExecutionRunStatusChoices.RUNNING,
            snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
            snapshot_id="snapshot-ledger-next",
            auto_merge=True,
            total_steps=2,
            next_step_index=2,
        )
        ForwardExecutionStep.objects.create(
            run=run,
            index=1,
            status=ForwardExecutionStepStatusChoices.MERGED,
            model_string="dcim.site",
            ingestion=ingestion,
            branch=branch,
            branch_name=branch.name,
        )
        step = ForwardExecutionStep.objects.create(
            run=run,
            index=2,
            status=ForwardExecutionStepStatusChoices.PENDING,
            model_string="dcim.device",
            label="dcim.device shard",
            query_name="Forward Devices",
            execution_mode="query_id",
            execution_value="query-device",
            shard_keys=["device:one"],
        )
        self.sync.clear_branch_run_state()
        queued_job = Job.objects.create(
            object_type=ContentType.objects.get_for_model(ForwardSync),
            object_id=self.sync.pk,
            name="ledger next stage",
            user=None,
            status=JobStatusChoices.STATUS_PENDING,
            job_id=uuid4(),
            created=timezone.now(),
            data={},
        )

        with patch(
            "forward_netbox.utilities.resumable_branching.Job.enqueue",
            return_value=queued_job,
        ) as mock_enqueue:
            job = maybe_enqueue_next_branch_stage(ingestion, user=None)

        self.assertEqual(job.pk, queued_job.pk)
        mock_enqueue.assert_called_once()
        self.sync.refresh_from_db()
        run.refresh_from_db()
        step.refresh_from_db()
        self.assertEqual(self.sync.get_branch_run_state(), {})
        self.assertEqual(step.status, ForwardExecutionStepStatusChoices.QUEUED)
        self.assertEqual(step.job_id, queued_job.pk)
        self.assertEqual(run.phase, "queued")
        self.assertEqual(run.next_step_index, 2)

    def test_maybe_enqueue_next_branch_stage_queues_merge_for_prestaged_step(self):
        previous_branch = Branch.objects.create(
            name=f"ledger-overlap-prev-{uuid4().hex[:12]}",
            schema_id=f"ledger_overlap_prev_{uuid4().hex[:12]}",
        )
        previous_ingestion = ForwardIngestion.objects.create(
            sync=self.sync,
            snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
            snapshot_id="snapshot-ledger-overlap",
            branch=previous_branch,
        )
        staged_branch = Branch.objects.create(
            name=f"ledger-overlap-next-{uuid4().hex[:12]}",
            schema_id=f"ledger_overlap_next_{uuid4().hex[:12]}",
        )
        staged_ingestion = ForwardIngestion.objects.create(
            sync=self.sync,
            snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
            snapshot_id="snapshot-ledger-overlap",
            branch=staged_branch,
        )
        run = ForwardExecutionRun.objects.create(
            sync=self.sync,
            source=self.source,
            backend="branching",
            status=ForwardExecutionRunStatusChoices.RUNNING,
            snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
            snapshot_id="snapshot-ledger-overlap",
            auto_merge=True,
            total_steps=2,
            next_step_index=2,
        )
        ForwardExecutionStep.objects.create(
            run=run,
            index=1,
            status=ForwardExecutionStepStatusChoices.MERGED,
            model_string="dcim.site",
            ingestion=previous_ingestion,
            branch=previous_branch,
            branch_name=previous_branch.name,
        )
        staged_step = ForwardExecutionStep.objects.create(
            run=run,
            index=2,
            status=ForwardExecutionStepStatusChoices.STAGED,
            model_string="dcim.device",
            ingestion=staged_ingestion,
            branch=staged_branch,
            branch_name=staged_branch.name,
        )
        queued_job = Job.objects.create(
            object_type=ContentType.objects.get_for_model(ForwardIngestion),
            object_id=staged_ingestion.pk,
            name="ledger overlap staged merge",
            user=None,
            status=JobStatusChoices.STATUS_PENDING,
            job_id=uuid4(),
            created=timezone.now(),
            data={},
        )

        with patch(
            "forward_netbox.utilities.ingestion_merge.Job.enqueue",
            return_value=queued_job,
        ) as mock_enqueue:
            job = maybe_enqueue_next_branch_stage(previous_ingestion, user=None)

        self.assertEqual(job.pk, queued_job.pk)
        mock_enqueue.assert_called_once()
        staged_ingestion.refresh_from_db()
        staged_step.refresh_from_db()
        run.refresh_from_db()
        self.sync.refresh_from_db()
        self.assertEqual(staged_ingestion.merge_job_id, queued_job.pk)
        self.assertEqual(
            staged_step.status,
            ForwardExecutionStepStatusChoices.MERGE_QUEUED,
        )
        self.assertEqual(run.status, ForwardExecutionRunStatusChoices.RUNNING)
        self.assertEqual(self.sync.status, ForwardSyncStatusChoices.QUEUED)
        self.assertEqual(staged_step.merge_job_id, queued_job.pk)

    def test_maybe_enqueue_next_branch_stage_queues_merge_when_run_waiting(self):
        previous_branch = Branch.objects.create(
            name=f"ledger-waiting-prev-{uuid4().hex[:12]}",
            schema_id=f"ledger_waiting_prev_{uuid4().hex[:12]}",
        )
        previous_ingestion = ForwardIngestion.objects.create(
            sync=self.sync,
            snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
            snapshot_id="snapshot-ledger-waiting",
            branch=previous_branch,
        )
        staged_branch = Branch.objects.create(
            name=f"ledger-waiting-next-{uuid4().hex[:12]}",
            schema_id=f"ledger_waiting_next_{uuid4().hex[:12]}",
        )
        staged_ingestion = ForwardIngestion.objects.create(
            sync=self.sync,
            snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
            snapshot_id="snapshot-ledger-waiting",
            branch=staged_branch,
        )
        run = ForwardExecutionRun.objects.create(
            sync=self.sync,
            source=self.source,
            backend="branching",
            status=ForwardExecutionRunStatusChoices.WAITING,
            snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
            snapshot_id="snapshot-ledger-waiting",
            auto_merge=True,
            total_steps=2,
            next_step_index=2,
        )
        ForwardExecutionStep.objects.create(
            run=run,
            index=1,
            status=ForwardExecutionStepStatusChoices.MERGED,
            model_string="dcim.site",
            ingestion=previous_ingestion,
            branch=previous_branch,
            branch_name=previous_branch.name,
        )
        staged_step = ForwardExecutionStep.objects.create(
            run=run,
            index=2,
            status=ForwardExecutionStepStatusChoices.STAGED,
            model_string="dcim.device",
            ingestion=staged_ingestion,
            branch=staged_branch,
            branch_name=staged_branch.name,
        )
        queued_job = Job.objects.create(
            object_type=ContentType.objects.get_for_model(ForwardIngestion),
            object_id=staged_ingestion.pk,
            name="ledger waiting staged merge",
            user=None,
            status=JobStatusChoices.STATUS_PENDING,
            job_id=uuid4(),
            created=timezone.now(),
            data={},
        )

        with patch(
            "forward_netbox.utilities.ingestion_merge.Job.enqueue",
            return_value=queued_job,
        ) as mock_enqueue:
            job = maybe_enqueue_next_branch_stage(previous_ingestion, user=None)

        self.assertEqual(job.pk, queued_job.pk)
        mock_enqueue.assert_called_once()
        staged_ingestion.refresh_from_db()
        staged_step.refresh_from_db()
        run.refresh_from_db()
        self.sync.refresh_from_db()
        self.assertEqual(staged_ingestion.merge_job_id, queued_job.pk)
        self.assertEqual(
            staged_step.status,
            ForwardExecutionStepStatusChoices.MERGE_QUEUED,
        )
        self.assertEqual(run.status, ForwardExecutionRunStatusChoices.RUNNING)
        self.assertEqual(self.sync.status, ForwardSyncStatusChoices.QUEUED)
        self.assertEqual(staged_step.merge_job_id, queued_job.pk)

    def test_maybe_enqueue_next_branch_stage_requires_opt_in_for_failed_run_recovery(
        self,
    ):
        previous_branch = Branch.objects.create(
            name=f"ledger-failed-prev-{uuid4().hex[:12]}",
            schema_id=f"ledger_failed_prev_{uuid4().hex[:12]}",
        )
        previous_ingestion = ForwardIngestion.objects.create(
            sync=self.sync,
            snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
            snapshot_id="snapshot-ledger-failed",
            branch=previous_branch,
        )
        staged_branch = Branch.objects.create(
            name=f"ledger-failed-next-{uuid4().hex[:12]}",
            schema_id=f"ledger_failed_next_{uuid4().hex[:12]}",
        )
        staged_ingestion = ForwardIngestion.objects.create(
            sync=self.sync,
            snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
            snapshot_id="snapshot-ledger-failed",
            branch=staged_branch,
        )
        run = ForwardExecutionRun.objects.create(
            sync=self.sync,
            source=self.source,
            backend="branching",
            status=ForwardExecutionRunStatusChoices.FAILED,
            phase="failed",
            snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
            snapshot_id="snapshot-ledger-failed",
            auto_merge=True,
            total_steps=2,
            next_step_index=2,
        )
        ForwardExecutionStep.objects.create(
            run=run,
            index=1,
            status=ForwardExecutionStepStatusChoices.MERGED,
            model_string="dcim.site",
            ingestion=previous_ingestion,
            branch=previous_branch,
            branch_name=previous_branch.name,
        )
        staged_step = ForwardExecutionStep.objects.create(
            run=run,
            index=2,
            status=ForwardExecutionStepStatusChoices.STAGED,
            model_string="dcim.device",
            ingestion=staged_ingestion,
            branch=staged_branch,
            branch_name=staged_branch.name,
        )
        queued_job = Job.objects.create(
            object_type=ContentType.objects.get_for_model(ForwardIngestion),
            object_id=staged_ingestion.pk,
            name="failed run staged merge",
            user=None,
            status=JobStatusChoices.STATUS_PENDING,
            job_id=uuid4(),
            created=timezone.now(),
            data={},
        )

        with patch(
            "forward_netbox.utilities.ingestion_merge.Job.enqueue",
            return_value=queued_job,
        ) as mock_enqueue:
            self.assertIsNone(
                maybe_enqueue_next_branch_stage(previous_ingestion, user=None)
            )
            job = maybe_enqueue_next_branch_stage(
                previous_ingestion,
                user=None,
                allow_failed_recovery=True,
            )

        self.assertEqual(job.pk, queued_job.pk)
        mock_enqueue.assert_called_once()
        staged_ingestion.refresh_from_db()
        staged_step.refresh_from_db()
        self.assertEqual(staged_ingestion.merge_job_id, queued_job.pk)
        self.assertEqual(
            staged_step.status,
            ForwardExecutionStepStatusChoices.MERGE_QUEUED,
        )

    def test_maybe_enqueue_next_branch_stage_retries_pending_step_for_failed_run(self):
        branch = Branch.objects.create(
            name=f"ledger-failed-step-{uuid4().hex[:12]}",
            schema_id=f"ledger_failed_step_{uuid4().hex[:12]}",
        )
        ingestion = ForwardIngestion.objects.create(
            sync=self.sync,
            snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
            snapshot_id="snapshot-ledger-failed-step",
            branch=branch,
        )
        run = ForwardExecutionRun.objects.create(
            sync=self.sync,
            source=self.source,
            backend="branching",
            status=ForwardExecutionRunStatusChoices.FAILED,
            phase="failed",
            snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
            snapshot_id="snapshot-ledger-failed-step",
            auto_merge=True,
            total_steps=2,
            next_step_index=2,
        )
        ForwardExecutionStep.objects.create(
            run=run,
            index=1,
            status=ForwardExecutionStepStatusChoices.MERGED,
            model_string="dcim.site",
            ingestion=ingestion,
            branch=branch,
            branch_name=branch.name,
        )
        step = ForwardExecutionStep.objects.create(
            run=run,
            index=2,
            status=ForwardExecutionStepStatusChoices.PENDING,
            model_string="dcim.device",
            label="dcim.device failed shard",
            query_name="Forward Devices",
            execution_mode="query_id",
            execution_value="query-device",
            shard_keys=["device:one"],
        )
        queued_job = Job.objects.create(
            object_type=ContentType.objects.get_for_model(ForwardSync),
            object_id=self.sync.pk,
            name="failed run stage retry",
            user=None,
            status=JobStatusChoices.STATUS_PENDING,
            job_id=uuid4(),
            created=timezone.now(),
            data={},
        )

        with patch(
            "forward_netbox.utilities.resumable_branching.Job.enqueue",
            return_value=queued_job,
        ) as mock_enqueue:
            job = maybe_enqueue_next_branch_stage(
                ingestion,
                user=None,
                allow_failed_recovery=True,
            )

        self.assertEqual(job.pk, queued_job.pk)
        mock_enqueue.assert_called_once()
        step.refresh_from_db()
        run.refresh_from_db()
        self.sync.refresh_from_db()
        self.assertEqual(step.status, ForwardExecutionStepStatusChoices.QUEUED)
        self.assertEqual(step.job_id, queued_job.pk)
        self.assertEqual(run.status, ForwardExecutionRunStatusChoices.RUNNING)
        self.assertEqual(self.sync.status, ForwardSyncStatusChoices.QUEUED)

    def test_maybe_enqueue_next_branch_stage_falls_back_without_ingestion_step_link(
        self,
    ):
        branch = Branch.objects.create(
            name=f"ledger-fallback-{uuid4().hex[:12]}",
            schema_id=f"ledger_fallback_{uuid4().hex[:12]}",
        )
        ingestion = ForwardIngestion.objects.create(
            sync=self.sync,
            snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
            snapshot_id="snapshot-ledger-fallback",
            branch=branch,
        )
        run = ForwardExecutionRun.objects.create(
            sync=self.sync,
            source=self.source,
            backend="branching",
            status=ForwardExecutionRunStatusChoices.RUNNING,
            snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
            snapshot_id="snapshot-ledger-fallback",
            auto_merge=True,
            total_steps=3,
            next_step_index=2,
        )
        ForwardExecutionStep.objects.create(
            run=run,
            index=1,
            status=ForwardExecutionStepStatusChoices.MERGED,
            model_string="dcim.site",
            ingestion=None,
        )
        step = ForwardExecutionStep.objects.create(
            run=run,
            index=2,
            status=ForwardExecutionStepStatusChoices.PENDING,
            model_string="dcim.device",
            label="dcim.device shard",
            query_name="Forward Devices",
            execution_mode="query_id",
            execution_value="query-device",
            shard_keys=["device:one"],
            ingestion=None,
        )
        self.sync.clear_branch_run_state()
        queued_job = Job.objects.create(
            object_type=ContentType.objects.get_for_model(ForwardSync),
            object_id=self.sync.pk,
            name="ledger fallback stage",
            user=None,
            status=JobStatusChoices.STATUS_PENDING,
            job_id=uuid4(),
            created=timezone.now(),
            data={},
        )

        with patch(
            "forward_netbox.utilities.resumable_branching.Job.enqueue",
            return_value=queued_job,
        ) as mock_enqueue:
            job = maybe_enqueue_next_branch_stage(ingestion, user=None)

        self.assertEqual(job.pk, queued_job.pk)
        mock_enqueue.assert_called_once()
        step.refresh_from_db()
        self.assertEqual(step.status, ForwardExecutionStepStatusChoices.QUEUED)
        self.assertEqual(step.job_id, queued_job.pk)

    def test_maybe_enqueue_next_branch_stage_prefers_ledger_when_compat_state_is_stale(
        self,
    ):
        branch = Branch.objects.create(
            name=f"ledger-stale-state-{uuid4().hex[:12]}",
            schema_id=f"ledger_stale_state_{uuid4().hex[:12]}",
        )
        ingestion = ForwardIngestion.objects.create(
            sync=self.sync,
            snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
            snapshot_id="snapshot-ledger-stale-state",
            branch=branch,
        )
        run = ForwardExecutionRun.objects.create(
            sync=self.sync,
            source=self.source,
            backend="branching",
            status=ForwardExecutionRunStatusChoices.RUNNING,
            snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
            snapshot_id="snapshot-ledger-stale-state",
            auto_merge=True,
            total_steps=2,
            next_step_index=2,
        )
        step = ForwardExecutionStep.objects.create(
            run=run,
            index=2,
            status=ForwardExecutionStepStatusChoices.PENDING,
            model_string="dcim.device",
            label="dcim.device shard",
            query_name="Forward Devices",
            execution_mode="query_id",
            execution_value="query-device",
            shard_keys=["device:one"],
        )
        self.sync.set_branch_run_state(
            {
                "auto_merge": False,
                "next_plan_index": 1,
                "total_plan_items": 1,
                "pending_ingestion_id": ingestion.pk,
            }
        )
        queued_job = Job.objects.create(
            object_type=ContentType.objects.get_for_model(ForwardSync),
            object_id=self.sync.pk,
            name="ledger stale-state stage",
            user=None,
            status=JobStatusChoices.STATUS_PENDING,
            job_id=uuid4(),
            created=timezone.now(),
            data={},
        )

        with patch(
            "forward_netbox.utilities.resumable_branching.Job.enqueue",
            return_value=queued_job,
        ) as mock_enqueue:
            job = maybe_enqueue_next_branch_stage(ingestion, user=None)

        self.assertEqual(job.pk, queued_job.pk)
        mock_enqueue.assert_called_once()
        step.refresh_from_db()
        self.assertEqual(step.status, ForwardExecutionStepStatusChoices.QUEUED)
        self.assertEqual(step.job_id, queued_job.pk)

    def test_maybe_enqueue_next_branch_stage_skips_completed_history_stale_compat(
        self,
    ):
        branch = Branch.objects.create(
            name=f"ledger-stale-{uuid4().hex[:12]}",
            schema_id=f"ledger_stale_{uuid4().hex[:12]}",
        )
        ingestion = ForwardIngestion.objects.create(
            sync=self.sync,
            snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
            snapshot_id="snapshot-ledger-stale",
            branch=branch,
        )
        ForwardExecutionRun.objects.create(
            sync=self.sync,
            source=self.source,
            backend="branching",
            status=ForwardExecutionRunStatusChoices.COMPLETED,
            snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
            snapshot_id="snapshot-ledger-stale",
            auto_merge=True,
            total_steps=1,
            next_step_index=2,
        )
        self.sync.set_branch_run_state(
            {
                "auto_merge": True,
                "next_plan_index": 1,
                "total_plan_items": 1,
                "plan_items": [{"index": 1, "status": "queued"}],
            }
        )

        with patch(
            "forward_netbox.utilities.resumable_branching.Job.enqueue"
        ) as mock_enqueue:
            job = maybe_enqueue_next_branch_stage(ingestion, user=None)

        self.assertIsNone(job)
        mock_enqueue.assert_not_called()

    def test_maybe_enqueue_next_branch_stage_auto_requeues_merge_timeout_within_budget(
        self,
    ):
        branch = Branch.objects.create(
            name=f"ledger-merge-timeout-{uuid4().hex[:12]}",
            schema_id=f"ledger_merge_timeout_{uuid4().hex[:12]}",
        )
        ingestion = ForwardIngestion.objects.create(
            sync=self.sync,
            snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
            snapshot_id="snapshot-ledger-merge-timeout",
            branch=branch,
        )
        run = ForwardExecutionRun.objects.create(
            sync=self.sync,
            source=self.source,
            backend="branching",
            status=ForwardExecutionRunStatusChoices.RUNNING,
            snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
            snapshot_id="snapshot-ledger-merge-timeout",
            auto_merge=True,
            total_steps=1,
            next_step_index=1,
        )
        step = ForwardExecutionStep.objects.create(
            run=run,
            index=1,
            status=ForwardExecutionStepStatusChoices.MERGE_TIMEOUT,
            model_string="dcim.site",
            ingestion=ingestion,
            branch=branch,
            retry_count=max(0, AUTO_MERGE_STALE_MERGE_REQUEUE_LIMIT - 1),
        )
        self.sync.clear_branch_run_state()
        queued_job = Job.objects.create(
            object_type=ContentType.objects.get_for_model(ForwardIngestion),
            object_id=ingestion.pk,
            name="ledger merge-timeout auto-requeue",
            user=None,
            status=JobStatusChoices.STATUS_PENDING,
            job_id=uuid4(),
            created=timezone.now(),
            data={},
        )

        with patch(
            "forward_netbox.utilities.ingestion_merge.Job.enqueue",
            return_value=queued_job,
        ) as mock_enqueue:
            job = maybe_enqueue_next_branch_stage(ingestion, user=None)

        self.assertEqual(job.pk, queued_job.pk)
        mock_enqueue.assert_called_once()
        ingestion.refresh_from_db()
        step.refresh_from_db()
        self.assertEqual(ingestion.merge_job_id, queued_job.pk)
        self.assertEqual(step.status, ForwardExecutionStepStatusChoices.MERGE_TIMEOUT)

    def test_maybe_enqueue_next_branch_stage_skips_merge_timeout_auto_requeue_over_budget(
        self,
    ):
        branch = Branch.objects.create(
            name=f"ledger-merge-timeout-over-{uuid4().hex[:12]}",
            schema_id=f"ledger_merge_timeout_over_{uuid4().hex[:12]}",
        )
        ingestion = ForwardIngestion.objects.create(
            sync=self.sync,
            snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
            snapshot_id="snapshot-ledger-merge-timeout-over",
            branch=branch,
        )
        run = ForwardExecutionRun.objects.create(
            sync=self.sync,
            source=self.source,
            backend="branching",
            status=ForwardExecutionRunStatusChoices.RUNNING,
            snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
            snapshot_id="snapshot-ledger-merge-timeout-over",
            auto_merge=True,
            total_steps=1,
            next_step_index=2,
        )
        step = ForwardExecutionStep.objects.create(
            run=run,
            index=1,
            status=ForwardExecutionStepStatusChoices.MERGE_TIMEOUT,
            model_string="dcim.site",
            ingestion=ingestion,
            branch=branch,
            retry_count=AUTO_MERGE_STALE_MERGE_REQUEUE_LIMIT + 1,
        )
        self.sync.clear_branch_run_state()

        with patch(
            "forward_netbox.utilities.ingestion_merge.Job.enqueue"
        ) as mock_enqueue:
            job = maybe_enqueue_next_branch_stage(ingestion, user=None)

        self.assertIsNone(job)
        mock_enqueue.assert_not_called()
        step.refresh_from_db()
        self.assertEqual(step.status, ForwardExecutionStepStatusChoices.MERGE_TIMEOUT)

    def test_sync_merge_ingestion_keeps_syncing_when_pending_ledger_steps_exist(self):
        branch = Branch.objects.create(
            name=f"ledger-pending-{uuid4().hex[:12]}",
            schema_id=f"ledger_pending_{uuid4().hex[:12]}",
        )
        ingestion = ForwardIngestion.objects.create(
            sync=self.sync,
            snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
            snapshot_id="snapshot-ledger-pending",
            branch=branch,
        )
        run = ForwardExecutionRun.objects.create(
            sync=self.sync,
            source=self.source,
            backend="branching",
            status=ForwardExecutionRunStatusChoices.WAITING,
            snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
            snapshot_id="snapshot-ledger-pending",
            total_steps=179,
            next_step_index=15,
            auto_merge=True,
        )
        merged_step = ForwardExecutionStep.objects.create(
            run=run,
            index=14,
            status=ForwardExecutionStepStatusChoices.STAGED,
            model_string="ipam.ipaddress",
            ingestion=ingestion,
            branch=branch,
        )
        ForwardExecutionStep.objects.create(
            run=run,
            index=15,
            status=ForwardExecutionStepStatusChoices.PENDING,
            model_string="ipam.ipaddress",
        )
        self.sync.clear_branch_run_state()
        self.assertTrue(has_pending_branch_run(self.sync))

        with (
            patch("forward_netbox.utilities.merge.merge_branch"),
            patch(
                "forward_netbox.utilities.ingestion_merge.suppress_branch_merge_side_effect_signals",
                return_value=nullcontext(),
            ),
        ):
            sync_merge_ingestion(ingestion, remove_branch=False)

        self.sync.refresh_from_db()
        run.refresh_from_db()
        merged_step.refresh_from_db()

        self.assertEqual(merged_step.status, ForwardExecutionStepStatusChoices.MERGED)
        self.assertEqual(run.status, ForwardExecutionRunStatusChoices.RUNNING)
        self.assertEqual(run.next_step_index, 15)
        self.assertEqual(self.sync.status, ForwardSyncStatusChoices.SYNCING)

    @patch("forward_netbox.models.ForwardIngestion.objects.filter")
    @patch("forward_netbox.utilities.ingestion_merge.Job.enqueue")
    def test_enqueue_merge_job_persists_merge_job_reference(
        self,
        mock_enqueue,
        mock_filter,
    ):
        ingestion = ForwardIngestion.objects.create(
            sync=self.sync,
            snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
            snapshot_id="snapshot-merge",
        )
        mock_enqueue.return_value = Job.objects.create(
            object_type=ContentType.objects.get_for_model(ForwardIngestion),
            object_id=ingestion.pk,
            name=f"{ingestion.sync.name} Merge",
            user=None,
            status=JobStatusChoices.STATUS_COMPLETED,
            job_id=uuid4(),
            created=timezone.now(),
            started=timezone.now(),
            completed=timezone.now(),
            data={},
        )
        mock_filter.return_value.update.return_value = 1

        job = enqueue_merge_job(ingestion, user=None, remove_branch=True)

        self.assertEqual(self.sync.status, ForwardSyncStatusChoices.QUEUED)
        self.assertEqual(ingestion.merge_job, job)
        mock_enqueue.assert_called_once()
        mock_filter.assert_called()

    def test_record_change_totals_persists_counts(self):
        ingestion = ForwardIngestion.objects.create(
            sync=self.sync,
            snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
            snapshot_id="snapshot-counts",
        )

        record_change_totals(
            ingestion,
            applied=12,
            failed=3,
            created=4,
            updated=5,
            deleted=6,
        )

        ingestion.refresh_from_db()
        self.assertEqual(ingestion.applied_change_count, 12)
        self.assertEqual(ingestion.failed_change_count, 3)
        self.assertEqual(ingestion.created_change_count, 4)
        self.assertEqual(ingestion.updated_change_count, 5)
        self.assertEqual(ingestion.deleted_change_count, 6)

    def test_merge_progress_updates_ledger_heartbeat_and_sparse_log(self):
        from forward_netbox.utilities.merge import _report_merge_progress

        ingestion = ForwardIngestion.objects.create(
            sync=self.sync,
            snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
            snapshot_id="snapshot-merge-progress",
        )
        sync_logger = MagicMock()

        with (
            patch(
                "forward_netbox.utilities.execution_ledger.touch_execution_step_progress"
            ) as mock_touch_progress,
            patch("forward_netbox.utilities.merge.time.monotonic", return_value=101.0),
        ):
            heartbeat_at, log_at = _report_merge_progress(
                ingestion,
                sync_logger=sync_logger,
                model_string="ipam.prefix",
                step_index=47,
                processed=5000,
                total_changes=9295,
                last_heartbeat_at=100.0,
                last_log_at=100.0,
            )

        mock_touch_progress.assert_called_once_with(
            self.sync,
            model_string="ipam.prefix",
            shard_index=47,
        )
        sync_logger.log_info.assert_called_once_with(
            "Merged 5000/9295 branch changes (current model `ipam.prefix`)."
        )
        self.assertEqual(heartbeat_at, 101.0)
        self.assertEqual(log_at, 101.0)

    def test_merge_progress_skips_noisy_updates_between_intervals(self):
        from forward_netbox.utilities.merge import _report_merge_progress

        ingestion = ForwardIngestion.objects.create(
            sync=self.sync,
            snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
            snapshot_id="snapshot-merge-progress-quiet",
        )
        sync_logger = MagicMock()

        with (
            patch(
                "forward_netbox.utilities.execution_ledger.touch_execution_step_progress"
            ) as mock_touch_progress,
            patch("forward_netbox.utilities.merge.time.monotonic", return_value=101.0),
        ):
            heartbeat_at, log_at = _report_merge_progress(
                ingestion,
                sync_logger=sync_logger,
                model_string="ipam.prefix",
                step_index=47,
                processed=999,
                total_changes=9295,
                last_heartbeat_at=100.0,
                last_log_at=100.0,
            )

        mock_touch_progress.assert_not_called()
        sync_logger.log_info.assert_not_called()
        self.assertEqual(heartbeat_at, 100.0)
        self.assertEqual(log_at, 100.0)


class MergeIssueRecorderTest(TestCase):
    def setUp(self):
        self.source = ForwardSource.objects.create(
            name="source-merge-issue-recorder",
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
            name="sync-merge-issue-recorder",
            source=self.source,
            parameters={"snapshot_id": LATEST_PROCESSED_SNAPSHOT, "dcim.module": True},
        )
        self.ingestion = ForwardIngestion.objects.create(
            sync=self.sync,
            snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
            snapshot_id="snapshot-merge-issue",
        )

    def test_module_bay_failures_collapse_into_single_actionable_issue(self):
        from forward_netbox.utilities.merge import _MergeIssueRecorder

        recorder = _MergeIssueRecorder(self.ingestion, None)
        exc = Exception("Save with update_fields did not affect any rows.")
        for index in range(5):
            recorder.record(
                model_string="dcim.modulebay",
                message=f"Failed to apply change {index} (create dcim.modulebay: {index})",
                exc=exc,
            )

        # Nothing is recorded until flush — failures accumulate.
        self.assertEqual(self.ingestion.issues.count(), 0)

        recorder.flush()

        issues = list(self.ingestion.issues.all())
        self.assertEqual(len(issues), 1)
        issue = issues[0]
        self.assertEqual(issue.model, "dcim.modulebay")
        self.assertEqual(issue.exception, "ModuleBayMergeUnsupported")
        self.assertIn("5 module-bay change", issue.message)
        self.assertIn("forward_module_readiness", issue.message)
        self.assertEqual(
            issue.raw_data.get("sample_error"),
            "Save with update_fields did not affect any rows.",
        )

    def test_synced_model_failures_recorded_per_change(self):
        from forward_netbox.utilities.merge import _MergeIssueRecorder

        recorder = _MergeIssueRecorder(self.ingestion, None)
        recorder.record(
            model_string="dcim.device",
            message="Failed to apply change 1 (create dcim.device: 1)",
            exc=ValueError("boom"),
        )
        recorder.record(
            model_string="dcim.device",
            message="Failed to apply change 2 (create dcim.device: 2)",
            exc=ValueError("boom2"),
        )
        recorder.flush()

        device_issues = list(self.ingestion.issues.filter(model="dcim.device"))
        self.assertEqual(len(device_issues), 2)
        self.assertEqual(device_issues[0].exception, "ValueError")
        # No spurious aggregated module-bay issue when none occurred.
        self.assertFalse(
            self.ingestion.issues.filter(exception="ModuleBayMergeUnsupported").exists()
        )
