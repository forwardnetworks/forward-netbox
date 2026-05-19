from contextlib import nullcontext
from unittest.mock import patch
from uuid import uuid4

from core.choices import JobStatusChoices
from core.models import Job
from django.contrib.contenttypes.models import ContentType
from django.test import TestCase
from django.utils import timezone
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
