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

from forward_netbox.choices import ForwardSyncStatusChoices
from forward_netbox.models import ForwardIngestion
from forward_netbox.models import ForwardSource
from forward_netbox.models import ForwardSync
from forward_netbox.utilities.forward_api import LATEST_PROCESSED_SNAPSHOT
from forward_netbox.utilities.ingestion_merge import cleanup_merged_branch
from forward_netbox.utilities.ingestion_merge import enqueue_merge_job
from forward_netbox.utilities.ingestion_merge import record_change_totals
from forward_netbox.utilities.ingestion_merge import sync_merge_ingestion


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
