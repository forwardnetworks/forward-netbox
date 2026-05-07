from contextlib import nullcontext
from unittest.mock import patch
from uuid import uuid4

from core.choices import JobStatusChoices
from core.models import Job
from django.contrib.contenttypes.models import ContentType
from django.test import TestCase
from django.utils import timezone

from forward_netbox.choices import ForwardSyncStatusChoices
from forward_netbox.models import ForwardIngestion
from forward_netbox.models import ForwardSource
from forward_netbox.models import ForwardSync
from forward_netbox.utilities.forward_api import LATEST_PROCESSED_SNAPSHOT
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
        ):
            sync_merge_ingestion(ingestion)

        self.sync.refresh_from_db()
        ingestion.refresh_from_db()

        self.assertEqual(self.sync.status, ForwardSyncStatusChoices.COMPLETED)
        self.assertTrue(ingestion.baseline_ready)
        self.assertEqual(self.sync.get_branch_run_state(), {})

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
