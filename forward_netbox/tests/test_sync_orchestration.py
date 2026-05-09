from unittest.mock import patch

from core.choices import JobStatusChoices
from core.models import Job
from django.contrib.contenttypes.models import ContentType
from django.test import TestCase
from django.utils import timezone

from forward_netbox.choices import ForwardExecutionBackendChoices
from forward_netbox.choices import ForwardSourceStatusChoices
from forward_netbox.choices import ForwardSyncStatusChoices
from forward_netbox.models import ForwardSource
from forward_netbox.models import ForwardSync
from forward_netbox.utilities.forward_api import LATEST_PROCESSED_SNAPSHOT
from forward_netbox.utilities.sync_orchestration import _finalize_forward_sync
from forward_netbox.utilities.sync_orchestration import _prepare_forward_sync
from forward_netbox.utilities.sync_orchestration import run_forward_sync


class ForwardSyncOrchestrationHelperTest(TestCase):
    def setUp(self):
        self.source = ForwardSource.objects.create(
            name="source-sync-orchestration",
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
            name="sync-sync-orchestration",
            source=self.source,
            parameters={
                "snapshot_id": LATEST_PROCESSED_SNAPSHOT,
                "dcim.device": True,
            },
        )

    @patch("forward_netbox.utilities.multi_branch.ForwardMultiBranchExecutor")
    def test_run_forward_sync_marks_sync_and_source_ready_on_success(
        self,
        mock_executor_class,
    ):
        mock_executor = mock_executor_class.return_value
        mock_executor.run.return_value = []

        run_forward_sync(self.sync)

        self.sync.refresh_from_db()
        self.source.refresh_from_db()

        self.assertEqual(self.sync.status, ForwardSyncStatusChoices.COMPLETED)
        self.assertEqual(self.source.status, ForwardSourceStatusChoices.READY)

    @patch(
        "forward_netbox.utilities.fast_bootstrap_executor.ForwardFastBootstrapExecutor"
    )
    def test_run_forward_sync_uses_fast_bootstrap_executor(
        self,
        mock_executor_class,
    ):
        self.sync.parameters = {
            **self.sync.parameters,
            "execution_backend": ForwardExecutionBackendChoices.FAST_BOOTSTRAP,
        }
        self.sync.save(update_fields=["parameters"])
        mock_executor = mock_executor_class.return_value
        mock_executor.run.return_value = []

        run_forward_sync(self.sync)

        mock_executor.run.assert_called_once_with()
        self.sync.refresh_from_db()
        self.assertEqual(self.sync.status, ForwardSyncStatusChoices.COMPLETED)

    def test_prepare_forward_sync_marks_sync_and_source_syncing(self):
        user = None

        user = _prepare_forward_sync(self.sync)

        self.sync.refresh_from_db()
        self.source.refresh_from_db()

        self.assertIsNone(user)
        self.assertEqual(self.sync.status, ForwardSyncStatusChoices.SYNCING)
        self.assertEqual(self.source.status, ForwardSourceStatusChoices.SYNCING)

    def test_finalize_forward_sync_persists_job_data(self):
        job = Job.objects.create(
            object_type=ContentType.objects.get_for_model(ForwardSync),
            object_id=self.sync.pk,
            name="sync-orchestration-job",
            user=None,
            status=JobStatusChoices.STATUS_COMPLETED,
            job_id="123e4567-e89b-12d3-a456-426614174000",
            created=timezone.now(),
            started=timezone.now(),
            completed=timezone.now(),
            data={},
        )
        self.sync.status = ForwardSyncStatusChoices.COMPLETED

        _finalize_forward_sync(self.sync, job)

        self.sync.refresh_from_db()
        self.source.refresh_from_db()
        job.refresh_from_db()

        self.assertEqual(self.sync.status, ForwardSyncStatusChoices.COMPLETED)
        self.assertEqual(self.source.status, ForwardSourceStatusChoices.READY)
        self.assertEqual(job.data, {"logs": [], "statistics": {}})
