from types import SimpleNamespace
from unittest.mock import Mock
from unittest.mock import patch

from core.choices import JobStatusChoices
from core.models import Job
from django.contrib.contenttypes.models import ContentType
from django.core.exceptions import ValidationError
from django.test import override_settings
from django.test import TestCase
from django.utils import timezone

from forward_netbox.choices import ForwardSourceStatusChoices
from forward_netbox.choices import ForwardSyncStatusChoices
from forward_netbox.exceptions import ForwardClientError
from forward_netbox.models import ForwardSource
from forward_netbox.models import ForwardSync
from forward_netbox.utilities.forward_api import LATEST_PROCESSED_SNAPSHOT
from forward_netbox.utilities.logging import SyncLogging
from forward_netbox.utilities.snapshot_freshness import (
    latest_processed_catchup_decision,
)
from forward_netbox.utilities.sync_orchestration import _finalize_forward_sync
from forward_netbox.utilities.sync_orchestration import _prepare_forward_sync
from forward_netbox.utilities.sync_orchestration import _record_forward_api_usage
from forward_netbox.utilities.sync_orchestration import run_forward_sync
from forward_netbox.utilities.sync_orchestration import (
    should_skip_unchanged_snapshot,
)


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

    @patch(
        "forward_netbox.utilities.single_branch_executor.ForwardSingleBranchExecutor"
    )
    def test_run_forward_sync_marks_sync_and_source_ready_on_success(
        self,
        mock_executor_class,
    ):
        mock_executor = mock_executor_class.return_value
        mock_executor.run.return_value = []
        mock_executor.current_ingestion = SimpleNamespace(snapshot_id="snapshot-1")
        mock_executor.client = SimpleNamespace(
            get_latest_processed_snapshot_id=Mock(return_value="snapshot-1")
        )

        run_forward_sync(self.sync)

        self.sync.refresh_from_db()
        self.source.refresh_from_db()

        self.assertEqual(self.sync.status, ForwardSyncStatusChoices.COMPLETED)
        self.assertEqual(self.source.status, ForwardSourceStatusChoices.READY)

    @patch(
        "forward_netbox.utilities.single_branch_executor.ForwardSingleBranchExecutor"
    )
    def test_run_forward_sync_rejects_child_models_without_dcim_device(
        self,
        mock_executor_class,
    ):
        self.sync.parameters = {
            "snapshot_id": LATEST_PROCESSED_SNAPSHOT,
            "dcim.device": False,
            "dcim.interface": True,
        }

        with self.assertRaises(ValidationError) as ctx:
            run_forward_sync(self.sync)

        self.assertIn("dcim.device", str(ctx.exception))
        mock_executor_class.assert_not_called()

    @override_settings(RQ_DEFAULT_TIMEOUT=300)
    @patch.object(SyncLogging, "log_warning")
    @patch(
        "forward_netbox.utilities.single_branch_executor.ForwardSingleBranchExecutor"
    )
    def test_run_forward_sync_warns_when_worker_timeout_is_lower_than_source_timeout(
        self,
        mock_executor_class,
        mock_log_warning,
    ):
        mock_executor = mock_executor_class.return_value
        mock_executor.run.return_value = []
        mock_executor.current_ingestion = SimpleNamespace(snapshot_id="snapshot-1")
        mock_executor.client = SimpleNamespace(
            get_latest_processed_snapshot_id=Mock(return_value="snapshot-1")
        )

        run_forward_sync(self.sync)

        warning_message = mock_log_warning.call_args.args[0]
        self.assertIn("RQ_DEFAULT_TIMEOUT is 300s", warning_message)
        self.assertIn("Forward source timeout (1200s)", warning_message)

    def test_latest_processed_catchup_decision_skips_when_snapshot_is_current(self):
        self.sync.status = ForwardSyncStatusChoices.COMPLETED
        self.sync.save(update_fields=["status"])
        client = SimpleNamespace(
            get_latest_processed_snapshot_id=Mock(return_value="snapshot-1")
        )

        decision = latest_processed_catchup_decision(
            self.sync,
            current_snapshot_id="snapshot-1",
            client=client,
        )

        self.assertFalse(decision["should_queue"])
        self.assertEqual(decision["reason"], "already_current")

    def test_latest_processed_catchup_decision_ignores_lookup_failure(self):
        self.sync.status = ForwardSyncStatusChoices.COMPLETED
        self.sync.save(update_fields=["status"])
        client = SimpleNamespace(
            get_latest_processed_snapshot_id=Mock(
                side_effect=ForwardClientError("boom")
            )
        )

        decision = latest_processed_catchup_decision(
            self.sync,
            current_snapshot_id="snapshot-1",
            client=client,
        )

        self.assertFalse(decision["should_queue"])
        self.assertEqual(decision["reason"], "latest_processed_lookup_failed")

    def test_latest_collected_catchup_queues_when_collected_snapshot_advances(self):
        from forward_netbox.utilities.forward_api import LATEST_COLLECTED_SNAPSHOT

        self.sync.parameters = {
            **self.sync.parameters,
            "snapshot_id": LATEST_COLLECTED_SNAPSHOT,
        }
        self.sync.status = ForwardSyncStatusChoices.COMPLETED
        self.sync.save(update_fields=["parameters", "status"])
        client = SimpleNamespace(
            get_latest_collected_snapshot_id=Mock(
                return_value="snapshot-collected-new"
            ),
            get_latest_processed_snapshot_id=Mock(return_value="snapshot-processed"),
        )

        decision = latest_processed_catchup_decision(
            self.sync,
            current_snapshot_id="snapshot-collected-old",
            client=client,
        )

        self.assertTrue(decision["should_queue"])
        self.assertEqual(decision["reason"], "latest_processed_advanced")
        self.assertEqual(decision["snapshot_selector"], LATEST_COLLECTED_SNAPSHOT)
        self.assertEqual(
            decision["latest_processed_snapshot_id"], "snapshot-collected-new"
        )
        # latestCollected must resolve via the collected probe, not latestProcessed.
        client.get_latest_collected_snapshot_id.assert_called_once()
        client.get_latest_processed_snapshot_id.assert_not_called()

    def test_latest_collected_catchup_skips_when_probe_finds_no_collected(self):
        from forward_netbox.utilities.forward_api import LATEST_COLLECTED_SNAPSHOT

        self.sync.parameters = {
            **self.sync.parameters,
            "snapshot_id": LATEST_COLLECTED_SNAPSHOT,
        }
        self.sync.status = ForwardSyncStatusChoices.COMPLETED
        self.sync.save(update_fields=["parameters", "status"])
        client = SimpleNamespace(
            get_latest_collected_snapshot_id=Mock(
                side_effect=ForwardClientError("all backfilled")
            )
        )

        decision = latest_processed_catchup_decision(
            self.sync,
            current_snapshot_id="snapshot-collected-old",
            client=client,
        )

        self.assertFalse(decision["should_queue"])
        self.assertEqual(decision["reason"], "latest_processed_lookup_failed")

    @patch(
        "forward_netbox.utilities.single_branch_executor.ForwardSingleBranchExecutor"
    )
    def test_run_forward_sync_queues_catchup_when_latest_processed_advances(
        self,
        mock_executor_class,
    ):
        self.sync.status = ForwardSyncStatusChoices.COMPLETED
        self.sync.save(update_fields=["status"])
        mock_executor = mock_executor_class.return_value
        mock_executor.run.return_value = []
        mock_executor.current_ingestion = SimpleNamespace(snapshot_id="snapshot-old")
        mock_executor.client = SimpleNamespace(
            get_latest_processed_snapshot_id=Mock(return_value="snapshot-new")
        )

        with patch.object(self.sync, "enqueue_sync_job") as mock_enqueue_sync_job:
            run_forward_sync(self.sync)

        mock_enqueue_sync_job.assert_called_once_with(adhoc=True, user=None)
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

    def test_record_forward_api_usage_stores_summary_and_log(self):
        self.sync.logger = SyncLogging()
        executor = SimpleNamespace(
            client=SimpleNamespace(
                api_usage_summary=lambda: {
                    "api_requests_per_minute": 1800,
                    "http_attempts": 7,
                    "http_retries": 1,
                    "http_429_failures": 0,
                    "nqe_query_calls": 2,
                    "nqe_diff_calls": 1,
                    "nqe_pages": 3,
                    "throttle_sleep_seconds": 1.25,
                    "read_cache_hits": 5,
                    "read_cache_misses": 2,
                    "read_cache_hit_rate": 0.714286,
                }
            )
        )

        _record_forward_api_usage(self.sync, executor)

        self.assertEqual(self.sync.logger.log_data["forward_api_usage"]["nqe_pages"], 3)
        self.assertEqual(
            self.sync.logger.log_data["forward_api_usage"]["read_cache_hits"], 5
        )
        self.assertEqual(
            self.sync.logger.log_data["forward_api_usage"]["budget"]["status"],
            "passed",
        )
        self.assertIn(
            "Forward API usage summary: api_usage_status=passed http_attempts=7",
            self.sync.logger.log_data["logs"][0][4],
        )


class SkipUnchangedSnapshotTest(TestCase):
    def setUp(self):
        self.source = ForwardSource.objects.create(
            name="source-skip-unchanged",
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
            name="sync-skip-unchanged",
            source=self.source,
            parameters={
                "snapshot_id": LATEST_PROCESSED_SNAPSHOT,
                "dcim.device": True,
                "skip_unchanged_snapshot": True,
            },
        )

    def _set_baseline(self, snapshot_id):
        self.sync.latest_baseline_ingestion = Mock(
            return_value=SimpleNamespace(snapshot_id=snapshot_id, pk=42)
        )

    def test_skips_scheduled_run_when_snapshot_matches_baseline(self):
        self._set_baseline("snapshot-1")
        self.sync.resolve_snapshot_id = Mock(return_value="snapshot-1")

        result = should_skip_unchanged_snapshot(self.sync, adhoc=False, client=object())

        self.assertEqual(result, "snapshot-1")

    def test_does_not_skip_adhoc_run(self):
        self._set_baseline("snapshot-1")
        self.sync.resolve_snapshot_id = Mock(return_value="snapshot-1")

        self.assertIsNone(
            should_skip_unchanged_snapshot(self.sync, adhoc=True, client=object())
        )

    def test_does_not_skip_when_flag_off(self):
        self.sync.parameters = {
            **self.sync.parameters,
            "skip_unchanged_snapshot": False,
        }
        self.sync.save(update_fields=["parameters"])
        self._set_baseline("snapshot-1")
        self.sync.resolve_snapshot_id = Mock(return_value="snapshot-1")

        self.assertIsNone(
            should_skip_unchanged_snapshot(self.sync, adhoc=False, client=object())
        )

    def test_does_not_skip_when_no_baseline(self):
        self.sync.latest_baseline_ingestion = Mock(return_value=None)
        self.sync.resolve_snapshot_id = Mock(return_value="snapshot-1")

        self.assertIsNone(
            should_skip_unchanged_snapshot(self.sync, adhoc=False, client=object())
        )

    def test_does_not_skip_when_snapshot_advanced(self):
        self._set_baseline("snapshot-1")
        self.sync.resolve_snapshot_id = Mock(return_value="snapshot-2")

        self.assertIsNone(
            should_skip_unchanged_snapshot(self.sync, adhoc=False, client=object())
        )

    def test_does_not_skip_when_resolution_fails(self):
        self._set_baseline("snapshot-1")
        self.sync.resolve_snapshot_id = Mock(side_effect=RuntimeError("boom"))

        self.assertIsNone(
            should_skip_unchanged_snapshot(self.sync, adhoc=False, client=object())
        )

    @patch(
        "forward_netbox.utilities.single_branch_executor.ForwardSingleBranchExecutor"
    )
    def test_run_forward_sync_noops_without_executing(self, mock_executor_class):
        self._set_baseline("snapshot-1")
        self.sync.resolve_snapshot_id = Mock(return_value="snapshot-1")

        run_forward_sync(self.sync, adhoc=False)

        mock_executor_class.assert_not_called()
        self.sync.refresh_from_db()
        self.assertEqual(self.sync.status, ForwardSyncStatusChoices.COMPLETED)
