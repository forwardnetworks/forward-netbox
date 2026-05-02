from datetime import timedelta
from unittest.mock import patch
from uuid import uuid4

from core.exceptions import SyncError
from django.contrib.auth import get_user_model
from django.contrib.contenttypes.models import ContentType
from django.core.exceptions import ValidationError
from django.test import TestCase
from django.utils import timezone

from forward_netbox.choices import ForwardDriftPolicyBaselineChoices
from forward_netbox.choices import ForwardSyncStatusChoices
from forward_netbox.jobs import sync_forwardsync
from forward_netbox.models import ForwardDriftPolicy
from forward_netbox.models import ForwardIngestion
from forward_netbox.models import ForwardIngestionIssue
from forward_netbox.models import ForwardNQEMap
from forward_netbox.models import ForwardSource
from forward_netbox.models import ForwardSync
from forward_netbox.models import ForwardValidationRun
from forward_netbox.signals import seed_builtin_nqe_maps
from forward_netbox.tables import ForwardSyncTable
from forward_netbox.utilities.branch_budget import DEFAULT_MAX_CHANGES_PER_BRANCH
from forward_netbox.utilities.forward_api import LATEST_PROCESSED_SNAPSHOT
from forward_netbox.utilities.query_registry import builtin_nqe_map_rows
from forward_netbox.utilities.query_registry import QuerySpec


class ForwardSyncModelTest(TestCase):
    def setUp(self):
        self.source = ForwardSource.objects.create(
            name="source-1",
            type="saas",
            url="https://fwd.app",
            parameters={
                "username": "user@example.com",
                "password": "secret",
                "verify": True,
                "timeout": 1200,
                "network_id": "test-network",
                "nqe_page_size": 1000,
            },
        )

    def test_source_rejects_invalid_nqe_page_size(self):
        source = ForwardSource(
            name="source-invalid-page-size",
            type="saas",
            url="https://fwd.app",
            parameters={
                "username": "user@example.com",
                "password": "secret",
                "verify": True,
                "timeout": 1200,
                "network_id": "test-network",
                "nqe_page_size": 10001,
            },
        )

        with self.assertRaises(ValidationError) as ctx:
            source.clean()

        self.assertIn(
            "`nqe_page_size` must be between 1 and 10000.", str(ctx.exception)
        )

    def test_sync_rejects_query_overrides_parameter(self):
        sync = ForwardSync(
            name="sync-1",
            source=self.source,
            parameters={
                "snapshot_id": LATEST_PROCESSED_SNAPSHOT,
                "dcim.device": True,
                "query_overrides": {
                    "dcim.device": {
                        "query_id": "FQ_123",
                    }
                },
            },
        )

        with self.assertRaises(ValidationError) as ctx:
            sync.clean()

        self.assertIn("Unsupported Forward sync keys", str(ctx.exception))
        self.assertIn("query_overrides", str(ctx.exception))

    def test_sync_forces_native_branching_budget(self):
        sync = ForwardSync(
            name="sync-default-branching",
            source=self.source,
            auto_merge=False,
            parameters={
                "snapshot_id": LATEST_PROCESSED_SNAPSHOT,
                "auto_merge": False,
                "multi_branch": False,
                "dcim.device": True,
            },
        )

        sync.clean()

        self.assertTrue(sync.uses_multi_branch())
        self.assertEqual(
            sync.get_max_changes_per_branch(),
            DEFAULT_MAX_CHANGES_PER_BRANCH,
        )
        self.assertTrue(sync.get_display_parameters()["multi_branch"])
        self.assertFalse(sync.get_display_parameters()["auto_merge"])
        self.assertFalse(sync.auto_merge)

    def test_save_forces_native_branching_execution_flags(self):
        sync = ForwardSync.objects.create(
            name="sync-forced-branching-save",
            source=self.source,
            auto_merge=False,
            parameters={
                "snapshot_id": LATEST_PROCESSED_SNAPSHOT,
                "auto_merge": False,
                "multi_branch": False,
                "max_changes_per_branch": "invalid",
                "dcim.device": True,
            },
        )

        self.assertFalse(sync.auto_merge)
        self.assertFalse(sync.parameters["auto_merge"])
        self.assertTrue(sync.parameters["multi_branch"])
        self.assertEqual(
            sync.parameters["max_changes_per_branch"],
            DEFAULT_MAX_CHANGES_PER_BRANCH,
        )

    def test_model_change_density_round_trip(self):
        sync = ForwardSync.objects.create(
            name="sync-density-round-trip",
            source=self.source,
            parameters={
                "snapshot_id": LATEST_PROCESSED_SNAPSHOT,
                "dcim.device": True,
            },
        )

        sync.set_model_change_density(
            {
                "dcim.device": 9.9,
                "dcim.interface": "4.2",
                "invalid": "abc",
                "dcim.site": -1,
            }
        )
        sync.refresh_from_db()

        self.assertEqual(
            sync.get_model_change_density(),
            {"dcim.device": 9.9, "dcim.interface": 4.2},
        )

    @patch("forward_netbox.models.ForwardSource.get_client")
    @patch("forward_netbox.utilities.multi_branch.ForwardMultiBranchExecutor")
    def test_sync_job_uses_multi_branch_path_by_default(
        self,
        mock_executor_class,
        _mock_get_client,
    ):
        mock_executor = mock_executor_class.return_value
        mock_executor.run.return_value = []
        sync = ForwardSync.objects.create(
            name="sync-default-exec",
            source=self.source,
            parameters={
                "snapshot_id": LATEST_PROCESSED_SNAPSHOT,
                "dcim.device": True,
            },
        )

        sync.sync()

        mock_executor.run.assert_called_once_with(
            max_changes_per_branch=DEFAULT_MAX_CHANGES_PER_BRANCH,
        )

    def test_enqueue_rejects_sync_waiting_for_branch_merge(self):
        sync = ForwardSync.objects.create(
            name="sync-awaiting-merge-enqueue",
            source=self.source,
            status="ready_to_merge",
            parameters={
                "snapshot_id": LATEST_PROCESSED_SNAPSHOT,
                "dcim.device": True,
                "_branch_run": {
                    "snapshot_id": "snapshot-before",
                    "next_plan_index": 2,
                    "total_plan_items": 3,
                    "awaiting_merge": True,
                },
            },
        )

        with self.assertRaises(SyncError):
            sync.enqueue_sync_job(adhoc=True)

        sync.refresh_from_db()
        self.assertEqual(sync.status, "ready_to_merge")

    def test_sync_does_not_fail_sync_waiting_for_branch_merge(self):
        sync = ForwardSync.objects.create(
            name="sync-awaiting-merge-run",
            source=self.source,
            status="ready_to_merge",
            parameters={
                "snapshot_id": LATEST_PROCESSED_SNAPSHOT,
                "dcim.device": True,
                "_branch_run": {
                    "snapshot_id": "snapshot-before",
                    "next_plan_index": 2,
                    "total_plan_items": 3,
                    "awaiting_merge": True,
                },
            },
        )

        sync.sync()

        sync.refresh_from_db()
        self.assertEqual(sync.status, "ready_to_merge")

    @patch("forward_netbox.models.Job.enqueue")
    def test_scheduled_enqueue_sets_queued_only_for_new_sync(self, mock_enqueue):
        sync = ForwardSync.objects.create(
            name="sync-first-scheduled-enqueue",
            source=self.source,
            parameters={
                "snapshot_id": LATEST_PROCESSED_SNAPSHOT,
                "dcim.device": True,
            },
        )
        ForwardSync.objects.filter(pk=sync.pk).update(
            scheduled=timezone.now() + timedelta(minutes=10),
            interval=30,
        )
        sync.refresh_from_db()

        sync.enqueue_sync_job()

        sync.refresh_from_db()
        self.assertEqual(sync.status, ForwardSyncStatusChoices.QUEUED)
        mock_enqueue.assert_called_once()

    @patch("forward_netbox.models.Job.enqueue")
    def test_scheduled_enqueue_preserves_last_terminal_status(self, mock_enqueue):
        sync = ForwardSync.objects.create(
            name="sync-terminal-scheduled-enqueue",
            source=self.source,
            parameters={
                "snapshot_id": LATEST_PROCESSED_SNAPSHOT,
                "dcim.device": True,
            },
        )
        ForwardSync.objects.filter(pk=sync.pk).update(
            status=ForwardSyncStatusChoices.COMPLETED,
            scheduled=timezone.now() + timedelta(minutes=10),
            interval=30,
        )
        sync.refresh_from_db()

        sync.enqueue_sync_job()

        sync.refresh_from_db()
        self.assertEqual(sync.status, ForwardSyncStatusChoices.COMPLETED)
        mock_enqueue.assert_called_once()

    def test_drift_policy_rejects_delete_threshold_without_baseline(self):
        policy = ForwardDriftPolicy(
            name="no-baseline-delete-threshold",
            baseline_mode=ForwardDriftPolicyBaselineChoices.NONE,
            max_deleted_objects=10,
        )

        with self.assertRaises(ValidationError):
            policy.full_clean()

    @patch("forward_netbox.models.Job.enqueue")
    @patch.object(ForwardSync, "sync", autospec=True)
    def test_recurring_reschedule_preserves_last_terminal_status(
        self,
        mock_sync,
        mock_enqueue,
    ):
        sync = ForwardSync.objects.create(
            name="sync-recurring-status",
            source=self.source,
            parameters={
                "snapshot_id": LATEST_PROCESSED_SNAPSHOT,
                "dcim.device": True,
            },
        )
        user = get_user_model().objects.create_user(username="recurring-user")
        started = timezone.now()
        ForwardSync.objects.filter(pk=sync.pk).update(
            status=ForwardSyncStatusChoices.QUEUED,
            scheduled=started - timedelta(minutes=1),
            interval=30,
            user=user,
        )

        def complete_sync(instance, job=None, **kwargs):
            ForwardSync.objects.filter(pk=instance.pk).update(
                status=ForwardSyncStatusChoices.COMPLETED
            )

        mock_sync.side_effect = complete_sync

        class DummyJob:
            object_id = sync.pk
            pk = 1001
            job_id = uuid4()
            user = None
            data = None

            def start(self):
                return None

            def save(self, **kwargs):
                return None

            def terminate(self, **kwargs):
                return None

        job = DummyJob()
        job.started = started
        job.user = user
        sync_forwardsync(job)

        sync.refresh_from_db()
        self.assertEqual(sync.status, ForwardSyncStatusChoices.COMPLETED)
        self.assertGreater(sync.scheduled, started)
        mock_enqueue.assert_called_once()

    def test_plugin_models_disable_local_docs_url(self):
        models = (
            ForwardSource,
            ForwardNQEMap,
            ForwardDriftPolicy,
            ForwardSync,
            ForwardValidationRun,
            ForwardIngestion,
            ForwardIngestionIssue,
        )

        for model in models:
            with self.subTest(model=model.__name__):
                self.assertEqual(model().docs_url, "")

    def test_sync_table_shows_scheduled_by_default(self):
        self.assertIn("scheduled", ForwardSyncTable.Meta.default_columns)

    @patch("forward_netbox.models.ForwardSource.get_client")
    @patch("forward_netbox.utilities.multi_branch.ForwardMultiBranchExecutor")
    def test_sync_failure_records_issue_on_current_executor_ingestion(
        self,
        mock_executor_class,
        _mock_get_client,
    ):
        sync = ForwardSync.objects.create(
            name="sync-current-ingestion-failure",
            source=self.source,
            parameters={
                "snapshot_id": LATEST_PROCESSED_SNAPSHOT,
                "dcim.device": True,
            },
        )
        ingestion = ForwardIngestion.objects.create(sync=sync)
        mock_executor = mock_executor_class.return_value
        mock_executor.current_ingestion = ingestion
        mock_executor.run.side_effect = RuntimeError("boom")

        sync.sync()

        self.assertEqual(ForwardIngestion.objects.filter(sync=sync).count(), 1)
        self.assertTrue(ingestion.issues.filter(message="boom").exists())

    def test_latest_baseline_ingestion_returns_latest_ready_snapshot(self):
        sync = ForwardSync.objects.create(
            name="sync-baseline",
            source=self.source,
            auto_merge=False,
            parameters={
                "snapshot_id": LATEST_PROCESSED_SNAPSHOT,
                "dcim.device": True,
            },
        )
        ForwardIngestion.objects.create(
            sync=sync,
            snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
            snapshot_id="snapshot-old",
            baseline_ready=False,
        )
        expected = ForwardIngestion.objects.create(
            sync=sync,
            snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
            snapshot_id="snapshot-before",
            baseline_ready=True,
        )
        ForwardIngestion.objects.create(
            sync=sync,
            snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
            snapshot_id="",
            baseline_ready=True,
        )

        self.assertEqual(sync.latest_baseline_ingestion(), expected)

    def test_latest_baseline_ingestion_excludes_current_ingestion(self):
        sync = ForwardSync.objects.create(
            name="sync-baseline-exclude",
            source=self.source,
            auto_merge=False,
            parameters={
                "snapshot_id": LATEST_PROCESSED_SNAPSHOT,
                "dcim.device": True,
            },
        )
        expected = ForwardIngestion.objects.create(
            sync=sync,
            snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
            snapshot_id="snapshot-before",
            baseline_ready=True,
        )
        current = ForwardIngestion.objects.create(
            sync=sync,
            snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
            snapshot_id="snapshot-after",
            baseline_ready=True,
        )

        self.assertEqual(
            sync.latest_baseline_ingestion(exclude_ingestion_id=current.pk),
            expected,
        )

    def test_incremental_diff_baseline_requires_latest_processed_and_query_ids(self):
        sync = ForwardSync.objects.create(
            name="sync-diff-baseline",
            source=self.source,
            auto_merge=False,
            parameters={
                "snapshot_id": LATEST_PROCESSED_SNAPSHOT,
                "dcim.device": True,
            },
        )
        baseline = ForwardIngestion.objects.create(
            sync=sync,
            snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
            snapshot_id="snapshot-before",
            baseline_ready=True,
        )
        specs = [
            QuerySpec(
                model_string="dcim.device",
                query_name="Device Query",
                query_id="Q_device",
            )
        ]

        self.assertEqual(
            sync.incremental_diff_baseline(
                specs=specs,
                current_snapshot_id="snapshot-after",
            ),
            baseline,
        )
        self.assertIsNone(
            sync.incremental_diff_baseline(
                specs=[
                    QuerySpec(
                        model_string="dcim.device",
                        query_name="Device Query",
                        query='select {name: "device-1"}',
                    )
                ],
                current_snapshot_id="snapshot-after",
            )
        )
        self.assertIsNone(
            sync.incremental_diff_baseline(
                specs=specs,
                current_snapshot_id="snapshot-before",
            )
        )


class ForwardIngestionSnapshotSummaryTest(TestCase):
    def setUp(self):
        self.source = ForwardSource.objects.create(
            name="source-2",
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
            name="sync-2",
            source=self.source,
            auto_merge=False,
            parameters={
                "snapshot_id": LATEST_PROCESSED_SNAPSHOT,
                "dcim.device": True,
            },
        )

    def test_snapshot_summary_helpers_return_expected_fields(self):
        ingestion = ForwardIngestion.objects.create(
            sync=self.sync,
            snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
            snapshot_id="snapshot-before",
            snapshot_info={
                "state": "PROCESSED",
                "createdAt": "2026-03-31T12:00:00Z",
                "processedAt": "2026-03-31T12:15:00Z",
            },
            snapshot_metrics={
                "snapshotState": "PROCESSED",
                "numSuccessfulDevices": 122,
                "numSuccessfulEndpoints": 1213,
                "processingDuration": 900,
                "extraMetric": "ignored",
            },
        )

        self.assertEqual(
            ingestion.get_snapshot_summary(),
            {
                "snapshot_selector": LATEST_PROCESSED_SNAPSHOT,
                "snapshot_id": "snapshot-before",
                "state": "PROCESSED",
                "created_at": "2026-03-31T12:00:00Z",
                "processed_at": "2026-03-31T12:15:00Z",
            },
        )
        self.assertEqual(
            ingestion.get_snapshot_metrics_summary(),
            {
                "snapshotState": "PROCESSED",
                "numSuccessfulDevices": 122,
                "numSuccessfulEndpoints": 1213,
                "processingDuration": 900,
            },
        )

    def test_ingestion_defaults_to_full_mode_and_not_baseline_ready(self):
        ingestion = ForwardIngestion.objects.create(
            sync=self.sync,
            snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
            snapshot_id="snapshot-before",
        )

        self.assertEqual(ingestion.sync_mode, "full")
        self.assertFalse(ingestion.baseline_ready)

    def test_sync_merge_can_skip_baseline_marker_for_intermediate_branch(self):
        ingestion = ForwardIngestion.objects.create(
            sync=self.sync,
            snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
            snapshot_id="snapshot-before",
        )

        with patch("forward_netbox.utilities.merge.merge_branch"):
            ingestion.sync_merge(mark_baseline_ready=False)

        ingestion.refresh_from_db()
        self.assertFalse(ingestion.baseline_ready)

    def test_sync_merge_uses_shared_signal_suppression_context(self):
        ingestion = ForwardIngestion.objects.create(
            sync=self.sync,
            snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
            snapshot_id="snapshot-before",
        )

        with (
            patch(
                "forward_netbox.models.suppress_branch_merge_side_effect_signals"
            ) as mock_suppress,
            patch("forward_netbox.utilities.merge.merge_branch"),
        ):
            ingestion.sync_merge(mark_baseline_ready=False)

        mock_suppress.assert_called_once_with()

    def test_sync_merge_removes_branch_by_default(self):
        ingestion = ForwardIngestion.objects.create(
            sync=self.sync,
            snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
            snapshot_id="snapshot-before",
        )

        with (
            patch("forward_netbox.utilities.merge.merge_branch"),
            patch.object(ForwardIngestion, "_cleanup_merged_branch") as mock_cleanup,
        ):
            ingestion.sync_merge(mark_baseline_ready=False)

        mock_cleanup.assert_called_once_with()

    def test_sync_merge_can_preserve_branch_when_requested(self):
        ingestion = ForwardIngestion.objects.create(
            sync=self.sync,
            snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
            snapshot_id="snapshot-before",
        )

        with (
            patch("forward_netbox.utilities.merge.merge_branch"),
            patch.object(ForwardIngestion, "_cleanup_merged_branch") as mock_cleanup,
        ):
            ingestion.sync_merge(mark_baseline_ready=False, remove_branch=False)

        mock_cleanup.assert_not_called()

    def test_sync_merge_advances_gated_branch_run_after_review_merge(self):
        ingestion = ForwardIngestion.objects.create(
            sync=self.sync,
            snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
            snapshot_id="snapshot-before",
        )
        self.sync.set_branch_run_state(
            {
                "snapshot_selector": LATEST_PROCESSED_SNAPSHOT,
                "snapshot_id": "snapshot-before",
                "max_changes_per_branch": DEFAULT_MAX_CHANGES_PER_BRANCH,
                "next_plan_index": 2,
                "total_plan_items": 3,
                "auto_merge": False,
                "awaiting_merge": True,
                "pending_ingestion_id": ingestion.pk,
                "pending_plan_index": 1,
                "pending_is_final": False,
            }
        )

        with patch("forward_netbox.utilities.merge.merge_branch"):
            ingestion.sync_merge()

        self.sync.refresh_from_db()
        ingestion.refresh_from_db()
        state = self.sync.get_branch_run_state()
        self.assertFalse(ingestion.baseline_ready)
        self.assertFalse(state["awaiting_merge"])
        self.assertEqual(state["next_plan_index"], 2)
        self.assertTrue(self.sync.ready_to_continue_sync)

    def test_sync_merge_clears_gated_branch_run_after_final_merge(self):
        ingestion = ForwardIngestion.objects.create(
            sync=self.sync,
            snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
            snapshot_id="snapshot-before",
        )
        self.sync.set_branch_run_state(
            {
                "snapshot_selector": LATEST_PROCESSED_SNAPSHOT,
                "snapshot_id": "snapshot-before",
                "max_changes_per_branch": DEFAULT_MAX_CHANGES_PER_BRANCH,
                "next_plan_index": 4,
                "total_plan_items": 3,
                "auto_merge": False,
                "awaiting_merge": True,
                "pending_ingestion_id": ingestion.pk,
                "pending_plan_index": 3,
                "pending_is_final": True,
            }
        )

        with patch("forward_netbox.utilities.merge.merge_branch"):
            ingestion.sync_merge()

        self.sync.refresh_from_db()
        ingestion.refresh_from_db()
        self.assertTrue(ingestion.baseline_ready)
        self.assertEqual(self.sync.get_branch_run_state(), {})


class ForwardNQEMapModelTest(TestCase):
    def test_map_defaults_coalesce_fields_from_model_contract(self):
        netbox_model = ContentType.objects.get(app_label="dcim", model="site")
        query_map = ForwardNQEMap(
            name="Site Map",
            netbox_model=netbox_model,
            query='select {\n  name: "site-a",\n  slug: "site-a"\n}',
        )

        query_map.clean()

        self.assertEqual(query_map.coalesce_fields, [["slug"], ["name"]])

    def test_prefix_map_defaults_include_vrf_optional_fallback(self):
        netbox_model = ContentType.objects.get(app_label="ipam", model="prefix")
        query_map = ForwardNQEMap(
            name="Prefix Map",
            netbox_model=netbox_model,
            query='select {\n  prefix: "10.0.0.0/24",\n  vrf: null,\n  status: "active"\n}',
        )

        query_map.clean()

        self.assertEqual(query_map.coalesce_fields, [["prefix", "vrf"], ["prefix"]])

    def test_ipaddress_map_defaults_include_vrf_optional_fallback(self):
        netbox_model = ContentType.objects.get(app_label="ipam", model="ipaddress")
        query_map = ForwardNQEMap(
            name="IP Address Map",
            netbox_model=netbox_model,
            query=(
                'select {\n  device: "device-1",\n  interface: "Ethernet1/1",\n'
                '  address: "10.0.0.1/24",\n  vrf: null,\n  status: "active"\n}'
            ),
        )

        query_map.clean()

        self.assertEqual(query_map.coalesce_fields, [["address", "vrf"], ["address"]])

    def test_map_rejects_invalid_coalesce_field(self):
        netbox_model = ContentType.objects.get(app_label="dcim", model="site")
        query_map = ForwardNQEMap(
            name="Site Map",
            netbox_model=netbox_model,
            query='select {\n  name: "site-a",\n  slug: "site-a"\n}',
            coalesce_fields=[["name"], ["invalid_field"]],
        )

        with self.assertRaises(ValidationError) as ctx:
            query_map.clean()

        self.assertIn("is not allowed", str(ctx.exception))

    def test_map_rejects_query_missing_required_fields(self):
        netbox_model = ContentType.objects.get(app_label="dcim", model="device")
        query_map = ForwardNQEMap(
            name="Device Map",
            netbox_model=netbox_model,
            query='select {name: "device-1"}',
        )

        with self.assertRaises(ValidationError) as ctx:
            query_map.clean()

        self.assertIn("missing required fields", str(ctx.exception))

    def test_seed_builtin_maps_updates_existing_prefix_map_defaults(self):
        netbox_model = ContentType.objects.get(app_label="ipam", model="prefix")
        query_map = ForwardNQEMap.objects.get(
            name="Forward IPv4 Prefixes",
            netbox_model=netbox_model,
            built_in=True,
        )
        query_map.coalesce_fields = [["prefix", "vrf"]]
        query_map.query = (
            'select {\n  prefix: "10.0.0.0/24",\n  vrf: null,\n  status: "active"\n}'
        )
        query_map.save(update_fields=["coalesce_fields", "query"])

        seed_builtin_nqe_maps(type("Sender", (), {"label": "forward_netbox"}))

        query_map.refresh_from_db()
        expected_row = next(
            row
            for row in builtin_nqe_map_rows()
            if row["model_string"] == "ipam.prefix"
            and row["name"] == "Forward IPv4 Prefixes"
        )
        self.assertEqual(query_map.coalesce_fields, [["prefix", "vrf"], ["prefix"]])
        self.assertEqual(query_map.query, expected_row["query"])

    def test_seed_builtin_maps_updates_existing_inventory_query(self):
        netbox_model = ContentType.objects.get(app_label="dcim", model="inventoryitem")
        query_map = ForwardNQEMap.objects.get(
            name="Forward Inventory Items",
            netbox_model=netbox_model,
            built_in=True,
        )
        query_map.query = (
            'select {\n  device: "device-1",\n  name: "fan-1",\n  part_id: "fan-1",\n'
            '  serial: "abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyz",\n'
            '  status: "active",\n  discovered: true\n}'
        )
        query_map.save(update_fields=["query"])

        seed_builtin_nqe_maps(type("Sender", (), {"label": "forward_netbox"}))

        query_map.refresh_from_db()
        expected_row = next(
            row
            for row in builtin_nqe_map_rows()
            if row["model_string"] == "dcim.inventoryitem"
            and row["name"] == "Forward Inventory Items"
        )
        self.assertEqual(query_map.query, expected_row["query"])
        self.assertIn("truncate(value: String, max_len: Integer)", query_map.query)

    def test_seed_builtin_maps_preserves_existing_enabled_state(self):
        netbox_model = ContentType.objects.get(app_label="dcim", model="device")
        query_map = ForwardNQEMap.objects.get(
            name="Forward Devices",
            netbox_model=netbox_model,
            built_in=True,
        )
        query_map.enabled = False
        query_map.save(update_fields=["enabled"])

        seed_builtin_nqe_maps(type("Sender", (), {"label": "forward_netbox"}))

        query_map.refresh_from_db()
        self.assertFalse(query_map.enabled)

    def test_seed_builtin_maps_creates_optional_alias_maps_disabled(self):
        netbox_model = ContentType.objects.get(app_label="dcim", model="device")
        ForwardNQEMap.objects.filter(
            name="Forward Devices with NetBox Device Type Aliases",
            netbox_model=netbox_model,
            built_in=True,
        ).delete()

        seed_builtin_nqe_maps(type("Sender", (), {"label": "forward_netbox"}))

        query_map = ForwardNQEMap.objects.get(
            name="Forward Devices with NetBox Device Type Aliases",
            netbox_model=netbox_model,
            built_in=True,
        )
        self.assertFalse(query_map.enabled)
