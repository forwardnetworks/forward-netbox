import json
from datetime import timedelta
from unittest.mock import Mock
from unittest.mock import patch

from django.contrib.auth import get_user_model
from django.contrib.contenttypes.models import ContentType
from django.test import TestCase
from django.urls import reverse
from django.utils import timezone

from forward_netbox.choices import ForwardSourceStatusChoices
from forward_netbox.models import ForwardExecutionRun
from forward_netbox.models import ForwardExecutionStep
from forward_netbox.models import ForwardIngestion
from forward_netbox.models import ForwardNQEMap
from forward_netbox.models import ForwardSource
from forward_netbox.models import ForwardSync
from forward_netbox.models import ForwardValidationRun
from forward_netbox.utilities.health import live_data_file_health_check
from forward_netbox.utilities.health import live_source_health_check
from forward_netbox.utilities.health import sync_health_summary
from forward_netbox.utilities.query_registry import read_compiled_builtin_query_source


class ForwardSyncHealthTest(TestCase):
    @classmethod
    def setUpTestData(cls):
        User = get_user_model()
        cls.user = User.objects.create_superuser(
            username="health-admin",
            password="TestPassword123!",
            email="health-admin@example.com",
        )
        cls.source = ForwardSource.objects.create(
            name="health-source",
            type="saas",
            url="https://fwd.app",
            status=ForwardSourceStatusChoices.READY,
            parameters={
                "username": "user@example.com",
                "password": "secret",
                "verify": True,
                "network_id": "test-network",
                "timeout": 1200,
            },
        )
        cls.sync = ForwardSync.objects.create(
            name="health-sync",
            source=cls.source,
            parameters={"snapshot_id": "latestProcessed"},
        )
        ForwardNQEMap.objects.update(enabled=False)
        site_type = ContentType.objects.get(app_label="dcim", model="site")
        device_type = ContentType.objects.get(app_label="dcim", model="device")
        ForwardNQEMap.objects.create(
            name="Health Sites",
            netbox_model=site_type,
            query_id="query-sites",
            enabled=True,
            weight=10,
        )
        ForwardNQEMap.objects.create(
            name="Health Devices with NetBox Device Type Aliases",
            netbox_model=device_type,
            query_path="/forward_netbox_validation/forward_devices",
            query_repository="org",
            enabled=True,
            weight=20,
        )
        ForwardValidationRun.objects.create(
            sync=cls.sync,
            status="passed",
            allowed=True,
            snapshot_selector="latestProcessed",
            snapshot_id="snapshot-1",
        )
        ForwardIngestion.objects.create(
            sync=cls.sync,
            snapshot_selector="latestProcessed",
            snapshot_id="snapshot-1",
            baseline_ready=True,
            applied_change_count=2,
        )
        now = timezone.now()
        cls.execution_run = ForwardExecutionRun.objects.create(
            sync=cls.sync,
            source=cls.source,
            backend="branching",
            status="running",
            snapshot_selector="latestProcessed",
            snapshot_id="snapshot-2",
            total_steps=2,
            next_step_index=2,
        )
        ForwardExecutionStep.objects.create(
            run=cls.execution_run,
            index=1,
            status="merged",
            model_string="dcim.site",
            started=now - timedelta(seconds=20),
            completed=now,
        )

    def test_sync_health_summary_reports_local_state(self):
        with patch.object(ForwardSource, "get_client") as get_client:
            summary = sync_health_summary(self.sync)

        self.assertEqual(summary["source"]["name"], "health-source")
        self.assertEqual(summary["runtime"]["source_timeout_seconds"], 1200)
        self.assertEqual(summary["query_modes"]["query_id"], 1)
        self.assertEqual(summary["query_modes"]["query_path"], 1)
        self.assertEqual(summary["query_modes"]["query"], 0)
        self.assertEqual(
            summary["query_modes"]["local_drift"][0]["status"],
            "direct_query_id_unverified",
        )
        self.assertEqual(
            summary["query_modes"]["local_drift"][0]["commit_binding"],
            "latest_commit",
        )
        self.assertIn(
            "latest committed Forward query revision",
            summary["query_modes"]["local_drift"][0]["commit_message"],
        )
        self.assertEqual(
            summary["query_modes"]["data_file_maps"][0]["model"],
            "dcim.device",
        )
        self.assertEqual(summary["next_run"]["mode"], "diff_eligible")
        self.assertEqual(summary["next_run"]["blockers"], [])
        self.assertTrue(summary["latest_validation"]["allowed"])
        self.assertTrue(summary["latest_ingestion"]["baseline_ready"])
        self.assertEqual(summary["capacity"]["completed_steps"], 1)
        self.assertEqual(summary["capacity"]["remaining_steps"], 1)
        self.assertEqual(summary["capacity"]["average_completed_step_seconds"], 20.0)
        self.assertIn("adapter", summary["apply_engines"]["selected"])
        self.assertNotIn("bulk_orm", summary["apply_engines"]["selected"])
        self.assertIn(
            "adapter_required_model_contract",
            summary["apply_engines"]["fallback_reasons"],
        )
        self.assertIn(
            "bulk_orm_disabled_by_default",
            summary["apply_engines"]["fallback_reasons"],
        )
        self.assertEqual(
            summary["apply_engines"]["global_selected"]["adapter"],
            len(summary["apply_engines"]["global_decisions"]),
        )
        self.assertIn(
            "adapter_required_model_contract",
            summary["apply_engines"]["global_fallback_reasons"],
        )
        self.assertIn(
            "bulk_orm_disabled_by_default",
            summary["apply_engines"]["global_fallback_reasons"],
        )
        self.assertIn(
            "dependency_resolution",
            summary["apply_engines"]["global_blocker_codes"],
        )
        self.assertIn(
            "plugin_model_dependencies",
            summary["apply_engines"]["global_blocker_codes"],
        )
        self.assertNotIn(
            "tree_model_constraints",
            summary["apply_engines"]["global_blocker_codes"],
        )
        self.assertIn("nqe_column_filter", summary["fetch_contracts"]["modes"])
        self.assertGreater(summary["fetch_contracts"]["shard_safe_count"], 0)
        self.assertNotIn(
            "model_fetch_fallback",
            summary["fetch_contracts"]["fallback_reasons"],
        )
        self.assertIn(
            "structured_column_filter",
            summary["fetch_contracts"]["fallback_reasons"],
        )

    def test_sync_health_summary_has_no_experimental_bulk_orm_allowlist_gap(self):
        self.sync.parameters["enable_bulk_orm"] = True
        self.sync.save(update_fields=["parameters"])

        summary = sync_health_summary(self.sync)
        self.assertNotIn(
            "bulk_orm_model_not_allowlisted",
            summary["apply_engines"]["global_fallback_reasons"],
        )

    def test_sync_health_summary_reports_next_run_blockers(self):
        platform_type = ContentType.objects.get(app_label="dcim", model="platform")
        raw_map = ForwardNQEMap.objects.create(
            name="Health Raw Platforms",
            netbox_model=platform_type,
            query="select {}",
            enabled=True,
            weight=30,
        )
        sync = ForwardSync.objects.create(
            name="health-sync-blocked",
            source=self.source,
            parameters={
                "snapshot_id": "fixed-snapshot",
                "dcim.platform": True,
            },
        )

        summary = sync_health_summary(sync)

        self.assertEqual(summary["next_run"]["mode"], "full_or_reconciliation")
        self.assertIn("snapshot_selector_is_fixed", summary["next_run"]["reasons"])
        self.assertIn("no_baseline_ready_ingestion", summary["next_run"]["reasons"])
        self.assertIn(
            "raw_query_maps_cannot_use_forward_diffs",
            summary["next_run"]["reasons"],
        )
        blockers = summary["next_run"]["blockers"]
        self.assertEqual(
            {blocker["reason"] for blocker in blockers},
            {
                "snapshot_selector_is_fixed",
                "no_baseline_ready_ingestion",
                "raw_query_maps_cannot_use_forward_diffs",
            },
        )
        map_blocker = next(blocker for blocker in blockers if blocker["scope"] == "map")
        self.assertEqual(map_blocker["map"], raw_map.name)
        self.assertEqual(map_blocker["model"], "dcim.platform")

        self.client.force_login(self.user)
        response = self.client.get(
            reverse(
                "plugins:forward_netbox:forwardsync_health",
                kwargs={"pk": sync.pk},
            )
        )
        self.assertContains(response, "Raw query text maps cannot use Forward nqe-diffs")

    def test_sync_health_view_renders_diagnostics(self):
        self.client.force_login(self.user)

        response = self.client.get(
            reverse(
                "plugins:forward_netbox:forwardsync_health",
                kwargs={"pk": self.sync.pk},
            )
        )

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Health Summary")
        self.assertContains(response, "Query Binding")
        self.assertContains(response, "Local Query Drift")
        self.assertContains(response, "Commit")
        self.assertContains(response, "latest committed Forward query revision")
        self.assertContains(response, "Apply Engines")
        self.assertContains(response, "Fetch Contracts")
        self.assertContains(response, "Capacity Projection")
        self.assertContains(response, "Export Live Source Check")
        self.assertContains(response, "Export Live Query Drift Check")
        self.assertContains(response, "Export Live Data File Check")
        self.assertContains(response, "Health Sites")
        self.assertContains(response, "The next run is eligible to use Forward diffs")

    def test_live_source_health_check_reports_reachability_without_ids(self):
        client = Mock()
        client.get_networks.return_value = [
            {"id": "test-network", "name": "Visible Network"},
        ]
        client.get_latest_processed_snapshot_id.return_value = "snapshot-1"

        with patch.object(ForwardSource, "get_client", return_value=client):
            result = live_source_health_check(self.sync)

        self.assertTrue(result["reachable"])
        self.assertTrue(result["configured_network_id_present"])
        self.assertTrue(result["configured_network_visible"])
        self.assertTrue(result["latest_processed_snapshot_available"])
        self.assertNotIn("test-network", json.dumps(result))
        self.assertNotIn("snapshot-1", json.dumps(result))

    def test_sync_live_source_health_downloads_reachability_diagnostics(self):
        self.client.force_login(self.user)
        client = Mock()
        client.get_networks.return_value = [
            {"id": "test-network", "name": "Visible Network"},
        ]
        client.get_latest_processed_snapshot_id.return_value = "snapshot-1"

        with patch.object(ForwardSource, "get_client", return_value=client):
            response = self.client.get(
                reverse(
                    "plugins:forward_netbox:forwardsync_source_health",
                    kwargs={"pk": self.sync.pk},
                )
            )

        self.assertEqual(response.status_code, 200)
        self.assertIn("attachment;", response["Content-Disposition"])
        data = json.loads(response.content)
        self.assertTrue(data["source_health"]["reachable"])
        self.assertTrue(data["source_health"]["configured_network_visible"])
        self.assertTrue(
            data["source_health"]["latest_processed_snapshot_available"]
        )

    def test_live_data_file_health_check_reports_snapshot_captured_rows(self):
        client = Mock()
        client.get_latest_processed_snapshot_id.return_value = "snapshot-1"
        client.run_nqe_query.return_value = [
            {
                "data_file": "netbox_device_type_aliases",
                "value_present": True,
                "row_count": 42,
            }
        ]

        with patch.object(ForwardSource, "get_client", return_value=client):
            result = live_data_file_health_check(self.sync)

        self.assertEqual(
            result["required_data_files"],
            ["netbox_device_type_aliases"],
        )
        self.assertEqual(result["results"][0]["status"], "present")
        self.assertEqual(result["results"][0]["row_count"], 42)
        self.assertNotIn("test-network", json.dumps(result))
        self.assertNotIn("snapshot-1", json.dumps(result))
        client.run_nqe_query.assert_called_once()
        probe_query = client.run_nqe_query.call_args.kwargs["query"]
        self.assertIn("network.extensions.netbox_device_type_aliases", probe_query)

    def test_live_data_file_health_check_reports_missing_snapshot_value(self):
        client = Mock()
        client.get_latest_processed_snapshot_id.return_value = "snapshot-1"
        client.run_nqe_query.return_value = [
            {
                "data_file": "netbox_device_type_aliases",
                "value_present": False,
                "row_count": 0,
            }
        ]

        with patch.object(ForwardSource, "get_client", return_value=client):
            result = live_data_file_health_check(self.sync)

        self.assertEqual(result["results"][0]["status"], "not_captured")
        self.assertEqual(result["checks"][0]["status"], "warn")

    def test_sync_live_data_file_health_downloads_freshness_diagnostics(self):
        self.client.force_login(self.user)
        client = Mock()
        client.get_latest_processed_snapshot_id.return_value = "snapshot-1"
        client.run_nqe_query.return_value = [
            {
                "data_file": "netbox_device_type_aliases",
                "value_present": True,
                "row_count": 42,
            }
        ]

        with patch.object(ForwardSource, "get_client", return_value=client):
            response = self.client.get(
                reverse(
                    "plugins:forward_netbox:forwardsync_data_file_health",
                    kwargs={"pk": self.sync.pk},
                )
            )

        self.assertEqual(response.status_code, 200)
        self.assertIn("attachment;", response["Content-Disposition"])
        data = json.loads(response.content)
        self.assertEqual(
            data["data_file_health"]["required_data_files"],
            ["netbox_device_type_aliases"],
        )
        self.assertEqual(data["data_file_health"]["results"][0]["status"], "present")

    def test_sync_live_query_drift_downloads_forward_checked_diagnostics(self):
        self.client.force_login(self.user)
        client = Mock()
        client.get_nqe_repository_queries.return_value = []
        client.get_committed_nqe_query.return_value = {
            "queryId": "Q_devices",
            "lastCommitId": "commit-1",
            "path": "/forward_netbox_validation/forward_devices",
            "sourceCode": read_compiled_builtin_query_source("forward_devices.nqe"),
        }

        with patch.object(ForwardSource, "get_client", return_value=client):
            response = self.client.get(
                reverse(
                    "plugins:forward_netbox:forwardsync_query_drift",
                    kwargs={"pk": self.sync.pk},
                )
            )

        self.assertEqual(response.status_code, 200)
        self.assertIn("attachment;", response["Content-Disposition"])
        data = json.loads(response.content)
        self.assertEqual(data["sync"]["pk"], self.sync.pk)
        self.assertEqual(len(data["results"]), 2)
        path_result = next(
            result
            for result in data["results"]
            if result["mode"] == "query_path"
        )
        self.assertEqual(path_result["status"], "live_repository_source_match")
        self.assertEqual(path_result["live_query_id"], "Q_devices")
        self.assertEqual(path_result["requested_commit_id"], "head")
        self.assertEqual(path_result["commit_binding"], "latest_commit")
