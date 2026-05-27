import json
from datetime import timedelta
from unittest.mock import Mock
from unittest.mock import patch

from django.contrib.auth import get_user_model
from django.contrib.contenttypes.models import ContentType
from django.test import override_settings
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
from forward_netbox.utilities.branch_budget import BRANCH_RUN_STATE_PARAMETER
from forward_netbox.utilities.health import live_data_file_health_check
from forward_netbox.utilities.health import live_source_health_check
from forward_netbox.utilities.health import sync_health_summary
from forward_netbox.utilities.health_checks import ingestion_check_message
from forward_netbox.utilities.health_checks import ingestion_check_status
from forward_netbox.utilities.health_summary_blocks import large_run_tuning_summary
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
        cls.sync.set_model_change_density({"dcim.device": 1.6, "dcim.cable": 2.4})
        cls.sync.set_model_change_density_profile(
            {
                "dcim.device": {
                    "density": 1.6,
                    "sample_count": 6,
                    "accepted_observations": 6,
                    "rejected_observations": 1,
                    "mean": 1.5,
                    "m2": 0.12,
                    "variance": 0.024,
                    "stddev": 0.154919,
                    "last_updated_at": timezone.now().isoformat(),
                },
                "dcim.cable": {
                    "density": 2.4,
                    "sample_count": 2,
                    "accepted_observations": 2,
                    "rejected_observations": 0,
                    "mean": 2.4,
                    "m2": 0.0,
                    "variance": 0.0,
                    "stddev": 0.0,
                    "last_updated_at": timezone.now().isoformat(),
                },
            }
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
            fetch_parameters={
                "partition_retry_summary": {
                    "operation": "full",
                    "partition_count": 1,
                    "split_retry_count": 2,
                    "split_retry_success_count": 1,
                    "alternate_operator_retry_count": 0,
                    "alternate_operator_success_count": 0,
                }
            },
        )

    def test_sync_health_summary_reports_local_state(self):
        with patch.object(ForwardSource, "get_client"):
            summary = sync_health_summary(self.sync)

        self.assertEqual(summary["source"]["name"], "health-source")
        self.assertEqual(summary["runtime"]["source_timeout_seconds"], 1200)
        self.assertEqual(
            summary["runtime"]["pushdown_alert_thresholds"]["fallback_warn_rate"], 0.5
        )
        self.assertEqual(
            summary["runtime"]["pushdown_alert_thresholds"][
                "runtime_fallback_warn_share"
            ],
            0.5,
        )
        self.assertEqual(
            summary["runtime"]["pushdown_alert_thresholds"]["diff_warn_ratio"], 0.0
        )
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
        self.assertIn("bulk_orm", summary["apply_engines"]["selected"])
        self.assertIn(
            "adapter_required_model_contract",
            summary["apply_engines"]["fallback_reasons"],
        )
        self.assertNotIn(
            "bulk_orm_disabled_by_default",
            summary["apply_engines"]["fallback_reasons"],
        )
        self.assertGreater(summary["apply_engines"]["global_selected"]["adapter"], 0)
        self.assertGreater(summary["apply_engines"]["global_selected"]["bulk_orm"], 0)
        self.assertIn(
            "adapter_required_model_contract",
            summary["apply_engines"]["global_fallback_reasons"],
        )
        self.assertNotIn(
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
        self.assertEqual(
            summary["apply_engines"]["bulk_orm_expansion"]["status"],
            "blocked_pending_parity",
        )
        self.assertGreater(
            summary["apply_engines"]["bulk_orm_expansion"]["blocked_model_count"],
            0,
        )
        self.assertEqual(
            summary["apply_engines"]["bulk_orm_expansion"]["parity_gates"][0]["code"],
            "netbox_validation_parity",
        )
        self.assertEqual(
            summary["apply_engines"]["bulk_orm_expansion"]["promotion_lanes"][0][
                "lane"
            ],
            "dependency_anchored_models",
        )
        self.assertEqual(
            summary["apply_engines"]["bulk_orm_expansion"][
                "high_impact_blocked_models"
            ][0]["model"],
            "dcim.device",
        )
        self.assertEqual(
            summary["apply_engines"]["bulk_orm_expansion"]["parity_plan"]["status"],
            "pending_candidate_parity",
        )
        self.assertEqual(
            summary["apply_engines"]["bulk_orm_expansion"]["parity_plan"]["candidates"][
                0
            ]["model"],
            "dcim.device",
        )
        self.assertIn(
            "ForwardApplyEngineParityTest.test_dcim_device_create_parity",
            summary["apply_engines"]["bulk_orm_expansion"]["parity_plan"]["candidates"][
                0
            ]["required_test_ids"],
        )
        self.assertNotIn(
            "tree_model_constraints",
            summary["apply_engines"]["global_blocker_codes"],
        )
        self.assertIn("nqe_column_filter", summary["fetch_contracts"]["modes"])
        self.assertGreater(summary["fetch_contracts"]["shard_safe_count"], 0)
        self.assertEqual(
            summary["fetch_contracts"]["contract_registry_status"],
            "pass",
        )
        self.assertEqual(summary["fetch_contracts"]["contract_registry_gap_count"], 0)
        self.assertNotIn(
            "model_fetch_fallback",
            summary["fetch_contracts"]["fallback_reasons"],
        )
        self.assertIn(
            "structured_column_filter",
            summary["fetch_contracts"]["fallback_reasons"],
        )
        self.assertTrue(summary["query_pushdown"]["available"])
        self.assertEqual(summary["query_pushdown"]["total_stage_steps"], 1)
        self.assertEqual(summary["query_pushdown"]["fetch_mode_counts"]["model"], 1)
        self.assertEqual(summary["query_pushdown"]["fallback_step_count"], 1)
        self.assertEqual(
            summary["query_pushdown"]["fallback_reason_summary"]["top_reasons"][0][
                "reason"
            ],
            "model_fetch_contract_fallback",
        )
        self.assertEqual(
            summary["query_pushdown"]["fallback_reason_summary"]["remediation_actions"][
                0
            ]["code"],
            "add_or_enable_shard_fetch_contract",
        )
        self.assertEqual(
            summary["query_pushdown"]["partition_retry_summary"][
                "split_retry_success_count"
            ],
            1,
        )
        self.assertEqual(
            summary["query_pushdown"]["partition_retry_summary"][
                "avoided_fallback_retry_count"
            ],
            1,
        )
        self.assertEqual(summary["query_pushdown"]["efficiency"]["status"], "info")
        self.assertEqual(summary["query_pushdown"]["efficiency"]["fallback_rate"], 1.0)
        self.assertEqual(summary["query_pushdown"]["efficiency"]["pushdown_rate"], 0.0)
        self.assertEqual(
            summary["query_pushdown"]["alert_thresholds"]["fallback_warn_rate"], 0.5
        )
        self.assertEqual(
            summary["query_pushdown"]["runtime_share"]["fallback_runtime_share"],
            None,
        )
        self.assertEqual(
            summary["query_pushdown"]["runtime_share"]["full_fallback_runtime_share"],
            None,
        )
        self.assertEqual(
            summary["query_pushdown"]["diff_utilization"]["eligible_steps"],
            0,
        )
        self.assertEqual(
            summary["query_pushdown"]["diff_utilization"]["non_diff_reason_counts"],
            {},
        )
        self.assertEqual(
            summary["query_pushdown"]["diff_baseline_transition"]["code"],
            "no_diff_capable_query_identity",
        )
        self.assertEqual(
            summary["query_pushdown"]["diff_baseline_transition"]["action_code"],
            "use_query_id_or_query_path_maps",
        )
        self.assertEqual(
            summary["query_pushdown"]["trend_snapshots"][0]["run_id"],
            self.execution_run.pk,
        )
        self.assertEqual(
            summary["query_pushdown"]["trend_snapshots"][0]["non_diff_reason_counts"],
            {},
        )
        self.assertEqual(
            summary["query_pushdown"]["trend_snapshots"][0]["baseline_reason_summary"],
            "",
        )
        self.assertGreaterEqual(len(summary["query_pushdown"]["tuning_guidance"]), 1)
        self.assertEqual(
            summary["query_pushdown"]["tuning_guidance"][0]["code"],
            "fallback_pushdown_coverage",
        )
        tuning_codes = {
            item["code"] for item in summary["query_pushdown"]["tuning_guidance"]
        }
        self.assertIn("partition_retry_avoided_fallback", tuning_codes)
        self.assertIn("partition_retry_pressure", tuning_codes)
        self.assertEqual(
            summary["query_pushdown"]["slow_models"][0]["model"], "dcim.site"
        )
        self.assertIn(summary["large_run_tuning"]["status"], {"info", "warn"})
        self.assertEqual(
            summary["large_run_tuning"]["first_order_actions"][0]["code"],
            "reduce_fallback_fetch",
        )
        self.assertEqual(
            summary["large_run_tuning"]["execution_backend_advice"]["code"],
            "branching_fix_pushdown_before_capacity",
        )
        self.assertEqual(
            summary["large_run_tuning"]["execution_backend_advice"][
                "recommended_backend"
            ],
            "branching",
        )
        large_run_action_codes = {
            item["code"] for item in summary["large_run_tuning"]["first_order_actions"]
        }
        self.assertIn("keep_branching_reduce_fallback_first", large_run_action_codes)
        self.assertEqual(
            summary["large_run_tuning"]["signals"]["fallback_rate"],
            1.0,
        )
        check_names = {item["name"] for item in summary["checks"]}
        self.assertIn("Pushdown efficiency", check_names)
        self.assertIn("Large-run tuning", check_names)
        pushdown_check = next(
            item for item in summary["checks"] if item["name"] == "Pushdown efficiency"
        )
        self.assertIn("Guidance:", pushdown_check["message"])
        tuning_check = next(
            item for item in summary["checks"] if item["name"] == "Large-run tuning"
        )
        self.assertIn(
            "Reduce fallback-heavy model fetches",
            tuning_check["message"],
        )
        self.assertTrue(summary["compatibility_cache"]["ledger_history"])
        self.assertTrue(summary["compatibility_cache"]["writes_suppressed"])
        self.assertFalse(summary["compatibility_cache"]["compatibility_state_present"])
        self.assertIn(
            "ledger-only",
            summary["compatibility_cache"]["message"],
        )
        self.assertEqual(summary["density_learning"]["model_count"], 2)
        self.assertEqual(summary["density_learning"]["high_confidence_count"], 0)
        self.assertEqual(summary["density_learning"]["medium_confidence_count"], 1)
        self.assertEqual(summary["density_learning"]["low_confidence_count"], 1)
        self.assertEqual(
            summary["density_learning"]["models"][0]["model"], "dcim.cable"
        )

    def test_sync_health_summary_reports_stale_compatibility_payload(self):
        stale_sync = ForwardSync.objects.create(
            name="health-sync-stale-compat",
            source=self.source,
            parameters={
                "snapshot_id": "latestProcessed",
                BRANCH_RUN_STATE_PARAMETER: {"phase": "planning", "next_step_index": 4},
            },
        )
        ForwardExecutionRun.objects.create(
            sync=stale_sync,
            source=self.source,
            backend="branching",
            status="completed",
            snapshot_selector="latestProcessed",
            snapshot_id="snapshot-stale",
            total_steps=1,
            next_step_index=1,
        )

        summary = sync_health_summary(stale_sync)
        compat = summary["compatibility_cache"]
        self.assertTrue(compat["ledger_history"])
        self.assertTrue(compat["compatibility_state_present"])
        self.assertTrue(compat["stale_payload_present"])
        self.assertTrue(compat["prune_recommended"])
        self.assertIn("prune is recommended", compat["message"])

    @override_settings(RQ_DEFAULT_TIMEOUT=100)
    def test_large_run_tuning_advises_fast_bootstrap_on_timeout_risk(self):
        summary = large_run_tuning_summary(
            self.sync,
            capacity={
                "available": True,
                "total_steps": 50,
                "remaining_steps": 49,
                "projected_remaining_seconds": 120,
            },
            query_pushdown={
                "efficiency": {"fallback_steps": 0, "fallback_rate": 0.0},
                "runtime_share": {},
                "diff_utilization": {"diff_actual_ratio": None},
                "tuning_guidance": [],
            },
        )

        self.assertEqual(summary["status"], "warn")
        self.assertEqual(
            summary["execution_backend_advice"]["code"],
            "branching_timeout_risk_consider_bootstrap",
        )
        self.assertEqual(
            summary["execution_backend_advice"]["recommended_backend"],
            "fast_bootstrap",
        )
        self.assertEqual(
            summary["first_order_actions"][0]["code"],
            "consider_fast_bootstrap_for_trusted_baseline",
        )

    def test_large_run_tuning_recommends_safe_speed_options(self):
        self.sync.parameters = {
            **(self.sync.parameters or {}),
            "execution_backend": "branching",
            "auto_merge": True,
            "enable_bulk_orm": False,
            "scheduler_overlap": False,
        }
        self.sync.auto_merge = True

        summary = large_run_tuning_summary(
            self.sync,
            capacity={
                "available": True,
                "total_steps": 50,
                "remaining_steps": 40,
                "projected_remaining_seconds": 60,
            },
            query_pushdown={
                "efficiency": {"fallback_steps": 0, "fallback_rate": 0.0},
                "runtime_share": {},
                "diff_utilization": {"diff_actual_ratio": None},
                "tuning_guidance": [],
            },
        )

        action_codes = {item["code"] for item in summary["first_order_actions"]}
        self.assertIn("enable_safe_bulk_orm", action_codes)
        self.assertIn("consider_scheduler_overlap", action_codes)

    def test_large_run_tuning_advises_switch_back_after_fast_bootstrap(self):
        self.sync.parameters["execution_backend"] = "fast_bootstrap"

        summary = large_run_tuning_summary(
            self.sync,
            capacity={"available": False},
            query_pushdown={
                "efficiency": {"fallback_steps": 0},
                "runtime_share": {},
                "diff_utilization": {},
                "tuning_guidance": [],
            },
        )

        self.assertEqual(
            summary["execution_backend_advice"]["code"],
            "fast_bootstrap_baseline_active",
        )
        self.assertEqual(
            summary["execution_backend_advice"]["next_backend"],
            "branching",
        )
        self.assertEqual(
            summary["first_order_actions"][0]["code"],
            "complete_fast_bootstrap_then_branching",
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
        self.assertContains(
            response, "Raw query text maps cannot use Forward nqe-diffs"
        )

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
        self.assertContains(response, "Bulk ORM expansion")
        self.assertContains(response, "Parity gates")
        self.assertContains(response, "Fetch Contracts")
        self.assertContains(response, "Query Runtime")
        self.assertContains(response, "Fallback steps")
        self.assertContains(response, "Pushdown rate")
        self.assertContains(response, "Fallback rate")
        self.assertContains(response, "Efficiency advisory")
        self.assertContains(response, "Fallback runtime share")
        self.assertContains(response, "Full-fallback runtime share")
        self.assertContains(response, "Diff actual ratio")
        self.assertContains(response, "Baseline to diff")
        self.assertContains(response, "Diff baseline correlation")
        self.assertContains(response, "Tuning guidance")
        self.assertContains(response, "Large Run Tuning")
        self.assertContains(response, "First actions")
        self.assertContains(response, "Backend advice")
        self.assertContains(response, "Compatibility Cache")
        self.assertContains(response, "Compatibility payload present")
        self.assertContains(response, "Density Learning")
        self.assertContains(response, "High confidence models")
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
        self.assertTrue(data["source_health"]["latest_processed_snapshot_available"])

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

    def test_sync_pushdown_trends_downloads_long_window_history(self):
        self.client.force_login(self.user)

        response = self.client.get(
            reverse(
                "plugins:forward_netbox:forwardsync_pushdown_trends",
                kwargs={"pk": self.sync.pk},
            )
            + "?limit=180"
        )

        self.assertEqual(response.status_code, 200)
        data = json.loads(response.content)
        self.assertEqual(data["sync"]["pk"], self.sync.pk)
        self.assertTrue(data["history"]["available"])
        self.assertEqual(data["history"]["selected_limit"], 180)
        self.assertGreaterEqual(data["history"]["snapshot_count"], 1)
        first = data["history"]["trends"][0]
        self.assertIn("run_id", first)
        self.assertIn("non_diff_reason_counts", first)
        self.assertIn("baseline_reason_summary", first)

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
            result for result in data["results"] if result["mode"] == "query_path"
        )
        self.assertEqual(path_result["status"], "live_repository_source_match")
        self.assertEqual(path_result["live_query_id"], "Q_devices")
        self.assertEqual(path_result["requested_commit_id"], "head")
        self.assertEqual(path_result["commit_binding"], "latest_commit")

    def test_ingestion_health_check_marks_non_blocking_issue_baseline_as_pass(self):
        ingestion = ForwardIngestion.objects.create(
            sync=self.sync,
            snapshot_selector="latestProcessed",
            snapshot_id="snapshot-2",
            baseline_ready=True,
            applied_change_count=5,
            failed_change_count=0,
        )
        ingestion.issues.create(
            model="ipam.ipaddress",
            message="Skipping delete for `ipam.ipaddress` due to protected dependencies.",
            exception="ForwardDependencySkipError",
        )

        self.assertEqual(ingestion_check_status(ingestion), "pass")
        self.assertIn("non-blocking", ingestion_check_message(ingestion))

    def test_ingestion_health_check_marks_blocking_issue_as_warn(self):
        ingestion = ForwardIngestion.objects.create(
            sync=self.sync,
            snapshot_selector="latestProcessed",
            snapshot_id="snapshot-3",
            baseline_ready=True,
            applied_change_count=5,
            failed_change_count=0,
        )
        ingestion.issues.create(
            model="dcim.device",
            message="Unable to apply device row.",
            exception="ForwardSyncDataError",
        )

        self.assertEqual(ingestion_check_status(ingestion), "warn")
        self.assertIn("including blocking rows", ingestion_check_message(ingestion))
