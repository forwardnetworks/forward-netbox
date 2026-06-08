import json
from datetime import timedelta
from unittest.mock import Mock
from unittest.mock import patch

from core.choices import JobStatusChoices
from core.models import Job
from django.conf import settings
from django.contrib.auth import get_user_model
from django.contrib.contenttypes.models import ContentType
from django.test import override_settings
from django.test import TestCase
from django.urls import reverse
from django.utils import timezone

from forward_netbox.choices import forward_configured_models
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
from forward_netbox.utilities.health_checks import query_drift_check_message
from forward_netbox.utilities.health_summary_blocks import large_run_tuning_summary
from forward_netbox.utilities.query_registry import read_compiled_builtin_query_source


BGP_PLUGIN_CONFIG = {
    **settings.PLUGINS_CONFIG,
    "forward_netbox": {
        **settings.PLUGINS_CONFIG.get("forward_netbox", {}),
        "enable_bgp_sync": True,
    },
}


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
            model_results=[
                {
                    "model": "dcim.device",
                    "query_name": "Health Devices with NetBox Device Type Aliases",
                    "row_count": 3,
                    "delete_count": 1,
                    "execution_mode": "query_path",
                    "fetch_mode": "nqe_parameters",
                    "query_path_resolution": {
                        "available": True,
                        "query_path_spec_count": 1,
                        "artifact_hit_count": 1,
                        "client_resolve_count": 0,
                        "repository_index_count": 1,
                        "cache_hit_rate": 1.0,
                    },
                }
            ],
        )
        now = timezone.now()
        cls.execution_job = Job.objects.create(
            object_type=ContentType.objects.get_for_model(ForwardSync),
            object_id=cls.sync.pk,
            name="health execution job",
            user=None,
            status=JobStatusChoices.STATUS_COMPLETED,
            job_id="123e4567-e89b-12d3-a456-426614174099",
            created=now,
            started=now,
            completed=now,
            data={
                "forward_api_usage": {
                    "api_requests_per_minute": 1800,
                    "http_attempts": 21,
                    "http_429_failures": 0,
                    "nqe_query_calls": 4,
                    "nqe_diff_calls": 3,
                    "nqe_pages": 9,
                    "throttle_sleep_seconds": 1.5,
                    "usage_window_seconds": 60.0,
                    "observed_http_attempts_per_minute": 20.0,
                },
                "statistics": {
                    "dcim.cable": {
                        "current": 4,
                        "total": 4,
                        "applied": 1,
                        "failed": 0,
                        "skipped": 0,
                        "unchanged": 2,
                    }
                },
                "dependency_lookup_cache": {
                    "available": True,
                    "row_count": 4,
                    "primed_target_count": 7,
                    "model_count": 1,
                    "models": [
                        {
                            "model": "dcim.device",
                            "row_count": 4,
                            "primed_target_count": 7,
                            "device_name_count": 4,
                            "tag_row_count": 0,
                            "interface_pair_count": 2,
                            "module_bay_pair_count": 0,
                            "fhrp_group_count": 1,
                            "ipam_identity_row_count": 0,
                            "ipam_global_host_row_count": 0,
                        }
                    ],
                },
            },
        )
        cls.sync.last_ingestion.job = cls.execution_job
        cls.sync.last_ingestion.save(update_fields=["job"])
        cls.sync.refresh_from_db()
        cls.execution_run = ForwardExecutionRun.objects.create(
            sync=cls.sync,
            source=cls.source,
            job=cls.execution_job,
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
            query_parameters={"forward_netbox_shard_keys": ["device-1"]},
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
        self.assertEqual(summary["query_drift_summary"]["total_maps"], 2)
        self.assertEqual(
            summary["query_drift_summary"]["status_counts"],
            {
                "direct_query_id_unverified": 1,
                "repository_path_matches_bundled_filename": 1,
            },
        )
        self.assertEqual(summary["query_drift_summary"]["warn_count"], 0)
        self.assertEqual(summary["query_drift_summary"]["info_count"], 1)
        self.assertEqual(summary["query_drift_summary"]["pass_count"], 1)
        self.assertEqual(
            summary["query_drift_summary"]["remediation_actions"][0]["count"],
            1,
        )
        self.assertIn(
            "repository-path",
            summary["query_drift_summary"]["remediation_actions"][0]["message"],
        )
        self.assertIn(
            "Top remediation:",
            query_drift_check_message(
                [
                    {
                        "severity": "info",
                        "remediation": "Prefer repository-path bindings for reproducible drift checks; pin a commit if the direct query ID must remain.",
                    }
                ],
                query_drift_summary=summary["query_drift_summary"],
            ),
        )
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
        self.assertEqual(
            summary["latest_ingestion"]["query_modes"]["execution_modes"],
            {"query_path": 1},
        )
        self.assertEqual(
            summary["latest_ingestion"]["query_modes"]["fetch_modes"],
            {"nqe_parameters": 1},
        )
        self.assertIn("execution_summary", summary["latest_ingestion"])
        self.assertIn("workload_preview", summary["latest_ingestion"])
        self.assertEqual(
            summary["analysis_summary"],
            summary["latest_ingestion"]["analysis_summary"],
        )
        self.assertEqual(
            summary["query_path_resolution"]["total_query_path_specs"],
            1,
        )
        self.assertEqual(
            summary["latest_ingestion"]["query_path_resolution"][
                "total_query_path_specs"
            ],
            1,
        )
        self.assertTrue(
            summary["latest_ingestion"]["dependency_lookup_cache"]["available"]
        )
        self.assertEqual(
            summary["latest_ingestion"]["query_path_resolution"]["artifact_hit_count"],
            1,
        )
        self.assertEqual(
            summary["latest_ingestion"]["query_path_resolution"][
                "repository_index_count"
            ],
            1,
        )
        self.assertTrue(summary["api_usage"]["available"])
        self.assertEqual(summary["api_usage"]["budget"]["status"], "passed")
        self.assertEqual(summary["api_usage"]["counters"]["http_attempts"], 21)
        self.assertEqual(
            summary["api_usage"]["budget"]["metrics"][
                "observed_http_attempts_per_minute"
            ],
            20.0,
        )
        self.assertEqual(
            summary["api_usage"]["budget"]["metrics"]["throttle_sleep_seconds"],
            1.5,
        )
        self.assertTrue(summary["api_usage"]["step_query_parameters"]["available"])
        self.assertEqual(summary["api_usage"]["step_query_parameters"]["step_count"], 1)
        self.assertEqual(
            summary["api_usage"]["step_query_parameters"]["top_steps"][0][
                "query_parameters"
            ],
            {"forward_netbox_shard_keys": ["device-1"]},
        )
        self.assertTrue(summary["dependency_lookup_cache"]["available"])
        self.assertEqual(summary["dependency_lookup_cache"]["row_count"], 4)
        self.assertEqual(summary["dependency_lookup_cache"]["model_count"], 1)
        self.assertEqual(
            summary["dependency_lookup_cache"]["models"][0]["fhrp_group_count"], 1
        )
        self.assertEqual(summary["capacity"]["completed_steps"], 1)
        self.assertEqual(summary["capacity"]["remaining_steps"], 1)
        self.assertEqual(summary["capacity"]["average_completed_step_seconds"], 20.0)
        self.assertEqual(summary["throughput"]["completed_shards"], 1)
        self.assertEqual(summary["throughput"]["remaining_shards"], 0)
        self.assertEqual(summary["throughput"]["shards_per_hour_1h"], 1.0)
        self.assertEqual(summary["throughput"]["apply_time_seconds_average"], 20.0)
        self.assertEqual(summary["throughput"]["bottleneck_phase"], "apply")
        self.assertEqual(
            summary["throughput"]["query_fetch_concurrency"],
            summary["runtime"]["query_fetch_concurrency"],
        )
        self.assertEqual(summary["throughput"]["nqe_page_size"], 10000)
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
        self.assertIn("nqe_parameters", summary["fetch_contracts"]["modes"])
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
            "structured_query_parameter",
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
        self.assertEqual(
            summary["large_run_tuning"]["adaptive_capacity"]["decision"],
            "hold_reduce_fallback_first",
        )
        check_names = {item["name"] for item in summary["checks"]}
        self.assertIn("Pushdown efficiency", check_names)
        self.assertIn("Large-run tuning", check_names)
        self.assertIn("Adaptive capacity", check_names)
        self.assertIn("Run throughput", check_names)
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

    @override_settings(PLUGINS_CONFIG=BGP_PLUGIN_CONFIG)
    def test_dependency_preflight_warns_for_interface_without_bgp_models(self):
        sync = self._sync_with_enabled_models(
            "health-sync-interface-no-bgp",
            ["dcim.interface", "ipam.ipaddress"],
        )
        ForwardIngestion.objects.create(
            sync=sync,
            snapshot_selector="latestProcessed",
            snapshot_id="snapshot-dependency",
            baseline_ready=True,
        )

        summary = sync_health_summary(sync)
        preflight = summary["dependency_preflight"]

        self.assertEqual(preflight["status"], "warn")
        self.assertTrue(preflight["delete_or_prune_possible"])
        self.assertIn(
            "baseline_ready_for_diff_deletes",
            preflight["delete_or_prune_evidence"],
        )
        interface_warning = next(
            item
            for item in preflight["warnings"]
            if item["selected_model"] == "dcim.interface"
        )
        self.assertEqual(
            interface_warning["omitted_models"],
            [
                "netbox_routing.bgppeer",
                "netbox_routing.bgppeeraddressfamily",
                "netbox_peering_manager.peeringsession",
            ],
        )
        self.assertIn(
            "netbox_routing.bgppeer",
            interface_warning["suggested_models"],
        )
        self.assertIsNotNone(interface_warning["delete_dependency_rank"])
        dependency_check = next(
            item
            for item in summary["checks"]
            if item["name"] == "Scoped dependency preflight"
        )
        self.assertEqual(dependency_check["status"], "warn")
        self.assertIn("netbox_routing.bgppeer", dependency_check["message"])

    @override_settings(PLUGINS_CONFIG=BGP_PLUGIN_CONFIG)
    def test_dependency_preflight_passes_when_routing_models_are_enabled(self):
        sync = self._sync_with_enabled_models(
            "health-sync-interface-with-bgp",
            [
                "dcim.interface",
                "ipam.ipaddress",
                "netbox_routing.bgppeer",
                "netbox_routing.bgppeeraddressfamily",
                "netbox_peering_manager.peeringsession",
            ],
        )

        summary = sync_health_summary(sync)

        self.assertEqual(summary["dependency_preflight"]["status"], "pass")
        self.assertEqual(summary["dependency_preflight"]["warnings"], [])
        dependency_check = next(
            item
            for item in summary["checks"]
            if item["name"] == "Scoped dependency preflight"
        )
        self.assertEqual(dependency_check["status"], "pass")

    def test_delete_wave_summary_reports_planned_deletes_and_dependency_skips(self):
        sync = self._sync_with_enabled_models(
            "health-sync-delete-wave",
            ["dcim.device", "dcim.interface"],
        )
        ingestion = ForwardIngestion.objects.create(
            sync=sync,
            snapshot_selector="latestProcessed",
            snapshot_id="snapshot-delete-wave",
            baseline_ready=True,
            deleted_change_count=1,
        )
        ingestion.issues.create(
            model="dcim.interface",
            message="Skipping delete for `dcim.interface` due to protected dependencies.",
            exception="ForwardDependencySkipError",
        )
        run = ForwardExecutionRun.objects.create(
            sync=sync,
            source=self.source,
            backend="branching",
            status="running",
            snapshot_selector="latestProcessed",
            snapshot_id="snapshot-delete-wave",
            total_steps=2,
            next_step_index=1,
            plan_preview={
                "delete_dependency_plan": {
                    "status": "high",
                    "delete_rows": 1250,
                    "delete_shards": 2,
                    "delete_model_count": 2,
                    "delete_share": 0.62,
                    "max_delete_shard_changes": 9000,
                    "execution_order": ["dcim.interface", "dcim.device"],
                    "models": {
                        "dcim.interface": {
                            "delete_rows": 50,
                            "delete_shards": 1,
                            "reference_blocker_risk": "medium",
                        },
                        "dcim.device": {
                            "delete_rows": 1200,
                            "delete_shards": 1,
                            "reference_blocker_risk": "high",
                        },
                    },
                    "warnings": [
                        {
                            "code": "delete_wave",
                            "severity": "warning",
                            "message": "Delete work is a material share of this plan.",
                        }
                    ],
                }
            },
        )
        ForwardExecutionStep.objects.create(
            run=run,
            index=1,
            kind="stage",
            status="running",
            model_string="dcim.interface",
            operation="apply",
            estimated_changes=100,
        )
        ForwardExecutionStep.objects.create(
            run=run,
            index=2,
            kind="stage",
            status="pending",
            model_string="dcim.device",
            operation="delete",
            estimated_changes=9000,
        )

        summary = sync_health_summary(sync)
        delete_wave = summary["delete_wave"]

        self.assertEqual(delete_wave["status"], "warn")
        self.assertEqual(delete_wave["phase"], "apply_before_delete")
        self.assertEqual(delete_wave["plan"]["delete_rows"], 1250)
        self.assertEqual(delete_wave["plan"]["execution_order"][0], "dcim.interface")
        self.assertEqual(delete_wave["steps"]["delete_step_count"], 1)
        self.assertEqual(delete_wave["steps"]["pending_apply_step_count"], 1)
        self.assertEqual(
            delete_wave["latest_ingestion"]["dependency_skip_issues"]["count"],
            1,
        )
        delete_check = next(
            item for item in summary["checks"] if item["name"] == "Delete wave"
        )
        self.assertEqual(delete_check["status"], "warn")
        self.assertIn("planned after earlier apply shards", delete_check["message"])

    @override_settings(PLUGINS_CONFIG=BGP_PLUGIN_CONFIG)
    def test_sync_health_view_renders_dependency_preflight_warning(self):
        sync = self._sync_with_enabled_models(
            "health-sync-dependency-view",
            ["dcim.interface"],
        )
        ForwardIngestion.objects.create(
            sync=sync,
            snapshot_selector="latestProcessed",
            snapshot_id="snapshot-dependency-view",
            baseline_ready=True,
        )
        self.client.force_login(self.user)

        response = self.client.get(
            reverse(
                "plugins:forward_netbox:forwardsync_health",
                kwargs={"pk": sync.pk},
            )
        )

        self.assertContains(response, "Scoped dependency preflight")
        self.assertContains(response, "netbox_routing.bgppeer")

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

    def test_sync_health_summary_reports_optional_plugin_capabilities(self):
        summary = sync_health_summary(self.sync)
        self.assertIn("optional_plugin_capabilities", summary)
        self.assertIn("optional_plugin_capabilities_ui", summary)
        self.assertIn(
            "aci.netbox_cisco_aci",
            summary["optional_plugin_capabilities"],
        )
        self.assertIn(
            "routing.netbox_routing",
            summary["optional_plugin_capabilities"],
        )
        self.assertIn(
            "peering.netbox_peering_manager",
            summary["optional_plugin_capabilities"],
        )
        self.assertIn("aci", summary["optional_plugin_capabilities_ui"])
        self.assertIn("routing", summary["optional_plugin_capabilities_ui"])
        self.assertIn("peering", summary["optional_plugin_capabilities_ui"])
        self.assertIn(
            "availability_status",
            summary["optional_plugin_capabilities"]["aci.netbox_cisco_aci"],
        )
        self.assertIn(
            "availability_reason",
            summary["optional_plugin_capabilities"]["aci.netbox_cisco_aci"],
        )
        self.assertIn(
            "version",
            summary["optional_plugin_capabilities"]["aci.netbox_cisco_aci"],
        )
        self.assertIn(
            "minimum_version",
            summary["optional_plugin_capabilities"]["aci.netbox_cisco_aci"],
        )
        self.assertIn(
            "package_names",
            summary["optional_plugin_capabilities"]["aci.netbox_cisco_aci"],
        )
        self.assertIn(
            "installed_package_name",
            summary["optional_plugin_capabilities"]["aci.netbox_cisco_aci"],
        )
        self.assertIn(
            "command_inventory",
            summary["optional_plugin_capabilities"]["aci.netbox_cisco_aci"],
        )
        self.assertEqual(
            summary["optional_plugin_capabilities"]["aci.netbox_cisco_aci"][
                "command_inventory_count"
            ],
            16,
        )

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

    def test_adaptive_capacity_recommends_one_tuning_batch(self):
        sync = self._sync_with_source_parameters(
            "health-sync-adaptive-recommend",
            {
                "timeout": 1200,
                "query_fetch_concurrency": 8,
                "nqe_page_size": 8000,
                "runtime_capacity_evidence": {
                    "active_worker_count": 12,
                    "database_headroom": "available",
                    "worker_headroom": "available",
                    "queue_backlog_depth": 3,
                },
            },
        )

        summary = large_run_tuning_summary(
            sync,
            capacity={"available": True, "total_steps": 80, "remaining_steps": 70},
            query_pushdown={
                "available": True,
                "efficiency": {"fallback_steps": 0, "fallback_rate": 0.0},
                "runtime_share": {},
                "diff_utilization": {},
                "tuning_guidance": [],
            },
            throughput={
                "available": True,
                "shards_per_hour_1h": 2.0,
                "shards_per_hour_6h": 2.0,
                "issue_rate_per_hour": 0.5,
                "bottleneck_phase": "fetch",
            },
        )

        adaptive = summary["adaptive_capacity"]
        batch = adaptive["next_tuning_batch"]

        self.assertEqual(adaptive["status"], "warn")
        self.assertEqual(adaptive["decision"], "recommend_tuning_batch")
        self.assertEqual(batch["worker_count"]["recommended"], 18)
        self.assertEqual(batch["query_fetch_concurrency"]["recommended"], 10)
        self.assertEqual(batch["nqe_page_size"]["recommended"], 9600)
        self.assertEqual(batch["restart_scope"], "restart_workers_only")
        self.assertEqual(batch["hold_minutes"], 60)

    def test_adaptive_capacity_holds_when_throughput_is_healthy(self):
        sync = self._sync_with_source_parameters(
            "health-sync-adaptive-hold",
            {
                "runtime_capacity_evidence": {
                    "active_worker_count": 12,
                    "database_headroom": "available",
                },
            },
        )

        summary = large_run_tuning_summary(
            sync,
            capacity={"available": True, "total_steps": 20, "remaining_steps": 10},
            query_pushdown={
                "available": True,
                "efficiency": {"fallback_steps": 0, "fallback_rate": 0.0},
                "runtime_share": {},
                "diff_utilization": {},
                "tuning_guidance": [],
            },
            throughput={
                "available": True,
                "shards_per_hour_1h": 6.0,
                "shards_per_hour_6h": 5.5,
                "issue_rate_per_hour": 0.0,
                "bottleneck_phase": "apply",
            },
        )

        adaptive = summary["adaptive_capacity"]

        self.assertEqual(adaptive["status"], "pass")
        self.assertEqual(adaptive["decision"], "hold_current_settings")

    def test_adaptive_capacity_rolls_back_on_issue_spike(self):
        sync = self._sync_with_source_parameters(
            "health-sync-adaptive-rollback",
            {
                "runtime_capacity_evidence": {
                    "active_worker_count": 12,
                    "database_headroom": "available",
                },
            },
        )

        summary = large_run_tuning_summary(
            sync,
            capacity={"available": True, "total_steps": 20, "remaining_steps": 10},
            query_pushdown={
                "available": True,
                "efficiency": {"fallback_steps": 0, "fallback_rate": 0.0},
                "runtime_share": {},
                "diff_utilization": {},
                "tuning_guidance": [],
            },
            throughput={
                "available": True,
                "shards_per_hour_1h": 2.0,
                "shards_per_hour_6h": 2.0,
                "issue_rate_per_hour": 3.0,
                "bottleneck_phase": "apply",
            },
        )

        adaptive = summary["adaptive_capacity"]

        self.assertEqual(adaptive["status"], "warn")
        self.assertEqual(adaptive["decision"], "rollback_latest_tuning_batch")

    def test_adaptive_capacity_requires_worker_and_database_evidence(self):
        sync = self._sync_with_source_parameters(
            "health-sync-adaptive-insufficient",
            {"query_fetch_concurrency": 8, "nqe_page_size": 8000},
        )

        summary = large_run_tuning_summary(
            sync,
            capacity={"available": True, "total_steps": 80, "remaining_steps": 70},
            query_pushdown={
                "available": True,
                "efficiency": {"fallback_steps": 0, "fallback_rate": 0.0},
                "runtime_share": {},
                "diff_utilization": {},
                "tuning_guidance": [],
            },
            throughput={
                "available": True,
                "shards_per_hour_1h": 2.0,
                "shards_per_hour_6h": 2.0,
                "issue_rate_per_hour": 0.5,
                "bottleneck_phase": "fetch",
            },
        )

        adaptive = summary["adaptive_capacity"]

        self.assertEqual(adaptive["status"], "info")
        self.assertEqual(adaptive["decision"], "insufficient_evidence")
        self.assertEqual(adaptive["capacity_evidence"]["status"], "unknown")

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
        self.assertContains(response, "Query Path Resolution")
        self.assertContains(response, "Local Query Drift")
        self.assertContains(response, "Remediation")
        self.assertContains(response, "Commit")
        self.assertContains(response, "latest committed Forward query revision")
        self.assertContains(response, "repository-path bindings")
        self.assertContains(response, "Apply Engines")
        self.assertContains(response, "Bulk ORM expansion")
        self.assertContains(response, "Parity gates")
        self.assertContains(response, "Fetch Contracts")
        self.assertContains(response, "Forward API Usage")
        self.assertContains(response, "Budget status")
        self.assertContains(response, "Configured rate")
        self.assertContains(response, "Observed rate")
        self.assertContains(response, "HTTP 429 failures")
        self.assertContains(response, "Throttle sleep")
        self.assertContains(response, "Query parameters")
        self.assertContains(response, "Dependency Lookup Cache")
        self.assertContains(response, "Optional Plugin Capabilities")
        self.assertContains(response, "NetBox Routing")
        self.assertContains(response, "NetBox Peering Manager")
        self.assertContains(response, "Installed version")
        self.assertContains(response, "Minimum version")
        self.assertContains(response, "Package names")
        self.assertContains(response, "Detected package")
        self.assertContains(response, "Availability status")
        self.assertContains(response, "Availability reason")
        self.assertContains(response, "Command inventory entries")
        self.assertContains(response, "Command inventory")
        self.assertContains(response, "CISCO_ACI_FABRIC_NODES")
        self.assertContains(response, "Required models missing")
        self.assertContains(response, "Missing required models")
        self.assertContains(response, "Missing optional models")
        self.assertContains(response, "FHRP groups")
        self.assertContains(response, "NQE calls")
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
        self.assertContains(response, "Delete Wave")
        self.assertContains(response, "Compatibility Cache")
        self.assertContains(response, "Compatibility payload present")
        self.assertContains(response, "Density Learning")
        self.assertContains(response, "High confidence models")
        self.assertContains(response, "Run Throughput")
        self.assertContains(response, "Throughput summary")
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
        client.get_nqe_repository_query_index.return_value = {"by_query_id": {}}
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
        self.assertEqual(data["query_drift_summary"]["total_maps"], 2)
        self.assertEqual(
            data["query_drift_summary"]["status_counts"],
            {
                "direct_query_id_unverified": 1,
                "repository_path_matches_bundled_filename": 1,
            },
        )
        self.assertEqual(data["query_drift_summary"]["warn_count"], 0)
        self.assertEqual(data["query_drift_summary"]["info_count"], 1)
        self.assertEqual(data["query_drift_summary"]["pass_count"], 1)
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

    def _sync_with_enabled_models(self, name, enabled_models):
        enabled_models = set(enabled_models)
        return ForwardSync.objects.create(
            name=name,
            source=self.source,
            parameters={
                "snapshot_id": "latestProcessed",
                **{
                    model_string: model_string in enabled_models
                    for model_string in forward_configured_models()
                },
            },
        )

    def _sync_with_source_parameters(self, name, source_parameters):
        source = ForwardSource.objects.create(
            name=f"{name}-source",
            type="saas",
            url="https://fwd.app",
            status=ForwardSourceStatusChoices.READY,
            parameters={
                "username": "user@example.com",
                "password": "secret",
                "verify": True,
                "network_id": "test-network",
                **dict(source_parameters or {}),
            },
        )
        return ForwardSync.objects.create(
            name=name,
            source=source,
            parameters={"snapshot_id": "latestProcessed"},
        )
