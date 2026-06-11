import io
import json
import zipfile
from unittest.mock import patch

from core.choices import JobStatusChoices
from core.choices import ObjectChangeActionChoices
from core.models import Job
from core.models import ObjectType
from django.contrib.auth import get_user_model
from django.contrib.contenttypes.models import ContentType
from django.test import TestCase
from django.urls import reverse
from django.utils import timezone
from netbox_branching.models import Branch
from netbox_branching.models import ChangeDiff

from forward_netbox.models import ForwardExecutionRun
from forward_netbox.models import ForwardExecutionStep
from forward_netbox.models import ForwardIngestion
from forward_netbox.models import ForwardSource
from forward_netbox.models import ForwardSync
from forward_netbox.utilities.branch_budget import BRANCH_RUN_STATE_PARAMETER
from forward_netbox.utilities.execution_ledger import active_execution_run
from forward_netbox.utilities.health import sync_health_summary


class ForwardIngestionLogExportViewTest(TestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls._live_diagnostics_patcher = patch(
            "forward_netbox.utilities.execution_ledger_serialization.live_support_diagnostics",
            return_value={
                "available": True,
                "source_health": {
                    "available": True,
                    "reachable": True,
                    "checks": [],
                },
                "query_drift": {
                    "available": True,
                    "summary": {"status_counts": {"pass": 1}},
                    "results": [],
                    "error": "",
                },
                "data_file_health": {
                    "enabled_data_file_map_count": 0,
                    "required_data_files": [],
                    "snapshot_selector": "latestProcessed",
                    "checks": [],
                    "results": [],
                },
                "enabled_map_count": 1,
            },
        )
        cls._live_diagnostics_patcher.start()

    @classmethod
    def tearDownClass(cls):
        cls._live_diagnostics_patcher.stop()
        super().tearDownClass()

    @classmethod
    def setUpTestData(cls):
        User = get_user_model()
        cls.user = User.objects.create_superuser(
            username="log-export-admin",
            password="TestPassword123!",
            email="admin@example.com",
        )
        cls.source = ForwardSource.objects.create(
            name="source-log-export",
            type="saas",
            url="https://fwd.app",
            parameters={
                "username": "user@example.com",
                "password": "secret",
                "verify": True,
                "network_id": "test-network",
            },
        )
        cls.sync = ForwardSync.objects.create(
            name="sync-log-export",
            source=cls.source,
            parameters={"snapshot_id": "latestProcessed"},
        )
        cls.ingestion = ForwardIngestion.objects.create(
            sync=cls.sync,
            snapshot_selector="latestProcessed",
            snapshot_id="snapshot-1",
            model_results=[
                {
                    "model": "ipam.prefix",
                    "query_name": "Forward Prefixes",
                    "execution_mode": "query_path",
                    "fetch_mode": "nqe_parameters",
                    "row_count": 1,
                    "delete_count": 0,
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
        cls.branch = Branch.objects.create(
            name="log-export-field-summary",
            schema_id="log_export_field_summary",
        )

        now = timezone.now()
        cls.job = Job.objects.create(
            object_type=ContentType.objects.get_for_model(ForwardIngestion),
            object_id=cls.ingestion.pk,
            name="ingestion-log-export-job",
            user=None,
            status=JobStatusChoices.STATUS_COMPLETED,
            job_id="123e4567-e89b-12d3-a456-426614174000",
            created=now,
            started=now,
            completed=now,
            data={
                "logs": [
                    [
                        now.isoformat(),
                        "success",
                        "sync-log-export",
                        "/plugins/forward/ingestion/1/",
                        "Synthetic sync stage completed.",
                    ]
                ],
                "forward_api_usage": {
                    "api_requests_per_minute": 1800,
                    "http_attempts": 11,
                    "http_429_failures": 0,
                    "nqe_query_calls": 3,
                    "nqe_diff_calls": 2,
                    "nqe_pages": 5,
                    "throttle_sleep_seconds": 0.75,
                    "usage_window_seconds": 30.0,
                    "observed_http_attempts_per_minute": 20.0,
                },
                "statistics": {
                    "dcim.site": {"current": 1, "total": 1},
                    "dcim.cable": {
                        "current": 4,
                        "total": 4,
                        "applied": 1,
                        "failed": 0,
                        "skipped": 0,
                        "unchanged": 2,
                    },
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
        cls.merge_job = Job.objects.create(
            object_type=ContentType.objects.get_for_model(ForwardIngestion),
            object_id=cls.ingestion.pk,
            name="ingestion-log-export-merge-job",
            user=None,
            status=JobStatusChoices.STATUS_COMPLETED,
            job_id="223e4567-e89b-12d3-a456-426614174001",
            created=now,
            started=now,
            completed=now,
            data={
                "logs": [
                    [
                        now.isoformat(),
                        "failure",
                        "sync-log-export",
                        "/plugins/forward/ingestion/1/",
                        "Synthetic merge stage failed.",
                    ]
                ],
                "statistics": {"dcim.site": {"current": 0, "total": 1}},
            },
        )
        cls.job.log_entries = [
            {
                "timestamp": now,
                "level": "info",
                "message": "Synthetic sync stage completed.",
            }
        ]
        cls.job.save(update_fields=["log_entries"])
        cls.merge_job.log_entries = [
            {
                "timestamp": now,
                "level": "error",
                "message": "Synthetic merge stage failed.",
            }
        ]
        cls.merge_job.save(update_fields=["log_entries"])
        cls.ingestion.job = cls.job
        cls.ingestion.merge_job = cls.merge_job
        cls.ingestion.branch = cls.branch
        cls.ingestion.save(update_fields=["job", "merge_job", "branch"])
        prefix_type = ObjectType.objects.get(app_label="ipam", model="prefix")
        ChangeDiff.objects.create(
            branch=cls.branch,
            object_type=prefix_type,
            object_id=1001,
            object_repr="192.0.2.0/27",
            action=ObjectChangeActionChoices.ACTION_UPDATE,
            original={
                "prefix": "192.0.2.0/27",
                "vrf": 100,
                "status": "active",
                "last_updated": "2026-06-03T19:00:00Z",
            },
            modified={
                "prefix": "192.0.2.0/27",
                "vrf": 200,
                "status": "active",
                "last_updated": "2026-06-03T19:05:00Z",
            },
            current={},
            conflicts=[],
        )
        cls.execution_run = ForwardExecutionRun.objects.create(
            sync=cls.sync,
            source=cls.source,
            job=cls.job,
            backend="branching",
            status="running",
            snapshot_selector="latestProcessed",
            snapshot_id="snapshot-1",
            total_steps=1,
            next_step_index=1,
        )
        cls.execution_step = ForwardExecutionStep.objects.create(
            run=cls.execution_run,
            index=1,
            status="merge_queued",
            model_string="dcim.site",
            query_name="Forward Sites",
            execution_mode="query_id",
            execution_value="query-1",
            query_parameters={"forward_netbox_shard_keys": ["device-1"]},
            fetch_parameters={
                "partition_retry_summary": {
                    "operation": "full",
                    "partition_count": 1,
                    "split_retry_count": 0,
                    "split_retry_success_count": 0,
                    "alternate_operator_retry_count": 1,
                    "alternate_operator_success_count": 1,
                }
            },
            ingestion=cls.ingestion,
            job=cls.job,
            merge_job=cls.merge_job,
            estimated_changes=1,
        )
        cls.sync.set_branch_run_state(
            {
                "execution_run_id": cls.execution_run.pk,
                "next_plan_index": 1,
                "total_plan_items": 1,
            }
        )

    def test_export_logs_downloads_json_bundle(self):
        self.client.force_login(self.user)

        response = self.client.get(
            reverse(
                "plugins:forward_netbox:forwardingestion_export_logs",
                kwargs={"pk": self.ingestion.pk},
            )
            + "?stage=merge"
        )

        self.assertEqual(
            response.status_code,
            200,
            response.content.decode("utf-8", errors="replace"),
        )
        self.assertIn("attachment;", response["Content-Disposition"])
        self.assertIn("forward-ingestion-", response["Content-Disposition"])

        data = json.loads(response.content)
        self.assertEqual(data["active_stage"], "merge")
        self.assertEqual(data["ingestion"]["pk"], self.ingestion.pk)
        self.assertEqual(data["ingestion"]["job"]["pk"], self.job.pk)
        self.assertEqual(data["ingestion"]["merge_job"]["pk"], self.merge_job.pk)
        self.assertEqual(data["sync"]["execution_state_source"], "execution_ledger")
        self.assertIn("execution_plan", data)
        self.assertEqual(data["execution_run"]["run"]["id"], self.execution_run.pk)
        self.assertEqual(data["execution_run"]["steps"][0]["job"], self.job.pk)
        self.assertEqual(
            data["execution_run"]["steps"][0]["merge_job"], self.merge_job.pk
        )
        self.assertEqual(
            data["execution_run"]["steps"][0]["job_detail"]["pk"], self.job.pk
        )
        self.assertEqual(
            data["job_results"]["logs"][0][4], "Synthetic sync stage completed."
        )
        self.assertEqual(
            data["merge_job_results"]["logs"][0][4],
            "Synthetic merge stage failed.",
        )
        self.assertEqual(data["job_results"]["statistics"]["dcim.site"]["total"], 1)
        self.assertEqual(
            data["merge_job_results"]["statistics"]["dcim.site"]["total"], 1
        )
        self.assertEqual(
            data["change_explainability"]["top_changed_fields"],
            [{"field": "vrf", "count": 1}],
        )

    def test_ingestion_detail_renders_change_explainability(self):
        self.client.force_login(self.user)

        response = self.client.get(self.ingestion.get_absolute_url())

        self.assertEqual(
            response.status_code,
            200,
            response.content.decode("utf-8", errors="replace"),
        )
        self.assertContains(response, "Change Explainability")
        self.assertContains(response, "ipam.prefix 1")
        self.assertContains(response, "vrf 1")

    def test_ingestion_poll_refreshes_change_explainability(self):
        self.client.force_login(self.user)

        response = self.client.get(
            reverse(
                "plugins:forward_netbox:forwardingestion_logs",
                kwargs={"pk": self.ingestion.pk},
            ),
            headers={"HX-Request": "true"},
        )

        self.assertEqual(
            response.status_code,
            200,
            response.content.decode("utf-8", errors="replace"),
        )
        self.assertContains(response, 'id="change_explainability"')
        self.assertContains(response, "ipam.prefix 1")
        self.assertContains(response, "vrf 1")

    def test_export_logs_uses_ledger_branch_state_when_cache_is_absent(self):
        self.sync.clear_branch_run_state()
        self.client.force_login(self.user)

        response = self.client.get(
            reverse(
                "plugins:forward_netbox:forwardingestion_export_logs",
                kwargs={"pk": self.ingestion.pk},
            )
        )

        self.assertEqual(
            response.status_code,
            200,
            response.content.decode("utf-8", errors="replace"),
        )
        data = json.loads(response.content)
        self.assertEqual(
            data["sync"]["execution_state"]["execution_run_id"],
            self.execution_run.pk,
        )
        self.assertEqual(data["sync"]["execution_state_source"], "execution_ledger")
        self.assertTrue(data["sync"]["execution_state"]["state_synthesized"])
        self.assertEqual(data["execution_plan"]["total_plan_items"], 1)
        self.assertEqual(data["execution_run"]["run"]["id"], self.execution_run.pk)

    def test_export_logs_compacts_large_execution_plan_items(self):
        sync = ForwardSync.objects.create(
            name="sync-log-export-plan-items",
            source=self.source,
            parameters={"snapshot_id": "latestProcessed"},
        )
        ingestion = ForwardIngestion.objects.create(
            sync=sync,
            snapshot_selector="latestProcessed",
            snapshot_id="snapshot-2",
        )
        sync.set_branch_run_state(
            {
                "snapshot_id": "snapshot-2",
                "phase": "planning",
                "total_plan_items": 99,
                "plan_items": [
                    {
                        "index": index,
                        "status": "queued",
                        "model": "dcim.device",
                    }
                    for index in range(99)
                ],
            }
        )

        self.client.force_login(self.user)

        response = self.client.get(
            reverse(
                "plugins:forward_netbox:forwardingestion_export_logs",
                kwargs={"pk": ingestion.pk},
            )
        )

        self.assertEqual(
            response.status_code,
            200,
            response.content.decode("utf-8", errors="replace"),
        )
        data = json.loads(response.content)
        self.assertEqual(data["execution_plan"]["total_plan_items"], 99)
        self.assertEqual(data["execution_plan"]["plan_items_count"], 99)
        self.assertTrue(data["execution_plan"]["plan_items_truncated"])
        self.assertEqual(len(data["execution_plan"]["plan_items"]), 25)
        self.assertEqual(
            data["sync"]["execution_state"]["plan_items_count"],
            99,
        )
        self.assertEqual(
            len(data["sync"]["execution_state"]["plan_items"]),
            25,
        )

    def test_ingestion_views_compact_execution_state_for_large_plan_items(self):
        legacy_sync = ForwardSync.objects.create(
            name="sync-log-export-legacy-state",
            source=self.source,
            parameters={"snapshot_id": "latestProcessed"},
        )
        legacy_ingestion = ForwardIngestion.objects.create(
            sync=legacy_sync,
            snapshot_selector="latestProcessed",
            snapshot_id="snapshot-3",
        )
        legacy_sync.set_branch_run_state(
            {
                "phase": "planning",
                "total_plan_items": 99,
                "next_plan_index": 2,
                "plan_items": [
                    {
                        "index": index,
                        "status": "queued",
                        "model": "dcim.device",
                    }
                    for index in range(99)
                ],
            }
        )

        self.client.force_login(self.user)

        detail_response = self.client.get(
            reverse(
                "plugins:forward_netbox:forwardingestion",
                kwargs={"pk": legacy_ingestion.pk},
            )
        )
        logs_response = self.client.get(
            reverse(
                "plugins:forward_netbox:forwardingestion_logs",
                kwargs={"pk": legacy_ingestion.pk},
            )
        )
        progress_response = self.client.get(
            reverse(
                "plugins:forward_netbox:forwardingestion_progress",
                kwargs={"pk": legacy_ingestion.pk},
            )
        )

        self.assertEqual(detail_response.status_code, 200)
        self.assertEqual(logs_response.status_code, 200)
        self.assertEqual(progress_response.status_code, 200)

        for response in [detail_response, logs_response, progress_response]:
            execution_state = response.context["execution_state"]
            self.assertEqual(execution_state["plan_items_count"], 99)
            self.assertTrue(execution_state["plan_items_truncated"])
            self.assertEqual(len(execution_state["plan_items"]), 25)

    def test_execution_run_support_bundle_downloads_json_bundle(self):
        self.job.data = {
            **self.job.data,
            "dependency_parent_coverage": {
                "available": True,
                "source": "run_job_data.dependency_parent_coverage",
                "row_count": 8,
                "blocked_row_count": 3,
                "missing_parent_count": 1,
                "model_count": 1,
                "models": [
                    {
                        "available": True,
                        "model": "dcim.interface",
                        "row_count": 8,
                        "blocked_row_count": 3,
                        "missing_parent_count": 1,
                        "missing_parent_names": ["device-1"],
                        "groups": [
                            {
                                "parent_model": "dcim.device",
                                "parent_field": "device",
                                "parent_name": "device-1",
                                "row_count": 3,
                                "sample_rows": ["eth1/1", "eth1/2"],
                            }
                        ],
                    }
                ],
            },
        }
        self.job.save(update_fields=["data"])
        self.client.force_login(self.user)

        response = self.client.get(
            reverse(
                "plugins:forward_netbox:forwardexecutionrun_export_bundle",
                kwargs={"pk": self.execution_run.pk},
            )
        )

        self.assertEqual(
            response.status_code,
            200,
            response.content.decode("utf-8", errors="replace"),
        )
        self.assertIn("attachment;", response["Content-Disposition"])
        self.assertIn(
            f"forward-execution-run-{self.execution_run.pk}-support-bundle.json",
            response["Content-Disposition"],
        )

        data = json.loads(response.content)
        self.assertEqual(data["run"]["id"], self.execution_run.pk)
        self.assertIn("compatibility_cache", data)
        self.assertTrue(data["compatibility_cache"]["ledger_history"])
        self.assertTrue(data["compatibility_cache"]["active_execution_run"])
        self.assertEqual(
            data["compatibility_cache"]["active_execution_run_id"],
            self.execution_run.pk,
        )
        self.assertFalse(data["compatibility_cache"]["stale_payload_present"])
        self.assertIn("recovery_policy_summary", data)
        self.assertEqual(
            data["query_drift_summary"],
            sync_health_summary(self.sync)["query_drift_summary"],
        )
        self.assertEqual(
            data["query_drift_results"],
            sync_health_summary(self.sync)["query_modes"]["local_drift"],
        )
        self.assertIn("remediation", data["query_drift_results"][0])
        self.assertTrue(data["dependency_lookup_cache"]["available"])
        self.assertEqual(data["dependency_lookup_cache"]["row_count"], 4)
        self.assertTrue(data["dependency_parent_coverage"]["available"])
        self.assertEqual(data["dependency_parent_coverage"]["row_count"], 8)
        self.assertIn("latest_ingestion", data)
        self.assertEqual(data["latest_ingestion"]["id"], self.ingestion.pk)
        self.assertEqual(
            data["analysis_summary"],
            data["latest_ingestion"]["analysis_summary"],
        )
        self.assertEqual(
            data["query_path_resolution"]["total_query_path_specs"],
            1,
        )
        self.assertEqual(
            data["latest_ingestion"]["execution_summary"]["query_path_resolution"][
                "total_query_path_specs"
            ],
            1,
        )
        self.assertTrue(
            data["latest_ingestion"]["dependency_lookup_cache"]["available"]
        )
        self.assertTrue(
            data["latest_ingestion"]["dependency_parent_coverage"]["available"]
        )
        self.assertEqual(
            data["latest_ingestion"]["dependency_lookup_cache"]["row_count"],
            4,
        )
        self.assertEqual(
            data["latest_ingestion"]["dependency_parent_coverage"]["row_count"],
            8,
        )
        self.assertEqual(
            data["latest_ingestion"]["execution_summary"]["unchanged_row_count"],
            2,
        )
        self.assertEqual(
            data["latest_ingestion"]["execution_summary"]["query_path_resolution"][
                "artifact_hit_count"
            ],
            1,
        )
        self.assertEqual(
            data["latest_ingestion"]["execution_summary"]["query_path_resolution"][
                "repository_index_count"
            ],
            1,
        )
        self.assertEqual(
            data["latest_ingestion"]["query_modes"]["execution_modes"],
            {"query_path": 1},
        )
        self.assertEqual(
            data["latest_ingestion"]["query_modes"]["fetch_modes"],
            {"nqe_parameters": 1},
        )
        self.assertTrue(data["dependency_lookup_cache"]["available"])
        self.assertEqual(
            data["query_modes"]["execution_modes"],
            {"query_path": 1},
        )
        self.assertEqual(
            data["query_modes"]["fetch_modes"],
            {"nqe_parameters": 1},
        )
        self.assertTrue(data["api_usage"]["available"])
        self.assertEqual(data["api_usage"]["counters"]["http_attempts"], 11)
        self.assertEqual(data["api_usage"]["counters"]["nqe_diff_calls"], 2)
        self.assertIn("insights_summary", data)
        self.assertTrue(data["insights_summary"]["available"])
        self.assertEqual(data["insights_summary"]["http_attempts"], 11)
        self.assertEqual(data["insights_summary"]["nqe_diff_calls"], 2)
        self.assertEqual(
            data["insights_summary"]["execution_mode_counts"],
            [["query_path", 1]],
        )
        self.assertEqual(
            data["api_usage"]["counters"]["observed_http_attempts_per_minute"],
            20.0,
        )
        self.assertEqual(data["api_usage"]["budget"]["status"], "passed")
        self.assertEqual(
            data["api_usage"]["budget"]["metrics"]["headroom_requests_per_minute"],
            200,
        )
        self.assertEqual(
            data["dependency_lookup_cache"]["models"][0]["fhrp_group_count"], 1
        )
        self.assertEqual(
            data["api_usage"]["budget"]["metrics"]["throttle_sleep_seconds"],
            0.75,
        )
        self.assertIn("auto_policy_event_count", data["recovery_policy_summary"])
        self.assertIn("auto_policy_reasons", data["recovery_policy_summary"])
        self.assertIn("escalation_event_count", data["recovery_policy_summary"])
        self.assertIn("escalation_reasons", data["recovery_policy_summary"])
        self.assertIn("escalation_threshold", data["recovery_policy_summary"])
        self.assertIn("escalation_required", data["recovery_policy_summary"])
        self.assertIn("watchdog_event_count", data["recovery_policy_summary"])
        self.assertIn("watchdog_reason", data["recovery_policy_summary"])
        self.assertIn("watchdog_threshold", data["recovery_policy_summary"])
        self.assertIn("watchdog_required", data["recovery_policy_summary"])
        self.assertEqual(data["steps"][0]["id"], self.execution_step.pk)
        self.assertEqual(
            data["steps"][0]["query_parameters"],
            {"forward_netbox_shard_keys": ["device-1"]},
        )
        self.assertTrue(data["api_usage"]["step_query_parameters"]["available"])
        self.assertEqual(
            data["api_usage"]["step_query_parameters"]["step_count"],
            1,
        )
        self.assertEqual(
            data["api_usage"]["step_query_parameters"]["top_steps"][0][
                "query_parameters"
            ],
            {"forward_netbox_shard_keys": ["device-1"]},
        )
        self.assertEqual(data["steps"][0]["job_detail"]["pk"], self.job.pk)
        self.assertEqual(
            data["steps"][0]["ingestion_detail"]["id"],
            self.ingestion.pk,
        )
        explainability = data["steps"][0]["ingestion_detail"]["change_explainability"]
        self.assertTrue(explainability["available"])
        self.assertEqual(explainability["model_counts"], {"ipam.prefix": 1})
        self.assertEqual(
            explainability["top_changed_fields_by_model"]["ipam.prefix"],
            [{"field": "vrf", "count": 1}],
        )
        self.assertEqual(
            data["metrics"]["fetch_mode_counts_by_model"].get("dcim.site", {}),
            {"model": 1},
        )
        self.assertEqual(
            data["metrics"]["fetched_row_count_by_model"].get("dcim.site"),
            0,
        )
        self.assertEqual(
            data["metrics"]["pushdown_efficiency"]["status"],
            "info",
        )
        self.assertIn(
            "model_fallback_guardrails",
            data["metrics"]["pushdown_efficiency"],
        )
        self.assertIn(
            "fallback_budget_exceeded_count",
            data["metrics"]["pushdown_efficiency"],
        )
        self.assertIn(
            "fallback_budget_threshold",
            data["metrics"]["pushdown_efficiency"],
        )
        self.assertIn(
            "fallback_budget_min_steps",
            data["metrics"]["pushdown_efficiency"],
        )
        self.assertIn("diff_utilization", data["metrics"])
        self.assertIn("pushdown_runtime", data["metrics"])
        self.assertIn("fallback_reason_summary", data["metrics"])
        self.assertEqual(
            data["metrics"]["fallback_reason_summary"]["top_reasons"][0]["reason"],
            "model_fetch_contract_fallback",
        )
        self.assertEqual(
            data["metrics"]["fallback_reason_summary"]["top_reasons"][0]["remediation"][
                "code"
            ],
            "add_or_enable_shard_fetch_contract",
        )
        self.assertEqual(
            data["metrics"]["fallback_reason_summary"]["remediation_actions"][0][
                "code"
            ],
            "add_or_enable_shard_fetch_contract",
        )
        self.assertIn("fallback_pressure", data["metrics"])
        self.assertEqual(
            data["metrics"]["fallback_pressure"]["ranked_models"][0]["model"],
            "dcim.site",
        )
        self.assertIn(
            "dcim.site",
            data["metrics"]["fallback_pressure"]["no_shard_safe_filter_models"],
        )
        self.assertEqual(
            data["metrics"]["partition_retry_summary"][
                "alternate_operator_success_count"
            ],
            1,
        )
        self.assertEqual(
            data["metrics"]["partition_retry_summary"]["avoided_fallback_retry_count"],
            1,
        )
        self.assertIn("throughput_smoothing", data["metrics"])
        self.assertIn(
            "scheduler_overlap_readiness",
            data["metrics"]["throughput_smoothing"],
        )
        self.assertIn("delete_dependency_plan", data["metrics"])
        self.assertIn("operator_tuning_summary", data["metrics"])
        action_codes = {
            item["code"]
            for item in data["metrics"]["operator_tuning_summary"][
                "first_order_actions"
            ]
        }
        self.assertIn("reduce_fallback_fetch", action_codes)
        self.assertIn("stage_queue_seconds", data["metrics"]["steps"][0])
        self.assertIn("merge_wait_seconds", data["metrics"]["steps"][0])
        self.assertIn("pushdown_alert_thresholds", data["metrics"])
        self.assertIn("trend_snapshots", data["metrics"])
        self.assertIn("tuning_guidance", data["metrics"])
        self.assertGreaterEqual(len(data["metrics"]["tuning_guidance"]), 1)
        tuning_codes = {item["code"] for item in data["metrics"]["tuning_guidance"]}
        self.assertIn("partition_retry_avoided_fallback", tuning_codes)
        self.assertIn("non_diff_reason_counts", data["metrics"]["diff_utilization"])
        self.assertEqual(
            data["metrics"]["diff_baseline_transition"]["code"],
            "missing_or_ineligible_diff_baseline",
        )
        self.assertEqual(
            data["metrics"]["diff_baseline_transition"]["action_code"],
            "complete_baseline_then_use_newer_snapshot",
        )
        self.assertIn("baseline_reason_summary", data["metrics"]["trend_snapshots"][0])

    def test_execution_run_support_bundle_includes_failure_summary(self):
        failed_sync = ForwardSync.objects.create(
            name="sync-log-export-failure",
            source=self.source,
            parameters={"snapshot_id": "latestProcessed"},
        )
        failed_run = ForwardExecutionRun.objects.create(
            sync=failed_sync,
            source=self.source,
            job=self.job,
            backend="branching",
            status="failed",
            phase="failed",
            phase_message="Forward execution failed.",
            total_steps=1,
            next_step_index=1,
        )
        ForwardExecutionStep.objects.create(
            run=failed_run,
            index=1,
            status="failed",
            model_string="ipam.prefix",
            query_name="Forward Prefixes",
            execution_mode="query_id",
            execution_value="Q_154ce88d2f6b9e896aff0e3d925a682d7d4247ad",
            last_error="Forward API request failed with HTTP 400: prefix shard broke.",
        )
        self.client.force_login(self.user)

        response = self.client.get(
            reverse(
                "plugins:forward_netbox:forwardexecutionrun_export_bundle",
                kwargs={"pk": failed_run.pk},
            )
        )

        self.assertEqual(response.status_code, 200)
        data = json.loads(response.content)
        self.assertEqual(
            data["failure_summary"]["message"],
            "Shard 1 ipam.prefix Forward Prefixes failed.",
        )
        self.assertEqual(
            data["failure_summary"]["query_id"],
            "Q_154ce88d2f6b9e896aff0e3d925a682d7d4247ad",
        )
        self.assertEqual(
            data["failure_summary"]["step_pk"], failed_run.steps.first().pk
        )
        self.assertEqual(
            data["failure_summary"]["error"],
            "Forward API request failed with HTTP 400: prefix shard broke.",
        )

    def test_sync_support_bundle_downloads_json_bundle(self):
        self.client.force_login(self.user)

        response = self.client.get(
            reverse(
                "plugins:forward_netbox:forwardsync_support_bundle",
                kwargs={"pk": self.sync.pk},
            )
        )

        self.assertEqual(
            response.status_code,
            200,
            response.content.decode("utf-8", errors="replace"),
        )
        self.assertIn("attachment;", response["Content-Disposition"])
        self.assertIn(
            f"forward-sync-{self.sync.pk}-support-bundle.json",
            response["Content-Disposition"],
        )

        data = json.loads(response.content)
        self.assertEqual(data["sync"]["pk"], self.sync.pk)
        self.assertEqual(data["sync"]["execution_state_source"], "execution_ledger")
        self.assertEqual(
            data["query_drift_summary"],
            sync_health_summary(self.sync)["query_drift_summary"],
        )
        self.assertEqual(
            data["query_drift_results"],
            sync_health_summary(self.sync)["query_modes"]["local_drift"],
        )
        self.assertIn("remediation", data["query_drift_results"][0])
        self.assertEqual(data["latest_ingestion"]["pk"], self.ingestion.pk)
        self.assertEqual(data["execution_run"]["run"]["id"], self.execution_run.pk)
        self.assertEqual(data["health"]["source"]["name"], self.source.name)
        self.assertEqual(
            data["execution_run"]["steps"][0]["merge_job_detail"]["pk"],
            self.merge_job.pk,
        )
        self.assertEqual(
            data["latest_ingestion"]["change_explainability"]["top_changed_fields"],
            [{"field": "vrf", "count": 1}],
        )
        self.assertIn("optional_plugin_capabilities", data["execution_run"])
        self.assertIn("health", data["execution_run"])
        self.assertEqual(
            data["execution_run"]["health"]["source"]["name"], self.source.name
        )
        self.assertIn("live_diagnostics", data["execution_run"])
        self.assertTrue(data["execution_run"]["live_diagnostics"]["available"])
        self.assertIn(
            "aci.netbox_cisco_aci",
            data["execution_run"]["optional_plugin_capabilities"],
        )
        self.assertIn(
            "routing.netbox_routing",
            data["execution_run"]["optional_plugin_capabilities"],
        )
        self.assertIn(
            "peering.netbox_peering_manager",
            data["execution_run"]["optional_plugin_capabilities"],
        )
        self.assertEqual(
            data["execution_run"]["optional_plugin_capabilities"][
                "routing.netbox_routing"
            ]["display_name"],
            "NetBox Routing",
        )
        self.assertEqual(
            data["execution_run"]["optional_plugin_capabilities"][
                "peering.netbox_peering_manager"
            ]["display_name"],
            "NetBox Peering Manager",
        )
        routing_capabilities = data["execution_run"]["optional_plugin_capabilities"][
            "routing.netbox_routing"
        ]
        peering_capabilities = data["execution_run"]["optional_plugin_capabilities"][
            "peering.netbox_peering_manager"
        ]
        self.assertIn("availability_status", routing_capabilities)
        self.assertIn("availability_reason", routing_capabilities)
        self.assertIn("package_names", routing_capabilities)
        self.assertIn("installed_package_name", routing_capabilities)
        self.assertIn("minimum_version", routing_capabilities)
        self.assertIn("version", routing_capabilities)
        self.assertIn("unsupported_version", routing_capabilities)
        self.assertIn("availability_status", peering_capabilities)
        self.assertIn("availability_reason", peering_capabilities)
        self.assertIn("package_names", peering_capabilities)
        self.assertIn("installed_package_name", peering_capabilities)
        self.assertIn("minimum_version", peering_capabilities)
        self.assertIn("version", peering_capabilities)
        self.assertIn("unsupported_version", peering_capabilities)
        aci_capabilities = data["execution_run"]["optional_plugin_capabilities"][
            "aci.netbox_cisco_aci"
        ]
        self.assertIn("availability_status", aci_capabilities)
        self.assertIn("availability_reason", aci_capabilities)
        self.assertIn("package_names", aci_capabilities)
        self.assertIn("installed_package_name", aci_capabilities)
        self.assertIn("minimum_version", aci_capabilities)
        self.assertIn("version", aci_capabilities)
        self.assertIn("unsupported_version", aci_capabilities)

    def test_sync_support_bundle_downloads_zip_bundle(self):
        self.client.force_login(self.user)

        response = self.client.post(
            reverse(
                "plugins:forward_netbox:forwardsync_support_bundle_zip",
                kwargs={"pk": self.sync.pk},
            ),
            data={"password": ""},
        )

        self.assertEqual(response.status_code, 200)
        self.assertIn("attachment;", response["Content-Disposition"])
        self.assertIn(
            f"forward-sync-{self.sync.pk}-support-bundle.zip",
            response["Content-Disposition"],
        )

        with zipfile.ZipFile(io.BytesIO(response.content)) as archive:
            names = archive.namelist()
            self.assertEqual(
                names,
                [f"forward-sync-{self.sync.pk}-support-bundle.json"],
            )
            data = json.loads(archive.read(names[0]))

        self.assertEqual(data["sync"]["pk"], self.sync.pk)
        self.assertIn("live_diagnostics", data["execution_run"])
        self.assertTrue(data["execution_run"]["live_diagnostics"]["available"])

    def test_execution_run_support_bundle_downloads_password_protected_zip_bundle(
        self,
    ):
        self.client.force_login(self.user)
        password = "support-pass-123"

        response = self.client.post(
            reverse(
                "plugins:forward_netbox:forwardexecutionrun_export_bundle_zip",
                kwargs={"pk": self.execution_run.pk},
            ),
            data={"password": password},
        )

        self.assertEqual(
            response.status_code,
            200,
            response.content.decode("utf-8", errors="replace"),
        )
        self.assertIn("attachment;", response["Content-Disposition"])
        self.assertIn(
            f"forward-execution-run-{self.execution_run.pk}-support-bundle.zip",
            response["Content-Disposition"],
        )

        import pyzipper

        with pyzipper.AESZipFile(io.BytesIO(response.content)) as archive:
            archive.setpassword(password.encode("utf-8"))
            names = archive.namelist()
            self.assertEqual(
                names,
                [f"forward-execution-run-{self.execution_run.pk}-support-bundle.json"],
            )
            data = json.loads(archive.read(names[0]))

        self.assertEqual(data["run"]["id"], self.execution_run.pk)
        self.assertIn("health", data)
        self.assertEqual(data["health"]["source"]["name"], self.source.name)

    def test_sync_support_bundle_uses_ledger_branch_state_when_cache_is_absent(self):
        self.sync.clear_branch_run_state()
        self.client.force_login(self.user)

        response = self.client.get(
            reverse(
                "plugins:forward_netbox:forwardsync_support_bundle",
                kwargs={"pk": self.sync.pk},
            )
        )

        self.assertEqual(response.status_code, 200)
        data = json.loads(response.content)
        self.assertEqual(
            data["sync"]["execution_state"]["execution_run_id"],
            self.execution_run.pk,
        )
        self.assertEqual(data["sync"]["execution_state_source"], "execution_ledger")
        self.assertTrue(data["sync"]["execution_state"]["state_synthesized"])
        self.assertEqual(data["execution_run"]["run"]["id"], self.execution_run.pk)

    def test_sync_support_bundle_compacts_advisory_workload_preview_plan_items(self):
        standalone_sync = ForwardSync.objects.create(
            name="sync-support-bundle-compact",
            source=self.source,
            parameters={"snapshot_id": "latestProcessed"},
        )
        standalone_sync.set_branch_run_state(
            {
                "snapshot_id": "snapshot-state",
                "phase": "executing",
                "plan_items": [
                    {"index": index, "status": "queued"} for index in range(150)
                ],
            }
        )
        self.client.force_login(self.user)

        response = self.client.get(
            reverse(
                "plugins:forward_netbox:forwardsync_support_bundle",
                kwargs={"pk": standalone_sync.pk},
            )
        )

        self.assertEqual(response.status_code, 200)
        data = json.loads(response.content)
        advisory = data["sync"]["advisory_summary"]
        self.assertNotIn("plan_items", advisory["branch_run"])
        self.assertEqual(advisory["branch_run"]["plan_items_count"], 150)

    def test_sync_support_bundle_survives_cleanup_and_later_run(self):
        self.sync.clear_branch_run_state()
        later_run = ForwardExecutionRun.objects.create(
            sync=self.sync,
            source=self.source,
            backend="branching",
            status="running",
            snapshot_selector="latestProcessed",
            snapshot_id="snapshot-2",
            total_steps=1,
            next_step_index=1,
        )
        self.sync.set_branch_run_state(
            {
                "execution_run_id": later_run.pk,
                "next_plan_index": 1,
                "total_plan_items": 1,
            }
        )
        self.client.force_login(self.user)

        response = self.client.get(
            reverse(
                "plugins:forward_netbox:forwardsync_support_bundle",
                kwargs={"pk": self.sync.pk},
            )
        )

        self.assertEqual(response.status_code, 200)
        data = json.loads(response.content)
        self.assertEqual(data["sync"]["execution_state_source"], "execution_ledger")
        self.assertEqual(
            data["sync"]["execution_state"]["execution_run_id"],
            later_run.pk,
        )
        self.assertEqual(data["execution_run"]["run"]["id"], later_run.pk)
        self.assertTrue(data["execution_run"]["compatibility_cache"]["ledger_history"])
        self.assertFalse(
            data["execution_run"]["compatibility_cache"]["stale_payload_present"]
        )

    def test_sync_support_bundle_survives_old_branch_upgrade_cleanup_and_later_run(
        self,
    ):
        branch = Branch.objects.create(
            name="legacy-upgrade-branch",
            schema_id=f"legacy_upgrade_{self.ingestion.pk}",
        )
        self.sync.set_branch_run_state(
            {
                "snapshot_selector": "latestProcessed",
                "snapshot_id": "snapshot-legacy",
                "phase": "executing",
                "phase_message": "Applying planned shard changes.",
                "next_plan_index": 1,
                "total_plan_items": 1,
                "plan_items": [
                    {
                        "index": 1,
                        "model": "dcim.site",
                        "label": "dcim.site legacy shard",
                        "estimated_changes": 1,
                        "status": "merged",
                        "sync_mode": "diff",
                        "query_name": "Forward Sites",
                        "execution_mode": "query_id",
                        "execution_value": "query-site",
                        "baseline_snapshot_id": "snapshot-before",
                        "apply_engine": "adapter",
                    }
                ],
            }
        )
        upgraded_run = active_execution_run(self.sync)
        ingestion = ForwardIngestion.objects.create(
            sync=self.sync,
            snapshot_selector="latestProcessed",
            snapshot_id="snapshot-legacy",
            branch=branch,
        )
        step = upgraded_run.steps.first()
        step.ingestion = ingestion
        step.branch_name = branch.name
        step.save(update_fields=["ingestion", "branch_name"])
        self.sync.clear_branch_run_state()
        branch.delete()
        later_run = ForwardExecutionRun.objects.create(
            sync=self.sync,
            source=self.source,
            backend="branching",
            status="running",
            snapshot_selector="latestProcessed",
            snapshot_id="snapshot-later",
            total_steps=1,
            next_step_index=1,
        )
        self.sync.set_branch_run_state(
            {
                "execution_run_id": later_run.pk,
                "next_plan_index": 1,
                "total_plan_items": 1,
            }
        )
        self.client.force_login(self.user)

        response = self.client.get(
            reverse(
                "plugins:forward_netbox:forwardsync_support_bundle",
                kwargs={"pk": self.sync.pk},
            )
        )

        self.assertEqual(response.status_code, 200)
        data = json.loads(response.content)
        self.assertEqual(data["sync"]["execution_state_source"], "execution_ledger")
        self.assertEqual(
            data["sync"]["execution_state"]["execution_run_id"],
            later_run.pk,
        )
        self.assertEqual(data["execution_run"]["run"]["id"], later_run.pk)

    @patch("forward_netbox.views.get_execution_display_state")
    def test_export_logs_does_not_invent_compatibility_source(
        self, mock_get_execution_display_state
    ):
        mock_get_execution_display_state.return_value = {
            "execution_run_id": self.execution_run.pk,
        }
        self.client.force_login(self.user)

        response = self.client.get(
            reverse(
                "plugins:forward_netbox:forwardingestion_export_logs",
                kwargs={"pk": self.ingestion.pk},
            )
        )

        self.assertEqual(response.status_code, 200)
        data = json.loads(response.content)
        self.assertIsNone(data["sync"]["execution_state_source"])

    def test_execution_run_support_bundle_reports_stale_compatibility_payload(self):
        self.execution_run.status = "completed"
        self.execution_run.completed = timezone.now()
        self.execution_run.save(update_fields=["status", "completed"])

        parameters = dict(self.sync.parameters or {})
        parameters[BRANCH_RUN_STATE_PARAMETER] = {
            "phase": "planning",
            "execution_run_id": self.execution_run.pk,
        }
        self.sync.parameters = parameters
        self.sync.save(update_fields=["parameters"])

        self.client.force_login(self.user)
        response = self.client.get(
            reverse(
                "plugins:forward_netbox:forwardexecutionrun_export_bundle",
                kwargs={"pk": self.execution_run.pk},
            )
        )

        self.assertEqual(response.status_code, 200)
        data = json.loads(response.content)
        self.assertTrue(data["compatibility_cache"]["compatibility_state_present"])
        self.assertTrue(data["compatibility_cache"]["stale_payload_present"])
        self.assertTrue(data["compatibility_cache"]["prune_recommended"])
