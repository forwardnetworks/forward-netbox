import json
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


class ForwardIngestionLogExportViewTest(TestCase):
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
                "statistics": {"dcim.site": {"current": 1, "total": 1}},
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

        self.assertEqual(response.status_code, 200)
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

        self.assertEqual(response.status_code, 200)
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

        self.assertEqual(response.status_code, 200)
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

        self.assertEqual(response.status_code, 200)
        data = json.loads(response.content)
        self.assertEqual(
            data["sync"]["execution_state"]["execution_run_id"],
            self.execution_run.pk,
        )
        self.assertEqual(data["sync"]["execution_state_source"], "execution_ledger")
        self.assertTrue(data["sync"]["execution_state"]["state_synthesized"])
        self.assertEqual(data["execution_plan"]["total_plan_items"], 1)
        self.assertEqual(data["execution_run"]["run"]["id"], self.execution_run.pk)

    def test_execution_run_support_bundle_downloads_json_bundle(self):
        self.client.force_login(self.user)

        response = self.client.get(
            reverse(
                "plugins:forward_netbox:forwardexecutionrun_export_bundle",
                kwargs={"pk": self.execution_run.pk},
            )
        )

        self.assertEqual(response.status_code, 200)
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
        self.assertTrue(data["api_usage"]["available"])
        self.assertEqual(data["api_usage"]["counters"]["http_attempts"], 11)
        self.assertEqual(data["api_usage"]["counters"]["nqe_diff_calls"], 2)
        self.assertEqual(
            data["api_usage"]["counters"]["observed_http_attempts_per_minute"],
            20.0,
        )
        self.assertEqual(data["api_usage"]["budget"]["status"], "passed")
        self.assertEqual(
            data["api_usage"]["budget"]["metrics"]["headroom_requests_per_minute"],
            200,
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

    def test_sync_support_bundle_downloads_json_bundle(self):
        self.client.force_login(self.user)

        response = self.client.get(
            reverse(
                "plugins:forward_netbox:forwardsync_support_bundle",
                kwargs={"pk": self.sync.pk},
            )
        )

        self.assertEqual(response.status_code, 200)
        self.assertIn("attachment;", response["Content-Disposition"])
        self.assertIn(
            f"forward-sync-{self.sync.pk}-support-bundle.json",
            response["Content-Disposition"],
        )

        data = json.loads(response.content)
        self.assertEqual(data["sync"]["pk"], self.sync.pk)
        self.assertEqual(data["sync"]["execution_state_source"], "execution_ledger")
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
