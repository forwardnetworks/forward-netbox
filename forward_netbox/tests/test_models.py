from datetime import timedelta
from unittest.mock import Mock
from unittest.mock import patch
from uuid import uuid4

from core.exceptions import SyncError
from django.conf import settings
from django.contrib.auth import get_user_model
from django.contrib.contenttypes.models import ContentType
from django.core.exceptions import ValidationError
from django.test import override_settings
from django.test import TestCase
from django.utils import timezone
from netbox_branching.models import Branch

from forward_netbox.choices import FORWARD_BGP_MODELS
from forward_netbox.choices import forward_configured_models
from forward_netbox.choices import ForwardDiffFallbackModeChoices
from forward_netbox.choices import ForwardDriftPolicyBaselineChoices
from forward_netbox.choices import ForwardSourceStatusChoices
from forward_netbox.choices import ForwardSyncStatusChoices
from forward_netbox.choices import ForwardValidationStatusChoices
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
from forward_netbox.utilities.branch_budget import BRANCH_RUN_STATE_PARAMETER
from forward_netbox.utilities.branch_budget import build_branch_budget_hints
from forward_netbox.utilities.branch_budget import DEFAULT_MAX_CHANGES_PER_BRANCH
from forward_netbox.utilities.execution_telemetry import build_branch_run_summary
from forward_netbox.utilities.execution_telemetry import (
    build_ingestion_execution_summary,
)
from forward_netbox.utilities.execution_telemetry import build_plan_preview
from forward_netbox.utilities.execution_telemetry import build_sync_execution_summary
from forward_netbox.utilities.forward_api import LATEST_PROCESSED_SNAPSHOT
from forward_netbox.utilities.forward_api import MAX_NQE_ASYNC_MAX_POLLS
from forward_netbox.utilities.forward_api import MAX_NQE_ASYNC_POLL_INTERVAL_SECONDS
from forward_netbox.utilities.query_registry import builtin_nqe_map_rows
from forward_netbox.utilities.query_registry import QuerySpec
from forward_netbox.utilities.validation import force_allow_validation_run
from forward_netbox.utilities.validation import ForwardValidationRunner
from forward_netbox.views import annotate_statistics


BGP_PLUGIN_CONFIG = {
    **settings.PLUGINS_CONFIG,
    "forward_netbox": {
        **settings.PLUGINS_CONFIG.get("forward_netbox", {}),
        "enable_bgp_sync": True,
    },
}
BGP_DISABLED_PLUGIN_CONFIG = {
    **settings.PLUGINS_CONFIG,
    "forward_netbox": {
        key: value
        for key, value in settings.PLUGINS_CONFIG.get("forward_netbox", {}).items()
        if key != "enable_bgp_sync"
    },
}
BGP_DISABLED_PLUGIN_CONFIG["forward_netbox"]["enable_bgp_sync"] = False


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
                "nqe_page_size": 10000,
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

    def test_source_rejects_invalid_nqe_fetch_all_max_pages(self):
        source = ForwardSource(
            name="source-invalid-fetch-page-cap",
            type="saas",
            url="https://fwd.app",
            parameters={
                "username": "user@example.com",
                "password": "secret",
                "verify": True,
                "timeout": 1200,
                "network_id": "test-network",
                "nqe_fetch_all_max_pages": 200001,
            },
        )

        with self.assertRaises(ValidationError) as ctx:
            source.clean()

        self.assertIn(
            "`nqe_fetch_all_max_pages` must be between 1 and 200000.",
            str(ctx.exception),
        )

    def test_source_rejects_invalid_identical_full_page_streak_limit(self):
        source = ForwardSource(
            name="source-invalid-identical-streak-cap",
            type="saas",
            url="https://fwd.app",
            parameters={
                "username": "user@example.com",
                "password": "secret",
                "verify": True,
                "timeout": 1200,
                "network_id": "test-network",
                "nqe_identical_full_page_streak_limit": 0,
            },
        )

        with self.assertRaises(ValidationError) as ctx:
            source.clean()

        self.assertIn(
            "`nqe_identical_full_page_streak_limit` must be between 1 and 1000.",
            str(ctx.exception),
        )

    def test_source_accepts_sync_device_tags_list(self):
        source = ForwardSource(
            name="source-sync-device-tags",
            type="saas",
            url="https://fwd.app",
            parameters={
                "username": "user@example.com",
                "password": "secret",
                "verify": True,
                "timeout": 1200,
                "network_id": "test-network",
                "sync_device_tags": ["Mgmt_Vl211", "Prod_Core"],
            },
        )

        source.clean()

        self.assertEqual(
            source.parameters["sync_device_tags"], ["Mgmt_Vl211", "Prod_Core"]
        )

    def test_source_rejects_non_list_sync_device_tags(self):
        source = ForwardSource(
            name="source-bad-sync-device-tags",
            type="saas",
            url="https://fwd.app",
            parameters={
                "username": "user@example.com",
                "password": "secret",
                "verify": True,
                "timeout": 1200,
                "network_id": "test-network",
                "sync_device_tags": "Mgmt_Vl211",
            },
        )

        with self.assertRaises(ValidationError) as ctx:
            source.clean()

        self.assertIn(
            "`sync_device_tags` must be a list of strings.", str(ctx.exception)
        )

    def test_source_preserves_api_requests_per_minute(self):
        source = ForwardSource(
            name="source-api-rpm",
            type="saas",
            url="https://fwd.app",
            parameters={
                "username": "user@example.com",
                "password": "secret",
                "verify": True,
                "timeout": 1200,
                "network_id": "test-network",
                "api_requests_per_minute": "1800",
            },
        )

        source.clean()

        self.assertEqual(source.parameters["api_requests_per_minute"], 1800)

    def test_source_defaults_saas_api_requests_per_minute(self):
        source = ForwardSource(
            name="source-api-rpm-default",
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

        source.clean()

        self.assertEqual(source.parameters["api_requests_per_minute"], 1800)

    def test_source_preserves_nqe_async_parameters(self):
        source = ForwardSource(
            name="source-nqe-async",
            type="saas",
            url="https://fwd.app",
            parameters={
                "username": "user@example.com",
                "password": "secret",
                "verify": True,
                "timeout": 1200,
                "network_id": "test-network",
                "nqe_async_poll_interval_seconds": "0.5",
                "nqe_async_max_polls": "600",
            },
        )

        source.clean()

        self.assertEqual(source.parameters["nqe_async_poll_interval_seconds"], 0.5)
        self.assertEqual(source.parameters["nqe_async_max_polls"], 600)

    def test_source_rejects_invalid_api_requests_per_minute(self):
        source = ForwardSource(
            name="source-invalid-api-rpm",
            type="saas",
            url="https://fwd.app",
            parameters={
                "username": "user@example.com",
                "password": "secret",
                "verify": True,
                "timeout": 1200,
                "network_id": "test-network",
                "api_requests_per_minute": -1,
            },
        )

        with self.assertRaises(ValidationError) as ctx:
            source.clean()

        self.assertIn(
            "`api_requests_per_minute` must be between 0 and 60000.",
            str(ctx.exception),
        )

    def test_source_rejects_invalid_nqe_async_poll_interval_seconds(self):
        source = ForwardSource(
            name="source-invalid-nqe-async-interval",
            type="saas",
            url="https://fwd.app",
            parameters={
                "username": "user@example.com",
                "password": "secret",
                "verify": True,
                "timeout": 1200,
                "network_id": "test-network",
                "nqe_async_poll_interval_seconds": MAX_NQE_ASYNC_POLL_INTERVAL_SECONDS
                + 1.0,
            },
        )

        with self.assertRaises(ValidationError) as ctx:
            source.clean()

        self.assertIn(
            "`nqe_async_poll_interval_seconds` must be between 0 and",
            str(ctx.exception),
        )

    def test_source_rejects_invalid_nqe_async_max_polls(self):
        source = ForwardSource(
            name="source-invalid-nqe-async-max-polls",
            type="saas",
            url="https://fwd.app",
            parameters={
                "username": "user@example.com",
                "password": "secret",
                "verify": True,
                "timeout": 1200,
                "network_id": "test-network",
                "nqe_async_max_polls": MAX_NQE_ASYNC_MAX_POLLS + 1,
            },
        )

        with self.assertRaises(ValidationError) as ctx:
            source.clean()

        self.assertIn(
            "`nqe_async_max_polls` must be between 1 and",
            str(ctx.exception),
        )

    def test_source_rejects_invalid_pushdown_alert_threshold(self):
        source = ForwardSource(
            name="source-invalid-pushdown-threshold",
            type="saas",
            url="https://fwd.app",
            parameters={
                "username": "user@example.com",
                "password": "secret",
                "verify": True,
                "timeout": 1200,
                "network_id": "test-network",
                "pushdown_fallback_warn_rate": 1.2,
            },
        )

        with self.assertRaises(ValidationError) as ctx:
            source.clean()

        self.assertIn(
            "`pushdown_fallback_warn_rate` must be between 0 and 1", str(ctx.exception)
        )

    def test_source_rejects_non_boolean_query_preflight_enabled(self):
        source = ForwardSource(
            name="source-invalid-preflight-toggle",
            type="saas",
            url="https://fwd.app",
            parameters={
                "username": "user@example.com",
                "password": "secret",
                "verify": True,
                "timeout": 1200,
                "network_id": "test-network",
                "query_preflight_enabled": "yes",
            },
        )

        with self.assertRaises(ValidationError) as ctx:
            source.clean()

        self.assertIn("`query_preflight_enabled` must be a boolean", str(ctx.exception))

    def test_source_rejects_invalid_query_preflight_row_limit(self):
        source = ForwardSource(
            name="source-invalid-preflight-row-limit",
            type="saas",
            url="https://fwd.app",
            parameters={
                "username": "user@example.com",
                "password": "secret",
                "verify": True,
                "timeout": 1200,
                "network_id": "test-network",
                "query_preflight_row_limit": 0,
            },
        )

        with self.assertRaises(ValidationError) as ctx:
            source.clean()

        self.assertIn(
            "`query_preflight_row_limit` must be between 1 and 100.",
            str(ctx.exception),
        )

    def test_source_rejects_non_boolean_query_diagnostics_enabled(self):
        source = ForwardSource(
            name="source-invalid-diagnostic-toggle",
            type="saas",
            url="https://fwd.app",
            parameters={
                "username": "user@example.com",
                "password": "secret",
                "verify": True,
                "timeout": 1200,
                "network_id": "test-network",
                "query_diagnostics_enabled": "yes",
            },
        )

        with self.assertRaises(ValidationError) as ctx:
            source.clean()

        self.assertIn(
            "`query_diagnostics_enabled` must be a boolean",
            str(ctx.exception),
        )

    @patch("forward_netbox.models.ForwardSource.get_client")
    def test_source_tag_scope_preview_reports_counts(self, mock_get_client):
        self.source.parameters.update(
            {
                "device_tag_include_tags": ["scope-alpha"],
                "device_tag_exclude_tags": ["Branch"],
                "device_tag_include_match": "any",
            }
        )
        self.source.save(update_fields=["parameters"])

        client = Mock()
        client.get_latest_processed_snapshot.return_value = {"id": "snap-1"}
        client.run_nqe_query.side_effect = [
            [
                {"name": "dev-a"},
                {"name": "dev-b"},
                {"name": "dev-c"},
            ],
            [
                {"name": "dev-a"},
                {"name": "dev-c"},
            ],
        ]
        mock_get_client.return_value = client

        preview = self.source.get_tag_scope_preview()
        self.assertTrue(preview["enabled"])
        self.assertEqual(preview["total_devices"], 3)
        self.assertEqual(preview["matched_devices"], 2)
        self.assertEqual(preview["excluded_devices"], 1)
        self.assertEqual(preview["error"], "")

    @patch("forward_netbox.models.ForwardSource.get_client")
    def test_source_tag_scope_preview_returns_error_when_snapshot_missing(
        self, mock_get_client
    ):
        self.source.parameters.update({"device_tag_include_tags": ["scope-alpha"]})
        self.source.save(update_fields=["parameters"])

        client = Mock()
        client.get_latest_processed_snapshot.return_value = {"id": ""}
        mock_get_client.return_value = client

        preview = self.source.get_tag_scope_preview()
        self.assertTrue(preview["enabled"])
        self.assertIn("No processed snapshot", preview["error"])

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

    def test_sync_accepts_legacy_branch_run_compatibility_state(self):
        sync = ForwardSync(
            name="sync-compat-branch",
            source=self.source,
            parameters={
                "snapshot_id": LATEST_PROCESSED_SNAPSHOT,
                "dcim.device": True,
                BRANCH_RUN_STATE_PARAMETER: {
                    "phase": "planning",
                    "next_plan_index": 2,
                    "total_plan_items": 4,
                },
            },
        )

        sync.clean()

        self.assertIn(BRANCH_RUN_STATE_PARAMETER, sync.parameters)
        self.assertEqual(
            sync.parameters[BRANCH_RUN_STATE_PARAMETER]["phase"], "planning"
        )

    def test_sync_rejects_invalid_diff_fallback_mode(self):
        sync = ForwardSync(
            name="sync-invalid-diff-fallback",
            source=self.source,
            parameters={
                "snapshot_id": LATEST_PROCESSED_SNAPSHOT,
                "dcim.device": True,
                "diff_fallback_mode": "invalid-mode",
            },
        )

        with self.assertRaises(ValidationError) as ctx:
            sync.clean()

        self.assertIn("`diff_fallback_mode` is not supported", str(ctx.exception))

    def test_sync_accepts_required_diff_fallback_mode(self):
        sync = ForwardSync(
            name="sync-required-diff-fallback",
            source=self.source,
            parameters={
                "snapshot_id": LATEST_PROCESSED_SNAPSHOT,
                "dcim.device": True,
                "diff_fallback_mode": ForwardDiffFallbackModeChoices.REQUIRE_DIFF,
            },
        )

        sync.clean()
        self.assertEqual(
            sync.parameters["diff_fallback_mode"],
            ForwardDiffFallbackModeChoices.REQUIRE_DIFF,
        )

    def test_sync_rejects_require_diff_with_prune_out_of_scope(self):
        # require_diff + prune-out-of-scope is incompatible: prune needs a full
        # query (the complete in-scope set), which require_diff forbids — every
        # model would fail the diff fetch. Reject at config time with the remedy
        # instead of failing the run with a cryptic device-coverage block.
        source = ForwardSource.objects.create(
            name="src-prune-require-diff",
            type="saas",
            url="https://fwd.app",
            status="ready",
            parameters={
                "username": "u@example.com",
                "password": "p",
                "verify": True,
                "network_id": "n",
                "device_tag_prune_out_of_scope": True,
            },
        )
        sync = ForwardSync(
            name="sync-prune-require-diff",
            source=source,
            parameters={
                "snapshot_id": LATEST_PROCESSED_SNAPSHOT,
                "dcim.device": True,
                "diff_fallback_mode": ForwardDiffFallbackModeChoices.REQUIRE_DIFF,
            },
        )

        with self.assertRaises(ValidationError) as ctx:
            sync.clean()
        self.assertIn("incompatible with prune-out-of-scope", str(ctx.exception))

    def test_new_sync_validation_defaults_safe_bulk_orm_enabled(self):
        sync = ForwardSync(
            name="sync-new-bulk-orm-default",
            source=self.source,
            parameters={
                "snapshot_id": LATEST_PROCESSED_SNAPSHOT,
                "dcim.device": True,
            },
        )

        sync.clean()

        self.assertTrue(sync.parameters["enable_bulk_orm"])

    def test_sync_rejects_past_scheduled_time(self):
        sync = ForwardSync(
            name="sync-past-scheduled",
            source=self.source,
            scheduled=timezone.now() - timedelta(minutes=5),
            parameters={
                "snapshot_id": LATEST_PROCESSED_SNAPSHOT,
                "dcim.device": True,
            },
        )

        with self.assertRaises(ValidationError) as ctx:
            sync.clean()

        self.assertIn("Scheduled time must be in the future.", str(ctx.exception))

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

    def test_display_parameters_include_branch_phase_details(self):
        sync = ForwardSync.objects.create(
            name="sync-display-phase",
            source=self.source,
            parameters={
                "snapshot_id": LATEST_PROCESSED_SNAPSHOT,
                "dcim.device": True,
            },
        )
        sync.set_branch_run_state(
            {
                "snapshot_id": "snapshot-1",
                "next_plan_index": 1,
                "total_plan_items": 3,
                "awaiting_merge": False,
                "phase": "planning",
                "phase_message": "Building shard plan.",
            }
        )

        params = sync.get_display_parameters()

        self.assertIn("branch_run", params)
        self.assertEqual(params["branch_run"]["phase"], "planning")
        self.assertEqual(params["branch_run"]["phase_message"], "Building shard plan.")

    def test_workload_summary_includes_branch_preview_details(self):
        sync = ForwardSync.objects.create(
            name="sync-workload",
            source=self.source,
            parameters={
                "snapshot_id": LATEST_PROCESSED_SNAPSHOT,
                "dcim.device": True,
            },
        )
        sync.set_branch_run_state(
            {
                "snapshot_id": "snapshot-1",
                "next_plan_index": 1,
                "total_plan_items": 3,
                "awaiting_merge": False,
                "phase": "planning",
                "phase_message": "Planning shard layout.",
                "plan_preview": {
                    "planned_shards": 3,
                    "estimated_changes": 15,
                    "model_count": 2,
                    "retry_risk": "medium",
                },
            }
        )

        summary = sync.get_workload_summary()

        self.assertTrue(summary["uses_multi_branch"])
        self.assertFalse(summary["baseline_ready"])
        self.assertEqual(summary["branch_run"]["phase"], "planning")
        self.assertEqual(
            summary["pre_run_estimate"]["planned_shards"],
            3,
        )
        self.assertEqual(summary["branch_budget_hints"]["dcim.device"], 10000)
        self.assertIn("dcim.device", summary["enabled_models"])

    def test_workload_summary_recommends_branching_for_bounded_projection(self):
        sync = ForwardSync.objects.create(
            name="sync-bounded-branching-guidance",
            source=self.source,
            parameters={
                "snapshot_id": LATEST_PROCESSED_SNAPSHOT,
                "dcim.site": True,
                "max_changes_per_branch": 10000,
            },
        )
        sync.set_branch_run_state(
            {
                "snapshot_id": "snapshot-2",
                "plan_preview": {
                    "planned_shards": 2,
                    "estimated_changes": 5000,
                    "model_count": 1,
                    "retry_risk": "low",
                },
            }
        )

        lane = sync.get_workload_summary()["initial_baseline_lane"]

        self.assertEqual(lane["recommendation"], "branching_bounded_review")
        self.assertEqual(lane["recommended_backend"], "branching")
        self.assertEqual(lane["status"], "pass")
        self.assertEqual(lane["lane_risk"], "low")

    def test_workload_summary_recommends_branching_with_tuning_after_baseline(self):
        sync = ForwardSync.objects.create(
            name="sync-large-diff-guidance",
            source=self.source,
            parameters={
                "snapshot_id": LATEST_PROCESSED_SNAPSHOT,
                "dcim.device": True,
                "max_changes_per_branch": 10000,
            },
        )
        ForwardIngestion.objects.create(
            sync=sync,
            snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
            snapshot_id="snapshot-baseline",
            baseline_ready=True,
        )
        sync.set_branch_run_state(
            {
                "snapshot_id": "snapshot-3",
                "plan_preview": {
                    "planned_shards": 25,
                    "estimated_changes": 250000,
                    "model_count": 1,
                    "retry_risk": "medium",
                    "delete_dependency_plan": {
                        "models": {
                            "dcim.device": {
                                "delete_rows": 200,
                                "delete_shards": 2,
                                "reference_blocker_risk": "high",
                            }
                        }
                    },
                },
            }
        )

        lane = sync.get_workload_summary()["initial_baseline_lane"]

        self.assertEqual(lane["recommendation"], "branching_with_tuning")
        self.assertEqual(lane["recommended_backend"], "branching")
        self.assertFalse(lane["first_baseline"])
        self.assertEqual(lane["lane_risk"], "medium")
        self.assertEqual(
            lane["estimate"]["delete_heavy_models"][0]["model"], "dcim.device"
        )

    def test_sync_detail_renders_initial_baseline_lane_advisory(self):
        user = get_user_model().objects.create_superuser(
            username="sync-detail-admin",
            password="TestPassword123!",
            email="sync-detail-admin@example.com",
        )
        sync = ForwardSync.objects.create(
            name="sync-detail-lane-guidance",
            source=self.source,
            parameters={
                "snapshot_id": LATEST_PROCESSED_SNAPSHOT,
                "dcim.device": True,
                "max_changes_per_branch": 10000,
            },
        )
        sync.set_branch_run_state(
            {
                "snapshot_id": "snapshot-detail",
                "plan_preview": {
                    "planned_shards": 12,
                    "estimated_changes": 120000,
                },
            }
        )
        self.client.force_login(user)

        response = self.client.get(sync.get_absolute_url())

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Use Fast bootstrap for trusted baseline")
        self.assertContains(response, "Use only for a trusted initial baseline.")

    def test_display_parameters_include_branch_budget_hints(self):
        sync = ForwardSync.objects.create(
            name="sync-display-budget",
            source=self.source,
            parameters={
                "snapshot_id": LATEST_PROCESSED_SNAPSHOT,
                "dcim.cable": True,
            },
        )

        params = sync.get_display_parameters()

        self.assertIn("branch_budget_hints", params)
        self.assertEqual(params["branch_budget_hints"]["dcim.cable"], 1666)
        self.assertNotIn("model_change_density", params)

    def test_display_parameters_include_model_change_density_when_present(self):
        sync = ForwardSync.objects.create(
            name="sync-display-density",
            source=self.source,
            parameters={
                "snapshot_id": LATEST_PROCESSED_SNAPSHOT,
                "dcim.cable": True,
            },
        )
        sync.set_model_change_density({"dcim.cable": 2.0})

        params = sync.get_display_parameters()

        self.assertEqual(params["model_change_density"]["dcim.cable"], 2.0)
        self.assertEqual(params["branch_budget_hints"]["dcim.cable"], 2500)

    def test_optional_module_model_is_disabled_by_default(self):
        sync = ForwardSync.objects.create(
            name="sync-optional-module-default",
            source=self.source,
            parameters={
                "snapshot_id": LATEST_PROCESSED_SNAPSHOT,
            },
        )

        self.assertFalse(sync.is_model_enabled("dcim.module"))
        self.assertNotIn("dcim.module", sync.enabled_models())

    def test_sync_rejects_when_no_models_are_enabled(self):
        sync = ForwardSync(
            name="sync-no-enabled-models",
            source=self.source,
            parameters={
                "snapshot_id": LATEST_PROCESSED_SNAPSHOT,
                **{model_string: False for model_string in forward_configured_models()},
            },
        )

        with self.assertRaises(ValidationError) as ctx:
            sync.clean()

        self.assertIn("Select at least one NetBox model to sync.", str(ctx.exception))

    def test_sync_requires_dcim_device_for_child_models(self):
        sync = ForwardSync(
            name="sync-child-model-without-device",
            source=self.source,
            parameters={
                "snapshot_id": LATEST_PROCESSED_SNAPSHOT,
                "dcim.device": False,
                "dcim.interface": True,
            },
        )

        with self.assertRaises(ValidationError) as ctx:
            sync.clean()

        self.assertIn("dcim.device", str(ctx.exception))
        self.assertIn("dcim.interface", str(ctx.exception))

    @override_settings(PLUGINS_CONFIG=BGP_DISABLED_PLUGIN_CONFIG)
    def test_bgp_models_are_disabled_without_feature_flag_even_when_parameter_is_true(
        self,
    ):
        sync = ForwardSync.objects.create(
            name="sync-bgp-flag-disabled",
            source=self.source,
            parameters={
                "snapshot_id": LATEST_PROCESSED_SNAPSHOT,
                "dcim.device": True,
                **{model_string: True for model_string in FORWARD_BGP_MODELS},
            },
        )

        for model_string in FORWARD_BGP_MODELS:
            self.assertFalse(sync.is_model_enabled(model_string))
            self.assertNotIn(model_string, sync.enabled_models())

    @override_settings(PLUGINS_CONFIG=BGP_PLUGIN_CONFIG)
    def test_bgp_models_follow_parameters_when_feature_flag_is_enabled(self):
        sync = ForwardSync.objects.create(
            name="sync-bgp-flag-enabled",
            source=self.source,
            parameters={
                "snapshot_id": LATEST_PROCESSED_SNAPSHOT,
                "dcim.device": True,
                **{model_string: True for model_string in FORWARD_BGP_MODELS},
                "netbox_peering_manager.peeringsession": False,
            },
        )

        for model_string in FORWARD_BGP_MODELS:
            if model_string == "netbox_peering_manager.peeringsession":
                continue
            self.assertTrue(sync.is_model_enabled(model_string))
            self.assertIn(model_string, sync.enabled_models())
        self.assertFalse(sync.is_model_enabled("netbox_peering_manager.peeringsession"))
        self.assertNotIn("netbox_peering_manager.peeringsession", sync.enabled_models())

    def test_get_sync_activity_prefers_phase_message(self):
        sync = ForwardSync.objects.create(
            name="sync-activity-phase-msg",
            source=self.source,
            parameters={
                "snapshot_id": LATEST_PROCESSED_SNAPSHOT,
                "dcim.device": True,
            },
        )
        sync.set_branch_run_state(
            {
                "phase": "planning",
                "phase_message": "Resolving snapshot context.",
            }
        )

        self.assertEqual(sync.get_sync_activity(), "Resolving snapshot context.")

    def test_get_sync_activity_appends_elapsed_phase_time(self):
        sync = ForwardSync.objects.create(
            name="sync-activity-phase-elapsed",
            source=self.source,
            parameters={
                "snapshot_id": LATEST_PROCESSED_SNAPSHOT,
                "dcim.device": True,
            },
        )
        started = (timezone.now() - timedelta(minutes=2, seconds=5)).isoformat()
        sync.set_branch_run_state(
            {
                "phase": "planning",
                "phase_message": "Resolving snapshot context.",
                "phase_started": started,
            }
        )

        activity = sync.get_sync_activity()
        self.assertIn("Resolving snapshot context.", activity)
        self.assertRegex(activity, r"\(\d+m \d+s\)$")

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
        sync.full_clean()

    def test_shared_telemetry_helpers_build_consistent_shapes(self):
        plan_preview = build_plan_preview([], max_changes_per_branch=10000)
        self.assertEqual(plan_preview["planned_shards"], 0)
        self.assertEqual(plan_preview["models"], {})

        branch_run = build_branch_run_summary(
            {
                "snapshot_id": "snapshot-2",
                "next_plan_index": 4,
                "total_plan_items": 9,
                "awaiting_merge": True,
                "phase": "executing",
                "phase_message": "Applying planned shard changes.",
            }
        )
        self.assertEqual(branch_run["snapshot_id"], "snapshot-2")
        self.assertTrue(branch_run["awaiting_merge"])
        self.assertEqual(branch_run["phase"], "executing")

        hints = build_branch_budget_hints(
            ["dcim.cable", "dcim.device"],
            max_changes_per_branch=10000,
            model_change_density={"dcim.cable": 2.0},
        )
        self.assertEqual(hints["dcim.cable"], 2500)
        self.assertEqual(hints["dcim.device"], 10000)

        ingestion_summary = build_ingestion_execution_summary(
            model_results=[],
            job_logs=[],
            applied_change_count=0,
            failed_change_count=0,
            created_change_count=0,
            updated_change_count=0,
            deleted_change_count=0,
        )
        self.assertEqual(ingestion_summary["model_count"], 0)
        self.assertEqual(ingestion_summary["retry_count"], 0)
        self.assertNotIn("model_results", ingestion_summary)
        self.assertEqual(ingestion_summary["query_modes"]["execution_modes"], {})
        self.assertEqual(ingestion_summary["query_modes"]["fetch_modes"], {})

        sync_summary = build_sync_execution_summary(
            enabled_models=["dcim.cable"],
            max_changes_per_branch=10000,
            model_change_density={"dcim.cable": 2.0},
            model_change_density_profile={},
            branch_run_state={"plan_preview": plan_preview},
            latest_ingestion_summary=ingestion_summary,
        )
        self.assertEqual(sync_summary["branch_budget_hints"]["dcim.cable"], 2500)
        self.assertEqual(sync_summary["pre_run_estimate"]["planned_shards"], 0)
        self.assertEqual(sync_summary["latest_ingestion"]["retry_count"], 0)
        self.assertIn("model_change_density_profile", sync_summary)

    def test_execution_summary_includes_latest_ingestion_telemetry(self):
        sync = ForwardSync.objects.create(
            name="sync-execution-summary",
            source=self.source,
            parameters={
                "snapshot_id": LATEST_PROCESSED_SNAPSHOT,
                "dcim.cable": True,
            },
        )
        ingestion = ForwardIngestion.objects.create(
            sync=sync,
            model_results=[
                {
                    "model": "dcim.cable",
                    "query_name": "Forward Cabling",
                    "runtime_ms": 12.5,
                    "row_count": 2,
                    "delete_count": 1,
                    "estimated_changes": 5,
                    "branch_plan_index": 1,
                    "branch_plan_total": 3,
                    "execution_mode": "query_path",
                    "fetch_mode": "nqe_parameters",
                },
                {
                    "model": "dcim.device",
                    "query_name": "Forward Devices",
                    "runtime_ms": 8.0,
                    "row_count": 10,
                    "delete_count": 0,
                    "branch_plan_index": 2,
                    "branch_plan_total": 3,
                    "execution_mode": "query_id",
                    "fetch_mode": "query",
                },
            ],
            applied_change_count=17,
            failed_change_count=2,
            created_change_count=10,
            updated_change_count=5,
            deleted_change_count=2,
        )
        sync.set_branch_run_state(
            {
                "phase": "planning",
                "phase_message": "Planning shard layout.",
                "plan_preview": {
                    "planned_shards": 3,
                    "estimated_changes": 15,
                    "model_count": 2,
                    "retry_risk": "medium",
                    "slowest_model": {
                        "model": "dcim.cable",
                        "query_name": "Forward Cabling",
                        "estimated_changes": 5,
                        "query_runtime_ms": 12.5,
                    },
                },
            }
        )

        with patch.object(
            ForwardIngestion,
            "get_job_logs",
            return_value={
                "statistics": {
                    "dcim.cable": {
                        "current": 4,
                        "total": 4,
                        "applied": 1,
                        "failed": 0,
                        "skipped": 0,
                        "unchanged": 2,
                    },
                    "dcim.device": {
                        "current": 12,
                        "total": 12,
                        "applied": 1,
                        "failed": 0,
                        "skipped": 0,
                        "unchanged": 3,
                    },
                },
                "logs": [
                    (
                        "2026-05-03T10:00:00Z",
                        "warning",
                        None,
                        None,
                        "Branch budget retry: shard produced 22 changes against budget 10; auto-splitting and retrying.",
                    ),
                    (
                        "2026-05-03T10:00:01Z",
                        "info",
                        None,
                        None,
                        "Forward ingestion completed.",
                    ),
                ],
            },
        ):
            summary = ingestion.get_execution_summary()
            sync_summary = sync.get_execution_summary()

        self.assertEqual(summary["model_count"], 2)
        self.assertEqual(summary["shard_count"], 3)
        self.assertEqual(summary["retry_count"], 1)
        self.assertEqual(summary["estimated_changes"], 15)
        self.assertEqual(summary["runtime_ms"], 20.5)
        self.assertEqual(summary["slowest_model"]["model"], "dcim.cable")
        self.assertEqual(summary["applied_change_count"], 17)
        self.assertEqual(summary["unchanged_row_count"], 5)
        self.assertEqual(
            summary["query_modes"]["execution_modes"],
            {"query_id": 1, "query_path": 1},
        )
        self.assertEqual(
            summary["query_modes"]["fetch_modes"],
            {"nqe_parameters": 1, "query": 1},
        )
        self.assertEqual(sync_summary["branch_budget_hints"]["dcim.cable"], 1666)
        self.assertEqual(sync_summary["pre_run_estimate"]["retry_risk"], "medium")
        self.assertIn("latest_ingestion", sync_summary)
        self.assertNotIn("model_results", sync_summary["latest_ingestion"])
        self.assertEqual(sync_summary["latest_ingestion"]["retry_count"], 1)

    def test_execution_summary_counts_platform_unchanged_rows(self):
        sync = ForwardSync.objects.create(
            name="sync-execution-summary-platform",
            source=self.source,
            parameters={
                "snapshot_id": LATEST_PROCESSED_SNAPSHOT,
                "dcim.platform": True,
            },
        )
        ingestion = ForwardIngestion.objects.create(
            sync=sync,
            model_results=[
                {
                    "model": "dcim.platform",
                    "query_name": "Forward Platforms",
                    "runtime_ms": 9.0,
                    "row_count": 1,
                    "delete_count": 0,
                    "estimated_changes": 1,
                    "branch_plan_index": 1,
                    "branch_plan_total": 1,
                    "execution_mode": "query_id",
                    "fetch_mode": "query",
                }
            ],
            applied_change_count=1,
            failed_change_count=0,
            created_change_count=0,
            updated_change_count=0,
            deleted_change_count=0,
        )

        with patch.object(
            ForwardIngestion,
            "get_job_logs",
            return_value={
                "statistics": {
                    "dcim.platform": {
                        "current": 1,
                        "total": 1,
                        "applied": 0,
                        "failed": 0,
                        "skipped": 0,
                        "unchanged": 1,
                    }
                },
                "logs": [],
            },
        ):
            summary = ingestion.get_execution_summary()
            sync_summary = sync.get_execution_summary()

        self.assertEqual(summary["unchanged_row_count"], 1)
        self.assertEqual(summary["query_modes"]["execution_modes"], {"query_id": 1})
        self.assertEqual(sync_summary["latest_ingestion"]["retry_count"], 0)
        self.assertEqual(
            sync_summary["latest_ingestion"]["unchanged_row_count"],
            1,
        )

    @patch("forward_netbox.models.ForwardSource.get_client")
    @patch(
        "forward_netbox.utilities.single_branch_executor.ForwardSingleBranchExecutor"
    )
    def test_sync_sets_source_status_to_syncing_during_run(
        self,
        mock_executor_class,
        _mock_get_client,
    ):
        sync = ForwardSync.objects.create(
            name="sync-source-status-syncing",
            source=self.source,
            parameters={
                "snapshot_id": LATEST_PROCESSED_SNAPSHOT,
                "dcim.device": True,
            },
        )

        def run_side_effect(*args, **kwargs):
            self.source.refresh_from_db()
            self.assertEqual(
                self.source.status,
                ForwardSourceStatusChoices.SYNCING,
            )
            return []

        mock_executor = mock_executor_class.return_value
        mock_executor.run.side_effect = run_side_effect

        sync.sync()

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

    def test_validation_run_force_allow_records_override_audit(self):
        user = get_user_model().objects.create_user(username="override-user")
        policy = ForwardDriftPolicy.objects.create(name="policy-override")
        sync = ForwardSync.objects.create(
            name="sync-override",
            source=self.source,
            drift_policy=policy,
            parameters={
                "snapshot_id": LATEST_PROCESSED_SNAPSHOT,
                "dcim.device": True,
            },
        )
        validation_run = ForwardValidationRun.objects.create(
            sync=sync,
            policy=policy,
            status=ForwardValidationStatusChoices.BLOCKED,
            allowed=False,
            snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
            snapshot_id="snapshot-override",
            blocking_reasons=["Target snapshot is not processed."],
            started=timezone.now(),
            completed=timezone.now(),
        )

        validation_run.force_allow(user=user, reason="Accepted for lab validation.")

        validation_run.refresh_from_db()
        self.assertTrue(validation_run.override_applied)
        self.assertTrue(validation_run.allowed)
        self.assertEqual(validation_run.status, ForwardValidationStatusChoices.PASSED)
        self.assertEqual(validation_run.override_user, user)
        self.assertEqual(validation_run.override_reason, "Accepted for lab validation.")
        self.assertEqual(
            validation_run.override_blocking_reasons,
            ["Target snapshot is not processed."],
        )

    def test_force_allow_validation_run_helper_records_override_audit(self):
        user = get_user_model().objects.create_user(username="override-helper")
        policy = ForwardDriftPolicy.objects.create(name="policy-override-helper")
        sync = ForwardSync.objects.create(
            name="sync-override-helper",
            source=self.source,
            drift_policy=policy,
            parameters={
                "snapshot_id": LATEST_PROCESSED_SNAPSHOT,
                "dcim.device": True,
            },
        )
        validation_run = ForwardValidationRun.objects.create(
            sync=sync,
            policy=policy,
            status=ForwardValidationStatusChoices.BLOCKED,
            allowed=False,
            snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
            snapshot_id="snapshot-override-helper",
            blocking_reasons=["Target snapshot is not processed."],
            started=timezone.now(),
            completed=timezone.now(),
        )

        force_allow_validation_run(
            validation_run,
            user=user,
            reason="Accepted for helper coverage.",
        )

        validation_run.refresh_from_db()
        self.assertTrue(validation_run.override_applied)
        self.assertTrue(validation_run.allowed)
        self.assertEqual(validation_run.status, ForwardValidationStatusChoices.PASSED)
        self.assertEqual(validation_run.override_user, user)
        self.assertEqual(
            validation_run.override_reason, "Accepted for helper coverage."
        )

    def test_validation_runner_skips_blocking_for_matching_force_allowed_run(self):
        policy = ForwardDriftPolicy.objects.create(name="policy-force-allow")
        sync = ForwardSync.objects.create(
            name="sync-force-allow",
            source=self.source,
            drift_policy=policy,
            parameters={
                "snapshot_id": LATEST_PROCESSED_SNAPSHOT,
                "dcim.device": True,
            },
        )
        ForwardValidationRun.objects.create(
            sync=sync,
            policy=policy,
            status=ForwardValidationStatusChoices.PASSED,
            allowed=True,
            override_applied=True,
            override_reason="Accepted for test coverage.",
            snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
            snapshot_id="snapshot-force-allow",
            blocking_reasons=["Would otherwise block."],
            override_blocking_reasons=["Would otherwise block."],
            started=timezone.now(),
            completed=timezone.now(),
        )

        runner = ForwardValidationRunner(
            sync=sync,
            client=None,
            logger_=Mock(),
        )

        reasons = runner._blocking_reasons(
            {
                "snapshot_selector": LATEST_PROCESSED_SNAPSHOT,
                "snapshot_id": "snapshot-force-allow",
            },
            plan=[],
            model_results=[],
            policy=policy,
        )

        self.assertEqual(reasons, [])

    def test_validation_blocks_failed_device_query_when_child_models_depend_on_devices(
        self,
    ):
        policy = ForwardDriftPolicy.objects.create(
            name="policy-child-parent-query-failure",
            block_on_query_errors=False,
        )
        sync = ForwardSync.objects.create(
            name="sync-child-parent-query-failure",
            source=self.source,
            drift_policy=policy,
            parameters={
                "snapshot_id": LATEST_PROCESSED_SNAPSHOT,
                "dcim.device": True,
                "dcim.interface": True,
            },
        )
        runner = ForwardValidationRunner(
            sync=sync,
            client=None,
            logger_=Mock(),
        )

        reasons = runner._blocking_reasons(
            {
                "snapshot_selector": LATEST_PROCESSED_SNAPSHOT,
                "snapshot_id": "snapshot-device-failure",
                "snapshot_info": {"state": "PROCESSED"},
                "snapshot_metrics": {},
            },
            plan=[],
            model_results=[
                {
                    "model": "dcim.device",
                    "failure_count": 1,
                    "row_count": 0,
                    "delete_count": 0,
                }
            ],
            policy=policy,
        )

        self.assertEqual(len(reasons), 1)
        self.assertIn(
            "`dcim.device` query failed while enabled child models depend on "
            "device coverage:",
            reasons[0],
        )
        self.assertIn("dcim.interface", reasons[0])

    def test_validation_blocks_failed_device_metadata_query_before_devices(self):
        policy = ForwardDriftPolicy.objects.create(
            name="policy-device-metadata-query-failure",
            block_on_query_errors=False,
        )
        sync = ForwardSync.objects.create(
            name="sync-device-metadata-query-failure",
            source=self.source,
            drift_policy=policy,
            parameters={
                "snapshot_id": LATEST_PROCESSED_SNAPSHOT,
                "dcim.platform": True,
                "dcim.devicetype": True,
                "dcim.device": True,
            },
        )
        runner = ForwardValidationRunner(
            sync=sync,
            client=None,
            logger_=Mock(),
        )

        reasons = runner._blocking_reasons(
            {
                "snapshot_selector": LATEST_PROCESSED_SNAPSHOT,
                "snapshot_id": "snapshot-metadata-failure",
                "snapshot_info": {"state": "PROCESSED"},
                "snapshot_metrics": {},
            },
            plan=[],
            model_results=[
                {
                    "model": "dcim.platform",
                    "failure_count": 1,
                    "row_count": 0,
                    "delete_count": 0,
                },
                {
                    "model": "dcim.devicetype",
                    "failure_count": 1,
                    "row_count": 0,
                    "delete_count": 0,
                },
            ],
            policy=policy,
        )

        self.assertEqual(
            reasons,
            [
                "Foundational device metadata query failed before `dcim.device`: "
                "dcim.devicetype, dcim.platform."
            ],
        )

    def test_validation_block_adds_require_diff_remediation_hint(self):
        # When the device-coverage gate fires AND the sync is in require_diff mode
        # (a diff run that could not fetch rows), the block must name that cause
        # and the allow_fallback remedy, not only the cryptic coverage message.
        policy = ForwardDriftPolicy.objects.create(
            name="policy-require-diff-hint",
            block_on_query_errors=False,
        )
        sync = ForwardSync.objects.create(
            name="sync-require-diff-hint",
            source=self.source,
            drift_policy=policy,
            parameters={
                "snapshot_id": LATEST_PROCESSED_SNAPSHOT,
                "dcim.device": True,
                "dcim.interface": True,
                "diff_fallback_mode": "require_diff",
            },
        )
        runner = ForwardValidationRunner(sync=sync, client=None, logger_=Mock())

        reasons = runner._blocking_reasons(
            {
                "snapshot_selector": LATEST_PROCESSED_SNAPSHOT,
                "snapshot_id": "snapshot-require-diff",
                "snapshot_info": {"state": "PROCESSED"},
                "snapshot_metrics": {},
            },
            plan=[],
            model_results=[
                {
                    "model": "dcim.device",
                    "failure_count": 1,
                    "row_count": 0,
                    "delete_count": 0,
                }
            ],
            policy=policy,
        )

        self.assertTrue(any("Require diff" in reason for reason in reasons))
        self.assertTrue(any("Allow full fallback" in reason for reason in reasons))

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
                # Isolate this test to the recurring-reschedule enqueue; the
                # vsys/vdom parent-link overlay is default-on and would add a
                # second enqueue (covered by its own test).
                "auto_link_vsys_parents": False,
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

    def test_sync_table_latest_failure_renders_without_ledger(self):
        # Regression: render_latest_failure called the removed execution-ledger
        # stubs and indexed summary["available"] -> KeyError, 500'ing the sync
        # list page in 2.0. It must render from the ingestion's failed-change
        # count and never crash.
        from forward_netbox.models import ForwardIngestion

        sync = ForwardSync.objects.create(name="lf-sync", source=self.source)
        table = ForwardSyncTable(ForwardSync.objects.filter(pk=sync.pk))
        # No ingestion -> graceful dash.
        self.assertEqual(table.render_latest_failure(value=None, record=sync), "---")
        # Failed changes -> surfaced count, still no crash.
        ForwardIngestion.objects.create(sync=sync, failed_change_count=3)
        rendered = str(table.render_latest_failure(value=None, record=sync))
        self.assertIn("3", rendered)
        self.assertNotIn("available", rendered)

    @patch("forward_netbox.models.ForwardSource.get_client")
    @patch(
        "forward_netbox.utilities.single_branch_executor.ForwardSingleBranchExecutor"
    )
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

        sync.refresh_from_db()
        self.source.refresh_from_db()
        self.assertEqual(ForwardIngestion.objects.filter(sync=sync).count(), 1)
        self.assertEqual(sync.status, ForwardSyncStatusChoices.FAILED)
        self.assertEqual(self.source.status, ForwardSourceStatusChoices.FAILED)
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

    def test_incremental_diff_baseline_skips_missing_snapshot_when_client_provided(
        self,
    ):
        sync = ForwardSync.objects.create(
            name="sync-diff-baseline-client",
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
            snapshot_id="ui-harness-snapshot",
            baseline_ready=True,
        )
        specs = [
            QuerySpec(
                model_string="dcim.device",
                query_name="Device Query",
                query_id="Q_device",
            )
        ]
        client = Mock()
        client.get_snapshots.return_value = [
            {"id": "snapshot-current", "state": "PROCESSED"},
            {"id": "snapshot-old", "state": "PROCESSED"},
        ]

        self.assertIsNone(
            sync.incremental_diff_baseline(
                specs=specs,
                current_snapshot_id="snapshot-current",
                client=client,
            )
        )

    def test_incremental_diff_baseline_ignores_non_iterable_snapshot_payload(self):
        sync = ForwardSync.objects.create(
            name="sync-diff-baseline-client-mock",
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
            snapshot_id="ui-harness-snapshot",
            baseline_ready=True,
        )
        specs = [
            QuerySpec(
                model_string="dcim.device",
                query_name="Device Query",
                query_id="Q_device",
            )
        ]
        client = Mock()
        client.get_snapshots.return_value = Mock()

        self.assertIsNone(
            sync.incremental_diff_baseline(
                specs=specs,
                current_snapshot_id="snapshot-current",
                client=client,
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

    def test_workload_summary_helpers_roll_up_execution_details(self):
        ingestion = ForwardIngestion.objects.create(
            sync=self.sync,
            snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
            snapshot_id="snapshot-before",
            sync_mode="hybrid",
            baseline_ready=True,
            model_results=[
                {
                    "model": "dcim.device",
                    "row_count": 4,
                    "delete_count": 1,
                    "estimated_changes": 5,
                    "runtime_ms": 12.5,
                    "branch_plan_total": 7,
                    "diagnostics": [{"message": "one"}],
                },
                {
                    "model": "ipam.prefix",
                    "row_count": 3,
                    "delete_count": 2,
                    "estimated_changes": 5,
                    "runtime_ms": 7.5,
                    "branch_plan_total": 7,
                    "diagnostics": [],
                },
            ],
        )

        workload = ingestion.get_workload_summary()

        self.assertEqual(workload["sync_mode"], "hybrid")
        self.assertTrue(workload["baseline_ready"])
        self.assertEqual(workload["model_count"], 2)
        self.assertEqual(workload["shard_count"], 7)
        self.assertEqual(workload["estimated_changes"], 10)
        self.assertEqual(workload["row_count"], 7)
        self.assertEqual(workload["delete_count"], 3)
        self.assertEqual(workload["runtime_ms"], 20.0)
        self.assertEqual(workload["diagnostic_count"], 1)

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
                "forward_netbox.utilities.ingestion_merge.suppress_branch_merge_side_effect_signals"
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
        self.assertFalse(ingestion.baseline_ready)
        self.assertEqual(self.sync.get_branch_run_state(), {})
        self.assertTrue(self.sync.ready_for_sync)

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

    def test_sync_merge_sets_merging_then_completed(self):
        ingestion = ForwardIngestion.objects.create(
            sync=self.sync,
            snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
            snapshot_id="snapshot-before",
        )
        observed_statuses = []

        def merge_side_effect(*args, **kwargs):
            self.sync.refresh_from_db()
            observed_statuses.append(self.sync.status)
            self.assertEqual(self.sync.status, ForwardSyncStatusChoices.MERGING)

        with patch(
            "forward_netbox.utilities.merge.merge_branch", side_effect=merge_side_effect
        ):
            ingestion.sync_merge(mark_baseline_ready=False)

        self.sync.refresh_from_db()
        self.assertIn(ForwardSyncStatusChoices.MERGING, observed_statuses)
        self.assertEqual(self.sync.status, ForwardSyncStatusChoices.COMPLETED)

    def test_sync_merge_marks_failed_when_merge_raises(self):
        ingestion = ForwardIngestion.objects.create(
            sync=self.sync,
            snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
            snapshot_id="snapshot-before",
        )

        with patch(
            "forward_netbox.utilities.merge.merge_branch",
            side_effect=RuntimeError("merge boom"),
        ):
            with self.assertRaises(RuntimeError):
                ingestion.sync_merge(mark_baseline_ready=False)

        self.sync.refresh_from_db()
        self.source.refresh_from_db()
        self.assertEqual(self.sync.status, ForwardSyncStatusChoices.FAILED)
        self.assertEqual(self.source.status, ForwardSourceStatusChoices.FAILED)

    def test_annotate_statistics_uses_persisted_counts_when_branch_missing(self):
        ingestion = ForwardIngestion.objects.create(
            sync=self.sync,
            snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
            snapshot_id="snapshot-before",
            applied_change_count=157,
            created_change_count=7,
            updated_change_count=130,
            deleted_change_count=20,
        )

        annotated = annotate_statistics(ForwardIngestion.objects).get(pk=ingestion.pk)

        self.assertEqual(annotated.staged_changes, 157)
        self.assertEqual(annotated.num_created, 7)
        self.assertEqual(annotated.num_updated, 130)
        self.assertEqual(annotated.num_deleted, 20)

    def test_annotate_statistics_uses_persisted_counts_when_branch_diffs_lag(self):
        branch = Branch.objects.create(
            name=f"stats-lag-{uuid4().hex[:12]}",
            schema_id=f"stats_lag_{uuid4().hex[:12]}",
        )
        ingestion = ForwardIngestion.objects.create(
            sync=self.sync,
            branch=branch,
            applied_change_count=500,
            deleted_change_count=500,
        )

        annotated = annotate_statistics(ForwardIngestion.objects).get(pk=ingestion.pk)

        self.assertEqual(annotated.staged_changes, 500)
        self.assertEqual(annotated.num_deleted, 500)


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

    def test_prefix_map_defaults_use_exact_vrf_identity(self):
        netbox_model = ContentType.objects.get(app_label="ipam", model="prefix")
        query_map = ForwardNQEMap(
            name="Prefix Map",
            netbox_model=netbox_model,
            query='select {\n  prefix: "10.0.0.0/24",\n  vrf: null,\n  status: "active"\n}',
        )

        query_map.clean()

        self.assertEqual(query_map.coalesce_fields, [["prefix", "vrf"]])

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

    def test_inventory_item_defaults_allow_missing_part_or_serial(self):
        netbox_model = ContentType.objects.get(app_label="dcim", model="inventoryitem")
        query_map = ForwardNQEMap(
            name="Inventory Map",
            netbox_model=netbox_model,
            query=(
                'select {\n  device: "device-1",\n  name: "fan-1",\n'
                '  part_id: "",\n  serial: "",\n  status: "active",\n'
                "  discovered: true\n}"
            ),
        )

        query_map.clean()

        self.assertEqual(
            query_map.coalesce_fields,
            [
                ["device", "name", "part_id", "serial"],
                ["device", "name", "part_id"],
                ["device", "name"],
            ],
        )

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

    def test_virtual_chassis_map_allows_query_missing_position(self):
        netbox_model = ContentType.objects.get(app_label="dcim", model="virtualchassis")
        query_map = ForwardNQEMap(
            name="Virtual Chassis Map",
            netbox_model=netbox_model,
            query=(
                'select {\n  device: "device-1",\n  vc_name: "vc-1",\n  name: "vc-1",\n'
                '  vc_domain: "domain-1"\n}'
            ),
        )

        query_map.clean()

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
        self.assertEqual(query_map.coalesce_fields, [["prefix", "vrf"]])
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

    def test_seed_builtin_maps_preserves_query_id_execution_mode(self):
        netbox_model = ContentType.objects.get(app_label="dcim", model="site")
        query_map = ForwardNQEMap.objects.get(
            name="Forward Locations",
            netbox_model=netbox_model,
            built_in=True,
        )
        query_map.query_id = "FQ_locations"
        query_map.query = ""
        query_map.commit_id = "commit-1"
        query_map.save(update_fields=["query_id", "query", "commit_id"])

        seed_builtin_nqe_maps(type("Sender", (), {"label": "forward_netbox"}))

        query_map.refresh_from_db()
        self.assertEqual(query_map.query_id, "FQ_locations")
        self.assertEqual(query_map.query, "")
        self.assertEqual(query_map.commit_id, "commit-1")

    def test_seed_builtin_maps_preserves_seeded_shard_parameters(self):
        netbox_model = ContentType.objects.get(app_label="dcim", model="interface")
        query_map = ForwardNQEMap.objects.get(
            name="Forward Interfaces",
            netbox_model=netbox_model,
            built_in=True,
        )
        query_map.parameters = {"forward_netbox_shard_keys": []}
        query_map.save(update_fields=["parameters"])

        seed_builtin_nqe_maps(type("Sender", (), {"label": "forward_netbox"}))

        query_map.refresh_from_db()
        self.assertEqual(query_map.parameters, {"forward_netbox_shard_keys": []})

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


class ForwardIngestionProgressStatisticsTest(TestCase):
    def _ingestion(self, *, status, raw_stats):
        from types import SimpleNamespace

        return SimpleNamespace(
            job=SimpleNamespace(status=status),
            merge_job=None,
            get_job_logs=lambda job: {"statistics": raw_stats},
            num_created=1,
            num_updated=1,
            num_deleted=1,
            staged_changes=1,
            created_change_count=0,
            updated_change_count=0,
            deleted_change_count=0,
            applied_change_count=0,
        )

    def test_completed_job_renders_full_bars(self):
        # A finished run must show 100% for every model. Relationship and
        # two-phase models (cable+termination, device+primary_ip, fhrp group+
        # assignment) leave current<total because `total` counts merge
        # ChangeDiff rows while `current` counts applied objects.
        from forward_netbox.utilities.ingestion_presentation import get_statistics

        ingestion = self._ingestion(
            status="completed",
            raw_stats={
                "dcim.cable": {"total": 6, "current": 3},
                "dcim.device": {"total": 12, "current": 6},
                "ipam.fhrpgroup": {"total": 7, "current": 5},
                "dcim.module": {"total": 1431, "current": 1431},
            },
        )

        stats = get_statistics(ingestion)["statistics"]
        self.assertEqual(stats["dcim.cable"], 100.0)
        self.assertEqual(stats["dcim.device"], 100.0)
        self.assertEqual(stats["ipam.fhrpgroup"], 100.0)
        self.assertEqual(stats["dcim.module"], 100.0)

    def test_running_job_shows_partial_progress(self):
        from forward_netbox.utilities.ingestion_presentation import get_statistics

        ingestion = self._ingestion(
            status="running",
            raw_stats={"dcim.device": {"total": 12, "current": 6}},
        )

        stats = get_statistics(ingestion)["statistics"]
        self.assertEqual(stats["dcim.device"], 50.0)
