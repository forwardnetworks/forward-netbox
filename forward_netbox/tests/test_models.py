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

from forward_netbox.choices import FORWARD_BGP_MODELS
from forward_netbox.choices import forward_configured_models
from forward_netbox.choices import ForwardDriftPolicyBaselineChoices
from forward_netbox.choices import ForwardExecutionBackendChoices
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
from forward_netbox.utilities.branch_budget import build_branch_budget_hints
from forward_netbox.utilities.branch_budget import DEFAULT_MAX_CHANGES_PER_BRANCH
from forward_netbox.utilities.execution_telemetry import build_branch_run_summary
from forward_netbox.utilities.execution_telemetry import (
    build_ingestion_execution_summary,
)
from forward_netbox.utilities.execution_telemetry import build_plan_preview
from forward_netbox.utilities.execution_telemetry import build_sync_execution_summary
from forward_netbox.utilities.forward_api import LATEST_PROCESSED_SNAPSHOT
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

    def test_sync_accepts_fast_bootstrap_execution_backend(self):
        sync = ForwardSync(
            name="sync-fast-bootstrap",
            source=self.source,
            parameters={
                "snapshot_id": LATEST_PROCESSED_SNAPSHOT,
                "execution_backend": ForwardExecutionBackendChoices.FAST_BOOTSTRAP,
                "dcim.device": True,
            },
        )

        sync.clean()

        self.assertEqual(
            sync.parameters["execution_backend"],
            ForwardExecutionBackendChoices.FAST_BOOTSTRAP,
        )

    def test_sync_display_parameters_include_execution_backend(self):
        sync = ForwardSync.objects.create(
            name="sync-display-fast-bootstrap",
            source=self.source,
            parameters={
                "snapshot_id": LATEST_PROCESSED_SNAPSHOT,
                "execution_backend": ForwardExecutionBackendChoices.FAST_BOOTSTRAP,
                "dcim.device": True,
            },
        )

        self.assertEqual(
            sync.get_display_parameters()["execution_backend"],
            ForwardExecutionBackendChoices.FAST_BOOTSTRAP,
        )

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
        self.assertEqual(params["branch_run"]["plan_preview"], {})

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
        self.assertEqual(ingestion_summary["model_results"], [])

        sync_summary = build_sync_execution_summary(
            enabled_models=["dcim.cable"],
            max_changes_per_branch=10000,
            model_change_density={"dcim.cable": 2.0},
            branch_run_state={"plan_preview": plan_preview},
            latest_ingestion_summary=ingestion_summary,
        )
        self.assertEqual(sync_summary["branch_budget_hints"]["dcim.cable"], 2500)
        self.assertEqual(sync_summary["pre_run_estimate"]["planned_shards"], 0)
        self.assertEqual(sync_summary["latest_ingestion"]["retry_count"], 0)

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
                },
                {
                    "model": "dcim.device",
                    "query_name": "Forward Devices",
                    "runtime_ms": 8.0,
                    "row_count": 10,
                    "delete_count": 0,
                    "branch_plan_index": 2,
                    "branch_plan_total": 3,
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
                ]
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
        self.assertEqual(sync_summary["branch_budget_hints"]["dcim.cable"], 1666)
        self.assertEqual(sync_summary["pre_run_estimate"]["retry_risk"], "medium")
        self.assertIn("latest_ingestion", sync_summary)
        self.assertEqual(sync_summary["latest_ingestion"]["retry_count"], 1)

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

    @patch("forward_netbox.models.ForwardSource.get_client")
    @patch("forward_netbox.utilities.multi_branch.ForwardMultiBranchExecutor")
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

    def test_virtual_chassis_map_rejects_query_missing_position(self):
        netbox_model = ContentType.objects.get(app_label="dcim", model="virtualchassis")
        query_map = ForwardNQEMap(
            name="Virtual Chassis Map",
            netbox_model=netbox_model,
            query=(
                'select {\n  device: "device-1",\n  vc_name: "vc-1",\n'
                '  vc_domain: "domain-1"\n}'
            ),
        )

        with self.assertRaises(ValidationError) as ctx:
            query_map.clean()

        self.assertIn("vc_position", str(ctx.exception))

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
