from django.test import TestCase
from django.utils import timezone

from forward_netbox.models import ForwardSource
from forward_netbox.models import ForwardSync
from forward_netbox.utilities.execution_ledger import active_execution_run
from forward_netbox.utilities.forward_api import LATEST_PROCESSED_SNAPSHOT
from forward_netbox.utilities.sync_state import clear_branch_run_state
from forward_netbox.utilities.sync_state import get_branch_run_state
from forward_netbox.utilities.sync_state import get_display_parameters
from forward_netbox.utilities.sync_state import has_pending_branch_run
from forward_netbox.utilities.sync_state import mark_branch_run_failed
from forward_netbox.utilities.sync_state import set_branch_run_state
from forward_netbox.utilities.sync_state import set_model_change_density
from forward_netbox.utilities.sync_state import set_model_change_density_profile


class ForwardSyncStateHelperTest(TestCase):
    def setUp(self):
        self.source = ForwardSource.objects.create(
            name="source-sync-state",
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
            name="sync-sync-state",
            source=self.source,
            parameters={
                "snapshot_id": LATEST_PROCESSED_SNAPSHOT,
                "dcim.device": True,
            },
        )

    def test_branch_run_state_round_trip(self):
        set_branch_run_state(self.sync, {"phase": "planning"})
        self.assertEqual(get_branch_run_state(self.sync), {"phase": "planning"})
        clear_branch_run_state(self.sync)
        self.assertEqual(get_branch_run_state(self.sync), {})

    def test_pending_branch_run_uses_legacy_compatibility_state_without_ledger(self):
        self.sync.set_branch_run_state(
            {
                "phase": "executing",
                "next_plan_index": 1,
                "total_plan_items": 1,
                "awaiting_merge": False,
            }
        )

        self.assertTrue(has_pending_branch_run(self.sync))

    def test_set_branch_run_state_writes_when_no_execution_run_exists(self):
        wrote = set_branch_run_state(
            self.sync,
            {
                "phase": "planning",
                "phase_message": "compatibility fallback write",
                "next_plan_index": 1,
                "total_plan_items": 1,
            },
        )

        self.sync.refresh_from_db()
        self.assertTrue(wrote)
        self.assertEqual(get_branch_run_state(self.sync)["phase"], "planning")

    def test_active_execution_run_does_not_upgrade_legacy_branch_run_payload(self):
        set_branch_run_state(self.sync, {"plan_items": [{"index": 1}]})
        self.assertIsNone(active_execution_run(self.sync))

    def test_display_parameters_include_density_and_branch_hints(self):
        set_model_change_density(self.sync, {"dcim.device": 2.0})
        set_model_change_density_profile(
            self.sync,
            {
                "dcim.device": {
                    "density": 2.0,
                    "sample_count": 4,
                    "accepted_observations": 4,
                    "rejected_observations": 1,
                    "mean": 2.1,
                    "m2": 0.2,
                    "variance": 0.066666,
                    "stddev": 0.258198,
                    "last_updated_at": timezone.now().isoformat(),
                }
            },
        )

        params = get_display_parameters(
            self.sync,
            max_changes_per_branch_default=10000,
        )

        self.assertEqual(params["model_change_density"]["dcim.device"], 2.0)
        self.assertEqual(
            params["model_change_density_profile"]["model_count"],
            1,
        )
        self.assertIn("branch_budget_hints", params)
        self.assertEqual(params["branch_budget_hints"]["dcim.device"], 4666)
        self.assertEqual(
            params["branch_budget_density_policy"]["dcim.device"]["policy"],
            "medium_confidence_blended_density",
        )

    def test_sync_summaries_compact_plan_items_for_ui_payload(self):
        self.sync.set_branch_run_state(
            {
                "snapshot_id": "snapshot-state",
                "phase": "planning",
                "phase_message": "Building shard plan.",
                "plan_items": [{"index": i, "status": "queued"} for i in range(99)],
                "plan_preview": {
                    "planned_shards": 99,
                    "estimated_changes": 123456,
                },
            }
        )

        workload = self.sync.get_workload_summary()
        execution = self.sync.get_execution_summary()

        self.assertNotIn("plan_items", workload["branch_run"])
        self.assertEqual(workload["branch_run"]["plan_items_count"], 99)
        self.assertNotIn("plan_items", execution["branch_run"])
        self.assertEqual(execution["branch_run"]["plan_items_count"], 99)
        self.assertEqual(workload["pre_run_estimate"]["planned_shards"], 99)
        self.assertEqual(execution["pre_run_estimate"]["estimated_changes"], 123456)

    def test_advisory_summary_compacts_nested_workload_preview_plan_items(self):
        self.sync.set_branch_run_state(
            {
                "snapshot_id": "snapshot-state",
                "phase": "executing",
                "plan_items": [{"index": i, "status": "queued"} for i in range(150)],
            }
        )

        advisory = self.sync.get_advisory_summary()

        self.assertIn("branch_run", advisory)
        branch_run = advisory["branch_run"]
        self.assertIsInstance(branch_run, dict)
        self.assertNotIn("plan_items", branch_run)
        self.assertEqual(branch_run["plan_items_count"], 150)
        self.assertIn("pre_run_estimate", advisory)

    def test_mark_branch_run_failed_clears_stale_progress(self):
        self.assertFalse(mark_branch_run_failed(self.sync, "Forward ingestion failed."))
