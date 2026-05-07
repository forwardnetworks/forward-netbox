from datetime import timedelta

from django.test import TestCase
from django.utils import timezone

from forward_netbox.models import ForwardSource
from forward_netbox.models import ForwardSync
from forward_netbox.utilities.forward_api import LATEST_PROCESSED_SNAPSHOT
from forward_netbox.utilities.sync_state import clear_branch_run_state
from forward_netbox.utilities.sync_state import get_branch_run_state
from forward_netbox.utilities.sync_state import get_display_parameters
from forward_netbox.utilities.sync_state import get_sync_activity
from forward_netbox.utilities.sync_state import touch_branch_run_progress
from forward_netbox.utilities.sync_state import set_branch_run_state
from forward_netbox.utilities.sync_state import set_model_change_density


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
        set_branch_run_state(
            self.sync,
            {
                "phase": "planning",
                "phase_message": "Building shard plan.",
                "next_plan_index": 2,
                "total_plan_items": 4,
            },
        )

        self.assertEqual(get_branch_run_state(self.sync)["phase"], "planning")

        clear_branch_run_state(self.sync)

        self.assertEqual(get_branch_run_state(self.sync), {})

    def test_display_parameters_include_density_and_branch_hints(self):
        set_model_change_density(self.sync, {"dcim.device": 2.0})

        params = get_display_parameters(
            self.sync,
            max_changes_per_branch_default=10000,
        )

        self.assertEqual(params["model_change_density"]["dcim.device"], 2.0)
        self.assertIn("branch_budget_hints", params)
        self.assertEqual(params["branch_budget_hints"]["dcim.device"], 3500)

    def test_sync_activity_uses_phase_message(self):
        set_branch_run_state(
            self.sync,
            {
                "phase": "executing",
                "phase_message": "Applying planned shard changes.",
            },
        )

        self.assertEqual(
            get_sync_activity(self.sync),
            "Applying planned shard changes.",
        )

    def test_sync_activity_prefers_progress_heartbeat(self):
        started = (timezone.now() - timedelta(minutes=4, seconds=12)).isoformat()
        set_branch_run_state(
            self.sync,
            {
                "phase": "executing",
                "phase_message": "Applying planned shard changes.",
                "phase_started": started,
                "last_progress_message": "Applying shard 131/146 for ipam.ipaddress.",
                "last_progress_at": started,
                "current_model_string": "ipam.ipaddress",
                "current_shard_index": 131,
                "total_plan_items": 146,
                "current_row_count": 4500,
                "current_row_total": 12000,
            },
        )

        activity = get_sync_activity(self.sync)

        self.assertIn("Applying shard 131/146 for ipam.ipaddress.", activity)
        self.assertIn("4m", activity)

    def test_touch_branch_run_progress_updates_state(self):
        set_branch_run_state(
            self.sync,
            {
                "phase": "executing",
                "phase_message": "Applying planned shard changes.",
            },
        )

        touched = touch_branch_run_progress(
            self.sync,
            phase_message="Applying shard 131/146 for ipam.ipaddress.",
            model_string="ipam.ipaddress",
            shard_index=131,
            total_plan_items=146,
            row_count=10,
            row_total=12000,
        )

        state = get_branch_run_state(self.sync)
        self.assertTrue(touched)
        self.assertEqual(state["last_progress_message"], "Applying shard 131/146 for ipam.ipaddress.")
        self.assertEqual(state["current_model_string"], "ipam.ipaddress")
        self.assertEqual(state["current_shard_index"], 131)
        self.assertEqual(state["current_row_count"], 10)
        self.assertEqual(state["current_row_total"], 12000)
        self.assertTrue(state["last_progress_at"])
