from datetime import timedelta
from unittest.mock import Mock

from django.test import TestCase
from django.utils import timezone
from django.utils.dateparse import parse_datetime

from forward_netbox.choices import ForwardExecutionRunStatusChoices
from forward_netbox.choices import ForwardExecutionStepStatusChoices
from forward_netbox.models import ForwardExecutionRun
from forward_netbox.models import ForwardExecutionStep
from forward_netbox.models import ForwardSource
from forward_netbox.models import ForwardSync
from forward_netbox.utilities.execution_ledger import active_execution_run
from forward_netbox.utilities.execution_ledger import claim_stage_step
from forward_netbox.utilities.execution_ledger import mark_run_completed
from forward_netbox.utilities.execution_ledger import update_run_from_branch_state
from forward_netbox.utilities.forward_api import LATEST_PROCESSED_SNAPSHOT
from forward_netbox.utilities.multi_branch_lifecycle import set_runtime_phase
from forward_netbox.utilities.sync_state import clear_branch_run_state
from forward_netbox.utilities.sync_state import get_branch_run_display_state
from forward_netbox.utilities.sync_state import get_branch_run_state
from forward_netbox.utilities.sync_state import get_display_parameters
from forward_netbox.utilities.sync_state import get_sync_activity
from forward_netbox.utilities.sync_state import has_pending_branch_run
from forward_netbox.utilities.sync_state import is_waiting_for_branch_merge
from forward_netbox.utilities.sync_state import mark_branch_run_failed
from forward_netbox.utilities.sync_state import ready_to_continue_sync
from forward_netbox.utilities.sync_state import set_branch_run_state
from forward_netbox.utilities.sync_state import set_model_change_density
from forward_netbox.utilities.sync_state import touch_branch_run_progress


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

    def test_set_branch_run_state_is_suppressed_when_execution_run_exists(self):
        ForwardExecutionRun.objects.create(
            sync=self.sync,
            source=self.source,
            backend="branching",
            status=ForwardExecutionRunStatusChoices.RUNNING,
            snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
            snapshot_id="snapshot-ledger",
            total_steps=1,
            next_step_index=1,
        )

        wrote = set_branch_run_state(
            self.sync,
            {
                "phase": "planning",
                "phase_message": "compatibility write should be suppressed",
                "next_plan_index": 1,
                "total_plan_items": 1,
            },
        )

        self.sync.refresh_from_db()
        self.assertFalse(wrote)
        self.assertEqual(get_branch_run_state(self.sync), {})

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

    def test_pending_branch_run_falls_back_to_execution_ledger(self):
        execution_run = ForwardExecutionRun.objects.create(
            sync=self.sync,
            source=self.source,
            backend="branching",
            status="running",
            snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
            snapshot_id="snapshot-1",
            total_steps=1,
            next_step_index=1,
        )
        ForwardExecutionStep.objects.create(
            run=execution_run,
            index=1,
            kind="stage",
            status=ForwardExecutionStepStatusChoices.RUNNING,
            model_string="dcim.site",
        )

        self.assertEqual(get_branch_run_state(self.sync), {})
        self.assertTrue(has_pending_branch_run(self.sync))

    def test_pending_branch_run_prefers_execution_ledger_over_stale_json(self):
        ForwardExecutionRun.objects.create(
            sync=self.sync,
            source=self.source,
            backend="branching",
            status="completed",
            snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
            snapshot_id="snapshot-complete",
            total_steps=1,
            next_step_index=2,
        )
        self.sync.set_branch_run_state(
            {
                "next_plan_index": 1,
                "total_plan_items": 1,
                "awaiting_merge": False,
            }
        )

        self.assertFalse(has_pending_branch_run(self.sync))

    def test_ready_to_continue_sync_uses_execution_ledger_without_json(self):
        execution_run = ForwardExecutionRun.objects.create(
            sync=self.sync,
            source=self.source,
            backend="branching",
            status="running",
            snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
            snapshot_id="snapshot-continue",
            total_steps=1,
            next_step_index=1,
        )
        ForwardExecutionStep.objects.create(
            run=execution_run,
            index=1,
            kind="stage",
            status=ForwardExecutionStepStatusChoices.PENDING,
            model_string="dcim.site",
        )

        self.assertEqual(get_branch_run_state(self.sync), {})
        self.assertTrue(ready_to_continue_sync(self.sync))
        self.assertTrue(self.sync.ready_to_continue_sync)

    def test_waiting_for_branch_merge_prefers_execution_ledger_over_stale_json(
        self,
    ):
        execution_run = ForwardExecutionRun.objects.create(
            sync=self.sync,
            source=self.source,
            backend="branching",
            status="running",
            snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
            snapshot_id="snapshot-wait",
            total_steps=1,
            next_step_index=1,
        )
        ForwardExecutionStep.objects.create(
            run=execution_run,
            index=1,
            kind="stage",
            status=ForwardExecutionStepStatusChoices.STAGED,
            model_string="dcim.site",
        )
        self.sync.set_branch_run_state(
            {
                "awaiting_merge": False,
                "next_plan_index": 1,
                "total_plan_items": 1,
            }
        )

        self.assertTrue(is_waiting_for_branch_merge(self.sync))

    def test_active_execution_run_falls_back_to_latest_nonterminal_ledger_run(self):
        completed_run = ForwardExecutionRun.objects.create(
            sync=self.sync,
            source=self.source,
            backend="branching",
            status="completed",
            snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
            snapshot_id="snapshot-complete",
            total_steps=1,
            next_step_index=2,
        )
        active_run = ForwardExecutionRun.objects.create(
            sync=self.sync,
            source=self.source,
            backend="branching",
            status="running",
            snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
            snapshot_id="snapshot-active",
            total_steps=1,
            next_step_index=1,
        )
        step = ForwardExecutionStep.objects.create(
            run=active_run,
            index=1,
            kind="stage",
            status=ForwardExecutionStepStatusChoices.PENDING,
            model_string="dcim.site",
        )

        self.assertEqual(get_branch_run_state(self.sync), {})
        self.assertEqual(active_execution_run(self.sync), active_run)

        claimed = claim_stage_step(self.sync, 1, job=None)

        self.assertEqual(claimed, step)
        step.refresh_from_db()
        active_run.refresh_from_db()
        completed_run.refresh_from_db()
        self.assertEqual(step.status, ForwardExecutionStepStatusChoices.RUNNING)
        self.assertEqual(active_run.next_step_index, 1)
        self.assertEqual(completed_run.status, "completed")

    def test_active_execution_run_does_not_upgrade_legacy_branch_run_payload(self):
        set_branch_run_state(self.sync, {"plan_items": [{"index": 1}]})
        self.assertIsNone(active_execution_run(self.sync))

    def test_mark_run_completed_uses_active_ledger_run_without_branch_state(self):
        execution_run = ForwardExecutionRun.objects.create(
            sync=self.sync,
            source=self.source,
            backend="branching",
            status="running",
            snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
            snapshot_id="snapshot-active",
            total_steps=1,
            next_step_index=1,
        )
        self.assertEqual(get_branch_run_state(self.sync), {})

        completed = mark_run_completed(self.sync, baseline_ready=True)

        self.assertEqual(completed, execution_run)
        execution_run.refresh_from_db()
        self.assertEqual(execution_run.status, "completed")
        self.assertTrue(execution_run.baseline_ready)
        self.assertEqual(execution_run.phase, "completed")

    def test_mark_run_completed_is_idempotent_for_completed_run(self):
        execution_run = ForwardExecutionRun.objects.create(
            sync=self.sync,
            source=self.source,
            backend="branching",
            status="running",
            snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
            snapshot_id="snapshot-active",
            total_steps=1,
            next_step_index=2,
        )
        set_branch_run_state(
            self.sync,
            {
                "execution_run_id": execution_run.pk,
                "next_plan_index": 2,
                "total_plan_items": 1,
            },
        )

        first = mark_run_completed(self.sync, baseline_ready=True)
        first_completed = first.completed
        second = mark_run_completed(self.sync, baseline_ready=False)

        self.assertEqual(second, first)
        second.refresh_from_db()
        self.assertEqual(second.status, "completed")
        self.assertTrue(second.baseline_ready)
        self.assertEqual(second.completed, first_completed)

    def test_waiting_for_branch_merge_falls_back_to_execution_ledger(self):
        execution_run = ForwardExecutionRun.objects.create(
            sync=self.sync,
            source=self.source,
            backend="branching",
            status="waiting",
            snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
            snapshot_id="snapshot-1",
            total_steps=1,
            next_step_index=1,
        )
        ForwardExecutionStep.objects.create(
            run=execution_run,
            index=1,
            kind="stage",
            status=ForwardExecutionStepStatusChoices.STAGED,
            model_string="dcim.site",
        )

        self.assertEqual(get_branch_run_state(self.sync), {})
        self.assertTrue(is_waiting_for_branch_merge(self.sync))

    def test_display_parameters_include_density_and_branch_hints(self):
        set_model_change_density(self.sync, {"dcim.device": 2.0})

        params = get_display_parameters(
            self.sync,
            max_changes_per_branch_default=10000,
        )

        self.assertEqual(params["model_change_density"]["dcim.device"], 2.0)
        self.assertIn("branch_budget_hints", params)
        self.assertEqual(params["branch_budget_hints"]["dcim.device"], 3500)

    def test_display_state_uses_execution_ledger_without_branch_run_json(self):
        execution_run = ForwardExecutionRun.objects.create(
            sync=self.sync,
            source=self.source,
            backend="branching",
            status="running",
            phase="executing",
            phase_message="Applying planned shard changes.",
            snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
            snapshot_id="snapshot-ledger",
            total_steps=3,
            next_step_index=2,
            plan_preview={
                "planned_shards": 3,
                "estimated_changes": 25000,
            },
        )
        ForwardExecutionStep.objects.create(
            run=execution_run,
            index=2,
            kind="stage",
            status=ForwardExecutionStepStatusChoices.RUNNING,
            model_string="dcim.interface",
            label="dcim.interface shard 2",
            estimated_changes=9000,
        )
        self.sync.status = "syncing"
        self.sync.save(update_fields=["status"])

        state = get_branch_run_display_state(self.sync)
        display = get_display_parameters(
            self.sync,
            max_changes_per_branch_default=10000,
        )
        workload = self.sync.get_workload_summary()
        execution = self.sync.get_execution_summary()
        activity = get_sync_activity(self.sync)

        self.assertEqual(get_branch_run_state(self.sync), {})
        self.assertEqual(state["execution_run_id"], execution_run.pk)
        self.assertEqual(state["state_source"], "execution_ledger")
        self.assertTrue(state["state_synthesized"])
        self.assertEqual(display["branch_run"]["phase"], "executing")
        self.assertEqual(workload["branch_run"]["phase"], "executing")
        self.assertEqual(workload["pre_run_estimate"]["planned_shards"], 3)
        self.assertEqual(execution["branch_run"]["phase"], "executing")
        self.assertEqual(activity, "Processing dcim.interface shard 2/3")

    def test_ledger_display_state_orders_plan_items_by_step_index(self):
        execution_run = ForwardExecutionRun.objects.create(
            sync=self.sync,
            source=self.source,
            backend="branching",
            status="running",
            snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
            snapshot_id="snapshot-ledger",
            total_steps=2,
            next_step_index=1,
        )
        ForwardExecutionStep.objects.create(
            run=execution_run,
            index=2,
            kind="stage",
            status=ForwardExecutionStepStatusChoices.PENDING,
            model_string="dcim.interface",
            label="dcim.interface shard 2",
            estimated_changes=2,
        )
        ForwardExecutionStep.objects.create(
            run=execution_run,
            index=1,
            kind="stage",
            status=ForwardExecutionStepStatusChoices.RUNNING,
            model_string="dcim.site",
            label="dcim.site shard 1",
            estimated_changes=1,
        )

        state = get_branch_run_display_state(self.sync)

        self.assertEqual(
            [item["index"] for item in state["plan_items"]],
            [1, 2],
        )
        self.assertEqual(state["current_model_string"], "dcim.site")

    def test_sync_activity_uses_phase_message(self):
        ForwardExecutionRun.objects.create(
            sync=self.sync,
            source=self.source,
            backend="branching",
            status=ForwardExecutionRunStatusChoices.RUNNING,
            phase="executing",
            phase_message="Applying planned shard changes.",
            snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
            snapshot_id="snapshot-activity",
            total_steps=1,
            next_step_index=1,
        )

        self.assertEqual(
            get_sync_activity(self.sync),
            "Applying planned shard changes.",
        )

    def test_sync_activity_prefers_progress_heartbeat(self):
        started = (timezone.now() - timedelta(minutes=4, seconds=12)).isoformat()
        run = ForwardExecutionRun.objects.create(
            sync=self.sync,
            source=self.source,
            backend="branching",
            status=ForwardExecutionRunStatusChoices.RUNNING,
            phase="executing",
            phase_message="Applying planned shard changes.",
            snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
            snapshot_id="snapshot-activity-progress",
            total_steps=146,
            next_step_index=131,
            latest_heartbeat=parse_datetime(started),
        )
        ForwardExecutionStep.objects.create(
            run=run,
            index=131,
            kind="stage",
            status=ForwardExecutionStepStatusChoices.RUNNING,
            model_string="ipam.ipaddress",
            attempted_row_count=4500,
            fetched_row_count=12000,
            heartbeat=parse_datetime(started),
        )

        activity = get_sync_activity(self.sync)

        self.assertIn("Processing ipam.ipaddress shard 131/146", activity)
        self.assertIn("4m", activity)

    def test_sync_activity_marks_stale_progress_heartbeat(self):
        self.sync.status = "syncing"
        self.sync.save(update_fields=["status"])
        started = (timezone.now() - timedelta(minutes=31)).isoformat()
        run = ForwardExecutionRun.objects.create(
            sync=self.sync,
            source=self.source,
            backend="branching",
            status=ForwardExecutionRunStatusChoices.RUNNING,
            phase="executing",
            phase_message="Applying planned shard changes.",
            snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
            snapshot_id="snapshot-activity-stale",
            total_steps=144,
            next_step_index=16,
            latest_heartbeat=parse_datetime(started),
        )
        ForwardExecutionStep.objects.create(
            run=run,
            index=16,
            kind="stage",
            status=ForwardExecutionStepStatusChoices.RUNNING,
            model_string="dcim.interface",
            attempted_row_count=4828,
            fetched_row_count=4918,
            heartbeat=parse_datetime(started),
        )

        activity = get_sync_activity(self.sync)

        self.assertIn("No shard progress reported", activity)
        self.assertIn("dcim.interface", activity)

    def test_touch_branch_run_progress_updates_state(self):
        execution_run = ForwardExecutionRun.objects.create(
            sync=self.sync,
            source=self.source,
            backend="branching",
            status=ForwardExecutionRunStatusChoices.RUNNING,
            phase="executing",
            phase_message="Applying planned shard changes.",
            snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
            snapshot_id="snapshot-touch-ledger",
            total_steps=146,
            next_step_index=131,
        )
        step = ForwardExecutionStep.objects.create(
            run=execution_run,
            index=131,
            kind="stage",
            status=ForwardExecutionStepStatusChoices.RUNNING,
            model_string="ipam.ipaddress",
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

        self.assertTrue(touched)
        execution_run.refresh_from_db()
        step.refresh_from_db()
        self.assertEqual(
            execution_run.phase_message,
            "Applying shard 131/146 for ipam.ipaddress.",
        )
        self.assertEqual(step.attempted_row_count, 10)
        self.assertEqual(step.fetched_row_count, 12000)

    def test_touch_branch_run_progress_updates_execution_ledger_without_json(self):
        execution_run = ForwardExecutionRun.objects.create(
            sync=self.sync,
            source=self.source,
            backend="branching",
            status=ForwardExecutionRunStatusChoices.RUNNING,
            phase="executing",
            phase_message="Applying planned shard changes.",
            snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
            snapshot_id="snapshot-ledger-progress",
            total_steps=4,
            next_step_index=2,
        )
        step = ForwardExecutionStep.objects.create(
            run=execution_run,
            index=2,
            kind="stage",
            status=ForwardExecutionStepStatusChoices.RUNNING,
            model_string="ipam.ipaddress",
            attempted_row_count=1,
            fetched_row_count=2,
        )
        clear_branch_run_state(self.sync)
        initial_parameters = dict(self.sync.parameters or {})

        touched = touch_branch_run_progress(
            self.sync,
            phase_message="Applying shard 2/4 for ipam.ipaddress.",
            model_string="ipam.ipaddress",
            shard_index=2,
            total_plan_items=4,
            row_count=10,
            row_total=12000,
        )

        step.refresh_from_db()
        execution_run.refresh_from_db()
        self.assertTrue(touched)
        self.assertEqual(
            execution_run.phase_message, "Applying shard 2/4 for ipam.ipaddress."
        )
        self.assertEqual(execution_run.next_step_index, 2)
        self.assertEqual(execution_run.total_steps, 4)
        self.assertTrue(execution_run.latest_heartbeat)
        self.assertEqual(step.attempted_row_count, 10)
        self.assertEqual(step.fetched_row_count, 12000)
        self.assertEqual(self.sync.parameters, initial_parameters)

    def test_mark_branch_run_failed_clears_stale_progress(self):
        self.assertFalse(mark_branch_run_failed(self.sync, "Forward ingestion failed."))

    def test_mark_branch_run_failed_updates_execution_ledger_without_json(self):
        execution_run = ForwardExecutionRun.objects.create(
            sync=self.sync,
            source=self.source,
            backend="branching",
            status=ForwardExecutionRunStatusChoices.RUNNING,
            phase="executing",
            phase_message="Applying planned shard changes.",
            snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
            snapshot_id="snapshot-ledger-failed",
            total_steps=1,
            next_step_index=1,
        )
        ForwardExecutionStep.objects.create(
            run=execution_run,
            index=1,
            kind="stage",
            status=ForwardExecutionStepStatusChoices.RUNNING,
            model_string="dcim.module",
        )
        clear_branch_run_state(self.sync)
        initial_parameters = dict(self.sync.parameters or {})

        self.assertTrue(mark_branch_run_failed(self.sync, "Forward ingestion failed."))

        execution_run.refresh_from_db()
        self.assertEqual(execution_run.status, ForwardExecutionRunStatusChoices.FAILED)
        self.assertEqual(execution_run.phase, "failed")
        self.assertEqual(execution_run.phase_message, "Forward ingestion failed.")
        self.assertEqual(execution_run.last_error, "Forward ingestion failed.")
        self.assertTrue(execution_run.latest_heartbeat)
        self.assertEqual(self.sync.parameters, initial_parameters)

    def test_update_run_from_branch_state_uses_ledger_display_state_when_json_missing(
        self,
    ):
        execution_run = ForwardExecutionRun.objects.create(
            sync=self.sync,
            source=self.source,
            backend="branching",
            status=ForwardExecutionRunStatusChoices.RUNNING,
            phase="executing",
            phase_message="Applying planned shard changes.",
            snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
            snapshot_id="snapshot-ledger-update",
            total_steps=1,
            next_step_index=1,
        )
        clear_branch_run_state(self.sync)

        refreshed = update_run_from_branch_state(self.sync)

        execution_run.refresh_from_db()
        self.assertIsNotNone(refreshed)
        self.assertEqual(execution_run.phase, "executing")
        self.assertEqual(
            execution_run.phase_message,
            "Applying planned shard changes.",
        )
        self.assertEqual(execution_run.next_step_index, 1)

    def test_set_runtime_phase_clears_previous_row_progress(self):
        execution_run = ForwardExecutionRun.objects.create(
            sync=self.sync,
            source=self.source,
            backend="branching",
            status=ForwardExecutionRunStatusChoices.RUNNING,
            phase="executing",
            phase_message="Fast bootstrap applying dcim.module (17/24).",
            snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
            snapshot_id="snapshot-runtime-phase",
            total_steps=24,
            next_step_index=17,
        )
        executor = Mock(sync=self.sync, logger=Mock())

        set_runtime_phase(
            executor,
            "executing",
            "Fast bootstrap applying netbox_routing.bgppeer (18/24).",
            next_plan_index=18,
            total_plan_items=24,
        )

        execution_run.refresh_from_db()
        self.assertEqual(
            execution_run.phase_message,
            "Fast bootstrap applying netbox_routing.bgppeer (18/24).",
        )
        self.assertIn("netbox_routing.bgppeer", get_sync_activity(self.sync))

    def test_set_runtime_phase_updates_execution_ledger_without_json(self):
        execution_run = ForwardExecutionRun.objects.create(
            sync=self.sync,
            source=self.source,
            backend="branching",
            status=ForwardExecutionRunStatusChoices.RUNNING,
            phase="planning",
            phase_message="Resolving snapshot.",
            snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
            snapshot_id="snapshot-phase-ledger",
            total_steps=1,
            next_step_index=1,
        )
        executor = Mock(sync=self.sync, logger=Mock())
        clear_branch_run_state(self.sync)
        initial_parameters = dict(self.sync.parameters or {})

        set_runtime_phase(
            executor,
            "executing",
            "Applying planned shard changes.",
            next_plan_index=1,
            total_plan_items=1,
        )

        execution_run.refresh_from_db()
        self.assertEqual(execution_run.phase, "executing")
        self.assertEqual(
            execution_run.phase_message,
            "Applying planned shard changes.",
        )
        self.assertEqual(execution_run.next_step_index, 1)
        self.assertEqual(execution_run.total_steps, 1)
        self.assertTrue(execution_run.latest_heartbeat)
        self.assertEqual(self.sync.parameters, initial_parameters)
