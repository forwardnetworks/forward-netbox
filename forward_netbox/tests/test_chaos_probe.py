from types import SimpleNamespace

from django.test import SimpleTestCase
from django.test import TestCase
from django.utils import timezone

from forward_netbox.management.commands.forward_chaos_probe import Command
from forward_netbox.models import ForwardExecutionRun
from forward_netbox.models import ForwardExecutionStep
from forward_netbox.models import ForwardSource
from forward_netbox.models import ForwardSync


class ForwardChaosProbeCommandTest(SimpleTestCase):
    def setUp(self):
        self.command = Command()

    def test_stage_during_apply_accepts_attempted_rows(self):
        step = SimpleNamespace(
            status="running",
            applied_row_count=0,
            attempted_row_count=7000,
            fetched_row_count=8614,
        )
        self.assertTrue(self.command._is_ready(step, "stage-during-apply"))

    def test_stage_during_apply_rejects_non_running_steps(self):
        step = SimpleNamespace(
            status="pending",
            applied_row_count=100,
            attempted_row_count=100,
            fetched_row_count=100,
        )
        self.assertFalse(self.command._is_ready(step, "stage-during-apply"))

    def test_stage_after_branch_requires_branch_name(self):
        queued_without_branch = SimpleNamespace(status="queued", branch_name="")
        queued_with_branch = SimpleNamespace(status="queued", branch_name="branch_abc")
        self.assertFalse(
            self.command._is_ready(queued_without_branch, "stage-after-branch")
        )
        self.assertTrue(
            self.command._is_ready(queued_with_branch, "stage-after-branch")
        )


class ForwardChaosProbeFixtureTest(TestCase):
    def setUp(self):
        self.command = Command()
        self.source = ForwardSource.objects.create(
            name="chaos-probe-source",
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
            name="chaos-probe-sync",
            source=self.source,
            parameters={"dcim.site": True},
        )
        self.run = ForwardExecutionRun.objects.create(
            sync=self.sync,
            source=self.source,
            backend="branching",
            status="completed",
            phase="completed",
            total_steps=1,
            next_step_index=2,
            completed=timezone.now(),
        )
        self.step = ForwardExecutionStep.objects.create(
            run=self.run,
            index=1,
            kind="stage",
            status="merged",
            model_string="dcim.site",
            branch_name="old-branch",
            fetched_row_count=0,
            attempted_row_count=0,
        )

    def test_prepare_fixture_sets_stage_before_branch_readiness(self):
        self.command._prepare_scenario_fixture(self.sync, "stage-before-branch")

        self.run.refresh_from_db()
        self.step.refresh_from_db()

        self.assertEqual(self.run.status, "running")
        self.assertEqual(self.run.phase, "staging")
        self.assertEqual(self.step.status, "running")
        self.assertEqual(self.step.branch_name, "")
        self.assertTrue(self.command._is_ready(self.step, "stage-before-branch"))

    def test_prepare_fixture_sets_stage_during_apply_readiness(self):
        self.command._prepare_scenario_fixture(self.sync, "stage-during-apply")

        self.step.refresh_from_db()

        self.assertEqual(self.step.status, "running")
        self.assertGreaterEqual(self.step.fetched_row_count, 1)
        self.assertGreaterEqual(self.step.attempted_row_count, 1)
        self.assertTrue(self.command._is_ready(self.step, "stage-during-apply"))
