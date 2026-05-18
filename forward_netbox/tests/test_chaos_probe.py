from types import SimpleNamespace

from django.test import SimpleTestCase

from forward_netbox.management.commands.forward_chaos_probe import Command


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
