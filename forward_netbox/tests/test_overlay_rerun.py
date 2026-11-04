# The primary-IP step lives in a run's branch stage, which a run with no Forward
# changes skips. A customer turned on a primary-IP setting, re-ran on the same
# snapshot, and nothing happened. Changing a setting now makes the next run stage
# the step, once. Synthetic data.
from unittest.mock import Mock
from unittest.mock import patch

from django.contrib.auth import get_user_model
from django.test import SimpleTestCase
from django.test import TestCase
from django.test import TransactionTestCase

from forward_netbox.models import ForwardSource
from forward_netbox.models import ForwardSync
from forward_netbox.utilities.primary_ip import overlay_rerun_needed
from forward_netbox.utilities.single_branch_executor import (
    ForwardSingleBranchExecutor,
)

FALLBACK = "set_primary_ip_from_forward_management_ip"
TAG = "set_primary_ip_from_mgmt_tag"
COPIES = "give_contexts_their_own_management_ip"


class OverlayMarkerRuleTest(SimpleTestCase):
    def test_turning_a_setting_on_asks_for_the_step(self):
        self.assertTrue(
            overlay_rerun_needed({FALLBACK: True}, {FALLBACK: True, COPIES: True})
        )
        self.assertTrue(overlay_rerun_needed({}, {FALLBACK: True}))
        self.assertTrue(overlay_rerun_needed({TAG: False}, {TAG: True}))

    def test_saving_without_changing_one_does_not(self):
        same = {FALLBACK: True, COPIES: True}
        self.assertFalse(overlay_rerun_needed(dict(same), dict(same)))

    def test_nothing_is_pending_when_no_primary_ip_source_is_on(self):
        self.assertFalse(overlay_rerun_needed({FALLBACK: True}, {FALLBACK: False}))
        self.assertFalse(overlay_rerun_needed({}, {COPIES: True}))

    def test_a_pending_marker_survives_an_unrelated_save(self):
        same = {FALLBACK: True}
        self.assertTrue(
            overlay_rerun_needed({**same, "overlay_rerun_pending": True}, dict(same))
        )

    def test_turning_a_setting_off_while_another_stays_on_is_a_change(self):
        self.assertTrue(
            overlay_rerun_needed({FALLBACK: True, COPIES: True}, {FALLBACK: True})
        )


class ExecutorMarkerTest(TestCase):
    def setUp(self):
        source = ForwardSource.objects.create(
            name="or-src",
            type="saas",
            url="https://fwd.app",
            parameters={"network_id": "net-1"},
        )
        self.sync = ForwardSync.objects.create(
            name="or-sync",
            source=source,
            parameters={FALLBACK: True, "overlay_rerun_pending": True},
        )

    def _executor(self):
        return ForwardSingleBranchExecutor(
            self.sync, Mock(), Mock(), user=None, job=None
        )

    def test_pending_is_seen_only_with_a_primary_ip_source_on(self):
        self.assertTrue(self._executor()._overlay_rerun_pending())
        self.sync.parameters = {"overlay_rerun_pending": True}
        self.assertFalse(self._executor()._overlay_rerun_pending())

    def test_clearing_removes_the_marker_in_memory_and_in_the_database(self):
        executor = self._executor()

        executor._clear_overlay_rerun_pending()

        self.assertNotIn("overlay_rerun_pending", self.sync.parameters)
        self.sync.refresh_from_db()
        self.assertNotIn("overlay_rerun_pending", self.sync.parameters)
        self.assertTrue(self.sync.parameters[FALLBACK])

    def test_clearing_when_nothing_is_pending_leaves_the_parameters_alone(self):
        self.sync.parameters = {FALLBACK: True}
        self.sync.save()
        executor = self._executor()

        executor._clear_overlay_rerun_pending()

        self.sync.refresh_from_db()
        # Saving a sync fills in defaults, so compare what matters, not the dict.
        self.assertTrue(self.sync.parameters[FALLBACK])
        self.assertNotIn("overlay_rerun_pending", self.sync.parameters)


class ForcedNoopRunTest(TransactionTestCase):
    """A run Forward has nothing new for still stages the primary-IP step when a
    setting changed, merges the (possibly empty) branch, and clears the marker."""

    def setUp(self):
        self.user = get_user_model().objects.create_user(username="or-user")
        source = ForwardSource.objects.create(
            name="or2-src",
            type="saas",
            url="https://fwd.app",
            parameters={"network_id": "net-1"},
        )
        self.sync = ForwardSync.objects.create(
            name="or2-sync",
            source=source,
            user=self.user,
            auto_merge=True,
            parameters={
                FALLBACK: True,
                COPIES: True,
                "overlay_rerun_pending": True,
                "snapshot_id": "latestProcessed",
            },
        )

    def _run(self, *, pending):
        from unittest.mock import MagicMock

        if not pending:
            self.sync.parameters = {
                key: value
                for key, value in self.sync.parameters.items()
                if key != "overlay_rerun_pending"
            }
            self.sync.save()
        context = MagicMock()
        context.as_dict.return_value = {
            "snapshot_selector": "latestProcessed",
            "snapshot_id": "snap-1",
            "snapshot_info": {},
            "snapshot_metrics": {},
        }
        context.scoped_device_names = set()
        context.device_tag_include_tags = []
        context.device_tag_exclude_tags = []
        fetcher = MagicMock()
        fetcher.resolve_context.return_value = context
        fetcher.fetch_workloads.return_value = []
        fetcher.model_results = []
        fetcher.pending_workload_states = {}
        fetcher.stage_pending_contributor_baseline.return_value = 0
        base = "forward_netbox.utilities.single_branch_executor."
        with (
            patch(base + "ForwardQueryFetcher", return_value=fetcher),
            patch(base + "ForwardValidationRunner") as runner,
            patch(base + "apply_primary_ip_from_mgmt_tags") as step,
            patch(base + "stage_and_promote_noop_workload_states", return_value=0),
        ):
            from forward_netbox.models import ForwardValidationRun

            run = ForwardValidationRun.objects.create(sync=self.sync)
            runner.return_value.start_run.return_value = run
            runner.return_value.record_plan_validation.return_value = run
            executor = ForwardSingleBranchExecutor(
                self.sync, Mock(), Mock(), user=self.user, job=None
            )
            ingestions = executor.run()
        return ingestions, step

    def test_a_pending_setting_stages_the_step_even_with_no_data_changes(self):
        ingestions, step = self._run(pending=True)

        from netbox_branching.models import Branch

        step.assert_called_once()
        # A branch was provisioned for the step and removed by the auto-merge:
        # the run completed instead of stopping at the no-op shortcut.
        self.assertEqual(len(ingestions), 1)
        self.assertEqual(Branch.objects.count(), 0)
        self.sync.refresh_from_db()
        self.assertNotIn("overlay_rerun_pending", self.sync.parameters)

    def test_without_the_marker_a_no_change_run_stays_branchless(self):
        ingestions, step = self._run(pending=False)

        step.assert_not_called()
        self.assertIsNone(ingestions[0].branch_id)


class MarkerClearedFromStoredParametersTest(TestCase):
    def test_a_form_saved_mid_run_is_not_reverted_by_clearing(self):
        source = ForwardSource.objects.create(
            name="mc-src",
            type="saas",
            url="https://fwd.app",
            parameters={"network_id": "net-1"},
        )
        sync = ForwardSync.objects.create(
            name="mc-sync",
            source=source,
            parameters={FALLBACK: True, "overlay_rerun_pending": True},
        )
        stale = ForwardSync.objects.get(pk=sync.pk)
        # The operator saves a new setting while the run holds `stale`.
        ForwardSync.objects.filter(pk=sync.pk).update(
            parameters={FALLBACK: True, COPIES: True, "overlay_rerun_pending": True}
        )
        executor = ForwardSingleBranchExecutor(
            stale, Mock(), Mock(), user=None, job=None
        )

        executor._clear_overlay_rerun_pending()

        sync.refresh_from_db()
        self.assertTrue(sync.parameters[COPIES])
        self.assertNotIn("overlay_rerun_pending", sync.parameters)
