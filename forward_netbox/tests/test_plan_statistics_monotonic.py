# A model split across several plan items (a 70k-row table like
# netbox_dlm.vulnerability routinely exceeds the staging-item budget) used to
# have its progress `total` grow in installments as each item's own
# `estimated_changes` landed right before that item applied - so a later
# item's total could arrive while `current` was already most of the way to
# the PRIOR, smaller total, dropping the displayed percentage before it
# climbed back up. `initialize_plan_statistics` sums every item's
# `estimated_changes` per model before any item runs, so the total is set
# once and `current` climbs to it monotonically.
from types import SimpleNamespace
from unittest.mock import patch

from django.test import SimpleTestCase

from forward_netbox.utilities.branch_budget import BranchPlanItem
from forward_netbox.utilities.branch_lifecycle import initialize_plan_statistics
from forward_netbox.utilities.logging import SyncLogging


def _plan_item(index, model_string, estimated_changes):
    return BranchPlanItem(
        index=index,
        model_string=model_string,
        label=model_string,
        estimated_changes=estimated_changes,
        upsert_rows=[{}] * estimated_changes,
        delete_rows=[],
        sync_mode="incremental",
    )


class InitializePlanStatisticsTest(SimpleTestCase):
    @patch("forward_netbox.utilities.logging.Job.objects.filter")
    def test_a_split_models_total_is_the_sum_across_every_item(self, _mock_filter):
        executor = SimpleNamespace(logger=SyncLogging(job=None))
        plan = [
            _plan_item(0, "netbox_dlm.vulnerability", 30_000),
            _plan_item(1, "netbox_dlm.vulnerability", 40_000),
            _plan_item(2, "dcim.device", 5),
        ]

        initialize_plan_statistics(executor, plan)

        stats = executor.logger.log_data["statistics"]
        self.assertEqual(stats["netbox_dlm.vulnerability"]["total"], 70_000)
        self.assertEqual(stats["dcim.device"]["total"], 5)

    @patch("forward_netbox.utilities.logging.Job.objects.filter")
    def test_current_never_regresses_the_ratio_once_initialized(self, _mock_filter):
        executor = SimpleNamespace(logger=SyncLogging(job=None))
        plan = [
            _plan_item(0, "netbox_dlm.vulnerability", 30_000),
            _plan_item(1, "netbox_dlm.vulnerability", 40_000),
        ]
        initialize_plan_statistics(executor, plan)
        logger = executor.logger

        # Item 0 applies: `current` climbs toward the FULL total (70,000),
        # not toward its own 30,000 slice - so the ratio only ever grows.
        logger.increment_statistics(
            "netbox_dlm.vulnerability", outcome="applied", amount=24_000
        )
        ratio_after_item_0 = (
            logger.log_data["statistics"]["netbox_dlm.vulnerability"]["current"]
            / logger.log_data["statistics"]["netbox_dlm.vulnerability"]["total"]
        )

        # Item 1 applies next. Before this fix, starting item 1 would itself
        # bump `total` by another 40,000 here - the drop this test guards
        # against. Now the total was already set once, so nothing changes.
        self.assertEqual(
            logger.log_data["statistics"]["netbox_dlm.vulnerability"]["total"],
            70_000,
        )
        logger.increment_statistics(
            "netbox_dlm.vulnerability", outcome="applied", amount=39_000
        )
        ratio_after_item_1 = (
            logger.log_data["statistics"]["netbox_dlm.vulnerability"]["current"]
            / logger.log_data["statistics"]["netbox_dlm.vulnerability"]["total"]
        )

        self.assertGreater(ratio_after_item_1, ratio_after_item_0)

    @patch("forward_netbox.utilities.logging.Job.objects.filter")
    def test_marks_every_model_as_initialized(self, _mock_filter):
        executor = SimpleNamespace(logger=SyncLogging(job=None))
        plan = [_plan_item(0, "ipam.prefix", 10)]

        initialize_plan_statistics(executor, plan)

        self.assertEqual(executor._statistics_initialized_models, {"ipam.prefix"})
