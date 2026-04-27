from unittest.mock import Mock
from unittest.mock import patch

from django.test import TestCase

from forward_netbox.exceptions import ForwardQueryError
from forward_netbox.models import ForwardIngestion
from forward_netbox.models import ForwardSource
from forward_netbox.models import ForwardSync
from forward_netbox.tests import scenarios
from forward_netbox.utilities.branch_budget import build_branch_plan
from forward_netbox.utilities.forward_api import LATEST_PROCESSED_SNAPSHOT
from forward_netbox.utilities.multi_branch import BranchBudgetExceeded
from forward_netbox.utilities.multi_branch import DEFAULT_PREFLIGHT_ROW_LIMIT
from forward_netbox.utilities.multi_branch import ForwardMultiBranchExecutor
from forward_netbox.utilities.multi_branch import ForwardMultiBranchPlanner
from forward_netbox.utilities.query_registry import QuerySpec
from forward_netbox.utilities.sync import ForwardSyncRunner


class SyntheticSyncScenarioHarnessTest(TestCase):
    def setUp(self):
        self.source = ForwardSource.objects.create(
            name="source-synthetic-scenarios",
            type="saas",
            url="https://fwd.app",
            parameters=scenarios.source_parameters(),
        )
        self.sync = ForwardSync.objects.create(
            name="sync-synthetic-scenarios",
            source=self.source,
            auto_merge=True,
            parameters={
                "snapshot_id": LATEST_PROCESSED_SNAPSHOT,
                "dcim.site": True,
            },
        )

    def test_large_interface_import_splits_without_live_data(self):
        workload = scenarios.branch_workload(
            "dcim.interface",
            scenarios.interface_rows(device_count=3, interfaces_per_device=4),
            coalesce_fields=[["device", "name"]],
        )

        plan = build_branch_plan([workload], max_changes_per_branch=5)

        self.assertEqual(sum(item.estimated_changes for item in plan), 12)
        self.assertGreater(len(plan), 1)
        self.assertTrue(all(item.estimated_changes <= 5 for item in plan))

    @patch("forward_netbox.utilities.query_fetch.get_query_specs")
    def test_bad_model_rows_fail_during_preflight(self, mock_specs):
        client = Mock()
        client.get_snapshots.return_value = [scenarios.snapshot()]
        client.get_snapshot_metrics.return_value = {}
        client.run_nqe_query.return_value = scenarios.invalid_site_rows()
        self.sync.resolve_snapshot_id = lambda client=None: scenarios.SNAPSHOT_AFTER
        self.sync.get_model_strings = lambda: ["dcim.site"]
        self.sync.incremental_diff_baseline = Mock(return_value=None)
        mock_specs.return_value = [
            QuerySpec(
                model_string="dcim.site",
                query_name="Forward Sites",
                query='select {name: "site-without-slug"}',
            )
        ]
        planner = ForwardMultiBranchPlanner(
            sync=self.sync,
            client=client,
            logger_=Mock(),
        )

        with self.assertRaisesRegex(ForwardQueryError, "missing required fields: slug"):
            planner.build_plan(max_changes_per_branch=10, run_preflight=True)

        client.run_nqe_query.assert_called_once()
        self.assertEqual(
            client.run_nqe_query.call_args.kwargs["limit"],
            DEFAULT_PREFLIGHT_ROW_LIMIT,
        )

    @patch("forward_netbox.utilities.sync.get_query_specs")
    def test_diff_scenario_routes_upserts_and_deletes(self, mock_specs):
        baseline = ForwardIngestion.objects.create(
            sync=self.sync,
            snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
            snapshot_id=scenarios.SNAPSHOT_BEFORE,
            baseline_ready=True,
        )
        ingestion = ForwardIngestion.objects.create(sync=self.sync)
        client = Mock()
        client.get_latest_processed_snapshot.return_value = scenarios.snapshot(
            scenarios.SNAPSHOT_AFTER
        )
        client.get_snapshot_metrics.return_value = {}
        client.run_nqe_diff.return_value = scenarios.diff_rows()
        mock_specs.return_value = [
            QuerySpec(
                model_string="dcim.site",
                query_name="Forward Sites",
                query_id="Q_sites",
            )
        ]
        runner = ForwardSyncRunner(
            sync=self.sync,
            ingestion=ingestion,
            client=client,
            logger_=Mock(),
        )
        runner._apply_model_rows = Mock()
        runner._delete_model_rows = Mock()
        self.sync.get_model_strings = lambda: ["dcim.site"]
        self.sync.resolve_snapshot_id = lambda client=None: scenarios.SNAPSHOT_AFTER

        runner.run()

        client.run_nqe_diff.assert_called_once_with(
            query_id="Q_sites",
            commit_id=None,
            before_snapshot_id=baseline.snapshot_id,
            after_snapshot_id=scenarios.SNAPSHOT_AFTER,
            fetch_all=True,
        )
        runner._apply_model_rows.assert_called_once_with(
            "dcim.site",
            [
                {"name": "site-added", "slug": "site-added"},
                {"name": "site-new", "slug": "site-modified"},
            ],
        )
        runner._delete_model_rows.assert_called_once_with(
            "dcim.site",
            [{"name": "site-deleted", "slug": "site-deleted"}],
        )

    def test_branch_overflow_scenario_splits_and_retries(self):
        workload = scenarios.branch_workload(
            "dcim.device",
            [{"name": f"device-{index}"} for index in range(8)],
            coalesce_fields=[["name"]],
        )
        oversized_item = build_branch_plan([workload], max_changes_per_branch=10)[0]
        split_items = build_branch_plan([workload], max_changes_per_branch=4)
        executor = ForwardMultiBranchExecutor(
            sync=self.sync,
            client=Mock(),
            logger_=Mock(),
        )
        executor.plan = Mock(
            return_value=(
                {
                    "snapshot_selector": LATEST_PROCESSED_SNAPSHOT,
                    "snapshot_id": scenarios.SNAPSHOT_AFTER,
                    "snapshot_info": {},
                    "snapshot_metrics": {},
                },
                [oversized_item],
            )
        )
        executor._record_model_density = Mock()
        executor._cleanup_overflow_branch = Mock()
        executor._split_overflow_item = Mock(return_value=split_items)
        executor._run_plan_item = Mock(
            side_effect=[
                BranchBudgetExceeded(
                    item=oversized_item,
                    actual_changes=25,
                    budget=10,
                    branch=None,
                    ingestion=None,
                ),
                Mock(name="ingestion-1"),
                Mock(name="ingestion-2"),
            ]
        )

        ingestions = executor.run(max_changes_per_branch=10)

        self.assertEqual(len(ingestions), 2)
        self.assertEqual(executor._run_plan_item.call_count, 3)
        executor._split_overflow_item.assert_called_once_with(oversized_item)
