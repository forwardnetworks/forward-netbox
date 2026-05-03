from unittest.mock import Mock
from unittest.mock import patch

from dcim.models import Cable
from dcim.models import Device
from dcim.models import DeviceRole
from dcim.models import DeviceType
from dcim.models import Interface
from dcim.models import Manufacturer
from dcim.models import Site
from dcim.models import VirtualChassis
from django.core.exceptions import ValidationError
from django.db import IntegrityError
from django.test import TestCase
from extras.models import Tag

from forward_netbox.choices import FORWARD_SUPPORTED_MODELS
from forward_netbox.exceptions import ForwardClientError
from forward_netbox.exceptions import ForwardDependencySkipError
from forward_netbox.exceptions import ForwardQueryError
from forward_netbox.exceptions import ForwardSyncDataError
from forward_netbox.exceptions import ForwardSyncError
from forward_netbox.models import ForwardDriftPolicy
from forward_netbox.models import ForwardIngestion
from forward_netbox.models import ForwardIngestionIssue
from forward_netbox.models import ForwardSource
from forward_netbox.models import ForwardSync
from forward_netbox.models import ForwardValidationRun
from forward_netbox.utilities.branch_budget import BranchWorkload
from forward_netbox.utilities.branch_budget import build_branch_plan
from forward_netbox.utilities.branch_budget import build_branch_plan_with_density
from forward_netbox.utilities.branch_budget import effective_row_budget_for_model
from forward_netbox.utilities.branch_budget import row_shard_key
from forward_netbox.utilities.forward_api import LATEST_PROCESSED_SNAPSHOT
from forward_netbox.utilities.multi_branch import BranchBudgetExceeded
from forward_netbox.utilities.multi_branch import DEFAULT_PREFLIGHT_ROW_LIMIT
from forward_netbox.utilities.multi_branch import ForwardMultiBranchExecutor
from forward_netbox.utilities.multi_branch import ForwardMultiBranchPlanner
from forward_netbox.utilities.query_registry import QuerySpec
from forward_netbox.utilities.sync import EventsClearer
from forward_netbox.utilities.sync import ForwardSyncRunner
from forward_netbox.utilities.sync_contracts import validate_row_shape_for_model


class ForwardBranchBudgetPlanTest(TestCase):
    def test_under_budget_workload_uses_one_branch(self):
        workload = BranchWorkload(
            model_string="dcim.interface",
            label="interfaces",
            upsert_rows=[
                {"device": "device-1", "name": "Ethernet1/1"},
                {"device": "device-2", "name": "Ethernet1/1"},
            ],
            coalesce_fields=[["device", "name"]],
        )

        plan = build_branch_plan([workload], max_changes_per_branch=10)

        self.assertEqual(len(plan), 1)
        self.assertEqual(plan[0].estimated_changes, 2)

    def test_large_device_keyed_workload_is_split_deterministically(self):
        rows = [
            {"device": f"device-{index // 2}", "name": f"Ethernet1/{index}"}
            for index in range(12)
        ]
        workload = BranchWorkload(
            model_string="dcim.interface",
            label="interfaces",
            upsert_rows=rows,
            coalesce_fields=[["device", "name"]],
        )

        plan_a = build_branch_plan([workload], max_changes_per_branch=5)
        plan_b = build_branch_plan([workload], max_changes_per_branch=5)

        self.assertEqual(
            [item.estimated_changes for item in plan_a],
            [item.estimated_changes for item in plan_b],
        )
        self.assertTrue(all(item.estimated_changes <= 5 for item in plan_a))
        self.assertEqual(sum(item.estimated_changes for item in plan_a), 12)

    def test_oversized_single_device_bucket_fails(self):
        workload = BranchWorkload(
            model_string="dcim.interface",
            label="interfaces",
            upsert_rows=[
                {"device": "device-1", "name": f"Ethernet1/{index}"}
                for index in range(6)
            ],
            coalesce_fields=[["device", "name"]],
        )

        with self.assertRaisesRegex(
            ForwardQueryError,
            "device:device-1.*exceeds the branch budget",
        ):
            build_branch_plan([workload], max_changes_per_branch=5)

    def test_cable_shard_key_is_direction_insensitive(self):
        row = {
            "device": "device-a",
            "interface": "Ethernet1/1",
            "remote_device": "device-b",
            "remote_interface": "Ethernet1/2",
        }
        reversed_row = {
            "device": "device-b",
            "interface": "Ethernet1/2",
            "remote_device": "device-a",
            "remote_interface": "Ethernet1/1",
        }
        coalesce_fields = [["device", "interface", "remote_device", "remote_interface"]]

        self.assertEqual(
            row_shard_key("dcim.cable", row, coalesce_fields),
            row_shard_key("dcim.cable", reversed_row, coalesce_fields),
        )

    def test_effective_row_budget_scales_by_density(self):
        budget = effective_row_budget_for_model(
            "dcim.device",
            max_changes_per_branch=10000,
            model_change_density={"dcim.device": 5.0},
        )

        self.assertEqual(budget, 1400)

    def test_effective_row_budget_uses_cable_default_density_and_safety(self):
        budget = effective_row_budget_for_model(
            "dcim.cable",
            max_changes_per_branch=10000,
            model_change_density={},
        )

        self.assertEqual(budget, 1666)

    def test_effective_row_budget_uses_cable_safety_override_with_observed_density(
        self,
    ):
        budget = effective_row_budget_for_model(
            "dcim.cable",
            max_changes_per_branch=10000,
            model_change_density={"dcim.cable": 2.0},
        )

        self.assertEqual(budget, 2500)

    def test_build_branch_plan_with_density_splits_more_aggressively(self):
        rows = [{"name": f"device-{index}"} for index in range(12)]
        workload = BranchWorkload(
            model_string="dcim.device",
            label="devices",
            upsert_rows=rows,
            coalesce_fields=[["name"]],
        )

        default_plan = build_branch_plan([workload], max_changes_per_branch=10)
        density_plan = build_branch_plan_with_density(
            [workload],
            max_changes_per_branch=10,
            model_change_density={"dcim.device": 2.0},
        )

        self.assertEqual(len(default_plan), 2)
        self.assertEqual(len(density_plan), 4)
        self.assertTrue(all(item.estimated_changes <= 3 for item in density_plan))


class ForwardMultiBranchPlannerPreflightTest(TestCase):
    def setUp(self):
        self.source = ForwardSource.objects.create(
            name="source-preflight",
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
            name="sync-preflight",
            source=self.source,
            auto_merge=False,
            parameters={
                "snapshot_id": LATEST_PROCESSED_SNAPSHOT,
                "dcim.site": True,
            },
        )

    @patch("forward_netbox.utilities.query_fetch.get_query_specs")
    def test_build_plan_runs_query_preflight_before_fetching_full_rows(
        self, mock_specs
    ):
        client = Mock()
        client.get_snapshots.return_value = [
            {
                "id": "snapshot-after",
                "state": "PROCESSED",
                "created_at": "",
                "processed_at": "2026-03-31T12:15:00Z",
            }
        ]
        client.get_snapshot_metrics.return_value = {}
        client.run_nqe_query.side_effect = [
            [{"name": "site-1", "slug": "site-1"}],
            [{"name": "site-1", "slug": "site-1"}],
        ]
        self.sync.resolve_snapshot_id = lambda client=None: "snapshot-after"
        self.sync.get_model_strings = lambda: ["dcim.site"]
        self.sync.incremental_diff_baseline = Mock(return_value=None)
        mock_specs.return_value = [
            QuerySpec(
                model_string="dcim.site",
                query_name="Forward Sites",
                query='select {name: "site-1", slug: "site-1"}',
            )
        ]
        planner = ForwardMultiBranchPlanner(
            sync=self.sync,
            client=client,
            logger_=Mock(),
        )

        planner.build_plan(max_changes_per_branch=10, run_preflight=True)

        first_call = client.run_nqe_query.call_args_list[0]
        self.assertEqual(first_call.kwargs["limit"], DEFAULT_PREFLIGHT_ROW_LIMIT)
        self.assertFalse(first_call.kwargs["fetch_all"])
        second_call = client.run_nqe_query.call_args_list[1]
        self.assertTrue(second_call.kwargs["fetch_all"])

    @patch("forward_netbox.utilities.query_fetch.get_query_specs")
    def test_preflight_raises_before_full_fetch_on_invalid_rows(self, mock_specs):
        client = Mock()
        client.get_snapshots.return_value = [
            {
                "id": "snapshot-after",
                "state": "PROCESSED",
                "created_at": "",
                "processed_at": "2026-03-31T12:15:00Z",
            }
        ]
        client.get_snapshot_metrics.return_value = {}
        client.run_nqe_query.return_value = [{"name": "site-1"}]
        self.sync.resolve_snapshot_id = lambda client=None: "snapshot-after"
        self.sync.get_model_strings = lambda: ["dcim.site"]
        self.sync.incremental_diff_baseline = Mock(return_value=None)
        mock_specs.return_value = [
            QuerySpec(
                model_string="dcim.site",
                query_name="Forward Sites",
                query='select {name: "site-1"}',
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


class ForwardMultiBranchExecutorAdaptiveSplitTest(TestCase):
    NETWORK_ID = "test-network"
    SNAPSHOT_ID = "snapshot-under-test"

    def setUp(self):
        self.source = ForwardSource.objects.create(
            name="source-adaptive-split",
            type="saas",
            url="https://fwd.app",
            parameters={
                "username": "user@example.com",
                "password": "secret",
                "verify": True,
                "network_id": self.NETWORK_ID,
            },
        )
        self.sync = ForwardSync.objects.create(
            name="sync-adaptive-split",
            source=self.source,
            parameters={
                "snapshot_id": LATEST_PROCESSED_SNAPSHOT,
                "dcim.device": True,
            },
        )

    def test_split_overflow_item_uses_density_based_row_budget(self):
        workload = BranchWorkload(
            model_string="dcim.device",
            label="dcim.device | Forward Devices",
            upsert_rows=[{"name": f"device-{index}"} for index in range(20)],
            coalesce_fields=[["name"]],
        )
        item = build_branch_plan([workload], max_changes_per_branch=20)[0]
        executor = ForwardMultiBranchExecutor(
            sync=self.sync,
            client=Mock(),
            logger_=Mock(),
        )
        executor.max_changes_per_branch = 10
        executor.model_change_density = {"dcim.device": 5.0}

        split_items = executor._split_overflow_item(item)

        self.assertGreater(len(split_items), 1)
        self.assertTrue(all(part.estimated_changes <= 1 for part in split_items))

    def test_run_retries_when_branch_budget_exceeded(self):
        workload = BranchWorkload(
            model_string="dcim.device",
            label="dcim.device | Forward Devices",
            upsert_rows=[{"name": f"device-{index}"} for index in range(8)],
            coalesce_fields=[["name"]],
        )
        oversized_item = build_branch_plan([workload], max_changes_per_branch=10)[0]
        split_items = build_branch_plan([workload], max_changes_per_branch=4)

        executor = ForwardMultiBranchExecutor(
            sync=self.sync,
            client=Mock(),
            logger_=Mock(),
        )
        self.sync.auto_merge = True
        context = {
            "snapshot_selector": "latest",
            "snapshot_id": self.SNAPSHOT_ID,
            "snapshot_info": {},
            "snapshot_metrics": {},
        }
        executor.plan = Mock(return_value=(context, [oversized_item]))
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
        self.assertEqual(executor._split_overflow_item.call_count, 1)
        self.assertEqual(self.sync.get_branch_run_state(), {})

    def test_run_records_validation_and_model_results_before_noop_ingestion(self):
        executor = ForwardMultiBranchExecutor(
            sync=self.sync,
            client=Mock(),
            logger_=Mock(),
        )
        context = {
            "snapshot_selector": "latestProcessed",
            "snapshot_id": self.SNAPSHOT_ID,
            "snapshot_info": {"state": "PROCESSED"},
            "snapshot_metrics": {},
        }
        executor.plan = Mock(return_value=(context, []))
        executor.last_model_results = [
            {
                "model": "dcim.device",
                "query_name": "Forward Devices",
                "execution_mode": "query_id",
                "execution_value": "Q_devices",
                "sync_mode": "full",
                "row_count": 0,
                "delete_count": 0,
                "failure_count": 0,
                "runtime_ms": 1.0,
                "snapshot_id": self.SNAPSHOT_ID,
                "baseline_snapshot_id": "",
            }
        ]

        ingestions = executor.run(max_changes_per_branch=10)

        self.assertEqual(len(ingestions), 1)
        ingestion = ingestions[0]
        validation_run = ForwardValidationRun.objects.get(sync=self.sync)
        self.assertEqual(ingestion.validation_run, validation_run)
        self.assertEqual(ingestion.model_results, executor.last_model_results)
        self.assertTrue(validation_run.allowed)
        self.assertEqual(validation_run.snapshot_id, self.SNAPSHOT_ID)

    def test_zero_row_policy_blocks_before_branch_creation(self):
        policy = ForwardDriftPolicy.objects.create(
            name="block-empty-models",
            block_on_zero_rows=True,
        )
        self.sync.drift_policy = policy
        self.sync.save()
        executor = ForwardMultiBranchExecutor(
            sync=self.sync,
            client=Mock(),
            logger_=Mock(),
        )
        context = {
            "snapshot_selector": "latestProcessed",
            "snapshot_id": self.SNAPSHOT_ID,
            "snapshot_info": {"state": "PROCESSED"},
            "snapshot_metrics": {},
        }
        executor.plan = Mock(return_value=(context, []))
        executor._run_plan_item = Mock()

        with self.assertRaisesRegex(ForwardSyncError, "No rows were returned"):
            executor.run(max_changes_per_branch=10)

        executor._run_plan_item.assert_not_called()
        validation_run = ForwardValidationRun.objects.get(sync=self.sync)
        self.assertFalse(validation_run.allowed)
        self.assertEqual(validation_run.status, "blocked")


class ForwardSyncRunnerTest(TestCase):
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
            },
        )
        self.sync = ForwardSync.objects.create(
            name="sync-1",
            source=self.source,
            auto_merge=False,
            parameters={
                "snapshot_id": LATEST_PROCESSED_SNAPSHOT,
                "dcim.device": True,
            },
        )

    def _create_device(self, name):
        site, _ = Site.objects.get_or_create(name="site-1", slug="site-1")
        manufacturer, _ = Manufacturer.objects.get_or_create(
            name="vendor-1", slug="vendor-1"
        )
        role, _ = DeviceRole.objects.get_or_create(
            name="role-1", slug="role-1", defaults={"color": "9e9e9e"}
        )
        device_type, _ = DeviceType.objects.get_or_create(
            manufacturer=manufacturer,
            model="model-1",
            slug="model-1",
        )
        return Device.objects.create(
            name=name,
            site=site,
            role=role,
            device_type=device_type,
            status="active",
        )

    def test_lookup_interface_requires_exact_name(self):
        device = self._create_device("device-1")
        interface = Interface.objects.create(
            device=device,
            name="Ethernet1/1",
            type="1000base-t",
        )
        runner = ForwardSyncRunner(
            sync=self.sync, ingestion=None, client=None, logger_=Mock()
        )

        self.assertEqual(runner._lookup_interface(device, "Ethernet1/1"), interface)
        self.assertIsNone(runner._lookup_interface(device, "ethernet1/1"))

    def test_apply_extras_taggeditem_adds_feature_tag_to_device(self):
        device = self._create_device("device-1")
        runner = ForwardSyncRunner(
            sync=self.sync, ingestion=None, client=None, logger_=Mock()
        )

        runner._apply_extras_taggeditem(
            {
                "device": "device-1",
                "tag": "Prot_BGP",
                "tag_slug": "prot-bgp",
                "tag_color": "2196f3",
            }
        )

        tag = Tag.objects.get(slug="prot-bgp")
        self.assertEqual(tag.name, "Prot_BGP")
        self.assertIn(tag, device.tags.all())

    def test_apply_extras_taggeditem_reuses_existing_tag(self):
        device = self._create_device("device-1")
        tag = Tag.objects.create(name="BGP", slug="prot-bgp", color="9e9e9e")
        runner = ForwardSyncRunner(
            sync=self.sync, ingestion=None, client=None, logger_=Mock()
        )

        runner._apply_extras_taggeditem(
            {
                "device": "device-1",
                "tag": "Prot_BGP",
                "tag_slug": "prot-bgp",
                "tag_color": "2196f3",
            }
        )

        tag.refresh_from_db()
        self.assertEqual(tag.name, "Prot_BGP")
        self.assertEqual(tag.color, "2196f3")
        self.assertIn(tag, device.tags.all())

    def test_delete_extras_taggeditem_removes_tag_from_device(self):
        device = self._create_device("device-1")
        tag = Tag.objects.create(name="Prot_BGP", slug="prot-bgp", color="2196f3")
        device.tags.add(tag)
        runner = ForwardSyncRunner(
            sync=self.sync, ingestion=None, client=None, logger_=Mock()
        )
        row = {
            "device": "device-1",
            "tag": "Prot_BGP",
            "tag_slug": "prot-bgp",
            "tag_color": "2196f3",
        }

        self.assertTrue(runner._delete_extras_taggeditem(row))
        self.assertNotIn(tag, device.tags.all())
        self.assertFalse(runner._delete_extras_taggeditem(row))

    def test_apply_dcim_cable_creates_cable_between_interfaces(self):
        device = self._create_device("device-a")
        remote_device = self._create_device("device-b")
        interface = Interface.objects.create(
            device=device,
            name="Ethernet1/1",
            type="1000base-t",
        )
        remote_interface = Interface.objects.create(
            device=remote_device,
            name="Ethernet1/2",
            type="1000base-t",
        )
        runner = ForwardSyncRunner(
            sync=self.sync, ingestion=None, client=None, logger_=Mock()
        )

        runner._apply_dcim_cable(
            {
                "device": "device-a",
                "interface": "Ethernet1/1",
                "remote_device": "device-b",
                "remote_interface": "Ethernet1/2",
                "status": "connected",
            }
        )

        self.assertEqual(Cable.objects.count(), 1)
        interface.refresh_from_db()
        remote_interface.refresh_from_db()
        self.assertEqual(interface.cable_id, remote_interface.cable_id)

    def test_apply_dcim_cable_reuses_existing_reverse_cable(self):
        device = self._create_device("device-a")
        remote_device = self._create_device("device-b")
        Interface.objects.create(
            device=device,
            name="Ethernet1/1",
            type="1000base-t",
        )
        Interface.objects.create(
            device=remote_device,
            name="Ethernet1/2",
            type="1000base-t",
        )
        runner = ForwardSyncRunner(
            sync=self.sync, ingestion=None, client=None, logger_=Mock()
        )
        row = {
            "device": "device-a",
            "interface": "Ethernet1/1",
            "remote_device": "device-b",
            "remote_interface": "Ethernet1/2",
            "status": "connected",
        }

        runner._apply_dcim_cable(row)
        runner._apply_dcim_cable(
            {
                "device": "device-b",
                "interface": "Ethernet1/2",
                "remote_device": "device-a",
                "remote_interface": "Ethernet1/1",
                "status": "connected",
            }
        )

        self.assertEqual(Cable.objects.count(), 1)

    def test_apply_dcim_cable_skips_conflicting_existing_cable(self):
        device = self._create_device("device-a")
        remote_device = self._create_device("device-b")
        other_device = self._create_device("device-c")
        interface = Interface.objects.create(
            device=device,
            name="Ethernet1/1",
            type="1000base-t",
        )
        Interface.objects.create(
            device=remote_device,
            name="Ethernet1/2",
            type="1000base-t",
        )
        other_interface = Interface.objects.create(
            device=other_device,
            name="Ethernet1/3",
            type="1000base-t",
        )
        Cable(
            a_terminations=[interface],
            b_terminations=[other_interface],
            status="connected",
        ).save()
        logger = Mock()
        runner = ForwardSyncRunner(
            sync=self.sync, ingestion=None, client=None, logger_=logger
        )

        result = runner._apply_dcim_cable(
            {
                "device": "device-a",
                "interface": "Ethernet1/1",
                "remote_device": "device-b",
                "remote_interface": "Ethernet1/2",
                "status": "connected",
            }
        )

        self.assertFalse(result)
        self.assertEqual(Cable.objects.count(), 1)
        logger.log_warning.assert_called_once()

    def test_apply_dcim_cable_aggregates_conflict_warnings(self):
        device = self._create_device("device-a")
        remote_device = self._create_device("device-b")
        other_device = self._create_device("device-c")
        interface = Interface.objects.create(
            device=device,
            name="Ethernet1/1",
            type="1000base-t",
        )
        Interface.objects.create(
            device=remote_device,
            name="Ethernet1/2",
            type="1000base-t",
        )
        other_interface = Interface.objects.create(
            device=other_device,
            name="Ethernet1/3",
            type="1000base-t",
        )
        Cable(
            a_terminations=[interface],
            b_terminations=[other_interface],
            status="connected",
        ).save()
        logger = Mock()
        runner = ForwardSyncRunner(
            sync=self.sync, ingestion=None, client=None, logger_=logger
        )
        rows = [
            {
                "device": "device-a",
                "interface": "Ethernet1/1",
                "remote_device": "device-b",
                "remote_interface": "Ethernet1/2",
                "status": "connected",
            }
            for _ in range(ForwardSyncRunner.CONFLICT_WARNING_DETAIL_LIMIT + 3)
        ]

        runner._apply_model_rows("dcim.cable", rows)

        warning_messages = [call.args[0] for call in logger.log_warning.call_args_list]
        self.assertEqual(len(warning_messages), 21)
        self.assertEqual(
            warning_messages[-1],
            "Suppressed 3 additional dcim.cable conflict warnings for "
            "`interface-already-cabled` after the first 20.",
        )

    def test_apply_dcim_cable_skips_unknown_remote_device(self):
        device = self._create_device("device-a")
        Interface.objects.create(
            device=device,
            name="Ethernet1/1",
            type="1000base-t",
        )
        logger = Mock()
        runner = ForwardSyncRunner(
            sync=self.sync, ingestion=None, client=None, logger_=logger
        )

        result = runner._apply_dcim_cable(
            {
                "device": "device-a",
                "interface": "Ethernet1/1",
                "remote_device": "synthetic-node",
                "remote_interface": "Ethernet1/2",
                "status": "connected",
            }
        )

        self.assertFalse(result)
        self.assertEqual(Cable.objects.count(), 0)
        logger.log_warning.assert_called_once()

    def test_delete_dcim_cable_deletes_exact_cable(self):
        device = self._create_device("device-a")
        remote_device = self._create_device("device-b")
        Interface.objects.create(
            device=device,
            name="Ethernet1/1",
            type="1000base-t",
        )
        Interface.objects.create(
            device=remote_device,
            name="Ethernet1/2",
            type="1000base-t",
        )
        runner = ForwardSyncRunner(
            sync=self.sync, ingestion=None, client=None, logger_=Mock()
        )
        row = {
            "device": "device-a",
            "interface": "Ethernet1/1",
            "remote_device": "device-b",
            "remote_interface": "Ethernet1/2",
            "status": "connected",
        }
        runner._apply_dcim_cable(row)

        self.assertTrue(runner._delete_dcim_cable(row))
        self.assertEqual(Cable.objects.count(), 0)
        self.assertFalse(runner._delete_dcim_cable(row))

    def test_split_diff_rows_treats_reversed_cable_endpoints_as_same_identity(self):
        runner = ForwardSyncRunner(
            sync=self.sync, ingestion=None, client=None, logger_=Mock()
        )
        runner._model_coalesce_fields["dcim.cable"] = [
            ["device", "interface", "remote_device", "remote_interface"]
        ]

        upsert_rows, delete_rows = runner._split_diff_rows(
            "dcim.cable",
            [
                {
                    "type": "MODIFIED",
                    "before": {
                        "device": "device-a",
                        "interface": "Ethernet1/1",
                        "remote_device": "device-b",
                        "remote_interface": "Ethernet1/2",
                        "status": "connected",
                    },
                    "after": {
                        "device": "device-b",
                        "interface": "Ethernet1/2",
                        "remote_device": "device-a",
                        "remote_interface": "Ethernet1/1",
                        "status": "connected",
                    },
                }
            ],
        )

        self.assertEqual(len(upsert_rows), 1)
        self.assertEqual(delete_rows, [])

    def test_coalesce_lookup_ignores_null_and_empty_values(self):
        runner = ForwardSyncRunner(
            sync=self.sync, ingestion=None, client=None, logger_=Mock()
        )

        self.assertEqual(
            runner._coalesce_lookup(
                {"rd": None, "name": "blue", "description": ""},
                "rd",
                "name",
                "description",
            ),
            {"name": "blue"},
        )

    def test_validate_row_shape_allows_secondary_coalesce_when_primary_is_null(self):
        validate_row_shape_for_model(
            "ipam.vrf",
            {
                "name": "blue",
                "rd": None,
                "description": "",
                "enforce_unique": False,
            },
            [["rd"], ["name"]],
        )

    def test_validate_row_shape_allows_prefix_without_vrf(self):
        validate_row_shape_for_model(
            "ipam.prefix",
            {
                "prefix": "10.0.0.0/24",
                "vrf": None,
                "status": "active",
            },
            [["prefix", "vrf"], ["prefix"]],
        )

    def test_validate_row_shape_allows_ipaddress_without_vrf(self):
        validate_row_shape_for_model(
            "ipam.ipaddress",
            {
                "device": "device-1",
                "interface": "Ethernet1/1",
                "address": "10.0.0.1/24",
                "vrf": None,
                "status": "active",
            },
            [["address", "vrf"], ["address"]],
        )

    def test_validate_row_shape_allows_cable_endpoint_identity(self):
        validate_row_shape_for_model(
            "dcim.cable",
            {
                "device": "device-a",
                "interface": "Ethernet1/1",
                "remote_device": "device-b",
                "remote_interface": "Ethernet1/2",
                "status": "connected",
            },
            [["device", "interface", "remote_device", "remote_interface"]],
        )

    def test_validate_row_shape_allows_device_feature_tag_identity(self):
        validate_row_shape_for_model(
            "extras.taggeditem",
            {
                "device": "device-1",
                "tag": "Prot_BGP",
                "tag_slug": "prot-bgp",
                "tag_color": "2196f3",
            },
            [["device", "tag_slug"]],
        )

    def test_ensure_device_type_reuses_existing_slug_match(self):
        manufacturer = Manufacturer.objects.create(name="Cisco", slug="cisco")
        existing = DeviceType.objects.create(
            manufacturer=manufacturer,
            model="legacy-c4507",
            slug="ws-c4507r-e",
            part_number="legacy-c4507",
        )
        runner = ForwardSyncRunner(
            sync=self.sync, ingestion=None, client=None, logger_=Mock()
        )

        device_type = runner._ensure_device_type(
            {
                "manufacturer": "Cisco",
                "manufacturer_slug": "cisco",
                "model": "WS-C4507R-E",
                "slug": "ws-c4507r-e",
                "part_number": "WS-C4507R-E",
            }
        )
        existing.refresh_from_db()

        self.assertEqual(device_type.pk, existing.pk)
        self.assertEqual(existing.model, "WS-C4507R-E")
        self.assertEqual(existing.slug, "ws-c4507r-e")
        self.assertEqual(existing.part_number, "WS-C4507R-E")

    def test_ensure_manufacturer_reuses_existing_slug_conflict(self):
        Manufacturer.objects.create(name="Cisco Systems", slug="cisco")
        runner = ForwardSyncRunner(
            sync=self.sync, ingestion=None, client=None, logger_=Mock()
        )

        manufacturer = runner._ensure_manufacturer({"name": "Cisco", "slug": "cisco"})

        self.assertEqual(manufacturer.slug, "cisco")
        self.assertEqual(Manufacturer.objects.filter(slug="cisco").count(), 1)

    def test_ensure_role_reuses_existing_slug_conflict(self):
        DeviceRole.objects.create(name="Switches", slug="switch", color="9e9e9e")
        runner = ForwardSyncRunner(
            sync=self.sync, ingestion=None, client=None, logger_=Mock()
        )

        role = runner._ensure_role(
            {"name": "SWITCH", "slug": "switch", "color": "9e9e9e"}
        )

        self.assertEqual(role.slug, "switch")
        self.assertEqual(DeviceRole.objects.filter(slug="switch").count(), 1)

    def test_ensure_site_reuses_existing_slug_conflict(self):
        Site.objects.create(name="legacy-site", slug="site-1")
        runner = ForwardSyncRunner(
            sync=self.sync, ingestion=None, client=None, logger_=Mock()
        )

        site = runner._ensure_site({"name": "site-1", "slug": "site-1"})

        self.assertEqual(site.slug, "site-1")
        self.assertEqual(Site.objects.filter(slug="site-1").count(), 1)

    def test_ensure_device_type_rejects_conflicting_model_and_slug_matches(self):
        manufacturer = Manufacturer.objects.create(name="Cisco", slug="cisco")
        DeviceType.objects.create(
            manufacturer=manufacturer,
            model="WS-C4507R-E",
            slug="ws-c4507r-e-legacy",
        )
        DeviceType.objects.create(
            manufacturer=manufacturer,
            model="legacy-c4507",
            slug="ws-c4507r-e",
        )
        runner = ForwardSyncRunner(
            sync=self.sync, ingestion=None, client=None, logger_=Mock()
        )

        with self.assertRaisesMessage(
            ForwardQueryError,
            "Conflicting NetBox device types already exist",
        ):
            runner._ensure_device_type(
                {
                    "manufacturer": "Cisco",
                    "manufacturer_slug": "cisco",
                    "model": "WS-C4507R-E",
                    "slug": "ws-c4507r-e",
                    "part_number": "WS-C4507R-E",
                }
            )

    def test_non_lookup_models_remain_strict_on_integrity_errors(self):
        runner = ForwardSyncRunner(
            sync=self.sync, ingestion=None, client=None, logger_=Mock()
        )

        with patch(
            "dcim.models.Interface.full_clean",
            side_effect=IntegrityError("unique violation"),
        ):
            with self.assertRaises(IntegrityError):
                runner._update_existing_or_create(
                    Interface,
                    lookup={"name": "Ethernet1/1", "device_id": 999999},
                    defaults={"type": "1000base-t", "enabled": True},
                    conflict_policy=runner._conflict_policy("dcim.interface"),
                )

    def test_non_lookup_models_raise_validation_errors_from_full_clean(self):
        runner = ForwardSyncRunner(
            sync=self.sync, ingestion=None, client=None, logger_=Mock()
        )

        with self.assertRaises(ValidationError):
            runner._update_existing_or_create(
                Interface,
                lookup={"name": "Ethernet1/1", "device_id": 999999},
                defaults={"type": "1000base-t", "enabled": True},
                conflict_policy=runner._conflict_policy("dcim.interface"),
            )

    def test_apply_device_uses_manufacturer_specific_device_type(self):
        Manufacturer.objects.create(name="Juniper", slug="juniper")
        DeviceType.objects.create(
            manufacturer=Manufacturer.objects.get(name="Juniper"),
            model="shared-model",
            slug="shared-model",
        )
        manufacturer = Manufacturer.objects.create(name="Cisco", slug="cisco")
        expected_device_type = DeviceType.objects.create(
            manufacturer=manufacturer,
            model="shared-model",
            slug="shared-model",
            part_number="shared-model",
        )
        runner = ForwardSyncRunner(
            sync=self.sync, ingestion=None, client=None, logger_=Mock()
        )

        runner._apply_dcim_device(
            {
                "name": "device-1",
                "manufacturer": "Cisco",
                "manufacturer_slug": "cisco",
                "device_type": "shared-model",
                "device_type_slug": "shared-model",
                "site": "site-1",
                "site_slug": "site-1",
                "role": "switch",
                "role_slug": "switch",
                "role_color": "9e9e9e",
                "status": "active",
            }
        )

        device = Device.objects.get(name="device-1")
        self.assertEqual(device.device_type.pk, expected_device_type.pk)
        expected_device_type.refresh_from_db()
        self.assertEqual(expected_device_type.part_number, "shared-model")

    def test_run_persists_latest_processed_snapshot_metadata(self):
        ingestion = ForwardIngestion.objects.create(sync=self.sync)
        client = Mock()
        client.get_latest_processed_snapshot.return_value = {
            "id": "snapshot-before",
            "state": "PROCESSED",
            "createdAt": "2026-03-31T12:00:00Z",
            "processedAt": "2026-03-31T12:15:00Z",
        }
        client.get_snapshot_metrics.return_value = {
            "snapshotState": "PROCESSED",
            "numSuccessfulDevices": 122,
            "numSuccessfulEndpoints": 1213,
        }
        logger = Mock()
        runner = ForwardSyncRunner(
            sync=self.sync,
            ingestion=ingestion,
            client=client,
            logger_=logger,
        )

        self.sync.get_model_strings = lambda: []
        self.sync.resolve_snapshot_id = lambda client=None: "snapshot-before"

        runner.run()
        ingestion.refresh_from_db()

        self.assertEqual(ingestion.snapshot_selector, LATEST_PROCESSED_SNAPSHOT)
        self.assertEqual(ingestion.snapshot_id, "snapshot-before")
        self.assertEqual(
            ingestion.snapshot_info,
            {
                "id": "snapshot-before",
                "state": "PROCESSED",
                "createdAt": "2026-03-31T12:00:00Z",
                "processedAt": "2026-03-31T12:15:00Z",
            },
        )
        self.assertEqual(
            ingestion.snapshot_metrics,
            {
                "snapshotState": "PROCESSED",
                "numSuccessfulDevices": 122,
                "numSuccessfulEndpoints": 1213,
            },
        )
        client.get_latest_processed_snapshot.assert_called_once_with("test-network")
        client.get_snapshot_metrics.assert_called_once_with("snapshot-before")

    def test_run_fetches_all_pages_for_sync_queries(self):
        ingestion = ForwardIngestion.objects.create(sync=self.sync)
        client = Mock()
        client.get_latest_processed_snapshot.return_value = {
            "id": "snapshot-before",
            "processedAt": "2026-03-31T12:15:00Z",
        }
        client.get_snapshot_metrics.return_value = {}
        client.run_nqe_query.return_value = []
        runner = ForwardSyncRunner(
            sync=self.sync,
            ingestion=ingestion,
            client=client,
            logger_=Mock(),
        )
        runner._apply_model_rows = Mock()

        self.sync.get_model_strings = lambda: ["dcim.device"]
        self.sync.resolve_snapshot_id = lambda client=None: "snapshot-before"

        with patch(
            "forward_netbox.utilities.sync.get_query_specs",
            return_value=[
                QuerySpec(
                    model_string="dcim.device",
                    query_name="Forward Devices",
                    query="foreach device select {name: device.name}",
                )
            ],
        ):
            runner.run()

        client.run_nqe_query.assert_called_once_with(
            query="foreach device select {name: device.name}",
            query_id=None,
            commit_id=None,
            network_id="test-network",
            snapshot_id="snapshot-before",
            parameters={},
            fetch_all=True,
        )

    def test_run_passes_query_rows_through_to_apply_and_statistics(self):
        ingestion = ForwardIngestion.objects.create(sync=self.sync)
        client = Mock()
        client.get_latest_processed_snapshot.return_value = {
            "id": "snapshot-before",
            "processedAt": "2026-03-31T12:15:00Z",
        }
        client.get_snapshot_metrics.return_value = {}
        client.run_nqe_query.return_value = [
            {"name": "site-1", "slug": "site-1"},
            {"name": "site-1", "slug": "site-1"},
            {"name": "site-2", "slug": "site-2"},
        ]
        logger = Mock()
        runner = ForwardSyncRunner(
            sync=self.sync,
            ingestion=ingestion,
            client=client,
            logger_=logger,
        )
        runner._apply_model_rows = Mock()

        self.sync.get_model_strings = lambda: ["dcim.site"]
        self.sync.resolve_snapshot_id = lambda client=None: "snapshot-before"

        with patch(
            "forward_netbox.utilities.sync.get_query_specs",
            return_value=[
                QuerySpec(
                    model_string="dcim.site",
                    query_name="Forward Sites",
                    query='select {name: "site-1", slug: "site-1"}',
                )
            ],
        ):
            runner.run()

        logger.init_statistics.assert_called_once_with("dcim.site", 0)
        logger.add_statistics_total.assert_called_once_with("dcim.site", 3)
        runner._apply_model_rows.assert_called_once()
        applied_rows = runner._apply_model_rows.call_args.args[1]
        self.assertEqual(len(applied_rows), 3)

    def test_run_uses_nqe_diff_when_eligible_baseline_exists(self):
        baseline = ForwardIngestion.objects.create(
            sync=self.sync,
            snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
            snapshot_id="snapshot-before",
            baseline_ready=True,
        )
        ingestion = ForwardIngestion.objects.create(sync=self.sync)
        client = Mock()
        client.get_latest_processed_snapshot.return_value = {
            "id": "snapshot-after",
            "processedAt": "2026-03-31T12:15:00Z",
        }
        client.get_snapshot_metrics.return_value = {}
        client.run_nqe_diff.return_value = [
            {
                "type": "ADDED",
                "before": None,
                "after": {"name": "site-2", "slug": "site-2"},
            },
            {
                "type": "DELETED",
                "before": {"name": "site-1", "slug": "site-1"},
                "after": None,
            },
            {
                "type": "MODIFIED",
                "before": {"name": "site-3", "slug": "site-3"},
                "after": {"name": "site-3b", "slug": "site-3"},
            },
        ]
        logger = Mock()
        runner = ForwardSyncRunner(
            sync=self.sync,
            ingestion=ingestion,
            client=client,
            logger_=logger,
        )
        runner._apply_model_rows = Mock()
        runner._delete_model_rows = Mock()

        self.sync.get_model_strings = lambda: ["dcim.site"]
        self.sync.resolve_snapshot_id = lambda client=None: "snapshot-after"

        with patch(
            "forward_netbox.utilities.sync.get_query_specs",
            return_value=[
                QuerySpec(
                    model_string="dcim.site",
                    query_name="Forward Sites",
                    query_id="Q_sites",
                )
            ],
        ):
            runner.run()

        client.run_nqe_diff.assert_called_once_with(
            query_id="Q_sites",
            commit_id=None,
            before_snapshot_id=baseline.snapshot_id,
            after_snapshot_id="snapshot-after",
            fetch_all=True,
        )
        client.run_nqe_query.assert_not_called()
        logger.add_statistics_total.assert_called_once_with("dcim.site", 3)
        runner._apply_model_rows.assert_called_once_with(
            "dcim.site",
            [
                {"name": "site-2", "slug": "site-2"},
                {"name": "site-3b", "slug": "site-3"},
            ],
        )
        runner._delete_model_rows.assert_called_once_with(
            "dcim.site",
            [{"name": "site-1", "slug": "site-1"}],
        )
        ingestion.refresh_from_db()
        self.assertEqual(ingestion.sync_mode, "diff")

    def test_run_falls_back_to_full_query_when_nqe_diff_fails(self):
        ForwardIngestion.objects.create(
            sync=self.sync,
            snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
            snapshot_id="snapshot-before",
            baseline_ready=True,
        )
        ingestion = ForwardIngestion.objects.create(sync=self.sync)
        client = Mock()
        client.get_latest_processed_snapshot.return_value = {
            "id": "snapshot-after",
            "processedAt": "2026-03-31T12:15:00Z",
        }
        client.get_snapshot_metrics.return_value = {}
        client.run_nqe_diff.side_effect = ForwardClientError("diff failed")
        client.run_nqe_query.return_value = [{"name": "site-1", "slug": "site-1"}]
        logger = Mock()
        runner = ForwardSyncRunner(
            sync=self.sync,
            ingestion=ingestion,
            client=client,
            logger_=logger,
        )
        runner._apply_model_rows = Mock()
        runner._delete_model_rows = Mock()

        self.sync.get_model_strings = lambda: ["dcim.site"]
        self.sync.resolve_snapshot_id = lambda client=None: "snapshot-after"

        with patch(
            "forward_netbox.utilities.sync.get_query_specs",
            return_value=[
                QuerySpec(
                    model_string="dcim.site",
                    query_name="Forward Sites",
                    query_id="Q_sites",
                )
            ],
        ):
            runner.run()

        client.run_nqe_diff.assert_called_once()
        client.run_nqe_query.assert_called_once()
        runner._delete_model_rows.assert_not_called()
        ingestion.refresh_from_db()
        self.assertEqual(ingestion.sync_mode, "full")

    def test_run_records_issue_when_rows_miss_required_identity_fields(self):
        ingestion = ForwardIngestion.objects.create(sync=self.sync)
        client = Mock()
        client.get_latest_processed_snapshot.return_value = {
            "id": "snapshot-before",
            "processedAt": "2026-03-31T12:15:00Z",
        }
        client.get_snapshot_metrics.return_value = {}
        client.run_nqe_query.return_value = [{"name": "device-1"}]
        runner = ForwardSyncRunner(
            sync=self.sync,
            ingestion=ingestion,
            client=client,
            logger_=Mock(),
        )

        self.sync.get_model_strings = lambda: ["dcim.device"]
        self.sync.resolve_snapshot_id = lambda client=None: "snapshot-before"

        with patch(
            "forward_netbox.utilities.sync.get_query_specs",
            return_value=[
                QuerySpec(
                    model_string="dcim.device",
                    query_name="Forward Devices",
                    query="foreach device select {name: device.name}",
                )
            ],
        ):
            runner.run()

        self.assertEqual(ingestion.issues.count(), 1)
        self.assertIn(
            "missing required fields",
            ingestion.issues.first().message,
        )

    def test_run_continues_with_next_model_after_model_abort(self):
        ingestion = ForwardIngestion.objects.create(sync=self.sync)
        client = Mock()
        client.get_latest_processed_snapshot.return_value = {
            "id": "snapshot-before",
            "processedAt": "2026-03-31T12:15:00Z",
        }
        client.get_snapshot_metrics.return_value = {}
        client.run_nqe_query.side_effect = [
            [{"name": "site-1", "slug": "site-1"}],
            [{"name": "site-2", "slug": "site-2"}],
        ]
        logger = Mock()
        runner = ForwardSyncRunner(
            sync=self.sync,
            ingestion=ingestion,
            client=client,
            logger_=logger,
        )
        runner._apply_model_rows = Mock(
            side_effect=[
                ForwardSyncDataError("boom", model_string="dcim.site"),
                None,
            ]
        )

        self.sync.get_model_strings = lambda: ["dcim.site", "dcim.manufacturer"]
        self.sync.resolve_snapshot_id = lambda client=None: "snapshot-before"

        with patch(
            "forward_netbox.utilities.sync.get_query_specs",
            side_effect=[
                [
                    QuerySpec(
                        model_string="dcim.site",
                        query_name="Forward Sites",
                        query='select {name: "site-1", slug: "site-1"}',
                    )
                ],
                [
                    QuerySpec(
                        model_string="dcim.manufacturer",
                        query_name="Forward Manufacturers",
                        query='select {name: "site-2", slug: "site-2"}',
                    )
                ],
            ],
        ):
            runner.run()

        self.assertEqual(runner._apply_model_rows.call_count, 2)

    def test_runner_defines_adapter_for_all_supported_models(self):
        runner = ForwardSyncRunner(
            sync=self.sync, ingestion=None, client=None, logger_=Mock()
        )

        for model_string in FORWARD_SUPPORTED_MODELS:
            handler_name = f"_apply_{model_string.replace('.', '_')}"
            self.assertTrue(
                hasattr(runner, handler_name),
                msg=f"Missing adapter handler for {model_string}",
            )

    def test_apply_model_rows_fails_fast_on_forward_query_error(self):
        runner = ForwardSyncRunner(
            sync=self.sync, ingestion=None, client=None, logger_=Mock()
        )

        def _raise(_):
            raise ForwardQueryError("boom")

        runner._apply_dcim_site = _raise
        runner._record_issue = Mock()

        with self.assertRaises(ForwardSyncDataError):
            runner._apply_model_rows(
                "dcim.site", [{"name": "site-1", "slug": "site-1"}]
            )

        runner._record_issue.assert_called_once()

    def test_apply_model_rows_records_structured_dependency_skip_issue(self):
        runner = ForwardSyncRunner(
            sync=self.sync, ingestion=None, client=None, logger_=Mock()
        )

        def _raise(_):
            raise ForwardDependencySkipError(
                "dependency failed",
                model_string="dcim.site",
                context={"slug": "site-1"},
                defaults={"name": "site-1"},
            )

        runner._apply_dcim_site = _raise
        runner._record_issue = Mock()

        with self.assertRaises(ForwardSyncDataError):
            runner._apply_model_rows(
                "dcim.site", [{"name": "site-1", "slug": "site-1"}]
            )

        _, _, kwargs = runner._record_issue.mock_calls[0]
        self.assertEqual(kwargs["context"], {"slug": "site-1"})
        self.assertEqual(kwargs["defaults"], {"name": "site-1"})

    def test_apply_model_rows_marks_handler_false_as_skipped(self):
        logger = Mock()
        runner = ForwardSyncRunner(
            sync=self.sync, ingestion=None, client=None, logger_=logger
        )

        runner._apply_dcim_site = Mock(return_value=False)

        runner._apply_model_rows("dcim.site", [{"name": "site-1", "slug": "site-1"}])

        logger.increment_statistics.assert_called_with("dcim.site", outcome="skipped")

    def test_record_issue_reuses_issue_id_and_does_not_duplicate(self):
        ingestion = ForwardIngestion.objects.create(sync=self.sync)
        runner = ForwardSyncRunner(
            sync=self.sync, ingestion=ingestion, client=Mock(), logger_=Mock()
        )
        exc = ForwardSyncDataError("duplicate-check")

        issue_1 = runner._record_issue(
            "dcim.site",
            "duplicate-check",
            {"name": "site-1", "slug": "site-1"},
            exception=exc,
            context={"slug": "site-1"},
            defaults={"name": "site-1"},
        )
        issue_2 = runner._record_issue(
            "dcim.site",
            "duplicate-check",
            {"name": "site-1", "slug": "site-1"},
            exception=exc,
            context={"slug": "site-1"},
            defaults={"name": "site-1"},
        )

        self.assertEqual(issue_1.pk, issue_2.pk)
        self.assertEqual(
            ForwardIngestionIssue.objects.filter(ingestion=ingestion).count(), 1
        )

    def test_apply_virtual_chassis_attaches_device_without_inventing_position(self):
        site = Site.objects.create(name="site-1", slug="site-1")
        manufacturer = Manufacturer.objects.create(name="vendor-1", slug="vendor-1")
        role = DeviceRole.objects.create(name="role-1", slug="role-1", color="9e9e9e")
        device_type = DeviceType.objects.create(
            manufacturer=manufacturer,
            model="model-1",
            slug="model-1",
        )
        device = Device.objects.create(
            name="device-1",
            site=site,
            role=role,
            device_type=device_type,
            status="active",
        )
        runner = ForwardSyncRunner(
            sync=self.sync, ingestion=None, client=None, logger_=Mock()
        )

        vc = runner._apply_dcim_virtualchassis(
            {
                "device": device.name,
                "vc_name": "site-1-mlag-device-1--device-2",
                "vc_domain": "device-1--device-2",
            }
        )
        device.refresh_from_db()

        self.assertEqual(vc.name, "site-1-mlag-device-1--device-2")
        self.assertEqual(vc.domain, "device-1--device-2")
        self.assertEqual(device.virtual_chassis, vc)
        self.assertIsNone(device.vc_position)

    def test_apply_virtual_chassis_uses_supplied_position(self):
        site = Site.objects.create(name="site-2", slug="site-2")
        manufacturer = Manufacturer.objects.create(name="vendor-2", slug="vendor-2")
        role = DeviceRole.objects.create(name="role-2", slug="role-2", color="9e9e9e")
        device_type = DeviceType.objects.create(
            manufacturer=manufacturer,
            model="model-2",
            slug="model-2",
        )
        device = Device.objects.create(
            name="device-2",
            site=site,
            role=role,
            device_type=device_type,
            status="active",
        )
        runner = ForwardSyncRunner(
            sync=self.sync, ingestion=None, client=None, logger_=Mock()
        )

        vc = runner._apply_dcim_virtualchassis(
            {
                "device": device.name,
                "vc_name": "site-2-vpc-100",
                "vc_domain": "100",
                "vc_position": 2,
            }
        )
        device.refresh_from_db()

        self.assertEqual(device.virtual_chassis, vc)
        self.assertEqual(device.vc_position, 2)
        self.assertEqual(VirtualChassis.objects.get(pk=vc.pk).domain, "100")


class EventsClearerTest(TestCase):
    @patch(
        "forward_netbox.utilities.sync.transaction.on_commit",
        side_effect=lambda callback: callback(),
    )
    @patch("forward_netbox.utilities.sync.clear_events.send")
    @patch("forward_netbox.utilities.sync.flush_events")
    @patch("forward_netbox.utilities.sync.events_queue")
    def test_events_clearer_flushes_on_commit(
        self,
        mock_events_queue,
        mock_flush_events,
        mock_clear_events_send,
        mock_on_commit,
    ):
        mock_events_queue.get.return_value = {
            "event-1": {"event_type": "create"},
        }
        clearer = EventsClearer()
        clearer.clear()
        mock_on_commit.assert_called_once()
        mock_flush_events.assert_called_once_with([{"event_type": "create"}])
        mock_clear_events_send.assert_called_once_with(sender=None)
