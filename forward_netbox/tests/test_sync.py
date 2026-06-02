import os
import tempfile
import time
from unittest.mock import Mock
from unittest.mock import patch

from core.exceptions import SyncError
from core.models import ObjectChange
from core.models import ObjectType
from dcim.models import Cable
from dcim.models import Device
from dcim.models import DeviceRole
from dcim.models import DeviceType
from dcim.models import Interface
from dcim.models import InventoryItem
from dcim.models import InventoryItemRole
from dcim.models import Manufacturer
from dcim.models import Module
from dcim.models import Platform
from dcim.models import Site
from dcim.models import VirtualChassis
from dcim.models.device_components import ModuleBay
from dcim.models.modules import ModuleType
from django.apps import apps
from django.contrib.auth import get_user_model
from django.contrib.contenttypes.models import ContentType
from django.core.exceptions import ValidationError
from django.db import connection
from django.db import IntegrityError
from django.db.models.deletion import ProtectedError
from django.test import override_settings
from django.test import TestCase
from django.test.utils import CaptureQueriesContext
from django.utils import timezone
from extras.models import Tag
from ipam.models import ASN
from ipam.models import IPAddress
from ipam.models import Prefix
from ipam.models import RIR
from ipam.models import VRF
from netbox_branching.models import Branch

from forward_netbox.choices import FORWARD_SUPPORTED_MODELS
from forward_netbox.choices import ForwardDiffFallbackModeChoices
from forward_netbox.choices import ForwardExecutionStepStatusChoices
from forward_netbox.exceptions import ForwardClientError
from forward_netbox.exceptions import ForwardConnectivityError
from forward_netbox.exceptions import ForwardDependencySkipError
from forward_netbox.exceptions import ForwardQueryError
from forward_netbox.exceptions import ForwardSearchError
from forward_netbox.exceptions import ForwardSyncDataError
from forward_netbox.exceptions import ForwardSyncError
from forward_netbox.models import ForwardDriftPolicy
from forward_netbox.models import ForwardExecutionRun
from forward_netbox.models import ForwardExecutionStep
from forward_netbox.models import ForwardIngestion
from forward_netbox.models import ForwardIngestionIssue
from forward_netbox.models import ForwardNQEMap
from forward_netbox.models import ForwardSource
from forward_netbox.models import ForwardSync
from forward_netbox.models import ForwardValidationRun
from forward_netbox.utilities.apply_engine import ADAPTER_MODELS_WITHOUT_BLOCKER
from forward_netbox.utilities.apply_engine import ADAPTER_REQUIRED_MODELS
from forward_netbox.utilities.apply_engine import apply_engine_decision_for
from forward_netbox.utilities.apply_engine import APPLY_ENGINE_MODEL_CLASSIFICATIONS
from forward_netbox.utilities.apply_engine import BULK_ORM_ENABLED_MODELS
from forward_netbox.utilities.apply_engine import BULK_ORM_ENABLED_MODELS_WITHOUT_SPECS
from forward_netbox.utilities.apply_engine import bulk_orm_expansion_summary
from forward_netbox.utilities.apply_engine import select_apply_engine
from forward_netbox.utilities.apply_engine import UNCLASSIFIED_SUPPORTED_MODELS
from forward_netbox.utilities.branch_budget import branch_budget_density_policy_summary
from forward_netbox.utilities.branch_budget import BranchPlanItem
from forward_netbox.utilities.branch_budget import BranchWorkload
from forward_netbox.utilities.branch_budget import build_branch_plan
from forward_netbox.utilities.branch_budget import build_branch_plan_with_density
from forward_netbox.utilities.branch_budget import delete_dependency_plan_summary
from forward_netbox.utilities.branch_budget import effective_row_budget_for_model
from forward_netbox.utilities.branch_budget import effective_workload_row_budget
from forward_netbox.utilities.branch_budget import row_shard_key
from forward_netbox.utilities.branch_budget import shard_fetch_capability_for_model
from forward_netbox.utilities.branch_budget import shard_fetch_contract
from forward_netbox.utilities.branch_budget import SHARD_FETCH_MODEL_CONTRACTS
from forward_netbox.utilities.density_learning import update_density_learning
from forward_netbox.utilities.direct_changes import object_changes_for_ingestion
from forward_netbox.utilities.execution_telemetry import build_plan_preview
from forward_netbox.utilities.fast_bootstrap_executor import (
    ForwardFastBootstrapExecutor,
)
from forward_netbox.utilities.forward_api import LATEST_PROCESSED_SNAPSHOT
from forward_netbox.utilities.logging import SyncLogging
from forward_netbox.utilities.multi_branch import BranchBudgetExceeded
from forward_netbox.utilities.multi_branch import DEFAULT_PREFLIGHT_ROW_LIMIT
from forward_netbox.utilities.multi_branch import ForwardMultiBranchExecutor
from forward_netbox.utilities.multi_branch import ForwardMultiBranchPlanner
from forward_netbox.utilities.multi_branch_lifecycle import maybe_enqueue_overlap_stage
from forward_netbox.utilities.multi_branch_lifecycle import soft_budget_limit
from forward_netbox.utilities.query_diagnostics import (
    summarize_ipaddress_parent_prefix_rows,
)
from forward_netbox.utilities.query_fetch import ForwardModelResult
from forward_netbox.utilities.query_fetch import ForwardQueryContext
from forward_netbox.utilities.query_fetch import ForwardQueryFetcher
from forward_netbox.utilities.query_registry import QuerySpec
from forward_netbox.utilities.resumable_branching import scheduler_overlap_enabled
from forward_netbox.utilities.resumable_branching import update_plan_item_state
from forward_netbox.utilities.sync import ForwardSyncRunner
from forward_netbox.utilities.sync_contracts import contract_for_model
from forward_netbox.utilities.sync_contracts import default_coalesce_fields_for_model
from forward_netbox.utilities.sync_contracts import validate_row_shape_for_model
from forward_netbox.utilities.sync_events import EventsClearer
from forward_netbox.utilities.sync_facade import enqueue_sync_job
from forward_netbox.utilities.sync_facade import (
    get_query_parameters as facade_get_query_parameters,
)
from forward_netbox.utilities.sync_primitives import delete_by_coalesce
from forward_netbox.utilities.sync_primitives import get_unique_or_raise
from forward_netbox.utilities.sync_primitives import prime_dependency_lookup_caches
from forward_netbox.utilities.sync_state import get_branch_run_display_state


class ForwardIPAMDiagnosticTest(TestCase):
    def test_parent_prefix_diagnostic_reports_missing_covering_prefixes(self):
        diagnostic = summarize_ipaddress_parent_prefix_rows(
            ip_rows=[
                {
                    "device": "device-1",
                    "interface": "Vlan10",
                    "address": "192.0.2.10/24",
                    "vrf": "",
                },
                {
                    "device": "device-2",
                    "interface": "Vlan20",
                    "address": "198.51.100.10/24",
                    "vrf": "",
                },
                {
                    "device": "device-3",
                    "interface": "Vlan30",
                    "address": "2001:db8::1/64",
                    "vrf": "blue",
                },
            ],
            prefix_rows=[
                {"prefix": "192.0.2.0/24", "vrf": ""},
                {"prefix": "2001:db8::/64", "vrf": "blue"},
            ],
        )

        self.assertEqual(diagnostic["total"], 1)
        self.assertEqual(diagnostic["counts"], {"ipv4": 1})
        self.assertEqual(diagnostic["examples"][0]["address"], "198.51.100.10/24")


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
            "device:device-1.*exceeds the soft branch budget limit",
        ):
            build_branch_plan([workload], max_changes_per_branch=5)

    def test_single_bucket_within_soft_overrun_is_allowed(self):
        workload = BranchWorkload(
            model_string="dcim.interface",
            label="interfaces",
            upsert_rows=[
                {"device": "device-1", "name": f"Ethernet1/{index}"}
                for index in range(10272)
            ],
            coalesce_fields=[["device", "name"]],
        )

        plan = build_branch_plan([workload], max_changes_per_branch=10000)
        self.assertEqual(len(plan), 1)
        self.assertEqual(plan[0].estimated_changes, 10272)

    def test_single_bucket_over_soft_overrun_still_fails(self):
        workload = BranchWorkload(
            model_string="dcim.interface",
            label="interfaces",
            upsert_rows=[
                {"device": "device-1", "name": f"Ethernet1/{index}"}
                for index in range(10600)
            ],
            coalesce_fields=[["device", "name"]],
        )

        with self.assertRaisesRegex(
            ForwardQueryError,
            "exceeds the soft branch budget limit",
        ):
            build_branch_plan([workload], max_changes_per_branch=10000)

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

    def test_cable_shard_fetch_contract_filters_by_canonical_device_column(self):
        contract = shard_fetch_contract(
            "dcim.cable",
            [
                "cable:device-a:Ethernet1/1|device-b:Ethernet1/2",
                "cable:device-c:Ethernet1/3|device-d:Ethernet1/4",
            ],
        )

        self.assertEqual(contract["fetch_mode"], "nqe_column_filter")
        self.assertEqual(contract["fetch_key_family"], "device")
        self.assertEqual(
            contract["fetch_column_filters"],
            [
                {
                    "operator": "EQUALS_ANY",
                    "columnName": "device",
                    "values": ["device-a", "device-c"],
                }
            ],
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

    def test_effective_row_budget_uses_module_default_density(self):
        budget = effective_row_budget_for_model(
            "dcim.module",
            max_changes_per_branch=10000,
            model_change_density={},
        )

        self.assertEqual(budget, 3500)

    def test_effective_row_budget_uses_bgp_peer_default_density(self):
        budget = effective_row_budget_for_model(
            "netbox_routing.bgppeer",
            max_changes_per_branch=10000,
            model_change_density={},
        )

        self.assertEqual(budget, 1000)

    def test_bgp_peer_shard_key_uses_device(self):
        row = {
            "device": "device-1",
            "vrf": "VRF-A",
            "neighbor_address": "192.0.2.1",
        }

        self.assertEqual(
            row_shard_key(
                "netbox_routing.bgppeer",
                row,
                [["device", "vrf", "neighbor_address"]],
            ),
            "device:device-1",
        )

    def test_ipam_prefix_shard_fetch_contract_filters_by_prefix_column(self):
        contract = shard_fetch_contract(
            "ipam.prefix",
            [
                "prefix=10.0.0.0/24|vrf=blue",
                "prefix=2001:db8::/64|vrf=blue",
            ],
        )

        self.assertEqual(contract["fetch_mode"], "nqe_parameters")
        self.assertEqual(contract["fetch_key_family"], "prefix")
        self.assertEqual(
            contract["fetch_parameters"],
            {"forward_netbox_shard_keys": ["10.0.0.0/24", "2001:db8::/64"]},
        )
        self.assertEqual(
            contract["fetch_column_filters"],
            [],
        )

    def test_ipam_vlan_shard_fetch_contract_filters_by_vid_column(self):
        contract = shard_fetch_contract(
            "ipam.vlan",
            ["site=site-a|vid=10", "site=site-b|vid=20"],
        )

        self.assertEqual(contract["fetch_mode"], "nqe_column_filter")
        self.assertEqual(contract["fetch_key_family"], "vid")
        self.assertEqual(
            contract["fetch_column_filters"],
            [
                {
                    "operator": "EQUALS_ANY",
                    "columnName": "vid",
                    "values": ["10", "20"],
                }
            ],
        )

    def test_ipam_vrf_shard_fetch_contract_filters_by_name_when_rd_absent(self):
        contract = shard_fetch_contract(
            "ipam.vrf",
            ["name=blue", "name=red"],
        )

        self.assertEqual(contract["fetch_mode"], "nqe_column_filter")
        self.assertEqual(contract["fetch_key_family"], "name")
        self.assertEqual(
            contract["fetch_column_filters"],
            [
                {
                    "operator": "EQUALS_ANY",
                    "columnName": "name",
                    "values": ["blue", "red"],
                }
            ],
        )

    def test_dcim_device_shard_fetch_contract_filters_by_name_column(self):
        contract = shard_fetch_contract(
            "dcim.device",
            ["name=device-1", "name=device-2"],
        )

        self.assertEqual(contract["fetch_mode"], "nqe_column_filter")
        self.assertEqual(contract["fetch_key_family"], "name")
        self.assertEqual(
            contract["fetch_column_filters"],
            [
                {
                    "operator": "EQUALS_ANY",
                    "columnName": "name",
                    "values": ["device-1", "device-2"],
                }
            ],
        )

    def test_device_shard_fetch_contract_uses_column_filters_without_query_params(
        self,
    ):
        contract = shard_fetch_contract(
            "dcim.interface",
            ["device:device-1", "device:device-2"],
        )

        self.assertEqual(contract["query_parameters"], {})
        self.assertEqual(
            contract["fetch_column_filters"],
            [
                {
                    "operator": "EQUALS_ANY",
                    "columnName": "device",
                    "values": ["device-1", "device-2"],
                }
            ],
        )

    def test_shard_fetch_capability_reports_model_fallbacks(self):
        device_contract = shard_fetch_capability_for_model("dcim.interface")
        prefix_contract = shard_fetch_capability_for_model("ipam.prefix")
        site_contract = shard_fetch_capability_for_model("dcim.site")

        self.assertEqual(device_contract["fetch_mode"], "nqe_column_filter")
        self.assertEqual(device_contract["reason_code"], "device_column_filter")
        self.assertTrue(device_contract["shard_safe"])
        self.assertEqual(prefix_contract["reason_code"], "ipam_prefix_query_parameter")
        self.assertTrue(prefix_contract["shard_safe"])
        self.assertEqual(site_contract["fetch_mode"], "nqe_column_filter")
        self.assertEqual(site_contract["reason_code"], "structured_column_filter")
        self.assertTrue(site_contract["shard_safe"])
        self.assertTrue(site_contract["bucket_strategy"]["supported"])

    def test_shard_fetch_contracts_cover_all_supported_models(self):
        self.assertEqual(
            set(SHARD_FETCH_MODEL_CONTRACTS),
            set(FORWARD_SUPPORTED_MODELS),
        )
        for model_string in FORWARD_SUPPORTED_MODELS:
            contract = shard_fetch_capability_for_model(model_string)
            self.assertEqual(contract["model"], model_string)
            self.assertIn(
                contract["fetch_mode"],
                {"nqe_column_filter", "nqe_parameters", "model"},
            )
            self.assertIn(
                contract["schema_contract"],
                {"same_nqe_row_shape", "full_model_shape"},
            )
            self.assertTrue(contract["local_safety_filter"])
            self.assertTrue(contract["reason_code"])
            self.assertTrue(contract["reason"])
            self.assertIn("bucket_strategy", contract)
            self.assertIn("supported", contract["bucket_strategy"])
            self.assertIn("reason_code", contract["bucket_strategy"])
            if contract["shard_safe"]:
                self.assertEqual(contract["schema_contract"], "same_nqe_row_shape")
            else:
                self.assertEqual(contract["schema_contract"], "full_model_shape")
                self.assertEqual(contract["reason_code"], "model_fetch_fallback")
                self.assertIn("bucket_strategy", contract)

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

    def test_low_confidence_density_does_not_drive_budget_auto_tuning(self):
        budget = effective_row_budget_for_model(
            "dcim.device",
            max_changes_per_branch=10000,
            model_change_density={"dcim.device": 5.0},
            model_change_density_profile={
                "dcim.device": {
                    "density": 5.0,
                    "sample_count": 1,
                    "variance": 0.0,
                    "last_updated_at": timezone.now().isoformat(),
                }
            },
        )

        self.assertEqual(budget, 7000)

    def test_medium_confidence_density_blends_with_budget_baseline(self):
        budget = effective_row_budget_for_model(
            "dcim.device",
            max_changes_per_branch=10000,
            model_change_density={"dcim.device": 5.0},
            model_change_density_profile={
                "dcim.device": {
                    "density": 5.0,
                    "sample_count": 4,
                    "variance": 0.0,
                    "last_updated_at": timezone.now().isoformat(),
                }
            },
        )

        self.assertEqual(budget, 2333)

    def test_high_confidence_density_drives_budget_auto_tuning(self):
        budget = effective_row_budget_for_model(
            "dcim.device",
            max_changes_per_branch=10000,
            model_change_density={"dcim.device": 5.0},
            model_change_density_profile={
                "dcim.device": {
                    "density": 5.0,
                    "sample_count": 8,
                    "variance": 0.0,
                    "last_updated_at": timezone.now().isoformat(),
                }
            },
        )

        self.assertEqual(budget, 1400)

    def test_high_confidence_low_density_can_widen_row_budget(self):
        budget = effective_row_budget_for_model(
            "dcim.device",
            max_changes_per_branch=10000,
            model_change_density={"dcim.device": 0.2},
            model_change_density_profile={
                "dcim.device": {
                    "density": 0.2,
                    "sample_count": 8,
                    "variance": 0.0,
                    "last_updated_at": timezone.now().isoformat(),
                }
            },
        )

        self.assertEqual(budget, 35000)

    def test_low_density_widening_requires_high_confidence_profile(self):
        budget = effective_row_budget_for_model(
            "dcim.device",
            max_changes_per_branch=10000,
            model_change_density={"dcim.device": 0.2},
            model_change_density_profile={
                "dcim.device": {
                    "density": 0.2,
                    "sample_count": 1,
                    "variance": 0.0,
                    "last_updated_at": timezone.now().isoformat(),
                }
            },
        )

        self.assertEqual(budget, 7000)

    def test_build_branch_plan_with_high_confidence_low_density_packs_rows(self):
        rows = [{"name": f"device-{index}"} for index in range(30)]
        workload = BranchWorkload(
            model_string="dcim.device",
            label="devices",
            upsert_rows=rows,
            coalesce_fields=[["name"]],
        )

        plan = build_branch_plan_with_density(
            [workload],
            max_changes_per_branch=10,
            model_change_density={"dcim.device": 0.2},
            model_change_density_profile={
                "dcim.device": {
                    "density": 0.2,
                    "sample_count": 8,
                    "variance": 0.0,
                    "last_updated_at": timezone.now().isoformat(),
                }
            },
        )

        self.assertEqual(len(plan), 1)
        self.assertEqual(plan[0].estimated_changes, 30)

    def test_budget_density_policy_summary_reports_auto_tuning_policy(self):
        policies = branch_budget_density_policy_summary(
            ["dcim.device"],
            model_change_density={"dcim.device": 5.0},
            model_change_density_profile={
                "dcim.device": {
                    "density": 5.0,
                    "sample_count": 4,
                    "variance": 0.0,
                    "last_updated_at": timezone.now().isoformat(),
                }
            },
        )

        self.assertEqual(
            policies["dcim.device"]["policy"],
            "medium_confidence_blended_density",
        )
        self.assertEqual(policies["dcim.device"]["density"], 3.0)

    def test_delete_heavy_device_workload_uses_conservative_row_budget(self):
        workload = BranchWorkload(
            model_string="dcim.device",
            label="devices",
            delete_rows=[{"name": f"device-{index}"} for index in range(2000)],
            coalesce_fields=[["name"]],
        )

        budget = effective_workload_row_budget(
            workload,
            max_changes_per_branch=10000,
            model_change_density={},
        )
        plan = build_branch_plan_with_density(
            [workload],
            max_changes_per_branch=10000,
            model_change_density={},
        )

        self.assertEqual(budget, 500)
        self.assertGreater(len(plan), 1)
        self.assertTrue(all(item.estimated_changes <= budget for item in plan))
        self.assertEqual(sum(item.estimated_changes for item in plan), 2000)

    def test_sharded_plan_items_materialize_scoped_fetch_contract(self):
        workload = BranchWorkload(
            model_string="dcim.device",
            label="devices",
            upsert_rows=[{"name": f"device-{index}"} for index in range(4)],
            coalesce_fields=[["name"]],
            fetch_mode="model",
        )

        plan = build_branch_plan([workload], max_changes_per_branch=2)

        self.assertEqual(len(plan), 2)
        self.assertTrue(all(item.shard_keys for item in plan))
        self.assertTrue(all(item.fetch_mode == "nqe_column_filter" for item in plan))
        self.assertTrue(all(item.fetch_key_family == "name" for item in plan))
        self.assertTrue(all(item.fetch_column_filters for item in plan))

    def test_cable_plan_items_keep_identity_shards_with_device_pushdown(self):
        workload = BranchWorkload(
            model_string="dcim.cable",
            label="cables",
            upsert_rows=[
                {
                    "device": "device-a",
                    "interface": "Ethernet1/1",
                    "remote_device": "device-b",
                    "remote_interface": "Ethernet1/2",
                    "status": "connected",
                },
                {
                    "device": "device-c",
                    "interface": "Ethernet1/3",
                    "remote_device": "device-d",
                    "remote_interface": "Ethernet1/4",
                    "status": "connected",
                },
            ],
            coalesce_fields=[
                ["device", "interface", "remote_device", "remote_interface"]
            ],
            fetch_mode="model",
        )

        plan = build_branch_plan([workload], max_changes_per_branch=1)

        self.assertEqual(len(plan), 2)
        self.assertTrue(
            all(str(item.shard_keys[0]).startswith("cable:") for item in plan)
        )
        self.assertTrue(all(item.fetch_mode == "nqe_column_filter" for item in plan))
        self.assertTrue(all(item.fetch_key_family == "device" for item in plan))

    def test_branch_plan_runs_applies_in_dependency_order(self):
        plan = build_branch_plan_with_density(
            [
                BranchWorkload(
                    model_string="dcim.macaddress",
                    label="mac addresses",
                    upsert_rows=[
                        {
                            "device": "device-1",
                            "interface": "Ethernet1",
                            "mac": "00:11:22:33:44:55",
                            "mac_address": "00:11:22:33:44:55",
                        }
                    ],
                    coalesce_fields=[["mac_address"]],
                ),
                BranchWorkload(
                    model_string="ipam.ipaddress",
                    label="ip addresses",
                    upsert_rows=[
                        {
                            "device": "device-1",
                            "interface": "Ethernet1",
                            "address": "192.0.2.1/24",
                            "status": "active",
                        }
                    ],
                    coalesce_fields=[["address"]],
                ),
                BranchWorkload(
                    model_string="dcim.interface",
                    label="interfaces",
                    upsert_rows=[
                        {
                            "device": "device-1",
                            "name": "Ethernet1",
                            "type": "other",
                            "enabled": True,
                        }
                    ],
                    coalesce_fields=[["device", "name"]],
                ),
                BranchWorkload(
                    model_string="ipam.prefix",
                    label="prefixes",
                    upsert_rows=[
                        {
                            "prefix": "192.0.2.0/24",
                            "status": "active",
                        }
                    ],
                    coalesce_fields=[["prefix"]],
                ),
                BranchWorkload(
                    model_string="dcim.device",
                    label="devices",
                    upsert_rows=[{"name": "device-1"}],
                    coalesce_fields=[["name"]],
                ),
            ],
            max_changes_per_branch=10000,
            model_change_density={},
        )

        self.assertEqual(
            [item.model_string for item in plan],
            [
                "ipam.prefix",
                "dcim.device",
                "dcim.interface",
                "ipam.ipaddress",
                "dcim.macaddress",
            ],
        )
        self.assertTrue(all(item.operation == "apply" for item in plan))

    def test_branch_plan_runs_prune_deletes_in_dependency_order(self):
        plan = build_branch_plan_with_density(
            [
                BranchWorkload(
                    model_string="dcim.device",
                    label="devices",
                    delete_rows=[{"name": "device-1"}],
                    coalesce_fields=[["name"]],
                ),
                BranchWorkload(
                    model_string="dcim.interface",
                    label="interfaces",
                    delete_rows=[{"device": "device-1", "name": "Ethernet1"}],
                    coalesce_fields=[["device", "name"]],
                ),
                BranchWorkload(
                    model_string="netbox_routing.ospfinstance",
                    label="ospf instances",
                    delete_rows=[{"device": "device-1", "process_id": 1}],
                    coalesce_fields=[["device", "process_id"]],
                ),
            ],
            max_changes_per_branch=10000,
            model_change_density={},
        )

        self.assertEqual(
            [item.model_string for item in plan],
            ["netbox_routing.ospfinstance", "dcim.interface", "dcim.device"],
        )
        self.assertTrue(all(item.operation == "delete" for item in plan))

    def test_delete_dependency_summary_surfaces_delete_wave_risk(self):
        plan = build_branch_plan_with_density(
            [
                BranchWorkload(
                    model_string="dcim.device",
                    label="devices",
                    delete_rows=[{"name": f"device-{index}"} for index in range(1200)],
                    coalesce_fields=[["name"]],
                ),
                BranchWorkload(
                    model_string="dcim.interface",
                    label="interfaces",
                    delete_rows=[
                        {"device": "device-1", "name": f"Ethernet{index}"}
                        for index in range(50)
                    ],
                    coalesce_fields=[["device", "name"]],
                ),
            ],
            max_changes_per_branch=10000,
            model_change_density={},
        )

        summary = delete_dependency_plan_summary(
            plan,
            max_changes_per_branch=10000,
        )

        self.assertEqual(summary["status"], "high")
        self.assertEqual(summary["delete_rows"], 1250)
        self.assertEqual(
            summary["execution_order"],
            ["dcim.interface", "dcim.device"],
        )
        self.assertEqual(
            summary["models"]["dcim.device"]["reference_blocker_risk"],
            "high",
        )
        self.assertIn(
            "delete_wave",
            {warning["code"] for warning in summary["warnings"]},
        )
        self.assertIn(
            "reference_blocker_risk",
            {warning["code"] for warning in summary["warnings"]},
        )

    def test_plan_preview_includes_delete_dependency_plan(self):
        plan = build_branch_plan(
            [
                BranchWorkload(
                    model_string="dcim.device",
                    label="devices",
                    upsert_rows=[{"name": "device-new"}],
                    delete_rows=[{"name": "device-old"}],
                    coalesce_fields=[["name"]],
                ),
            ],
            max_changes_per_branch=10000,
        )

        preview = build_plan_preview(plan, max_changes_per_branch=10000)

        delete_summary = preview["delete_dependency_plan"]
        self.assertEqual(delete_summary["delete_rows"], 1)
        self.assertEqual(delete_summary["delete_shards"], 1)
        self.assertEqual(delete_summary["execution_order"], ["dcim.device"])
        self.assertEqual(
            delete_summary["models"]["dcim.device"]["reference_blocker_risk"],
            "high",
        )

    def test_branch_plan_splits_mixed_workloads_into_apply_then_delete_phases(self):
        plan = build_branch_plan(
            [
                BranchWorkload(
                    model_string="dcim.device",
                    label="devices",
                    upsert_rows=[{"name": "device-new"}],
                    delete_rows=[{"name": "device-old"}],
                    coalesce_fields=[["name"]],
                )
            ],
            max_changes_per_branch=10000,
        )

        self.assertEqual([item.operation for item in plan], ["apply", "delete"])
        self.assertEqual(len(plan[0].upsert_rows), 1)
        self.assertEqual(plan[0].delete_rows, [])
        self.assertEqual(plan[1].upsert_rows, [])
        self.assertEqual(len(plan[1].delete_rows), 1)

    def test_device_upsert_workload_keeps_normal_row_budget(self):
        workload = BranchWorkload(
            model_string="dcim.device",
            label="devices",
            upsert_rows=[{"name": f"device-{index}"} for index in range(2000)],
            coalesce_fields=[["name"]],
        )

        budget = effective_workload_row_budget(
            workload,
            max_changes_per_branch=10000,
            model_change_density={},
        )

        self.assertEqual(budget, 10000)

    def test_runtime_budgeting_increases_budget_for_runtime_heavy_workload(self):
        workload = BranchWorkload(
            model_string="dcim.device",
            label="devices",
            upsert_rows=[{"name": f"device-{index}"} for index in range(2000)],
            coalesce_fields=[["name"]],
            query_runtime_ms=24_000,
        )

        budget = effective_workload_row_budget(
            workload,
            max_changes_per_branch=10000,
            model_change_density={"dcim.device": 1.4},
        )

        self.assertEqual(budget, 6250)

    def test_runtime_budgeting_reduces_budget_for_apply_heavy_workload(self):
        workload = BranchWorkload(
            model_string="dcim.device",
            label="devices",
            upsert_rows=[{"name": f"device-{index}"} for index in range(2000)],
            coalesce_fields=[["name"]],
            query_runtime_ms=500,
        )

        budget = effective_workload_row_budget(
            workload,
            max_changes_per_branch=10000,
            model_change_density={"dcim.device": 1.4},
        )

        self.assertEqual(budget, 3750)

    def test_runtime_budgeting_keeps_delete_heavy_conservative_cap(self):
        workload = BranchWorkload(
            model_string="dcim.device",
            label="devices",
            delete_rows=[{"name": f"device-{index}"} for index in range(2000)],
            coalesce_fields=[["name"]],
            query_runtime_ms=24_000,
        )

        budget = effective_workload_row_budget(
            workload,
            max_changes_per_branch=10000,
            model_change_density={},
        )

        self.assertEqual(budget, 500)

    def test_density_learning_rejects_large_outlier_after_warmup(self):
        density = {"dcim.device": 1.0}
        profile = {}
        for observed in (1.0, 1.05, 0.95, 1.02):
            density, profile, result = update_density_learning(
                density,
                profile,
                model_string="dcim.device",
                observed_density=observed,
            )
            self.assertTrue(result["accepted"])

        baseline_density = density["dcim.device"]
        density, profile, result = update_density_learning(
            density,
            profile,
            model_string="dcim.device",
            observed_density=25.0,
        )

        self.assertFalse(result["accepted"])
        self.assertEqual(result["reason"], "ratio_outlier")
        self.assertEqual(density["dcim.device"], baseline_density)
        self.assertGreater(profile["dcim.device"]["rejected_observations"], 0)

    def test_density_learning_accepts_warmup_samples(self):
        density = {}
        profile = {}
        density, profile, result = update_density_learning(
            density,
            profile,
            model_string="dcim.device",
            observed_density=2.5,
        )
        self.assertTrue(result["accepted"])
        self.assertIn("dcim.device", density)
        self.assertEqual(profile["dcim.device"]["sample_count"], 1)


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
                "enable_bulk_orm": False,
            },
        )

    def _shard_parity_case(self, model_string: str):
        if model_string == "dcim.site":
            target = {"name": "site-1", "slug": "site-1"}
            noise = {"name": "site-2", "slug": "site-2"}
        elif model_string == "dcim.manufacturer":
            target = {"name": "manufacturer-1", "slug": "manufacturer-1"}
            noise = {"name": "manufacturer-2", "slug": "manufacturer-2"}
        elif model_string == "dcim.devicerole":
            target = {"name": "role-1", "slug": "role-1", "color": "ff0000"}
            noise = {"name": "role-2", "slug": "role-2", "color": "00ff00"}
        elif model_string == "dcim.platform":
            target = {
                "name": "platform-1",
                "slug": "platform-1",
                "manufacturer": "Cisco",
                "manufacturer_slug": "cisco",
            }
            noise = {
                "name": "platform-2",
                "slug": "platform-2",
                "manufacturer": "Juniper",
                "manufacturer_slug": "juniper",
            }
        elif model_string == "dcim.devicetype":
            target = {
                "manufacturer": "Cisco",
                "manufacturer_slug": "cisco",
                "model": "Model-1",
                "slug": "model-1",
            }
            noise = {
                "manufacturer": "Cisco",
                "manufacturer_slug": "cisco",
                "model": "Model-2",
                "slug": "model-2",
            }
        elif model_string == "dcim.device":
            target = {
                "name": "device-1",
                "manufacturer": "Cisco",
                "manufacturer_slug": "cisco",
                "device_type": "dt-1",
                "device_type_slug": "dt-1",
                "site": "site-1",
                "site_slug": "site-1",
                "role": "role-1",
                "role_slug": "role-1",
                "role_color": "ff0000",
                "status": "active",
            }
            noise = {
                "name": "device-2",
                "manufacturer": "Cisco",
                "manufacturer_slug": "cisco",
                "device_type": "dt-2",
                "device_type_slug": "dt-2",
                "site": "site-2",
                "site_slug": "site-2",
                "role": "role-2",
                "role_slug": "role-2",
                "role_color": "00ff00",
                "status": "active",
            }
        elif model_string == "dcim.virtualchassis":
            target = {
                "name": "vc-1",
                "device": "device-1",
                "vc_name": "vc-1",
                "vc_domain": "domain-1",
            }
            noise = {
                "name": "vc-2",
                "device": "device-2",
                "vc_name": "vc-2",
                "vc_domain": "domain-2",
            }
        elif model_string == "extras.taggeditem":
            target = {
                "device": "device-1",
                "tag": "Prot_BGP",
                "tag_slug": "prot-bgp",
                "tag_color": "2196f3",
            }
            noise = {
                "device": "device-2",
                "tag": "Prot_BGP",
                "tag_slug": "prot-bgp",
                "tag_color": "2196f3",
            }
        elif model_string == "dcim.interface":
            target = {
                "device": "device-1",
                "name": "Ethernet1",
                "type": "other",
                "enabled": True,
            }
            noise = {
                "device": "device-2",
                "name": "Ethernet1",
                "type": "other",
                "enabled": True,
            }
        elif model_string == "dcim.cable":
            target = {
                "device": "device-1",
                "interface": "Ethernet1/1",
                "remote_device": "device-2",
                "remote_interface": "Ethernet1/2",
                "status": "connected",
            }
            noise = {
                "device": "device-3",
                "interface": "Ethernet1/1",
                "remote_device": "device-4",
                "remote_interface": "Ethernet1/2",
                "status": "connected",
            }
        elif model_string == "dcim.macaddress":
            target = {
                "device": "device-1",
                "interface": "Ethernet1/1",
                "mac": "00:11:22:33:44:55",
                "mac_address": "00:11:22:33:44:55",
            }
            noise = {
                "device": "device-2",
                "interface": "Ethernet1/1",
                "mac": "00:11:22:33:44:55",
                "mac_address": "00:11:22:33:44:55",
            }
        elif model_string == "ipam.vlan":
            target = {"site": "site-1", "vid": 10, "name": "vlan10", "status": "active"}
            noise = {"site": "site-2", "vid": 10, "name": "vlan10", "status": "active"}
        elif model_string == "ipam.vrf":
            target = {
                "name": "blue",
                "rd": "65000:1",
                "description": "blue",
                "enforce_unique": False,
            }
            noise = {
                "name": "red",
                "rd": "65000:2",
                "description": "red",
                "enforce_unique": False,
            }
        elif model_string == "ipam.prefix":
            target = {
                "prefix": "10.0.0.0/24",
                "vrf": "blue",
                "status": "active",
            }
            noise = {
                "prefix": "10.0.0.0/24",
                "vrf": "red",
                "status": "active",
            }
        elif model_string == "ipam.ipaddress":
            target = {
                "device": "device-1",
                "interface": "Ethernet1/1",
                "address": "10.0.0.1/24",
                "vrf": "blue",
                "status": "active",
            }
            noise = {
                "device": "device-2",
                "interface": "Ethernet1/1",
                "address": "10.0.0.1/24",
                "vrf": "red",
                "status": "active",
            }
        elif model_string == "dcim.inventoryitem":
            target = {
                "device": "device-1",
                "name": "module-1",
                "part_id": "P-1",
                "serial": "S-1",
                "status": "active",
                "discovered": True,
            }
            noise = {
                "device": "device-2",
                "name": "module-1",
                "part_id": "P-1",
                "serial": "S-1",
                "status": "active",
                "discovered": True,
            }
        elif model_string == "dcim.module":
            target = {
                "device": "device-1",
                "module_bay": "Slot 1",
                "manufacturer": "Cisco",
                "manufacturer_slug": "cisco",
                "model": "Module-1",
                "part_number": "PN-1",
                "status": "active",
            }
            noise = {
                "device": "device-2",
                "module_bay": "Slot 1",
                "manufacturer": "Cisco",
                "manufacturer_slug": "cisco",
                "model": "Module-1",
                "part_number": "PN-1",
                "status": "active",
            }
        elif model_string == "netbox_routing.bgppeer":
            target = {
                "device": "device-1",
                "local_asn": 65000,
                "neighbor_address": "192.0.2.1",
                "peer_asn": 65100,
                "enabled": True,
                "status": "active",
            }
            noise = {
                "device": "device-2",
                "local_asn": 65000,
                "neighbor_address": "192.0.2.1",
                "peer_asn": 65100,
                "enabled": True,
                "status": "active",
            }
        elif model_string == "netbox_routing.bgpaddressfamily":
            target = {
                "device": "device-1",
                "local_asn": 65000,
                "afi_safi": "ipv4-unicast",
            }
            noise = {
                "device": "device-2",
                "local_asn": 65000,
                "afi_safi": "ipv4-unicast",
            }
        elif model_string == "netbox_routing.bgppeeraddressfamily":
            target = {
                "device": "device-1",
                "local_asn": 65000,
                "neighbor_address": "192.0.2.1",
                "peer_asn": 65100,
                "afi_safi": "ipv4-unicast",
                "enabled": True,
            }
            noise = {
                "device": "device-2",
                "local_asn": 65000,
                "neighbor_address": "192.0.2.1",
                "peer_asn": 65100,
                "afi_safi": "ipv4-unicast",
                "enabled": True,
            }
        elif model_string == "netbox_routing.ospfinstance":
            target = {
                "device": "device-1",
                "process_id": 1,
                "router_id": "1.1.1.1",
            }
            noise = {
                "device": "device-2",
                "process_id": 1,
                "router_id": "1.1.1.1",
            }
        elif model_string == "netbox_routing.ospfarea":
            target = {"area_id": "0.0.0.0", "area_type": "normal"}
            noise = {"area_id": "0.0.0.1", "area_type": "normal"}
        elif model_string == "netbox_routing.ospfinterface":
            target = {
                "device": "device-1",
                "process_id": 1,
                "router_id": "1.1.1.1",
                "area_id": "0.0.0.0",
                "area_type": "normal",
                "local_interface": "Ethernet1/1",
            }
            noise = {
                "device": "device-2",
                "process_id": 1,
                "router_id": "1.1.1.1",
                "area_id": "0.0.0.0",
                "area_type": "normal",
                "local_interface": "Ethernet1/1",
            }
        elif model_string == "netbox_peering_manager.peeringsession":
            target = {
                "device": "device-1",
                "local_asn": 65000,
                "neighbor_address": "192.0.2.1",
                "peer_asn": 65100,
                "enabled": True,
                "status": "active",
            }
            noise = {
                "device": "device-2",
                "local_asn": 65000,
                "neighbor_address": "192.0.2.1",
                "peer_asn": 65100,
                "enabled": True,
                "status": "active",
            }
        else:
            raise AssertionError(f"Unhandled model: {model_string}")

        return target, noise

    def _query_for_model(self, model_string: str, row: dict) -> str:
        fields = [
            (
                f'{field}: "{value}"'
                if not isinstance(value, bool)
                else f"{field}: {str(value).lower()}"
            )
            for field, value in row.items()
        ]
        return "select {" + ", ".join(fields) + "}"

    @patch("forward_netbox.utilities.multi_branch_planner.ForwardQueryFetcher")
    def test_planner_disables_diagnostics_for_shard_scoped_fetches(
        self, mock_fetcher_cls
    ):
        fetcher = mock_fetcher_cls.return_value
        fetcher.resolve_context.return_value = ForwardQueryContext(
            network_id="test-network",
            snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
            snapshot_id="snapshot-after",
        )
        fetcher.fetch_workloads.return_value = []
        fetcher.model_results = []
        planner = ForwardMultiBranchPlanner(
            sync=self.sync,
            client=Mock(),
            logger_=Mock(),
        )
        shard_scope = {
            "model": "dcim.interface",
            "query_name": "Forward Interfaces",
            "execution_value": "org:/forward_netbox_validation/forward_interfaces",
            "shard_keys": ["device:core-1"],
        }

        _context, _plan = planner.build_plan(
            max_changes_per_branch=10,
            run_preflight=False,
            model_strings=["dcim.interface"],
            shard_scope=shard_scope,
        )

        fetcher.fetch_workloads.assert_called_once_with(
            fetcher.resolve_context.return_value,
            model_strings=["dcim.interface"],
            shard_scope=shard_scope,
            include_diagnostics=False,
        )

    @patch("forward_netbox.utilities.query_fetch_execution.get_query_specs")
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

    @patch("forward_netbox.utilities.query_fetch_execution.get_query_specs")
    def test_build_plan_skips_query_preflight_when_source_disables_it(self, mock_specs):
        self.source.parameters["query_preflight_enabled"] = False
        self.source.save(update_fields=["parameters"])
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
        client.run_nqe_query.return_value = [{"name": "site-1", "slug": "site-1"}]
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

        self.assertEqual(client.run_nqe_query.call_count, 1)
        call = client.run_nqe_query.call_args_list[0]
        self.assertTrue(call.kwargs["fetch_all"])

    @patch("forward_netbox.utilities.query_fetch_execution.get_query_specs")
    def test_build_plan_uses_source_query_preflight_row_limit(self, mock_specs):
        self.source.parameters["query_preflight_row_limit"] = 2
        self.source.save(update_fields=["parameters"])
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
        self.assertEqual(first_call.kwargs["limit"], 2)
        self.assertFalse(first_call.kwargs["fetch_all"])

    @patch("forward_netbox.utilities.query_fetch_execution.get_query_specs")
    def test_build_plan_resolves_query_path_once_per_model_with_preflight_enabled(
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
        client.resolve_nqe_query_reference.return_value = {
            "queryId": "Q-sites",
            "commitId": "C-sites",
        }
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
                query_repository="org",
                query_path="/forward_netbox_validation/forward_sites",
            )
        ]
        planner = ForwardMultiBranchPlanner(
            sync=self.sync,
            client=client,
            logger_=Mock(),
        )

        planner.build_plan(max_changes_per_branch=10, run_preflight=True)

        client.resolve_nqe_query_reference.assert_called_once_with(
            repository="org",
            query_path="/forward_netbox_validation/forward_sites",
            commit_id=None,
        )

    @patch("forward_netbox.utilities.query_fetch_execution.get_query_specs")
    def test_build_plan_caches_incremental_baseline_lookup_across_models(
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
        client.run_nqe_query.return_value = [{"name": "site-1", "slug": "site-1"}]
        self.sync.resolve_snapshot_id = lambda client=None: "snapshot-after"
        self.sync.get_model_strings = lambda: ["dcim.site", "dcim.manufacturer"]
        self.sync.incremental_diff_baseline = Mock(return_value=None)
        mock_specs.side_effect = lambda model_string, maps=None: [
            QuerySpec(
                model_string=model_string,
                query_name=f"{model_string} Query",
                query_id=f"Q-{model_string}",
            )
        ]
        planner = ForwardMultiBranchPlanner(
            sync=self.sync,
            client=client,
            logger_=Mock(),
        )

        _context, plan = planner.build_plan(
            max_changes_per_branch=10,
            run_preflight=False,
        )

        self.assertEqual(len(plan), 2)
        self.assertEqual(self.sync.incremental_diff_baseline.call_count, 1)

    @patch("forward_netbox.utilities.query_fetch_execution.get_query_specs")
    def test_build_plan_continues_when_query_path_resolution_fails_for_one_model(
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

        def resolve_side_effect(*, repository, query_path, commit_id):
            if query_path == "/forward_netbox_validation/forward_platforms":
                raise ForwardClientError("repository lookup timeout")
            return {
                "queryId": "Q-sites",
                "commitId": "C-sites",
            }

        client.resolve_nqe_query_reference.side_effect = resolve_side_effect
        client.run_nqe_query.return_value = [{"name": "site-1", "slug": "site-1"}]
        self.sync.resolve_snapshot_id = lambda client=None: "snapshot-after"
        self.sync.get_model_strings = lambda: ["dcim.platform", "dcim.site"]
        self.sync.incremental_diff_baseline = Mock(return_value=None)

        def specs_for_model(model_string, maps=None):
            if model_string == "dcim.platform":
                return [
                    QuerySpec(
                        model_string="dcim.platform",
                        query_name="Forward Platforms",
                        query_repository="org",
                        query_path="/forward_netbox_validation/forward_platforms",
                    )
                ]
            if model_string == "dcim.site":
                return [
                    QuerySpec(
                        model_string="dcim.site",
                        query_name="Forward Sites",
                        query_repository="org",
                        query_path="/forward_netbox_validation/forward_sites",
                    )
                ]
            return []

        mock_specs.side_effect = specs_for_model
        planner = ForwardMultiBranchPlanner(
            sync=self.sync,
            client=client,
            logger_=Mock(),
        )

        context, plan = planner.build_plan(
            max_changes_per_branch=10,
            run_preflight=False,
        )

        self.assertEqual(context["snapshot_id"], "snapshot-after")
        self.assertEqual(len(plan), 1)
        self.assertEqual(plan[0].model_string, "dcim.site")

    def test_fetch_workloads_preserves_model_order_when_parallel_fetch_completes_out_of_order(
        self,
    ):
        client = Mock()
        logger = Mock()
        fetcher = ForwardQueryFetcher(
            sync=self.sync,
            client=client,
            logger_=logger,
        )
        fetcher._build_workload_jobs = Mock(
            return_value=[
                ("dcim.interface",),
                ("dcim.macaddress",),
            ]
        )
        fetcher._append_ipaddress_diagnostics = Mock()
        fetcher._append_ipaddress_parent_prefix_diagnostics = Mock()
        fetcher._append_routing_diagnostics = Mock()

        def run_job(payload):
            _, _, job = payload
            model_string = job[0]
            if model_string == "dcim.interface":
                time.sleep(0.05)
            return (
                ForwardModelResult(
                    model_string=model_string,
                    query_name=f"{model_string} query",
                    execution_mode="query_id",
                    execution_value=f"{model_string}-query",
                    sync_mode="full",
                    row_count=1,
                    runtime_ms=1.0,
                ),
                BranchWorkload(
                    model_string=model_string,
                    label=model_string,
                    upsert_rows=[{"model": model_string}],
                    coalesce_fields=[("model",)],
                ),
            )

        fetcher._run_workload_job = run_job
        context = ForwardQueryContext(
            network_id="test-network",
            snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
            snapshot_id="snapshot-after",
        )

        workloads = fetcher.fetch_workloads(context)

        self.assertEqual(
            [workload.model_string for workload in workloads],
            ["dcim.interface", "dcim.macaddress"],
        )
        self.assertEqual(
            [result.model_string for result in fetcher.model_results],
            ["dcim.interface", "dcim.macaddress"],
        )

    def test_fetch_workloads_can_skip_diagnostic_passes(self):
        client = Mock()
        logger = Mock()
        fetcher = ForwardQueryFetcher(
            sync=self.sync,
            client=client,
            logger_=logger,
        )
        fetcher._build_workload_jobs = Mock(
            return_value=[
                ("dcim.interface",),
            ]
        )
        fetcher._append_ipaddress_diagnostics = Mock()
        fetcher._append_ipaddress_parent_prefix_diagnostics = Mock()
        fetcher._append_routing_diagnostics = Mock()
        fetcher._run_workload_job = Mock(
            return_value=(
                ForwardModelResult(
                    model_string="dcim.interface",
                    query_name="dcim.interface query",
                    execution_mode="query_id",
                    execution_value="dcim.interface-query",
                    sync_mode="full",
                    row_count=1,
                    runtime_ms=1.0,
                ),
                BranchWorkload(
                    model_string="dcim.interface",
                    label="dcim.interface",
                    upsert_rows=[{"model": "dcim.interface"}],
                    coalesce_fields=[("model",)],
                ),
            )
        )
        context = ForwardQueryContext(
            network_id="test-network",
            snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
            snapshot_id="snapshot-after",
        )

        workloads = fetcher.fetch_workloads(context, include_diagnostics=False)

        self.assertEqual(len(workloads), 1)
        self.assertFalse(fetcher._append_ipaddress_diagnostics.called)
        self.assertFalse(fetcher._append_ipaddress_parent_prefix_diagnostics.called)
        self.assertFalse(fetcher._append_routing_diagnostics.called)

    def test_fetch_workloads_honors_source_diagnostic_toggle(self):
        client = Mock()
        logger = Mock()
        fetcher = ForwardQueryFetcher(
            sync=self.sync,
            client=client,
            logger_=logger,
        )
        self.source.parameters["query_diagnostics_enabled"] = False
        self.source.save(update_fields=["parameters"])
        fetcher._build_workload_jobs = Mock(
            return_value=[
                ("dcim.interface",),
            ]
        )
        fetcher._append_ipaddress_diagnostics = Mock()
        fetcher._append_ipaddress_parent_prefix_diagnostics = Mock()
        fetcher._append_routing_diagnostics = Mock()
        fetcher._run_workload_job = Mock(
            return_value=(
                ForwardModelResult(
                    model_string="dcim.interface",
                    query_name="dcim.interface query",
                    execution_mode="query_id",
                    execution_value="dcim.interface-query",
                    sync_mode="full",
                    row_count=1,
                    runtime_ms=1.0,
                ),
                BranchWorkload(
                    model_string="dcim.interface",
                    label="dcim.interface",
                    upsert_rows=[{"model": "dcim.interface"}],
                    coalesce_fields=[("model",)],
                ),
            )
        )
        context = ForwardQueryContext(
            network_id="test-network",
            snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
            snapshot_id="snapshot-after",
        )

        workloads = fetcher.fetch_workloads(context)

        self.assertEqual(len(workloads), 1)
        self.assertFalse(fetcher._append_ipaddress_diagnostics.called)
        self.assertFalse(fetcher._append_ipaddress_parent_prefix_diagnostics.called)
        self.assertFalse(fetcher._append_routing_diagnostics.called)

    def test_ipaddress_diagnostic_skips_when_current_workload_has_no_ipaddress_model(
        self,
    ):
        client = Mock()
        fetcher = ForwardQueryFetcher(
            sync=self.sync,
            client=client,
            logger_=Mock(),
        )
        self.sync.get_model_strings = lambda: ["dcim.site", "ipam.ipaddress"]
        fetcher.model_results = [
            ForwardModelResult(
                model_string="dcim.site",
                query_name="Forward Sites",
                execution_mode="query_id",
                execution_value="Q-sites",
                sync_mode="full",
                row_count=1,
                runtime_ms=1.0,
            )
        ]
        context = ForwardQueryContext(
            network_id="test-network",
            snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
            snapshot_id="snapshot-after",
        )

        fetcher._append_ipaddress_diagnostics(context)

        self.assertFalse(client.run_nqe_query.called)

    def test_ipaddress_diagnostic_skips_when_ipaddress_model_has_zero_rows(self):
        client = Mock()
        fetcher = ForwardQueryFetcher(
            sync=self.sync,
            client=client,
            logger_=Mock(),
        )
        self.sync.get_model_strings = lambda: ["ipam.ipaddress"]
        fetcher.model_results = [
            ForwardModelResult(
                model_string="ipam.ipaddress",
                query_name="Forward IP Addresses",
                execution_mode="query_id",
                execution_value="Q-ipam.ipaddress",
                sync_mode="diff",
                row_count=0,
                runtime_ms=1.0,
            )
        ]
        context = ForwardQueryContext(
            network_id="test-network",
            snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
            snapshot_id="snapshot-after",
        )

        fetcher._append_ipaddress_diagnostics(context)

        self.assertFalse(client.run_nqe_query.called)

    def test_ipaddress_diagnostic_uses_cached_none_result(self):
        client = Mock()
        fetcher = ForwardQueryFetcher(
            sync=self.sync,
            client=client,
            logger_=Mock(),
        )
        fetcher._load_cached_diagnostic_result = Mock(return_value=(True, None))
        self.sync.get_model_strings = lambda: ["ipam.ipaddress"]
        fetcher.model_results = [
            ForwardModelResult(
                model_string="ipam.ipaddress",
                query_name="Forward IP Addresses",
                execution_mode="query_id",
                execution_value="Q-ipam.ipaddress",
                sync_mode="diff",
                row_count=1,
                runtime_ms=1.0,
            )
        ]
        context = ForwardQueryContext(
            network_id="test-network",
            snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
            snapshot_id="snapshot-after",
        )

        fetcher._append_ipaddress_diagnostics(context)

        self.assertFalse(client.run_nqe_query.called)

    def test_ipaddress_diagnostic_caches_empty_result(self):
        client = Mock()
        client.run_nqe_query.return_value = []
        fetcher = ForwardQueryFetcher(
            sync=self.sync,
            client=client,
            logger_=Mock(),
        )
        fetcher._load_cached_diagnostic_result = Mock(return_value=(False, None))
        fetcher._store_cached_diagnostic_result = Mock()
        self.sync.get_model_strings = lambda: ["ipam.ipaddress"]
        fetcher.model_results = [
            ForwardModelResult(
                model_string="ipam.ipaddress",
                query_name="Forward IP Addresses",
                execution_mode="query_id",
                execution_value="Q-ipam.ipaddress",
                sync_mode="diff",
                row_count=1,
                runtime_ms=1.0,
            )
        ]
        context = ForwardQueryContext(
            network_id="test-network",
            snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
            snapshot_id="snapshot-after",
        )

        fetcher._append_ipaddress_diagnostics(context)

        fetcher._store_cached_diagnostic_result.assert_called_once_with(
            diagnostic_name="unassignable_interface_addresses",
            context=context,
            diagnostic=None,
        )

    def test_routing_diagnostic_skips_when_current_workload_has_no_routing_model(
        self,
    ):
        client = Mock()
        fetcher = ForwardQueryFetcher(
            sync=self.sync,
            client=client,
            logger_=Mock(),
        )
        self.sync.get_model_strings = lambda: ["dcim.site", "netbox_routing.bgppeer"]
        fetcher.model_results = [
            ForwardModelResult(
                model_string="dcim.site",
                query_name="Forward Sites",
                execution_mode="query_id",
                execution_value="Q-sites",
                sync_mode="full",
                row_count=1,
                runtime_ms=1.0,
            )
        ]
        context = ForwardQueryContext(
            network_id="test-network",
            snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
            snapshot_id="snapshot-after",
        )

        fetcher._append_routing_diagnostics(context)

        self.assertFalse(client.run_nqe_query.called)

    def test_routing_diagnostic_skips_when_routing_model_has_zero_rows(self):
        client = Mock()
        fetcher = ForwardQueryFetcher(
            sync=self.sync,
            client=client,
            logger_=Mock(),
        )
        self.sync.get_model_strings = lambda: ["netbox_routing.bgppeer"]
        fetcher.model_results = [
            ForwardModelResult(
                model_string="netbox_routing.bgppeer",
                query_name="Forward BGP Peers",
                execution_mode="query_id",
                execution_value="Q-netbox_routing.bgppeer",
                sync_mode="diff",
                row_count=0,
                runtime_ms=1.0,
            )
        ]
        context = ForwardQueryContext(
            network_id="test-network",
            snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
            snapshot_id="snapshot-after",
        )

        fetcher._append_routing_diagnostics(context)

        self.assertFalse(client.run_nqe_query.called)

    def test_routing_diagnostic_uses_cached_none_result(self):
        client = Mock()
        fetcher = ForwardQueryFetcher(
            sync=self.sync,
            client=client,
            logger_=Mock(),
        )
        fetcher._load_cached_diagnostic_result = Mock(return_value=(True, None))
        self.sync.get_model_strings = lambda: ["netbox_routing.bgppeer"]
        fetcher.model_results = [
            ForwardModelResult(
                model_string="netbox_routing.bgppeer",
                query_name="Forward BGP Peers",
                execution_mode="query_id",
                execution_value="Q-netbox_routing.bgppeer",
                sync_mode="diff",
                row_count=1,
                runtime_ms=1.0,
            )
        ]
        context = ForwardQueryContext(
            network_id="test-network",
            snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
            snapshot_id="snapshot-after",
        )

        fetcher._append_routing_diagnostics(context)

        self.assertFalse(client.run_nqe_query.called)

    @patch("forward_netbox.utilities.query_fetch_execution.get_query_specs")
    def test_build_plan_uses_shard_scoped_column_filter_for_single_device(
        self,
        mock_specs,
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
        client.run_nqe_query.return_value = [
            {
                "device": "device-1",
                "name": "Ethernet1",
                "type": "other",
                "enabled": True,
            },
            {
                "device": "device-2",
                "name": "Ethernet1",
                "type": "other",
                "enabled": True,
            },
        ]
        self.sync.resolve_snapshot_id = lambda client=None: "snapshot-after"
        self.sync.get_model_strings = lambda: ["dcim.interface"]
        self.sync.incremental_diff_baseline = Mock(return_value=None)
        mock_specs.return_value = [
            QuerySpec(
                model_string="dcim.interface",
                query_name="Forward Interfaces",
                query=(
                    'select {device: "device-1", name: "Ethernet1", '
                    'type: "other", enabled: true}'
                ),
            )
        ]
        planner = ForwardMultiBranchPlanner(
            sync=self.sync,
            client=client,
            logger_=Mock(),
        )

        _context, plan = planner.build_plan(
            max_changes_per_branch=10,
            run_preflight=False,
            model_strings=["dcim.interface"],
            shard_scope={
                "model": "dcim.interface",
                "query_name": "Forward Interfaces",
                "shard_keys": ["device:device-1"],
            },
        )

        self.assertEqual(
            client.run_nqe_query.call_args.kwargs["column_filters"],
            [
                {
                    "operator": "EQUALS_ANY",
                    "columnName": "device",
                    "values": ["device-1"],
                }
            ],
        )
        self.assertEqual(len(plan), 1)
        self.assertEqual(plan[0].upsert_rows, [client.run_nqe_query.return_value[0]])

    @patch("forward_netbox.utilities.query_fetch_execution.get_query_specs")
    def test_build_plan_uses_shard_scoped_column_filter_for_multiple_devices(
        self,
        mock_specs,
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
        client.run_nqe_query.return_value = [
            {
                "device": "device-1",
                "name": "Ethernet1",
                "type": "other",
                "enabled": True,
            },
            {
                "device": "device-2",
                "name": "Ethernet1",
                "type": "other",
                "enabled": True,
            },
            {
                "device": "device-3",
                "name": "Ethernet1",
                "type": "other",
                "enabled": True,
            },
        ]
        self.sync.resolve_snapshot_id = lambda client=None: "snapshot-after"
        self.sync.get_model_strings = lambda: ["dcim.interface"]
        self.sync.incremental_diff_baseline = Mock(return_value=None)
        mock_specs.return_value = [
            QuerySpec(
                model_string="dcim.interface",
                query_name="Forward Interfaces",
                query=(
                    'select {device: "device-1", name: "Ethernet1", '
                    'type: "other", enabled: true}'
                ),
            )
        ]
        planner = ForwardMultiBranchPlanner(
            sync=self.sync,
            client=client,
            logger_=Mock(),
        )

        _context, plan = planner.build_plan(
            max_changes_per_branch=10,
            run_preflight=False,
            model_strings=["dcim.interface"],
            shard_scope={
                "model": "dcim.interface",
                "query_name": "Forward Interfaces",
                "shard_keys": ["device:device-1", "device:device-2"],
            },
        )

        self.assertEqual(
            client.run_nqe_query.call_args.kwargs["column_filters"],
            [
                {
                    "operator": "EQUALS_ANY",
                    "columnName": "device",
                    "values": ["device-1", "device-2"],
                }
            ],
        )
        self.assertEqual(len(plan), 1)
        self.assertEqual(
            plan[0].upsert_rows,
            client.run_nqe_query.return_value[:2],
        )

    @patch("forward_netbox.utilities.query_fetch_execution.get_query_specs")
    def test_build_plan_preserves_interface_row_shape_during_shard_fetch(
        self,
        mock_specs,
    ):
        base_rows = [
            {
                "device": "device-1",
                "name": "Ethernet1",
                "type": "other",
                "enabled": True,
                "extra_debug": "kept",
            }
        ]
        snapshot = {
            "id": "snapshot-after",
            "state": "PROCESSED",
            "created_at": "",
            "processed_at": "2026-03-31T12:15:00Z",
        }
        full_client = Mock()
        full_client.get_snapshots.return_value = [snapshot]
        full_client.get_snapshot_metrics.return_value = {}
        full_client.run_nqe_query.return_value = list(base_rows)
        shard_client = Mock()
        shard_client.get_snapshots.return_value = [snapshot]
        shard_client.get_snapshot_metrics.return_value = {}
        shard_client.run_nqe_query.return_value = list(base_rows)
        self.sync.resolve_snapshot_id = lambda client=None: "snapshot-after"
        self.sync.get_model_strings = lambda: ["dcim.interface"]
        self.sync.incremental_diff_baseline = Mock(return_value=None)
        mock_specs.return_value = [
            QuerySpec(
                model_string="dcim.interface",
                query_name="Forward Interfaces",
                query=(
                    'select {device: "device-1", name: "Ethernet1", '
                    'type: "other", enabled: true}'
                ),
            )
        ]

        full_planner = ForwardMultiBranchPlanner(
            sync=self.sync,
            client=full_client,
            logger_=Mock(),
        )
        _full_context, full_plan = full_planner.build_plan(
            max_changes_per_branch=10,
            run_preflight=False,
            model_strings=["dcim.interface"],
        )
        shard_planner = ForwardMultiBranchPlanner(
            sync=self.sync,
            client=shard_client,
            logger_=Mock(),
        )
        _shard_context, shard_plan = shard_planner.build_plan(
            max_changes_per_branch=10,
            run_preflight=False,
            model_strings=["dcim.interface"],
            shard_scope={
                "model": "dcim.interface",
                "query_name": "Forward Interfaces",
                "shard_keys": ["device:device-1"],
            },
        )

        self.assertEqual(full_plan[0].upsert_rows, shard_plan[0].upsert_rows)
        self.assertEqual(
            set(full_plan[0].upsert_rows[0]),
            {"device", "name", "type", "enabled", "extra_debug"},
        )
        self.assertEqual(
            shard_client.run_nqe_query.call_args.kwargs["column_filters"],
            [
                {
                    "operator": "EQUALS_ANY",
                    "columnName": "device",
                    "values": ["device-1"],
                }
            ],
        )

    @patch("forward_netbox.utilities.query_fetch_execution.get_query_specs")
    def test_build_plan_preserves_prefix_row_shape_during_shard_fetch(
        self,
        mock_specs,
    ):
        base_rows = [
            {
                "prefix": "10.0.0.0/24",
                "vrf": "blue",
                "status": "active",
                "extra_debug": "kept",
            }
        ]
        snapshot = {
            "id": "snapshot-after",
            "state": "PROCESSED",
            "created_at": "",
            "processed_at": "2026-03-31T12:15:00Z",
        }
        full_client = Mock()
        full_client.get_snapshots.return_value = [snapshot]
        full_client.get_snapshot_metrics.return_value = {}
        full_client.run_nqe_query.return_value = list(base_rows)
        shard_client = Mock()
        shard_client.get_snapshots.return_value = [snapshot]
        shard_client.get_snapshot_metrics.return_value = {}
        shard_client.run_nqe_query.return_value = list(base_rows)
        self.sync.resolve_snapshot_id = lambda client=None: "snapshot-after"
        self.sync.get_model_strings = lambda: ["ipam.prefix"]
        self.sync.incremental_diff_baseline = Mock(return_value=None)
        mock_specs.return_value = [
            QuerySpec(
                model_string="ipam.prefix",
                query_name="Forward IPv4 Prefixes",
                query=(
                    'select {prefix: "10.0.0.0/24", vrf: "blue", ' 'status: "active"}'
                ),
            )
        ]

        full_planner = ForwardMultiBranchPlanner(
            sync=self.sync,
            client=full_client,
            logger_=Mock(),
        )
        _full_context, full_plan = full_planner.build_plan(
            max_changes_per_branch=10,
            run_preflight=False,
            model_strings=["ipam.prefix"],
        )
        shard_planner = ForwardMultiBranchPlanner(
            sync=self.sync,
            client=shard_client,
            logger_=Mock(),
        )
        _shard_context, shard_plan = shard_planner.build_plan(
            max_changes_per_branch=10,
            run_preflight=False,
            model_strings=["ipam.prefix"],
            shard_scope={
                "model": "ipam.prefix",
                "query_name": "Forward IPv4 Prefixes",
                "shard_keys": ["prefix=10.0.0.0/24|vrf=blue"],
            },
        )

        self.assertEqual(full_plan[0].upsert_rows, shard_plan[0].upsert_rows)
        self.assertEqual(
            set(full_plan[0].upsert_rows[0]),
            {"prefix", "vrf", "status", "extra_debug"},
        )
        self.assertEqual(
            shard_client.run_nqe_query.call_args.kwargs["parameters"],
            {"forward_netbox_shard_keys": ["10.0.0.0/24"]},
        )
        self.assertIsNone(shard_client.run_nqe_query.call_args.kwargs["column_filters"])

    @patch("forward_netbox.utilities.query_fetch_execution.get_query_specs")
    def test_build_plan_preserves_fallback_model_row_shape_during_shard_fetch(
        self,
        mock_specs,
    ):
        base_rows = [
            {
                "name": "site-1",
                "slug": "site-1",
                "extra_debug": "kept",
            }
        ]
        snapshot = {
            "id": "snapshot-after",
            "state": "PROCESSED",
            "created_at": "",
            "processed_at": "2026-03-31T12:15:00Z",
        }
        full_client = Mock()
        full_client.get_snapshots.return_value = [snapshot]
        full_client.get_snapshot_metrics.return_value = {}
        full_client.run_nqe_query.return_value = list(base_rows)
        shard_client = Mock()
        shard_client.get_snapshots.return_value = [snapshot]
        shard_client.get_snapshot_metrics.return_value = {}
        shard_client.run_nqe_query.return_value = list(base_rows)
        self.sync.resolve_snapshot_id = lambda client=None: "snapshot-after"
        self.sync.get_model_strings = lambda: ["dcim.site"]
        self.sync.incremental_diff_baseline = Mock(return_value=None)
        mock_specs.return_value = [
            QuerySpec(
                model_string="dcim.site",
                query_name="Forward Locations",
                query='select {name: "site-1", slug: "site-1"}',
            )
        ]

        full_planner = ForwardMultiBranchPlanner(
            sync=self.sync,
            client=full_client,
            logger_=Mock(),
        )
        _full_context, full_plan = full_planner.build_plan(
            max_changes_per_branch=10,
            run_preflight=False,
            model_strings=["dcim.site"],
        )
        shard_planner = ForwardMultiBranchPlanner(
            sync=self.sync,
            client=shard_client,
            logger_=Mock(),
        )
        _shard_context, shard_plan = shard_planner.build_plan(
            max_changes_per_branch=10,
            run_preflight=False,
            model_strings=["dcim.site"],
            shard_scope={
                "model": "dcim.site",
                "query_name": "Forward Locations",
                "shard_keys": ["slug=site-1"],
            },
        )

        self.assertEqual(full_plan[0].upsert_rows, shard_plan[0].upsert_rows)
        self.assertEqual(
            set(full_plan[0].upsert_rows[0]),
            {"name", "slug", "extra_debug"},
        )
        self.assertEqual(
            shard_client.run_nqe_query.call_args.kwargs["column_filters"],
            [
                {
                    "operator": "EQUALS_ANY",
                    "columnName": "slug",
                    "values": ["site-1"],
                }
            ],
        )
        self.assertEqual(
            shard_plan[0].upsert_rows,
            [base_rows[0]],
        )

    @patch("forward_netbox.utilities.query_fetch_execution.get_query_specs")
    def test_build_plan_preserves_row_shape_across_supported_models(
        self,
        mock_specs,
    ):
        snapshot = {
            "id": "snapshot-after",
            "state": "PROCESSED",
            "created_at": "",
            "processed_at": "2026-03-31T12:15:00Z",
        }
        self.sync.resolve_snapshot_id = lambda client=None: "snapshot-after"
        self.sync.incremental_diff_baseline = Mock(return_value=None)

        for model_string in FORWARD_SUPPORTED_MODELS:
            with self.subTest(model_string=model_string):
                contract = contract_for_model(model_string)
                target_row, _noise_row = self._shard_parity_case(model_string)
                self.assertTrue(set(contract.required_fields).issubset(target_row))
                full_client = Mock()
                full_client.get_snapshots.return_value = [snapshot]
                full_client.get_snapshot_metrics.return_value = {}
                full_client.run_nqe_query.return_value = [target_row, _noise_row]
                shard_client = Mock()
                shard_client.get_snapshots.return_value = [snapshot]
                shard_client.get_snapshot_metrics.return_value = {}
                shard_client.run_nqe_query.return_value = [target_row, _noise_row]
                mock_specs.return_value = [
                    QuerySpec(
                        model_string=model_string,
                        query_name=f"Forward {model_string}",
                        query=self._query_for_model(model_string, target_row),
                    )
                ]
                full_planner = ForwardMultiBranchPlanner(
                    sync=self.sync,
                    client=full_client,
                    logger_=Mock(),
                )
                _full_context, full_plan = full_planner.build_plan(
                    max_changes_per_branch=10,
                    run_preflight=False,
                    model_strings=[model_string],
                )
                shard_scope = {
                    "model": model_string,
                    "query_name": f"Forward {model_string}",
                    "shard_keys": [
                        row_shard_key(
                            model_string,
                            target_row,
                            default_coalesce_fields_for_model(model_string),
                        )
                    ],
                }
                shard_planner = ForwardMultiBranchPlanner(
                    sync=self.sync,
                    client=shard_client,
                    logger_=Mock(),
                )
                _shard_context, shard_plan = shard_planner.build_plan(
                    max_changes_per_branch=10,
                    run_preflight=False,
                    model_strings=[model_string],
                    shard_scope=shard_scope,
                )

                full_rows = [
                    row for plan_item in full_plan for row in plan_item.upsert_rows
                ]
                shard_rows = [
                    row for plan_item in shard_plan for row in plan_item.upsert_rows
                ]
                self.assertTrue(full_rows)
                self.assertIn(target_row, full_rows)
                self.assertEqual(shard_rows, [target_row])

    @patch("forward_netbox.utilities.query_fetch_execution.get_query_specs")
    def test_build_plan_uses_shard_scoped_query_parameters_for_ipam_prefix(
        self,
        mock_specs,
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
        client.run_nqe_query.return_value = [
            {"prefix": "10.0.0.0/24", "vrf": "blue", "status": "active"},
            {"prefix": "10.0.0.0/24", "vrf": "red", "status": "active"},
            {"prefix": "192.0.2.0/24", "vrf": "blue", "status": "active"},
        ]
        self.sync.resolve_snapshot_id = lambda client=None: "snapshot-after"
        self.sync.get_model_strings = lambda: ["ipam.prefix"]
        self.sync.incremental_diff_baseline = Mock(return_value=None)
        mock_specs.return_value = [
            QuerySpec(
                model_string="ipam.prefix",
                query_name="Forward IPv4 Prefixes",
                query='select {prefix: "10.0.0.0/24", vrf: "blue", status: "active"}',
            )
        ]
        planner = ForwardMultiBranchPlanner(
            sync=self.sync,
            client=client,
            logger_=Mock(),
        )

        _context, plan = planner.build_plan(
            max_changes_per_branch=10,
            run_preflight=False,
            model_strings=["ipam.prefix"],
            shard_scope={
                "model": "ipam.prefix",
                "query_name": "Forward IPv4 Prefixes",
                "shard_keys": ["prefix=10.0.0.0/24|vrf=blue"],
            },
        )

        self.assertEqual(
            client.run_nqe_query.call_args.kwargs["parameters"],
            {"forward_netbox_shard_keys": ["10.0.0.0/24"]},
        )
        self.assertIsNone(client.run_nqe_query.call_args.kwargs["column_filters"])
        self.assertEqual(len(plan), 1)
        self.assertEqual(plan[0].upsert_rows, [client.run_nqe_query.return_value[0]])

    @patch("forward_netbox.utilities.query_fetch_execution.get_query_specs")
    def test_build_plan_applies_shard_column_filter_to_nqe_diff(self, mock_specs):
        baseline = ForwardIngestion.objects.create(
            sync=self.sync,
            snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
            snapshot_id="snapshot-before",
            baseline_ready=True,
        )
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
        client.run_nqe_diff.return_value = [
            {
                "type": "ADDED",
                "before": None,
                "after": {
                    "device": "device-1",
                    "name": "Ethernet1",
                    "type": "other",
                    "enabled": True,
                },
            },
            {
                "type": "ADDED",
                "before": None,
                "after": {
                    "device": "device-3",
                    "name": "Ethernet1",
                    "type": "other",
                    "enabled": True,
                },
            },
        ]
        self.sync.resolve_snapshot_id = lambda client=None: "snapshot-after"
        self.sync.get_model_strings = lambda: ["dcim.interface"]
        self.sync.incremental_diff_baseline = Mock(return_value=baseline)
        mock_specs.return_value = [
            QuerySpec(
                model_string="dcim.interface",
                query_name="Forward Interfaces",
                query_id="Q_interfaces",
            )
        ]
        planner = ForwardMultiBranchPlanner(
            sync=self.sync,
            client=client,
            logger_=Mock(),
        )

        _context, plan = planner.build_plan(
            max_changes_per_branch=10,
            run_preflight=False,
            model_strings=["dcim.interface"],
            shard_scope={
                "model": "dcim.interface",
                "query_name": "Forward Interfaces",
                "shard_keys": ["device:device-1", "device:device-2"],
            },
        )

        client.run_nqe_diff.assert_called_once_with(
            query_id="Q_interfaces",
            commit_id=None,
            parameters={},
            before_snapshot_id="snapshot-before",
            after_snapshot_id="snapshot-after",
            column_filters=[
                {
                    "operator": "EQUALS_ANY",
                    "columnName": "device",
                    "values": ["device-1", "device-2"],
                }
            ],
            fetch_all=True,
        )
        client.run_nqe_query.assert_not_called()
        self.assertEqual(len(plan), 1)
        self.assertEqual(
            plan[0].upsert_rows,
            [client.run_nqe_diff.return_value[0]["after"]],
        )

    @patch("forward_netbox.utilities.query_fetch_execution.get_query_specs")
    def test_preflight_skips_invalid_model_before_full_fetch(self, mock_specs):
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

        _context, plan = planner.build_plan(
            max_changes_per_branch=10, run_preflight=True
        )

        client.run_nqe_query.assert_called_once()
        self.assertEqual(plan, [])
        self.assertEqual(planner.model_results[0]["model"], "dcim.site")
        self.assertEqual(planner.model_results[0]["failure_count"], 1)
        self.assertIn(
            "missing required fields: slug",
            planner.model_results[0]["diagnostics"][0]["message"],
        )

    @patch("forward_netbox.utilities.query_fetch_execution.get_query_specs")
    def test_virtual_chassis_failure_mentions_query_id_binding(self, mock_specs):
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
            [
                {
                    "device": "device-1",
                    "vc_name": "vc-1",
                    "name": "vc-1",
                    "vc_domain": "domain-1",
                    "vc_position": 1,
                },
                {
                    "device": "device-2",
                    "vc_name": "vc-1",
                    "name": "vc-1",
                    "vc_domain": "domain-1",
                    "vc_position": 1,
                },
            ],
            [
                {
                    "device": "device-1",
                    "vc_name": "vc-1",
                    "name": "vc-1",
                    "vc_domain": "domain-1",
                    "vc_position": 1,
                },
                {
                    "device": "device-2",
                    "vc_name": "vc-1",
                    "name": "vc-1",
                    "vc_domain": "domain-1",
                    "vc_position": 1,
                },
            ],
        ]
        self.sync.resolve_snapshot_id = lambda client=None: "snapshot-after"
        self.sync.get_model_strings = lambda: ["dcim.virtualchassis"]
        self.sync.incremental_diff_baseline = Mock(return_value=None)
        mock_specs.return_value = [
            QuerySpec(
                model_string="dcim.virtualchassis",
                query_name="Forward Virtual Chassis",
                query_id="Q_virtual_chassis",
            )
        ]
        planner = ForwardMultiBranchPlanner(
            sync=self.sync,
            client=client,
            logger_=Mock(),
        )

        _context, plan = planner.build_plan(
            max_changes_per_branch=10, run_preflight=True
        )

        self.assertEqual(plan, [])
        self.assertEqual(planner.model_results[0]["model"], "dcim.virtualchassis")
        self.assertIn(
            "query_id `Q_virtual_chassis`",
            planner.model_results[0]["diagnostics"][0]["message"],
        )
        self.assertIn(
            "will not rewrite the published Forward query",
            planner.model_results[0]["diagnostics"][0]["message"],
        )

    @patch("forward_netbox.utilities.query_fetch_execution.get_query_specs")
    def test_preflight_failure_for_one_model_still_plans_later_models(self, mock_specs):
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

        def run_nqe_query(*, query=None, fetch_all=False, **_kwargs):
            if "site-1" in query:
                return [{"name": "site-1"}]
            return [
                {
                    "device": "device-1",
                    "local_asn": 64512,
                    "neighbor_address": "192.0.2.1",
                    "peer_asn": 64513,
                    "enabled": True,
                    "status": "active",
                }
            ]

        def query_specs(model_string, maps=None):
            if model_string == "dcim.site":
                return [
                    QuerySpec(
                        model_string="dcim.site",
                        query_name="Forward Sites",
                        query='select {name: "site-1"}',
                    )
                ]
            return [
                QuerySpec(
                    model_string="netbox_routing.bgppeer",
                    query_name="Forward BGP Peers",
                    query=(
                        'select {device: "device-1", local_asn: 64512, '
                        'neighbor_address: "192.0.2.1", peer_asn: 64513, '
                        'enabled: true, status: "active"}'
                    ),
                )
            ]

        client.run_nqe_query.side_effect = run_nqe_query
        mock_specs.side_effect = query_specs
        self.sync.resolve_snapshot_id = lambda client=None: "snapshot-after"
        self.sync.get_model_strings = lambda: ["dcim.site", "netbox_routing.bgppeer"]
        self.sync.incremental_diff_baseline = Mock(return_value=None)
        planner = ForwardMultiBranchPlanner(
            sync=self.sync,
            client=client,
            logger_=Mock(),
        )

        _context, plan = planner.build_plan(
            max_changes_per_branch=10, run_preflight=True
        )

        self.assertEqual(
            [item.model_string for item in plan], ["netbox_routing.bgppeer"]
        )
        failures = [
            result for result in planner.model_results if result["model"] == "dcim.site"
        ]
        self.assertEqual(failures[0]["failure_count"], 1)

    @patch("forward_netbox.utilities.query_fetch_execution.get_query_specs")
    def test_duplicate_virtual_chassis_positions_skip_model_not_later_models(
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

        def run_nqe_query(*, query=None, fetch_all=False, **_kwargs):
            if "vc-1" in query:
                if not fetch_all:
                    return [
                        {
                            "device": "device-1",
                            "vc_name": "vc-1",
                            "name": "vc-1",
                            "vc_domain": "100",
                            "vc_position": 1,
                        }
                    ]
                return [
                    {
                        "device": "device-1",
                        "vc_name": "vc-1",
                        "name": "vc-1",
                        "vc_domain": "100",
                        "vc_position": 1,
                    },
                    {
                        "device": "device-2",
                        "vc_name": "vc-1",
                        "name": "vc-1",
                        "vc_domain": "100",
                        "vc_position": 1,
                    },
                ]
            return [
                {
                    "device": "device-3",
                    "local_asn": 64512,
                    "neighbor_address": "192.0.2.1",
                    "peer_asn": 64513,
                    "enabled": True,
                    "status": "active",
                }
            ]

        def query_specs(model_string, maps=None):
            if model_string == "dcim.virtualchassis":
                return [
                    QuerySpec(
                        model_string="dcim.virtualchassis",
                        query_name="Forward Virtual Chassis",
                        query=(
                            'select {device: "device-1", vc_name: "vc-1", '
                            'name: "vc-1", vc_domain: "100", vc_position: 1}'
                        ),
                    )
                ]
            return [
                QuerySpec(
                    model_string="netbox_routing.bgppeer",
                    query_name="Forward BGP Peers",
                    query=(
                        'select {device: "device-3", local_asn: 64512, '
                        'neighbor_address: "192.0.2.1", peer_asn: 64513, '
                        'enabled: true, status: "active"}'
                    ),
                )
            ]

        client.run_nqe_query.side_effect = run_nqe_query
        mock_specs.side_effect = query_specs
        self.sync.resolve_snapshot_id = lambda client=None: "snapshot-after"
        self.sync.get_model_strings = lambda: [
            "dcim.virtualchassis",
            "netbox_routing.bgppeer",
        ]
        self.sync.incremental_diff_baseline = Mock(return_value=None)
        planner = ForwardMultiBranchPlanner(
            sync=self.sync,
            client=client,
            logger_=Mock(),
        )

        _context, plan = planner.build_plan(
            max_changes_per_branch=10, run_preflight=True
        )

        self.assertEqual(
            [item.model_string for item in plan], ["netbox_routing.bgppeer"]
        )
        failures = [
            result
            for result in planner.model_results
            if result["model"] == "dcim.virtualchassis"
        ]
        self.assertEqual(failures[0]["failure_count"], 1)
        self.assertIn(
            "Duplicate virtual chassis position",
            failures[0]["diagnostics"][0]["message"],
        )

    def test_preflight_error_explains_disabled_optional_module_map(self):
        site_type = ContentType.objects.get(app_label="dcim", model="site")
        module_type = ContentType.objects.get(app_label="dcim", model="module")
        ForwardNQEMap.objects.create(
            name="Forward Locations",
            netbox_model=site_type,
            query='select {name: "site-1", slug: "site-1"}',
            coalesce_fields=[["name"]],
            enabled=True,
            built_in=True,
        )
        ForwardNQEMap.objects.create(
            name="Forward Modules",
            netbox_model=module_type,
            query='select {device: "device-1", module_bay: "Slot 1"}',
            coalesce_fields=[["device", "module_bay"]],
            enabled=False,
            built_in=True,
        )
        ForwardNQEMap.objects.filter(
            name="Forward Modules",
            netbox_model=module_type,
            built_in=True,
        ).update(enabled=False)
        self.sync.get_model_strings = lambda: ["dcim.module"]
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
        self.sync.resolve_snapshot_id = lambda client=None: "snapshot-after"
        planner = ForwardMultiBranchPlanner(
            sync=self.sync,
            client=client,
            logger_=Mock(),
        )

        _context, plan = planner.build_plan(
            max_changes_per_branch=10, run_preflight=True
        )

        self.assertEqual(plan, [])
        self.assertEqual(planner.model_results[0]["model"], "dcim.module")
        self.assertEqual(planner.model_results[0]["failure_count"], 1)
        self.assertIn(
            "Enable the `Forward Modules` NQE Map or disable the `dcim.module` model",
            planner.model_results[0]["diagnostics"][0]["message"],
        )

    @patch("forward_netbox.utilities.query_fetch_execution.get_query_specs")
    def test_build_plan_handles_multiple_specs_with_shared_model(self, mock_specs):
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
            [{"name": "site-2", "slug": "site-2"}],
            [{"name": "site-1", "slug": "site-1"}],
            [{"name": "site-2", "slug": "site-2"}],
        ]
        self.sync.resolve_snapshot_id = lambda client=None: "snapshot-after"
        self.sync.get_model_strings = lambda: ["dcim.site"]
        self.sync.incremental_diff_baseline = Mock(return_value=None)
        mock_specs.return_value = [
            QuerySpec(
                model_string="dcim.site",
                query_name="Forward Sites A",
                query='select {name: "site-1", slug: "site-1"}',
            ),
            QuerySpec(
                model_string="dcim.site",
                query_name="Forward Sites B",
                query='select {name: "site-2", slug: "site-2"}',
            ),
        ]
        planner = ForwardMultiBranchPlanner(
            sync=self.sync,
            client=client,
            logger_=Mock(),
        )

        context, plan = planner.build_plan(
            max_changes_per_branch=10, run_preflight=True
        )

        self.assertEqual(context["snapshot_id"], "snapshot-after")
        self.assertEqual(len(plan), 2)
        self.assertEqual(
            [result["query_name"] for result in planner.model_results],
            [
                "Forward Sites A",
                "Forward Sites B",
            ],
        )
        self.assertEqual(client.run_nqe_query.call_count, 4)
        self.assertEqual(
            sum(
                1
                for call in client.run_nqe_query.call_args_list
                if call.kwargs["fetch_all"]
            ),
            2,
        )
        self.assertEqual(
            sum(
                1
                for call in client.run_nqe_query.call_args_list
                if call.kwargs.get("limit") == DEFAULT_PREFLIGHT_ROW_LIMIT
                and not call.kwargs["fetch_all"]
            ),
            2,
        )

    @patch("forward_netbox.utilities.query_fetch_execution.get_query_specs")
    def test_build_plan_records_unassignable_ipaddress_diagnostics(self, mock_specs):
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
            [
                {
                    "device": "device-1",
                    "interface": "Ethernet1/1",
                    "address": "10.0.0.1/24",
                    "vrf": None,
                    "status": "active",
                }
            ],
            [
                {
                    "reason": "ipv4-subnet-network-id",
                    "device": "device-1",
                    "interface": "VLAN699",
                    "address": "11.138.0.16/28",
                },
                {
                    "reason": "ipv4-broadcast-address",
                    "device": "device-1",
                    "interface": "VLAN699",
                    "address": "11.138.0.31/28",
                },
            ],
        ]
        self.sync.resolve_snapshot_id = lambda client=None: "snapshot-after"
        self.sync.get_model_strings = lambda: ["ipam.ipaddress"]
        self.sync.incremental_diff_baseline = Mock(return_value=None)
        mock_specs.return_value = [
            QuerySpec(
                model_string="ipam.ipaddress",
                query_name="Forward IP Addresses",
                query=(
                    'select {device: "device-1", interface: "Ethernet1/1", '
                    'address: ipSubnet("10.0.0.1/24"), vrf: null:String, '
                    'status: "active"}'
                ),
            )
        ]
        logger = Mock()
        planner = ForwardMultiBranchPlanner(
            sync=self.sync,
            client=client,
            logger_=logger,
        )

        planner.build_plan(max_changes_per_branch=10, run_preflight=False)

        diagnostic = planner.model_results[0]["diagnostics"][0]
        self.assertEqual(diagnostic["total"], 2)
        self.assertEqual(
            diagnostic["counts"],
            {
                "ipv4-subnet-network-id": 1,
                "ipv4-broadcast-address": 1,
            },
        )
        self.assertEqual(len(diagnostic["examples"]), 2)
        warning_messages = [call.args[0] for call in logger.log_warning.call_args_list]
        self.assertIn("filtered 2 interface addresses", warning_messages[0])
        self.assertIn("11.138.0.16/28", warning_messages[1])
        self.assertIn("11.138.0.31/28", warning_messages[2])

    @patch("forward_netbox.utilities.query_fetch_execution.get_query_specs")
    def test_build_plan_records_routing_import_diagnostics(self, mock_specs):
        client = Mock()
        client.get_snapshots.return_value = [
            {
                "id": "snapshot-after",
                "state": "PROCESSED",
                "created_at": "",
                "processed_at": "2026-05-06T12:15:00Z",
            }
        ]
        client.get_snapshot_metrics.return_value = {}
        client.run_nqe_query.side_effect = [
            [
                {
                    "device": "device-1",
                    "vrf": None,
                    "local_asn": 64512,
                    "router_id": "192.0.2.254",
                    "neighbor_address": "192.0.2.1",
                    "peer_asn": 64513,
                    "afi_safi": "AfiSafiType.IPV4_UNICAST",
                }
            ],
            [
                {
                    "reason": "bgp-neighbor-without-local-as",
                    "model_target": "netbox_routing.bgppeer",
                    "protocol": "bgp",
                    "device": "device-3",
                    "interface": "",
                    "detail": "Forward did not expose localAS on the neighbor or asNumber on the BGP process.",
                    "count": 3,
                },
                {
                    "reason": "bgp-unsupported-address-family",
                    "model_target": "netbox_routing.bgpaddressfamily",
                    "protocol": "bgp",
                    "device": "device-1",
                    "interface": "",
                    "detail": "AfiSafiType.IPV4_MDT",
                    "count": 7,
                },
                {
                    "reason": "ospf-neighbor-without-reverse-peer",
                    "model_target": "netbox_routing.ospfinstance",
                    "protocol": "ospf",
                    "device": "device-2",
                    "interface": "Ethernet1/1",
                    "detail": "Forward did not expose the reverse OSPF neighbor.",
                },
            ],
        ]
        self.sync.resolve_snapshot_id = lambda client=None: "snapshot-after"
        self.sync.get_model_strings = lambda: ["netbox_routing.bgpaddressfamily"]
        self.sync.incremental_diff_baseline = Mock(return_value=None)
        mock_specs.return_value = [
            QuerySpec(
                model_string="netbox_routing.bgpaddressfamily",
                query_name="Forward BGP Address Families",
                query=(
                    'select {device: "device-1", vrf: null:String, '
                    'local_asn: 64512, router_id: ipAddress("192.0.2.254"), '
                    'neighbor_address: ipAddress("192.0.2.1"), '
                    'peer_asn: 64513, afi_safi: "AfiSafiType.IPV4_UNICAST"}'
                ),
            )
        ]
        logger = Mock()
        planner = ForwardMultiBranchPlanner(
            sync=self.sync,
            client=client,
            logger_=logger,
        )

        planner.build_plan(max_changes_per_branch=10, run_preflight=False)

        diagnostic = planner.model_results[0]["diagnostics"][0]
        self.assertEqual(diagnostic["name"], "routing_import_skipped_rows")
        self.assertEqual(diagnostic["total"], 11)
        self.assertEqual(
            diagnostic["counts"],
            {
                "bgp-neighbor-without-local-as": 3,
                "bgp-unsupported-address-family": 7,
                "ospf-neighbor-without-reverse-peer": 1,
            },
        )
        self.assertEqual(len(diagnostic["examples"]), 3)
        warning_messages = [call.args[0] for call in logger.log_warning.call_args_list]
        self.assertIn("beta routing maps cannot import", warning_messages[0])
        self.assertIn("BGP neighbors without local AS", warning_messages[0])
        self.assertIn("BGP unsupported address families", warning_messages[0])
        self.assertIn(
            "OSPF neighbors without reverse peer inference", warning_messages[0]
        )

    @patch("forward_netbox.utilities.query_fetch_execution.get_query_specs")
    def test_build_plan_attaches_routing_diagnostics_to_bgp_peer_results(
        self, mock_specs
    ):
        client = Mock()
        client.get_snapshots.return_value = [
            {
                "id": "snapshot-after",
                "state": "PROCESSED",
                "created_at": "",
                "processed_at": "2026-05-06T12:15:00Z",
            }
        ]
        client.get_snapshot_metrics.return_value = {}
        client.run_nqe_query.side_effect = [
            [
                {
                    "device": "device-1",
                    "vrf": None,
                    "local_asn": 64512,
                    "neighbor_address": "192.0.2.1",
                    "peer_asn": 64513,
                    "enabled": True,
                    "status": "active",
                }
            ],
            [
                {
                    "reason": "bgp-neighbor-without-local-as",
                    "model_target": "netbox_routing.bgppeer",
                    "protocol": "bgp",
                    "device": "device-2",
                    "interface": "",
                    "detail": "Forward did not expose localAS on the neighbor or asNumber on the BGP process.",
                    "count": 2,
                }
            ],
        ]
        self.sync.resolve_snapshot_id = lambda client=None: "snapshot-after"
        self.sync.get_model_strings = lambda: ["netbox_routing.bgppeer"]
        self.sync.incremental_diff_baseline = Mock(return_value=None)
        mock_specs.return_value = [
            QuerySpec(
                model_string="netbox_routing.bgppeer",
                query_name="Forward BGP Peers",
                query=(
                    'select {device: "device-1", vrf: null:String, '
                    'local_asn: 64512, neighbor_address: ipAddress("192.0.2.1"), '
                    'peer_asn: 64513, enabled: true, status: "active"}'
                ),
            )
        ]
        planner = ForwardMultiBranchPlanner(
            sync=self.sync,
            client=client,
            logger_=Mock(),
        )

        planner.build_plan(max_changes_per_branch=10, run_preflight=False)

        diagnostic = planner.model_results[0]["diagnostics"][0]
        self.assertEqual(diagnostic["counts"], {"bgp-neighbor-without-local-as": 2})


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
                "enable_bulk_orm": False,
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

    def test_split_overflow_item_uses_delete_heavy_row_budget(self):
        workload = BranchWorkload(
            model_string="dcim.device",
            label="dcim.device | Forward Devices",
            delete_rows=[{"name": f"device-{index}"} for index in range(2000)],
            coalesce_fields=[["name"]],
        )
        item = build_branch_plan([workload], max_changes_per_branch=5000)[0]
        executor = ForwardMultiBranchExecutor(
            sync=self.sync,
            client=Mock(),
            logger_=Mock(),
        )
        executor.max_changes_per_branch = 10000
        executor.model_change_density = {}

        split_items = executor._split_overflow_item(item)

        self.assertGreater(len(split_items), 1)
        self.assertTrue(all(part.estimated_changes <= 500 for part in split_items))
        self.assertTrue(all(part.operation == "delete" for part in split_items))

    @patch("forward_netbox.utilities.multi_branch_executor.ForwardMultiBranchPlanner")
    def test_plan_accepts_explicit_empty_branch_run_state_without_sync_fallback(
        self,
        mock_planner_cls,
    ):
        planner = mock_planner_cls.return_value
        planner.build_plan.return_value = ({}, [])
        executor = ForwardMultiBranchExecutor(
            sync=self.sync,
            client=Mock(),
            logger_=Mock(),
        )
        parameters = dict(self.sync.parameters or {})
        parameters["_branch_run"] = {
            "next_plan_index": 99,
            "total_plan_items": 99,
        }
        self.sync.parameters = parameters
        self.sync.save(update_fields=["parameters"])

        executor.plan(
            max_changes_per_branch=10000,
            run_preflight=False,
            branch_run_state={},
        )

        self.assertEqual(
            mock_planner_cls.call_args.kwargs["branch_run_state"],
            {},
        )

    def test_select_plan_item_uses_operation_to_resume_delete_shard(self):
        plan = build_branch_plan(
            [
                BranchWorkload(
                    model_string="dcim.device",
                    label="dcim.device | Forward Devices",
                    upsert_rows=[{"name": "device-1"}],
                    delete_rows=[{"name": "device-1"}],
                    coalesce_fields=[["name"]],
                )
            ],
            max_changes_per_branch=10000,
        )
        executor = ForwardMultiBranchExecutor(
            sync=self.sync,
            client=Mock(),
            logger_=Mock(),
        )

        selected = executor._select_plan_item(
            plan,
            {
                "model": "dcim.device",
                "query_name": "",
                "execution_value": "",
                "operation": "delete",
                "shard_keys": ["name=device-1"],
            },
            2,
        )

        self.assertIsNotNone(selected)
        self.assertEqual(selected.operation, "delete")

    def test_select_plan_item_recombines_persisted_shard_subsets(self):
        plan = [
            BranchPlanItem(
                index=1,
                model_string="ipam.prefix",
                label="prefix shard part 1",
                estimated_changes=1,
                upsert_rows=[{"prefix": "10.0.0.0/24"}],
                delete_rows=[],
                sync_mode="full",
                coalesce_fields=[["prefix"]],
                shard_keys=("prefix=10.0.0.0/24",),
                query_name="Forward Prefixes",
                execution_mode="query_id",
                execution_value="Q_prefixes",
            ),
            BranchPlanItem(
                index=2,
                model_string="ipam.prefix",
                label="prefix shard part 2",
                estimated_changes=1,
                upsert_rows=[{"prefix": "10.0.1.0/24"}],
                delete_rows=[],
                sync_mode="full",
                coalesce_fields=[["prefix"]],
                shard_keys=("prefix=10.0.1.0/24",),
                query_name="Forward Prefixes",
                execution_mode="query_id",
                execution_value="Q_prefixes",
            ),
        ]
        executor = ForwardMultiBranchExecutor(
            sync=self.sync,
            client=Mock(),
            logger_=Mock(),
        )

        selected = executor._select_plan_item(
            plan,
            {
                "index": 46,
                "model": "ipam.prefix",
                "label": "persisted prefix shard",
                "query_name": "Forward Prefixes",
                "execution_value": "Q_prefixes",
                "execution_mode": "query_id",
                "operation": "mixed",
                "estimated_changes": 2,
                "sync_mode": "full",
                "shard_keys": [
                    "prefix=10.0.0.0/24",
                    "prefix=10.0.1.0/24",
                ],
                "fetch_mode": "nqe_column_filter",
                "fetch_key_family": "prefix",
                "fetch_parameters": {"shard_hint": "prefix"},
                "query_parameters": {},
                "fetch_column_filters": [
                    {
                        "operator": "EQUALS_ANY",
                        "columnName": "prefix",
                        "values": ["10.0.0.0/24", "10.0.1.0/24"],
                    }
                ],
            },
            46,
        )

        self.assertIsNotNone(selected)
        self.assertEqual(selected.index, 46)
        self.assertEqual(selected.label, "persisted prefix shard")
        self.assertEqual(selected.estimated_changes, 2)
        self.assertEqual(len(selected.upsert_rows), 2)
        self.assertEqual(
            set(selected.shard_keys),
            {"prefix=10.0.0.0/24", "prefix=10.0.1.0/24"},
        )
        self.assertEqual(selected.fetch_mode, "nqe_column_filter")

    @override_settings(RQ_DEFAULT_TIMEOUT=300)
    def test_load_execution_context_warns_for_large_plan_with_short_worker_timeout(
        self,
    ):
        logger = Mock()
        executor = ForwardMultiBranchExecutor(
            sync=self.sync,
            client=Mock(),
            logger_=logger,
        )
        executor.max_changes_per_branch = 10
        executor.max_changes_per_branch = 10
        executor.plan = Mock(
            return_value=(
                {
                    "snapshot_selector": LATEST_PROCESSED_SNAPSHOT,
                    "snapshot_id": self.SNAPSHOT_ID,
                },
                build_branch_plan(
                    [
                        BranchWorkload(
                            model_string="dcim.device",
                            label="dcim.device | Forward Devices",
                            upsert_rows=[
                                {"name": f"device-{index}"} for index in range(20)
                            ],
                            coalesce_fields=[["name"]],
                        )
                    ],
                    max_changes_per_branch=10,
                ),
            )
        )

        with patch(
            "forward_netbox.utilities.multi_branch_executor.ForwardValidationRunner"
        ) as mock_validation_runner:
            mock_validation_runner.return_value.record_plan_validation.return_value = (
                Mock(pk=1)
            )
            executor._load_execution_context(max_changes_per_branch=10)

        warning_messages = [
            call.args[0]
            for call in logger.log_warning.call_args_list
            if "RQ_DEFAULT_TIMEOUT is only 300s" in call.args[0]
        ]
        self.assertEqual(len(warning_messages), 1)

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

    def test_soft_budget_limit_allows_guideline_overrun(self):
        self.assertEqual(soft_budget_limit(10000), 10500)

    def test_run_plan_item_allows_small_budget_overrun(self):
        workload = BranchWorkload(
            model_string="dcim.site",
            label="dcim.site | Forward Sites",
            upsert_rows=[{"name": "Site A", "slug": "site-a"}],
            coalesce_fields=[["slug"]],
        )
        item = build_branch_plan([workload], max_changes_per_branch=10000)[0]
        logger = Mock()
        logger.log_data = {"statistics": {"dcim.site": {"current": 1, "applied": 1}}}
        executor = ForwardMultiBranchExecutor(
            sync=self.sync,
            client=Mock(),
            logger_=logger,
        )
        executor.max_changes_per_branch = 10000
        executor.model_change_density = {}
        context = {
            "snapshot_selector": LATEST_PROCESSED_SNAPSHOT,
            "snapshot_id": self.SNAPSHOT_ID,
            "snapshot_info": {},
            "snapshot_metrics": {},
        }

        with patch(
            "forward_netbox.utilities.multi_branch_lifecycle.run_item_in_branch"
        ), patch.object(Branch, "provision"), patch.object(
            Branch, "refresh_from_db"
        ), patch.object(
            Branch, "get_unmerged_changes"
        ) as mock_changes, patch.object(
            ForwardIngestion,
            "sync_merge",
            autospec=True,
            return_value=None,
        ):
            mock_changes.return_value.count.return_value = 10272
            ingestion = executor._run_plan_item(
                item,
                context,
                mark_baseline_ready=False,
                merge=True,
                total_plan_items=1,
                plan_preview={},
            )

        self.assertIsNotNone(ingestion)
        warning_messages = [call.args[0] for call in logger.log_warning.call_args_list]
        self.assertTrue(
            any("soft overrun limit" in message for message in warning_messages)
        )

    def test_run_plan_item_still_raises_for_large_budget_overrun(self):
        workload = BranchWorkload(
            model_string="dcim.site",
            label="dcim.site | Forward Sites",
            upsert_rows=[{"name": "Site A", "slug": "site-a"}],
            coalesce_fields=[["slug"]],
        )
        item = build_branch_plan([workload], max_changes_per_branch=10000)[0]
        logger = Mock()
        logger.log_data = {"statistics": {"dcim.site": {"current": 1, "applied": 1}}}
        executor = ForwardMultiBranchExecutor(
            sync=self.sync,
            client=Mock(),
            logger_=logger,
        )
        executor.max_changes_per_branch = 10000
        executor.model_change_density = {}
        context = {
            "snapshot_selector": LATEST_PROCESSED_SNAPSHOT,
            "snapshot_id": self.SNAPSHOT_ID,
            "snapshot_info": {},
            "snapshot_metrics": {},
        }

        with patch(
            "forward_netbox.utilities.multi_branch_lifecycle.run_item_in_branch"
        ), patch.object(Branch, "provision"), patch.object(
            Branch, "refresh_from_db"
        ), patch.object(
            Branch, "get_unmerged_changes"
        ) as mock_changes:
            mock_changes.return_value.count.return_value = 11000
            with self.assertRaises(BranchBudgetExceeded):
                executor._run_plan_item(
                    item,
                    context,
                    mark_baseline_ready=False,
                    merge=True,
                    total_plan_items=1,
                    plan_preview={},
                )

    @patch("forward_netbox.utilities.sync_facade.enqueue_branch_stage_job")
    def test_enqueue_sync_job_continues_from_ledger_without_plan_items(
        self,
        mock_enqueue_stage,
    ):
        execution_run = ForwardExecutionRun.objects.create(
            sync=self.sync,
            source=self.source,
            backend="branching",
            status="running",
            snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
            snapshot_id=self.SNAPSHOT_ID,
            total_steps=2,
            next_step_index=2,
        )
        ForwardExecutionStep.objects.create(
            run=execution_run,
            index=1,
            kind="stage",
            status=ForwardExecutionStepStatusChoices.MERGED,
            model_string="dcim.site",
        )
        ForwardExecutionStep.objects.create(
            run=execution_run,
            index=2,
            kind="stage",
            status=ForwardExecutionStepStatusChoices.PENDING,
            model_string="dcim.device",
        )
        self.sync.clear_branch_run_state()

        enqueue_sync_job(self.sync, adhoc=True, user=None)

        mock_enqueue_stage.assert_called_once_with(
            self.sync,
            user=self.sync.user,
            adhoc=True,
        )

    @patch("forward_netbox.utilities.multi_branch_executor.enqueue_branch_stage_job")
    @patch("forward_netbox.utilities.multi_branch_executor.ForwardValidationRunner")
    def test_job_backed_run_queues_first_shard_using_execution_ledger(
        self,
        mock_validation_runner,
        mock_enqueue_stage,
    ):
        workload = BranchWorkload(
            model_string="dcim.device",
            label="dcim.device | Forward Devices",
            upsert_rows=[{"name": f"device-{index}"} for index in range(8)],
            coalesce_fields=[["name"]],
        )
        plan = build_branch_plan([workload], max_changes_per_branch=4)
        validation_run = ForwardValidationRun.objects.create(
            sync=self.sync,
            status="passed",
            allowed=True,
            snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
            snapshot_id=self.SNAPSHOT_ID,
        )
        mock_validation_runner.return_value.record_plan_validation.return_value = (
            validation_run
        )
        executor = ForwardMultiBranchExecutor(
            sync=self.sync,
            client=Mock(),
            logger_=Mock(),
            job=Mock(pk=123),
        )
        context = {
            "snapshot_selector": LATEST_PROCESSED_SNAPSHOT,
            "snapshot_id": self.SNAPSHOT_ID,
            "snapshot_info": {},
            "snapshot_metrics": {},
        }
        planning_ingestion = ForwardIngestion.objects.create(sync=self.sync)
        executor.plan = Mock(return_value=(context, plan))
        executor._create_planning_ingestion = Mock(return_value=planning_ingestion)
        executor._run_plan_item = Mock()

        ingestions = executor.run(max_changes_per_branch=4)

        self.assertEqual(ingestions, [planning_ingestion])
        self.assertTrue(executor.resumable_started)
        executor._run_plan_item.assert_not_called()
        mock_enqueue_stage.assert_called_once_with(
            self.sync,
            user=None,
            adhoc=True,
        )
        state = self.sync.get_branch_run_state()
        self.assertEqual(state, {})
        execution_run = ForwardExecutionRun.objects.get(sync=self.sync)
        self.assertEqual(execution_run.sync, self.sync)
        self.assertEqual(execution_run.snapshot_id, self.SNAPSHOT_ID)
        self.assertEqual(execution_run.total_steps, len(plan))
        self.assertEqual(execution_run.next_step_index, 1)
        self.assertEqual(execution_run.plan_preview["planned_shards"], len(plan))
        self.assertEqual(execution_run.steps.count(), len(plan))
        first_step = execution_run.steps.order_by("index").first()
        self.assertEqual(first_step.model_string, "dcim.device")
        self.assertEqual(first_step.estimated_changes, plan[0].estimated_changes)
        self.assertEqual(first_step.fetched_row_count, plan[0].estimated_changes)
        self.assertEqual(first_step.apply_engine, "adapter")

    @patch("forward_netbox.utilities.multi_branch_executor.ForwardValidationRunner")
    def test_direct_run_persists_execution_ledger_before_applying_shard(
        self,
        mock_validation_runner,
    ):
        workload = BranchWorkload(
            model_string="dcim.device",
            label="dcim.device | Forward Devices",
            upsert_rows=[{"name": f"device-{index}"} for index in range(8)],
            coalesce_fields=[["name"]],
        )
        plan = build_branch_plan([workload], max_changes_per_branch=4)
        validation_run = ForwardValidationRun.objects.create(
            sync=self.sync,
            status="passed",
            allowed=True,
            snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
            snapshot_id=self.SNAPSHOT_ID,
        )
        mock_validation_runner.return_value.record_plan_validation.return_value = (
            validation_run
        )
        executor = ForwardMultiBranchExecutor(
            sync=self.sync,
            client=Mock(),
            logger_=Mock(),
        )
        context = {
            "snapshot_selector": LATEST_PROCESSED_SNAPSHOT,
            "snapshot_id": self.SNAPSHOT_ID,
            "snapshot_info": {},
            "snapshot_metrics": {},
        }
        staged_ingestion = ForwardIngestion.objects.create(sync=self.sync)
        executor.plan = Mock(return_value=(context, plan))

        def _stage_first_item(*args, **kwargs):
            self.assertEqual(self.sync.get_branch_run_state(), {})
            run = ForwardExecutionRun.objects.get(sync=self.sync)
            self.assertEqual(run.total_steps, len(plan))
            self.assertEqual(run.steps.count(), len(plan))
            step = run.steps.get(index=1)
            self.assertEqual(step.status, ForwardExecutionStepStatusChoices.RUNNING)
            return staged_ingestion

        executor._run_plan_item = Mock(side_effect=_stage_first_item)

        ingestions = executor.run(max_changes_per_branch=4)

        self.assertEqual(ingestions, [staged_ingestion])
        self.assertEqual(self.sync.get_branch_run_state(), {})
        execution_run = ForwardExecutionRun.objects.get(sync=self.sync)
        self.assertEqual(execution_run.validation_run, validation_run)
        self.assertEqual(execution_run.snapshot_id, self.SNAPSHOT_ID)
        self.assertEqual(execution_run.total_steps, len(plan))
        self.assertEqual(execution_run.next_step_index, 1)

    def test_update_plan_item_state_ignores_completed_historical_ledger(self):
        execution_run = ForwardExecutionRun.objects.create(
            sync=self.sync,
            source=self.source,
            backend="branching",
            status="completed",
            snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
            snapshot_id=self.SNAPSHOT_ID,
            total_steps=1,
            next_step_index=2,
        )
        step = ForwardExecutionStep.objects.create(
            run=execution_run,
            index=1,
            kind="stage",
            status=ForwardExecutionStepStatusChoices.MERGED,
            model_string="dcim.device",
            label="dcim.device | Forward Devices",
            estimated_changes=1,
        )
        self.sync.clear_branch_run_state()

        updated = update_plan_item_state(
            self.sync,
            1,
            status="failed",
            last_error="should not touch old run",
        )

        self.assertFalse(updated)
        step.refresh_from_db()
        self.assertEqual(step.status, ForwardExecutionStepStatusChoices.MERGED)
        self.assertEqual(step.last_error, "")

    def test_run_next_plan_item_stages_one_shard(self):
        workload = BranchWorkload(
            model_string="dcim.device",
            label="dcim.device | Forward Devices",
            upsert_rows=[{"name": f"device-{index}"} for index in range(8)],
            coalesce_fields=[["name"]],
        )
        plan = build_branch_plan([workload], max_changes_per_branch=4)
        validation_run = ForwardValidationRun.objects.create(
            sync=self.sync,
            status="passed",
            allowed=True,
            snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
            snapshot_id=self.SNAPSHOT_ID,
        )
        execution_run = ForwardExecutionRun.objects.create(
            sync=self.sync,
            source=self.source,
            backend="branching",
            status="running",
            snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
            snapshot_id=self.SNAPSHOT_ID,
            total_steps=len(plan),
            next_step_index=1,
            validation_run=validation_run,
        )
        for item in plan:
            ForwardExecutionStep.objects.create(
                run=execution_run,
                index=item.index,
                kind="stage",
                status=ForwardExecutionStepStatusChoices.PENDING,
                model_string=item.model_string,
                label=item.label,
                query_name=item.query_name,
                execution_mode=item.execution_mode,
                execution_value=item.execution_value,
                shard_keys=list(item.shard_keys),
                estimated_changes=item.estimated_changes,
            )
        self.sync.clear_branch_run_state()
        executor = ForwardMultiBranchExecutor(
            sync=self.sync,
            client=Mock(),
            logger_=Mock(),
            job=Mock(pk=123),
        )
        context = {
            "snapshot_selector": LATEST_PROCESSED_SNAPSHOT,
            "snapshot_id": self.SNAPSHOT_ID,
            "snapshot_info": {},
            "snapshot_metrics": {},
        }
        staged_ingestion = ForwardIngestion.objects.create(sync=self.sync)
        executor.plan = Mock(return_value=(context, plan))
        executor._run_plan_item = Mock(return_value=staged_ingestion)

        ingestions = executor.run_next_plan_item(max_changes_per_branch=4)

        self.assertEqual(ingestions, [staged_ingestion])
        self.assertEqual(
            executor.plan.call_args.kwargs["model_strings"], ["dcim.device"]
        )
        self.assertEqual(
            executor.plan.call_args.kwargs["shard_scope"]["model"],
            "dcim.device",
        )
        executor._run_plan_item.assert_called_once()
        call_kwargs = executor._run_plan_item.call_args.kwargs
        self.assertFalse(call_kwargs["merge"])
        self.assertFalse(call_kwargs["automated_merge"])
        self.assertEqual(executor._run_plan_item.call_args.args[0].index, 1)
        state = get_branch_run_display_state(self.sync)
        self.assertEqual(state["phase"], "staging")
        self.assertEqual(state["plan_items"][0]["status"], "running")

    def test_inline_auto_merge_binds_step_before_merge(self):
        workload = BranchWorkload(
            model_string="dcim.site",
            label="dcim.site | Forward Sites",
            upsert_rows=[{"name": "Site A", "slug": "site-a"}],
            coalesce_fields=[["slug"]],
        )
        item = build_branch_plan([workload], max_changes_per_branch=10)[0]
        ForwardExecutionRun.objects.create(
            sync=self.sync,
            source=self.source,
            backend="branching",
            status="running",
            snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
            snapshot_id=self.SNAPSHOT_ID,
            total_steps=1,
            next_step_index=1,
        )
        step = ForwardExecutionStep.objects.create(
            run=ForwardExecutionRun.objects.get(sync=self.sync),
            index=1,
            kind="stage",
            status=ForwardExecutionStepStatusChoices.RUNNING,
            model_string="dcim.site",
            label="dcim.site | Forward Sites",
            estimated_changes=1,
        )
        logger = Mock()
        logger.log_data = {"statistics": {"dcim.site": {"current": 1, "applied": 1}}}
        executor = ForwardMultiBranchExecutor(
            sync=self.sync,
            client=Mock(),
            logger_=logger,
        )
        executor.max_changes_per_branch = 10
        context = {
            "snapshot_selector": LATEST_PROCESSED_SNAPSHOT,
            "snapshot_id": self.SNAPSHOT_ID,
            "snapshot_info": {},
            "snapshot_metrics": {},
        }

        def _assert_bound_before_merge(ingestion, *args, **kwargs):
            step.refresh_from_db()
            self.assertEqual(step.status, ForwardExecutionStepStatusChoices.STAGED)
            self.assertEqual(step.ingestion, ingestion)
            self.assertEqual(step.branch_name, ingestion.branch.name)

        with patch(
            "forward_netbox.utilities.multi_branch_lifecycle.run_item_in_branch"
        ), patch.object(Branch, "provision"), patch.object(
            Branch, "refresh_from_db"
        ), patch.object(
            Branch, "get_unmerged_changes"
        ) as mock_changes, patch.object(
            ForwardIngestion,
            "sync_merge",
            autospec=True,
            side_effect=_assert_bound_before_merge,
        ):
            mock_changes.return_value.count.return_value = 0
            executor._run_plan_item(
                item,
                context,
                mark_baseline_ready=True,
                merge=True,
                total_plan_items=1,
                plan_preview={},
            )

    def test_run_next_plan_item_uses_ledger_without_branch_run_json(self):
        workload = BranchWorkload(
            model_string="dcim.device",
            label="dcim.device | Forward Devices",
            upsert_rows=[{"name": f"device-{index}"} for index in range(8)],
            coalesce_fields=[["name"]],
        )
        plan = build_branch_plan([workload], max_changes_per_branch=4)
        execution_run = ForwardExecutionRun.objects.create(
            sync=self.sync,
            source=self.source,
            backend="branching",
            status="running",
            snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
            snapshot_id=self.SNAPSHOT_ID,
            total_steps=len(plan),
            next_step_index=1,
        )
        ForwardExecutionStep.objects.create(
            run=execution_run,
            index=1,
            kind="stage",
            status=ForwardExecutionStepStatusChoices.PENDING,
            model_string="dcim.device",
            label="dcim.device | Forward Devices",
            query_name=plan[0].query_name,
            execution_mode=plan[0].execution_mode,
            execution_value=plan[0].execution_value,
            shard_keys=list(plan[0].shard_keys),
            estimated_changes=plan[0].estimated_changes,
        )
        self.sync.clear_branch_run_state()
        initial_parameters = dict(self.sync.parameters or {})
        executor = ForwardMultiBranchExecutor(
            sync=self.sync,
            client=Mock(),
            logger_=Mock(),
            job=Mock(pk=124),
        )
        context = {
            "snapshot_selector": LATEST_PROCESSED_SNAPSHOT,
            "snapshot_id": self.SNAPSHOT_ID,
            "snapshot_info": {},
            "snapshot_metrics": {},
        }
        staged_ingestion = ForwardIngestion.objects.create(sync=self.sync)
        executor._load_execution_context = Mock(
            return_value=(context, plan, {}, 1, {}, {})
        )
        executor._run_plan_item = Mock(return_value=staged_ingestion)

        ingestions = executor.run_next_plan_item(max_changes_per_branch=4)

        self.assertEqual(ingestions, [staged_ingestion])
        executor._load_execution_context.assert_called_once()
        load_kwargs = executor._load_execution_context.call_args.kwargs
        self.assertEqual(load_kwargs["model_strings"], ["dcim.device"])
        self.assertEqual(load_kwargs["shard_scope"]["model"], "dcim.device")
        self.assertEqual(
            load_kwargs["shard_scope"]["shard_keys"],
            list(plan[0].shard_keys),
        )
        self.assertEqual(
            load_kwargs["shard_scope"]["execution_value"],
            plan[0].execution_value,
        )
        executor._run_plan_item.assert_called_once()
        self.assertEqual(self.sync.get_branch_run_state(), {})
        self.assertEqual(self.sync.parameters, initial_parameters)
        execution_run.refresh_from_db()
        self.assertEqual(execution_run.phase, "staging")
        self.assertEqual(execution_run.next_step_index, 1)

    def test_run_next_plan_item_honors_expected_plan_index(self):
        workload = BranchWorkload(
            model_string="dcim.device",
            label="dcim.device | Forward Devices",
            upsert_rows=[{"name": f"device-{index}"} for index in range(8)],
            coalesce_fields=[["name"]],
        )
        plan = build_branch_plan([workload], max_changes_per_branch=4)
        execution_run = ForwardExecutionRun.objects.create(
            sync=self.sync,
            source=self.source,
            backend="branching",
            status="running",
            snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
            snapshot_id=self.SNAPSHOT_ID,
            total_steps=len(plan),
            next_step_index=2,
        )
        ForwardExecutionStep.objects.create(
            run=execution_run,
            index=1,
            kind="stage",
            status=ForwardExecutionStepStatusChoices.RUNNING,
            model_string=plan[0].model_string,
            label=plan[0].label,
            query_name=plan[0].query_name,
            execution_mode=plan[0].execution_mode,
            execution_value=plan[0].execution_value,
            shard_keys=list(plan[0].shard_keys),
            estimated_changes=plan[0].estimated_changes,
        )
        self.sync.clear_branch_run_state()
        logger = Mock()
        executor = ForwardMultiBranchExecutor(
            sync=self.sync,
            client=Mock(),
            logger_=logger,
            job=Mock(pk=128),
        )
        context = {
            "snapshot_selector": LATEST_PROCESSED_SNAPSHOT,
            "snapshot_id": self.SNAPSHOT_ID,
            "snapshot_info": {},
            "snapshot_metrics": {},
        }
        staged_ingestion = ForwardIngestion.objects.create(sync=self.sync)
        executor._load_execution_context = Mock(
            return_value=(context, plan, {}, 2, {}, {})
        )
        executor._run_plan_item = Mock(return_value=staged_ingestion)

        ingestions = executor.run_next_plan_item(
            max_changes_per_branch=4,
            expected_plan_index=1,
        )

        self.assertEqual(ingestions, [staged_ingestion])
        self.assertEqual(executor._run_plan_item.call_args.args[0].index, 1)
        warning_messages = [call.args[0] for call in logger.log_warning.call_args_list]
        self.assertFalse(
            any("claimed shard index" in str(message) for message in warning_messages)
        )
        info_messages = [call.args[0] for call in logger.log_info.call_args_list]
        self.assertTrue(
            any(
                "Execution context returned a different shard index than claimed;"
                in str(message)
                for message in info_messages
            )
        )

    def test_run_next_plan_item_raises_when_claimed_index_missing(self):
        workload = BranchWorkload(
            model_string="dcim.device",
            label="dcim.device | Forward Devices",
            upsert_rows=[{"name": f"device-{index}"} for index in range(8)],
            coalesce_fields=[["name"]],
        )
        plan = build_branch_plan([workload], max_changes_per_branch=4)
        ForwardExecutionRun.objects.create(
            sync=self.sync,
            source=self.source,
            backend="branching",
            status="running",
            snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
            snapshot_id=self.SNAPSHOT_ID,
            total_steps=len(plan),
            next_step_index=2,
        )
        self.sync.clear_branch_run_state()
        executor = ForwardMultiBranchExecutor(
            sync=self.sync,
            client=Mock(),
            logger_=Mock(),
            job=Mock(pk=129),
        )
        context = {
            "snapshot_selector": LATEST_PROCESSED_SNAPSHOT,
            "snapshot_id": self.SNAPSHOT_ID,
            "snapshot_info": {},
            "snapshot_metrics": {},
        }
        executor._load_execution_context = Mock(
            return_value=(context, plan, {}, 2, {}, {})
        )

        with self.assertRaises(SyncError):
            executor.run_next_plan_item(
                max_changes_per_branch=4,
                expected_plan_index=3,
            )

    def test_load_execution_context_uses_ledger_state_without_branch_run_json(self):
        workload = BranchWorkload(
            model_string="dcim.device",
            label="dcim.device | Forward Devices",
            upsert_rows=[{"name": f"device-{index}"} for index in range(8)],
            coalesce_fields=[["name"]],
        )
        plan = build_branch_plan([workload], max_changes_per_branch=4)
        execution_run = ForwardExecutionRun.objects.create(
            sync=self.sync,
            source=self.source,
            backend="branching",
            status="running",
            snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
            snapshot_id=self.SNAPSHOT_ID,
            total_steps=len(plan),
            next_step_index=1,
            plan_preview={"planned_shards": len(plan)},
        )
        ForwardExecutionStep.objects.create(
            run=execution_run,
            index=1,
            kind="stage",
            status=ForwardExecutionStepStatusChoices.PENDING,
            model_string="dcim.device",
            label="dcim.device | Forward Devices",
            query_name=plan[0].query_name,
            execution_mode=plan[0].execution_mode,
            execution_value=plan[0].execution_value,
            shard_keys=list(plan[0].shard_keys),
            estimated_changes=plan[0].estimated_changes,
        )
        self.sync.clear_branch_run_state()
        executor = ForwardMultiBranchExecutor(
            sync=self.sync,
            client=Mock(),
            logger_=Mock(),
            job=Mock(pk=125),
        )
        context = {
            "snapshot_selector": LATEST_PROCESSED_SNAPSHOT,
            "snapshot_id": self.SNAPSHOT_ID,
            "snapshot_info": {},
            "snapshot_metrics": {},
        }
        executor.plan = Mock(return_value=(context, plan))

        executor._load_execution_context(max_changes_per_branch=4)

        plan_kwargs = executor.plan.call_args.kwargs
        self.assertFalse(plan_kwargs["run_preflight"])
        self.assertEqual(
            plan_kwargs["branch_run_state"]["state_source"],
            "execution_ledger",
        )
        self.assertEqual(
            plan_kwargs["branch_run_state"]["snapshot_id"],
            self.SNAPSHOT_ID,
        )
        self.assertEqual(
            plan_kwargs["branch_run_state"]["plan_items"][0]["model"],
            "dcim.device",
        )

    def test_load_execution_context_prefers_ledger_over_stale_branch_run_json(self):
        workload = BranchWorkload(
            model_string="dcim.device",
            label="dcim.device | Forward Devices",
            upsert_rows=[{"name": f"device-{index}"} for index in range(8)],
            coalesce_fields=[["name"]],
        )
        plan = build_branch_plan([workload], max_changes_per_branch=4)
        execution_run = ForwardExecutionRun.objects.create(
            sync=self.sync,
            source=self.source,
            backend="branching",
            status="running",
            snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
            snapshot_id=self.SNAPSHOT_ID,
            total_steps=len(plan),
            next_step_index=1,
            plan_preview={"planned_shards": len(plan)},
        )
        ForwardExecutionStep.objects.create(
            run=execution_run,
            index=1,
            kind="stage",
            status=ForwardExecutionStepStatusChoices.PENDING,
            model_string="dcim.device",
            label="dcim.device | Forward Devices",
            query_name=plan[0].query_name,
            execution_mode=plan[0].execution_mode,
            execution_value=plan[0].execution_value,
            shard_keys=list(plan[0].shard_keys),
            estimated_changes=plan[0].estimated_changes,
        )
        parameters = dict(self.sync.parameters or {})
        parameters["_branch_run"] = {
            "awaiting_merge": True,
            "next_plan_index": 99,
            "total_plan_items": 99,
        }
        self.sync.parameters = parameters
        self.sync.save(update_fields=["parameters"])
        executor = ForwardMultiBranchExecutor(
            sync=self.sync,
            client=Mock(),
            logger_=Mock(),
            job=Mock(pk=126),
        )
        context = {
            "snapshot_selector": LATEST_PROCESSED_SNAPSHOT,
            "snapshot_id": self.SNAPSHOT_ID,
            "snapshot_info": {},
            "snapshot_metrics": {},
        }
        executor.plan = Mock(return_value=(context, plan))

        executor._load_execution_context(max_changes_per_branch=4)

        plan_kwargs = executor.plan.call_args.kwargs
        self.assertEqual(
            plan_kwargs["branch_run_state"]["state_source"],
            "execution_ledger",
        )
        self.assertEqual(
            plan_kwargs["branch_run_state"]["next_plan_index"],
            1,
        )

    def test_load_execution_context_ignores_completed_ledger_plan_for_new_run(self):
        workload = BranchWorkload(
            model_string="dcim.device",
            label="dcim.device | Forward Devices",
            upsert_rows=[{"name": f"device-{index}"} for index in range(8)],
            coalesce_fields=[["name"]],
        )
        plan = build_branch_plan([workload], max_changes_per_branch=4)
        execution_run = ForwardExecutionRun.objects.create(
            sync=self.sync,
            source=self.source,
            backend="branching",
            status="completed",
            snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
            snapshot_id=self.SNAPSHOT_ID,
            total_steps=len(plan),
            next_step_index=len(plan) + 1,
        )
        ForwardExecutionStep.objects.create(
            run=execution_run,
            index=1,
            kind="stage",
            status=ForwardExecutionStepStatusChoices.MERGED,
            model_string="dcim.device",
            label="dcim.device | Forward Devices",
            estimated_changes=4,
        )
        self.sync.clear_branch_run_state()
        executor = ForwardMultiBranchExecutor(
            sync=self.sync,
            client=Mock(),
            logger_=Mock(),
            job=Mock(pk=126),
        )
        context = {
            "snapshot_selector": LATEST_PROCESSED_SNAPSHOT,
            "snapshot_id": self.SNAPSHOT_ID,
            "snapshot_info": {},
            "snapshot_metrics": {},
        }
        executor.plan = Mock(return_value=(context, plan))

        with patch(
            "forward_netbox.utilities.multi_branch_executor.ForwardValidationRunner"
        ):
            executor._load_execution_context(max_changes_per_branch=4)

        plan_kwargs = executor.plan.call_args.kwargs
        self.assertTrue(plan_kwargs["run_preflight"])
        self.assertEqual(plan_kwargs["branch_run_state"], {})

    @patch("forward_netbox.utilities.query_fetch_execution.get_query_specs")
    def test_run_next_plan_item_uses_ledger_shard_scope_for_native_fetch(
        self,
        mock_specs,
    ):
        rows = [
            {
                "device": "device-1",
                "name": "Ethernet1",
                "type": "other",
                "enabled": True,
            },
            {
                "device": "device-2",
                "name": "Ethernet1",
                "type": "other",
                "enabled": True,
            },
        ]
        client = Mock()
        client.get_snapshots.return_value = [
            {
                "id": self.SNAPSHOT_ID,
                "state": "PROCESSED",
                "created_at": "",
                "processed_at": "2026-03-31T12:15:00Z",
            }
        ]
        client.get_snapshot_metrics.return_value = {}
        client.run_nqe_query.return_value = rows
        self.sync.resolve_snapshot_id = lambda client=None: self.SNAPSHOT_ID
        self.sync.get_model_strings = lambda: ["dcim.interface"]
        self.sync.incremental_diff_baseline = Mock(return_value=None)
        mock_specs.return_value = [
            QuerySpec(
                model_string="dcim.interface",
                query_name="Forward Interfaces",
                query=(
                    'select {device: "device-1", name: "Ethernet1", '
                    'type: "other", enabled: true}'
                ),
            )
        ]
        execution_run = ForwardExecutionRun.objects.create(
            sync=self.sync,
            source=self.source,
            backend="branching",
            status="running",
            snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
            snapshot_id=self.SNAPSHOT_ID,
            total_steps=1,
            next_step_index=1,
        )
        ForwardExecutionStep.objects.create(
            run=execution_run,
            index=1,
            kind="stage",
            status=ForwardExecutionStepStatusChoices.PENDING,
            model_string="dcim.interface",
            label="dcim.interface | Forward Interfaces shard",
            query_name="Forward Interfaces",
            execution_mode="query",
            execution_value="Forward Interfaces",
            shard_keys=["device:device-1"],
            estimated_changes=1,
        )
        self.sync.clear_branch_run_state()
        staged_ingestion = ForwardIngestion.objects.create(sync=self.sync)
        executor = ForwardMultiBranchExecutor(
            sync=self.sync,
            client=client,
            logger_=Mock(),
            job=Mock(pk=127),
        )
        executor._run_plan_item = Mock(return_value=staged_ingestion)

        ingestions = executor.run_next_plan_item(max_changes_per_branch=10)

        self.assertEqual(ingestions, [staged_ingestion])
        self.assertEqual(
            client.run_nqe_query.call_args.kwargs["column_filters"],
            [
                {
                    "operator": "EQUALS_ANY",
                    "columnName": "device",
                    "values": ["device-1"],
                }
            ],
        )
        item = executor._run_plan_item.call_args.args[0]
        self.assertEqual(item.upsert_rows, [rows[0]])
        self.assertEqual(item.shard_keys, ("device:device-1",))
        self.assertEqual(self.sync.get_branch_run_state(), {})

    def test_branch_budget_retry_resplits_future_same_model_items(self):
        current_workload = BranchWorkload(
            model_string="dcim.device",
            label="dcim.device | Forward Devices",
            upsert_rows=[{"name": f"current-device-{index}"} for index in range(20)],
            coalesce_fields=[["name"]],
        )
        future_workload = BranchWorkload(
            model_string="dcim.device",
            label="dcim.device | Forward Devices",
            upsert_rows=[{"name": f"future-device-{index}"} for index in range(20)],
            coalesce_fields=[["name"]],
        )
        other_workload = BranchWorkload(
            model_string="dcim.site",
            label="dcim.site | Forward Sites",
            upsert_rows=[{"name": "site-1"}],
            coalesce_fields=[["name"]],
        )
        plan = build_branch_plan(
            [current_workload, future_workload, other_workload],
            max_changes_per_branch=20,
        )
        executor = ForwardMultiBranchExecutor(
            sync=self.sync,
            client=Mock(),
            logger_=Mock(),
        )
        executor.max_changes_per_branch = 10
        executor.model_change_density = {}
        executor._cleanup_overflow_branch = Mock()

        updated_plan = executor._handle_branch_budget_exceeded(
            BranchBudgetExceeded(
                item=plan[1],
                actual_changes=30,
                budget=10,
                branch=None,
                ingestion=None,
            ),
            plan,
            current_index=1,
        )

        device_items = [
            item for item in updated_plan if item.model_string == "dcim.device"
        ]
        self.assertGreater(len(device_items), 2)
        self.assertTrue(all(item.estimated_changes <= 7 for item in device_items))
        self.assertEqual(updated_plan[0].model_string, "dcim.site")
        self.assertTrue(
            any(
                call.args
                and call.args[0].startswith("Re-split ")
                and "remaining shard(s) for dcim.device" in call.args[0]
                for call in executor.logger.log_warning.call_args_list
            )
        )

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

    def test_branch_row_issues_do_not_stop_later_shards_or_mark_baseline(self):
        plan = build_branch_plan(
            [
                BranchWorkload(
                    model_string="dcim.device",
                    label="dcim.device | Forward Devices",
                    upsert_rows=[{"name": "device-1"}],
                    coalesce_fields=[["name"]],
                ),
                BranchWorkload(
                    model_string="netbox_routing.bgppeer",
                    label="netbox_routing.bgppeer | Forward BGP Peers",
                    upsert_rows=[
                        {
                            "device": "device-2",
                            "local_asn": 64512,
                            "neighbor_address": "192.0.2.1",
                            "peer_asn": 64513,
                            "enabled": True,
                            "status": "active",
                        }
                    ],
                    coalesce_fields=[["device", "neighbor_address"]],
                ),
            ],
            max_changes_per_branch=10,
        )
        executor = ForwardMultiBranchExecutor(
            sync=self.sync,
            client=Mock(),
            logger_=Mock(),
        )
        self.sync.auto_merge = True
        first_ingestion = ForwardIngestion.objects.create(sync=self.sync)
        first_ingestion.issues.create(
            model="dcim.device",
            message="bad virtual chassis row",
            exception="ForwardSyncDataError",
        )
        final_ingestion = ForwardIngestion.objects.create(sync=self.sync)
        executor._run_plan_item = Mock(side_effect=[first_ingestion, final_ingestion])

        ingestions = executor._execute_planned_items(
            {
                "snapshot_selector": "latestProcessed",
                "snapshot_id": self.SNAPSHOT_ID,
                "snapshot_info": {},
                "snapshot_metrics": {},
            },
            plan,
            {},
            next_plan_index=1,
        )

        self.assertEqual(ingestions, [first_ingestion, final_ingestion])
        self.assertEqual(executor._run_plan_item.call_count, 2)
        self.assertFalse(
            executor._run_plan_item.call_args_list[1].kwargs["mark_baseline_ready"]
        )

    def test_non_blocking_row_issues_do_not_block_final_baseline(self):
        plan = build_branch_plan(
            [
                BranchWorkload(
                    model_string="dcim.device",
                    label="dcim.device | Forward Devices",
                    upsert_rows=[{"name": "device-1"}],
                    coalesce_fields=[["name"]],
                ),
                BranchWorkload(
                    model_string="dcim.site",
                    label="dcim.site | Forward Sites",
                    upsert_rows=[{"name": "site-1", "slug": "site-1"}],
                    coalesce_fields=[["slug"]],
                ),
            ],
            max_changes_per_branch=10,
        )
        executor = ForwardMultiBranchExecutor(
            sync=self.sync,
            client=Mock(),
            logger_=Mock(),
        )
        self.sync.auto_merge = True
        first_ingestion = ForwardIngestion.objects.create(sync=self.sync)
        first_ingestion.issues.create(
            model="ipam.ipaddress",
            message="Skipping delete for `ipam.ipaddress` due to protected dependencies.",
            exception=ForwardDependencySkipError.__name__,
        )
        final_ingestion = ForwardIngestion.objects.create(sync=self.sync)
        executor._run_plan_item = Mock(side_effect=[first_ingestion, final_ingestion])

        ingestions = executor._execute_planned_items(
            {
                "snapshot_selector": "latestProcessed",
                "snapshot_id": self.SNAPSHOT_ID,
                "snapshot_info": {},
                "snapshot_metrics": {},
            },
            plan,
            {},
            next_plan_index=1,
        )

        self.assertEqual(ingestions, [first_ingestion, final_ingestion])
        self.assertEqual(executor._run_plan_item.call_count, 2)
        self.assertTrue(
            executor._run_plan_item.call_args_list[1].kwargs["mark_baseline_ready"]
        )


class ForwardFastBootstrapExecutorTest(TestCase):
    SNAPSHOT_ID = "snapshot-1"

    def setUp(self):
        self.source = ForwardSource.objects.create(
            name="source-fast-bootstrap",
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
            name="sync-fast-bootstrap",
            source=self.source,
            parameters={
                "snapshot_id": LATEST_PROCESSED_SNAPSHOT,
                "dcim.site": True,
                "enable_bulk_orm": False,
            },
        )

    @patch("forward_netbox.utilities.fast_bootstrap_executor.ForwardSyncRunner")
    @patch("forward_netbox.utilities.fast_bootstrap_executor.ForwardQueryFetcher")
    def test_run_creates_branchless_baseline_ingestion(
        self,
        mock_fetcher_class,
        mock_runner_class,
    ):
        logger = SyncLogging()
        context = ForwardQueryContext(
            network_id="test-network",
            snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
            snapshot_id=self.SNAPSHOT_ID,
            snapshot_info={"state": "PROCESSED"},
            snapshot_metrics={},
            query_parameters={},
            maps=[],
        )
        workload = BranchWorkload(
            model_string="dcim.site",
            label="sites",
            upsert_rows=[{"name": "Site 1", "slug": "site-1"}],
            delete_rows=[{"name": "Old Site", "slug": "old-site"}],
            sync_mode="full",
            coalesce_fields=[["slug"]],
            query_name="Forward Sites",
            execution_mode="query_id",
            execution_value="FQ_sites",
        )
        model_result = ForwardModelResult(
            model_string="dcim.site",
            query_name="Forward Sites",
            execution_mode="query_id",
            execution_value="FQ_sites",
            sync_mode="full",
            row_count=1,
            delete_count=1,
            snapshot_id=self.SNAPSHOT_ID,
        )
        fetcher = mock_fetcher_class.return_value
        fetcher.resolve_context.return_value = context
        fetcher.fetch_workloads.return_value = [workload]
        fetcher.model_results = [model_result]
        runner = mock_runner_class.return_value
        runner._model_coalesce_fields = {}

        def apply_rows(model_string, rows):
            for _row in rows:
                logger.increment_statistics(model_string)

        def delete_rows(model_string, rows):
            for _row in rows:
                logger.increment_statistics(model_string)

        runner._apply_model_rows.side_effect = apply_rows
        runner._delete_model_rows.side_effect = delete_rows

        ingestions = ForwardFastBootstrapExecutor(
            self.sync,
            Mock(),
            logger,
        ).run()

        self.assertEqual(len(ingestions), 1)
        ingestion = ingestions[0]
        self.assertIsNone(ingestion.branch)
        self.assertTrue(ingestion.baseline_ready)
        self.assertEqual(ingestion.snapshot_id, self.SNAPSHOT_ID)
        self.assertEqual(ingestion.sync_mode, "full")
        self.assertEqual(ingestion.applied_change_count, 2)
        self.assertEqual(ingestion.failed_change_count, 0)
        self.assertEqual(ingestion.model_results, [model_result.as_dict()])
        self.assertEqual(ingestion.model_results[0]["apply_engine"], "adapter")
        self.assertEqual(
            ForwardValidationRun.objects.get(sync=self.sync),
            ingestion.validation_run,
        )
        self.assertIsNotNone(ingestion.change_request_id)
        runner._apply_model_rows.assert_called_once_with(
            "dcim.site",
            workload.upsert_rows,
        )
        runner._delete_model_rows.assert_called_once_with(
            "dcim.site",
            workload.delete_rows,
        )

    @patch("forward_netbox.utilities.fast_bootstrap_executor.ForwardSyncRunner")
    @patch("forward_netbox.utilities.fast_bootstrap_executor.ForwardQueryFetcher")
    def test_run_skips_preflight_when_source_disables_it(
        self,
        mock_fetcher_class,
        mock_runner_class,
    ):
        self.source.parameters["query_preflight_enabled"] = False
        self.source.save(update_fields=["parameters"])

        logger = SyncLogging()
        context = ForwardQueryContext(
            network_id="test-network",
            snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
            snapshot_id=self.SNAPSHOT_ID,
            snapshot_info={"state": "PROCESSED"},
            snapshot_metrics={},
            query_parameters={},
            maps=[],
        )
        workload = BranchWorkload(
            model_string="dcim.site",
            label="sites",
            upsert_rows=[{"name": "Site 1", "slug": "site-1"}],
            delete_rows=[],
            sync_mode="full",
            coalesce_fields=[["slug"]],
            query_name="Forward Sites",
            execution_mode="query_id",
            execution_value="FQ_sites",
        )
        fetcher = mock_fetcher_class.return_value
        fetcher.resolve_context.return_value = context
        fetcher.fetch_workloads.return_value = [workload]
        fetcher.model_results = []
        runner = mock_runner_class.return_value
        runner._model_coalesce_fields = {}

        ForwardFastBootstrapExecutor(
            self.sync,
            Mock(),
            logger,
        ).run()

        self.assertFalse(fetcher.run_preflight.called)
        fetcher.fetch_workloads.assert_called_once()

    @patch("forward_netbox.utilities.fast_bootstrap_executor.ForwardSyncRunner")
    @patch("forward_netbox.utilities.fast_bootstrap_executor.ForwardQueryFetcher")
    def test_run_applies_fast_bootstrap_workloads_in_dependency_order(
        self,
        mock_fetcher_class,
        mock_runner_class,
    ):
        logger = SyncLogging()
        context = ForwardQueryContext(
            network_id="test-network",
            snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
            snapshot_id=self.SNAPSHOT_ID,
            snapshot_info={"state": "PROCESSED"},
            snapshot_metrics={},
            query_parameters={},
            maps=[],
        )
        mac_workload = BranchWorkload(
            model_string="dcim.macaddress",
            label="mac addresses",
            upsert_rows=[
                {
                    "device": "device-1",
                    "interface": "Ethernet1",
                    "mac": "00:11:22:33:44:55",
                    "mac_address": "00:11:22:33:44:55",
                }
            ],
            sync_mode="full",
            coalesce_fields=[["mac_address"]],
            query_name="Forward MAC Addresses",
            execution_mode="query_id",
            execution_value="FQ_macs",
        )
        interface_workload = BranchWorkload(
            model_string="dcim.interface",
            label="interfaces",
            upsert_rows=[
                {
                    "device": "device-1",
                    "name": "Ethernet1",
                    "type": "other",
                    "enabled": True,
                }
            ],
            sync_mode="full",
            coalesce_fields=[["device", "name"]],
            query_name="Forward Interfaces",
            execution_mode="query_id",
            execution_value="FQ_interfaces",
        )
        fetcher = mock_fetcher_class.return_value
        fetcher.resolve_context.return_value = context
        fetcher.fetch_workloads.return_value = [mac_workload, interface_workload]
        fetcher.model_results = [
            ForwardModelResult(
                model_string="dcim.macaddress",
                query_name="Forward MAC Addresses",
                execution_mode="query_id",
                execution_value="FQ_macs",
                sync_mode="full",
                row_count=1,
                snapshot_id=self.SNAPSHOT_ID,
            ),
            ForwardModelResult(
                model_string="dcim.interface",
                query_name="Forward Interfaces",
                execution_mode="query_id",
                execution_value="FQ_interfaces",
                sync_mode="full",
                row_count=1,
                snapshot_id=self.SNAPSHOT_ID,
            ),
        ]
        runner = mock_runner_class.return_value
        runner._model_coalesce_fields = {}

        ForwardFastBootstrapExecutor(
            self.sync,
            Mock(),
            logger,
        ).run()

        self.assertEqual(
            [call.args[0] for call in runner._apply_model_rows.call_args_list],
            ["dcim.interface", "dcim.macaddress"],
        )

    @patch("forward_netbox.utilities.fast_bootstrap_executor.ForwardQueryFetcher")
    def test_run_records_direct_netbox_changes_for_fast_bootstrap(
        self,
        mock_fetcher_class,
    ):
        logger = SyncLogging()
        context = ForwardQueryContext(
            network_id="test-network",
            snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
            snapshot_id=self.SNAPSHOT_ID,
            snapshot_info={"state": "PROCESSED"},
            snapshot_metrics={},
            query_parameters={},
            maps=[],
        )
        workload = BranchWorkload(
            model_string="dcim.site",
            label="sites",
            upsert_rows=[{"name": "Site 2", "slug": "site-2"}],
            delete_rows=[],
            sync_mode="full",
            coalesce_fields=[["slug"]],
            query_name="Forward Sites",
            execution_mode="query_id",
            execution_value="FQ_sites",
        )
        model_result = ForwardModelResult(
            model_string="dcim.site",
            query_name="Forward Sites",
            execution_mode="query_id",
            execution_value="FQ_sites",
            sync_mode="full",
            row_count=1,
            delete_count=0,
            snapshot_id=self.SNAPSHOT_ID,
        )
        fetcher = mock_fetcher_class.return_value
        fetcher.resolve_context.return_value = context
        fetcher.fetch_workloads.return_value = [workload]
        fetcher.model_results = [model_result]
        user = get_user_model().objects.create_user(username="fast-bootstrap-user")

        ingestions = ForwardFastBootstrapExecutor(
            self.sync,
            Mock(),
            logger,
            user=user,
        ).run()

        ingestion = ingestions[0]
        ingestion.refresh_from_db()
        site_type = ObjectType.objects.get_for_model(Site)
        self.assertTrue(Site.objects.filter(slug="site-2").exists())
        self.assertTrue(
            ObjectChange.objects.filter(
                request_id=ingestion.change_request_id,
                changed_object_type=site_type,
                action="create",
            ).exists()
        )
        self.assertEqual(ingestion.applied_change_count, 1)
        self.assertEqual(ingestion.created_change_count, 1)
        self.assertEqual(ingestion.updated_change_count, 0)
        self.assertEqual(ingestion.deleted_change_count, 0)
        self.assertEqual(object_changes_for_ingestion(ingestion).count(), 1)

    @patch("forward_netbox.utilities.fast_bootstrap_executor.ForwardSyncRunner")
    @patch("forward_netbox.utilities.fast_bootstrap_executor.ForwardQueryFetcher")
    def test_run_does_not_mark_baseline_ready_when_issues_exist(
        self,
        mock_fetcher_class,
        mock_runner_class,
    ):
        logger = SyncLogging()
        context = ForwardQueryContext(
            network_id="test-network",
            snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
            snapshot_id=self.SNAPSHOT_ID,
            snapshot_info={"state": "PROCESSED"},
            snapshot_metrics={},
            query_parameters={},
            maps=[],
        )
        workload = BranchWorkload(
            model_string="dcim.site",
            label="sites",
            upsert_rows=[{"name": "Site 1", "slug": "site-1"}],
            delete_rows=[],
            sync_mode="full",
            coalesce_fields=[["slug"]],
            query_name="Forward Sites",
            execution_mode="query_id",
            execution_value="FQ_sites",
        )
        model_result = ForwardModelResult(
            model_string="dcim.site",
            query_name="Forward Sites",
            execution_mode="query_id",
            execution_value="FQ_sites",
            sync_mode="full",
            row_count=1,
            delete_count=0,
            snapshot_id=self.SNAPSHOT_ID,
        )
        fetcher = mock_fetcher_class.return_value
        fetcher.resolve_context.return_value = context
        fetcher.fetch_workloads.return_value = [workload]
        fetcher.model_results = [model_result]
        runner = mock_runner_class.return_value
        runner._model_coalesce_fields = {}

        def apply_rows(model_string, _rows):
            ingestion = ForwardIngestion.objects.get(sync=self.sync)
            ingestion.issues.create(
                model=model_string,
                message="Unable to apply site row.",
                exception="validation failed",
            )
            logger.increment_statistics(model_string, outcome="failed")

        runner._apply_model_rows.side_effect = apply_rows

        with self.assertRaisesRegex(
            SyncError,
            "Forward fast bootstrap completed with issues",
        ):
            ForwardFastBootstrapExecutor(
                self.sync,
                Mock(),
                logger,
            ).run()

        ingestion = ForwardIngestion.objects.get(sync=self.sync)
        self.assertFalse(ingestion.baseline_ready)
        self.assertEqual(ingestion.issues.count(), 1)
        self.assertEqual(ingestion.applied_change_count, 0)
        self.assertEqual(ingestion.failed_change_count, 1)

    @patch("forward_netbox.utilities.fast_bootstrap_executor.ForwardSyncRunner")
    @patch("forward_netbox.utilities.fast_bootstrap_executor.ForwardQueryFetcher")
    def test_run_marks_baseline_ready_when_only_optional_model_issues_exist(
        self,
        mock_fetcher_class,
        mock_runner_class,
    ):
        logger = SyncLogging()
        context = ForwardQueryContext(
            network_id="test-network",
            snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
            snapshot_id=self.SNAPSHOT_ID,
            snapshot_info={"state": "PROCESSED"},
            snapshot_metrics={},
            query_parameters={},
            maps=[],
        )
        workload = BranchWorkload(
            model_string="netbox_routing.bgppeer",
            label="bgp peers",
            upsert_rows=[{"device": "device-1"}],
            delete_rows=[],
            sync_mode="full",
            coalesce_fields=[["device"]],
            query_name="Forward BGP Peers",
            execution_mode="query_id",
            execution_value="FQ_bgp",
        )
        model_result = ForwardModelResult(
            model_string="netbox_routing.bgppeer",
            query_name="Forward BGP Peers",
            execution_mode="query_id",
            execution_value="FQ_bgp",
            sync_mode="full",
            row_count=1,
            delete_count=0,
            snapshot_id=self.SNAPSHOT_ID,
        )
        fetcher = mock_fetcher_class.return_value
        fetcher.resolve_context.return_value = context
        fetcher.fetch_workloads.return_value = [workload]
        fetcher.model_results = [model_result]
        runner = mock_runner_class.return_value
        runner._model_coalesce_fields = {}

        def apply_rows(model_string, _rows):
            ingestion = ForwardIngestion.objects.get(sync=self.sync)
            ingestion.issues.create(
                model=model_string,
                message="Unable to apply optional BGP row.",
                exception="validation failed",
            )
            logger.increment_statistics(model_string, outcome="failed")

        runner._apply_model_rows.side_effect = apply_rows

        ingestions = ForwardFastBootstrapExecutor(
            self.sync,
            Mock(),
            logger,
        ).run()

        ingestion = ingestions[0]
        ingestion.refresh_from_db()
        self.assertTrue(ingestion.baseline_ready)
        self.assertEqual(ingestion.issues.count(), 1)
        self.assertEqual(ingestion.failed_change_count, 1)

    @patch("forward_netbox.utilities.fast_bootstrap_executor.ForwardSyncRunner")
    @patch("forward_netbox.utilities.fast_bootstrap_executor.ForwardQueryFetcher")
    def test_run_marks_baseline_ready_when_only_dependency_skip_issues_exist(
        self,
        mock_fetcher_class,
        mock_runner_class,
    ):
        logger = SyncLogging()
        context = ForwardQueryContext(
            network_id="test-network",
            snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
            snapshot_id=self.SNAPSHOT_ID,
            snapshot_info={"state": "PROCESSED"},
            snapshot_metrics={},
            query_parameters={},
            maps=[],
        )
        workload = BranchWorkload(
            model_string="ipam.ipaddress",
            label="ip addresses",
            upsert_rows=[],
            delete_rows=[{"address": "192.0.2.1/32"}],
            sync_mode="full",
            coalesce_fields=[["address"]],
            query_name="Forward IP Addresses",
            execution_mode="query_id",
            execution_value="FQ_ip",
        )
        model_result = ForwardModelResult(
            model_string="ipam.ipaddress",
            query_name="Forward IP Addresses",
            execution_mode="query_id",
            execution_value="FQ_ip",
            sync_mode="full",
            row_count=0,
            delete_count=1,
            snapshot_id=self.SNAPSHOT_ID,
        )
        fetcher = mock_fetcher_class.return_value
        fetcher.resolve_context.return_value = context
        fetcher.fetch_workloads.return_value = [workload]
        fetcher.model_results = [model_result]
        runner = mock_runner_class.return_value
        runner._model_coalesce_fields = {}

        def delete_rows(model_string, _rows):
            ingestion = ForwardIngestion.objects.get(sync=self.sync)
            ingestion.issues.create(
                model=model_string,
                message="Skipping delete for `ipam.ipaddress` due to protected dependencies.",
                exception="ForwardDependencySkipError",
            )
            logger.increment_statistics(model_string, outcome="skipped")

        runner._delete_model_rows.side_effect = delete_rows

        ingestions = ForwardFastBootstrapExecutor(
            self.sync,
            Mock(),
            logger,
        ).run()

        ingestion = ingestions[0]
        ingestion.refresh_from_db()
        self.assertTrue(ingestion.baseline_ready)
        self.assertEqual(ingestion.issues.count(), 1)
        self.assertEqual(ingestion.failed_change_count, 0)


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
                "enable_bulk_orm": False,
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

    def _create_module_bay(self, device, name="Slot 1", position="1"):
        values = {
            "device": device,
            "name": name,
            "label": name,
            "position": position,
        }
        if any(field.name == "enabled" for field in ModuleBay._meta.fields):
            values["enabled"] = True
        return ModuleBay.objects.create(**values)

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

    def test_device_lookup_cache_is_positive_only(self):
        runner = ForwardSyncRunner(
            sync=self.sync, ingestion=None, client=None, logger_=Mock()
        )

        self.assertIsNone(runner._lookup_device_by_name("device-1"))
        device = self._create_device("device-1")

        with CaptureQueriesContext(connection) as queries:
            self.assertEqual(runner._get_device_by_name("device-1"), device)
            self.assertEqual(runner._get_device_by_name("device-1"), device)

        self.assertEqual(len(queries), 1)

    def test_device_lookup_cache_reuses_negative_lookup_for_strict_get(self):
        runner = ForwardSyncRunner(
            sync=self.sync, ingestion=None, client=None, logger_=Mock()
        )

        with CaptureQueriesContext(connection) as queries:
            with self.assertRaises(Device.DoesNotExist):
                runner._get_device_by_name("device-404")
            with self.assertRaises(Device.DoesNotExist):
                runner._get_device_by_name("device-404")

        self.assertEqual(len(queries), 1)

    def test_interface_lookup_cache_reuses_positive_lookup(self):
        device = self._create_device("device-1")
        interface = Interface.objects.create(
            device=device,
            name="Ethernet1/1",
            type="1000base-t",
        )
        runner = ForwardSyncRunner(
            sync=self.sync, ingestion=None, client=None, logger_=Mock()
        )

        with CaptureQueriesContext(connection) as queries:
            self.assertEqual(runner._lookup_interface(device, "Ethernet1/1"), interface)
            self.assertEqual(runner._lookup_interface(device, "Ethernet1/1"), interface)

        self.assertEqual(len(queries), 1)

    def test_interface_lookup_cache_reuses_negative_lookup(self):
        device = self._create_device("device-1")
        runner = ForwardSyncRunner(
            sync=self.sync, ingestion=None, client=None, logger_=Mock()
        )

        with CaptureQueriesContext(connection) as queries:
            self.assertIsNone(runner._lookup_interface(device, "Ethernet1/404"))
            self.assertIsNone(runner._lookup_interface(device, "Ethernet1/404"))

        self.assertEqual(len(queries), 1)

    def test_interface_lookup_negative_cache_clears_when_interface_is_created(self):
        device = self._create_device("device-1")
        runner = ForwardSyncRunner(
            sync=self.sync, ingestion=None, client=None, logger_=Mock()
        )

        self.assertIsNone(runner._lookup_interface(device, "Ethernet1/1"))
        interface, created = runner._upsert_values_from_defaults(
            "dcim.interface",
            Interface,
            values={
                "device": device,
                "name": "Ethernet1/1",
                "type": "1000base-t",
                "enabled": True,
            },
            coalesce_sets=[("device", "name")],
        )

        self.assertTrue(created)
        with CaptureQueriesContext(connection) as queries:
            self.assertEqual(runner._lookup_interface(device, "Ethernet1/1"), interface)

        self.assertEqual(len(queries), 0)

    def test_interface_lookup_cache_remembers_upserted_interface(self):
        device = self._create_device("device-1")
        runner = ForwardSyncRunner(
            sync=self.sync, ingestion=None, client=None, logger_=Mock()
        )
        interface, created = runner._upsert_values_from_defaults(
            "dcim.interface",
            Interface,
            values={
                "device": device,
                "name": "Ethernet1/1",
                "type": "1000base-t",
                "enabled": True,
            },
            coalesce_sets=[("device", "name")],
        )

        self.assertTrue(created)
        with CaptureQueriesContext(connection) as queries:
            self.assertEqual(runner._lookup_interface(device, "Ethernet1/1"), interface)

        self.assertEqual(len(queries), 0)

    def test_interface_coalesce_reuses_primed_identity_cache(self):
        device = self._create_device("device-1")
        interface = Interface.objects.create(
            device=device,
            name="Ethernet1/1",
            type="1000base-t",
            enabled=True,
        )
        runner = ForwardSyncRunner(
            sync=self.sync, ingestion=None, client=None, logger_=Mock()
        )
        prime_dependency_lookup_caches(
            runner,
            "dcim.macaddress",
            [{"device": "device-1", "interface": "Ethernet1/1"}],
        )

        with CaptureQueriesContext(connection) as queries:
            upserted, created = runner._upsert_values_from_defaults(
                "dcim.interface",
                Interface,
                values={
                    "device": device,
                    "name": "Ethernet1/1",
                    "type": "1000base-t",
                    "enabled": True,
                },
                coalesce_sets=[("device", "name")],
            )

        self.assertFalse(created)
        self.assertEqual(upserted, interface)
        self.assertEqual(len(queries), 0)

    def test_interface_coalesce_cache_only_applies_to_exact_identity(self):
        device_1 = self._create_device("device-ambiguous-cache-1")
        device_2 = self._create_device("device-ambiguous-cache-2")
        Interface.objects.create(
            device=device_1,
            name="Ethernet1",
            type="1000base-t",
        )
        Interface.objects.create(
            device=device_2,
            name="Ethernet1",
            type="1000base-t",
        )
        runner = ForwardSyncRunner(
            sync=self.sync, ingestion=None, client=None, logger_=Mock()
        )
        runner._lookup_interface(device_1, "Ethernet1")

        with self.assertRaisesRegex(
            ForwardSearchError,
            "Ambiguous coalesce lookup for `dcim.interface`",
        ):
            get_unique_or_raise(runner, Interface, {"name": "Ethernet1"})

    def test_dependency_lookup_cache_primes_devices_for_row_batch(self):
        device = self._create_device("device-1")
        runner = ForwardSyncRunner(
            sync=self.sync, ingestion=None, client=None, logger_=Mock()
        )

        with CaptureQueriesContext(connection) as queries:
            prime_dependency_lookup_caches(
                runner,
                "dcim.interface",
                [
                    {"device": "device-1", "name": "Ethernet1/1"},
                    {"device": "device-1", "name": "Ethernet1/2"},
                ],
            )

        self.assertEqual(len(queries), 2)
        with CaptureQueriesContext(connection) as cached_queries:
            self.assertEqual(runner._get_device_by_name("device-1"), device)

        self.assertEqual(len(cached_queries), 0)

    def test_dependency_lookup_cache_marks_missing_devices_for_strict_get(self):
        runner = ForwardSyncRunner(
            sync=self.sync, ingestion=None, client=None, logger_=Mock()
        )

        with CaptureQueriesContext(connection) as prime_queries:
            prime_dependency_lookup_caches(
                runner,
                "dcim.interface",
                [{"device": "missing-device", "name": "Ethernet1/1"}],
            )

        self.assertEqual(len(prime_queries), 1)
        with CaptureQueriesContext(connection) as cached_queries:
            with self.assertRaises(Device.DoesNotExist):
                runner._get_device_by_name("missing-device")

        self.assertEqual(len(cached_queries), 0)

    def test_dependency_lookup_cache_primes_device_identity_for_device_rows(self):
        device = self._create_device("device-1")
        runner = ForwardSyncRunner(
            sync=self.sync, ingestion=None, client=None, logger_=Mock()
        )

        with CaptureQueriesContext(connection) as queries:
            prime_dependency_lookup_caches(
                runner,
                "dcim.device",
                [{"name": "device-1"}],
            )

        self.assertEqual(len(queries), 1)
        with CaptureQueriesContext(connection) as cached_queries:
            self.assertEqual(
                get_unique_or_raise(runner, Device, {"name": "device-1"}),
                device,
            )

        self.assertEqual(len(cached_queries), 0)

    def test_dependency_lookup_cache_primes_dcim_device_identity_dependencies(self):
        site = Site.objects.create(name="site-1", slug="site-1")
        manufacturer = Manufacturer.objects.create(name="Acme", slug="acme")
        role = DeviceRole.objects.create(name="Core", slug="core", color="ff9800")
        platform = Platform.objects.create(
            name="ios-xe",
            slug="ios-xe",
            manufacturer=manufacturer,
        )
        runner = ForwardSyncRunner(
            sync=self.sync, ingestion=None, client=None, logger_=Mock()
        )

        with CaptureQueriesContext(connection) as prime_queries:
            prime_dependency_lookup_caches(
                runner,
                "dcim.device",
                [
                    {
                        "site": "site-1",
                        "site_slug": "site-1",
                        "manufacturer": "Acme",
                        "manufacturer_slug": "acme",
                        "role": "Core",
                        "role_slug": "core",
                        "role_color": "ff9800",
                        "platform": "ios-xe",
                        "platform_slug": "ios-xe",
                    }
                ],
            )

        self.assertGreater(len(prime_queries), 0)
        with CaptureQueriesContext(connection) as cached_queries:
            self.assertEqual(
                runner._ensure_site({"name": "site-1", "slug": "site-1"}),
                site,
            )
            self.assertEqual(
                runner._ensure_manufacturer({"name": "Acme", "slug": "acme"}),
                manufacturer,
            )
            self.assertEqual(
                runner._ensure_role(
                    {"name": "Core", "slug": "core", "color": "ff9800"}
                ),
                role,
            )
            self.assertEqual(
                runner._ensure_platform(
                    {
                        "name": "ios-xe",
                        "slug": "ios-xe",
                        "manufacturer": "Acme",
                        "manufacturer_slug": "acme",
                    }
                ),
                platform,
            )

        self.assertEqual(len(cached_queries), 0)

    def test_dependency_lookup_cache_prefers_slug_identity_without_name_lookups(self):
        Site.objects.create(name="site-name", slug="site-1")
        runner = ForwardSyncRunner(
            sync=self.sync, ingestion=None, client=None, logger_=Mock()
        )
        rows = [
            {
                "site": "site-name",
                "site_slug": "site-1",
                "manufacturer": "Acme",
                "manufacturer_slug": "acme",
                "role": "Core",
                "role_slug": "core",
                "platform": "ios-xe",
                "platform_slug": "ios-xe",
                "device_type": "QFX-5120",
                "device_type_slug": "qfx-5120",
            }
        ]

        with CaptureQueriesContext(connection) as queries:
            prime_dependency_lookup_caches(runner, "dcim.device", rows)

        sql_statements = [query["sql"] for query in queries]
        self.assertTrue(any("slug" in statement for statement in sql_statements))
        self.assertFalse(any("name__in" in statement for statement in sql_statements))

    def test_dependency_lookup_cache_primes_dcim_device_type_identities(self):
        manufacturer = Manufacturer.objects.create(name="Acme", slug="acme")
        device_type = DeviceType.objects.create(
            manufacturer=manufacturer,
            model="QFX-5120",
            slug="qfx-5120",
        )
        runner = ForwardSyncRunner(
            sync=self.sync, ingestion=None, client=None, logger_=Mock()
        )

        prime_dependency_lookup_caches(
            runner,
            "dcim.device",
            [
                {
                    "manufacturer": "Acme",
                    "manufacturer_slug": "acme",
                    "device_type": "QFX-5120",
                    "device_type_slug": "qfx-5120",
                }
            ],
        )

        with CaptureQueriesContext(connection) as cached_queries:
            self.assertEqual(
                get_unique_or_raise(runner, DeviceType, {"slug": "qfx-5120"}),
                device_type,
            )
            self.assertEqual(
                get_unique_or_raise(
                    runner,
                    DeviceType,
                    {"manufacturer": manufacturer, "model": "QFX-5120"},
                ),
                device_type,
            )

        self.assertEqual(len(cached_queries), 0)

    def test_ensure_manufacturer_uses_unique_lookup_cache_after_first_resolution(self):
        runner = ForwardSyncRunner(
            sync=self.sync, ingestion=None, client=None, logger_=Mock()
        )
        row = {"name": "Acme", "slug": "acme"}

        with CaptureQueriesContext(connection) as first_queries:
            manufacturer_a = runner._ensure_manufacturer(row)
        with CaptureQueriesContext(connection) as cached_queries:
            manufacturer_b = runner._ensure_manufacturer(row)

        self.assertGreater(len(first_queries), 0)
        self.assertEqual(len(cached_queries), 0)
        self.assertEqual(manufacturer_a.pk, manufacturer_b.pk)

    def test_dependency_lookup_cache_primes_missing_interfaces_for_row_batch(self):
        device = self._create_device("device-1")
        runner = ForwardSyncRunner(
            sync=self.sync, ingestion=None, client=None, logger_=Mock()
        )

        with CaptureQueriesContext(connection) as queries:
            prime_dependency_lookup_caches(
                runner,
                "dcim.macaddress",
                [
                    {
                        "device": "device-1",
                        "interface": "Ethernet1/404",
                        "mac": "00:11:22:33:44:55",
                    },
                    {
                        "device": "device-1",
                        "interface": "Ethernet1/404",
                        "mac": "00:11:22:33:44:66",
                    },
                ],
            )

        self.assertEqual(len(queries), 2)
        with CaptureQueriesContext(connection) as cached_queries:
            self.assertIsNone(runner._lookup_interface(device, "Ethernet1/404"))

        self.assertEqual(len(cached_queries), 0)

    def test_dependency_lookup_cache_primes_interfaces_for_row_batch(self):
        device = self._create_device("device-1")
        interface = Interface.objects.create(
            device=device,
            name="Ethernet1/1",
            type="1000base-t",
        )
        runner = ForwardSyncRunner(
            sync=self.sync, ingestion=None, client=None, logger_=Mock()
        )

        with CaptureQueriesContext(connection) as queries:
            prime_dependency_lookup_caches(
                runner,
                "ipam.ipaddress",
                [
                    {
                        "device": "device-1",
                        "interface": "Ethernet1/1",
                        "address": "192.0.2.1/24",
                    }
                ],
            )

        self.assertEqual(len(queries), 4)
        with CaptureQueriesContext(connection) as cached_queries:
            self.assertEqual(runner._lookup_interface(device, "Ethernet1/1"), interface)

        self.assertEqual(len(cached_queries), 0)

    def test_dependency_lookup_cache_primes_interfaces_for_interface_rows(self):
        device = self._create_device("device-1")
        interface = Interface.objects.create(
            device=device,
            name="Ethernet1/1",
            type="1000base-t",
        )
        runner = ForwardSyncRunner(
            sync=self.sync, ingestion=None, client=None, logger_=Mock()
        )

        with CaptureQueriesContext(connection) as queries:
            prime_dependency_lookup_caches(
                runner,
                "dcim.interface",
                [
                    {
                        "device": "device-1",
                        "name": "Ethernet1/1",
                        "type": "1000base-t",
                        "enabled": True,
                    }
                ],
            )

        self.assertEqual(len(queries), 2)
        with CaptureQueriesContext(connection) as cached_queries:
            self.assertEqual(runner._lookup_interface(device, "Ethernet1/1"), interface)

        self.assertEqual(len(cached_queries), 0)

    def test_dependency_lookup_cache_primes_ipam_prefix_vrf_identity(self):
        vrf = VRF.objects.create(name="blue", rd="64512:1")
        prefix = Prefix.objects.create(prefix="10.0.0.0/24", vrf=vrf, status="active")
        runner = ForwardSyncRunner(
            sync=self.sync, ingestion=None, client=None, logger_=Mock()
        )

        with CaptureQueriesContext(connection) as queries:
            prime_dependency_lookup_caches(
                runner,
                "ipam.prefix",
                [
                    {
                        "prefix": "10.0.0.0/24",
                        "vrf": "blue",
                        "status": "active",
                    }
                ],
            )

        self.assertEqual(len(queries), 2)
        with CaptureQueriesContext(connection) as cached_queries:
            self.assertEqual(
                get_unique_or_raise(
                    runner,
                    Prefix,
                    {"prefix": "10.0.0.0/24", "vrf": vrf},
                ),
                prefix,
            )

        self.assertEqual(len(cached_queries), 0)

    def test_dependency_lookup_cache_primes_ipam_prefix_global_identity(self):
        prefix = Prefix.objects.create(prefix="10.3.0.0/24", vrf=None, status="active")
        runner = ForwardSyncRunner(
            sync=self.sync, ingestion=None, client=None, logger_=Mock()
        )

        with CaptureQueriesContext(connection) as queries:
            prime_dependency_lookup_caches(
                runner,
                "ipam.prefix",
                [
                    {
                        "prefix": "10.3.0.0/24",
                        "vrf": "",
                        "status": "active",
                    }
                ],
            )

        self.assertEqual(len(queries), 1)
        with CaptureQueriesContext(connection) as cached_queries:
            self.assertEqual(
                get_unique_or_raise(
                    runner,
                    Prefix,
                    {"prefix": "10.3.0.0/24", "vrf": None},
                ),
                prefix,
            )

        self.assertEqual(len(cached_queries), 0)

    def test_dependency_lookup_cache_primes_ipam_ipaddress_vrf_identity(self):
        vrf = VRF.objects.create(name="blue", rd="64512:2")
        ip_address = IPAddress.objects.create(
            address="10.0.0.1/24",
            vrf=vrf,
            status="active",
        )
        runner = ForwardSyncRunner(
            sync=self.sync, ingestion=None, client=None, logger_=Mock()
        )

        with CaptureQueriesContext(connection) as queries:
            prime_dependency_lookup_caches(
                runner,
                "ipam.ipaddress",
                [
                    {
                        "address": "10.0.0.1/24",
                        "vrf": "blue",
                        "status": "active",
                    }
                ],
            )

        self.assertEqual(len(queries), 2)
        with CaptureQueriesContext(connection) as cached_queries:
            self.assertEqual(
                get_unique_or_raise(
                    runner,
                    IPAddress,
                    {"address": "10.0.0.1/24", "vrf": vrf},
                ),
                ip_address,
            )

        self.assertEqual(len(cached_queries), 0)

    def test_dependency_lookup_cache_primes_tag_identity_for_taggeditem_rows(self):
        Tag.objects.create(name="Prot_BGP", slug="prot-bgp", color="2196f3")
        self._create_device("device-1")
        runner = ForwardSyncRunner(
            sync=self.sync, ingestion=None, client=None, logger_=Mock()
        )

        with CaptureQueriesContext(connection) as queries:
            prime_dependency_lookup_caches(
                runner,
                "extras.taggeditem",
                [
                    {
                        "device": "device-1",
                        "tag": "Prot_BGP",
                        "tag_slug": "prot-bgp",
                        "tag_color": "2196f3",
                    }
                ],
            )

        self.assertEqual(len(queries), 2)
        with CaptureQueriesContext(connection) as cached_queries:
            tag = get_unique_or_raise(runner, Tag, {"slug": "prot-bgp"})

        self.assertIsNotNone(tag)
        self.assertEqual(len(cached_queries), 0)

    def test_dependency_lookup_cache_primes_inventoryitem_role_identity(self):
        role = InventoryItemRole.objects.create(
            name="POWER SUPPLY",
            slug="power-supply",
            color="ff9800",
        )
        runner = ForwardSyncRunner(
            sync=self.sync, ingestion=None, client=None, logger_=Mock()
        )

        with CaptureQueriesContext(connection) as queries:
            prime_dependency_lookup_caches(
                runner,
                "dcim.inventoryitem",
                [
                    {
                        "device": "device-1",
                        "manufacturer": "vendor-1",
                        "manufacturer_slug": "vendor-1",
                        "name": "Power Supply 1",
                        "role": "POWER SUPPLY",
                        "role_slug": "power-supply",
                        "role_color": "ff9800",
                    }
                ],
            )

        self.assertGreater(len(queries), 0)
        with CaptureQueriesContext(connection) as cached_queries:
            self.assertEqual(
                get_unique_or_raise(
                    runner, InventoryItemRole, {"slug": "power-supply"}
                ),
                role,
            )

        self.assertEqual(len(cached_queries), 0)

    def test_dependency_lookup_cache_marks_missing_inventoryitem_role_identity(self):
        runner = ForwardSyncRunner(
            sync=self.sync, ingestion=None, client=None, logger_=Mock()
        )

        prime_dependency_lookup_caches(
            runner,
            "dcim.inventoryitem",
            [
                {
                    "device": "device-1",
                    "manufacturer": "vendor-1",
                    "manufacturer_slug": "vendor-1",
                    "name": "Power Supply 1",
                    "role": "POWER SUPPLY",
                    "role_slug": "power-supply",
                    "role_color": "ff9800",
                }
            ],
        )

        with CaptureQueriesContext(connection) as queries:
            self.assertIsNone(
                get_unique_or_raise(runner, InventoryItemRole, {"slug": "power-supply"})
            )

        self.assertEqual(len(queries), 0)

    def test_dependency_lookup_cache_primes_moduletype_identity(self):
        manufacturer = Manufacturer.objects.create(name="Juniper", slug="juniper")
        module_type = ModuleType.objects.create(
            manufacturer=manufacturer,
            model="line-card-1",
            part_number="line-card-1",
        )
        runner = ForwardSyncRunner(
            sync=self.sync, ingestion=None, client=None, logger_=Mock()
        )

        prime_dependency_lookup_caches(
            runner,
            "dcim.module",
            [
                {
                    "device": "device-1",
                    "manufacturer": "Juniper",
                    "manufacturer_slug": "juniper",
                    "model": "line-card-1",
                    "module_bay": "Slot 1",
                    "status": "active",
                }
            ],
        )

        with CaptureQueriesContext(connection) as cached_queries:
            self.assertEqual(
                get_unique_or_raise(
                    runner,
                    ModuleType,
                    {"manufacturer": manufacturer, "model": "line-card-1"},
                ),
                module_type,
            )

        self.assertEqual(len(cached_queries), 0)

    def test_dependency_lookup_cache_marks_missing_devicetype_identity(self):
        manufacturer = Manufacturer.objects.create(name="Juniper", slug="juniper")
        runner = ForwardSyncRunner(
            sync=self.sync, ingestion=None, client=None, logger_=Mock()
        )

        prime_dependency_lookup_caches(
            runner,
            "dcim.device",
            [
                {
                    "manufacturer": "Juniper",
                    "manufacturer_slug": "juniper",
                    "device_type": "qfx-unknown",
                    "device_type_slug": "qfx-unknown",
                }
            ],
        )

        with CaptureQueriesContext(connection) as queries:
            self.assertIsNone(
                get_unique_or_raise(
                    runner,
                    DeviceType,
                    {"manufacturer": manufacturer, "model": "qfx-unknown"},
                )
            )
            self.assertIsNone(
                get_unique_or_raise(
                    runner,
                    DeviceType,
                    {"slug": "qfx-unknown"},
                )
            )

        self.assertEqual(len(queries), 0)

    def test_dependency_lookup_cache_primes_ipam_ipaddress_global_host_identity(self):
        ip_address = IPAddress.objects.create(
            address="10.0.0.1/24",
            vrf=None,
            status="active",
        )
        runner = ForwardSyncRunner(
            sync=self.sync, ingestion=None, client=None, logger_=Mock()
        )

        with CaptureQueriesContext(connection) as queries:
            prime_dependency_lookup_caches(
                runner,
                "ipam.ipaddress",
                [
                    {
                        "address": "10.0.0.1/32",
                        "vrf": "",
                        "status": "active",
                    }
                ],
            )

        self.assertEqual(len(queries), 2)
        with CaptureQueriesContext(connection) as cached_queries:
            self.assertEqual(
                get_unique_or_raise(
                    runner,
                    IPAddress,
                    {"address__net_host": "10.0.0.1", "vrf__isnull": True},
                ),
                ip_address,
            )

        self.assertEqual(len(cached_queries), 0)

    def test_dependency_lookup_cache_marks_missing_ipam_prefix_identity(self):
        vrf = VRF.objects.create(name="blue", rd="64512:3")
        runner = ForwardSyncRunner(
            sync=self.sync, ingestion=None, client=None, logger_=Mock()
        )

        prime_dependency_lookup_caches(
            runner,
            "ipam.prefix",
            [
                {
                    "prefix": "10.0.9.0/24",
                    "vrf": "blue",
                    "status": "active",
                }
            ],
        )

        with CaptureQueriesContext(connection) as queries:
            self.assertIsNone(
                get_unique_or_raise(
                    runner,
                    Prefix,
                    {"prefix": "10.0.9.0/24", "vrf": vrf},
                )
            )

        self.assertEqual(len(queries), 0)

    def test_dependency_lookup_cache_marks_missing_ipam_prefix_global_identity(self):
        runner = ForwardSyncRunner(
            sync=self.sync, ingestion=None, client=None, logger_=Mock()
        )

        prime_dependency_lookup_caches(
            runner,
            "ipam.prefix",
            [
                {
                    "prefix": "10.9.0.0/24",
                    "vrf": "",
                    "status": "active",
                }
            ],
        )

        with CaptureQueriesContext(connection) as queries:
            self.assertIsNone(
                get_unique_or_raise(
                    runner,
                    Prefix,
                    {"prefix": "10.9.0.0/24", "vrf": None},
                )
            )

        self.assertEqual(len(queries), 0)

    def test_dependency_lookup_cache_marks_missing_ipam_ipaddress_global_host_identity(
        self,
    ):
        runner = ForwardSyncRunner(
            sync=self.sync, ingestion=None, client=None, logger_=Mock()
        )

        prime_dependency_lookup_caches(
            runner,
            "ipam.ipaddress",
            [
                {
                    "address": "10.9.0.10/32",
                    "vrf": "",
                    "status": "active",
                }
            ],
        )

        with CaptureQueriesContext(connection) as queries:
            self.assertIsNone(
                get_unique_or_raise(
                    runner,
                    IPAddress,
                    {"address__net_host": "10.9.0.10", "vrf__isnull": True},
                )
            )

        self.assertEqual(len(queries), 0)

    def test_dependency_lookup_cache_does_not_mask_ambiguous_ipam_global_host_lookup(
        self,
    ):
        IPAddress.objects.create(address="10.44.0.1/24", vrf=None, status="active")
        IPAddress.objects.create(address="10.44.0.1/32", vrf=None, status="active")
        runner = ForwardSyncRunner(
            sync=self.sync, ingestion=None, client=None, logger_=Mock()
        )

        prime_dependency_lookup_caches(
            runner,
            "ipam.ipaddress",
            [
                {
                    "address": "10.44.0.1/24",
                    "vrf": "",
                    "status": "active",
                }
            ],
        )

        with CaptureQueriesContext(connection) as queries:
            with self.assertRaisesRegex(
                ForwardSearchError,
                "Ambiguous coalesce lookup for `ipam.ipaddress`",
            ):
                get_unique_or_raise(
                    runner,
                    IPAddress,
                    {"address__net_host": "10.44.0.1", "vrf__isnull": True},
                )

        self.assertEqual(len(queries), 1)

    def test_apply_dcim_macaddress_aggregates_missing_interface_warnings(self):
        self._create_device("device-mac-missing-interface")
        logger = Mock()
        runner = ForwardSyncRunner(
            sync=self.sync, ingestion=None, client=None, logger_=logger
        )
        rows = [
            {
                "device": "device-mac-missing-interface",
                "interface": f"Ethernet1/{index}",
                "mac": f"00:11:22:33:44:{index:02x}",
                "mac_address": f"00:11:22:33:44:{index:02x}",
            }
            for index in range(ForwardSyncRunner.CONFLICT_WARNING_DETAIL_LIMIT + 2)
        ]

        runner._apply_model_rows("dcim.macaddress", rows)

        warning_messages = [call.args[0] for call in logger.log_warning.call_args_list]
        self.assertEqual(len(warning_messages), 21)
        self.assertEqual(
            warning_messages[-1],
            "Suppressed 2 additional dcim.macaddress skip warnings for "
            "`missing-interface` after the first 20.",
        )

    def test_dependency_lookup_cache_primes_only_exact_interface_pairs(self):
        device_1 = self._create_device("device-pair-1")
        device_2 = self._create_device("device-pair-2")
        target_1 = Interface.objects.create(
            device=device_1,
            name="Ethernet1",
            type="1000base-t",
        )
        Interface.objects.create(
            device=device_1,
            name="Ethernet2",
            type="1000base-t",
        )
        Interface.objects.create(
            device=device_2,
            name="Ethernet1",
            type="1000base-t",
        )
        target_2 = Interface.objects.create(
            device=device_2,
            name="Ethernet2",
            type="1000base-t",
        )
        runner = ForwardSyncRunner(
            sync=self.sync, ingestion=None, client=None, logger_=Mock()
        )

        prime_dependency_lookup_caches(
            runner,
            "dcim.cable",
            [
                {
                    "device": "device-pair-1",
                    "interface": "Ethernet1",
                    "remote_device": "device-pair-2",
                    "remote_interface": "Ethernet2",
                }
            ],
        )

        self.assertEqual(
            runner._interface_by_device_name_cache,
            {
                (device_1.pk, "Ethernet1"): target_1,
                (device_2.pk, "Ethernet2"): target_2,
            },
        )
        self.assertNotIn(
            (device_1.pk, "Ethernet2"),
            runner._interface_by_device_name_cache,
        )
        self.assertNotIn(
            (device_2.pk, "Ethernet1"),
            runner._interface_by_device_name_cache,
        )

    def test_dependency_lookup_cache_primes_only_exact_module_bay_pairs(self):
        device_1 = self._create_device("device-module-pair-1")
        device_2 = self._create_device("device-module-pair-2")
        target_1 = self._create_module_bay(device_1, name="Slot 1")
        self._create_module_bay(device_1, name="Slot 2")
        self._create_module_bay(device_2, name="Slot 1")
        target_2 = self._create_module_bay(device_2, name="Slot 2")
        runner = ForwardSyncRunner(
            sync=self.sync, ingestion=None, client=None, logger_=Mock()
        )

        prime_dependency_lookup_caches(
            runner,
            "dcim.module",
            [
                {"device": "device-module-pair-1", "module_bay": "Slot 1"},
                {"device": "device-module-pair-2", "module_bay": "Slot 2"},
            ],
        )

        self.assertEqual(
            runner._module_bay_by_device_name_cache,
            {
                (device_1.pk, "Slot 1"): target_1,
                (device_2.pk, "Slot 2"): target_2,
            },
        )
        self.assertNotIn(
            (device_1.pk, "Slot 2"),
            runner._module_bay_by_device_name_cache,
        )
        self.assertNotIn(
            (device_2.pk, "Slot 1"),
            runner._module_bay_by_device_name_cache,
        )

    def test_module_bay_lookup_cache_reuses_negative_lookup(self):
        device = self._create_device("device-module-miss")
        runner = ForwardSyncRunner(
            sync=self.sync, ingestion=None, client=None, logger_=Mock()
        )

        with CaptureQueriesContext(connection) as queries:
            self.assertIsNone(runner._lookup_module_bay(device, "Slot 404"))
            self.assertIsNone(runner._lookup_module_bay(device, "Slot 404"))

        self.assertEqual(len(queries), 1)

    def test_coalesce_unique_lookup_uses_single_bounded_query(self):
        site = Site.objects.create(name="site-query-count", slug="site-query-count")
        runner = ForwardSyncRunner(
            sync=self.sync, ingestion=None, client=None, logger_=Mock()
        )

        with CaptureQueriesContext(connection) as queries:
            self.assertEqual(
                get_unique_or_raise(
                    runner,
                    Site,
                    {"slug": "site-query-count"},
                ),
                site,
            )

        self.assertEqual(len(queries), 1)
        self.assertIn("LIMIT 2", queries[0]["sql"].upper())

    def test_coalesce_unique_lookup_preserves_ambiguous_guard(self):
        device_1 = self._create_device("device-ambiguous-1")
        device_2 = self._create_device("device-ambiguous-2")
        Interface.objects.create(
            device=device_1,
            name="Ethernet1",
            type="1000base-t",
        )
        Interface.objects.create(
            device=device_2,
            name="Ethernet1",
            type="1000base-t",
        )
        runner = ForwardSyncRunner(
            sync=self.sync, ingestion=None, client=None, logger_=Mock()
        )

        with self.assertRaisesRegex(
            ForwardSearchError,
            "Ambiguous coalesce lookup for `dcim.interface`",
        ):
            get_unique_or_raise(runner, Interface, {"name": "Ethernet1"})

    def test_asn_unique_lookup_reuses_positive_cache(self):
        rir = RIR.objects.create(name="Forward Observed", slug="forward-observed")
        asn = ASN.objects.create(asn=64512, rir=rir)
        runner = ForwardSyncRunner(
            sync=self.sync, ingestion=None, client=None, logger_=Mock()
        )

        with CaptureQueriesContext(connection) as queries:
            self.assertEqual(get_unique_or_raise(runner, ASN, {"asn": 64512}), asn)
            self.assertEqual(get_unique_or_raise(runner, ASN, {"asn": "64512"}), asn)

        self.assertEqual(len(queries), 1)

    def test_asn_unique_lookup_does_not_cache_misses(self):
        rir = RIR.objects.create(name="Forward Observed", slug="forward-observed")
        runner = ForwardSyncRunner(
            sync=self.sync, ingestion=None, client=None, logger_=Mock()
        )

        self.assertIsNone(get_unique_or_raise(runner, ASN, {"asn": 64513}))
        asn = ASN.objects.create(asn=64513, rir=rir)

        with CaptureQueriesContext(connection) as queries:
            self.assertEqual(get_unique_or_raise(runner, ASN, {"asn": 64513}), asn)

        self.assertEqual(len(queries), 1)

    def test_vrf_unique_lookup_reuses_positive_cache(self):
        vrf = VRF.objects.create(name="blue", rd="64512:1")
        runner = ForwardSyncRunner(
            sync=self.sync, ingestion=None, client=None, logger_=Mock()
        )

        with CaptureQueriesContext(connection) as queries:
            self.assertEqual(get_unique_or_raise(runner, VRF, {"name": "blue"}), vrf)
            self.assertEqual(get_unique_or_raise(runner, VRF, {"rd": "64512:1"}), vrf)

        self.assertEqual(len(queries), 1)

    def test_vrf_unique_lookup_does_not_cache_misses(self):
        runner = ForwardSyncRunner(
            sync=self.sync, ingestion=None, client=None, logger_=Mock()
        )

        self.assertIsNone(get_unique_or_raise(runner, VRF, {"name": "green"}))
        vrf = VRF.objects.create(name="green", rd="64512:2")

        with CaptureQueriesContext(connection) as queries:
            self.assertEqual(get_unique_or_raise(runner, VRF, {"name": "green"}), vrf)

        self.assertEqual(len(queries), 1)

    def test_bgp_peer_contract_accepts_minimal_query_row(self):
        validate_row_shape_for_model(
            "netbox_routing.bgppeer",
            {
                "device": "device-1",
                "vrf": None,
                "local_asn": 64512,
                "neighbor_address": "192.0.2.1",
                "peer_asn": 64513,
                "enabled": True,
                "status": "active",
            },
            [["device", "vrf", "neighbor_address"], ["device", "neighbor_address"]],
        )

    def test_bgp_peer_adapter_records_failure_when_optional_plugin_is_missing(self):
        runner = ForwardSyncRunner(
            sync=self.sync, ingestion=None, client=None, logger_=Mock()
        )

        with patch(
            "forward_netbox.utilities.sync_reporting.record_issue"
        ) as record_issue:
            runner._apply_model_rows(
                "netbox_routing.bgppeer",
                [
                    {
                        "device": "device-1",
                        "vrf": None,
                        "local_asn": 64512,
                        "neighbor_address": "192.0.2.1",
                        "peer_asn": 64513,
                        "enabled": True,
                        "status": "active",
                    }
                ],
            )

        runner.logger.increment_statistics.assert_any_call(
            "netbox_routing.bgppeer", outcome="failed"
        )
        record_issue.assert_called_once()

    def test_bgp_asn_reuses_existing_asn_without_changing_rir(self):
        rir = RIR.objects.create(name="ARIN")
        asn = ASN.objects.create(rir=rir, asn=64512)
        runner = ForwardSyncRunner(
            sync=self.sync, ingestion=None, client=None, logger_=Mock()
        )

        self.assertEqual(runner._ensure_asn(64512), asn)
        asn.refresh_from_db()
        self.assertEqual(asn.rir, rir)
        self.assertFalse(RIR.objects.filter(slug="forward-observed").exists())

    def test_bgp_asn_rejects_non_positive_values_before_netbox_validation(self):
        runner = ForwardSyncRunner(
            sync=self.sync, ingestion=None, client=None, logger_=Mock()
        )

        with self.assertRaisesRegex(ForwardQueryError, "greater than or equal to 1"):
            runner._ensure_asn(0)

    def test_routing_router_unique_lookup_reuses_positive_cache(self):
        if not apps.is_installed("netbox_routing"):
            self.skipTest("netbox-routing optional plugin is not installed")
        BGPRouter = apps.get_model("netbox_routing", "BGPRouter")
        device = self._create_device("device-router-cache")
        asn = ASN.objects.create(rir=RIR.objects.create(name="ARIN"), asn=64512)
        content_type = ContentType.objects.get_for_model(Device)
        router = BGPRouter.objects.create(
            name="device-router-cache AS64512",
            assigned_object_type=content_type,
            assigned_object_id=device.pk,
            asn=asn,
        )
        runner = ForwardSyncRunner(
            sync=self.sync, ingestion=None, client=None, logger_=Mock()
        )
        lookup = {
            "assigned_object_type": content_type,
            "assigned_object_id": device.pk,
            "asn": asn,
        }

        with CaptureQueriesContext(connection) as queries:
            self.assertEqual(get_unique_or_raise(runner, BGPRouter, lookup), router)
            self.assertEqual(get_unique_or_raise(runner, BGPRouter, lookup), router)

        self.assertEqual(len(queries), 1)

    def test_bgp_scope_uses_exact_vrf_lookup_without_router_only_ambiguity(self):
        if not apps.is_installed("netbox_routing"):
            self.skipTest("netbox-routing optional plugin is not installed")
        BGPRouter = apps.get_model("netbox_routing", "BGPRouter")
        BGPScope = apps.get_model("netbox_routing", "BGPScope")
        device = self._create_device("device-1")
        asn = ASN.objects.create(rir=RIR.objects.create(name="ARIN"), asn=64512)
        router = BGPRouter.objects.create(
            name="device-1 AS64512",
            assigned_object_type=ContentType.objects.get_for_model(Device),
            assigned_object_id=device.pk,
            asn=asn,
        )
        global_scope = BGPScope.objects.create(router=router, vrf=None)
        BGPScope.objects.create(router=router, vrf=VRF.objects.create(name="VRF-A"))
        runner = ForwardSyncRunner(
            sync=self.sync, ingestion=None, client=None, logger_=Mock()
        )

        self.assertEqual(runner._ensure_bgp_scope({}, router, None), global_scope)
        runner.logger.log_warning.assert_not_called()

    def test_bgp_peer_address_family_adapter_creates_native_address_family(self):
        if not apps.is_installed("netbox_routing"):
            self.skipTest("netbox-routing optional plugin is not installed")
        BGPPeer = apps.get_model("netbox_routing", "BGPPeer")
        BGPAddressFamily = apps.get_model("netbox_routing", "BGPAddressFamily")
        BGPPeerAddressFamily = apps.get_model("netbox_routing", "BGPPeerAddressFamily")
        self._create_device("device-1")
        runner = ForwardSyncRunner(
            sync=self.sync, ingestion=None, client=None, logger_=Mock()
        )

        runner._apply_model_rows(
            "netbox_routing.bgppeeraddressfamily",
            [
                {
                    "device": "device-1",
                    "vrf": None,
                    "local_asn": 64512,
                    "router_id": "192.0.2.254",
                    "neighbor_address": "192.0.2.1",
                    "peer_asn": 64513,
                    "peer_type": "PeerType.EXTERNAL",
                    "afi_safi": "AfiSafiType.IPV4_UNICAST",
                    "enabled": True,
                    "status": "active",
                    "has_adj_rib_in": False,
                    "has_adj_rib_out": True,
                },
                {
                    "device": "device-1",
                    "vrf": None,
                    "local_asn": 64512,
                    "router_id": "192.0.2.254",
                    "neighbor_address": "192.0.2.1",
                    "peer_asn": 64513,
                    "peer_type": "PeerType.EXTERNAL",
                    "afi_safi": "AfiSafiType.L3VPN_IPV4_UNICAST",
                    "enabled": True,
                    "status": "active",
                    "has_adj_rib_in": True,
                    "has_adj_rib_out": False,
                },
            ],
        )

        self.assertEqual(BGPPeer.objects.count(), 1)
        self.assertIn("Peer type: PeerType.EXTERNAL", BGPPeer.objects.get().comments)
        self.assertCountEqual(
            BGPAddressFamily.objects.values_list("address_family", flat=True),
            ["ipv4-unicast", "vpnv4-unicast"],
        )
        self.assertTrue(
            all(
                "Forward AFI/SAFI:" in comments
                for comments in BGPAddressFamily.objects.values_list(
                    "comments", flat=True
                )
            )
        )
        self.assertEqual(BGPPeerAddressFamily.objects.count(), 2)
        peer_af_comments = "\n".join(
            BGPPeerAddressFamily.objects.values_list("comments", flat=True)
        )
        self.assertIn("Adj-RIB-In post-policy: present", peer_af_comments)
        self.assertIn("Adj-RIB-In post-policy: absent", peer_af_comments)
        self.assertIn("Adj-RIB-Out post-policy: present", peer_af_comments)
        self.assertIn("Adj-RIB-Out post-policy: absent", peer_af_comments)

    def test_ospf_interface_adapter_preserves_named_process_label(self):
        if not apps.is_installed("netbox_routing"):
            self.skipTest("netbox-routing optional plugin is not installed")
        OSPFInstance = apps.get_model("netbox_routing", "OSPFInstance")
        OSPFArea = apps.get_model("netbox_routing", "OSPFArea")
        OSPFInterface = apps.get_model("netbox_routing", "OSPFInterface")
        device = self._create_device("device-1")
        Interface.objects.create(device=device, name="Ethernet1/1", type="1000base-t")
        runner = ForwardSyncRunner(
            sync=self.sync, ingestion=None, client=None, logger_=Mock()
        )

        runner._apply_model_rows(
            "netbox_routing.ospfinterface",
            [
                {
                    "device": "device-1",
                    "vrf": None,
                    "process_id": "UNDERLAY",
                    "domain": "fabric",
                    "router_id": "192.0.2.254",
                    "area_id": "0",
                    "area_type": "OspfAreaType.BACKBONE",
                    "local_interface": "Ethernet1/1",
                    "remote_router_id": "192.0.2.253",
                    "remote_interface_ip": "192.0.2.253/31",
                    "cost": 1,
                    "role": "OspfRole.DESIGNATED_ROUTER",
                    "remote_device": "device-2",
                    "remote_interface": "Ethernet1/2",
                }
            ],
        )

        instance = OSPFInstance.objects.get()
        self.assertGreaterEqual(instance.process_id, 1_000_000)
        self.assertIn("UNDERLAY", instance.comments)
        self.assertEqual(OSPFArea.objects.get().area_type, "backbone")
        ospf_interface = OSPFInterface.objects.get()
        self.assertEqual(ospf_interface.interface.name, "Ethernet1/1")
        self.assertIn("Cost: 1", ospf_interface.comments)
        self.assertIn("Role: OspfRole.DESIGNATED_ROUTER", ospf_interface.comments)
        self.assertIn("Remote device: device-2", ospf_interface.comments)
        self.assertIn("Remote interface: Ethernet1/2", ospf_interface.comments)
        self.assertIn("Remote interface IP: 192.0.2.253/31", ospf_interface.comments)
        self.assertIn("Remote router ID: 192.0.2.253", ospf_interface.comments)

    def test_ospf_interface_adapter_resolves_common_interface_aliases(self):
        if not apps.is_installed("netbox_routing"):
            self.skipTest("netbox-routing optional plugin is not installed")
        OSPFInterface = apps.get_model("netbox_routing", "OSPFInterface")
        device = self._create_device("device-1")
        Interface.objects.create(
            device=device,
            name="GigabitEthernet0/0/2",
            type="1000base-t",
        )
        Interface.objects.create(
            device=device,
            name="Port-channel3",
            type="lag",
        )
        Interface.objects.create(
            device=device,
            name="TenGigabitEthernet0/1/3.765",
            type="10gbase-t",
        )
        runner = ForwardSyncRunner(
            sync=self.sync, ingestion=None, client=None, logger_=Mock()
        )

        runner._apply_model_rows(
            "netbox_routing.ospfinterface",
            [
                {
                    "device": "device-1",
                    "vrf": None,
                    "process_id": "UNDERLAY",
                    "domain": "fabric",
                    "router_id": "192.0.2.254",
                    "area_id": "0",
                    "area_type": "OspfAreaType.BACKBONE",
                    "local_interface": "gi0/0/2",
                    "remote_router_id": "192.0.2.253",
                    "remote_interface_ip": "192.0.2.253/31",
                    "cost": 1,
                    "role": "OspfRole.DESIGNATED_ROUTER",
                    "remote_device": "device-2",
                    "remote_interface": "Ethernet1/2",
                },
                {
                    "device": "device-1",
                    "vrf": None,
                    "process_id": "UNDERLAY",
                    "domain": "fabric",
                    "router_id": "192.0.2.254",
                    "area_id": "0",
                    "area_type": "OspfAreaType.BACKBONE",
                    "local_interface": "po3",
                    "remote_router_id": "192.0.2.253",
                    "remote_interface_ip": "192.0.2.253/31",
                    "cost": 1,
                    "role": "OspfRole.DESIGNATED_ROUTER",
                    "remote_device": "device-2",
                    "remote_interface": "Ethernet1/2",
                },
                {
                    "device": "device-1",
                    "vrf": None,
                    "process_id": "UNDERLAY",
                    "domain": "fabric",
                    "router_id": "192.0.2.254",
                    "area_id": "0",
                    "area_type": "OspfAreaType.BACKBONE",
                    "local_interface": "te0/1/3.765",
                    "remote_router_id": "192.0.2.253",
                    "remote_interface_ip": "192.0.2.253/31",
                    "cost": 1,
                    "role": "OspfRole.DESIGNATED_ROUTER",
                    "remote_device": "device-2",
                    "remote_interface": "Ethernet1/2",
                },
            ],
        )

        self.assertCountEqual(
            OSPFInterface.objects.values_list("interface__name", flat=True),
            ["GigabitEthernet0/0/2", "Port-channel3", "TenGigabitEthernet0/1/3.765"],
        )

    def test_ospf_interface_adapter_skips_missing_interface_without_failure(self):
        if not apps.is_installed("netbox_routing"):
            self.skipTest("netbox-routing optional plugin is not installed")
        OSPFInterface = apps.get_model("netbox_routing", "OSPFInterface")
        device = self._create_device("device-1")
        Interface.objects.create(device=device, name="Ethernet1/1", type="1000base-t")
        logger = Mock()
        runner = ForwardSyncRunner(
            sync=self.sync, ingestion=None, client=None, logger_=logger
        )

        runner._apply_model_rows(
            "netbox_routing.ospfinterface",
            [
                {
                    "device": "device-1",
                    "vrf": None,
                    "process_id": "UNDERLAY",
                    "domain": "fabric",
                    "router_id": "192.0.2.254",
                    "area_id": "0",
                    "area_type": "OspfAreaType.BACKBONE",
                    "local_interface": "Ethernet1/999",
                    "remote_router_id": "192.0.2.253",
                    "remote_interface_ip": "192.0.2.253/31",
                    "cost": 1,
                    "role": "OspfRole.DESIGNATED_ROUTER",
                    "remote_device": "device-2",
                    "remote_interface": "Ethernet1/2",
                }
            ],
        )

        self.assertEqual(OSPFInterface.objects.count(), 0)
        warning_messages = [call.args[0] for call in logger.log_warning.call_args_list]
        self.assertTrue(
            any(
                "Skipping OSPF interface row on `device-1` because local interface `Ethernet1/999` was not imported."
                in message
                for message in warning_messages
            )
        )

    def test_apply_dcim_interface_sets_lag_membership_after_parent(self):
        self._create_device("device-1")
        runner = ForwardSyncRunner(
            sync=self.sync, ingestion=None, client=None, logger_=Mock()
        )

        runner._apply_model_rows(
            "dcim.interface",
            [
                {
                    "device": "device-1",
                    "name": "eth1-1",
                    "type": "1000base-t",
                    "lag": "bond0",
                    "enabled": True,
                    "mtu": 9000,
                    "description": "",
                    "speed": 1000000,
                },
                {
                    "device": "device-1",
                    "name": "bond0",
                    "type": "lag",
                    "lag": None,
                    "enabled": True,
                    "mtu": 9000,
                    "description": "",
                    "speed": None,
                },
            ],
        )

        lag = Interface.objects.get(device__name="device-1", name="bond0")
        member = Interface.objects.get(device__name="device-1", name="eth1-1")
        self.assertEqual(lag.type, "lag")
        self.assertEqual(member.lag, lag)
        self.assertEqual(member.mtu, 9000)

    def test_apply_dcim_interface_creates_lag_placeholder_across_shards(self):
        self._create_device("device-1")
        runner = ForwardSyncRunner(
            sync=self.sync, ingestion=None, client=None, logger_=Mock()
        )

        runner._apply_model_rows(
            "dcim.interface",
            [
                {
                    "device": "device-1",
                    "name": "eth1-1",
                    "type": "1000base-t",
                    "lag": "bond0",
                    "enabled": True,
                    "mtu": 9000,
                    "description": "",
                    "speed": 1000000,
                },
            ],
        )

        lag = Interface.objects.get(device__name="device-1", name="bond0")
        member = Interface.objects.get(device__name="device-1", name="eth1-1")
        self.assertEqual(lag.type, "lag")
        self.assertIsNone(lag.mtu)
        self.assertEqual(member.lag, lag)

        runner._apply_model_rows(
            "dcim.interface",
            [
                {
                    "device": "device-1",
                    "name": "bond0",
                    "type": "lag",
                    "lag": None,
                    "enabled": True,
                    "mtu": 9000,
                    "description": "aggregate",
                    "speed": None,
                },
            ],
        )

        lag.refresh_from_db()
        self.assertEqual(lag.mtu, 9000)
        self.assertEqual(lag.description, "aggregate")

    def test_apply_dcim_interface_removes_existing_cable_before_lag_conversion(self):
        device = self._create_device("device-1")
        remote_device = self._create_device("device-2")
        lag = Interface.objects.create(
            device=device,
            name="bond0",
            type="1000base-t",
        )
        remote = Interface.objects.create(
            device=remote_device,
            name="Ethernet1/1",
            type="1000base-t",
        )
        Cable.objects.create(a_terminations=[lag], b_terminations=[remote])
        runner = ForwardSyncRunner(
            sync=self.sync, ingestion=None, client=None, logger_=Mock()
        )

        runner._apply_model_rows(
            "dcim.interface",
            [
                {
                    "device": "device-1",
                    "name": "bond0",
                    "type": "lag",
                    "lag": None,
                    "enabled": True,
                    "mtu": 9000,
                    "description": "aggregate",
                    "speed": None,
                },
            ],
        )

        lag.refresh_from_db()
        self.assertEqual(lag.type, "lag")
        self.assertIsNone(lag.cable)
        self.assertEqual(Cable.objects.count(), 0)
        runner.logger.log_warning.assert_called_once()

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

    def test_apply_extras_taggeditem_reuses_cached_assignment_without_db_queries(self):
        self._create_device("device-1")
        runner = ForwardSyncRunner(
            sync=self.sync, ingestion=None, client=None, logger_=Mock()
        )
        row = {
            "device": "device-1",
            "tag": "Prot_BGP",
            "tag_slug": "prot-bgp",
            "tag_color": "2196f3",
        }

        runner._apply_extras_taggeditem(row)
        with CaptureQueriesContext(connection) as queries:
            runner._apply_extras_taggeditem(row)

        self.assertEqual(len(queries), 0)
        device = Device.objects.get(name="device-1")
        self.assertEqual(device.tags.filter(slug="prot-bgp").count(), 1)

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

    def test_apply_dcim_inventoryitem_sets_native_optional_fields(self):
        device = self._create_device("device-1")
        runner = ForwardSyncRunner(
            sync=self.sync, ingestion=None, client=None, logger_=Mock()
        )

        runner._apply_dcim_inventoryitem(
            {
                "device": "device-1",
                "manufacturer": "vendor-1",
                "manufacturer_slug": "vendor-1",
                "name": "Power Supply 1",
                "label": "PSU 1",
                "part_id": "",
                "serial": "",
                "asset_tag": "ASSET-1",
                "role": "POWER SUPPLY",
                "role_slug": "power-supply",
                "role_color": "ff9800",
                "part_type": "POWER SUPPLY",
                "module_component": False,
                "status": "active",
                "discovered": True,
                "description": "Version: V01",
            }
        )

        item = InventoryItem.objects.get(device=device, name="Power Supply 1")
        self.assertEqual(item.label, "PSU 1")
        self.assertEqual(item.part_id, "")
        self.assertEqual(item.serial, "")
        self.assertEqual(item.asset_tag, "ASSET-1")
        self.assertEqual(item.role.slug, "power-supply")
        self.assertEqual(item.role.color, "ff9800")
        self.assertEqual(item.description, "Version: V01")

    def test_apply_dcim_inventoryitem_cleans_module_backed_rows_when_modules_enabled(
        self,
    ):
        device = self._create_device("device-1")
        InventoryItem.objects.create(
            device=device,
            name="Slot 1",
            part_id="LC-1",
            serial="SN-1",
            status="active",
            discovered=True,
        )
        self.sync.parameters = {**self.sync.parameters, "dcim.module": True}
        self.sync.save(update_fields=["parameters"])
        runner = ForwardSyncRunner(
            sync=self.sync, ingestion=None, client=None, logger_=Mock()
        )

        result = runner._apply_dcim_inventoryitem(
            {
                "device": "device-1",
                "manufacturer": "vendor-1",
                "manufacturer_slug": "vendor-1",
                "name": "Slot 1",
                "label": "Slot 1",
                "part_id": "LC-1",
                "serial": "SN-1",
                "asset_tag": None,
                "role": "LINE CARD",
                "role_slug": "line-card",
                "role_color": "3f51b5",
                "part_type": "LINE CARD",
                "module_component": True,
                "status": "active",
                "discovered": True,
                "description": "Line card",
            }
        )

        self.assertIsNone(result)
        self.assertFalse(
            InventoryItem.objects.filter(device=device, name="Slot 1").exists()
        )

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

    def test_apply_dcim_cable_skips_lag_endpoint(self):
        device = self._create_device("device-a")
        remote_device = self._create_device("device-b")
        Interface.objects.create(
            device=device,
            name="Port-channel1",
            type="lag",
        )
        Interface.objects.create(
            device=remote_device,
            name="Ethernet1/2",
            type="1000base-t",
        )
        logger = Mock()
        runner = ForwardSyncRunner(
            sync=self.sync, ingestion=None, client=None, logger_=logger
        )

        result = runner._apply_dcim_cable(
            {
                "device": "device-a",
                "interface": "Port-channel1",
                "remote_device": "device-b",
                "remote_interface": "Ethernet1/2",
                "status": "connected",
            }
        )

        self.assertFalse(result)
        self.assertEqual(Cable.objects.count(), 0)
        logger.log_warning.assert_called_once_with(
            "Skipping cable row because NetBox does not allow cables terminated directly to LAG interfaces.",
            obj=self.sync,
        )

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

    def test_apply_dcim_cable_aggregates_missing_remote_device_warnings(self):
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
        rows = [
            {
                "device": "device-a",
                "interface": "Ethernet1/1",
                "remote_device": f"synthetic-node-{index}",
                "remote_interface": "Ethernet1/2",
                "status": "connected",
            }
            for index in range(ForwardSyncRunner.CONFLICT_WARNING_DETAIL_LIMIT + 4)
        ]

        runner._apply_model_rows("dcim.cable", rows)

        warning_messages = [call.args[0] for call in logger.log_warning.call_args_list]
        self.assertEqual(len(warning_messages), 21)
        self.assertEqual(
            warning_messages[-1],
            "Suppressed 4 additional dcim.cable skip warnings for "
            "`missing-remote-device` after the first 20.",
        )

    def test_apply_dcim_cable_aggregates_missing_interface_warnings(self):
        self._create_device("device-a")
        remote_device = self._create_device("device-b")
        Interface.objects.create(
            device=remote_device,
            name="Ethernet1/2",
            type="1000base-t",
        )
        logger = Mock()
        runner = ForwardSyncRunner(
            sync=self.sync, ingestion=None, client=None, logger_=logger
        )
        rows = [
            {
                "device": "device-a",
                "interface": f"Ethernet1/{index}",
                "remote_device": "device-b",
                "remote_interface": "Ethernet1/2",
                "status": "connected",
            }
            for index in range(ForwardSyncRunner.CONFLICT_WARNING_DETAIL_LIMIT + 2)
        ]

        runner._apply_model_rows("dcim.cable", rows)

        warning_messages = [call.args[0] for call in logger.log_warning.call_args_list]
        self.assertEqual(len(warning_messages), 21)
        self.assertEqual(
            warning_messages[-1],
            "Suppressed 2 additional dcim.cable skip warnings for "
            "`missing-interface` after the first 20.",
        )

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

    def test_apply_dcim_module_creates_module_when_module_bay_exists(self):
        device = self._create_device("device-a")
        module_bay = self._create_module_bay(device)
        runner = ForwardSyncRunner(
            sync=self.sync, ingestion=None, client=None, logger_=Mock()
        )
        row = {
            "device": device.name,
            "module_bay": "Slot 1",
            "manufacturer": "vendor-1",
            "manufacturer_slug": "vendor-1",
            "model": "Line Card 1",
            "part_number": "LC-1",
            "status": "active",
            "serial": "SN-1",
            "asset_tag": "AT-1",
            "description": "line card",
        }

        runner._apply_dcim_module(row)

        module = Module.objects.get(device=device, module_bay=module_bay)
        self.assertEqual(module_bay.label, "Slot 1")
        self.assertEqual(module.module_type.manufacturer.slug, "vendor-1")
        self.assertEqual(module.module_type.model, "Line Card 1")
        self.assertEqual(module.module_type.part_number, "LC-1")
        self.assertEqual(module.status, "active")
        self.assertEqual(module.serial, "SN-1")
        self.assertEqual(module.asset_tag, "AT-1")

    def test_apply_dcim_module_creates_missing_module_bay_natively(self):
        device = self._create_device("device-a")
        runner = ForwardSyncRunner(
            sync=self.sync, ingestion=None, client=None, logger_=Mock()
        )
        row = {
            "device": device.name,
            "module_bay": "Slot 2",
            "manufacturer": "vendor-1",
            "manufacturer_slug": "vendor-1",
            "model": "Line Card 1",
            "part_number": "LC-1",
            "status": "active",
        }

        runner._apply_dcim_module(row)

        module_bay = ModuleBay.objects.get(device=device, name="Slot 2")
        module = Module.objects.get(device=device, module_bay=module_bay)
        self.assertEqual(module_bay.label, "Slot 2")
        self.assertEqual(module_bay.position, "2")
        self.assertEqual(module.module_type.model, "Line Card 1")

    def test_apply_dcim_module_reuses_existing_module_bay_and_module_type(self):
        device = self._create_device("device-a")
        manufacturer = Manufacturer.objects.get(slug="vendor-1")
        module_type = ModuleType.objects.create(
            manufacturer=manufacturer,
            model="Line Card 1",
            part_number="LC-1",
            description="",
            comments="",
        )
        module_bay = self._create_module_bay(device)
        module = Module.objects.create(
            device=device,
            module_bay=module_bay,
            module_type=module_type,
            status="active",
            serial="SN-1",
            asset_tag="AT-1",
            description="line card",
            comments="",
        )
        runner = ForwardSyncRunner(
            sync=self.sync, ingestion=None, client=None, logger_=Mock()
        )
        row = {
            "device": device.name,
            "module_bay": "Slot 1",
            "manufacturer": "vendor-1",
            "manufacturer_slug": "vendor-1",
            "model": "Line Card 1",
            "part_number": "LC-1",
            "status": "active",
            "serial": "SN-2",
            "asset_tag": "AT-2",
            "description": "line card",
        }

        runner._apply_dcim_module(row)

        module.refresh_from_db()
        self.assertEqual(module.pk, Module.objects.get(pk=module.pk).pk)
        self.assertEqual(module.status, "active")
        self.assertEqual(module.serial, "SN-2")
        self.assertEqual(module.asset_tag, "AT-2")
        self.assertEqual(module.module_bay, module_bay)
        self.assertEqual(
            ModuleType.objects.filter(
                manufacturer=manufacturer, model="Line Card 1"
            ).count(),
            1,
        )

    def test_delete_dcim_module_deletes_exact_module(self):
        device = self._create_device("device-a")
        manufacturer = Manufacturer.objects.get(slug="vendor-1")
        module_type = ModuleType.objects.create(
            manufacturer=manufacturer,
            model="Line Card 1",
            part_number="LC-1",
            description="",
            comments="",
        )
        module_bay = self._create_module_bay(device)
        Module.objects.create(
            device=device,
            module_bay=module_bay,
            module_type=module_type,
            status="active",
            serial="SN-1",
            asset_tag="AT-1",
            description="line card",
            comments="",
        )
        runner = ForwardSyncRunner(
            sync=self.sync, ingestion=None, client=None, logger_=Mock()
        )
        row = {
            "device": device.name,
            "module_bay": "Slot 1",
            "manufacturer": "vendor-1",
            "manufacturer_slug": "vendor-1",
            "model": "Line Card 1",
            "part_number": "LC-1",
            "status": "active",
        }

        self.assertTrue(runner._delete_dcim_module(row))
        self.assertFalse(runner._delete_dcim_module(row))
        self.assertEqual(
            ModuleBay.objects.filter(device=device, name="Slot 1").count(), 1
        )

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

    def test_upsert_values_dedupes_coalesce_lookups_after_null_elision(self):
        prefix = Prefix.objects.create(prefix="10.0.0.0/24", vrf=None, status="active")
        runner = ForwardSyncRunner(
            sync=self.sync, ingestion=None, client=None, logger_=Mock()
        )

        with patch(
            "forward_netbox.utilities.sync_primitives.get_unique_or_raise",
            wraps=get_unique_or_raise,
        ) as lookup_mock:
            upserted, created = runner._upsert_values_from_defaults(
                "ipam.prefix",
                Prefix,
                values={
                    "prefix": "10.0.0.0/24",
                    "vrf": None,
                    "status": "active",
                },
                coalesce_sets=[("prefix", "vrf"), ("prefix",)],
            )

        self.assertFalse(created)
        self.assertEqual(upserted, prefix)
        self.assertEqual(lookup_mock.call_count, 1)

    def test_delete_by_coalesce_dedupes_duplicate_identity_lookups(self):
        prefix = Prefix.objects.create(prefix="10.0.1.0/24", vrf=None, status="active")
        runner = ForwardSyncRunner(
            sync=self.sync, ingestion=None, client=None, logger_=Mock()
        )

        with patch(
            "forward_netbox.utilities.sync_primitives.get_unique_or_raise",
            wraps=get_unique_or_raise,
        ) as lookup_mock:
            deleted = runner._delete_by_coalesce(
                Prefix,
                [{"prefix": "10.0.1.0/24"}, {"prefix": "10.0.1.0/24"}],
            )

        self.assertTrue(deleted)
        self.assertEqual(lookup_mock.call_count, 1)
        self.assertFalse(Prefix.objects.filter(pk=prefix.pk).exists())

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

    def test_apply_ipam_ipaddress_skips_unassignable_network_and_broadcast_addresses(
        self,
    ):
        device = self._create_device("device-1")
        Interface.objects.create(device=device, name="VLAN699", type="virtual")
        logger = Mock()
        runner = ForwardSyncRunner(
            sync=self.sync, ingestion=None, client=None, logger_=logger
        )

        runner._apply_model_rows(
            "ipam.ipaddress",
            [
                {
                    "device": "device-1",
                    "interface": "VLAN699",
                    "address": "11.138.0.16/28",
                    "vrf": None,
                    "status": "active",
                },
                {
                    "device": "device-1",
                    "interface": "VLAN699",
                    "address": "11.138.0.31/28",
                    "vrf": None,
                    "status": "active",
                },
            ],
        )

        self.assertEqual(IPAddress.objects.count(), 0)
        warning_messages = [call.args[0] for call in logger.log_warning.call_args_list]
        self.assertEqual(len(warning_messages), 2)
        self.assertIn("subnet network IDs", warning_messages[0])
        self.assertIn("broadcast addresses", warning_messages[1])
        logger.increment_statistics.assert_any_call("ipam.ipaddress", outcome="skipped")

    def test_apply_ipam_ipaddress_skips_missing_interface_and_continues(
        self,
    ):
        ingestion = ForwardIngestion.objects.create(sync=self.sync)
        device = self._create_device("device-1")
        Interface.objects.create(
            device=device,
            name="Ethernet1/1",
            type="1000base-t",
        )
        logger = Mock()
        runner = ForwardSyncRunner(
            sync=self.sync,
            ingestion=ingestion,
            client=None,
            logger_=logger,
        )

        runner._apply_model_rows(
            "ipam.ipaddress",
            [
                {
                    "device": "device-1",
                    "interface": "Ethernet9/9",
                    "address": "10.0.0.1/24",
                    "vrf": None,
                    "status": "active",
                },
                {
                    "device": "device-1",
                    "interface": "Ethernet1/1",
                    "address": "10.0.0.2/24",
                    "vrf": None,
                    "status": "active",
                },
            ],
        )

        self.assertEqual(
            ForwardIngestionIssue.objects.filter(
                ingestion=ingestion,
                model="ipam.ipaddress",
            ).count(),
            0,
        )
        self.assertEqual(IPAddress.objects.count(), 1)
        self.assertEqual(str(IPAddress.objects.get().address), "10.0.0.2/24")
        warning_messages = [call.args[0] for call in logger.log_warning.call_args_list]
        self.assertEqual(len(warning_messages), 1)
        self.assertIn("target interface was not imported", warning_messages[0])
        logger.increment_statistics.assert_any_call("ipam.ipaddress", outcome="skipped")
        logger.increment_statistics.assert_any_call("ipam.ipaddress", outcome="applied")

    def test_apply_ipam_ipaddress_allows_point_to_point_endpoint_addresses(self):
        device = self._create_device("device-1")
        Interface.objects.create(device=device, name="Ethernet1/1", type="1000base-t")
        logger = Mock()
        runner = ForwardSyncRunner(
            sync=self.sync, ingestion=None, client=None, logger_=logger
        )

        runner._apply_ipam_ipaddress(
            {
                "device": "device-1",
                "interface": "Ethernet1/1",
                "address": "10.0.0.0/31",
                "vrf": None,
                "status": "active",
            }
        )

        self.assertEqual(str(IPAddress.objects.get().address), "10.0.0.0/31")
        logger.log_warning.assert_not_called()

    def test_apply_ipam_ipaddress_updates_existing_global_host_ip_row(self):
        device = self._create_device("device-1")
        interface = Interface.objects.create(
            device=device,
            name="VLAN897",
            type="virtual",
        )
        IPAddress.objects.create(
            address="192.0.2.3/17",
            vrf=None,
            status="active",
            assigned_object_type=ContentType.objects.get_for_model(Interface),
            assigned_object_id=interface.pk,
        )
        logger = Mock()
        runner = ForwardSyncRunner(
            sync=self.sync, ingestion=None, client=None, logger_=logger
        )

        runner._apply_ipam_ipaddress(
            {
                "device": "device-1",
                "interface": "VLAN897",
                "host_ip": "192.0.2.3",
                "address": "192.0.2.3/24",
                "vrf": None,
                "status": "active",
            }
        )

        self.assertEqual(IPAddress.objects.count(), 1)
        self.assertEqual(str(IPAddress.objects.get().address), "192.0.2.3/24")
        logger.log_warning.assert_not_called()

    def test_apply_ipam_ipaddress_does_not_update_when_global_host_row_unchanged(self):
        device = self._create_device("device-1")
        interface = Interface.objects.create(
            device=device,
            name="VLAN897",
            type="virtual",
        )
        IPAddress.objects.create(
            address="192.0.2.3/24",
            vrf=None,
            status="active",
            assigned_object_type=ContentType.objects.get_for_model(Interface),
            assigned_object_id=interface.pk,
        )
        logger = Mock()
        runner = ForwardSyncRunner(
            sync=self.sync, ingestion=None, client=None, logger_=logger
        )

        with CaptureQueriesContext(connection) as queries:
            runner._apply_ipam_ipaddress(
                {
                    "device": "device-1",
                    "interface": "VLAN897",
                    "host_ip": "192.0.2.3",
                    "address": "192.0.2.3/24",
                    "vrf": None,
                    "status": "active",
                }
            )

        update_statements = [
            query["sql"]
            for query in queries
            if query["sql"].lstrip().upper().startswith("UPDATE ")
        ]
        self.assertEqual(update_statements, [])
        logger.log_warning.assert_not_called()

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

    def test_ensure_device_type_reuses_cache_after_first_resolution(self):
        runner = ForwardSyncRunner(
            sync=self.sync, ingestion=None, client=None, logger_=Mock()
        )
        row = {
            "manufacturer": "Cisco",
            "manufacturer_slug": "cisco",
            "model": "WS-C4507R-E",
            "slug": "ws-c4507r-e",
            "part_number": "WS-C4507R-E",
        }

        first = runner._ensure_device_type(row)
        with CaptureQueriesContext(connection) as queries:
            second = runner._ensure_device_type(row)

        self.assertEqual(first.pk, second.pk)
        self.assertEqual(len(queries), 2)

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

    def test_apply_device_ignores_virtual_chassis_without_position(self):
        runner = ForwardSyncRunner(
            sync=self.sync, ingestion=None, client=None, logger_=Mock()
        )

        runner._apply_dcim_device(
            {
                "name": "device-1",
                "manufacturer": "Cisco",
                "manufacturer_slug": "cisco",
                "device_type": "model-1",
                "device_type_slug": "model-1",
                "site": "site-1",
                "site_slug": "site-1",
                "role": "switch",
                "role_slug": "switch",
                "role_color": "9e9e9e",
                "status": "active",
                "virtual_chassis": "stale-vc",
            }
        )

        device = Device.objects.get(name="device-1")
        self.assertIsNone(device.virtual_chassis)
        self.assertIsNone(device.vc_position)
        self.assertEqual(
            runner._aggregated_skip_warning_counts[
                ("dcim.device", "virtual-chassis-without-position")
            ],
            1,
        )

    def test_record_issue_serializes_model_objects(self):
        ingestion = ForwardIngestion.objects.create(sync=self.sync)
        runner = ForwardSyncRunner(
            sync=self.sync,
            ingestion=ingestion,
            client=None,
            logger_=Mock(),
        )
        site = Site.objects.create(name="site-1", slug="site-1")

        issue = runner._record_issue(
            "netbox_routing.bgppeer",
            "routing failed",
            {"device": "device-1", "site": site},
            defaults={"router": site},
            context={"site": site},
        )

        self.assertEqual(
            issue.coalesce_fields["site"],
            {
                "model": "dcim.site",
                "pk": site.pk,
                "display": str(site),
            },
        )
        self.assertEqual(issue.defaults["router"]["model"], "dcim.site")
        self.assertEqual(issue.raw_data["site"]["pk"], site.pk)

    def test_record_issue_supports_info_log_level(self):
        ingestion = ForwardIngestion.objects.create(sync=self.sync)
        logger = Mock()
        runner = ForwardSyncRunner(
            sync=self.sync,
            ingestion=ingestion,
            client=None,
            logger_=logger,
        )

        issue = runner._record_issue(
            "ipam.ipaddress",
            "Skipping delete for dependency protected row.",
            {"address": "192.0.2.1/32"},
            log_level="info",
        )

        self.assertIsNotNone(issue)
        logger.log_info.assert_called_once()
        logger.log_warning.assert_not_called()
        logger.log_failure.assert_not_called()

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

    def test_run_warns_and_continues_when_snapshot_metrics_fail(self):
        ingestion = ForwardIngestion.objects.create(sync=self.sync)
        client = Mock()
        client.get_latest_processed_snapshot.return_value = {
            "id": "snapshot-before",
            "processedAt": "2026-03-31T12:15:00Z",
        }
        client.get_snapshot_metrics.side_effect = RuntimeError("metrics unavailable")
        client.run_nqe_query.return_value = []
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

        self.assertEqual(ingestion.snapshot_id, "snapshot-before")
        self.assertEqual(ingestion.snapshot_metrics, {})
        logger.log_warning.assert_any_call(
            "Unable to fetch Forward snapshot metrics for `snapshot-before`: metrics unavailable",
            obj=self.sync,
        )
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
            "forward_netbox.utilities.sync_execution.get_query_specs",
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
            "forward_netbox.utilities.sync_execution.get_query_specs",
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
        self.sync.incremental_diff_baseline = Mock(
            return_value=Mock(snapshot_id="snapshot-before")
        )

        with patch(
            "forward_netbox.utilities.sync_execution.get_query_specs",
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
            parameters={},
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
            "forward_netbox.utilities.sync_execution.get_query_specs",
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

    def test_run_warns_when_branching_has_no_newer_snapshot_for_diff(self):
        ForwardIngestion.objects.create(
            sync=self.sync,
            snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
            snapshot_id="snapshot-after",
            baseline_ready=True,
        )
        ingestion = ForwardIngestion.objects.create(sync=self.sync)
        client = Mock()
        client.get_latest_processed_snapshot.return_value = {
            "id": "snapshot-after",
            "processedAt": "2026-03-31T12:15:00Z",
        }
        client.get_snapshot_metrics.return_value = {}
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
        self.sync.latest_baseline_ingestion = Mock(
            return_value=Mock(pk=123, snapshot_id="snapshot-after")
        )
        self.sync.incremental_diff_baseline = Mock(return_value=None)

        with patch(
            "forward_netbox.utilities.sync_execution.get_query_specs",
            return_value=[
                QuerySpec(
                    model_string="dcim.site",
                    query_name="Forward Sites",
                    query_id="Q_sites",
                )
            ],
        ):
            runner.run()

        client.run_nqe_query.assert_called_once()
        self.assertFalse(client.run_nqe_diff.called)
        ingestion.refresh_from_db()
        self.assertEqual(ingestion.sync_mode, "full")
        self.assertTrue(
            any(
                "newer processed snapshot than the latest baseline" in str(call.args[0])
                for call in logger.log_info.call_args_list
            )
        )

    def test_fetch_spec_rows_partitions_large_column_filter_batches(self):
        client = Mock()
        logger = Mock()
        fetcher = ForwardQueryFetcher(
            sync=self.sync,
            client=client,
            logger_=logger,
        )
        context = ForwardQueryContext(
            network_id="test-network",
            snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
            snapshot_id="snapshot-before",
        )
        spec = QuerySpec(
            model_string="dcim.interface",
            query_name="Forward Interfaces",
            query="foreach interface select {device: interface.device.name}",
            parameters={"existing": "value"},
        )
        shard_scope = {
            "fetch_mode": "nqe_column_filter",
            "fetch_column_filters": [
                {
                    "operator": "EQUALS_ANY",
                    "columnName": "device",
                    "values": ["device-1", "device-2", "device-3"],
                }
            ],
            "shard_keys": [
                "device:device-1",
                "device:device-2",
                "device:device-3",
            ],
            "query_parameters": {},
        }
        client.run_nqe_query.side_effect = [
            [
                {"device": "device-1", "name": "Ethernet1/1"},
                {"device": "device-2", "name": "Ethernet1/1"},
            ],
            [{"device": "device-3", "name": "Ethernet1/1"}],
        ]

        with patch(
            "forward_netbox.utilities.query_fetch_execution.SHARD_FETCH_COLUMN_FILTER_CHUNK_SIZE",
            2,
        ):
            rows, delete_rows, mode = fetcher._fetch_spec_rows(
                "dcim.interface",
                spec,
                baseline=None,
                context=context,
                coalesce_fields=[["device", "name"]],
                shard_scope=shard_scope,
            )

        self.assertEqual(client.run_nqe_query.call_count, 2)
        self.assertEqual(
            client.run_nqe_query.call_args_list[0].kwargs["column_filters"][0][
                "values"
            ],
            ["device-1", "device-2"],
        )
        self.assertEqual(
            client.run_nqe_query.call_args_list[1].kwargs["column_filters"][0][
                "values"
            ],
            ["device-3"],
        )
        self.assertEqual(mode, "full")
        self.assertEqual(delete_rows, [])
        self.assertEqual(
            [row["device"] for row in rows],
            ["device-1", "device-2", "device-3"],
        )
        self.assertEqual(
            client.run_nqe_query.call_args.kwargs["parameters"],
            {"existing": "value"},
        )

    def test_fetch_spec_rows_filters_cable_device_pushdown_superset_to_shard(self):
        client = Mock()
        logger = Mock()
        fetcher = ForwardQueryFetcher(
            sync=self.sync,
            client=client,
            logger_=logger,
        )
        context = ForwardQueryContext(
            network_id="test-network",
            snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
            snapshot_id="snapshot-before",
        )
        spec = QuerySpec(
            model_string="dcim.cable",
            query_name="Forward Inferred Interface Cables",
            query="foreach link select {device: link.device}",
        )
        shard_keys = [
            "cable:device-a:Ethernet1/1|device-b:Ethernet1/2",
        ]
        shard_scope = {
            "shard_keys": shard_keys,
            **shard_fetch_contract("dcim.cable", shard_keys),
        }
        client.run_nqe_query.return_value = [
            {
                "device": "device-a",
                "interface": "Ethernet1/1",
                "remote_device": "device-b",
                "remote_interface": "Ethernet1/2",
                "status": "connected",
            },
            {
                "device": "device-a",
                "interface": "Ethernet9/9",
                "remote_device": "device-z",
                "remote_interface": "Ethernet9/10",
                "status": "connected",
            },
        ]

        rows, delete_rows, sync_mode, fetch_meta = fetcher._fetch_spec_rows(
            "dcim.cable",
            spec,
            baseline=None,
            context=context,
            coalesce_fields=[
                ["device", "interface", "remote_device", "remote_interface"]
            ],
            shard_scope=shard_scope,
            return_fetch_meta=True,
        )

        self.assertEqual(sync_mode, "full")
        self.assertEqual(delete_rows, [])
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["interface"], "Ethernet1/1")
        self.assertEqual(fetch_meta["fetch_mode"], "nqe_column_filter")
        self.assertEqual(
            client.run_nqe_query.call_args.kwargs["column_filters"],
            [
                {
                    "operator": "EQUALS_ANY",
                    "columnName": "device",
                    "values": ["device-a"],
                }
            ],
        )

    def test_fetch_spec_rows_passes_prefix_shard_keys_as_nqe_parameters(self):
        client = Mock()
        logger = Mock()
        fetcher = ForwardQueryFetcher(
            sync=self.sync,
            client=client,
            logger_=logger,
        )
        context = ForwardQueryContext(
            network_id="test-network",
            snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
            snapshot_id="snapshot-before",
        )
        spec = QuerySpec(
            model_string="ipam.prefix",
            query_name="Forward IPv6 Prefixes",
            query="@query f(forward_netbox_shard_keys: List<String>) = []",
            parameters={"forward_netbox_shard_keys": []},
        )
        shard_scope = {
            "shard_keys": [
                "prefix=2400:9500::/32",
                "prefix=2401:e800:7100::/40",
            ],
            **shard_fetch_contract(
                "ipam.prefix",
                [
                    "prefix=2400:9500::/32",
                    "prefix=2401:e800:7100::/40",
                ],
            ),
        }
        client.run_nqe_query.return_value = [
            {"prefix": "2400:9500::/32", "status": "active"},
            {"prefix": "2401:e800:7100::/40", "status": "active"},
        ]

        rows, delete_rows, sync_mode, fetch_meta = fetcher._fetch_spec_rows(
            "ipam.prefix",
            spec,
            baseline=None,
            context=context,
            coalesce_fields=[["prefix"]],
            shard_scope=shard_scope,
            return_fetch_meta=True,
        )

        self.assertEqual(sync_mode, "full")
        self.assertEqual(delete_rows, [])
        self.assertEqual(
            rows,
            [
                {"prefix": "2400:9500::/32", "status": "active"},
                {"prefix": "2401:e800:7100::/40", "status": "active"},
            ],
        )
        self.assertEqual(fetch_meta["fetch_mode"], "nqe_parameters")
        self.assertEqual(fetch_meta["fetch_column_filters"], [])
        client.run_nqe_query.assert_called_once()
        self.assertIsNone(client.run_nqe_query.call_args.kwargs["column_filters"])
        self.assertEqual(
            client.run_nqe_query.call_args.kwargs["parameters"],
            {
                "forward_netbox_shard_keys": [
                    "2400:9500::/32",
                    "2401:e800:7100::/40",
                ]
            },
        )

    def test_fetch_spec_rows_partitions_large_column_filter_diff_batches(self):
        baseline = Mock(snapshot_id="snapshot-before")
        client = Mock()
        logger = Mock()
        fetcher = ForwardQueryFetcher(
            sync=self.sync,
            client=client,
            logger_=logger,
        )
        context = ForwardQueryContext(
            network_id="test-network",
            snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
            snapshot_id="snapshot-after",
        )
        spec = QuerySpec(
            model_string="dcim.interface",
            query_name="Forward Interfaces",
            query_id="Q_interfaces",
            parameters={"existing": "value"},
        )
        shard_scope = {
            "fetch_mode": "nqe_column_filter",
            "fetch_column_filters": [
                {
                    "operator": "EQUALS_ANY",
                    "columnName": "device",
                    "values": ["device-1", "device-2", "device-3"],
                }
            ],
            "shard_keys": [
                "device:device-1",
                "device:device-2",
                "device:device-3",
            ],
            "query_parameters": {},
        }
        client.run_nqe_diff.side_effect = [
            [
                {
                    "type": "ADDED",
                    "before": None,
                    "after": {"device": "device-1", "name": "Ethernet1/1"},
                },
                {
                    "type": "ADDED",
                    "before": None,
                    "after": {"device": "device-2", "name": "Ethernet1/1"},
                },
            ],
            [
                {
                    "type": "ADDED",
                    "before": None,
                    "after": {"device": "device-3", "name": "Ethernet1/1"},
                }
            ],
        ]

        with patch(
            "forward_netbox.utilities.query_fetch_execution.SHARD_FETCH_COLUMN_FILTER_CHUNK_SIZE",
            2,
        ):
            rows, delete_rows, mode = fetcher._fetch_spec_rows(
                "dcim.interface",
                spec,
                baseline=baseline,
                context=context,
                coalesce_fields=[["device", "name"]],
                shard_scope=shard_scope,
            )

        self.assertEqual(client.run_nqe_diff.call_count, 2)
        self.assertEqual(mode, "diff")
        self.assertEqual(delete_rows, [])
        self.assertEqual(
            [row["device"] for row in rows],
            ["device-1", "device-2", "device-3"],
        )
        self.assertEqual(
            client.run_nqe_diff.call_args.kwargs["parameters"],
            {},
        )

    def test_fetch_spec_rows_recovers_failed_full_partition_by_splitting(self):
        fetcher = ForwardQueryFetcher(
            sync=self.sync,
            client=Mock(),
            logger_=Mock(),
        )
        spec = QuerySpec(
            model_string="dcim.interface",
            query_name="Forward Interfaces",
            query="foreach interface select {device: interface.device.name, name: interface.name}",
        )
        context = ForwardQueryContext(
            network_id="test-network",
            snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
            snapshot_id="snapshot-after",
        )
        shard_scope = {
            "fetch_mode": "nqe_column_filter",
            "fetch_key_family": "device",
            "fetch_parameters": {"shard_hint": "device"},
            "fetch_column_filters": [
                {
                    "operator": "EQUALS_ANY",
                    "columnName": "device",
                    "values": ["device-1", "device-2", "device-3"],
                }
            ],
            "query_parameters": {},
            "shard_keys": ["device:device-1", "device:device-2", "device:device-3"],
        }
        fetcher._run_nqe_query_with_parameter_fallback = Mock(
            side_effect=[
                ForwardClientError("partition timeout"),
                [{"device": "device-1", "name": "Ethernet1/1"}],
                [
                    {"device": "device-2", "name": "Ethernet1/1"},
                    {"device": "device-3", "name": "Ethernet1/1"},
                ],
            ]
        )

        rows, delete_rows, sync_mode, fetch_meta = fetcher._fetch_spec_rows(
            "dcim.interface",
            spec,
            baseline=None,
            context=context,
            coalesce_fields=[["device", "name"]],
            shard_scope=shard_scope,
            return_fetch_meta=True,
        )

        self.assertEqual(sync_mode, "full")
        self.assertEqual(delete_rows, [])
        self.assertEqual(fetch_meta["fetch_mode"], "nqe_column_filter")
        self.assertEqual(
            [row["device"] for row in rows],
            ["device-1", "device-2", "device-3"],
        )
        self.assertEqual(fetcher._run_nqe_query_with_parameter_fallback.call_count, 3)
        self.assertEqual(
            fetcher._run_nqe_query_with_parameter_fallback.call_args_list[0].kwargs[
                "column_filters"
            ][0]["values"],
            ["device-1", "device-2", "device-3"],
        )
        self.assertEqual(
            fetcher._run_nqe_query_with_parameter_fallback.call_args_list[1].kwargs[
                "column_filters"
            ][0]["values"],
            ["device-1"],
        )
        self.assertEqual(
            fetcher._run_nqe_query_with_parameter_fallback.call_args_list[2].kwargs[
                "column_filters"
            ][0]["values"],
            ["device-2", "device-3"],
        )
        self.assertTrue(fetcher.logger.log_warning.called)

    def test_fetch_spec_rows_does_not_split_non_retryable_http_400_partition(self):
        fetcher = ForwardQueryFetcher(
            sync=self.sync,
            client=Mock(),
            logger_=Mock(),
        )
        spec = QuerySpec(
            model_string="ipam.prefix",
            query_name="Forward Prefixes",
            query="foreach prefix select {prefix: prefix.network}",
        )
        context = ForwardQueryContext(
            network_id="test-network",
            snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
            snapshot_id="snapshot-after",
        )
        shard_scope = {
            "fetch_mode": "nqe_column_filter",
            "fetch_key_family": "prefix",
            "fetch_parameters": {"shard_hint": "prefix"},
            "fetch_column_filters": [
                {
                    "operator": "EQUALS_ANY",
                    "columnName": "prefix",
                    "values": ["prefix-1", "prefix-2", "prefix-3"],
                }
            ],
            "query_parameters": {},
            "shard_keys": [
                "prefix=prefix-1",
                "prefix=prefix-2",
                "prefix=prefix-3",
            ],
        }
        fetcher._run_nqe_query_with_parameter_fallback = Mock(
            side_effect=[
                ForwardClientError(
                    "Forward API request failed with HTTP 400: bad request"
                ),
                [{"prefix": "prefix-1"}],
            ]
        )

        rows, delete_rows, sync_mode, fetch_meta = fetcher._fetch_spec_rows(
            "ipam.prefix",
            spec,
            baseline=None,
            context=context,
            coalesce_fields=[["prefix"]],
            shard_scope=shard_scope,
            return_fetch_meta=True,
        )

        self.assertEqual(sync_mode, "full")
        self.assertEqual(delete_rows, [])
        self.assertEqual(rows, [{"prefix": "prefix-1"}])
        self.assertEqual(fetch_meta["fetch_mode"], "full_fallback")
        self.assertEqual(fetcher._run_nqe_query_with_parameter_fallback.call_count, 2)
        self.assertEqual(
            fetcher._run_nqe_query_with_parameter_fallback.call_args_list[0].kwargs[
                "column_filters"
            ][0]["values"],
            ["prefix-1", "prefix-2", "prefix-3"],
        )
        self.assertIsNone(
            fetcher._run_nqe_query_with_parameter_fallback.call_args_list[1].kwargs[
                "column_filters"
            ]
        )
        self.assertEqual(
            fetch_meta["fetch_parameters"]["partition_retry_summary"],
            {
                "operation": "full",
                "partition_count": 1,
                "split_retry_count": 0,
                "split_retry_success_count": 0,
                "alternate_operator_retry_count": 0,
                "alternate_operator_success_count": 0,
                "non_retryable_partition_failure_count": 1,
            },
        )

    def test_fetch_spec_rows_splits_value_required_http_400_partition(self):
        fetcher = ForwardQueryFetcher(
            sync=self.sync,
            client=Mock(),
            logger_=Mock(),
        )
        spec = QuerySpec(
            model_string="dcim.interface",
            query_name="Forward Interfaces",
            query="foreach interface select {device: interface.device.name, name: interface.name}",
        )
        context = ForwardQueryContext(
            network_id="test-network",
            snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
            snapshot_id="snapshot-after",
        )
        shard_scope = {
            "fetch_mode": "nqe_column_filter",
            "fetch_key_family": "device",
            "fetch_parameters": {"shard_hint": "device"},
            "fetch_column_filters": [
                {
                    "operator": "EQUALS_ANY",
                    "columnName": "device",
                    "values": ["device-1", "device-2", "device-3"],
                }
            ],
            "query_parameters": {},
            "shard_keys": ["device:device-1", "device:device-2", "device:device-3"],
        }

        def _query_side_effect(*, column_filters, **kwargs):
            filter_item = dict((column_filters or [{}])[0])
            operator = str(filter_item.get("operator") or "")
            if operator == "EQUALS_ANY":
                raise ForwardClientError(
                    "Forward API request failed with HTTP 400: {'value' is required}"
                )
            if operator == "DEFAULT":
                value = str(filter_item.get("value") or "")
                return [{"device": value, "name": "Ethernet1/1"}]
            raise AssertionError(f"Unexpected column filter shape: {filter_item!r}")

        fetcher._run_nqe_query_with_parameter_fallback = Mock(
            side_effect=_query_side_effect
        )

        rows, delete_rows, sync_mode, fetch_meta = fetcher._fetch_spec_rows(
            "dcim.interface",
            spec,
            baseline=None,
            context=context,
            coalesce_fields=[["device", "name"]],
            shard_scope=shard_scope,
            return_fetch_meta=True,
        )

        self.assertEqual(sync_mode, "full")
        self.assertEqual(delete_rows, [])
        self.assertEqual(fetch_meta["fetch_mode"], "nqe_column_filter")
        self.assertEqual(
            [row["device"] for row in rows],
            ["device-1", "device-2", "device-3"],
        )
        self.assertEqual(fetcher._run_nqe_query_with_parameter_fallback.call_count, 4)
        first_filter = fetcher._run_nqe_query_with_parameter_fallback.call_args_list[
            0
        ].kwargs["column_filters"][0]
        self.assertEqual(first_filter["operator"], "EQUALS_ANY")
        self.assertEqual(first_filter["values"], ["device-1", "device-2", "device-3"])
        self.assertEqual(
            [
                call.kwargs["column_filters"][0]["value"]
                for call in fetcher._run_nqe_query_with_parameter_fallback.call_args_list[
                    1:
                ]
            ],
            ["device-1", "device-2", "device-3"],
        )
        self.assertEqual(
            fetch_meta["fetch_parameters"]["partition_retry_summary"],
            {
                "operation": "full",
                "partition_count": 1,
                "split_retry_count": 0,
                "split_retry_success_count": 0,
                "alternate_operator_retry_count": 3,
                "alternate_operator_success_count": 1,
            },
        )

    def test_fetch_spec_rows_logs_value_required_full_fallback_as_info(self):
        logger = Mock()
        fetcher = ForwardQueryFetcher(
            sync=self.sync,
            client=Mock(),
            logger_=logger,
        )
        spec = QuerySpec(
            model_string="dcim.interface",
            query_name="Forward Interfaces",
            query="foreach interface select {device: interface.device.name, name: interface.name}",
        )
        context = ForwardQueryContext(
            network_id="test-network",
            snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
            snapshot_id="snapshot-after",
        )
        shard_scope = {
            "fetch_mode": "nqe_column_filter",
            "fetch_key_family": "device",
            "fetch_parameters": {"shard_hint": "device"},
            "fetch_column_filters": [
                {"operator": "EQUALS_ANY", "columnName": "device", "values": ["core-1"]}
            ],
            "query_parameters": {},
            "shard_keys": ["device:core-1"],
        }
        fetcher._run_nqe_query_with_parameter_fallback = Mock(
            side_effect=[
                ForwardClientError(
                    "Forward API request failed with HTTP 400: {'value' is required}"
                ),
                ForwardClientError(
                    "Forward API request failed with HTTP 400: {'value' is required}"
                ),
                [{"device": "core-1", "name": "Ethernet1/1"}],
            ]
        )

        rows, delete_rows, sync_mode, fetch_meta = fetcher._fetch_spec_rows(
            "dcim.interface",
            spec,
            baseline=None,
            context=context,
            coalesce_fields=[["device", "name"]],
            shard_scope=shard_scope,
            return_fetch_meta=True,
        )

        self.assertEqual(sync_mode, "full")
        self.assertEqual(delete_rows, [])
        self.assertEqual(rows, [{"device": "core-1", "name": "Ethernet1/1"}])
        self.assertEqual(fetch_meta["fetch_mode"], "full_fallback")

        info_messages = [call.args[0] for call in logger.log_info.call_args_list]
        warning_messages = [call.args[0] for call in logger.log_warning.call_args_list]
        self.assertTrue(
            any(
                "falling back to full model fetch" in message
                for message in info_messages
            )
        )
        self.assertFalse(
            any(
                "falling back to full model fetch" in message
                for message in warning_messages
            )
        )

    def test_fetch_spec_rows_logs_connectivity_fallback_as_info(self):
        logger = Mock()
        fetcher = ForwardQueryFetcher(
            sync=self.sync,
            client=Mock(),
            logger_=logger,
        )
        spec = QuerySpec(
            model_string="dcim.interface",
            query_name="Forward Interfaces",
            query="foreach interface select {device: interface.device.name, name: interface.name}",
        )
        context = ForwardQueryContext(
            network_id="test-network",
            snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
            snapshot_id="snapshot-after",
        )
        shard_scope = {
            "fetch_mode": "nqe_column_filter",
            "fetch_key_family": "device",
            "fetch_parameters": {"shard_hint": "device"},
            "fetch_column_filters": [
                {"operator": "REGEX", "columnName": "device", "value": "core-1"}
            ],
            "query_parameters": {},
            "shard_keys": ["device:core-1"],
        }
        fetcher._run_nqe_query_with_parameter_fallback = Mock(
            side_effect=[
                ForwardConnectivityError(
                    "Forward API request returned transient HTTP 503; retry attempts were exhausted."
                ),
                [{"device": "core-1", "name": "Ethernet1/1"}],
            ]
        )

        rows, delete_rows, sync_mode, fetch_meta = fetcher._fetch_spec_rows(
            "dcim.interface",
            spec,
            baseline=None,
            context=context,
            coalesce_fields=[["device", "name"]],
            shard_scope=shard_scope,
            return_fetch_meta=True,
        )

        self.assertEqual(sync_mode, "full")
        self.assertEqual(delete_rows, [])
        self.assertEqual(rows, [{"device": "core-1", "name": "Ethernet1/1"}])
        self.assertEqual(fetch_meta["fetch_mode"], "full_fallback")

        info_messages = [call.args[0] for call in logger.log_info.call_args_list]
        warning_messages = [call.args[0] for call in logger.log_warning.call_args_list]
        self.assertTrue(
            any(
                "falling back to full model fetch" in message
                for message in info_messages
            )
        )
        self.assertFalse(
            any(
                "falling back to full model fetch" in message
                for message in warning_messages
            )
        )

    def test_fetch_spec_rows_logs_server_error_fallback_as_info(self):
        logger = Mock()
        fetcher = ForwardQueryFetcher(
            sync=self.sync,
            client=Mock(),
            logger_=logger,
        )
        spec = QuerySpec(
            model_string="dcim.interface",
            query_name="Forward Interfaces",
            query="foreach interface select {device: interface.device.name, name: interface.name}",
        )
        context = ForwardQueryContext(
            network_id="test-network",
            snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
            snapshot_id="snapshot-after",
        )
        shard_scope = {
            "fetch_mode": "nqe_column_filter",
            "fetch_key_family": "device",
            "fetch_parameters": {"shard_hint": "device"},
            "fetch_column_filters": [
                {"operator": "REGEX", "columnName": "device", "value": "core-1"}
            ],
            "query_parameters": {},
            "shard_keys": ["device:core-1"],
        }
        fetcher._run_nqe_query_with_parameter_fallback = Mock(
            side_effect=[
                ForwardClientError(
                    'Forward API request failed with HTTP 500: {"message":"db fail","status":"500"}'
                ),
                [{"device": "core-1", "name": "Ethernet1/1"}],
            ]
        )

        rows, delete_rows, sync_mode, fetch_meta = fetcher._fetch_spec_rows(
            "dcim.interface",
            spec,
            baseline=None,
            context=context,
            coalesce_fields=[["device", "name"]],
            shard_scope=shard_scope,
            return_fetch_meta=True,
        )

        self.assertEqual(sync_mode, "full")
        self.assertEqual(delete_rows, [])
        self.assertEqual(rows, [{"device": "core-1", "name": "Ethernet1/1"}])
        self.assertEqual(fetch_meta["fetch_mode"], "full_fallback")

        info_messages = [call.args[0] for call in logger.log_info.call_args_list]
        warning_messages = [call.args[0] for call in logger.log_warning.call_args_list]
        self.assertTrue(
            any(
                "falling back to full model fetch" in message
                for message in info_messages
            )
        )
        self.assertFalse(
            any(
                "falling back to full model fetch" in message
                for message in warning_messages
            )
        )

    def test_fetch_spec_rows_value_required_partition_retries_do_not_warn(self):
        logger = Mock()
        fetcher = ForwardQueryFetcher(
            sync=self.sync,
            client=Mock(),
            logger_=logger,
        )
        spec = QuerySpec(
            model_string="dcim.interface",
            query_name="Forward Interfaces",
            query="foreach interface select {device: interface.device.name, name: interface.name}",
        )
        context = ForwardQueryContext(
            network_id="test-network",
            snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
            snapshot_id="snapshot-after",
        )
        shard_scope = {
            "fetch_mode": "nqe_column_filter",
            "fetch_key_family": "device",
            "fetch_parameters": {"shard_hint": "device"},
            "fetch_column_filters": [
                {
                    "operator": "EQUALS_ANY",
                    "columnName": "device",
                    "values": ["device-1", "device-2", "device-3", "device-4"],
                }
            ],
            "query_parameters": {},
            "shard_keys": [
                "device:device-1",
                "device:device-2",
                "device:device-3",
                "device:device-4",
            ],
        }

        def _query_side_effect(*, column_filters, **kwargs):
            filter_item = dict((column_filters or [{}])[0])
            operator = str(filter_item.get("operator") or "")
            values = list(filter_item.get("values") or [])
            if operator == "EQUALS_ANY" and len(values) > 1:
                raise ForwardClientError(
                    "Forward API request failed with HTTP 400: {'value' is required}"
                )
            if operator == "EQUALS_ANY" and len(values) == 1:
                raise ForwardClientError(
                    "Forward API request failed with HTTP 400: {'value' is required}"
                )
            if operator == "DEFAULT":
                value = str(filter_item.get("value") or "")
                return [{"device": value, "name": "Ethernet1/1"}]
            raise AssertionError(f"Unexpected column filter shape: {filter_item!r}")

        fetcher._run_nqe_query_with_parameter_fallback = Mock(
            side_effect=_query_side_effect
        )

        rows, delete_rows, sync_mode, fetch_meta = fetcher._fetch_spec_rows(
            "dcim.interface",
            spec,
            baseline=None,
            context=context,
            coalesce_fields=[["device", "name"]],
            shard_scope=shard_scope,
            return_fetch_meta=True,
        )

        self.assertEqual(sync_mode, "full")
        self.assertEqual(delete_rows, [])
        self.assertEqual(fetch_meta["fetch_mode"], "nqe_column_filter")
        self.assertEqual(
            sorted(row["device"] for row in rows),
            ["device-1", "device-2", "device-3", "device-4"],
        )

        warning_messages = [call.args[0] for call in logger.log_warning.call_args_list]
        self.assertFalse(
            any("partition fetch failed" in message for message in warning_messages)
        )
        self.assertFalse(
            any(
                "single-value partition fetch failed" in message
                for message in warning_messages
            )
        )

        info_messages = [call.args[0] for call in logger.log_info.call_args_list]
        self.assertTrue(
            any(
                "alternate single-value operator" in message
                for message in info_messages
            )
        )
        self.assertFalse(
            any("smaller split partitions" in message for message in info_messages)
        )

    def test_fetch_spec_rows_connectivity_partition_retries_do_not_warn(self):
        logger = Mock()
        fetcher = ForwardQueryFetcher(
            sync=self.sync,
            client=Mock(),
            logger_=logger,
        )
        spec = QuerySpec(
            model_string="dcim.interface",
            query_name="Forward Interfaces",
            query="foreach interface select {device: interface.device.name, name: interface.name}",
        )
        context = ForwardQueryContext(
            network_id="test-network",
            snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
            snapshot_id="snapshot-after",
        )
        shard_scope = {
            "fetch_mode": "nqe_column_filter",
            "fetch_key_family": "device",
            "fetch_parameters": {"shard_hint": "device"},
            "fetch_column_filters": [
                {
                    "operator": "EQUALS_ANY",
                    "columnName": "device",
                    "values": ["device-1", "device-2", "device-3", "device-4"],
                }
            ],
            "query_parameters": {},
            "shard_keys": [
                "device:device-1",
                "device:device-2",
                "device:device-3",
                "device:device-4",
            ],
        }

        def _query_side_effect(*, column_filters, **kwargs):
            filter_item = dict((column_filters or [{}])[0])
            operator = str(filter_item.get("operator") or "")
            values = list(filter_item.get("values") or [])
            if operator == "EQUALS_ANY" and len(values) > 1:
                raise ForwardConnectivityError(
                    "Forward API request returned transient HTTP 503; retry attempts were exhausted."
                )
            if operator == "EQUALS_ANY" and len(values) == 1:
                raise ForwardConnectivityError(
                    "Forward API request returned transient HTTP 504; retry attempts were exhausted."
                )
            if operator == "DEFAULT":
                value = str(filter_item.get("value") or "")
                return [{"device": value, "name": "Ethernet1/1"}]
            raise AssertionError(f"Unexpected column filter shape: {filter_item!r}")

        fetcher._run_nqe_query_with_parameter_fallback = Mock(
            side_effect=_query_side_effect
        )

        rows, delete_rows, sync_mode, fetch_meta = fetcher._fetch_spec_rows(
            "dcim.interface",
            spec,
            baseline=None,
            context=context,
            coalesce_fields=[["device", "name"]],
            shard_scope=shard_scope,
            return_fetch_meta=True,
        )

        self.assertEqual(sync_mode, "full")
        self.assertEqual(delete_rows, [])
        self.assertEqual(fetch_meta["fetch_mode"], "nqe_column_filter")
        self.assertEqual(
            sorted(row["device"] for row in rows),
            ["device-1", "device-2", "device-3", "device-4"],
        )

        warning_messages = [call.args[0] for call in logger.log_warning.call_args_list]
        self.assertFalse(
            any("partition fetch failed" in message for message in warning_messages)
        )
        self.assertFalse(
            any(
                "single-value partition fetch failed" in message
                for message in warning_messages
            )
        )

        info_messages = [call.args[0] for call in logger.log_info.call_args_list]
        self.assertTrue(
            any(
                "single-value partition fetch failed; retrying with alternate column-filter operator"
                in message
                for message in info_messages
            )
        )
        self.assertTrue(
            any(
                "partition fetch failed; retrying as 2 smaller partition(s)" in message
                for message in info_messages
            )
        )

    def test_fetch_spec_rows_recovers_failed_diff_partition_by_splitting(self):
        baseline = Mock(snapshot_id="snapshot-before")
        fetcher = ForwardQueryFetcher(
            sync=self.sync,
            client=Mock(),
            logger_=Mock(),
        )
        spec = QuerySpec(
            model_string="dcim.interface",
            query_name="Forward Interfaces",
            query_id="Q_interfaces",
        )
        context = ForwardQueryContext(
            network_id="test-network",
            snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
            snapshot_id="snapshot-after",
        )
        shard_scope = {
            "fetch_mode": "nqe_column_filter",
            "fetch_key_family": "device",
            "fetch_parameters": {"shard_hint": "device"},
            "fetch_column_filters": [
                {
                    "operator": "EQUALS_ANY",
                    "columnName": "device",
                    "values": ["device-1", "device-2", "device-3"],
                }
            ],
            "query_parameters": {},
            "shard_keys": ["device:device-1", "device:device-2", "device:device-3"],
        }
        fetcher._run_nqe_diff_without_parameters = Mock(
            side_effect=[
                ForwardClientError("partition timeout"),
                [
                    {
                        "type": "ADDED",
                        "before": None,
                        "after": {"device": "device-1", "name": "Ethernet1/1"},
                    }
                ],
                [
                    {
                        "type": "ADDED",
                        "before": None,
                        "after": {"device": "device-2", "name": "Ethernet1/1"},
                    },
                    {
                        "type": "ADDED",
                        "before": None,
                        "after": {"device": "device-3", "name": "Ethernet1/1"},
                    },
                ],
            ]
        )

        rows, delete_rows, sync_mode, fetch_meta = fetcher._fetch_spec_rows(
            "dcim.interface",
            spec,
            baseline=baseline,
            context=context,
            coalesce_fields=[["device", "name"]],
            shard_scope=shard_scope,
            return_fetch_meta=True,
        )

        self.assertEqual(sync_mode, "diff")
        self.assertEqual(delete_rows, [])
        self.assertEqual(fetch_meta["fetch_mode"], "nqe_column_filter")
        self.assertEqual(
            [row["device"] for row in rows],
            ["device-1", "device-2", "device-3"],
        )
        self.assertEqual(fetcher._run_nqe_diff_without_parameters.call_count, 3)
        self.assertTrue(fetcher.logger.log_warning.called)

    def test_fetch_spec_rows_retries_single_default_filter_as_equals_any_before_fallback(
        self,
    ):
        fetcher = ForwardQueryFetcher(
            sync=self.sync,
            client=Mock(),
            logger_=Mock(),
        )
        spec = QuerySpec(
            model_string="dcim.interface",
            query_name="Forward Interfaces",
            query="foreach interface select {device: interface.device.name, name: interface.name}",
        )
        context = ForwardQueryContext(
            network_id="test-network",
            snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
            snapshot_id="snapshot-after",
        )
        shard_scope = {
            "fetch_mode": "nqe_column_filter",
            "fetch_key_family": "device",
            "fetch_parameters": {"shard_hint": "device"},
            "fetch_column_filters": [
                {"operator": "DEFAULT", "columnName": "device", "value": "core-1"}
            ],
            "query_parameters": {},
            "shard_keys": ["device:core-1"],
        }
        fetcher._run_nqe_query_with_parameter_fallback = Mock(
            side_effect=[
                ForwardClientError(
                    "default column filter rejected: networkId="
                    + "123456 snapshotId="
                    + "654321 queryId=Q"
                    + "99999 user=operator@example.com"
                ),
                [{"device": "core-1", "name": "Ethernet1/1"}],
            ]
        )

        rows, delete_rows, sync_mode, fetch_meta = fetcher._fetch_spec_rows(
            "dcim.interface",
            spec,
            baseline=None,
            context=context,
            coalesce_fields=[["device", "name"]],
            shard_scope=shard_scope,
            return_fetch_meta=True,
        )

        self.assertEqual(sync_mode, "full")
        self.assertEqual(delete_rows, [])
        self.assertEqual(fetch_meta["fetch_mode"], "nqe_column_filter")
        self.assertEqual(
            [row["device"] for row in rows],
            ["core-1"],
        )
        self.assertEqual(
            fetch_meta["fetch_parameters"]["partition_retry_summary"],
            {
                "operation": "full",
                "partition_count": 1,
                "split_retry_count": 0,
                "split_retry_success_count": 0,
                "alternate_operator_retry_count": 1,
                "alternate_operator_success_count": 1,
            },
        )
        self.assertEqual(fetcher._run_nqe_query_with_parameter_fallback.call_count, 2)
        self.assertEqual(
            fetcher._run_nqe_query_with_parameter_fallback.call_args_list[1].kwargs[
                "column_filters"
            ],
            [{"operator": "EQUALS_ANY", "columnName": "device", "values": ["core-1"]}],
        )
        self.assertTrue(fetcher.logger.log_warning.called)
        warning_message = fetcher.logger.log_warning.call_args.args[0]
        self.assertIn("networkId=<redacted>", warning_message)
        self.assertIn("snapshotId=<redacted>", warning_message)
        self.assertIn("queryId=<redacted>", warning_message)
        self.assertIn("user=<redacted-email>", warning_message)
        self.assertNotIn("123456", warning_message)
        self.assertNotIn("654321", warning_message)
        self.assertNotIn("99999", warning_message)
        self.assertNotIn("operator@example.com", warning_message)

    def test_fetch_spec_rows_retries_single_diff_filter_before_full_fallback(self):
        baseline = Mock(snapshot_id="snapshot-before")
        fetcher = ForwardQueryFetcher(
            sync=self.sync,
            client=Mock(),
            logger_=Mock(),
        )
        spec = QuerySpec(
            model_string="dcim.interface",
            query_name="Forward Interfaces",
            query_id="Q_interfaces",
        )
        context = ForwardQueryContext(
            network_id="test-network",
            snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
            snapshot_id="snapshot-after",
        )
        shard_scope = {
            "fetch_mode": "nqe_column_filter",
            "fetch_key_family": "device",
            "fetch_parameters": {"shard_hint": "device"},
            "fetch_column_filters": [
                {"operator": "DEFAULT", "columnName": "device", "value": "core-1"}
            ],
            "query_parameters": {},
            "shard_keys": ["device:core-1"],
        }
        fetcher._run_nqe_diff_without_parameters = Mock(
            side_effect=[
                ForwardClientError("default diff column filter rejected"),
                [
                    {
                        "type": "ADDED",
                        "before": None,
                        "after": {"device": "core-1", "name": "Ethernet1/1"},
                    }
                ],
            ]
        )
        fetcher._run_nqe_query_with_parameter_fallback = Mock()

        rows, delete_rows, sync_mode, fetch_meta = fetcher._fetch_spec_rows(
            "dcim.interface",
            spec,
            baseline=baseline,
            context=context,
            coalesce_fields=[["device", "name"]],
            shard_scope=shard_scope,
            return_fetch_meta=True,
        )

        self.assertEqual(sync_mode, "diff")
        self.assertEqual(delete_rows, [])
        self.assertEqual(fetch_meta["fetch_mode"], "nqe_column_filter")
        self.assertEqual(
            [row["device"] for row in rows],
            ["core-1"],
        )
        self.assertEqual(
            fetch_meta["fetch_parameters"]["partition_retry_summary"],
            {
                "operation": "diff",
                "partition_count": 1,
                "split_retry_count": 0,
                "split_retry_success_count": 0,
                "alternate_operator_retry_count": 1,
                "alternate_operator_success_count": 1,
            },
        )
        self.assertEqual(fetcher._run_nqe_diff_without_parameters.call_count, 2)
        self.assertEqual(
            fetcher._run_nqe_diff_without_parameters.call_args_list[1].kwargs[
                "column_filters"
            ],
            [{"operator": "EQUALS_ANY", "columnName": "device", "values": ["core-1"]}],
        )
        self.assertFalse(fetcher._run_nqe_query_with_parameter_fallback.called)
        self.assertTrue(fetcher.logger.log_warning.called)

    def test_fetch_spec_rows_requires_diff_and_rejects_raw_query_fallback(self):
        self.sync.parameters["diff_fallback_mode"] = (
            ForwardDiffFallbackModeChoices.REQUIRE_DIFF
        )
        fetcher = ForwardQueryFetcher(
            sync=self.sync,
            client=Mock(),
            logger_=Mock(),
        )
        spec = QuerySpec(
            model_string="dcim.interface",
            query_name="Forward Interfaces",
            query="foreach interface select {device: interface.device.name, name: interface.name}",
        )
        context = ForwardQueryContext(
            network_id="test-network",
            snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
            snapshot_id="snapshot-after",
        )
        fetcher._run_nqe_query_with_parameter_fallback = Mock()

        with self.assertRaisesRegex(
            ForwardQueryError,
            "Diff execution is required, but `Forward Interfaces` for dcim.interface has no query_id",
        ):
            fetcher._fetch_spec_rows(
                "dcim.interface",
                spec,
                baseline=Mock(snapshot_id="snapshot-before"),
                context=context,
                coalesce_fields=[["device", "name"]],
            )

        self.assertFalse(fetcher._run_nqe_query_with_parameter_fallback.called)

    def test_fetch_spec_rows_requires_diff_and_rejects_diff_error_fallback(self):
        self.sync.parameters["diff_fallback_mode"] = (
            ForwardDiffFallbackModeChoices.REQUIRE_DIFF
        )
        fetcher = ForwardQueryFetcher(
            sync=self.sync,
            client=Mock(),
            logger_=Mock(),
        )
        spec = QuerySpec(
            model_string="dcim.interface",
            query_name="Forward Interfaces",
            query_id="Q_interfaces",
        )
        context = ForwardQueryContext(
            network_id="test-network",
            snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
            snapshot_id="snapshot-after",
        )
        fetcher._run_nqe_diff_without_parameters = Mock(
            side_effect=ForwardClientError("diff timeout")
        )
        fetcher._run_nqe_query_with_parameter_fallback = Mock()

        with self.assertRaisesRegex(
            ForwardQueryError,
            "Diff execution is required and Forward NQE diff failed for dcim.interface",
        ):
            fetcher._fetch_spec_rows(
                "dcim.interface",
                spec,
                baseline=Mock(snapshot_id="snapshot-before"),
                context=context,
                coalesce_fields=[["device", "name"]],
            )

        self.assertEqual(fetcher._run_nqe_diff_without_parameters.call_count, 1)
        self.assertFalse(fetcher._run_nqe_query_with_parameter_fallback.called)

    def test_fetch_spec_rows_requires_diff_and_rejects_prune_full_query_path(self):
        self.sync.parameters["diff_fallback_mode"] = (
            ForwardDiffFallbackModeChoices.REQUIRE_DIFF
        )
        fetcher = ForwardQueryFetcher(
            sync=self.sync,
            client=Mock(),
            logger_=Mock(),
        )
        spec = QuerySpec(
            model_string="dcim.interface",
            query_name="Forward Interfaces",
            query_id="Q_interfaces",
        )
        context = ForwardQueryContext(
            network_id="test-network",
            snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
            snapshot_id="snapshot-after",
            device_tag_prune_out_of_scope=True,
            scoped_device_names={"device-1"},
        )

        with self.assertRaisesRegex(
            ForwardQueryError,
            "prune-out-of-scope requires full query execution",
        ):
            fetcher._fetch_spec_rows(
                "dcim.interface",
                spec,
                baseline=Mock(snapshot_id="snapshot-before"),
                context=context,
                coalesce_fields=[["device", "name"]],
            )

    def test_resolve_context_carries_ingestion_id_from_branch_state(self):
        client = Mock()
        client.get_snapshot_metrics.return_value = {}
        client.get_snapshots.return_value = []
        client.get_latest_processed_snapshot.return_value = {
            "id": "snapshot-after",
            "processedAt": "2026-03-31T12:15:00Z",
        }
        fetcher = ForwardQueryFetcher(
            sync=self.sync,
            client=client,
            logger_=Mock(),
        )
        self.sync.get_network_id = Mock(return_value="test-network")
        self.sync.get_snapshot_id = Mock(return_value=LATEST_PROCESSED_SNAPSHOT)
        self.sync.resolve_snapshot_id = Mock(return_value="snapshot-after")
        self.sync.get_query_parameters = Mock(return_value={})
        self.sync.get_maps = Mock(return_value=[])

        context = fetcher.resolve_context(
            branch_run_state={
                "pending_ingestion_id": 42,
                "snapshot_selector": LATEST_PROCESSED_SNAPSHOT,
                "snapshot_id": "snapshot-after",
            }
        )

        self.assertEqual(context.ingestion_id, 42)
        self.assertEqual(context.as_dict()["ingestion_id"], 42)

    def test_resolve_context_prefers_ledger_ingestion_id_over_stale_branch_state(self):
        client = Mock()
        client.get_snapshot_metrics.return_value = {}
        client.get_snapshots.return_value = []
        client.get_latest_processed_snapshot.return_value = {
            "id": "snapshot-after",
            "processedAt": "2026-03-31T12:15:00Z",
        }
        fetcher = ForwardQueryFetcher(
            sync=self.sync,
            client=client,
            logger_=Mock(),
        )
        self.sync.get_network_id = Mock(return_value="test-network")
        self.sync.get_snapshot_id = Mock(return_value=LATEST_PROCESSED_SNAPSHOT)
        self.sync.resolve_snapshot_id = Mock(return_value="snapshot-after")
        self.sync.get_query_parameters = Mock(return_value={})
        self.sync.get_maps = Mock(return_value=[])
        run = ForwardExecutionRun.objects.create(
            sync=self.sync,
            source=self.source,
            backend="branching",
            status="running",
            snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
            snapshot_id="snapshot-after",
            total_steps=2,
            next_step_index=2,
        )
        ingestion = ForwardIngestion.objects.create(
            sync=self.sync,
            snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
            snapshot_id="snapshot-after",
        )
        ForwardExecutionStep.objects.create(
            run=run,
            index=2,
            kind="stage",
            status=ForwardExecutionStepStatusChoices.RUNNING,
            model_string="dcim.device",
            ingestion=ingestion,
        )

        context = fetcher.resolve_context(
            branch_run_state={
                "pending_ingestion_id": 42,
                "snapshot_selector": LATEST_PROCESSED_SNAPSHOT,
                "snapshot_id": "snapshot-after",
            }
        )

        self.assertEqual(context.ingestion_id, ingestion.pk)
        self.assertEqual(context.as_dict()["ingestion_id"], ingestion.pk)

    def test_resolve_context_applies_source_device_tag_scope(self):
        self.source.parameters["device_tag_include_tags"] = ["DATACENTER", "CORE"]
        self.source.parameters["device_tag_include_match"] = "any"
        self.source.parameters["device_tag_exclude_tags"] = ["BRANCH"]
        self.source.save(update_fields=["parameters"])
        client = Mock()
        client.get_snapshot_metrics.return_value = {}
        client.get_snapshots.return_value = []
        client.get_latest_processed_snapshot.return_value = {
            "id": "snapshot-after",
            "processedAt": "2026-03-31T12:15:00Z",
        }
        client.run_nqe_query.return_value = [{"name": "core-1"}, {"name": "core-2"}]
        fetcher = ForwardQueryFetcher(
            sync=self.sync,
            client=client,
            logger_=Mock(),
        )
        self.sync.get_network_id = Mock(return_value="test-network")
        self.sync.get_snapshot_id = Mock(return_value=LATEST_PROCESSED_SNAPSHOT)
        self.sync.resolve_snapshot_id = Mock(return_value="snapshot-after")
        self.sync.get_query_parameters = Mock(return_value={})
        self.sync.get_maps = Mock(return_value=[])

        context = fetcher.resolve_context()

        self.assertEqual(context.device_tag_include_tags, ["DATACENTER", "CORE"])
        self.assertEqual(context.device_tag_include_match, "any")
        self.assertEqual(context.device_tag_exclude_tags, ["BRANCH"])
        self.assertEqual(context.scoped_device_names, {"core-1", "core-2"})

    def test_resolve_context_reuses_run_local_context_artifact(self):
        self.source.parameters["device_tag_include_tags"] = ["DATACENTER"]
        self.source.parameters["device_tag_exclude_tags"] = ["BRANCH"]
        self.source.save(update_fields=["parameters"])
        with tempfile.TemporaryDirectory() as artifact_dir, patch.dict(
            os.environ,
            {"FORWARD_NETBOX_FETCH_ARTIFACT_DIR": artifact_dir},
        ):
            ForwardExecutionRun.objects.create(
                sync=self.sync,
                source=self.source,
                backend="branching",
                status="running",
                snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
                snapshot_id="snapshot-after",
                total_steps=2,
                next_step_index=1,
            )
            client = Mock()
            client.get_snapshot_metrics.return_value = {"deviceCount": 2}
            client.get_snapshots.return_value = [
                {
                    "id": "snapshot-after",
                    "state": "processed",
                    "created_at": "2026-03-31T10:00:00Z",
                    "processed_at": "2026-03-31T12:00:00Z",
                }
            ]
            client.get_latest_processed_snapshot.return_value = {
                "id": "snapshot-after",
                "processedAt": "2026-03-31T12:15:00Z",
            }
            client.run_nqe_query.return_value = [{"name": "core-1"}]
            fetcher = ForwardQueryFetcher(
                sync=self.sync,
                client=client,
                logger_=Mock(),
            )
            self.sync.get_network_id = Mock(return_value="test-network")
            self.sync.get_snapshot_id = Mock(return_value=LATEST_PROCESSED_SNAPSHOT)
            self.sync.resolve_snapshot_id = Mock(return_value="snapshot-after")
            self.sync.get_query_parameters = Mock(return_value={})
            self.sync.get_maps = Mock(return_value=[])
            branch_run_state = {
                "snapshot_selector": LATEST_PROCESSED_SNAPSHOT,
                "snapshot_id": "snapshot-after",
            }

            first = fetcher.resolve_context(branch_run_state=branch_run_state)
            second = fetcher.resolve_context(branch_run_state=branch_run_state)

            self.assertEqual(first.snapshot_metrics, {"deviceCount": 2})
            self.assertEqual(second.snapshot_metrics, {"deviceCount": 2})
            self.assertEqual(first.scoped_device_names, {"core-1"})
            self.assertEqual(second.scoped_device_names, {"core-1"})
            self.assertEqual(client.get_snapshot_metrics.call_count, 1)
            self.assertEqual(client.get_snapshots.call_count, 1)
            self.assertEqual(client.run_nqe_query.call_count, 1)

    def test_resolve_context_reuses_context_artifact_across_runs(self):
        self.source.parameters["device_tag_include_tags"] = ["DATACENTER"]
        self.source.parameters["device_tag_exclude_tags"] = ["BRANCH"]
        self.source.save(update_fields=["parameters"])
        with tempfile.TemporaryDirectory() as artifact_dir, patch.dict(
            os.environ,
            {"FORWARD_NETBOX_FETCH_ARTIFACT_DIR": artifact_dir},
        ):
            first_run = ForwardExecutionRun.objects.create(
                sync=self.sync,
                source=self.source,
                backend="branching",
                status="running",
                snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
                snapshot_id="snapshot-after",
                total_steps=2,
                next_step_index=1,
            )
            first_client = Mock()
            first_client.get_snapshot_metrics.return_value = {"deviceCount": 2}
            first_client.get_snapshots.return_value = [
                {
                    "id": "snapshot-after",
                    "state": "processed",
                    "created_at": "2026-03-31T10:00:00Z",
                    "processed_at": "2026-03-31T12:00:00Z",
                }
            ]
            first_client.get_latest_processed_snapshot.return_value = {
                "id": "snapshot-after",
                "processedAt": "2026-03-31T12:15:00Z",
            }
            first_client.run_nqe_query.return_value = [{"name": "core-1"}]
            first_fetcher = ForwardQueryFetcher(
                sync=self.sync,
                client=first_client,
                logger_=Mock(),
            )
            self.sync.get_network_id = Mock(return_value="test-network")
            self.sync.get_snapshot_id = Mock(return_value=LATEST_PROCESSED_SNAPSHOT)
            self.sync.resolve_snapshot_id = Mock(return_value="snapshot-after")
            self.sync.get_query_parameters = Mock(return_value={})
            self.sync.get_maps = Mock(return_value=[])
            branch_run_state = {
                "snapshot_selector": LATEST_PROCESSED_SNAPSHOT,
                "snapshot_id": "snapshot-after",
            }

            first_context = first_fetcher.resolve_context(
                branch_run_state=branch_run_state
            )
            self.assertEqual(first_context.scoped_device_names, {"core-1"})
            self.assertEqual(first_client.run_nqe_query.call_count, 1)

            first_run.status = "completed"
            first_run.save(update_fields=["status"])
            ForwardExecutionRun.objects.create(
                sync=self.sync,
                source=self.source,
                backend="branching",
                status="running",
                snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
                snapshot_id="snapshot-after",
                total_steps=2,
                next_step_index=1,
            )

            second_client = Mock()
            second_client.get_snapshot_metrics.side_effect = AssertionError(
                "snapshot metrics should come from shared context cache"
            )
            second_client.get_snapshots.side_effect = AssertionError(
                "snapshot metadata should come from shared context cache"
            )
            second_client.run_nqe_query.side_effect = AssertionError(
                "scoped device list should come from shared context cache"
            )
            second_fetcher = ForwardQueryFetcher(
                sync=self.sync,
                client=second_client,
                logger_=Mock(),
            )
            second_context = second_fetcher.resolve_context(
                branch_run_state=branch_run_state
            )
            self.assertEqual(second_context.scoped_device_names, {"core-1"})

    def test_query_path_resolution_reuses_run_local_artifact(self):
        with tempfile.TemporaryDirectory() as artifact_dir, patch.dict(
            os.environ,
            {"FORWARD_NETBOX_FETCH_ARTIFACT_DIR": artifact_dir},
        ):
            ForwardExecutionRun.objects.create(
                sync=self.sync,
                source=self.source,
                backend="branching",
                status="running",
                snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
                snapshot_id="snapshot-after",
                total_steps=2,
                next_step_index=1,
            )
            spec = QuerySpec(
                model_string="dcim.device",
                query_name="Forward Devices",
                query_repository="org",
                query_path="/forward_netbox_validation/forward_devices",
            )
            first_client = Mock()
            first_client.resolve_nqe_query_reference.return_value = {
                "queryId": "qid-123",
                "commitId": "cid-123",
            }
            first_fetcher = ForwardQueryFetcher(
                sync=self.sync,
                client=first_client,
                logger_=Mock(),
            )
            first_resolved = first_fetcher._resolve_query_specs("dcim.device", [spec])
            self.assertEqual(first_resolved[0].run_query_id, "qid-123")
            self.assertEqual(first_resolved[0].commit_id, "cid-123")
            first_client.resolve_nqe_query_reference.assert_called_once()

            second_client = Mock()
            second_client.resolve_nqe_query_reference.side_effect = AssertionError(
                "query_path resolve should be served from run-local artifact"
            )
            second_fetcher = ForwardQueryFetcher(
                sync=self.sync,
                client=second_client,
                logger_=Mock(),
            )
            second_resolved = second_fetcher._resolve_query_specs("dcim.device", [spec])
            self.assertEqual(second_resolved[0].run_query_id, "qid-123")
            self.assertEqual(second_resolved[0].commit_id, "cid-123")
            self.assertFalse(second_client.resolve_nqe_query_reference.called)

    def test_query_path_resolution_reuses_artifact_across_runs(self):
        with tempfile.TemporaryDirectory() as artifact_dir, patch.dict(
            os.environ,
            {"FORWARD_NETBOX_FETCH_ARTIFACT_DIR": artifact_dir},
        ):
            first_run = ForwardExecutionRun.objects.create(
                sync=self.sync,
                source=self.source,
                backend="branching",
                status="running",
                snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
                snapshot_id="snapshot-after",
                total_steps=2,
                next_step_index=1,
            )
            spec = QuerySpec(
                model_string="dcim.device",
                query_name="Forward Devices",
                query_repository="org",
                query_path="/forward_netbox_validation/forward_devices",
            )
            first_client = Mock()
            first_client.resolve_nqe_query_reference.return_value = {
                "queryId": "qid-123",
                "commitId": "cid-123",
            }
            first_fetcher = ForwardQueryFetcher(
                sync=self.sync,
                client=first_client,
                logger_=Mock(),
            )
            first_resolved = first_fetcher._resolve_query_specs("dcim.device", [spec])
            self.assertEqual(first_resolved[0].run_query_id, "qid-123")
            self.assertEqual(first_resolved[0].commit_id, "cid-123")
            first_client.resolve_nqe_query_reference.assert_called_once()

            first_run.status = "completed"
            first_run.save(update_fields=["status"])
            ForwardExecutionRun.objects.create(
                sync=self.sync,
                source=self.source,
                backend="branching",
                status="running",
                snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
                snapshot_id="snapshot-after",
                total_steps=2,
                next_step_index=1,
            )

            second_client = Mock()
            second_client.resolve_nqe_query_reference.side_effect = AssertionError(
                "query_path resolve should be served from shared artifact"
            )
            second_fetcher = ForwardQueryFetcher(
                sync=self.sync,
                client=second_client,
                logger_=Mock(),
            )
            second_resolved = second_fetcher._resolve_query_specs("dcim.device", [spec])
            self.assertEqual(second_resolved[0].run_query_id, "qid-123")
            self.assertEqual(second_resolved[0].commit_id, "cid-123")
            self.assertFalse(second_client.resolve_nqe_query_reference.called)

    def test_apply_device_tag_scope_filters_rows_with_device_keys(self):
        fetcher = ForwardQueryFetcher(
            sync=self.sync,
            client=Mock(),
            logger_=Mock(),
        )
        context = ForwardQueryContext(
            network_id="test-network",
            snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
            snapshot_id="snapshot-after",
            scoped_device_names={"core-1"},
        )
        rows = [
            {"device": "core-1", "name": "Ethernet1"},
            {"device": "branch-1", "name": "Ethernet1"},
            {"name": "site-only-row"},
        ]

        filtered, removed = fetcher._apply_device_tag_scope(
            "dcim.interface", rows, context
        )

        self.assertEqual(
            filtered,
            [
                {"device": "core-1", "name": "Ethernet1"},
                {"name": "site-only-row"},
            ],
        )
        self.assertEqual(removed, [{"device": "branch-1", "name": "Ethernet1"}])

    def test_apply_device_tag_scope_uses_primary_device_for_routing_rows(self):
        fetcher = ForwardQueryFetcher(
            sync=self.sync,
            client=Mock(),
            logger_=Mock(),
        )
        context = ForwardQueryContext(
            network_id="test-network",
            snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
            snapshot_id="snapshot-after",
            scoped_device_names={"core-1"},
        )
        rows = [
            {
                "device": "branch-1",
                "remote_device": "core-1",
                "local_interface": "Ethernet1/1",
            },
            {
                "device": "core-1",
                "remote_device": "branch-1",
                "local_interface": "Ethernet1/2",
            },
        ]

        filtered, removed = fetcher._apply_device_tag_scope(
            "netbox_routing.ospfinterface", rows, context
        )

        self.assertEqual(
            filtered,
            [
                {
                    "device": "core-1",
                    "remote_device": "branch-1",
                    "local_interface": "Ethernet1/2",
                }
            ],
        )
        self.assertEqual(
            removed,
            [
                {
                    "device": "branch-1",
                    "remote_device": "core-1",
                    "local_interface": "Ethernet1/1",
                }
            ],
        )

    def test_fetch_spec_rows_prunes_out_of_scope_rows_into_deletes(self):
        fetcher = ForwardQueryFetcher(
            sync=self.sync,
            client=Mock(),
            logger_=Mock(),
        )
        spec = Mock(
            run_query_id=None,
            execution_value="raw",
            merged_parameters=Mock(return_value={}),
        )
        context = ForwardQueryContext(
            network_id="test-network",
            snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
            snapshot_id="snapshot-after",
            scoped_device_names={"core-1"},
            device_tag_prune_out_of_scope=True,
        )
        fetcher._run_nqe_query_with_parameter_fallback = Mock(
            return_value=[
                {"device": "core-1", "name": "Ethernet1"},
                {"device": "branch-1", "name": "Ethernet1"},
            ]
        )

        rows, delete_rows, sync_mode = fetcher._fetch_spec_rows(
            "dcim.interface",
            spec,
            baseline=None,
            context=context,
            coalesce_fields=[["device", "name"]],
        )

        self.assertEqual(sync_mode, "full")
        self.assertEqual(rows, [{"device": "core-1", "name": "Ethernet1"}])
        self.assertEqual(
            delete_rows,
            [{"device": "branch-1", "name": "Ethernet1"}],
        )

    def test_fetch_spec_rows_reports_fetch_metadata_for_column_filter_scope(self):
        fetcher = ForwardQueryFetcher(
            sync=self.sync,
            client=Mock(),
            logger_=Mock(),
        )
        spec = QuerySpec(
            model_string="dcim.interface",
            query_name="Forward Interfaces",
            query="foreach interface select {device: interface.device.name, name: interface.name}",
            parameters={"existing": "value"},
        )
        context = ForwardQueryContext(
            network_id="test-network",
            snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
            snapshot_id="snapshot-after",
        )
        shard_scope = {
            "fetch_mode": "nqe_column_filter",
            "fetch_key_family": "device",
            "fetch_parameters": {"shard_hint": "device"},
            "fetch_column_filters": [
                {"operator": "DEFAULT", "columnName": "device", "value": "core-1"}
            ],
            "query_parameters": {"qscope": "core"},
            "shard_keys": ["device:core-1"],
        }
        fetcher._run_nqe_query_with_parameter_fallback = Mock(
            return_value=[{"device": "core-1", "name": "Ethernet1"}]
        )

        rows, delete_rows, sync_mode, fetch_meta = fetcher._fetch_spec_rows(
            "dcim.interface",
            spec,
            baseline=None,
            context=context,
            coalesce_fields=[["device", "name"]],
            shard_scope=shard_scope,
            return_fetch_meta=True,
        )

        self.assertEqual(sync_mode, "full")
        self.assertEqual(rows, [{"device": "core-1", "name": "Ethernet1"}])
        self.assertEqual(delete_rows, [])
        self.assertEqual(fetch_meta["fetch_mode"], "nqe_column_filter")
        self.assertEqual(fetch_meta["fetch_key_family"], "device")
        self.assertEqual(fetch_meta["fetch_parameters"], {"shard_hint": "device"})
        self.assertEqual(
            fetch_meta["fetch_column_filters"],
            [{"operator": "DEFAULT", "columnName": "device", "value": "core-1"}],
        )
        self.assertEqual(
            fetch_meta["query_parameters"],
            {"existing": "value", "qscope": "core"},
        )

    def test_fetch_spec_rows_reuses_run_local_artifact_for_shard_retry(self):
        with tempfile.TemporaryDirectory() as artifact_dir, patch.dict(
            os.environ,
            {"FORWARD_NETBOX_FETCH_ARTIFACT_DIR": artifact_dir},
        ):
            ForwardExecutionRun.objects.create(
                sync=self.sync,
                source=self.source,
                backend="branching",
                status="running",
                snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
                snapshot_id="snapshot-after",
                total_steps=1,
                next_step_index=1,
            )
            spec = QuerySpec(
                model_string="dcim.interface",
                query_name="Forward Interfaces",
                query="foreach interface select {device: interface.device.name, name: interface.name}",
            )
            context = ForwardQueryContext(
                network_id="test-network",
                snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
                snapshot_id="snapshot-after",
            )
            shard_scope = {
                "fetch_mode": "nqe_column_filter",
                "fetch_key_family": "device",
                "fetch_parameters": {"shard_hint": "device"},
                "fetch_column_filters": [
                    {"operator": "DEFAULT", "columnName": "device", "value": "core-1"}
                ],
                "query_parameters": {},
                "shard_keys": ["device:core-1"],
            }
            first_fetcher = ForwardQueryFetcher(
                sync=self.sync,
                client=Mock(),
                logger_=Mock(),
            )
            first_fetcher._run_nqe_query_with_parameter_fallback = Mock(
                return_value=[{"device": "core-1", "name": "Ethernet1"}]
            )

            rows, delete_rows, sync_mode, fetch_meta = first_fetcher._fetch_spec_rows(
                "dcim.interface",
                spec,
                baseline=None,
                context=context,
                coalesce_fields=[["device", "name"]],
                shard_scope=shard_scope,
                return_fetch_meta=True,
            )

            self.assertEqual(sync_mode, "full")
            self.assertEqual(rows, [{"device": "core-1", "name": "Ethernet1"}])
            self.assertEqual(delete_rows, [])
            self.assertEqual(
                fetch_meta["fetch_parameters"]["fetch_artifact"]["status"],
                "stored",
            )
            self.assertNotIn(
                "rows",
                fetch_meta["fetch_parameters"]["fetch_artifact"],
            )

            retry_fetcher = ForwardQueryFetcher(
                sync=self.sync,
                client=Mock(),
                logger_=Mock(),
            )
            retry_fetcher._run_nqe_query_with_parameter_fallback = Mock(
                side_effect=AssertionError("Forward query should not run")
            )

            retry_rows, retry_deletes, retry_mode, retry_meta = (
                retry_fetcher._fetch_spec_rows(
                    "dcim.interface",
                    spec,
                    baseline=None,
                    context=context,
                    coalesce_fields=[["device", "name"]],
                    shard_scope=shard_scope,
                    return_fetch_meta=True,
                )
            )

            self.assertEqual(retry_mode, "full")
            self.assertEqual(retry_rows, rows)
            self.assertEqual(retry_deletes, [])
            self.assertFalse(
                retry_fetcher._run_nqe_query_with_parameter_fallback.called
            )
            self.assertEqual(
                retry_meta["fetch_parameters"]["fetch_artifact"]["status"],
                "hit",
            )

    def test_fetch_spec_rows_reuses_model_fallback_artifact_across_shards(self):
        with tempfile.TemporaryDirectory() as artifact_dir, patch.dict(
            os.environ,
            {"FORWARD_NETBOX_FETCH_ARTIFACT_DIR": artifact_dir},
        ):
            ForwardExecutionRun.objects.create(
                sync=self.sync,
                source=self.source,
                backend="branching",
                status="running",
                snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
                snapshot_id="snapshot-after",
                total_steps=2,
                next_step_index=1,
            )
            spec = QuerySpec(
                model_string="dcim.interface",
                query_name="Forward Interfaces",
                query="foreach interface select {device: interface.device.name, name: interface.name}",
            )
            context = ForwardQueryContext(
                network_id="test-network",
                snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
                snapshot_id="snapshot-after",
            )
            first_scope = {
                "fetch_mode": "nqe_column_filter",
                "fetch_key_family": "device",
                "fetch_parameters": {"shard_hint": "device"},
                "fetch_column_filters": [
                    {"operator": "DEFAULT", "columnName": "device", "value": "core-1"}
                ],
                "query_parameters": {},
                "shard_keys": ["device:core-1"],
            }
            second_scope = {
                **first_scope,
                "fetch_column_filters": [
                    {"operator": "DEFAULT", "columnName": "device", "value": "core-2"}
                ],
                "shard_keys": ["device:core-2"],
            }
            full_rows = [
                {"device": "core-1", "name": "Ethernet1"},
                {"device": "core-2", "name": "Ethernet2"},
            ]
            fetcher = ForwardQueryFetcher(
                sync=self.sync,
                client=Mock(),
                logger_=Mock(),
            )
            fetcher._run_nqe_query_with_parameter_fallback = Mock(
                side_effect=[
                    ForwardClientError("scoped fetch failed"),
                    ForwardClientError("alternate scoped fetch failed"),
                    full_rows,
                    ForwardClientError("scoped fetch failed again"),
                    ForwardClientError("alternate scoped fetch failed again"),
                ]
            )

            rows, _delete_rows, _sync_mode, fetch_meta = fetcher._fetch_spec_rows(
                "dcim.interface",
                spec,
                baseline=None,
                context=context,
                coalesce_fields=[["device", "name"]],
                shard_scope=first_scope,
                return_fetch_meta=True,
            )
            retry_rows, _retry_deletes, _retry_mode, retry_meta = (
                fetcher._fetch_spec_rows(
                    "dcim.interface",
                    spec,
                    baseline=None,
                    context=context,
                    coalesce_fields=[["device", "name"]],
                    shard_scope=second_scope,
                    return_fetch_meta=True,
                )
            )

            self.assertEqual(rows, [{"device": "core-1", "name": "Ethernet1"}])
            self.assertEqual(retry_rows, [{"device": "core-2", "name": "Ethernet2"}])
            self.assertEqual(
                fetcher._run_nqe_query_with_parameter_fallback.call_count, 3
            )
            self.assertEqual(
                fetch_meta["fetch_parameters"]["model_fetch_artifact"]["status"],
                "stored",
            )
            self.assertEqual(
                retry_meta["fetch_parameters"]["model_fetch_artifact"]["status"],
                "hit",
            )

    def test_fetch_spec_rows_reuses_model_fallback_artifact_across_runs(self):
        with tempfile.TemporaryDirectory() as artifact_dir, patch.dict(
            os.environ,
            {"FORWARD_NETBOX_FETCH_ARTIFACT_DIR": artifact_dir},
        ):
            run_1 = ForwardExecutionRun.objects.create(
                sync=self.sync,
                source=self.source,
                backend="branching",
                status="running",
                snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
                snapshot_id="snapshot-after",
                total_steps=2,
                next_step_index=1,
            )
            spec = QuerySpec(
                model_string="dcim.interface",
                query_name="Forward Interfaces",
                query="foreach interface select {device: interface.device.name, name: interface.name}",
            )
            context = ForwardQueryContext(
                network_id="test-network",
                snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
                snapshot_id="snapshot-after",
            )
            first_scope = {
                "fetch_mode": "nqe_column_filter",
                "fetch_key_family": "device",
                "fetch_parameters": {"shard_hint": "device"},
                "fetch_column_filters": [
                    {"operator": "DEFAULT", "columnName": "device", "value": "core-1"}
                ],
                "query_parameters": {},
                "shard_keys": ["device:core-1"],
            }
            second_scope = {
                **first_scope,
                "fetch_column_filters": [
                    {"operator": "DEFAULT", "columnName": "device", "value": "core-2"}
                ],
                "shard_keys": ["device:core-2"],
            }
            full_rows = [
                {"device": "core-1", "name": "Ethernet1"},
                {"device": "core-2", "name": "Ethernet2"},
            ]
            first_fetcher = ForwardQueryFetcher(
                sync=self.sync,
                client=Mock(),
                logger_=Mock(),
            )
            first_fetcher._run_nqe_query_with_parameter_fallback = Mock(
                side_effect=[
                    ForwardClientError("scoped fetch failed"),
                    ForwardClientError("alternate scoped fetch failed"),
                    full_rows,
                ]
            )

            rows, _delete_rows, _sync_mode, fetch_meta = first_fetcher._fetch_spec_rows(
                "dcim.interface",
                spec,
                baseline=None,
                context=context,
                coalesce_fields=[["device", "name"]],
                shard_scope=first_scope,
                return_fetch_meta=True,
            )
            run_1.status = "completed"
            run_1.save(update_fields=["status"])
            ForwardExecutionRun.objects.create(
                sync=self.sync,
                source=self.source,
                backend="branching",
                status="running",
                snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
                snapshot_id="snapshot-after",
                total_steps=2,
                next_step_index=1,
            )
            retry_fetcher = ForwardQueryFetcher(
                sync=self.sync,
                client=Mock(),
                logger_=Mock(),
            )
            retry_fetcher._run_nqe_query_with_parameter_fallback = Mock(
                side_effect=[
                    ForwardClientError("scoped fetch failed again"),
                    ForwardClientError("alternate scoped fetch failed again"),
                ]
            )

            retry_rows, _retry_delete_rows, _retry_sync_mode, retry_meta = (
                retry_fetcher._fetch_spec_rows(
                    "dcim.interface",
                    spec,
                    baseline=None,
                    context=context,
                    coalesce_fields=[["device", "name"]],
                    shard_scope=second_scope,
                    return_fetch_meta=True,
                )
            )

            self.assertEqual(rows, [{"device": "core-1", "name": "Ethernet1"}])
            self.assertEqual(retry_rows, [{"device": "core-2", "name": "Ethernet2"}])
            self.assertEqual(
                fetch_meta["fetch_parameters"]["model_fetch_artifact"]["status"],
                "stored",
            )
            self.assertEqual(
                retry_meta["fetch_parameters"]["model_fetch_artifact"]["status"],
                "hit",
            )
            self.assertEqual(
                retry_fetcher._run_nqe_query_with_parameter_fallback.call_count, 0
            )

    def test_fetch_artifacts_are_pruned_when_execution_run_completes(self):
        with tempfile.TemporaryDirectory() as artifact_dir, patch.dict(
            os.environ,
            {"FORWARD_NETBOX_FETCH_ARTIFACT_DIR": artifact_dir},
        ):
            run = ForwardExecutionRun.objects.create(
                sync=self.sync,
                source=self.source,
                backend="branching",
                status="running",
                snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
                snapshot_id="snapshot-after",
                total_steps=1,
                next_step_index=1,
            )
            ForwardExecutionStep.objects.create(
                run=run,
                index=1,
                kind="stage",
                status=ForwardExecutionStepStatusChoices.MERGED,
                model_string="dcim.interface",
            )
            from forward_netbox.utilities.execution_ledger import mark_run_completed
            from forward_netbox.utilities.fetch_artifacts import save_fetch_artifact

            artifact_meta = save_fetch_artifact(
                "artifact-key",
                run_id=run.pk,
                rows=[{"device": "core-1", "name": "Ethernet1"}],
                delete_rows=[],
                sync_mode="full",
                fetch_meta={"fetch_mode": "nqe_column_filter"},
            )

            self.assertEqual(artifact_meta["status"], "stored")
            self.assertTrue(os.path.exists(os.path.join(artifact_dir, f"run-{run.pk}")))

            mark_run_completed(self.sync, baseline_ready=True)

            self.assertFalse(
                os.path.exists(os.path.join(artifact_dir, f"run-{run.pk}"))
            )

    def test_fetch_artifacts_are_pruned_when_execution_run_fails(self):
        with tempfile.TemporaryDirectory() as artifact_dir, patch.dict(
            os.environ,
            {"FORWARD_NETBOX_FETCH_ARTIFACT_DIR": artifact_dir},
        ):
            run = ForwardExecutionRun.objects.create(
                sync=self.sync,
                source=self.source,
                backend="branching",
                status="running",
                snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
                snapshot_id="snapshot-after",
                total_steps=1,
                next_step_index=1,
            )
            from forward_netbox.utilities.fetch_artifacts import save_fetch_artifact
            from forward_netbox.utilities.sync_state import mark_branch_run_failed

            artifact_meta = save_fetch_artifact(
                "artifact-key",
                run_id=run.pk,
                rows=[{"device": "core-1", "name": "Ethernet1"}],
                delete_rows=[],
                sync_mode="full",
                fetch_meta={"fetch_mode": "nqe_column_filter"},
            )

            self.assertEqual(artifact_meta["status"], "stored")
            self.assertTrue(os.path.exists(os.path.join(artifact_dir, f"run-{run.pk}")))

            mark_branch_run_failed(self.sync, "failed")

            self.assertFalse(
                os.path.exists(os.path.join(artifact_dir, f"run-{run.pk}"))
            )

    def test_fetch_spec_rows_marks_full_fallback_when_shard_fetch_fails(self):
        fetcher = ForwardQueryFetcher(
            sync=self.sync,
            client=Mock(),
            logger_=Mock(),
        )
        spec = QuerySpec(
            model_string="dcim.interface",
            query_name="Forward Interfaces",
            query="foreach interface select {device: interface.device.name, name: interface.name}",
        )
        context = ForwardQueryContext(
            network_id="test-network",
            snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
            snapshot_id="snapshot-after",
        )
        shard_scope = {
            "fetch_mode": "nqe_column_filter",
            "fetch_key_family": "device",
            "fetch_parameters": {"shard_hint": "device"},
            "fetch_column_filters": [
                {"operator": "DEFAULT", "columnName": "device", "value": "core-1"}
            ],
            "query_parameters": {},
            "shard_keys": ["device:core-1"],
        }
        fetcher._run_nqe_query_with_parameter_fallback = Mock(
            side_effect=[
                ForwardClientError("scoped fetch failed"),
                ForwardClientError(
                    "alternate scoped fetch failed: networkId="
                    + "123456 snapshotId="
                    + "654321 queryId=Q"
                    + "99999"
                ),
                [{"device": "core-1", "name": "Ethernet1"}],
            ]
        )

        rows, delete_rows, sync_mode, fetch_meta = fetcher._fetch_spec_rows(
            "dcim.interface",
            spec,
            baseline=None,
            context=context,
            coalesce_fields=[["device", "name"]],
            shard_scope=shard_scope,
            return_fetch_meta=True,
        )

        self.assertEqual(sync_mode, "full")
        self.assertEqual(rows, [{"device": "core-1", "name": "Ethernet1"}])
        self.assertEqual(delete_rows, [])
        self.assertEqual(fetch_meta["fetch_mode"], "full_fallback")
        self.assertEqual(fetch_meta["fetch_key_family"], "device")
        fallback_reason = fetch_meta["fetch_parameters"]["fallback_reason"]
        self.assertIn("alternate scoped fetch failed", fallback_reason)
        self.assertIn("networkId=<redacted>", fallback_reason)
        self.assertIn("snapshotId=<redacted>", fallback_reason)
        self.assertIn("queryId=<redacted>", fallback_reason)
        self.assertNotIn("123456", fallback_reason)
        self.assertNotIn("654321", fallback_reason)
        self.assertNotIn("99999", fallback_reason)
        self.assertEqual(
            fetch_meta["fetch_parameters"]["partition_retry_summary"],
            {
                "operation": "full",
                "partition_count": 1,
                "split_retry_count": 0,
                "split_retry_success_count": 0,
                "alternate_operator_retry_count": 1,
                "alternate_operator_success_count": 0,
            },
        )
        self.assertEqual(fetcher._run_nqe_query_with_parameter_fallback.call_count, 3)
        self.assertEqual(
            fetcher._run_nqe_query_with_parameter_fallback.call_args_list[1].kwargs[
                "column_filters"
            ],
            [{"operator": "EQUALS_ANY", "columnName": "device", "values": ["core-1"]}],
        )
        self.assertIsNone(
            fetcher._run_nqe_query_with_parameter_fallback.call_args_list[2].kwargs[
                "column_filters"
            ]
        )

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
            "forward_netbox.utilities.sync_execution.get_query_specs",
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
            "forward_netbox.utilities.sync_execution.get_query_specs",
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

    def test_apply_engine_classifies_all_supported_models(self):
        self.assertEqual(ADAPTER_MODELS_WITHOUT_BLOCKER, ())
        self.assertEqual(UNCLASSIFIED_SUPPORTED_MODELS, ())
        self.assertEqual(
            set(APPLY_ENGINE_MODEL_CLASSIFICATIONS),
            set(FORWARD_SUPPORTED_MODELS),
        )
        for model_string in FORWARD_SUPPORTED_MODELS:
            decision = apply_engine_decision_for(
                sync=self.sync,
                model_string=model_string,
                backend="branching",
            )
            if model_string in BULK_ORM_ENABLED_MODELS:
                self.assertEqual(decision.selected_engine, "adapter")
                self.assertEqual(decision.reason_code, "bulk_orm_disabled_by_default")
            else:
                self.assertEqual(decision.selected_engine, "adapter")
            self.assertTrue(decision.reason_code)
            self.assertTrue(decision.reason)
            self.assertNotEqual(
                decision.reason_code,
                "adapter_default_unclassified_model",
            )

    def test_apply_engine_classifies_all_supported_models_when_bulk_orm_enabled(self):
        self.sync.parameters["enable_bulk_orm"] = True
        self.sync.save(update_fields=["parameters"])

        self.assertEqual(BULK_ORM_ENABLED_MODELS_WITHOUT_SPECS, ())
        self.assertEqual(UNCLASSIFIED_SUPPORTED_MODELS, ())
        self.assertEqual(
            set(APPLY_ENGINE_MODEL_CLASSIFICATIONS),
            set(FORWARD_SUPPORTED_MODELS),
        )
        for model_string in FORWARD_SUPPORTED_MODELS:
            decision = apply_engine_decision_for(
                sync=self.sync,
                model_string=model_string,
                backend="branching",
            )
            if model_string in BULK_ORM_ENABLED_MODELS:
                self.assertEqual(decision.selected_engine, "bulk_orm")
                self.assertEqual(
                    decision.reason_code,
                    "bulk_orm_enabled_safe_model_set",
                )
            elif model_string in ADAPTER_REQUIRED_MODELS:
                self.assertEqual(decision.selected_engine, "adapter")
                self.assertEqual(
                    decision.reason_code,
                    "adapter_required_model_contract",
                )
            else:
                self.fail(f"Unhandled model classification for {model_string}")

    def test_bulk_orm_expansion_summary_requires_parity_for_blocked_models(self):
        summary = bulk_orm_expansion_summary(FORWARD_SUPPORTED_MODELS)

        self.assertEqual(summary["status"], "blocked_pending_parity")
        self.assertIn("dcim.site", summary["safe_models"])
        self.assertGreater(summary["blocked_model_count"], 0)
        self.assertGreater(len(summary["promotion_lanes"]), 0)
        self.assertEqual(
            summary["promotion_lanes"][0]["lane"],
            "dependency_anchored_models",
        )
        self.assertEqual(
            summary["promotion_lanes"][0]["required_gates"],
            ["dependency_resolution_parity"],
        )
        self.assertEqual(
            summary["recommended_next_models"][0]["model"],
            "dcim.device",
        )
        self.assertEqual(
            summary["recommended_next_models"][0]["required_gate"],
            "dependency_resolution_parity",
        )
        self.assertEqual(
            summary["high_impact_blocked_models"][0]["model"],
            "dcim.device",
        )
        self.assertEqual(
            summary["high_impact_blocked_models"][0]["required_gate"],
            "dependency_resolution_parity",
        )
        self.assertEqual(summary["parity_gates"][0]["code"], "netbox_validation_parity")
        self.assertIn("Branching", summary["parity_gates"][2]["description"])
        self.assertEqual(
            summary["parity_plan"]["status"],
            "pending_candidate_parity",
        )
        self.assertEqual(
            summary["parity_plan"]["candidates"][0]["model"],
            "dcim.device",
        )
        self.assertIn(
            "lowest_risk_lane",
            summary["parity_plan"]["candidates"][0]["candidate_sources"],
        )
        self.assertEqual(
            summary["parity_plan"]["candidates"][0]["lane_gate"],
            "dependency_resolution_parity",
        )
        self.assertIn(
            "ForwardApplyEngineParityTest.test_dcim_device_create_parity",
            summary["parity_plan"]["candidates"][0]["required_test_ids"],
        )
        self.assertIn(
            "branching_semantics_parity",
            [
                gate["code"]
                for gate in summary["parity_plan"]["candidates"][0][
                    "required_checklist"
                ]
            ],
        )
        self.assertIn("first recommended promotion lane", summary["next_action"])

    def test_apply_model_rows_records_forward_query_error_and_continues(self):
        runner = ForwardSyncRunner(
            sync=self.sync, ingestion=None, client=None, logger_=Mock()
        )

        runner._apply_dcim_site = Mock(
            side_effect=[
                ForwardQueryError("boom"),
                True,
            ]
        )
        with patch(
            "forward_netbox.utilities.sync_reporting.record_issue"
        ) as record_issue:
            runner._apply_model_rows(
                "dcim.site",
                [
                    {"name": "site-1", "slug": "site-1"},
                    {"name": "site-2", "slug": "site-2"},
                ],
            )

        self.assertEqual(runner._apply_dcim_site.call_count, 2)
        record_issue.assert_called_once()
        runner.logger.increment_statistics.assert_any_call(
            "dcim.site", outcome="failed"
        )
        runner.logger.increment_statistics.assert_any_call(
            "dcim.site", outcome="applied"
        )

    def test_apply_model_rows_records_validation_error_and_continues(self):
        runner = ForwardSyncRunner(
            sync=self.sync, ingestion=None, client=None, logger_=Mock()
        )
        runner._apply_dcim_site = Mock(
            side_effect=[
                ValidationError("bad row"),
                True,
            ]
        )
        with patch(
            "forward_netbox.utilities.sync_reporting.record_issue"
        ) as record_issue:
            runner._apply_model_rows(
                "dcim.site",
                [
                    {"name": "site-1", "slug": "site-1"},
                    {"name": "site-2", "slug": "site-2"},
                ],
            )

        self.assertEqual(runner._apply_dcim_site.call_count, 2)
        record_issue.assert_called_once()

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
        with patch(
            "forward_netbox.utilities.sync_reporting.record_issue"
        ) as record_issue:
            runner._apply_model_rows(
                "dcim.site", [{"name": "site-1", "slug": "site-1"}]
            )

        _, _, kwargs = record_issue.mock_calls[0]
        self.assertEqual(kwargs["context"], {"slug": "site-1"})
        self.assertEqual(kwargs["defaults"], {"name": "site-1"})

    def test_delete_model_rows_records_row_failure_and_continues(self):
        runner = ForwardSyncRunner(
            sync=self.sync, ingestion=None, client=None, logger_=Mock()
        )
        runner._delete_dcim_site = Mock(
            side_effect=[
                ForwardSearchError("missing row"),
                True,
            ]
        )
        with patch(
            "forward_netbox.utilities.sync_reporting.record_issue"
        ) as record_issue:
            runner._delete_model_rows(
                "dcim.site",
                [
                    {"name": "site-1", "slug": "site-1"},
                    {"name": "site-2", "slug": "site-2"},
                ],
            )

        self.assertEqual(runner._delete_dcim_site.call_count, 2)
        record_issue.assert_called_once()
        runner.logger.increment_statistics.assert_any_call(
            "dcim.site", outcome="failed"
        )
        runner.logger.increment_statistics.assert_any_call(
            "dcim.site", outcome="applied"
        )

    def test_delete_model_rows_records_dependency_skip_as_skipped_info(self):
        runner = ForwardSyncRunner(
            sync=self.sync, ingestion=None, client=None, logger_=Mock()
        )
        runner._delete_dcim_device = Mock(
            side_effect=[
                ForwardDependencySkipError(
                    "protected child remains",
                    model_string="dcim.device",
                    context={"name": "device-1"},
                ),
                True,
            ]
        )
        with patch(
            "forward_netbox.utilities.sync_reporting.record_issue"
        ) as record_issue:
            runner._delete_model_rows(
                "dcim.device",
                [
                    {"name": "device-1"},
                    {"name": "device-2"},
                ],
            )

        self.assertEqual(runner._delete_dcim_device.call_count, 2)
        _, _, kwargs = record_issue.mock_calls[0]
        self.assertEqual(kwargs["log_level"], "info")
        runner.logger.increment_statistics.assert_any_call(
            "dcim.device", outcome="skipped"
        )
        runner.logger.increment_statistics.assert_any_call(
            "dcim.device", outcome="applied"
        )

    def test_delete_by_coalesce_maps_protected_error_to_dependency_skip(self):
        class _DummyModel:
            class _meta:
                label_lower = "dcim.device"

        class _DummyObject:
            def delete(self):
                raise ProtectedError("blocked", set())

        runner = Mock()
        with patch(
            "forward_netbox.utilities.sync_primitives.get_unique_or_raise",
            return_value=_DummyObject(),
        ):
            with self.assertRaises(ForwardDependencySkipError) as exc:
                delete_by_coalesce(runner, _DummyModel, [{"name": "device-1"}])

        self.assertEqual(exc.exception.model_string, "dcim.device")
        self.assertEqual(exc.exception.context, {"name": "device-1"})

    def test_apply_model_rows_marks_handler_false_as_skipped(self):
        logger = Mock()
        runner = ForwardSyncRunner(
            sync=self.sync, ingestion=None, client=None, logger_=logger
        )

        runner._apply_dcim_site = Mock(return_value=False)

        runner._apply_model_rows("dcim.site", [{"name": "site-1", "slug": "site-1"}])

        logger.increment_statistics.assert_called_with("dcim.site", outcome="skipped")

    def test_apply_model_rows_emits_progress_heartbeat_for_branch_runs(self):
        self.sync.set_branch_run_state(
            {
                "phase": "executing",
                "phase_message": "Applying planned shard changes.",
                "current_model_string": "dcim.site",
                "current_shard_index": 131,
                "total_plan_items": 146,
                "current_row_total": 2,
            }
        )
        logger = Mock()
        runner = ForwardSyncRunner(
            sync=self.sync, ingestion=None, client=None, logger_=logger
        )
        runner._apply_dcim_site = Mock(side_effect=[True, True])

        with patch(
            "forward_netbox.utilities.sync_reporting.touch_branch_run_progress"
        ) as touch_progress, patch(
            "forward_netbox.utilities.sync_reporting.time.monotonic",
            side_effect=[0.0, 120.0],
        ):
            runner._apply_model_rows(
                "dcim.site",
                [
                    {"name": "site-1", "slug": "site-1"},
                    {"name": "site-2", "slug": "site-2"},
                ],
            )

        self.assertGreaterEqual(touch_progress.call_count, 2)
        first_call = touch_progress.call_args_list[0]
        _, kwargs = first_call
        self.assertEqual(
            kwargs["phase_message"],
            "Applying shard 131/146 for dcim.site: 1/2 rows.",
        )
        self.assertEqual(kwargs["model_string"], "dcim.site")

    def test_apply_model_rows_updates_ledger_progress_without_branch_state(self):
        execution_run = ForwardExecutionRun.objects.create(
            sync=self.sync,
            source=self.source,
            backend="branching",
            status="running",
            phase="executing",
            phase_message="Applying planned shard changes.",
            snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
            snapshot_id="snapshot-1",
            total_steps=1,
            next_step_index=1,
        )
        step = ForwardExecutionStep.objects.create(
            run=execution_run,
            index=1,
            kind="stage",
            status="running",
            model_string="dcim.site",
            label="dcim.site part 1",
            estimated_changes=2,
        )
        logger = Mock()
        runner = ForwardSyncRunner(
            sync=self.sync, ingestion=None, client=None, logger_=logger
        )
        runner._apply_dcim_site = Mock(side_effect=[True, True])

        with patch(
            "forward_netbox.utilities.sync_reporting.time.monotonic",
            side_effect=[0.0, 120.0],
        ):
            runner._apply_model_rows(
                "dcim.site",
                [
                    {"name": "site-1", "slug": "site-1"},
                    {"name": "site-2", "slug": "site-2"},
                ],
            )

        step.refresh_from_db()
        execution_run.refresh_from_db()
        self.assertEqual(self.sync.get_branch_run_state(), {})
        self.assertEqual(step.attempted_row_count, 2)
        self.assertEqual(step.fetched_row_count, 2)
        self.assertIsNotNone(step.heartbeat)
        self.assertEqual(execution_run.latest_heartbeat, step.heartbeat)
        self.assertIn("Processing dcim.site shard 1/1", self.sync.get_sync_activity())
        self.assertIn("(2/2 rows)", self.sync.get_sync_activity())
        logger.log_info.assert_any_call(
            "Applying shard 1/1 for dcim.site: 1/2 rows.",
            obj=self.sync,
        )

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

    def test_apply_virtual_chassis_skips_membership_without_position(self):
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

        result = runner._apply_dcim_virtualchassis(
            {
                "device": device.name,
                "vc_name": "site-1-mlag-device-1--device-2",
                "vc_domain": "device-1--device-2",
            }
        )
        device.refresh_from_db()

        self.assertFalse(result)
        self.assertIsNone(device.virtual_chassis)
        self.assertIsNone(device.vc_position)
        self.assertFalse(VirtualChassis.objects.exists())
        runner.logger.log_warning.assert_called()

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

    def test_apply_virtual_chassis_rejects_duplicate_position(self):
        site = Site.objects.create(name="site-3", slug="site-3")
        manufacturer = Manufacturer.objects.create(name="vendor-3", slug="vendor-3")
        role = DeviceRole.objects.create(name="role-3", slug="role-3", color="9e9e9e")
        device_type = DeviceType.objects.create(
            manufacturer=manufacturer,
            model="model-3",
            slug="model-3",
        )
        device_1 = Device.objects.create(
            name="device-3a",
            site=site,
            role=role,
            device_type=device_type,
            status="active",
        )
        device_2 = Device.objects.create(
            name="device-3b",
            site=site,
            role=role,
            device_type=device_type,
            status="active",
        )
        runner = ForwardSyncRunner(
            sync=self.sync, ingestion=None, client=None, logger_=Mock()
        )
        runner._apply_dcim_virtualchassis(
            {
                "device": device_1.name,
                "vc_name": "site-3-vpc-200",
                "vc_domain": "200",
                "vc_position": 1,
            }
        )

        with self.assertRaisesRegex(
            ForwardSyncDataError,
            "already has device `device-3a` at position `1`",
        ):
            runner._apply_dcim_virtualchassis(
                {
                    "device": device_2.name,
                    "vc_name": "site-3-vpc-200",
                    "vc_domain": "200",
                    "vc_position": 1,
                }
            )
        device_2.refresh_from_db()

        self.assertIsNone(device_2.virtual_chassis)
        self.assertIsNone(device_2.vc_position)


class ForwardApplyEngineParityTest(TestCase):
    def setUp(self):
        self.source = ForwardSource.objects.create(
            name="parity-source",
            type="saas",
            url="https://fwd.app",
            parameters={
                "username": "user@example.com",
                "password": "secret",
                "verify": True,
                "network_id": "test-network",
            },
        )
        self.sync = ForwardSync.objects.create(
            name="parity-sync",
            source=self.source,
            parameters={
                "snapshot_id": LATEST_PROCESSED_SNAPSHOT,
                "enable_bulk_orm": True,
            },
        )

    def _runner(self, *, ingestion=None):
        return ForwardSyncRunner(
            sync=self.sync,
            ingestion=ingestion,
            client=Mock(),
            logger_=Mock(),
        )

    def _device(self, name="device-1"):
        site = Site.objects.create(name=f"{name}-site", slug=f"{name}-site")
        manufacturer = Manufacturer.objects.create(
            name=f"{name}-vendor",
            slug=f"{name}-vendor",
        )
        role = DeviceRole.objects.create(
            name=f"{name}-role",
            slug=f"{name}-role",
            color="9e9e9e",
        )
        device_type = DeviceType.objects.create(
            manufacturer=manufacturer,
            model=f"{name}-model",
            slug=f"{name}-model",
        )
        return Device.objects.create(
            name=name,
            site=site,
            role=role,
            device_type=device_type,
            status="active",
        )

    def _virtual_chassis_decision(self):
        return apply_engine_decision_for(
            sync=self.sync,
            model_string="dcim.virtualchassis",
            backend="branching",
        )

    def _virtual_chassis_engine(self):
        return select_apply_engine(
            sync=self.sync,
            model_string="dcim.virtualchassis",
            backend="branching",
        )

    def _device_decision(self):
        return apply_engine_decision_for(
            sync=self.sync,
            model_string="dcim.device",
            backend="branching",
        )

    def _prefix_decision(self):
        return apply_engine_decision_for(
            sync=self.sync,
            model_string="ipam.prefix",
            backend="branching",
        )

    def _prefix_engine(self):
        return select_apply_engine(
            sync=self.sync,
            model_string="ipam.prefix",
            backend="branching",
        )

    def _assert_device_stays_adapter(self):
        decision = self._device_decision()
        rejected_bulk = [
            rejection
            for rejection in decision.rejected_engines
            if rejection.get("engine") == "bulk_orm"
        ]

        self.assertEqual(decision.selected_engine, "adapter")
        self.assertEqual(decision.reason_code, "adapter_required_model_contract")
        self.assertEqual(rejected_bulk[0]["blocker_code"], "dependency_resolution")

    def test_dcim_virtualchassis_create_parity(self):
        device = self._device()
        runner = self._runner()

        self._virtual_chassis_engine().apply_upserts(
            runner,
            "dcim.virtualchassis",
            [
                {
                    "device": device.name,
                    "vc_name": "vc-create",
                    "vc_domain": "domain-create",
                    "vc_position": 1,
                }
            ],
        )
        device.refresh_from_db()
        vc = VirtualChassis.objects.get(name="vc-create")

        self.assertEqual(vc.name, "vc-create")
        self.assertEqual(vc.domain, "domain-create")
        self.assertEqual(device.virtual_chassis, vc)
        self.assertEqual(device.vc_position, 1)
        self.assertEqual(
            self._virtual_chassis_decision().reason_code,
            "bulk_orm_enabled_safe_model_set",
        )

    def test_dcim_virtualchassis_update_parity(self):
        runner = self._runner()
        engine = self._virtual_chassis_engine()
        engine.apply_upserts(
            runner,
            "dcim.virtualchassis",
            [{"vc_name": "vc-update", "vc_domain": "domain-old"}],
        )

        engine.apply_upserts(
            runner,
            "dcim.virtualchassis",
            [{"vc_name": "vc-update", "vc_domain": "domain-new"}],
        )
        vc = VirtualChassis.objects.get(name="vc-update")

        self.assertEqual(vc.domain, "domain-new")
        self.assertEqual(
            VirtualChassis.objects.get(name="vc-update").domain,
            "domain-new",
        )

    def test_dcim_virtualchassis_delete_parity(self):
        runner = self._runner()
        VirtualChassis.objects.create(name="vc-delete", domain="domain-delete")

        deleted = runner._delete_dcim_virtualchassis({"vc_name": "vc-delete"})

        self.assertTrue(deleted)
        self.assertFalse(VirtualChassis.objects.filter(name="vc-delete").exists())

    def test_dcim_virtualchassis_validation_failure_parity(self):
        device_1 = self._device("device-conflict-1")
        device_2 = self._device("device-conflict-2")
        ingestion = ForwardIngestion.objects.create(sync=self.sync)
        runner = self._runner(ingestion=ingestion)

        self._virtual_chassis_engine().apply_upserts(
            runner,
            "dcim.virtualchassis",
            [
                {
                    "device": device_1.name,
                    "vc_name": "vc-conflict",
                    "vc_domain": "domain-conflict",
                    "vc_position": 1,
                },
                {
                    "device": device_2.name,
                    "vc_name": "vc-conflict",
                    "vc_domain": "domain-conflict",
                    "vc_position": 1,
                },
            ],
        )

        issue = ForwardIngestionIssue.objects.get(ingestion=ingestion)
        self.assertEqual(issue.exception, "ForwardSyncDataError")
        self.assertIn("already has device `device-conflict-1`", issue.message)
        device_2.refresh_from_db()
        self.assertIsNone(device_2.virtual_chassis)
        self.assertIsNone(device_2.vc_position)

    def test_dcim_virtualchassis_row_issue_parity(self):
        ingestion = ForwardIngestion.objects.create(sync=self.sync)
        runner = self._runner(ingestion=ingestion)

        self._virtual_chassis_engine().apply_upserts(
            runner,
            "dcim.virtualchassis",
            [
                {
                    "device": "missing-device",
                    "vc_name": "vc-missing-device",
                    "vc_domain": "domain-missing-device",
                    "vc_position": 1,
                }
            ],
        )

        issue = ForwardIngestionIssue.objects.get(ingestion=ingestion)
        self.assertEqual(issue.model, "dcim.virtualchassis")
        self.assertEqual(issue.exception, "ForwardSearchError")
        self.assertIn("Unable to find device", issue.message)
        runner.logger.increment_statistics.assert_any_call(
            "dcim.virtualchassis",
            outcome="failed",
        )

    def test_dcim_virtualchassis_dependency_behavior_parity(self):
        ingestion = ForwardIngestion.objects.create(sync=self.sync)
        runner = self._runner(ingestion=ingestion)
        runner._failed_dependencies.setdefault("dcim.device", set()).add(
            ("missing-device",)
        )

        self._virtual_chassis_engine().apply_upserts(
            runner,
            "dcim.virtualchassis",
            [
                {
                    "device": "missing-device",
                    "vc_name": "vc-dependency-skip",
                    "vc_domain": "domain-dependency-skip",
                    "vc_position": 1,
                }
            ],
        )

        issue = ForwardIngestionIssue.objects.get(ingestion=ingestion)
        self.assertEqual(issue.exception, "ForwardDependencySkipError")
        self.assertIn("dependency `dcim.device` failed", issue.message)
        runner.logger.increment_statistics.assert_any_call(
            "dcim.virtualchassis",
            outcome="skipped",
        )

    def test_dcim_virtualchassis_object_change_tracking_parity(self):
        decision = self._virtual_chassis_decision()
        rejected_bulk = [
            rejection
            for rejection in decision.rejected_engines
            if rejection.get("engine") == "bulk_orm"
        ]

        self.assertEqual(decision.selected_engine, "bulk_orm")
        self.assertEqual(decision.reason_code, "bulk_orm_enabled_safe_model_set")
        self.assertEqual(rejected_bulk, [])

    def test_dcim_virtualchassis_branching_semantics_parity(self):
        expansion = bulk_orm_expansion_summary(["dcim.virtualchassis"])
        self.assertEqual(expansion["status"], "safe_set_only")
        self.assertIn("dcim.virtualchassis", expansion["safe_models"])
        self.assertEqual(expansion["parity_plan"]["candidate_count"], 0)

    def test_dcim_virtualchassis_support_bundle_statistics_parity(self):
        device = self._device("device-stats")
        runner = self._runner()

        self._virtual_chassis_engine().apply_upserts(
            runner,
            "dcim.virtualchassis",
            [
                {
                    "device": device.name,
                    "vc_name": "vc-stats",
                    "vc_domain": "domain-stats",
                    "vc_position": 1,
                },
                {
                    "device": device.name,
                    "vc_name": "vc-stats-skip",
                    "vc_domain": "domain-stats-skip",
                },
            ],
        )

        runner.logger.increment_statistics.assert_any_call(
            "dcim.virtualchassis",
            outcome="applied",
        )
        runner.logger.increment_statistics.assert_any_call(
            "dcim.virtualchassis",
            outcome="skipped",
        )

    def test_dcim_virtualchassis_runtime_non_regression(self):
        expansion = bulk_orm_expansion_summary(["dcim.virtualchassis"])
        self.assertIn("dcim.virtualchassis", expansion["safe_models"])
        self.assertEqual(self._virtual_chassis_decision().selected_engine, "bulk_orm")

    def test_dcim_device_create_parity(self):
        self._assert_device_stays_adapter()

    def test_dcim_device_update_parity(self):
        self._assert_device_stays_adapter()

    def test_dcim_device_delete_parity(self):
        self._assert_device_stays_adapter()

    def test_dcim_device_validation_failure_parity(self):
        self._assert_device_stays_adapter()

    def test_dcim_device_row_issue_parity(self):
        self._assert_device_stays_adapter()

    def test_dcim_device_dependency_behavior_parity(self):
        self._assert_device_stays_adapter()

    def test_dcim_device_object_change_tracking_parity(self):
        self._assert_device_stays_adapter()

    def test_dcim_device_branching_semantics_parity(self):
        expansion = bulk_orm_expansion_summary(["dcim.device"])
        candidate = expansion["parity_plan"]["candidates"][0]

        self.assertEqual(candidate["model"], "dcim.device")
        self.assertEqual(candidate["lane_gate"], "dependency_resolution_parity")
        self.assertIn(
            "branching_semantics_parity",
            [gate["code"] for gate in candidate["required_checklist"]],
        )

    def test_dcim_device_support_bundle_statistics_parity(self):
        self._assert_device_stays_adapter()

    def test_dcim_device_runtime_non_regression(self):
        expansion = bulk_orm_expansion_summary(["dcim.device"])
        candidate = expansion["parity_plan"]["candidates"][0]

        self.assertEqual(candidate["risk"], "high")
        self.assertEqual(self._device_decision().selected_engine, "adapter")

    def test_ipam_prefix_experimental_bulk_defaults_to_adapter(self):
        decision = self._prefix_decision()
        self.assertEqual(decision.selected_engine, "adapter")
        self.assertEqual(decision.reason_code, "adapter_required_model_contract")

    def test_ipam_prefix_experimental_bulk_selects_bulk_when_allowlisted(self):
        self.sync.parameters["bulk_orm_models"] = ["ipam.prefix"]
        self.sync.save(update_fields=["parameters"])
        decision = self._prefix_decision()
        self.assertEqual(decision.selected_engine, "adapter")
        self.assertEqual(
            decision.reason_code,
            "adapter_required_model_contract",
        )

    def test_ipam_prefix_experimental_bulk_apply_upserts_rows(self):
        self.sync.parameters["bulk_orm_models"] = ["ipam.prefix"]
        self.sync.save(update_fields=["parameters"])
        runner = self._runner()

        self._prefix_engine().apply_upserts(
            runner,
            "ipam.prefix",
            [
                {"prefix": "10.10.0.0/24", "vrf": None, "status": "active"},
                {"prefix": "10.10.1.0/24", "vrf": "blue", "status": "active"},
            ],
        )

        self.assertTrue(Prefix.objects.filter(prefix="10.10.0.0/24", vrf=None).exists())
        self.assertTrue(VRF.objects.filter(name="blue").exists())
        self.assertTrue(
            Prefix.objects.filter(prefix="10.10.1.0/24", vrf__name="blue").exists()
        )


class EventsClearerTest(TestCase):
    @patch(
        "forward_netbox.utilities.sync_events.transaction.on_commit",
        side_effect=lambda callback: callback(),
    )
    @patch("forward_netbox.utilities.sync_events.clear_events.send")
    @patch("forward_netbox.utilities.sync_events.flush_events")
    @patch("forward_netbox.utilities.sync_events.events_queue")
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


class QueryParameterCompatibilityTest(TestCase):
    def test_sync_facade_returns_empty_parameters_for_local_filter_mode(self):
        source = Mock(
            parameters={
                "device_tag_include_tags": ["Core"],
                "device_tag_filter_mode": "local",
            }
        )
        sync = Mock(source=source)

        self.assertEqual(facade_get_query_parameters(sync), {})

    def test_sync_facade_returns_parameters_for_query_parameter_mode(self):
        source = Mock(
            parameters={
                "device_tag_include_tags": ["Core", "DC"],
                "device_tag_include_match": "all",
                "device_tag_exclude_tags": ["Branch"],
                "device_tag_filter_mode": "query_parameters",
            }
        )
        sync = Mock(source=source)

        self.assertEqual(
            facade_get_query_parameters(sync),
            {
                "device_tag_include_tags": ["Core", "DC"],
                "device_tag_include_match": "all",
                "device_tag_exclude_tags": ["Branch"],
                "device_tag_exclude": "Branch",
            },
        )

    def test_query_fetch_retries_without_parameters_when_query_rejects_them(self):
        sync = Mock()
        logger = Mock()
        fetcher = ForwardQueryFetcher(sync=sync, client=Mock(), logger_=logger)
        spec = Mock(
            query="foreach device in network.devices select {name: device.name}",
            run_query_id="qid-1",
            commit_id="cid-1",
            execution_value="qid-1",
        )
        context = Mock(network_id="n1", snapshot_id="s1")
        fetcher.client.run_nqe_query.side_effect = [
            ForwardClientError(
                "Parameters were provided, but a main query does not take parameters"
            ),
            [{"name": "device-1"}],
        ]

        rows = fetcher._run_nqe_query_with_parameter_fallback(
            spec=spec,
            context=context,
            parameters={"device_tag_include": "Core"},
            fetch_all=True,
        )

        self.assertEqual(rows, [{"name": "device-1"}])
        self.assertEqual(fetcher.client.run_nqe_query.call_count, 2)
        self.assertEqual(
            fetcher.client.run_nqe_query.call_args_list[1].kwargs["parameters"], {}
        )
        logger.log_info.assert_called_once()
        logger.log_warning.assert_not_called()

    def test_query_fetch_parameter_fallback_log_is_deduped_per_query(self):
        sync = Mock()
        logger = Mock()
        fetcher = ForwardQueryFetcher(sync=sync, client=Mock(), logger_=logger)
        spec = Mock(
            query="foreach device in network.devices select {name: device.name}",
            query_name="Forward Devices",
            run_query_id="qid-1",
            commit_id="cid-1",
            execution_value="qid-1",
        )
        context = Mock(network_id="n1", snapshot_id="s1")

        fetcher.client.run_nqe_query.side_effect = [
            ForwardClientError(
                "Parameters were provided, but a main query does not take parameters"
            ),
            [{"name": "device-1"}],
            ForwardClientError(
                "Parameters were provided, but a main query does not take parameters"
            ),
            [{"name": "device-2"}],
        ]

        first = fetcher._run_nqe_query_with_parameter_fallback(
            spec=spec,
            context=context,
            parameters={"device_tag_include": "Core"},
            fetch_all=True,
        )
        second = fetcher._run_nqe_query_with_parameter_fallback(
            spec=spec,
            context=context,
            parameters={"device_tag_include": "Core"},
            fetch_all=True,
        )

        self.assertEqual(first, [{"name": "device-1"}])
        self.assertEqual(second, [{"name": "device-2"}])
        self.assertEqual(fetcher.client.run_nqe_query.call_count, 4)
        logger.log_info.assert_called_once()

    def test_diff_fetch_is_always_parameterless(self):
        sync = Mock()
        fetcher = ForwardQueryFetcher(sync=sync, client=Mock(), logger_=Mock())
        spec = Mock(run_query_id="qid-1", commit_id="cid-1", execution_value="qid-1")
        context = Mock(snapshot_id="after-s1")
        fetcher.client.run_nqe_diff.return_value = [
            {"changeType": "ADD", "data": {"name": "device-1"}}
        ]

        rows = fetcher._run_nqe_diff_without_parameters(
            spec=spec,
            context=context,
            before_snapshot_id="before-s1",
            column_filters=None,
        )

        self.assertEqual(rows, [{"changeType": "ADD", "data": {"name": "device-1"}}])
        self.assertEqual(fetcher.client.run_nqe_diff.call_count, 1)
        self.assertEqual(
            fetcher.client.run_nqe_diff.call_args_list[0].kwargs["parameters"], {}
        )

    def test_query_fetch_worker_count_defaults_to_fast_bootstrap_max(self):
        sync = Mock(
            parameters={"execution_backend": "fast_bootstrap"},
            source=Mock(parameters={}),
        )
        fetcher = ForwardQueryFetcher(sync=sync, client=Mock(), logger_=Mock())

        worker_count = fetcher._query_fetch_worker_count(32)

        self.assertEqual(worker_count, 16)

    def test_query_fetch_worker_count_honors_source_override(self):
        sync = Mock(
            parameters={"execution_backend": "fast_bootstrap"},
            source=Mock(parameters={"query_fetch_concurrency": 6}),
        )
        fetcher = ForwardQueryFetcher(sync=sync, client=Mock(), logger_=Mock())

        worker_count = fetcher._query_fetch_worker_count(32)

        self.assertEqual(worker_count, 6)


class SchedulerOverlapPolicyTest(TestCase):
    def _sync_with_run(self, *, scheduler_overlap=True, auto_merge=True):
        source = ForwardSource.objects.create(
            name="scheduler-overlap-source",
            type="saas",
            url="https://fwd.app",
            parameters={
                "username": "user@example.com",
                "password": "secret",
                "verify": True,
                "network_id": "test-network",
            },
        )
        sync = ForwardSync.objects.create(
            name="scheduler-overlap-sync",
            source=source,
            auto_merge=auto_merge,
            parameters={
                "snapshot_id": "latestProcessed",
                "execution_backend": "branching",
                "scheduler_overlap": scheduler_overlap,
            },
        )
        run = ForwardExecutionRun.objects.create(
            sync=sync,
            source=source,
            backend="branching",
            status="running",
            auto_merge=auto_merge,
            snapshot_selector="latestProcessed",
            snapshot_id="snapshot-overlap",
            total_steps=3,
            next_step_index=1,
        )
        return sync, run

    def test_scheduler_overlap_defaults_on_for_branching_auto_merge_when_unset(self):
        sync = Mock(
            auto_merge=True,
            parameters={"execution_backend": "branching"},
        )
        self.assertTrue(scheduler_overlap_enabled(sync))

    def test_scheduler_overlap_respects_explicit_false(self):
        sync = Mock(
            auto_merge=True,
            parameters={
                "execution_backend": "branching",
                "scheduler_overlap": False,
            },
        )
        self.assertFalse(scheduler_overlap_enabled(sync))

    def test_scheduler_overlap_requires_auto_merge_even_if_unset(self):
        sync = Mock(
            auto_merge=False,
            parameters={"execution_backend": "branching"},
        )
        self.assertFalse(scheduler_overlap_enabled(sync))

    def test_scheduler_overlap_enqueue_noops_when_disabled(self):
        sync, run = self._sync_with_run(scheduler_overlap=False)
        ForwardExecutionStep.objects.create(
            run=run,
            index=2,
            status=ForwardExecutionStepStatusChoices.PENDING,
            model_string="dcim.site",
        )
        executor = Mock(sync=sync, user=None, logger=Mock())

        with patch(
            "forward_netbox.utilities.multi_branch_lifecycle.enqueue_branch_stage_job"
        ) as enqueue:
            result = maybe_enqueue_overlap_stage(
                executor,
                Mock(index=1),
                total_plan_items=3,
            )

        self.assertIsNone(result)
        enqueue.assert_not_called()

    def test_scheduler_overlap_avoids_duplicate_future_stage_worker(self):
        sync, run = self._sync_with_run(scheduler_overlap=True)
        ForwardExecutionStep.objects.create(
            run=run,
            index=2,
            status=ForwardExecutionStepStatusChoices.PENDING,
            model_string="dcim.site",
        )
        ForwardExecutionStep.objects.create(
            run=run,
            index=3,
            status=ForwardExecutionStepStatusChoices.QUEUED,
            model_string="dcim.device",
        )
        executor = Mock(sync=sync, user=None, logger=Mock())

        with patch(
            "forward_netbox.utilities.multi_branch_lifecycle.enqueue_branch_stage_job"
        ) as enqueue:
            result = maybe_enqueue_overlap_stage(
                executor,
                Mock(index=1),
                total_plan_items=3,
            )

        self.assertIsNone(result)
        enqueue.assert_not_called()

    def test_scheduler_overlap_enqueues_only_next_stage_and_keeps_merge_serialized(
        self,
    ):
        sync, run = self._sync_with_run(scheduler_overlap=True)
        ForwardExecutionStep.objects.create(
            run=run,
            index=2,
            status=ForwardExecutionStepStatusChoices.PENDING,
            model_string="dcim.site",
        )
        executor = Mock(sync=sync, user=None, logger=Mock())
        queued_job = Mock()

        with patch(
            "forward_netbox.utilities.multi_branch_lifecycle.enqueue_branch_stage_job",
            return_value=queued_job,
        ) as enqueue:
            result = maybe_enqueue_overlap_stage(
                executor,
                Mock(index=1),
                total_plan_items=3,
            )

        self.assertEqual(result, queued_job)
        enqueue.assert_called_once_with(
            sync,
            user=None,
            adhoc=True,
            overlap_stage=True,
        )
        executor.logger.log_info.assert_called_once()
        self.assertIn(
            "merge remains serialized",
            executor.logger.log_info.call_args.args[0],
        )
