import json
from unittest.mock import Mock
from unittest.mock import patch

from core.models import ObjectChange
from dcim.models import Cable
from dcim.models import Device
from dcim.models import DeviceRole
from dcim.models import DeviceType
from dcim.models import Interface
from dcim.models import InventoryItem
from dcim.models import InventoryItemRole
from dcim.models import MACAddress
from dcim.models import Manufacturer
from dcim.models import Module
from dcim.models import Platform
from dcim.models import Site
from dcim.models import VirtualChassis
from dcim.models.device_components import ModuleBay
from dcim.models.modules import ModuleType
from django.apps import apps
from django.contrib.contenttypes.models import ContentType
from django.core.exceptions import ValidationError
from django.db import connection
from django.db import IntegrityError
from django.db.models.deletion import ProtectedError
from django.test import TestCase
from django.test.utils import CaptureQueriesContext
from django.utils import timezone
from extras.models import Tag
from ipam.models import ASN
from ipam.models import FHRPGroup
from ipam.models import FHRPGroupAssignment
from ipam.models import IPAddress
from ipam.models import Prefix
from ipam.models import RIR
from ipam.models import VLAN
from ipam.models import VRF

from forward_netbox.choices import FORWARD_SUPPORTED_MODELS
from forward_netbox.choices import ForwardDiffFallbackModeChoices
from forward_netbox.exceptions import ForwardClientError
from forward_netbox.exceptions import ForwardDependencySkipError
from forward_netbox.exceptions import ForwardQueryError
from forward_netbox.exceptions import ForwardSearchError
from forward_netbox.exceptions import ForwardSyncDataError
from forward_netbox.models import ForwardIngestion
from forward_netbox.models import ForwardIngestionIssue
from forward_netbox.models import ForwardSource
from forward_netbox.models import ForwardSync
from forward_netbox.signals import seed_builtin_nqe_maps
from forward_netbox.utilities.apply_engine import ADAPTER_MODELS_WITHOUT_BLOCKER
from forward_netbox.utilities.apply_engine import ADAPTER_REQUIRED_MODELS
from forward_netbox.utilities.apply_engine import apply_engine_decision_for
from forward_netbox.utilities.apply_engine import APPLY_ENGINE_MODEL_CLASSIFICATIONS
from forward_netbox.utilities.apply_engine import BULK_ORM_ENABLED_MODELS
from forward_netbox.utilities.apply_engine import BULK_ORM_ENABLED_MODELS_WITHOUT_SPECS
from forward_netbox.utilities.apply_engine import select_apply_engine
from forward_netbox.utilities.apply_engine import UNCLASSIFIED_SUPPORTED_MODELS
from forward_netbox.utilities.apply_engine_bulk import (
    bulk_orm_apply_tree_models,
)
from forward_netbox.utilities.branch_budget import APPLY_DEPENDENCY_MODEL_RANK
from forward_netbox.utilities.branch_budget import apply_parent_dependency_contracts
from forward_netbox.utilities.branch_budget import branch_budget_density_policy_summary
from forward_netbox.utilities.branch_budget import BranchWorkload
from forward_netbox.utilities.branch_budget import build_branch_plan
from forward_netbox.utilities.branch_budget import effective_row_budget_for_model
from forward_netbox.utilities.branch_budget import row_shard_key
from forward_netbox.utilities.branch_budget import shard_fetch_capability_for_model
from forward_netbox.utilities.branch_budget import shard_fetch_contract
from forward_netbox.utilities.branch_budget import SHARD_FETCH_MODEL_CONTRACTS
from forward_netbox.utilities.execution_telemetry import build_plan_preview
from forward_netbox.utilities.forward_api import DEFAULT_QUERY_FETCH_CONCURRENCY
from forward_netbox.utilities.forward_api import LATEST_PROCESSED_SNAPSHOT
from forward_netbox.utilities.query_diagnostics import (
    summarize_ipaddress_parent_prefix_rows,
)
from forward_netbox.utilities.query_fetch import ForwardQueryContext
from forward_netbox.utilities.query_fetch import ForwardQueryFetcher
from forward_netbox.utilities.query_registry import QuerySpec
from forward_netbox.utilities.sync import ForwardSyncRunner
from forward_netbox.utilities.sync_contracts import validate_row_shape_for_model
from forward_netbox.utilities.sync_events import EventsClearer
from forward_netbox.utilities.sync_facade import (
    get_query_parameters as facade_get_query_parameters,
)
from forward_netbox.utilities.sync_primitives import delete_by_coalesce
from forward_netbox.utilities.sync_primitives import get_unique_or_raise
from forward_netbox.utilities.sync_primitives import prime_dependency_lookup_caches
from forward_netbox.utilities.sync_routing_impl import lookup_ipaddress_by_host
from forward_netbox.utilities.sync_routing_impl import lookup_routing_interface_name


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

    def test_cable_shard_fetch_contract_parameters_by_canonical_device(self):
        contract = shard_fetch_contract(
            "dcim.cable",
            [
                "cable:device-a:Ethernet1/1|device-b:Ethernet1/2",
                "cable:device-c:Ethernet1/3|device-d:Ethernet1/4",
            ],
        )

        self.assertEqual(contract["fetch_mode"], "nqe_parameters")
        self.assertEqual(contract["fetch_key_family"], "device")
        self.assertEqual(
            contract["fetch_parameters"],
            {"forward_netbox_shard_keys": ["device-a", "device-c"]},
        )

    def test_effective_row_budget_uses_baseline_for_unprofiled_density(self):
        budget = effective_row_budget_for_model(
            "dcim.device",
            max_changes_per_staging_item=10000,
            model_change_density={"dcim.device": 5.0},
        )

        self.assertEqual(budget, 7000)

    def test_effective_row_budget_uses_cable_default_density_and_safety(self):
        budget = effective_row_budget_for_model(
            "dcim.cable",
            max_changes_per_staging_item=10000,
            model_change_density={},
        )

        self.assertEqual(budget, 1666)

    def test_effective_row_budget_uses_module_default_density(self):
        budget = effective_row_budget_for_model(
            "dcim.module",
            max_changes_per_staging_item=10000,
            model_change_density={},
        )

        self.assertEqual(budget, 3500)

    def test_effective_row_budget_uses_bgp_peer_default_density(self):
        budget = effective_row_budget_for_model(
            "netbox_routing.bgppeer",
            max_changes_per_staging_item=10000,
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

    def test_ipam_prefix_shard_fetch_contract_parameters_by_prefix(self):
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

    def test_ipam_prefix_global_shard_key_preserves_parameterized_fetch(self):
        shard_key = row_shard_key(
            "ipam.prefix",
            {"prefix": "192.0.2.0/27", "vrf": None, "status": "active"},
            [["prefix", "vrf"]],
        )
        contract = shard_fetch_contract("ipam.prefix", [shard_key])

        self.assertEqual(shard_key, "prefix=192.0.2.0/27|vrf=<global>")
        self.assertEqual(contract["fetch_mode"], "nqe_parameters")
        self.assertEqual(
            contract["fetch_parameters"],
            {"forward_netbox_shard_keys": ["192.0.2.0/27"]},
        )

    def test_ipam_vlan_shard_fetch_contract_parameters_by_vid(self):
        contract = shard_fetch_contract(
            "ipam.vlan",
            ["site=site-a|vid=10", "site=site-b|vid=20"],
        )

        self.assertEqual(contract["fetch_mode"], "nqe_parameters")
        self.assertEqual(contract["fetch_key_family"], "vid")
        self.assertEqual(
            contract["fetch_parameters"],
            {"forward_netbox_shard_keys": ["10", "20"]},
        )

    def test_ipam_vrf_shard_fetch_contract_parameters_by_name_when_rd_absent(self):
        contract = shard_fetch_contract(
            "ipam.vrf",
            ["name=blue", "name=red"],
        )

        self.assertEqual(contract["fetch_mode"], "nqe_parameters")
        self.assertEqual(contract["fetch_key_family"], "name")
        self.assertEqual(
            contract["fetch_parameters"],
            {"forward_netbox_shard_keys": ["blue", "red"]},
        )

    def test_dcim_device_shard_fetch_contract_parameters_by_name(self):
        contract = shard_fetch_contract(
            "dcim.device",
            ["name=device-1", "name=device-2"],
        )

        self.assertEqual(contract["fetch_mode"], "nqe_parameters")
        self.assertEqual(contract["fetch_key_family"], "name")
        self.assertEqual(
            contract["fetch_parameters"],
            {"forward_netbox_shard_keys": ["device-1", "device-2"]},
        )

    def test_device_shard_fetch_contract_uses_query_parameters(self):
        contract = shard_fetch_contract(
            "dcim.interface",
            ["device:device-1", "device:device-2"],
        )

        self.assertEqual(contract["query_parameters"], {})
        self.assertEqual(
            contract["fetch_parameters"],
            {"forward_netbox_shard_keys": ["device-1", "device-2"]},
        )

    def test_shard_fetch_capability_reports_model_fallbacks(self):
        device_contract = shard_fetch_capability_for_model("dcim.interface")
        prefix_contract = shard_fetch_capability_for_model("ipam.prefix")
        site_contract = shard_fetch_capability_for_model("dcim.site")

        self.assertEqual(device_contract["fetch_mode"], "nqe_parameters")
        self.assertEqual(device_contract["reason_code"], "device_query_parameter")
        self.assertTrue(device_contract["shard_safe"])
        self.assertEqual(prefix_contract["reason_code"], "ipam_prefix_query_parameter")
        self.assertTrue(prefix_contract["shard_safe"])
        self.assertEqual(site_contract["fetch_mode"], "nqe_parameters")
        self.assertEqual(site_contract["reason_code"], "structured_query_parameter")
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
                {"nqe_parameters", "model"},
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

    def test_unprofiled_density_uses_conservative_cable_baseline(
        self,
    ):
        budget = effective_row_budget_for_model(
            "dcim.cable",
            max_changes_per_staging_item=10000,
            model_change_density={"dcim.cable": 2.0},
        )

        self.assertEqual(budget, 1666)

    def test_low_confidence_density_does_not_drive_budget_auto_tuning(self):
        budget = effective_row_budget_for_model(
            "dcim.device",
            max_changes_per_staging_item=10000,
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
            max_changes_per_staging_item=10000,
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
            max_changes_per_staging_item=10000,
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
            max_changes_per_staging_item=10000,
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
            max_changes_per_staging_item=10000,
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

    def test_apply_parent_dependency_contracts_are_ranked_before_children(self):
        missing_ranks = []
        inverted_ranks = []
        for child_model, parent_models in apply_parent_dependency_contracts().items():
            child_rank = APPLY_DEPENDENCY_MODEL_RANK.get(child_model)
            if child_rank is None:
                missing_ranks.append(child_model)
                continue
            for parent_model in parent_models:
                parent_rank = APPLY_DEPENDENCY_MODEL_RANK.get(parent_model)
                if parent_rank is None:
                    missing_ranks.append(parent_model)
                    continue
                if parent_rank >= child_rank:
                    inverted_ranks.append((parent_model, child_model))

        self.assertEqual(missing_ranks, [])
        self.assertEqual(inverted_ranks, [])

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
            ]
        )

        preview = build_plan_preview(plan, max_changes_per_staging_item=10000)

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
            ]
        )

        self.assertEqual([item.operation for item in plan], ["apply", "delete"])
        self.assertEqual(len(plan[0].upsert_rows), 1)
        self.assertEqual(plan[0].delete_rows, [])
        self.assertEqual(plan[1].upsert_rows, [])
        self.assertEqual(len(plan[1].delete_rows), 1)


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

    def _update_statements(self, queries):
        return [
            query["sql"]
            for query in queries
            if query["sql"].lstrip().upper().startswith("UPDATE ")
        ]

    def test_lookup_interface_matches_canonical_form(self):
        # Forward reports abbreviated names; the lookup matches them to an existing
        # canonical-form interface so the sync never creates a duplicate.
        device = self._create_device("device-1")
        gig = Interface.objects.create(
            device=device,
            name="GigabitEthernet0/0/2",
            type="1000base-t",
        )
        runner = ForwardSyncRunner(
            sync=self.sync, ingestion=None, client=None, logger_=Mock()
        )

        # Exact, abbreviated, and case variants all resolve to the same interface.
        self.assertEqual(runner._lookup_interface(device, "GigabitEthernet0/0/2"), gig)
        self.assertEqual(runner._lookup_interface(device, "gi0/0/2"), gig)
        self.assertEqual(runner._lookup_interface(device, "gigabitethernet0/0/2"), gig)
        # A genuinely different interface does not match.
        self.assertIsNone(runner._lookup_interface(device, "gi0/0/3"))

    def test_device_lookup_cache_reuses_positive_lookup(self):
        device = self._create_device("device-1")
        runner = ForwardSyncRunner(
            sync=self.sync, ingestion=None, client=None, logger_=Mock()
        )

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

    def test_device_lookup_cache_reuses_negative_lookup_for_optional_get(self):
        runner = ForwardSyncRunner(
            sync=self.sync, ingestion=None, client=None, logger_=Mock()
        )

        with CaptureQueriesContext(connection) as queries:
            self.assertIsNone(runner._lookup_device_by_name("device-404"))
            self.assertIsNone(runner._lookup_device_by_name("device-404"))

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

        # First miss: exact lookup + one-time per-device canonical map build (2).
        # Second miss: served from the negative cache (0). The canonical map is
        # cached per device, so it is not rebuilt.
        self.assertEqual(len(queries), 2)

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

    def test_routing_interface_alias_lookup_reuses_cache_after_first_resolution(self):
        device = self._create_device("device-1")
        interface = Interface.objects.create(
            device=device,
            name="GigabitEthernet0/0/2",
            type="1000base-t",
        )
        runner = ForwardSyncRunner(
            sync=self.sync, ingestion=None, client=None, logger_=Mock()
        )

        first = lookup_routing_interface_name(runner, device, "gi0/0/2")
        with CaptureQueriesContext(connection) as queries:
            second = lookup_routing_interface_name(runner, device, "gi0/0/2")

        self.assertEqual(first, interface)
        self.assertEqual(second, interface)
        self.assertEqual(len(queries), 0)

    def test_dependency_lookup_cache_primes_routing_interface_alias_candidates(self):
        device = self._create_device("device-ospf-alias")
        interface = Interface.objects.create(
            device=device,
            name="GigabitEthernet0/0/2",
            type="1000base-t",
        )
        runner = ForwardSyncRunner(
            sync=self.sync, ingestion=None, client=None, logger_=Mock()
        )

        with CaptureQueriesContext(connection) as prime_queries:
            summary = prime_dependency_lookup_caches(
                runner,
                "netbox_routing.ospfinterface",
                [
                    {
                        "device": device.name,
                        "local_interface": "gi0/0/2",
                    }
                ],
            )

        self.assertEqual(summary["routing_interface_alias_count"], 2)
        self.assertGreaterEqual(len(prime_queries), 1)

        with CaptureQueriesContext(connection) as lookup_queries:
            resolved = lookup_routing_interface_name(runner, device, "gi0/0/2")

        self.assertEqual(resolved, interface)
        self.assertEqual(len(lookup_queries), 0)

    def test_dependency_lookup_cache_skips_optional_plugin_priming_failure(self):
        runner = ForwardSyncRunner(
            sync=self.sync, ingestion=None, client=None, logger_=Mock()
        )
        runner._optional_model = Mock(
            side_effect=ForwardQueryError("optional plugin missing")
        )

        summary = prime_dependency_lookup_caches(
            runner,
            "netbox_routing.bgppeer",
            [
                {
                    "device": "device-1",
                    "vrf": None,
                    "local_asn": 64512,
                    "neighbor_address": "192.0.2.1",
                    "peer_asn": 64513,
                }
            ],
        )

        self.assertEqual(summary["routing_interface_alias_count"], 0)
        self.assertNotIn("routing_bgp_router_count", summary)
        self.assertNotIn("routing_bgp_scope_count", summary)

    def test_lookup_ipaddress_by_host_reuses_vrf_scoped_cache_after_first_resolution(
        self,
    ):
        vrf = VRF.objects.create(name="blue", rd="64512:1")
        ip_address = IPAddress.objects.create(
            address="192.0.2.1/24",
            vrf=vrf,
            status="active",
        )
        runner = ForwardSyncRunner(
            sync=self.sync, ingestion=None, client=None, logger_=Mock()
        )

        first = lookup_ipaddress_by_host(runner, address="192.0.2.1", vrf=vrf)
        with CaptureQueriesContext(connection) as queries:
            second = lookup_ipaddress_by_host(runner, address="192.0.2.1", vrf=vrf)

        self.assertEqual(first, ip_address)
        self.assertEqual(second, ip_address)
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
            summary = prime_dependency_lookup_caches(
                runner,
                "dcim.interface",
                [
                    {"device": "device-1", "name": "Ethernet1/1"},
                    {"device": "device-1", "name": "Ethernet1/2"},
                ],
            )

        self.assertEqual(len(queries), 2)
        self.assertEqual(summary["model"], "dcim.interface")
        self.assertEqual(summary["row_count"], 2)
        self.assertEqual(summary["device_name_count"], 1)
        self.assertEqual(summary["interface_pair_count"], 2)
        self.assertTrue(summary["available"])
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
        # Platform identity remains global even when ownership is unambiguous.
        platform = Platform.objects.create(name="ios-xe", slug="ios-xe")
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
                runner._ensure_platform({"name": "ios-xe", "slug": "ios-xe"}),
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

    def test_apply_dcim_site_repeat_sync_is_noop(self):
        Site.objects.create(name="site-1", slug="site-1")
        runner = ForwardSyncRunner(
            sync=self.sync, ingestion=None, client=None, logger_=Mock()
        )
        row = {"name": "site-1", "slug": "site-1"}

        before_count = ObjectChange.objects.count()
        with CaptureQueriesContext(connection) as queries:
            runner._apply_dcim_site(row)
            runner._apply_dcim_site(row)

        self.assertEqual(Site.objects.filter(slug="site-1").count(), 1)
        self.assertEqual(self._update_statements(queries), [])
        self.assertEqual(ObjectChange.objects.count(), before_count)

    def test_apply_dcim_platform_repeat_sync_is_noop(self):
        # An ambiguous platform row has no manufacturer; re-applying it is a no-op.
        Platform.objects.create(name="platform-1", slug="platform-1")
        runner = ForwardSyncRunner(
            sync=self.sync, ingestion=None, client=None, logger_=Mock()
        )
        row = {"name": "platform-1", "slug": "platform-1"}

        before_count = ObjectChange.objects.count()
        with CaptureQueriesContext(connection) as queries:
            runner._apply_dcim_platform(row)
            runner._apply_dcim_platform(row)

        self.assertEqual(Platform.objects.filter(slug="platform-1").count(), 1)
        self.assertEqual(self._update_statements(queries), [])
        self.assertEqual(ObjectChange.objects.count(), before_count)

    def test_apply_dcim_platform_repeat_sync_is_noop_for_aci_platform(self):
        # Re-applying an identical Platform map row makes no UPDATE.
        Platform.objects.create(name="ACI", slug="aci")
        runner = ForwardSyncRunner(
            sync=self.sync, ingestion=None, client=None, logger_=Mock()
        )
        row = {"name": "ACI", "slug": "aci"}

        before_count = ObjectChange.objects.count()
        with CaptureQueriesContext(connection) as queries:
            runner._apply_dcim_platform(row)
            runner._apply_dcim_platform(row)

        self.assertEqual(Platform.objects.filter(slug="aci").count(), 1)
        self.assertEqual(self._update_statements(queries), [])
        self.assertEqual(ObjectChange.objects.count(), before_count)

    def test_apply_platform_assigns_unambiguous_manufacturer(self):
        cisco = Manufacturer.objects.create(name="Cisco", slug="cisco")
        platform = Platform.objects.create(
            name="ACI",
            slug="aci",
        )
        runner = ForwardSyncRunner(
            sync=self.sync, ingestion=None, client=None, logger_=Mock()
        )
        row = {
            "name": "ACI",
            "slug": "aci",
            "manufacturer": "Cisco",
            "manufacturer_slug": "cisco",
        }

        runner._apply_dcim_platform(row)

        platform.refresh_from_db()
        self.assertEqual(platform.manufacturer_id, cisco.pk)

    def test_apply_platform_clears_manufacturer_when_platform_is_ambiguous(self):
        cisco = Manufacturer.objects.create(name="Cisco", slug="cisco")
        platform = Platform.objects.create(
            name="Linux",
            slug="linux",
            manufacturer=cisco,
        )
        runner = ForwardSyncRunner(
            sync=self.sync, ingestion=None, client=None, logger_=Mock()
        )

        runner._apply_dcim_platform(
            {
                "name": "Linux",
                "slug": "linux",
                "manufacturer": None,
                "manufacturer_slug": None,
            }
        )

        platform.refresh_from_db()
        self.assertIsNone(platform.manufacturer_id)

    def test_device_platform_ensure_preserves_platform_map_manufacturer(self):
        cisco = Manufacturer.objects.create(name="Cisco", slug="cisco")
        Manufacturer.objects.create(name="Unknown", slug="unknown")
        platform = Platform.objects.create(
            name="IOS_XE",
            slug="ios-xe",
            manufacturer=cisco,
        )
        runner = ForwardSyncRunner(
            sync=self.sync, ingestion=None, client=None, logger_=Mock()
        )

        result = runner._ensure_platform(
            {
                "name": "IOS_XE",
                "slug": "ios-xe",
                "manufacturer": "Unknown",
                "manufacturer_slug": "unknown",
            }
        )

        platform.refresh_from_db()
        self.assertEqual(result.pk, platform.pk)
        self.assertEqual(platform.manufacturer_id, cisco.pk)

    def test_device_platform_ensure_sets_manufacturer_on_endpoint_only_create(self):
        opengear = Manufacturer.objects.create(name="Opengear", slug="opengear")
        runner = ForwardSyncRunner(
            sync=self.sync, ingestion=None, client=None, logger_=Mock()
        )

        platform = runner._ensure_platform(
            {
                "name": "Opengear",
                "slug": "opengear",
                "manufacturer": "Opengear",
                "manufacturer_slug": "opengear",
            },
            manufacturer_authoritative=True,
        )

        self.assertEqual(platform.manufacturer_id, opengear.pk)

    def test_device_platform_ensure_does_not_infer_manufacturer_on_create(self):
        Manufacturer.objects.create(name="Cisco", slug="cisco")
        runner = ForwardSyncRunner(
            sync=self.sync, ingestion=None, client=None, logger_=Mock()
        )

        platform = runner._ensure_platform(
            {
                "name": "IOS_XE",
                "slug": "ios-xe",
                "manufacturer": "Cisco",
                "manufacturer_slug": "cisco",
            }
        )

        self.assertIsNone(platform.manufacturer_id)

    def test_apply_dcim_manufacturer_repeat_sync_is_noop(self):
        Manufacturer.objects.create(name="vendor-2", slug="vendor-2")
        runner = ForwardSyncRunner(
            sync=self.sync, ingestion=None, client=None, logger_=Mock()
        )
        row = {"name": "vendor-2", "slug": "vendor-2"}

        before_count = ObjectChange.objects.count()
        with CaptureQueriesContext(connection) as queries:
            runner._apply_dcim_manufacturer(row)
            runner._apply_dcim_manufacturer(row)

        self.assertEqual(Manufacturer.objects.filter(slug="vendor-2").count(), 1)
        self.assertEqual(self._update_statements(queries), [])
        self.assertEqual(ObjectChange.objects.count(), before_count)

    def test_apply_dcim_devicerole_repeat_sync_is_noop(self):
        DeviceRole.objects.create(name="role-1", slug="role-1", color="9e9e9e")
        runner = ForwardSyncRunner(
            sync=self.sync, ingestion=None, client=None, logger_=Mock()
        )
        row = {"name": "role-1", "slug": "role-1", "color": "9e9e9e"}

        before_count = ObjectChange.objects.count()
        with CaptureQueriesContext(connection) as queries:
            runner._apply_dcim_devicerole(row)
            runner._apply_dcim_devicerole(row)

        self.assertEqual(DeviceRole.objects.filter(slug="role-1").count(), 1)
        self.assertEqual(self._update_statements(queries), [])
        self.assertEqual(ObjectChange.objects.count(), before_count)

    def test_apply_dcim_devicetype_repeat_sync_is_noop(self):
        manufacturer = Manufacturer.objects.create(name="vendor-3", slug="vendor-3")
        DeviceType.objects.create(
            manufacturer=manufacturer,
            model="model-1",
            slug="model-1",
        )
        runner = ForwardSyncRunner(
            sync=self.sync, ingestion=None, client=None, logger_=Mock()
        )
        row = {
            "manufacturer": "vendor-3",
            "manufacturer_slug": "vendor-3",
            "model": "model-1",
            "slug": "model-1",
        }

        before_count = ObjectChange.objects.count()
        with CaptureQueriesContext(connection) as queries:
            runner._apply_dcim_devicetype(row)
            runner._apply_dcim_devicetype(row)

        self.assertEqual(
            DeviceType.objects.filter(
                manufacturer=manufacturer, slug="model-1"
            ).count(),
            1,
        )
        self.assertEqual(self._update_statements(queries), [])
        self.assertEqual(ObjectChange.objects.count(), before_count)

    def test_apply_extras_taggeditem_repeat_sync_is_noop(self):
        device = self._create_device("device-tag-noop")
        Tag.objects.create(name="feature", slug="feature", color="9e9e9e")
        runner = ForwardSyncRunner(
            sync=self.sync, ingestion=None, client=None, logger_=Mock()
        )
        row = {
            "device": device.name,
            "tag": "feature",
            "tag_slug": "feature",
            "tag_color": "9e9e9e",
        }

        before_count = ObjectChange.objects.count()
        with CaptureQueriesContext(connection) as queries:
            runner._apply_extras_taggeditem(row)
            runner._apply_extras_taggeditem(row)

        self.assertEqual(device.tags.filter(slug="feature").count(), 1)
        self.assertEqual(self._update_statements(queries), [])
        self.assertEqual(ObjectChange.objects.count(), before_count)

    def test_ensure_platform_reuses_cache_after_first_resolution(self):
        runner = ForwardSyncRunner(
            sync=self.sync, ingestion=None, client=None, logger_=Mock()
        )
        row = {
            "name": "platform-2",
            "slug": "platform-2",
            "manufacturer": "vendor-2",
            "manufacturer_slug": "vendor-2",
        }

        first = runner._ensure_platform(row)
        with CaptureQueriesContext(connection) as queries:
            second = runner._ensure_platform(row)

        self.assertEqual(first.pk, second.pk)
        self.assertEqual(len(queries), 0)

    def test_ensure_module_type_reuses_cache_after_first_resolution(self):
        manufacturer = Manufacturer.objects.create(name="vendor-5", slug="vendor-5")
        module_type = ModuleType.objects.create(
            manufacturer=manufacturer,
            model="Line Card 5",
            part_number="LC-5",
            description="",
            comments="",
        )
        runner = ForwardSyncRunner(
            sync=self.sync, ingestion=None, client=None, logger_=Mock()
        )
        row = {
            "manufacturer": "vendor-5",
            "manufacturer_slug": "vendor-5",
            "model": "Line Card 5",
            "part_number": "LC-5",
            "description": "",
            "comments": "",
        }

        first = runner._ensure_module_type(row)
        with CaptureQueriesContext(connection) as queries:
            second = runner._ensure_module_type(row)

        self.assertEqual(first.pk, module_type.pk)
        self.assertEqual(second.pk, module_type.pk)
        self.assertEqual(len(queries), 0)

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

    def test_dependency_lookup_cache_primes_ipam_vlan_site_identity(self):
        site = Site.objects.create(name="site-1", slug="site-1")
        vlan = VLAN.objects.create(site=site, vid=10, name="VLAN10", status="active")
        runner = ForwardSyncRunner(
            sync=self.sync, ingestion=None, client=None, logger_=Mock()
        )

        with CaptureQueriesContext(connection) as queries:
            summary = prime_dependency_lookup_caches(
                runner,
                "ipam.vlan",
                [
                    {
                        "site": "site-1",
                        "vid": 10,
                        "name": "VLAN10",
                        "status": "active",
                    }
                ],
            )

        self.assertEqual(len(queries), 2)
        self.assertEqual(summary["vlan_pair_count"], 1)
        with CaptureQueriesContext(connection) as cached_queries:
            self.assertEqual(
                get_unique_or_raise(runner, VLAN, {"site": site, "vid": 10}),
                vlan,
            )

        self.assertEqual(len(cached_queries), 0)

    def test_dependency_lookup_cache_primes_ipam_fhrpgroup_identity(self):
        VRF.objects.create(name="blue", rd="64512:3")
        group_name = "hsrp-10-blue-10.0.0.1"
        group = FHRPGroup.objects.create(
            protocol="hsrp",
            group_id=10,
            name=group_name,
        )
        runner = ForwardSyncRunner(
            sync=self.sync, ingestion=None, client=None, logger_=Mock()
        )

        with CaptureQueriesContext(connection) as queries:
            summary = prime_dependency_lookup_caches(
                runner,
                "ipam.fhrpgroup",
                [
                    {
                        "device": "device-1",
                        "interface": "Vlan10",
                        "protocol": "hsrp",
                        "group_id": 10,
                        "address": "10.0.0.1/24",
                        "vrf": "blue",
                        "state": "active",
                    }
                ],
            )

        self.assertGreaterEqual(len(queries), 2)
        self.assertEqual(summary["fhrp_group_count"], 1)
        with CaptureQueriesContext(connection) as cached_queries:
            self.assertEqual(
                get_unique_or_raise(
                    runner,
                    FHRPGroup,
                    {"protocol": "hsrp", "group_id": 10, "name": group_name},
                ),
                group,
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

    def test_apply_dcim_module_creates_all_missing_module_bays(self):
        device = self._create_device("device-module-readiness")
        logger = Mock()
        runner = ForwardSyncRunner(
            sync=self.sync, ingestion=None, client=None, logger_=logger
        )
        rows = [
            {
                "device": "device-module-readiness",
                "module_bay": f"module {index}",
                "manufacturer": "vendor-1",
                "manufacturer_slug": "vendor-1",
                "model": f"Line Card {index}",
                "part_number": f"LC-{index}",
                "status": "active",
            }
            for index in range(7)
        ]

        runner._apply_model_rows("dcim.module", rows)

        self.assertEqual(ModuleBay.objects.filter(device=device).count(), 7)
        self.assertEqual(Module.objects.filter(device=device).count(), 7)
        self.assertEqual(logger.log_warning.call_count, 0)

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

    def test_module_bay_lookup_cache_reuses_positive_lookup(self):
        device = self._create_device("device-module-hit")
        module_bay = self._create_module_bay(device, name="Slot 1")
        runner = ForwardSyncRunner(
            sync=self.sync, ingestion=None, client=None, logger_=Mock()
        )

        with CaptureQueriesContext(connection) as queries:
            self.assertEqual(runner._lookup_module_bay(device, "Slot 1"), module_bay)
            self.assertEqual(runner._lookup_module_bay(device, "Slot 1"), module_bay)

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
        self._create_device("device-1")
        runner = ForwardSyncRunner(
            sync=self.sync, ingestion=None, client=None, logger_=Mock()
        )
        runner._optional_model = Mock(
            side_effect=ForwardQueryError("netbox-routing is unavailable")
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

        self.assertEqual(get_unique_or_raise(runner, BGPRouter, lookup), router)
        with CaptureQueriesContext(connection) as queries:
            self.assertEqual(get_unique_or_raise(runner, BGPRouter, lookup), router)

        self.assertEqual(len(queries), 0)

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

    def test_bgp_scope_reuses_positive_cache_after_first_resolution(self):
        if not apps.is_installed("netbox_routing"):
            self.skipTest("netbox-routing optional plugin is not installed")
        BGPRouter = apps.get_model("netbox_routing", "BGPRouter")
        BGPScope = apps.get_model("netbox_routing", "BGPScope")
        device = self._create_device("device-bgp-scope-cache")
        asn = ASN.objects.create(rir=RIR.objects.create(name="ARIN"), asn=64512)
        router = BGPRouter.objects.create(
            name="device-bgp-scope-cache AS64512",
            assigned_object_type=ContentType.objects.get_for_model(Device),
            assigned_object_id=device.pk,
            asn=asn,
        )
        global_scope = BGPScope.objects.create(router=router, vrf=None)
        runner = ForwardSyncRunner(
            sync=self.sync, ingestion=None, client=None, logger_=Mock()
        )

        self.assertEqual(runner._ensure_bgp_scope({}, router, None), global_scope)
        with CaptureQueriesContext(connection) as queries:
            second = runner._ensure_bgp_scope({}, router, None)

        self.assertEqual(second, global_scope)
        self.assertEqual(len(queries), 0)

    def test_bgp_scope_delete_resolution_reuses_cache_after_first_resolution(self):
        if not apps.is_installed("netbox_routing"):
            self.skipTest("netbox-routing optional plugin is not installed")
        BGPRouter = apps.get_model("netbox_routing", "BGPRouter")
        BGPScope = apps.get_model("netbox_routing", "BGPScope")
        device = self._create_device("device-bgp-scope-delete-cache")
        asn = ASN.objects.create(rir=RIR.objects.create(name="ARIN"), asn=64512)
        router = BGPRouter.objects.create(
            name="device-bgp-scope-delete-cache AS64512",
            assigned_object_type=ContentType.objects.get_for_model(Device),
            assigned_object_id=device.pk,
            asn=asn,
        )
        global_scope = BGPScope.objects.create(router=router, vrf=None)
        runner = ForwardSyncRunner(
            sync=self.sync, ingestion=None, client=None, logger_=Mock()
        )
        row = {
            "device": "device-bgp-scope-delete-cache",
            "vrf": None,
            "local_asn": 64512,
        }

        first = runner._resolve_bgp_scope_for_delete(row)
        with CaptureQueriesContext(connection) as queries:
            second = runner._resolve_bgp_scope_for_delete(row)

        self.assertEqual(first, global_scope)
        self.assertEqual(second, global_scope)
        self.assertEqual(len(queries), 0)

    def test_dependency_lookup_cache_primes_bgp_router_scope_identity(self):
        if not apps.is_installed("netbox_routing"):
            self.skipTest("netbox-routing optional plugin is not installed")
        BGPRouter = apps.get_model("netbox_routing", "BGPRouter")
        BGPScope = apps.get_model("netbox_routing", "BGPScope")
        device = self._create_device("device-bgp-prime")
        asn = ASN.objects.create(rir=RIR.objects.create(name="ARIN"), asn=64512)
        router = BGPRouter.objects.create(
            name="device-bgp-prime AS64512",
            assigned_object_type=ContentType.objects.get_for_model(Device),
            assigned_object_id=device.pk,
            asn=asn,
        )
        scope = BGPScope.objects.create(router=router, vrf=None)
        runner = ForwardSyncRunner(
            sync=self.sync, ingestion=None, client=None, logger_=Mock()
        )

        with CaptureQueriesContext(connection) as prime_queries:
            summary = prime_dependency_lookup_caches(
                runner,
                "netbox_routing.bgppeer",
                [
                    {
                        "device": device.name,
                        "vrf": None,
                        "local_asn": 64512,
                        "peer_asn": 64512,
                    }
                ],
            )

        self.assertGreaterEqual(len(prime_queries), 1)
        self.assertEqual(summary["routing_asn_count"], 1)
        self.assertEqual(summary["routing_bgp_router_count"], 1)
        self.assertEqual(summary["routing_bgp_scope_count"], 1)

        with CaptureQueriesContext(connection) as lookup_queries:
            self.assertEqual(runner._ensure_asn(64512), asn)
            self.assertEqual(
                get_unique_or_raise(
                    runner,
                    BGPRouter,
                    {
                        "assigned_object_type": ContentType.objects.get_for_model(
                            Device
                        ),
                        "assigned_object_id": device.pk,
                        "asn": asn,
                    },
                ),
                router,
            )
            self.assertEqual(
                get_unique_or_raise(runner, BGPScope, {"router": router, "vrf": None}),
                scope,
            )

        self.assertEqual(len(lookup_queries), 0)

    def test_dependency_lookup_cache_primes_ospf_instance_area_identity(self):
        if not apps.is_installed("netbox_routing"):
            self.skipTest("netbox-routing optional plugin is not installed")
        OSPFInstance = apps.get_model("netbox_routing", "OSPFInstance")
        OSPFArea = apps.get_model("netbox_routing", "OSPFArea")
        device = self._create_device("device-ospf-prime")
        instance = OSPFInstance.objects.create(
            name="device-ospf-prime OSPF 1",
            router_id="192.0.2.1",
            process_id=1,
            device=device,
            vrf=None,
            comments="Observed by Forward from structured OSPF state.",
        )
        area = OSPFArea.objects.create(
            area_id="0.0.0.0",
            area_type="backbone",
            description="Observed by Forward from structured OSPF state.",
        )
        runner = ForwardSyncRunner(
            sync=self.sync, ingestion=None, client=None, logger_=Mock()
        )

        with CaptureQueriesContext(connection) as prime_queries:
            summary = prime_dependency_lookup_caches(
                runner,
                "netbox_routing.ospfinstance",
                [
                    {
                        "device": device.name,
                        "vrf": None,
                        "process_id": 1,
                        "router_id": "192.0.2.1",
                        "area_id": "0.0.0.0",
                    }
                ],
            )

        self.assertGreaterEqual(len(prime_queries), 1)
        self.assertEqual(summary["routing_ospf_area_count"], 1)
        self.assertEqual(summary["routing_ospf_instance_count"], 1)

        with CaptureQueriesContext(connection) as lookup_queries:
            self.assertEqual(
                get_unique_or_raise(
                    runner,
                    OSPFInstance,
                    {"device": device, "vrf": None, "process_id": 1},
                ),
                instance,
            )
            self.assertEqual(
                get_unique_or_raise(runner, OSPFArea, {"area_id": "0.0.0.0"}),
                area,
            )

        self.assertEqual(len(lookup_queries), 0)

    def test_bgp_address_family_reuses_positive_cache_after_first_resolution(self):
        if not apps.is_installed("netbox_routing"):
            self.skipTest("netbox-routing optional plugin is not installed")
        BGPAddressFamily = apps.get_model("netbox_routing", "BGPAddressFamily")
        self._create_device("device-bgp-af-cache")
        runner = ForwardSyncRunner(
            sync=self.sync, ingestion=None, client=None, logger_=Mock()
        )
        row = {
            "device": "device-bgp-af-cache",
            "vrf": None,
            "local_asn": 64512,
            "afi_safi": "AfiSafiType.IPV4_UNICAST",
        }

        first = runner._ensure_bgp_address_family(row)
        with CaptureQueriesContext(connection) as queries:
            second = runner._ensure_bgp_address_family(row)

        self.assertEqual(first.pk, second.pk)
        self.assertEqual(BGPAddressFamily.objects.count(), 1)
        self.assertEqual(len(queries), 0)

    def test_ospf_area_reuses_positive_cache_after_first_resolution(self):
        if not apps.is_installed("netbox_routing"):
            self.skipTest("netbox-routing optional plugin is not installed")
        OSPFArea = apps.get_model("netbox_routing", "OSPFArea")
        runner = ForwardSyncRunner(
            sync=self.sync, ingestion=None, client=None, logger_=Mock()
        )
        row = {
            "area_id": "0",
            "area_type": "OspfAreaType.BACKBONE",
        }

        first = runner._ensure_ospf_area(row)
        with CaptureQueriesContext(connection) as queries:
            second = runner._ensure_ospf_area(row)

        self.assertEqual(first.pk, second.pk)
        self.assertEqual(OSPFArea.objects.count(), 1)
        self.assertEqual(len(queries), 0)

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

    def test_bgp_peer_adapter_repeat_sync_is_noop(self):
        if not apps.is_installed("netbox_routing"):
            self.skipTest("netbox-routing optional plugin is not installed")
        BGPPeer = apps.get_model("netbox_routing", "BGPPeer")
        self._create_device("device-1")
        runner = ForwardSyncRunner(
            sync=self.sync, ingestion=None, client=None, logger_=Mock()
        )
        row = {
            "device": "device-1",
            "vrf": None,
            "local_asn": 65000,
            "neighbor_address": "192.0.2.1",
            "peer_asn": 65100,
            "enabled": True,
            "status": "active",
        }

        before_count = ObjectChange.objects.count()
        runner._apply_netbox_routing_bgppeer(row)
        with CaptureQueriesContext(connection) as queries:
            runner._apply_netbox_routing_bgppeer(row)

        self.assertEqual(BGPPeer.objects.count(), 1)
        self.assertEqual(ObjectChange.objects.count(), before_count)
        self.assertEqual(self._update_statements(queries), [])

    def test_bgp_peer_address_family_adapter_repeat_sync_is_noop(self):
        if not apps.is_installed("netbox_routing"):
            self.skipTest("netbox-routing optional plugin is not installed")
        BGPPeerAddressFamily = apps.get_model("netbox_routing", "BGPPeerAddressFamily")
        self._create_device("device-1")
        runner = ForwardSyncRunner(
            sync=self.sync, ingestion=None, client=None, logger_=Mock()
        )
        row = {
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
        }

        before_count = ObjectChange.objects.count()
        runner._apply_netbox_routing_bgppeeraddressfamily(row)
        with CaptureQueriesContext(connection) as queries:
            runner._apply_netbox_routing_bgppeeraddressfamily(row)

        self.assertEqual(BGPPeerAddressFamily.objects.count(), 1)
        self.assertEqual(ObjectChange.objects.count(), before_count)
        self.assertEqual(self._update_statements(queries), [])

    def test_peering_relationship_reuses_cache_after_first_resolution(self):
        if not apps.is_installed("netbox_peering_manager"):
            self.skipTest("netbox-peering-manager optional plugin is not installed")
        Relationship = apps.get_model("netbox_peering_manager", "Relationship")
        runner = ForwardSyncRunner(
            sync=self.sync, ingestion=None, client=None, logger_=Mock()
        )
        row = {
            "relationship": "External BGP",
            "relationship_slug": "external-bgp",
        }

        first = runner._ensure_peering_relationship(row)
        with CaptureQueriesContext(connection) as queries:
            second = runner._ensure_peering_relationship(row)

        self.assertEqual(first.pk, second.pk)
        self.assertEqual(Relationship.objects.filter(slug="external-bgp").count(), 1)
        self.assertEqual(len(queries), 0)

    def test_routing_remaining_apply_helpers_repeat_sync_is_noop(self):
        if not apps.is_installed("netbox_routing"):
            self.skipTest("netbox-routing optional plugin is not installed")
        BGPAddressFamily = apps.get_model("netbox_routing", "BGPAddressFamily")
        OSPFInstance = apps.get_model("netbox_routing", "OSPFInstance")
        OSPFArea = apps.get_model("netbox_routing", "OSPFArea")
        self._create_device("device-1")

        cases = [
            (
                "bgpaddressfamily",
                "netbox_routing.bgpaddressfamily",
                {
                    "device": "device-1",
                    "vrf": None,
                    "local_asn": 64512,
                    "afi_safi": "AfiSafiType.IPV4_UNICAST",
                },
                BGPAddressFamily,
            ),
            (
                "ospfinstance",
                "netbox_routing.ospfinstance",
                {
                    "device": "device-1",
                    "vrf": None,
                    "process_id": "UNDERLAY",
                    "domain": "fabric",
                    "router_id": "192.0.2.254",
                },
                OSPFInstance,
            ),
            (
                "ospfarea",
                "netbox_routing.ospfarea",
                {
                    "area_id": "0",
                    "area_type": "OspfAreaType.BACKBONE",
                },
                OSPFArea,
            ),
        ]

        for label, model_string, row, model in cases:
            with self.subTest(label=label):
                runner = ForwardSyncRunner(
                    sync=self.sync, ingestion=None, client=None, logger_=Mock()
                )
                before_count = ObjectChange.objects.count()
                runner._apply_model_rows(model_string, [row])
                with CaptureQueriesContext(connection) as queries:
                    runner._apply_model_rows(model_string, [row])

                self.assertEqual(model.objects.count(), 1)
                self.assertEqual(ObjectChange.objects.count(), before_count)
                self.assertEqual(self._update_statements(queries), [])

    def test_peering_session_adapter_repeat_sync_is_noop(self):
        if not apps.is_installed("netbox_peering_manager"):
            self.skipTest("netbox-peering-manager optional plugin is not installed")
        PeeringSession = apps.get_model("netbox_peering_manager", "PeeringSession")
        self._create_device("device-1")
        runner = ForwardSyncRunner(
            sync=self.sync, ingestion=None, client=None, logger_=Mock()
        )
        row = {
            "device": "device-1",
            "vrf": None,
            "local_asn": 65000,
            "neighbor_address": "192.0.2.1",
            "peer_asn": 65100,
            "enabled": True,
            "status": "active",
            "peer_type": "PeerType.EXTERNAL",
        }

        before_count = ObjectChange.objects.count()
        runner._apply_netbox_peering_manager_peeringsession(row)
        with CaptureQueriesContext(connection) as queries:
            runner._apply_netbox_peering_manager_peeringsession(row)

        self.assertEqual(PeeringSession.objects.count(), 1)
        self.assertEqual(ObjectChange.objects.count(), before_count)
        self.assertEqual(self._update_statements(queries), [])

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

    def test_ospf_interface_adapter_reuses_cache_after_first_resolution(self):
        if not apps.is_installed("netbox_routing"):
            self.skipTest("netbox-routing optional plugin is not installed")
        OSPFInterface = apps.get_model("netbox_routing", "OSPFInterface")
        device = self._create_device("device-ospf-cache")
        Interface.objects.create(device=device, name="Ethernet1/1", type="1000base-t")
        runner = ForwardSyncRunner(
            sync=self.sync, ingestion=None, client=None, logger_=Mock()
        )
        row = {
            "device": "device-ospf-cache",
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

        first = runner._ensure_ospf_interface(row)
        with CaptureQueriesContext(connection) as queries:
            second = runner._ensure_ospf_interface(row)

        self.assertEqual(first.pk, second.pk)
        self.assertEqual(OSPFInterface.objects.count(), 1)
        self.assertEqual(len(queries), 0)

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

    def test_ospf_interface_adapter_repeat_sync_is_noop(self):
        if not apps.is_installed("netbox_routing"):
            self.skipTest("netbox-routing optional plugin is not installed")
        OSPFInterface = apps.get_model("netbox_routing", "OSPFInterface")
        device = self._create_device("device-1")
        Interface.objects.create(device=device, name="Ethernet1/1", type="1000base-t")
        runner = ForwardSyncRunner(
            sync=self.sync, ingestion=None, client=None, logger_=Mock()
        )
        row = {
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

        before_count = ObjectChange.objects.count()
        runner._apply_netbox_routing_ospfinterface(row)
        with CaptureQueriesContext(connection) as queries:
            runner._apply_netbox_routing_ospfinterface(row)

        self.assertEqual(OSPFInterface.objects.count(), 1)
        self.assertEqual(ObjectChange.objects.count(), before_count)
        self.assertEqual(self._update_statements(queries), [])

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

    def test_apply_dcim_interface_sets_access_mode_and_untagged_vlan(self):
        device = self._create_device("device-1")
        vlan = VLAN.objects.create(
            site=device.site,
            vid=10,
            name="users",
            status="active",
        )
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
                    "lag": None,
                    "mode": "access",
                    "untagged_vlan": 10,
                    "enabled": True,
                    "mtu": 9000,
                    "description": "",
                    "speed": 1000000,
                },
            ],
        )

        interface = Interface.objects.get(device=device, name="eth1-1")
        self.assertEqual(interface.mode, "access")
        self.assertEqual(interface.untagged_vlan, vlan)

    def test_apply_dcim_interface_reuses_untagged_vlan_cache_after_first_resolution(
        self,
    ):
        device = self._create_device("device-1")
        VLAN.objects.create(
            site=device.site,
            vid=10,
            name="users",
            status="active",
        )
        runner = ForwardSyncRunner(
            sync=self.sync, ingestion=None, client=None, logger_=Mock()
        )
        row = {
            "device": "device-1",
            "name": "eth1-1",
            "type": "1000base-t",
            "lag": None,
            "mode": "access",
            "untagged_vlan": 10,
            "enabled": True,
            "mtu": 9000,
            "description": "",
            "speed": 1000000,
        }

        runner._apply_model_rows("dcim.interface", [row])
        with CaptureQueriesContext(connection) as queries:
            runner._apply_model_rows("dcim.interface", [row])

        self.assertEqual(
            Interface.objects.filter(device=device, name="eth1-1").count(), 1
        )
        self.assertEqual(self._update_statements(queries), [])

    def test_apply_dcim_interface_repeat_sync_is_noop(self):
        device = self._create_device("device-1")
        Interface.objects.create(
            device=device,
            name="Ethernet1/1",
            type="1000base-t",
            enabled=True,
            mtu=1500,
            description="uplink",
            speed=1000000,
        )
        runner = ForwardSyncRunner(
            sync=self.sync, ingestion=None, client=None, logger_=Mock()
        )
        row = {
            "device": "device-1",
            "name": "Ethernet1/1",
            "type": "1000base-t",
            "enabled": True,
            "mtu": 1500,
            "description": "uplink",
            "speed": 1000000,
        }

        before_count = ObjectChange.objects.count()
        with CaptureQueriesContext(connection) as queries:
            runner._apply_dcim_interface(row)
            runner._apply_dcim_interface(row)

        self.assertEqual(
            Interface.objects.filter(device=device, name="Ethernet1/1").count(),
            1,
        )
        self.assertEqual(self._update_statements(queries), [])
        self.assertEqual(ObjectChange.objects.count(), before_count)

    def test_apply_dcim_interface_update_records_object_change(self):
        """Adapter-path UPDATES must go through per-row save() so that
        post_save fires and the Branching framework can record the change.
        Guards against bulk_update, which skips post_save and silently drops
        the changelog. Update-side load is reduced by fetching fewer rows (NQE
        diffs), never by bypassing per-row writes.

        Note: ObjectChange creation also requires a web request in thread-local
        storage (NetBox's handle_changed_object returns early when
        current_request is None). We verify the invariant by asserting that
        per-row save() is called — the mechanism that allows signals to fire —
        rather than checking the ObjectChange count, which depends on request
        context unavailable in unit tests."""
        device = self._create_device("device-1")
        Interface.objects.create(
            device=device,
            name="Ethernet1/1",
            type="1000base-t",
            enabled=True,
            mtu=1500,
            description="before",
            speed=1000000,
        )
        runner = ForwardSyncRunner(
            sync=self.sync, ingestion=None, client=None, logger_=Mock()
        )
        row = {
            "device": "device-1",
            "name": "Ethernet1/1",
            "type": "1000base-t",
            "enabled": True,
            "mtu": 9000,
            "description": "after",
            "speed": 1000000,
        }

        from django.db.models.signals import post_save

        post_save_fired = []

        def capture_post_save(sender, instance, created, **kwargs):
            if sender is Interface and not created:
                post_save_fired.append(instance)

        post_save.connect(capture_post_save, sender=Interface)
        try:
            runner._apply_model_rows("dcim.interface", [row])
        finally:
            post_save.disconnect(capture_post_save, sender=Interface)

        interface = Interface.objects.get(device=device, name="Ethernet1/1")
        self.assertEqual(interface.mtu, 9000)
        self.assertEqual(interface.description, "after")
        self.assertEqual(
            len(post_save_fired),
            1,
            "post_save must fire once for the interface update so that "
            "signals (including ObjectChange creation) can run. If this "
            "fails, an update is using bulk_update which skips post_save.",
        )

    def test_apply_dcim_interface_preserves_existing_description_when_source_is_blank(
        self,
    ):
        device = self._create_device("device-1")
        Interface.objects.create(
            device=device,
            name="Ethernet1/2",
            type="1000base-t",
            enabled=True,
            mtu=1500,
            description="uplink",
            speed=1000000,
        )
        runner = ForwardSyncRunner(
            sync=self.sync, ingestion=None, client=None, logger_=Mock()
        )
        row = {
            "device": "device-1",
            "name": "Ethernet1/2",
            "type": "1000base-t",
            "enabled": True,
            "mtu": 1500,
            "description": "",
            "speed": 1000000,
        }

        before_count = ObjectChange.objects.count()
        with CaptureQueriesContext(connection) as queries:
            runner._apply_dcim_interface(row)

        interface = Interface.objects.get(device=device, name="Ethernet1/2")
        self.assertEqual(interface.description, "uplink")
        self.assertEqual(self._update_statements(queries), [])
        self.assertEqual(ObjectChange.objects.count(), before_count)

    def test_apply_dcim_interface_skips_rows_with_missing_parent_device_once(self):
        runner = ForwardSyncRunner(
            sync=self.sync, ingestion=None, client=None, logger_=Mock()
        )
        runner._apply_dcim_interface = Mock()
        runner._missing_device_by_name_cache = {"missing-device"}

        with patch(
            "forward_netbox.utilities.sync_reporting.prime_dependency_lookup_caches",
            return_value={
                "available": False,
                "model": "dcim.interface",
                "row_count": 1,
                "primed_target_count": 0,
                "model_count": 0,
                "models": [],
            },
        ), patch(
            "forward_netbox.utilities.sync_reporting.record_issue"
        ) as record_issue:
            runner._apply_model_rows(
                "dcim.interface",
                [
                    {
                        "device": "missing-device",
                        "name": "Ethernet1/1",
                        "type": "1000base-t",
                        "enabled": True,
                        "mtu": 1500,
                        "description": "",
                    }
                ],
            )

        runner._apply_dcim_interface.assert_not_called()
        record_issue.assert_called_once()
        _, _, kwargs = record_issue.mock_calls[0]
        self.assertEqual(kwargs["log_level"], "info")
        self.assertEqual(kwargs["context"]["missing_parent_count"], 1)
        self.assertEqual(kwargs["context"]["missing_parent_names"], ["missing-device"])
        runner.logger.increment_statistics.assert_called_once_with(
            "dcim.interface",
            outcome="skipped",
            amount=1,
        )
        self.assertEqual(
            Interface.objects.filter(device__name="missing-device").count(),
            0,
        )

    def test_apply_dcim_macaddress_repeat_sync_is_noop(self):
        device = self._create_device("device-mac-noop")
        interface = Interface.objects.create(
            device=device,
            name="Ethernet1/1",
            type="1000base-t",
        )
        MACAddress.objects.create(
            mac_address="00:11:22:33:44:55",
            assigned_object_type=ContentType.objects.get_for_model(Interface),
            assigned_object_id=interface.pk,
        )
        runner = ForwardSyncRunner(
            sync=self.sync, ingestion=None, client=None, logger_=Mock()
        )
        row = {
            "device": device.name,
            "interface": interface.name,
            "mac": "00:11:22:33:44:55",
        }

        before_count = ObjectChange.objects.count()
        with CaptureQueriesContext(connection) as queries:
            runner._apply_dcim_macaddress(row)
            runner._apply_dcim_macaddress(row)

        self.assertEqual(
            MACAddress.objects.filter(mac_address="00:11:22:33:44:55").count(), 1
        )
        self.assertEqual(self._update_statements(queries), [])
        self.assertEqual(ObjectChange.objects.count(), before_count)

    def test_apply_dcim_device_repeat_sync_is_noop(self):
        device = self._create_device("device-1")
        row = {
            "name": "device-1",
            "site": device.site.name,
            "site_slug": device.site.slug,
            "role": device.role.name,
            "role_slug": device.role.slug,
            "role_color": device.role.color,
            "manufacturer": device.device_type.manufacturer.name,
            "manufacturer_slug": device.device_type.manufacturer.slug,
            "device_type": device.device_type.model,
            "device_type_slug": device.device_type.slug,
            "platform": None,
            "status": device.status,
            "serial": "",
        }

        before_count = ObjectChange.objects.count()
        runner = ForwardSyncRunner(
            sync=self.sync, ingestion=None, client=None, logger_=Mock()
        )
        with CaptureQueriesContext(connection) as queries:
            runner._apply_dcim_device(row)
            runner._apply_dcim_device(row)

        device.refresh_from_db()
        self.assertEqual(Device.objects.filter(name="device-1").count(), 1)
        self.assertEqual(self._update_statements(queries), [])
        self.assertEqual(ObjectChange.objects.count(), before_count)

    def test_apply_dcim_device_sparse_row_preserves_existing_serial(self):
        device = self._create_device("device-serial-preserve")
        device.serial = "SERIAL-1"
        device.save(update_fields=["serial"])
        row = {
            "name": device.name,
            "site": device.site.name,
            "site_slug": device.site.slug,
            "role": device.role.name,
            "role_slug": device.role.slug,
            "role_color": device.role.color,
            "manufacturer": device.device_type.manufacturer.name,
            "manufacturer_slug": device.device_type.manufacturer.slug,
            "device_type": device.device_type.model,
            "device_type_slug": device.device_type.slug,
            "platform": None,
            "status": device.status,
            "serial": "",
        }

        before_count = ObjectChange.objects.count()
        runner = ForwardSyncRunner(
            sync=self.sync, ingestion=None, client=None, logger_=Mock()
        )
        with CaptureQueriesContext(connection) as queries:
            runner._apply_dcim_device(row)

        device.refresh_from_db()
        self.assertEqual(device.serial, "SERIAL-1")
        self.assertEqual(self._update_statements(queries), [])
        self.assertEqual(ObjectChange.objects.count(), before_count)

    def test_apply_dcim_device_does_not_mutate_owned_status_tags(self):
        from extras.models import Tag

        device = self._create_device("device-now-in-scope")
        out_of_scope = Tag.objects.create(
            name="Forward Out Of Scope",
            slug="forward-out-of-scope",
            color="f44336",
        )
        customer_tag = Tag.objects.create(
            name="Customer Managed",
            slug="customer-managed",
            color="9e9e9e",
        )
        device.tags.add(out_of_scope, customer_tag)
        row = {
            "name": device.name,
            "site": device.site.name,
            "site_slug": device.site.slug,
            "role": device.role.name,
            "role_slug": device.role.slug,
            "role_color": device.role.color,
            "manufacturer": device.device_type.manufacturer.name,
            "manufacturer_slug": device.device_type.manufacturer.slug,
            "device_type": device.device_type.model,
            "device_type_slug": device.device_type.slug,
            "platform": None,
            "status": device.status,
            "serial": "",
        }
        runner = ForwardSyncRunner(
            sync=self.sync, ingestion=None, client=None, logger_=Mock()
        )

        runner._apply_dcim_device(row)

        self.assertTrue(device.tags.filter(pk=out_of_scope.pk).exists())
        self.assertTrue(device.tags.filter(pk=customer_tag.pk).exists())

    def test_apply_dcim_interface_keeps_import_when_untagged_vlan_missing(self):
        device = self._create_device("device-1")
        existing_vlan = VLAN.objects.create(
            site=device.site,
            vid=20,
            name="existing",
            status="active",
        )
        Interface.objects.create(
            device=device,
            name="eth1-1",
            type="1000base-t",
            mode="access",
            untagged_vlan=existing_vlan,
        )
        logger = Mock()
        runner = ForwardSyncRunner(
            sync=self.sync, ingestion=None, client=None, logger_=logger
        )

        runner._apply_model_rows(
            "dcim.interface",
            [
                {
                    "device": "device-1",
                    "name": "eth1-1",
                    "type": "1000base-t",
                    "lag": None,
                    "mode": "access",
                    "untagged_vlan": 10,
                    "enabled": True,
                    "mtu": 9000,
                    "description": "",
                    "speed": 1000000,
                },
            ],
        )

        interface = Interface.objects.get(device__name="device-1", name="eth1-1")
        self.assertEqual(interface.mode, "access")
        self.assertEqual(interface.untagged_vlan, existing_vlan)
        warning_messages = [call.args[0] for call in logger.log_warning.call_args_list]
        self.assertTrue(
            any("VLAN was not imported" in message for message in warning_messages)
        )

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

    def test_apply_dcim_interface_lag_member_preserves_parent_description(self):
        device = self._create_device("device-1")
        lag = Interface.objects.create(
            device=device,
            name="po8",
            type="lag",
            enabled=True,
            mtu=9000,
            description="existing aggregate",
        )
        runner = ForwardSyncRunner(
            sync=self.sync, ingestion=None, client=None, logger_=Mock()
        )
        row = {
            "device": "device-1",
            "name": "eth1/10",
            "type": "1000base-t",
            "lag": "po8",
            "enabled": True,
            "mtu": 9000,
            "description": "",
            "speed": 1000000,
        }

        runner._apply_dcim_interface(row)
        lag.refresh_from_db()
        self.assertEqual(lag.description, "existing aggregate")
        self.assertEqual(lag.mtu, 9000)

        before_count = ObjectChange.objects.count()
        with CaptureQueriesContext(connection) as queries:
            runner._apply_dcim_interface(row)

        lag.refresh_from_db()
        member = Interface.objects.get(device=device, name="eth1/10")
        self.assertEqual(member.lag, lag)
        self.assertEqual(lag.description, "existing aggregate")
        self.assertEqual(lag.mtu, 9000)
        self.assertEqual(self._update_statements(queries), [])
        self.assertEqual(ObjectChange.objects.count(), before_count)

    def test_apply_dcim_interface_sparse_row_preserves_existing_owned_fields(self):
        device = self._create_device("device-1")
        interface = Interface.objects.create(
            device=device,
            name="eth1/20",
            type="1000base-t",
            enabled=True,
            mtu=9000,
            speed=1000000,
            description="server uplink",
        )
        runner = ForwardSyncRunner(
            sync=self.sync, ingestion=None, client=None, logger_=Mock()
        )
        sparse_row = {
            "device": "device-1",
            "name": "eth1/20",
            "type": "1000base-t",
            "lag": None,
            "enabled": True,
            "description": "",
        }

        before_count = ObjectChange.objects.count()
        with CaptureQueriesContext(connection) as queries:
            runner._apply_dcim_interface(sparse_row)

        interface.refresh_from_db()
        self.assertEqual(interface.description, "server uplink")
        self.assertEqual(interface.mtu, 9000)
        self.assertEqual(interface.speed, 1000000)
        self.assertEqual(self._update_statements(queries), [])
        self.assertEqual(ObjectChange.objects.count(), before_count)

        runner._apply_dcim_interface(
            {
                **sparse_row,
                "description": "new server uplink",
                "mtu": 9216,
                "speed": 25000000,
            }
        )

        interface.refresh_from_db()
        self.assertEqual(interface.description, "new server uplink")
        self.assertEqual(interface.mtu, 9216)
        self.assertEqual(interface.speed, 25000000)

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

    def test_apply_extras_taggeditem_reuses_existing_tag_by_name_when_slug_differs(
        self,
    ):
        # Regression (2.3.0 field report): a NetBox tag with the same NAME but a
        # different SLUG must be REUSED, not re-created — the slug-only match
        # missed it and the create failed the unique-name constraint
        # ("Tag with this Name already exists.").
        device = self._create_device("device-1")
        existing = Tag.objects.create(
            name="Prot_BGP", slug="prot_bgp_legacy", color="9e9e9e"
        )
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

        self.assertEqual(Tag.objects.filter(name="Prot_BGP").count(), 1)
        existing.refresh_from_db()
        self.assertIn(existing, device.tags.all())

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

    def test_apply_dcim_inventoryitem_repeat_sync_is_noop(self):
        device = self._create_device("device-inventory-noop")
        runner = ForwardSyncRunner(
            sync=self.sync, ingestion=None, client=None, logger_=Mock()
        )
        row = {
            "device": device.name,
            "manufacturer": "vendor-1",
            "manufacturer_slug": "vendor-1",
            "name": "Power Supply 1",
            "label": "PSU 1",
            "part_id": "PSU-1",
            "serial": "SN-1",
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

        before_count = ObjectChange.objects.count()
        runner._apply_dcim_inventoryitem(row)
        with CaptureQueriesContext(connection) as queries:
            runner._apply_dcim_inventoryitem(row)

        self.assertEqual(
            InventoryItem.objects.filter(device=device, name="Power Supply 1").count(),
            1,
        )
        self.assertEqual(ObjectChange.objects.count(), before_count)
        self.assertEqual(self._update_statements(queries), [])

    def test_apply_dcim_inventoryitem_sparse_row_preserves_owned_fields(self):
        device = self._create_device("device-inventory-preserve")
        role = InventoryItemRole.objects.create(
            name="POWER SUPPLY",
            slug="power-supply",
            color="ff9800",
        )
        manufacturer, _ = Manufacturer.objects.get_or_create(
            slug="vendor-1",
            defaults={"name": "vendor-1"},
        )
        InventoryItem.objects.create(
            device=device,
            name="Power Supply 1",
            manufacturer=manufacturer,
            label="PSU 1",
            part_id="PSU-1",
            serial="SN-1",
            asset_tag="ASSET-1",
            role=role,
            status="active",
            discovered=True,
            description="Version: V01",
        )
        runner = ForwardSyncRunner(
            sync=self.sync, ingestion=None, client=None, logger_=Mock()
        )
        row = {
            "device": device.name,
            "manufacturer": "vendor-1",
            "manufacturer_slug": "vendor-1",
            "name": "Power Supply 1",
            "label": "",
            "part_id": "",
            "serial": "",
            "asset_tag": None,
            "role": role.name,
            "role_slug": role.slug,
            "role_color": role.color,
            "part_type": "POWER SUPPLY",
            "module_component": False,
            "status": "active",
            "discovered": True,
            "description": "",
        }

        before_count = ObjectChange.objects.count()
        with CaptureQueriesContext(connection) as queries:
            runner._apply_dcim_inventoryitem(row)

        item = InventoryItem.objects.get(device=device, name="Power Supply 1")
        self.assertEqual(item.label, "PSU 1")
        self.assertEqual(item.part_id, "PSU-1")
        self.assertEqual(item.serial, "SN-1")
        self.assertEqual(item.asset_tag, "ASSET-1")
        self.assertEqual(item.description, "Version: V01")
        self.assertEqual(self._update_statements(queries), [])
        self.assertEqual(ObjectChange.objects.count(), before_count)

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

    def test_apply_dcim_cable_repeat_sync_is_noop(self):
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

        before_count = ObjectChange.objects.count()
        runner._apply_dcim_cable(row)
        with CaptureQueriesContext(connection) as queries:
            runner._apply_dcim_cable(row)

        self.assertEqual(Cable.objects.count(), 1)
        self.assertEqual(ObjectChange.objects.count(), before_count)
        self.assertEqual(self._update_statements(queries), [])

    def test_lookup_cable_between_reuses_cache_after_first_resolution(self):
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
        cable = Cable.objects.create(
            status="connected",
            a_terminations=[interface],
            b_terminations=[remote_interface],
        )
        interface.cable_id = cable.pk
        remote_interface.cable_id = cable.pk
        runner = ForwardSyncRunner(
            sync=self.sync, ingestion=None, client=None, logger_=Mock()
        )

        with CaptureQueriesContext(connection) as first_queries:
            first = runner._lookup_cable_between(interface, remote_interface)
        with CaptureQueriesContext(connection) as queries:
            second = runner._lookup_cable_between(interface, remote_interface)

        self.assertEqual(first.pk, cable.pk)
        self.assertEqual(second.pk, cable.pk)
        self.assertEqual(len(first_queries), 1)
        self.assertEqual(len(queries), 0)

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

    def test_apply_dcim_cable_prefilters_missing_remote_devices_once(self):
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

        with patch(
            "forward_netbox.utilities.sync_reporting.record_issue"
        ) as record_issue:
            runner._apply_model_rows("dcim.cable", rows)

        warning_messages = [call.args[0] for call in logger.log_warning.call_args_list]
        self.assertEqual(warning_messages, [])
        record_issue.assert_called_once()
        self.assertEqual(
            record_issue.call_args.kwargs["context"]["blocked_row_count"],
            ForwardSyncRunner.CONFLICT_WARNING_DETAIL_LIMIT + 4,
        )
        logger.increment_statistics.assert_any_call(
            "dcim.cable",
            outcome="skipped",
            amount=ForwardSyncRunner.CONFLICT_WARNING_DETAIL_LIMIT + 4,
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

    def test_apply_dcim_module_adopts_existing_interface(self):
        # The module type's interface template collides by name with an interface
        # Forward already synced onto the device. The module apply must ADOPT it
        # instead of recreating it (which would raise a unique-constraint
        # IntegrityError on dcim_interface_unique_device_name).
        from dcim.models import InterfaceTemplate

        device = self._create_device("device-a")
        module_bay = self._create_module_bay(device)
        existing_interface = Interface.objects.create(
            device=device, name="GigabitEthernet0/0/0", type="1000base-t"
        )
        manufacturer = Manufacturer.objects.get(slug="vendor-1")
        module_type = ModuleType.objects.create(
            manufacturer=manufacturer, model="Line Card 1", part_number="LC-1"
        )
        InterfaceTemplate.objects.create(
            module_type=module_type, name="GigabitEthernet0/0/0", type="1000base-t"
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

        runner._apply_dcim_module(row)

        module = Module.objects.get(device=device, module_bay=module_bay)
        # No duplicate interface created; the existing one was adopted.
        self.assertEqual(
            Interface.objects.filter(
                device=device, name="GigabitEthernet0/0/0"
            ).count(),
            1,
        )
        existing_interface.refresh_from_db()
        self.assertEqual(existing_interface.module_id, module.pk)

    def test_apply_dcim_module_creates_missing_module_bay(self):
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
        self.assertEqual(module_bay.position, "2")
        self.assertEqual(module_bay.label, "Slot 2")
        self.assertTrue(
            Module.objects.filter(device=device, module_bay=module_bay).exists()
        )
        self.assertNotIn(
            ("dcim.module", "missing-module-bay"),
            runner._aggregated_skip_warning_counts,
        )

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

    def test_apply_dcim_module_sparse_row_preserves_owned_fields(self):
        device = self._create_device("device-module-preserve")
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
            "serial": "",
            "asset_tag": None,
            "description": "",
        }

        before_count = ObjectChange.objects.count()
        with CaptureQueriesContext(connection) as queries:
            runner._apply_dcim_module(row)

        module.refresh_from_db()
        self.assertEqual(module.serial, "SN-1")
        self.assertEqual(module.asset_tag, "AT-1")
        self.assertEqual(self._update_statements(queries), [])
        self.assertEqual(ObjectChange.objects.count(), before_count)

    def test_apply_dcim_module_repeat_sync_is_noop(self):
        device = self._create_device("device-module-noop")
        self._create_module_bay(device)
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

        before_count = ObjectChange.objects.count()
        runner._apply_dcim_module(row)
        with CaptureQueriesContext(connection) as queries:
            runner._apply_dcim_module(row)

        self.assertEqual(Module.objects.filter(device=device).count(), 1)
        self.assertEqual(ObjectChange.objects.count(), before_count)
        self.assertEqual(self._update_statements(queries), [])

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

    def test_validate_row_shape_allows_prefix_with_null_vrf_identity(self):
        validate_row_shape_for_model(
            "ipam.prefix",
            {
                "prefix": "10.0.0.0/24",
                "vrf": None,
                "status": "active",
            },
            [["prefix", "vrf"]],
        )

    def test_validate_row_shape_allows_prefix_with_empty_vrf_identity(self):
        validate_row_shape_for_model(
            "ipam.prefix",
            {
                "prefix": "10.0.0.0/24",
                "vrf": "",
                "status": "active",
            },
            [["prefix", "vrf"]],
        )

    def test_validate_row_shape_rejects_prefix_missing_vrf_identity(self):
        with self.assertRaises(ForwardQueryError):
            validate_row_shape_for_model(
                "ipam.prefix",
                {
                    "prefix": "10.0.0.0/24",
                    "status": "active",
                },
                [["prefix", "vrf"]],
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

    def test_apply_ipam_prefix_keeps_global_and_vrf_scoped_rows_distinct(self):
        global_prefix = Prefix.objects.create(
            prefix="192.0.2.0/27",
            vrf=None,
            status="active",
        )
        runner = ForwardSyncRunner(
            sync=self.sync, ingestion=None, client=None, logger_=Mock()
        )

        runner._apply_ipam_prefix(
            {
                "prefix": "192.0.2.0/27",
                "vrf": "blue",
                "status": "active",
            }
        )

        global_prefix.refresh_from_db()
        scoped_prefix = Prefix.objects.get(
            prefix="192.0.2.0/27",
            vrf__name="blue",
        )
        self.assertIsNone(global_prefix.vrf)
        self.assertEqual(scoped_prefix.status, "active")
        self.assertEqual(Prefix.objects.filter(prefix="192.0.2.0/27").count(), 2)

    def test_apply_ipam_prefix_repeat_sync_does_not_rewrite_vrf(self):
        vrf = VRF.objects.create(name="blue", rd="64512:106")
        prefix = Prefix.objects.create(
            prefix="192.0.2.0/27",
            vrf=vrf,
            status="active",
        )
        runner = ForwardSyncRunner(
            sync=self.sync, ingestion=None, client=None, logger_=Mock()
        )
        row = {
            "prefix": "192.0.2.0/27",
            "vrf": "blue",
            "status": "active",
        }

        before_count = ObjectChange.objects.count()
        with CaptureQueriesContext(connection) as queries:
            first_result = runner._apply_ipam_prefix(row)
            second_result = runner._apply_ipam_prefix(row)

        prefix.refresh_from_db()
        self.assertEqual(prefix.vrf, vrf)
        self.assertEqual(Prefix.objects.filter(prefix="192.0.2.0/27").count(), 1)
        self.assertEqual(first_result, "unchanged")
        self.assertEqual(second_result, "unchanged")
        self.assertEqual(self._update_statements(queries), [])
        self.assertEqual(ObjectChange.objects.count(), before_count)

    def test_apply_ipam_vlan_repeat_sync_is_noop(self):
        site = Site.objects.create(name="site-1", slug="site-1")
        VLAN.objects.create(site=site, vid=10, name="VLAN10", status="active")
        runner = ForwardSyncRunner(
            sync=self.sync, ingestion=None, client=None, logger_=Mock()
        )
        row = {
            "site": "site-1",
            "site_slug": "site-1",
            "vid": 10,
            "name": "VLAN10",
            "status": "active",
        }

        before_count = ObjectChange.objects.count()
        with CaptureQueriesContext(connection) as queries:
            runner._apply_ipam_vlan(row)
            runner._apply_ipam_vlan(row)

        self.assertEqual(VLAN.objects.filter(site=site, vid=10).count(), 1)
        self.assertEqual(self._update_statements(queries), [])
        self.assertEqual(ObjectChange.objects.count(), before_count)

    def test_delete_ipam_vlan_reuses_cached_site_lookup_after_first_resolution(self):
        site = Site.objects.create(name="site-1", slug="site-1")
        VLAN.objects.create(site=site, vid=10, name="VLAN10", status="active")
        runner = ForwardSyncRunner(
            sync=self.sync, ingestion=None, client=None, logger_=Mock()
        )
        row = {
            "site": "site-1",
            "site_slug": "site-1",
            "vid": 10,
        }

        with patch.object(runner, "_delete_by_coalesce", return_value=False):
            self.assertFalse(runner._delete_ipam_vlan(row))
            with CaptureQueriesContext(connection) as queries:
                self.assertFalse(runner._delete_ipam_vlan(row))

        self.assertEqual(len(queries), 0)

    def test_apply_ipam_vrf_repeat_sync_is_noop(self):
        VRF.objects.create(
            name="blue",
            rd="64512:106",
            description="",
            enforce_unique=False,
        )
        runner = ForwardSyncRunner(
            sync=self.sync, ingestion=None, client=None, logger_=Mock()
        )
        row = {
            "name": "blue",
            "rd": "64512:106",
            "description": "",
            "enforce_unique": False,
        }

        before_count = ObjectChange.objects.count()
        with CaptureQueriesContext(connection) as queries:
            runner._apply_ipam_vrf(row)
            runner._apply_ipam_vrf(row)

        self.assertEqual(VRF.objects.filter(name="blue").count(), 1)
        self.assertEqual(self._update_statements(queries), [])
        self.assertEqual(ObjectChange.objects.count(), before_count)

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

    def test_apply_ipam_ipaddress_repeat_sync_is_noop(self):
        device = self._create_device("device-ip-noop")
        Interface.objects.create(device=device, name="Ethernet1/1", type="1000base-t")
        runner = ForwardSyncRunner(
            sync=self.sync, ingestion=None, client=None, logger_=Mock()
        )
        row = {
            "device": "device-ip-noop",
            "interface": "Ethernet1/1",
            "address": "10.0.0.1/24",
            "vrf": None,
            "status": "active",
        }

        before_count = ObjectChange.objects.count()
        runner._apply_ipam_ipaddress(row)
        with CaptureQueriesContext(connection) as queries:
            runner._apply_ipam_ipaddress(row)

        self.assertEqual(IPAddress.objects.filter(address="10.0.0.1/24").count(), 1)
        self.assertEqual(ObjectChange.objects.count(), before_count)
        self.assertEqual(self._update_statements(queries), [])

    def test_apply_ipam_fhrpgroup_creates_group_assignment_and_vip(self):
        device = self._create_device("device-1")
        interface = Interface.objects.create(
            device=device,
            name="Vlan100",
            type="virtual",
        )
        runner = ForwardSyncRunner(
            sync=self.sync, ingestion=None, client=None, logger_=Mock()
        )

        runner._apply_ipam_fhrpgroup(
            {
                "protocol": "hsrp",
                "group_id": 10,
                "name": "hsrp",
                "device": "device-1",
                "interface": "Vlan100",
                "vrf": None,
                "address": "10.0.0.1/32",
                "state": "MASTER",
                "priority": 100,
                "status": "active",
            }
        )

        group = FHRPGroup.objects.get()
        self.assertEqual(group.protocol, "hsrp")
        self.assertEqual(group.group_id, 10)
        self.assertEqual(group.name, "hsrp-10-10.0.0.1")
        assignment = FHRPGroupAssignment.objects.get()
        self.assertEqual(assignment.group, group)
        self.assertEqual(assignment.interface, interface)
        self.assertEqual(assignment.priority, 100)
        ip_address = IPAddress.objects.get()
        self.assertEqual(str(ip_address.address), "10.0.0.1/32")
        self.assertEqual(ip_address.role, "hsrp")
        self.assertEqual(ip_address.assigned_object, group)

    def test_apply_ipam_fhrpgroup_shared_vip_persists_both_groups_idempotently(self):
        # Two distinct HSRP groups (different group_id) legitimately share a
        # virtual IP. NetBox attaches the VIP IPAddress to ONE group; the second
        # group must still PERSIST (with its interface assignment), and a re-sync
        # must NOT delete-and-recreate it. This is the root of the pernicious
        # 13-FHRP-group add/remove churn (shared VIPs across two group_ids).
        device = self._create_device("device-1")
        Interface.objects.create(device=device, name="Vlan100", type="virtual")
        Interface.objects.create(device=device, name="Vlan200", type="virtual")

        def apply_both():
            runner = ForwardSyncRunner(
                sync=self.sync, ingestion=None, client=None, logger_=Mock()
            )
            for gid, iface in ((1, "Vlan100"), (16, "Vlan200")):
                runner._apply_ipam_fhrpgroup(
                    {
                        "protocol": "hsrp",
                        "group_id": gid,
                        "name": "hsrp",
                        "device": "device-1",
                        "interface": iface,
                        "vrf": None,
                        "address": "10.0.0.1/32",
                        "state": "MASTER",
                        "priority": 100,
                        "status": "active",
                    }
                )

        apply_both()
        # Both groups exist; VIP attached once (to the first group); no dup IP.
        self.assertEqual(FHRPGroup.objects.count(), 2)
        self.assertEqual(FHRPGroupAssignment.objects.count(), 2)
        self.assertEqual(IPAddress.objects.filter(address="10.0.0.1/32").count(), 1)
        original_pks = set(FHRPGroup.objects.values_list("pk", flat=True))

        # Re-sync: idempotent — same groups, same PKs (NOT deleted + recreated).
        apply_both()
        self.assertEqual(FHRPGroup.objects.count(), 2)
        self.assertEqual(FHRPGroupAssignment.objects.count(), 2)
        self.assertEqual(IPAddress.objects.filter(address="10.0.0.1/32").count(), 1)
        self.assertEqual(
            set(FHRPGroup.objects.values_list("pk", flat=True)), original_pks
        )

        # Removing the second (VIP-less) group must NOT delete the first group's
        # shared VIP.
        runner = ForwardSyncRunner(
            sync=self.sync, ingestion=None, client=None, logger_=Mock()
        )
        runner._delete_ipam_fhrpgroup(
            {
                "protocol": "hsrp",
                "group_id": 16,
                "name": "hsrp",
                "device": "device-1",
                "interface": "Vlan200",
                "vrf": None,
                "address": "10.0.0.1/32",
                "status": "active",
            }
        )
        self.assertFalse(FHRPGroup.objects.filter(group_id=16).exists())
        self.assertTrue(FHRPGroup.objects.filter(group_id=1).exists())
        self.assertEqual(IPAddress.objects.filter(address="10.0.0.1/32").count(), 1)

    def test_apply_ipam_fhrpgroup_creates_vrrp_group_assignment_and_vip(self):
        device = self._create_device("device-1")
        interface = Interface.objects.create(
            device=device,
            name="Vlan100",
            type="virtual",
        )
        runner = ForwardSyncRunner(
            sync=self.sync, ingestion=None, client=None, logger_=Mock()
        )

        runner._apply_ipam_fhrpgroup(
            {
                "protocol": "vrrp2",
                "group_id": 10,
                "name": "vrrp",
                "device": "device-1",
                "interface": "Vlan100",
                "vrf": None,
                "address": "10.0.0.1/32",
                "state": "MASTER",
                "priority": 100,
                "status": "active",
            }
        )

        group = FHRPGroup.objects.get()
        self.assertEqual(group.protocol, "vrrp2")
        self.assertEqual(group.group_id, 10)
        self.assertEqual(group.name, "vrrp2-10-10.0.0.1")
        self.assertEqual(group.description, "Forward FHRP group")
        assignment = FHRPGroupAssignment.objects.get()
        self.assertEqual(assignment.group, group)
        self.assertEqual(assignment.interface, interface)
        ip_address = IPAddress.objects.get()
        self.assertEqual(str(ip_address.address), "10.0.0.1/32")
        self.assertEqual(ip_address.role, "vrrp")
        self.assertEqual(ip_address.assigned_object, group)

    def test_apply_ipam_fhrpgroup_separates_vrrp2_and_vrrp3_protocols(self):
        device = self._create_device("device-1")
        Interface.objects.create(device=device, name="Vlan100", type="virtual")
        Interface.objects.create(device=device, name="Vlan200", type="virtual")
        runner = ForwardSyncRunner(
            sync=self.sync, ingestion=None, client=None, logger_=Mock()
        )
        row = {
            "protocol": "vrrp2",
            "group_id": 10,
            "name": "vrrp",
            "device": "device-1",
            "interface": "Vlan100",
            "vrf": None,
            "address": "10.0.0.1/32",
            "state": "MASTER",
            "priority": 100,
            "status": "active",
        }

        runner._apply_ipam_fhrpgroup(row)
        runner._apply_ipam_fhrpgroup(
            {
                **row,
                "protocol": "vrrp3",
                "interface": "Vlan200",
                "address": "2001:db8::1/128",
            }
        )

        self.assertEqual(FHRPGroup.objects.count(), 2)
        self.assertEqual(FHRPGroupAssignment.objects.count(), 2)
        self.assertEqual(IPAddress.objects.count(), 2)
        self.assertEqual(
            set(FHRPGroup.objects.values_list("protocol", flat=True)),
            {"vrrp2", "vrrp3"},
        )
        self.assertEqual(
            set(IPAddress.objects.values_list("role", flat=True)),
            {"vrrp"},
        )

    def test_apply_ipam_fhrpgroup_is_idempotent_and_updates_priority(self):
        device = self._create_device("device-1")
        Interface.objects.create(device=device, name="Vlan100", type="virtual")
        runner = ForwardSyncRunner(
            sync=self.sync, ingestion=None, client=None, logger_=Mock()
        )
        row = {
            "protocol": "hsrp",
            "group_id": 10,
            "name": "hsrp",
            "device": "device-1",
            "interface": "Vlan100",
            "vrf": None,
            "address": "10.0.0.1/32",
            "state": "MASTER",
            "priority": 100,
            "status": "active",
        }

        runner._apply_ipam_fhrpgroup(row)
        runner._apply_ipam_fhrpgroup({**row, "priority": 110, "state": "BACKUP"})

        self.assertEqual(FHRPGroup.objects.count(), 1)
        self.assertEqual(FHRPGroupAssignment.objects.count(), 1)
        self.assertEqual(IPAddress.objects.count(), 1)
        self.assertEqual(FHRPGroupAssignment.objects.get().priority, 110)

    def test_apply_ipam_fhrpgroup_repeat_sync_is_noop(self):
        device = self._create_device("device-1")
        Interface.objects.create(device=device, name="Vlan100", type="virtual")
        runner = ForwardSyncRunner(
            sync=self.sync, ingestion=None, client=None, logger_=Mock()
        )
        row = {
            "protocol": "hsrp",
            "group_id": 10,
            "name": "hsrp",
            "device": "device-1",
            "interface": "Vlan100",
            "vrf": None,
            "address": "10.0.0.1/32",
            "state": "MASTER",
            "priority": 100,
            "status": "active",
        }
        runner._apply_ipam_fhrpgroup(row)

        before_count = ObjectChange.objects.count()
        with CaptureQueriesContext(connection) as queries:
            runner._apply_ipam_fhrpgroup(row)

        self.assertEqual(FHRPGroup.objects.count(), 1)
        self.assertEqual(FHRPGroupAssignment.objects.count(), 1)
        self.assertEqual(IPAddress.objects.count(), 1)
        self.assertEqual(self._update_statements(queries), [])
        self.assertEqual(ObjectChange.objects.count(), before_count)

    def test_apply_ipam_fhrpgroup_skips_missing_interface_and_continues(self):
        device = self._create_device("device-1")
        Interface.objects.create(device=device, name="Vlan100", type="virtual")
        logger = Mock()
        runner = ForwardSyncRunner(
            sync=self.sync, ingestion=None, client=None, logger_=logger
        )

        runner._apply_model_rows(
            "ipam.fhrpgroup",
            [
                {
                    "protocol": "hsrp",
                    "group_id": 10,
                    "name": "hsrp",
                    "device": "device-1",
                    "interface": "Vlan999",
                    "vrf": None,
                    "address": "10.0.0.1/32",
                    "state": "MASTER",
                    "priority": 100,
                    "status": "active",
                },
                {
                    "protocol": "hsrp",
                    "group_id": 10,
                    "name": "hsrp",
                    "device": "device-1",
                    "interface": "Vlan100",
                    "vrf": None,
                    "address": "10.0.0.1/32",
                    "state": "BACKUP",
                    "priority": 100,
                    "status": "active",
                },
            ],
        )

        self.assertEqual(FHRPGroup.objects.count(), 1)
        self.assertEqual(FHRPGroupAssignment.objects.count(), 1)
        warning_messages = [call.args[0] for call in logger.log_warning.call_args_list]
        self.assertEqual(len(warning_messages), 1)
        self.assertIn("target interface was not imported", warning_messages[0])
        logger.increment_statistics.assert_any_call("ipam.fhrpgroup", outcome="skipped")
        logger.increment_statistics.assert_any_call("ipam.fhrpgroup", outcome="applied")

    def test_delete_ipam_fhrpgroup_removes_last_assignment_group_and_vip(self):
        device = self._create_device("device-1")
        Interface.objects.create(device=device, name="Vlan100", type="virtual")
        runner = ForwardSyncRunner(
            sync=self.sync, ingestion=None, client=None, logger_=Mock()
        )
        row = {
            "protocol": "hsrp",
            "group_id": 10,
            "name": "hsrp",
            "device": "device-1",
            "interface": "Vlan100",
            "vrf": None,
            "address": "10.0.0.1/32",
            "state": "MASTER",
            "priority": 100,
            "status": "active",
        }
        runner._apply_ipam_fhrpgroup(row)

        deleted = runner._delete_ipam_fhrpgroup(row)

        self.assertTrue(deleted)
        self.assertEqual(FHRPGroupAssignment.objects.count(), 0)
        self.assertEqual(FHRPGroup.objects.count(), 0)
        self.assertEqual(IPAddress.objects.count(), 0)

    def test_delete_ipam_fhrpgroup_reuses_cached_vip_lookup_after_first_resolution(
        self,
    ):
        device = self._create_device("device-1")
        Interface.objects.create(device=device, name="Vlan100", type="virtual")
        runner = ForwardSyncRunner(
            sync=self.sync, ingestion=None, client=None, logger_=Mock()
        )
        row = {
            "protocol": "hsrp",
            "group_id": 10,
            "name": "hsrp",
            "device": "device-1",
            "interface": "Vlan100",
            "vrf": None,
            "address": "10.0.0.1/32",
            "state": "MASTER",
            "priority": 100,
            "status": "active",
        }
        runner._apply_ipam_fhrpgroup(row)
        runner._get_unique_or_raise(IPAddress, {"address": row["address"], "vrf": None})

        with patch("ipam.models.IPAddress.objects.filter", side_effect=AssertionError):
            self.assertTrue(runner._delete_ipam_fhrpgroup(row))

        self.assertEqual(FHRPGroupAssignment.objects.count(), 0)
        self.assertEqual(FHRPGroup.objects.count(), 0)
        self.assertEqual(IPAddress.objects.count(), 0)

    def test_apply_ipam_fhrpgroup_multiple_participants_share_group_and_vip(self):
        device_1 = self._create_device("device-1")
        device_2 = self._create_device("device-2")
        interface_1 = Interface.objects.create(
            device=device_1,
            name="Vlan100",
            type="virtual",
        )
        interface_2 = Interface.objects.create(
            device=device_2,
            name="Vlan100",
            type="virtual",
        )
        runner = ForwardSyncRunner(
            sync=self.sync, ingestion=None, client=None, logger_=Mock()
        )
        row = {
            "protocol": "hsrp",
            "group_id": 10,
            "name": "hsrp",
            "device": "device-1",
            "interface": "Vlan100",
            "vrf": None,
            "address": "10.0.0.1/32",
            "state": "MASTER",
            "priority": 110,
            "status": "active",
        }

        runner._apply_ipam_fhrpgroup(row)
        runner._apply_ipam_fhrpgroup(
            {**row, "device": "device-2", "state": "BACKUP", "priority": 90}
        )

        group = FHRPGroup.objects.get()
        self.assertEqual(FHRPGroupAssignment.objects.count(), 2)
        self.assertEqual(IPAddress.objects.count(), 1)
        self.assertEqual(IPAddress.objects.get().assigned_object, group)
        assignments = {
            assignment.interface: assignment.priority
            for assignment in FHRPGroupAssignment.objects.all()
        }
        self.assertEqual(assignments, {interface_1: 110, interface_2: 90})

    def test_delete_ipam_fhrpgroup_keeps_shared_group_until_last_assignment(self):
        device_1 = self._create_device("device-1")
        device_2 = self._create_device("device-2")
        Interface.objects.create(device=device_1, name="Vlan100", type="virtual")
        Interface.objects.create(device=device_2, name="Vlan100", type="virtual")
        runner = ForwardSyncRunner(
            sync=self.sync, ingestion=None, client=None, logger_=Mock()
        )
        row = {
            "protocol": "hsrp",
            "group_id": 10,
            "name": "hsrp",
            "device": "device-1",
            "interface": "Vlan100",
            "vrf": None,
            "address": "10.0.0.1/32",
            "state": "MASTER",
            "priority": 110,
            "status": "active",
        }
        runner._apply_ipam_fhrpgroup(row)
        runner._apply_ipam_fhrpgroup(
            {**row, "device": "device-2", "state": "BACKUP", "priority": 90}
        )

        first_deleted = runner._delete_ipam_fhrpgroup(row)

        self.assertTrue(first_deleted)
        self.assertEqual(FHRPGroup.objects.count(), 1)
        self.assertEqual(FHRPGroupAssignment.objects.count(), 1)
        self.assertEqual(IPAddress.objects.count(), 1)

        second_deleted = runner._delete_ipam_fhrpgroup({**row, "device": "device-2"})

        self.assertTrue(second_deleted)
        self.assertEqual(FHRPGroup.objects.count(), 0)
        self.assertEqual(FHRPGroupAssignment.objects.count(), 0)
        self.assertEqual(IPAddress.objects.count(), 0)

    def test_apply_ipam_fhrpgroup_separates_same_group_and_vip_by_vrf(self):
        device = self._create_device("device-1")
        Interface.objects.create(device=device, name="Vlan100", type="virtual")
        Interface.objects.create(device=device, name="Vlan200", type="virtual")
        runner = ForwardSyncRunner(
            sync=self.sync, ingestion=None, client=None, logger_=Mock()
        )
        row = {
            "protocol": "hsrp",
            "group_id": 10,
            "name": "hsrp",
            "device": "device-1",
            "interface": "Vlan100",
            "vrf": "blue",
            "address": "10.0.0.1/32",
            "state": "MASTER",
            "priority": 100,
            "status": "active",
        }

        runner._apply_ipam_fhrpgroup(row)
        runner._apply_ipam_fhrpgroup({**row, "interface": "Vlan200", "vrf": "red"})

        self.assertEqual(VRF.objects.count(), 2)
        self.assertEqual(FHRPGroup.objects.count(), 2)
        self.assertEqual(FHRPGroupAssignment.objects.count(), 2)
        self.assertEqual(IPAddress.objects.count(), 2)
        self.assertEqual(
            set(FHRPGroup.objects.values_list("name", flat=True)),
            {"hsrp-10-blue-10.0.0.1", "hsrp-10-red-10.0.0.1"},
        )
        self.assertEqual(
            set(IPAddress.objects.values_list("vrf__name", flat=True)),
            {"blue", "red"},
        )

    def test_apply_ipam_fhrpgroup_does_not_steal_existing_interface_ip(self):
        device = self._create_device("device-1")
        interface = Interface.objects.create(
            device=device,
            name="Vlan100",
            type="virtual",
        )
        existing_ip = IPAddress.objects.create(
            address="10.0.0.1/24",
            vrf=None,
            status="active",
            assigned_object_type=ContentType.objects.get_for_model(Interface),
            assigned_object_id=interface.pk,
        )
        logger = Mock()
        runner = ForwardSyncRunner(
            sync=self.sync, ingestion=None, client=None, logger_=logger
        )

        runner._apply_model_rows(
            "ipam.fhrpgroup",
            [
                {
                    "protocol": "hsrp",
                    "group_id": 10,
                    "name": "hsrp",
                    "device": "device-1",
                    "interface": "Vlan100",
                    "vrf": None,
                    "address": "10.0.0.1/32",
                    "state": "MASTER",
                    "priority": 100,
                    "status": "active",
                }
            ],
        )

        existing_ip.refresh_from_db()
        self.assertEqual(existing_ip.assigned_object, interface)
        self.assertEqual(str(existing_ip.address), "10.0.0.1/24")
        self.assertEqual(FHRPGroup.objects.count(), 0)
        self.assertEqual(FHRPGroupAssignment.objects.count(), 0)
        self.assertEqual(IPAddress.objects.count(), 1)
        warning_messages = [call.args[0] for call in logger.log_warning.call_args_list]
        self.assertTrue(
            any("assigned to another object" in msg for msg in warning_messages)
        )
        logger.increment_statistics.assert_any_call("ipam.fhrpgroup", outcome="skipped")

    def test_fhrp_state_flip_does_not_churn_group(self):
        # Regression (REDACTED's 13/13 churn): HSRP uses `select distinct row` in
        # NQE, so any state change (MASTER→BACKUP) emits DELETED+ADDED per router.
        # Applying those DELETE rows through delete_ipam_fhrpgroup removes valid
        # FHRPGroupAssignments; when all assignments are gone the group is deleted,
        # producing Created:N / Deleted:N every sync.
        #
        # Fix: _split_diff_rows deduplicates DELETED rows whose group identity
        # also appears in an UPSERT row (same diff batch).
        from forward_netbox.utilities.sync_contracts import (
            default_coalesce_fields_for_model,
        )

        device1 = self._create_device("device-1")
        device2 = self._create_device("device-2")
        Interface.objects.create(device=device1, name="Vlan100", type="virtual")
        Interface.objects.create(device=device2, name="Vlan100", type="virtual")
        runner = ForwardSyncRunner(
            sync=self.sync, ingestion=None, client=None, logger_=Mock()
        )
        # Prime coalesce fields (normally set by the fetch execution layer)
        runner._model_coalesce_fields["ipam.fhrpgroup"] = (
            default_coalesce_fields_for_model("ipam.fhrpgroup")
        )

        row_base = {
            "protocol": "hsrp",
            "group_id": 10,
            "name": "hsrp",
            "vrf": None,
            "address": "10.0.0.1/32",
            "priority": 100,
            "status": "active",
        }

        # First sync: establish two router members
        runner._apply_model_rows(
            "ipam.fhrpgroup",
            [
                {
                    **row_base,
                    "device": "device-1",
                    "interface": "Vlan100",
                    "state": "MASTER",
                },
                {
                    **row_base,
                    "device": "device-2",
                    "interface": "Vlan100",
                    "state": "BACKUP",
                },
            ],
        )
        self.assertEqual(FHRPGroup.objects.count(), 1)
        self.assertEqual(FHRPGroupAssignment.objects.count(), 2)
        group_pk = FHRPGroup.objects.get().pk

        # Simulate the incremental diff after HSRP state flips: each router emits
        # DELETED (old state row) + ADDED (new state row).
        state_flip_diff = [
            {
                "type": "DELETED",
                "before": {
                    **row_base,
                    "device": "device-1",
                    "interface": "Vlan100",
                    "state": "MASTER",
                },
                "after": None,
            },
            {
                "type": "ADDED",
                "before": None,
                "after": {
                    **row_base,
                    "device": "device-1",
                    "interface": "Vlan100",
                    "state": "BACKUP",
                },
            },
            {
                "type": "DELETED",
                "before": {
                    **row_base,
                    "device": "device-2",
                    "interface": "Vlan100",
                    "state": "BACKUP",
                },
                "after": None,
            },
            {
                "type": "ADDED",
                "before": None,
                "after": {
                    **row_base,
                    "device": "device-2",
                    "interface": "Vlan100",
                    "state": "MASTER",
                },
            },
        ]

        upsert_rows, delete_rows = runner._split_diff_rows(
            "ipam.fhrpgroup", state_flip_diff
        )

        self.assertEqual(len(upsert_rows), 2)
        self.assertEqual(
            delete_rows,
            [],
            "state-flip DELETED rows must be suppressed when group identity re-appears in upsert_rows",
        )

        # Applying the upserts must NOT destroy the group.
        runner._apply_model_rows("ipam.fhrpgroup", upsert_rows)
        self.assertEqual(FHRPGroup.objects.count(), 1)
        self.assertEqual(FHRPGroup.objects.get().pk, group_pk)
        self.assertEqual(FHRPGroupAssignment.objects.count(), 2)

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

    def test_ensure_site_reuses_cache_after_first_resolution(self):
        Site.objects.create(name="site-1", slug="site-1")
        runner = ForwardSyncRunner(
            sync=self.sync, ingestion=None, client=None, logger_=Mock()
        )

        first = runner._ensure_site({"name": "site-1", "slug": "site-1"})
        with CaptureQueriesContext(connection) as queries:
            second = runner._ensure_site({"name": "site-1", "slug": "site-1"})

        self.assertEqual(first.pk, second.pk)
        self.assertEqual(Site.objects.filter(slug="site-1").count(), 1)
        self.assertEqual(len(queries), 0)

    def test_global_prefix_lookup_reuses_cache_after_first_resolution(self):
        Prefix.objects.create(prefix="192.0.2.0/24", status="active")
        runner = ForwardSyncRunner(
            sync=self.sync, ingestion=None, client=None, logger_=Mock()
        )
        lookup = {"prefix": "192.0.2.0/24", "vrf__isnull": True}

        first = runner._get_unique_or_raise(Prefix, lookup)
        with CaptureQueriesContext(connection) as queries:
            second = runner._get_unique_or_raise(Prefix, lookup)

        self.assertEqual(first.pk, second.pk)
        self.assertEqual(Prefix.objects.filter(prefix="192.0.2.0/24").count(), 1)
        self.assertEqual(len(queries), 0)

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
        self.assertEqual(len(queries), 0)

    def test_delete_dcim_devicetype_reuses_cached_manufacturer_lookup(self):
        Manufacturer.objects.create(name="Cisco", slug="cisco")
        runner = ForwardSyncRunner(
            sync=self.sync, ingestion=None, client=None, logger_=Mock()
        )
        row = {
            "manufacturer_slug": "cisco",
            "model": "WS-C4507R-E",
            "slug": "ws-c4507r-e",
        }

        with patch.object(runner, "_delete_by_coalesce", return_value=False):
            self.assertFalse(runner._delete_dcim_devicetype(row))
            with CaptureQueriesContext(connection) as queries:
                self.assertFalse(runner._delete_dcim_devicetype(row))

        self.assertEqual(len(queries), 0)

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
            issue.coalesce_fields,
            {"type": "mapping", "fields": ["site"]},
        )
        self.assertEqual(issue.defaults, {"type": "mapping", "fields": ["router"]})
        self.assertEqual(
            issue.raw_data, {"type": "mapping", "fields": ["device", "site"]}
        )
        self.assertNotIn("site-1", str(issue.coalesce_fields))

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
            "Forward snapshot metrics fetch failed (RuntimeError).",
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

    def test_run_prefix_only_fresh_sync_imports_prefix_rows(self):
        seed_builtin_nqe_maps(type("Sender", (), {"label": "forward_netbox"}))
        parameters = {
            "snapshot_id": "snapshot-prefix",
            "enable_bulk_orm": False,
            **{model_string: False for model_string in FORWARD_SUPPORTED_MODELS},
            "ipam.prefix": True,
        }
        self.sync.parameters = parameters
        self.sync.save(update_fields=["parameters"])
        self.assertEqual(self.sync.get_model_strings(), ["ipam.prefix"])

        ingestion = ForwardIngestion.objects.create(sync=self.sync)
        client = Mock()
        client.get_snapshots.return_value = [
            {
                "id": "snapshot-prefix",
                "state": "PROCESSED",
                "created_at": "",
                "processed_at": "2026-06-02T04:52:00Z",
            }
        ]
        client.get_snapshot_metrics.return_value = {}
        client.run_nqe_query.side_effect = [
            [{"prefix": "10.0.0.0/24", "vrf": None, "status": "active"}],
            [{"prefix": "2001:db8::/64", "vrf": None, "status": "active"}],
        ]
        runner = ForwardSyncRunner(
            sync=self.sync,
            ingestion=ingestion,
            client=client,
            logger_=Mock(),
        )

        runner.run()
        ingestion.refresh_from_db()

        self.assertEqual(ingestion.sync_mode, "full")
        self.assertTrue(Prefix.objects.filter(prefix="10.0.0.0/24").exists())
        self.assertTrue(Prefix.objects.filter(prefix="2001:db8::/64").exists())
        self.assertEqual(client.run_nqe_query.call_count, 2)
        called_queries = {
            call.kwargs["query"] for call in client.run_nqe_query.call_args_list
        }
        self.assertTrue(
            all("forward_netbox_shard_keys" in query for query in called_queries)
        )

    def test_run_prefix_only_repeat_sync_is_noop(self):
        seed_builtin_nqe_maps(type("Sender", (), {"label": "forward_netbox"}))
        parameters = {
            "snapshot_id": "snapshot-prefix",
            "enable_bulk_orm": False,
            **{model_string: False for model_string in FORWARD_SUPPORTED_MODELS},
            "ipam.prefix": True,
        }
        self.sync.parameters = parameters
        self.sync.save(update_fields=["parameters"])
        self.assertEqual(self.sync.get_model_strings(), ["ipam.prefix"])

        first_client = Mock()
        first_client.get_snapshots.return_value = [
            {
                "id": "snapshot-prefix",
                "state": "PROCESSED",
                "created_at": "",
                "processed_at": "2026-06-02T04:52:00Z",
            }
        ]
        first_client.get_snapshot_metrics.return_value = {}
        first_client.run_nqe_query.side_effect = [
            [{"prefix": "10.0.0.0/24", "vrf": None, "status": "active"}],
            [{"prefix": "2001:db8::/64", "vrf": None, "status": "active"}],
        ]
        second_client = Mock()
        second_client.get_snapshots.return_value = [
            {
                "id": "snapshot-prefix",
                "state": "PROCESSED",
                "created_at": "",
                "processed_at": "2026-06-02T04:52:00Z",
            }
        ]
        second_client.get_snapshot_metrics.return_value = {}
        second_client.run_nqe_query.side_effect = [
            [{"prefix": "10.0.0.0/24", "vrf": None, "status": "active"}],
            [{"prefix": "2001:db8::/64", "vrf": None, "status": "active"}],
        ]

        with patch(
            "forward_netbox.utilities.sync_execution.get_query_specs",
            return_value=[
                QuerySpec(
                    model_string="ipam.prefix",
                    query_name="Forward IPv4 Prefixes",
                    query='select {prefix: "10.0.0.0/24", vrf: null, status: "active"}',
                ),
                QuerySpec(
                    model_string="ipam.prefix",
                    query_name="Forward IPv6 Prefixes",
                    query='select {prefix: "2001:db8::/64", vrf: null, status: "active"}',
                ),
            ],
        ):
            first_logger = Mock()
            first_runner = ForwardSyncRunner(
                sync=self.sync,
                ingestion=ForwardIngestion.objects.create(sync=self.sync),
                client=first_client,
                logger_=first_logger,
            )
            first_runner.run()

            second_logger = Mock()
            second_runner = ForwardSyncRunner(
                sync=self.sync,
                ingestion=ForwardIngestion.objects.create(sync=self.sync),
                client=second_client,
                logger_=second_logger,
            )
            second_runner.run()

        first_logger.increment_statistics.assert_any_call(
            "ipam.prefix", outcome="applied"
        )
        second_logger.increment_statistics.assert_any_call(
            "ipam.prefix", outcome="unchanged"
        )
        self.assertEqual(
            [
                call.kwargs.get("outcome")
                for call in second_logger.increment_statistics.call_args_list
                if call.args == ("ipam.prefix",)
            ],
            ["unchanged", "unchanged"],
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
        client.get_snapshots.return_value = [
            {"id": "snapshot-before", "state": "PROCESSED"},
            {"id": "snapshot-after", "state": "PROCESSED"},
        ]
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
        self.sync.get_snapshot_id = lambda: LATEST_PROCESSED_SNAPSHOT
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

    def test_run_updates_existing_rows_from_nqe_diff_modifications(self):
        Site.objects.create(name="site-before", slug="site-update", status="active")

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
                "type": "MODIFIED",
                "before": {"name": "site-before", "slug": "site-update"},
                "after": {"name": "site-after", "slug": "site-update"},
            },
        ]
        logger = Mock()
        runner = ForwardSyncRunner(
            sync=self.sync,
            ingestion=ingestion,
            client=client,
            logger_=logger,
        )
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
            before_snapshot_id=baseline.snapshot_id,
            after_snapshot_id="snapshot-after",
            fetch_all=True,
        )
        client.run_nqe_query.assert_not_called()

        site = Site.objects.get(slug="site-update")
        self.assertEqual(site.name, "site-after")
        self.assertEqual(site.status, "active")
        ingestion.refresh_from_db()
        self.assertEqual(ingestion.sync_mode, "diff")
        self.assertEqual(Site.objects.filter(slug="site-update").count(), 1)

    def test_run_updates_existing_rows_when_diff_changing_identity_key(self):
        original_site = Site.objects.create(
            name="site-before",
            slug="site-before",
            status="active",
        )

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
                "type": "MODIFIED",
                "before": {"name": "site-before", "slug": "site-before"},
                "after": {"name": "site-after", "slug": "site-after"},
            },
        ]
        logger = Mock()
        runner = ForwardSyncRunner(
            sync=self.sync,
            ingestion=ingestion,
            client=client,
            logger_=logger,
        )
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
            before_snapshot_id=baseline.snapshot_id,
            after_snapshot_id="snapshot-after",
            fetch_all=True,
        )
        client.run_nqe_query.assert_not_called()

        site_by_old_slug = Site.objects.filter(slug="site-before").first()
        updated_site = Site.objects.get(slug="site-after")
        self.assertIsNone(site_by_old_slug)
        self.assertNotEqual(original_site.pk, updated_site.pk)
        self.assertEqual(updated_site.name, "site-after")
        self.assertEqual(updated_site.status, "active")
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
        self.sync.get_snapshot_id = lambda: LATEST_PROCESSED_SNAPSHOT
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
        self.assertEqual(fetch_meta["fetch_mode"], "nqe_parameters")
        self.assertEqual(
            client.run_nqe_query.call_args.kwargs["parameters"],
            {"forward_netbox_shard_keys": ["device-a"]},
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
        client.run_nqe_query.assert_called_once()
        self.assertEqual(
            client.run_nqe_query.call_args.kwargs["parameters"],
            {
                "forward_netbox_shard_keys": [
                    "2400:9500::/32",
                    "2401:e800:7100::/40",
                ]
            },
        )

    def test_apply_context_tag_parameters_injects_sync_device_tags(self):
        # The operator's selected Forward tags (context.sync_device_tags) must be
        # injected into any query that declares the sync_device_tags parameter, and
        # sorted so query results are deterministic across runs.
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
            sync_device_tags=["Prod_Core", "Mgmt_Vl211"],
        )
        spec = QuerySpec(
            model_string="extras.taggeditem",
            query_name="Forward Device Feature Tags",
            query=(
                "@query f(forward_netbox_shard_keys: List<String>, "
                "sync_device_tags: List<String>) = []"
            ),
            parameters={"forward_netbox_shard_keys": [], "sync_device_tags": []},
        )

        resolved = fetcher._apply_context_tag_parameters(
            spec, {"forward_netbox_shard_keys": []}, context
        )

        self.assertEqual(resolved["sync_device_tags"], ["Mgmt_Vl211", "Prod_Core"])

    def test_apply_context_tag_parameters_scopes_dlm_vulnerabilities(self):
        fetcher = ForwardQueryFetcher(
            sync=self.sync,
            client=Mock(),
            logger_=Mock(),
        )
        context = ForwardQueryContext(
            network_id="test-network",
            snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
            snapshot_id="snapshot-before",
            scoped_device_names={"core-2", "core-1"},
        )
        spec = QuerySpec(
            model_string="netbox_dlm.vulnerability",
            query_name="Forward DLM Vulnerabilities",
            query="@query f(forward_netbox_shard_keys: List<String>) = []",
            parameters={"forward_netbox_shard_keys": []},
        )

        resolved = fetcher._apply_context_tag_parameters(
            spec, {"forward_netbox_shard_keys": []}, context
        )

        self.assertEqual(resolved["forward_netbox_shard_keys"], ["core-1", "core-2"])

    def test_apply_context_tag_parameters_injects_generic_endpoint_policy(self):
        fetcher = ForwardQueryFetcher(
            sync=self.sync,
            client=Mock(),
            logger_=Mock(),
        )
        context = ForwardQueryContext(
            network_id="test-network",
            snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
            snapshot_id="snapshot-before",
            sync_endpoints=True,
            sync_generic_endpoints=True,
        )
        spec = QuerySpec(
            model_string="dcim.device",
            query_name="Forward Devices",
            query=(
                "@query f(sync_endpoints: Bool, " "sync_generic_endpoints: Bool) = []"
            ),
            parameters={
                "sync_endpoints": False,
                "sync_generic_endpoints": False,
            },
        )

        resolved = fetcher._apply_context_tag_parameters(spec, {}, context)

        self.assertIs(resolved["sync_endpoints"], True)
        self.assertIs(resolved["sync_generic_endpoints"], True)

    def test_apply_context_tag_parameters_skips_sync_device_tags_when_undeclared(self):
        # A query that does not declare sync_device_tags must not receive it, or the
        # Forward engine rejects the fetch with an unexpected-parameter error.
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
            sync_device_tags=["Mgmt_Vl211"],
        )
        spec = QuerySpec(
            model_string="dcim.interface",
            query_name="Forward Interfaces",
            query="@query f(forward_netbox_shard_keys: List<String>) = []",
            parameters={"forward_netbox_shard_keys": []},
        )

        resolved = fetcher._apply_context_tag_parameters(
            spec, {"forward_netbox_shard_keys": []}, context
        )

        self.assertNotIn("sync_device_tags", resolved)

    def test_fetch_spec_rows_rejects_unsupported_fetch_mode(self):
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
            query_name="Forward IPv4 Prefixes",
            query="@query f() = []",
        )
        shard_scope = {
            "fetch_mode": "unsupported",
            "fetch_key_family": "prefix",
            "shard_keys": ["prefix=10.0.0.0/24"],
        }

        with self.assertRaisesRegex(
            ForwardQueryError,
            "Unsupported shard fetch mode `unsupported`",
        ):
            fetcher._fetch_spec_rows(
                "ipam.prefix",
                spec,
                baseline=None,
                context=context,
                coalesce_fields=[["prefix"]],
                shard_scope=shard_scope,
            )

        client.run_nqe_query.assert_not_called()
        client.run_nqe_diff.assert_not_called()

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
        fetcher._run_nqe_query = Mock()

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

        self.assertFalse(fetcher._run_nqe_query.called)

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
        fetcher._run_nqe_diff = Mock(side_effect=ForwardClientError("diff timeout"))
        fetcher._run_nqe_query = Mock()

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

        self.assertEqual(fetcher._run_nqe_diff.call_count, 1)
        self.assertFalse(fetcher._run_nqe_query.called)

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

    def test_workload_planning_uses_full_for_all_maps_when_one_is_parameterized(self):
        fetcher = ForwardQueryFetcher(
            sync=self.sync,
            client=Mock(),
            logger_=Mock(),
        )
        specs = [
            QuerySpec(
                model_string="dcim.interface",
                query_name="Parameterized Interfaces",
                query_id="Q_parameterized",
                parameters={"scope": []},
            ),
            QuerySpec(
                model_string="dcim.interface",
                query_name="Parameterless Interfaces",
                query_id="Q_parameterless",
            ),
        ]
        baseline = Mock(pk=7, snapshot_id="snapshot-before")
        context = ForwardQueryContext(
            network_id="test-network",
            snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
            snapshot_id="snapshot-after",
        )
        fetcher._drop_unavailable_integration_models = Mock(
            return_value=["dcim.interface"]
        )
        fetcher._resolve_specs_for_models = Mock(
            return_value=({"dcim.interface": specs}, {})
        )
        fetcher._incremental_baseline_for_specs = Mock(return_value=baseline)
        fetcher._scope_for_spec = Mock(return_value=None)

        jobs = fetcher._build_workload_jobs(
            context,
            model_strings=["dcim.interface"],
        )

        self.assertEqual(len(jobs), 2)
        self.assertTrue(all(job[2] is None for job in jobs))

    def test_workload_planning_rejects_mixed_parameterized_model_before_fetch(self):
        self.sync.parameters["diff_fallback_mode"] = (
            ForwardDiffFallbackModeChoices.REQUIRE_DIFF
        )
        fetcher = ForwardQueryFetcher(
            sync=self.sync,
            client=Mock(),
            logger_=Mock(),
        )
        specs = [
            QuerySpec(
                model_string="dcim.interface",
                query_name="Parameterized Interfaces",
                query_id="Q_parameterized",
                parameters={"scope": []},
            ),
            QuerySpec(
                model_string="dcim.interface",
                query_name="Parameterless Interfaces",
                query_id="Q_parameterless",
            ),
        ]
        context = ForwardQueryContext(
            network_id="test-network",
            snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
            snapshot_id="snapshot-after",
        )
        fetcher._drop_unavailable_integration_models = Mock(
            return_value=["dcim.interface"]
        )
        fetcher._resolve_specs_for_models = Mock(
            return_value=({"dcim.interface": specs}, {})
        )
        fetcher._incremental_baseline_for_specs = Mock(
            return_value=Mock(pk=7, snapshot_id="snapshot-before")
        )
        fetcher._scope_for_spec = Mock(return_value=None)

        jobs = fetcher._build_workload_jobs(
            context,
            model_strings=["dcim.interface"],
        )

        self.assertEqual(jobs, [])
        self.assertIn("dcim.interface", fetcher._failed_model_results)
        fetcher.client.run_nqe_query.assert_not_called()
        fetcher.client.run_nqe_diff.assert_not_called()

    def test_workload_planning_rejects_duplicates_after_context_parameters(self):
        fetcher = ForwardQueryFetcher(
            sync=self.sync,
            client=Mock(),
            logger_=Mock(),
        )
        specs = [
            QuerySpec(
                model_string="dcim.device",
                query_name="Forward Devices A",
                query_id="Q_devices",
                parameters={"sync_endpoints": False},
            ),
            QuerySpec(
                model_string="dcim.device",
                query_name="Forward Devices B",
                query_id="Q_devices",
                parameters={"sync_endpoints": True},
            ),
        ]
        context = ForwardQueryContext(
            network_id="test-network",
            snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
            snapshot_id="snapshot-after",
            sync_endpoints=True,
        )
        fetcher._drop_unavailable_integration_models = Mock(
            return_value=["dcim.device"]
        )
        fetcher._resolve_specs_for_models = Mock(
            return_value=({"dcim.device": specs}, {})
        )
        fetcher._incremental_baseline_for_specs = Mock(return_value=None)
        fetcher._scope_for_spec = Mock(return_value=None)

        jobs = fetcher._build_workload_jobs(
            context,
            model_strings=["dcim.device"],
        )

        self.assertEqual(jobs, [])
        self.assertIn("dcim.device", fetcher._failed_model_results)

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
        client.run_nqe_query.return_value = [
            {"name": "core-1", "site": "main dc"},
            {"name": "core-2", "site": "main dc"},
        ]
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
        self.assertEqual(context.scoped_site_names, {"main dc", "main-dc"})

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

    def test_apply_device_tag_scope_filters_site_rows_by_tagged_device_sites(self):
        fetcher = ForwardQueryFetcher(
            sync=self.sync,
            client=Mock(),
            logger_=Mock(),
        )
        context = ForwardQueryContext(
            network_id="test-network",
            snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
            snapshot_id="snapshot-after",
            device_tag_include_tags=["Core"],
            scoped_device_names={"core-1"},
            scoped_site_names={"main dc", "main-dc"},
        )
        rows = [
            {"name": "main dc", "slug": "main-dc"},
            {"name": "branch", "slug": "branch"},
        ]

        filtered, removed = fetcher._apply_device_tag_scope("dcim.site", rows, context)

        self.assertEqual(filtered, [{"name": "main dc", "slug": "main-dc"}])
        self.assertEqual(removed, [{"name": "branch", "slug": "branch"}])

    def test_apply_device_tag_scope_zero_matches_does_not_keep_broad_rows(self):
        fetcher = ForwardQueryFetcher(
            sync=self.sync,
            client=Mock(),
            logger_=Mock(),
        )
        context = ForwardQueryContext(
            network_id="test-network",
            snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
            snapshot_id="snapshot-after",
            device_tag_include_tags=["Core"],
            scoped_device_names=set(),
        )
        rows = [
            {"device": "branch-1", "name": "Ethernet1"},
            {"name": "branch", "slug": "branch"},
        ]

        filtered, removed = fetcher._apply_device_tag_scope("dcim.site", rows, context)

        self.assertEqual(filtered, [])
        self.assertEqual(removed, rows)

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

    def test_apply_device_tag_scope_filters_dlm_vulnerabilities_by_name(self):
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
            {"name": "core-1", "cve_id": "CVE-2026-0001"},
            {"name": "branch-1", "cve_id": "CVE-2026-0002"},
        ]

        filtered, removed = fetcher._apply_device_tag_scope(
            "netbox_dlm.vulnerability", rows, context
        )

        self.assertEqual(filtered, [rows[0]])
        self.assertEqual(removed, [rows[1]])

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
        fetcher._run_nqe_query = Mock(
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
        self.assertEqual(
            ingestion.issues.first().message,
            "dcim.device row processing failed (ForwardQueryError).",
        )

    def test_record_issue_redacts_unexpected_failure_content(self):
        ingestion = ForwardIngestion.objects.create(sync=self.sync)
        logger = Mock()
        runner = ForwardSyncRunner(
            sync=self.sync,
            ingestion=ingestion,
            client=None,
            logger_=logger,
        )
        sentinel = "sentinel-private-detail"

        issue = runner._record_issue(
            "dcim.device",
            sentinel,
            {"name": sentinel, "serial": sentinel},
            exception=RuntimeError(sentinel),
            defaults={"device_type": sentinel},
            context={"site": sentinel},
        )

        self.assertEqual(
            issue.message,
            "dcim.device row processing failed (RuntimeError).",
        )
        self.assertEqual(
            issue.raw_data,
            {"type": "mapping", "fields": ["name", "serial"]},
        )
        self.assertEqual(
            issue.coalesce_fields,
            {"type": "mapping", "fields": ["site"]},
        )
        self.assertEqual(
            issue.defaults,
            {"type": "mapping", "fields": ["device_type"]},
        )
        self.assertNotIn(sentinel, str(issue.__dict__))
        self.assertNotIn(sentinel, str(logger.mock_calls))

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

    def test_delete_model_rows_persists_successful_delete_statistics(self):
        ingestion = ForwardIngestion.objects.create(sync=self.sync)
        runner = ForwardSyncRunner(
            sync=self.sync, ingestion=ingestion, client=None, logger_=Mock()
        )
        runner._delete_dcim_site = Mock(side_effect=[True, False, True])

        runner._delete_model_rows(
            "dcim.site",
            [
                {"name": "site-1", "slug": "site-1"},
                {"name": "site-2", "slug": "site-2"},
                {"name": "site-3", "slug": "site-3"},
            ],
        )

        ingestion.refresh_from_db()
        self.assertEqual(ingestion.applied_change_count, 2)
        self.assertEqual(ingestion.deleted_change_count, 2)
        runner.logger.increment_statistics.assert_any_call(
            "dcim.site", outcome="skipped"
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

    def test_apply_model_rows_emits_job_log_progress_heartbeat(self):
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

        logger.log_info.assert_any_call(
            "Applying 1/2 rows for dcim.site.", obj=self.sync
        )
        logger.log_info.assert_any_call(
            "Applying 2/2 rows for dcim.site.", obj=self.sync
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

    def test_apply_virtual_chassis_repeat_sync_is_noop(self):
        site = Site.objects.create(name="site-4", slug="site-4")
        manufacturer = Manufacturer.objects.create(name="vendor-4", slug="vendor-4")
        role = DeviceRole.objects.create(name="role-4", slug="role-4", color="9e9e9e")
        device_type = DeviceType.objects.create(
            manufacturer=manufacturer,
            model="model-4",
            slug="model-4",
        )
        vc = VirtualChassis.objects.create(name="site-4-vpc-400", domain="400")
        device = Device.objects.create(
            name="device-4",
            site=site,
            role=role,
            device_type=device_type,
            status="active",
            virtual_chassis=vc,
            vc_position=4,
        )
        runner = ForwardSyncRunner(
            sync=self.sync, ingestion=None, client=None, logger_=Mock()
        )
        row = {
            "device": device.name,
            "vc_name": vc.name,
            "vc_domain": vc.domain,
            "vc_position": 4,
        }

        before_count = ObjectChange.objects.count()
        with CaptureQueriesContext(connection) as queries:
            runner._apply_dcim_virtualchassis(row)
            runner._apply_dcim_virtualchassis(row)

        device.refresh_from_db()
        self.assertEqual(device.virtual_chassis, vc)
        self.assertEqual(device.vc_position, 4)
        self.assertEqual(self._update_statements(queries), [])
        self.assertEqual(ObjectChange.objects.count(), before_count)

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
        )

    def _virtual_chassis_engine(self):
        return select_apply_engine(
            sync=self.sync,
            model_string="dcim.virtualchassis",
        )

    def _device_decision(self):
        return apply_engine_decision_for(
            sync=self.sync,
            model_string="dcim.device",
        )

    def _prefix_decision(self):
        return apply_engine_decision_for(
            sync=self.sync,
            model_string="ipam.prefix",
        )

    def _prefix_engine(self):
        return select_apply_engine(
            sync=self.sync,
            model_string="ipam.prefix",
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

    def test_netbox_routing_bgppeer_delete_parity(self):
        if not apps.is_installed("netbox_routing"):
            self.skipTest("netbox-routing optional plugin is not installed")
        BGPPeer = apps.get_model("netbox_routing", "BGPPeer")
        BGPRouter = apps.get_model("netbox_routing", "BGPRouter")
        BGPScope = apps.get_model("netbox_routing", "BGPScope")
        device = self._device("device-bgp-delete")
        ASN.objects.create(rir=RIR.objects.create(name="ARIN"), asn=64512)
        runner = self._runner()
        row = {
            "device": device.name,
            "vrf": None,
            "local_asn": 64512,
            "neighbor_address": "192.0.2.1",
            "peer_asn": 64513,
            "enabled": True,
            "status": "active",
        }

        runner._apply_netbox_routing_bgppeer(row)
        self.assertEqual(BGPPeer.objects.count(), 1)
        self.assertEqual(BGPRouter.objects.count(), 1)
        self.assertEqual(BGPScope.objects.count(), 1)

        deleted = runner._delete_netbox_routing_bgppeer(row)

        self.assertTrue(deleted)
        self.assertFalse(BGPPeer.objects.exists())
        self.assertFalse(BGPRouter.objects.exists())
        self.assertFalse(BGPScope.objects.exists())

    def test_netbox_routing_bgppeeraddressfamily_delete_parity(self):
        if not apps.is_installed("netbox_routing"):
            self.skipTest("netbox-routing optional plugin is not installed")
        BGPPeerAddressFamily = apps.get_model("netbox_routing", "BGPPeerAddressFamily")
        self._device("device-bgp-af-delete")
        runner = self._runner()
        row = {
            "device": "device-bgp-af-delete",
            "vrf": None,
            "local_asn": 64512,
            "neighbor_address": "192.0.2.1",
            "peer_asn": 64513,
            "enabled": True,
            "status": "active",
            "peer_type": "PeerType.EXTERNAL",
            "afi_safi": "AfiSafiType.IPV4_UNICAST",
            "has_adj_rib_in": False,
            "has_adj_rib_out": True,
        }

        runner._apply_netbox_routing_bgppeeraddressfamily(row)
        self.assertEqual(BGPPeerAddressFamily.objects.count(), 1)

        deleted = runner._delete_netbox_routing_bgppeeraddressfamily(row)

        self.assertTrue(deleted)
        self.assertFalse(BGPPeerAddressFamily.objects.exists())

    def test_netbox_routing_bgpaddressfamily_delete_parity(self):
        if not apps.is_installed("netbox_routing"):
            self.skipTest("netbox-routing optional plugin is not installed")
        BGPAddressFamily = apps.get_model("netbox_routing", "BGPAddressFamily")
        self._device("device-bgp-address-family-delete")
        ASN.objects.create(rir=RIR.objects.create(name="ARIN"), asn=64512)
        runner = self._runner()
        row = {
            "device": "device-bgp-address-family-delete",
            "vrf": None,
            "local_asn": 64512,
            "afi_safi": "AfiSafiType.IPV4_UNICAST",
        }

        runner._apply_netbox_routing_bgpaddressfamily(row)
        self.assertEqual(BGPAddressFamily.objects.count(), 1)

        deleted = runner._delete_netbox_routing_bgpaddressfamily(row)

        self.assertTrue(deleted)
        self.assertFalse(BGPAddressFamily.objects.exists())

    def test_netbox_peering_manager_peeringsession_delete_parity(self):
        if not apps.is_installed("netbox_peering_manager"):
            self.skipTest("netbox-peering-manager optional plugin is not installed")
        PeeringSession = apps.get_model("netbox_peering_manager", "PeeringSession")
        BGPPeer = apps.get_model("netbox_routing", "BGPPeer")
        self._device("device-peering-delete")
        runner = self._runner()
        row = {
            "device": "device-peering-delete",
            "vrf": None,
            "local_asn": 65000,
            "neighbor_address": "192.0.2.1",
            "peer_asn": 65100,
            "enabled": True,
            "status": "active",
            "peer_type": "PeerType.EXTERNAL",
        }

        runner._apply_netbox_peering_manager_peeringsession(row)
        self.assertEqual(PeeringSession.objects.count(), 1)
        self.assertEqual(BGPPeer.objects.count(), 1)

        deleted = runner._delete_netbox_peering_manager_peeringsession(row)

        self.assertTrue(deleted)
        self.assertFalse(PeeringSession.objects.exists())
        self.assertTrue(BGPPeer.objects.exists())

    def test_netbox_routing_ospfinstance_delete_parity(self):
        if not apps.is_installed("netbox_routing"):
            self.skipTest("netbox-routing optional plugin is not installed")
        OSPFInstance = apps.get_model("netbox_routing", "OSPFInstance")
        self._device("device-ospf-instance-delete")
        runner = self._runner()
        row = {
            "device": "device-ospf-instance-delete",
            "vrf": None,
            "process_id": "UNDERLAY",
            "domain": "fabric",
            "router_id": "192.0.2.254",
        }

        runner._apply_netbox_routing_ospfinstance(row)
        self.assertEqual(OSPFInstance.objects.count(), 1)

        deleted = runner._delete_netbox_routing_ospfinstance(row)

        self.assertTrue(deleted)
        self.assertFalse(OSPFInstance.objects.exists())

    def test_netbox_routing_ospfarea_delete_parity(self):
        if not apps.is_installed("netbox_routing"):
            self.skipTest("netbox-routing optional plugin is not installed")
        OSPFArea = apps.get_model("netbox_routing", "OSPFArea")
        runner = self._runner()
        row = {
            "area_id": "0",
            "area_type": "OspfAreaType.BACKBONE",
        }

        runner._apply_netbox_routing_ospfarea(row)
        self.assertEqual(OSPFArea.objects.count(), 1)

        deleted = runner._delete_netbox_routing_ospfarea(row)

        self.assertTrue(deleted)
        self.assertFalse(OSPFArea.objects.exists())

    def test_netbox_routing_ospfinterface_delete_parity(self):
        if not apps.is_installed("netbox_routing"):
            self.skipTest("netbox-routing optional plugin is not installed")
        OSPFInterface = apps.get_model("netbox_routing", "OSPFInterface")
        self._device("device-ospf-delete")
        Interface.objects.create(
            device=Device.objects.get(name="device-ospf-delete"),
            name="Ethernet1/1",
            type="1000base-t",
        )
        runner = self._runner()
        row = {
            "device": "device-ospf-delete",
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

        runner._apply_netbox_routing_ospfinterface(row)
        self.assertEqual(OSPFInterface.objects.count(), 1)

        deleted = runner._delete_netbox_routing_ospfinterface(row)

        self.assertTrue(deleted)
        self.assertFalse(OSPFInterface.objects.exists())

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
        self.assertEqual(
            issue.message,
            "dcim.virtualchassis row processing failed (ForwardSyncDataError).",
        )
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
        self.assertEqual(
            issue.message,
            "dcim.virtualchassis row processing failed (ForwardSearchError).",
        )
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
        self.assertEqual(
            issue.message,
            "dcim.virtualchassis row processing failed (ForwardDependencySkipError).",
        )
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

    def test_bulk_tree_lookup_cache_reuses_device_role_lookup_across_rows(self):
        role = DeviceRole.objects.create(
            name="role-1",
            slug="role-1",
            color="9e9e9e",
        )
        runner = self._runner()
        rows = [
            {"name": "role-1", "slug": "role-1", "color": "9e9e9e"},
            {"name": "role-2", "slug": "role-2", "color": "9e9e9e"},
        ]

        with CaptureQueriesContext(connection) as queries:
            self.assertTrue(
                bulk_orm_apply_tree_models(
                    runner=runner,
                    model_string="dcim.devicerole",
                    model=DeviceRole,
                    fields=("name", "slug", "color"),
                    lookup_sets=(("slug",), ("name",)),
                    normalized_rows=rows,
                )
            )

        self.assertEqual(
            DeviceRole.objects.filter(slug__in=["role-1", "role-2"]).count(), 2
        )
        prefetch_selects = [
            query["sql"]
            for query in queries
            if 'FROM "dcim_devicerole"' in query["sql"] and " IN " in query["sql"]
        ]
        self.assertEqual(len(prefetch_selects), 1)
        self.assertEqual(role.pk, DeviceRole.objects.get(slug="role-1").pk)

    def _device_engine(self):
        return select_apply_engine(sync=self.sync, model_string="dcim.device")

    def _device_row(self, name, *, status="active", platform=""):
        return {
            "name": name,
            "site": f"{name}-site",
            "site_slug": f"{name}-site",
            "role": f"{name}-role",
            "role_slug": f"{name}-role",
            "role_color": "9e9e9e",
            "manufacturer": f"{name}-vendor",
            "manufacturer_slug": f"{name}-vendor",
            "device_type": f"{name}-model",
            "device_type_slug": f"{name}-model",
            "platform": platform,
            "platform_slug": platform,
            "status": status,
        }

    def _stage_device_parents(self, name):
        Site.objects.create(name=f"{name}-site", slug=f"{name}-site")
        DeviceRole.objects.create(
            name=f"{name}-role", slug=f"{name}-role", color="9e9e9e"
        )
        mfr = Manufacturer.objects.create(name=f"{name}-vendor", slug=f"{name}-vendor")
        DeviceType.objects.create(
            manufacturer=mfr, model=f"{name}-model", slug=f"{name}-model"
        )

    def test_dcim_device_defaults_to_bulk(self):
        # 2.0 speed: dcim.device is promoted to the bulk apply path.
        self.assertEqual(self._device_decision().selected_engine, "bulk_orm")

    def test_dcim_device_create_parity(self):
        # Parents pre-staged (as the dependency-phased plan does); the bulk path
        # resolves them by FK lookup and creates the device.
        self._stage_device_parents("dev-c")
        runner = self._runner()
        self._device_engine().apply_upserts(
            runner, "dcim.device", [self._device_row("dev-c")]
        )
        device = Device.objects.get(name="dev-c")
        self.assertEqual(device.site.slug, "dev-c-site")
        self.assertEqual(device.role.slug, "dev-c-role")
        self.assertEqual(device.device_type.slug, "dev-c-model")

    def test_dcim_device_bulk_repairs_endpoint_platform_manufacturer(self):
        self._stage_device_parents("endpoint-c")
        platform = Platform.objects.create(
            name="endpoint-c-vendor",
            slug="endpoint-c-vendor",
        )
        row = self._device_row("endpoint-c", platform="endpoint-c-vendor")
        row["platform_manufacturer_authoritative"] = True
        runner = self._runner()

        self._device_engine().apply_upserts(runner, "dcim.device", [row])

        platform.refresh_from_db()
        self.assertEqual(platform.manufacturer.slug, "endpoint-c-vendor")
        self.assertEqual(Device.objects.get(name="endpoint-c").platform_id, platform.pk)

    def test_dcim_device_update_parity(self):
        device = self._device("device-1")
        runner = self._runner()
        self._device_engine().apply_upserts(
            runner, "dcim.device", [self._device_row("device-1", status="offline")]
        )
        device.refresh_from_db()
        self.assertEqual(device.status, "offline")

    def test_dcim_device_unchanged_is_noop(self):
        from django.db import connection
        from django.test.utils import CaptureQueriesContext

        self._device("device-1")
        runner = self._runner()
        with CaptureQueriesContext(connection) as queries:
            self._device_engine().apply_upserts(
                runner, "dcim.device", [self._device_row("device-1")]
            )
        updates = [
            q["sql"]
            for q in queries.captured_queries
            if q["sql"].lstrip().upper().startswith("UPDATE")
        ]
        self.assertEqual(updates, [])
        self.assertEqual(Device.objects.filter(name="device-1").count(), 1)

    def test_dcim_device_bulk_does_not_mutate_owned_status_tags(self):
        device = self._device("device-1")
        out_of_scope = Tag.objects.create(
            name="Forward Out Of Scope",
            slug="forward-out-of-scope",
            color="f44336",
        )
        customer_tag = Tag.objects.create(
            name="Customer Managed",
            slug="customer-managed",
            color="9e9e9e",
        )
        device.tags.add(out_of_scope, customer_tag)
        runner = self._runner()

        self._device_engine().apply_upserts(
            runner, "dcim.device", [self._device_row("device-1")]
        )

        self.assertTrue(device.tags.filter(pk=out_of_scope.pk).exists())
        self.assertTrue(device.tags.filter(pk=customer_tag.pk).exists())

    def test_dcim_device_delegates_when_parent_missing(self):
        # No pre-staged parents -> the bulk path delegates to the adapter, which
        # creates them (exact parity), so the device still lands.
        runner = self._runner()
        self._device_engine().apply_upserts(
            runner, "dcim.device", [self._device_row("dev-d")]
        )
        device = Device.objects.get(name="dev-d")
        self.assertEqual(device.site.slug, "dev-d-site")

    def test_ipam_prefix_defaults_to_bulk(self):
        # Promoted to the default safe set (it runs the per-object tree path that
        # preserves the NetBox hierarchy signal).
        decision = self._prefix_decision()
        self.assertEqual(decision.selected_engine, "bulk_orm")
        self.assertEqual(decision.reason_code, "bulk_orm_enabled_safe_model_set")

    def test_ipam_prefix_bulk_apply_upserts_rows(self):
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

    def test_ipam_prefix_bulk_null_vrf_reapply_no_duplicate(self):
        # Null-VRF (global table) prefix must match an existing row on re-apply
        # instead of creating a duplicate (the composite (prefix, vrf) key encodes
        # the null vrf as a sentinel rather than bailing to None).
        runner = self._runner()
        rows = [{"prefix": "10.20.0.0/24", "vrf": None, "status": "active"}]

        self._prefix_engine().apply_upserts(runner, "ipam.prefix", rows)
        self._prefix_engine().apply_upserts(runner, "ipam.prefix", rows)

        self.assertEqual(
            Prefix.objects.filter(prefix="10.20.0.0/24", vrf=None).count(), 1
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


class QueryParameterContractTest(TestCase):
    @patch(
        "forward_netbox.utilities.plugin_integrations.registry.integration_capability",
        return_value={
            "available": False,
            "availability_status": "unsupported_version",
            "availability_reason": "Canonical package version must be exactly 0.4.3.",
        },
    )
    def test_query_fetch_skips_optional_model_when_exact_contract_is_unavailable(
        self, _capability
    ):
        sync = Mock()
        logger = Mock()
        fetcher = ForwardQueryFetcher(sync=sync, client=Mock(), logger_=logger)

        kept = fetcher._drop_unavailable_integration_models(
            ["dcim.device", "netbox_routing.bgppeer"]
        )

        self.assertEqual(kept, ["dcim.device"])
        logger.log_warning.assert_called_once()
        self.assertIn(
            "unsupported_version",
            logger.log_warning.call_args.args[0],
        )

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
            },
        )

    def test_query_fetch_passes_parameters_without_retry(self):
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
        fetcher.client.run_nqe_query.return_value = [{"name": "device-1"}]

        rows = fetcher._run_nqe_query(
            spec=spec,
            context=context,
            parameters={"device_tag_include_tags": ["Core"]},
            fetch_all=True,
        )

        self.assertEqual(rows, [{"name": "device-1"}])
        self.assertEqual(fetcher.client.run_nqe_query.call_count, 1)
        self.assertEqual(
            fetcher.client.run_nqe_query.call_args.kwargs["parameters"],
            {"device_tag_include_tags": ["Core"]},
        )
        logger.log_info.assert_not_called()
        logger.log_warning.assert_not_called()

    def test_query_fetch_pushes_context_tags_to_tag_capable_specs(self):
        sync = Mock()
        fetcher = ForwardQueryFetcher(sync=sync, client=Mock(), logger_=Mock())
        spec = Mock(
            query="foreach device in network.devices select {name: device.name}",
            run_query_id="qid-1",
            commit_id="cid-1",
            execution_value="qid-1",
            parameters={
                "device_tag_include_tags": [],
                "device_tag_include_match": "any",
                "device_tag_exclude_tags": [],
                "forward_netbox_shard_keys": [],
            },
            merged_parameters=Mock(
                return_value={
                    "device_tag_include_tags": [],
                    "device_tag_include_match": "any",
                    "device_tag_exclude_tags": [],
                    "forward_netbox_shard_keys": [],
                }
            ),
        )
        context = ForwardQueryContext(
            network_id="n1",
            snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
            snapshot_id="s1",
            device_tag_include_tags=["Core", "DC"],
            device_tag_include_match="all",
            device_tag_exclude_tags=["Branch"],
        )
        fetcher._run_nqe_query = Mock(return_value=[])

        fetcher._fetch_spec_rows(
            "ipam.prefix",
            spec,
            baseline=None,
            context=context,
            coalesce_fields=[["prefix", "vrf"]],
        )

        self.assertEqual(
            fetcher._run_nqe_query.call_args.kwargs["parameters"],
            {
                "device_tag_include_tags": ["Core", "DC"],
                "device_tag_include_match": "all",
                "device_tag_exclude_tags": ["Branch"],
                "forward_netbox_shard_keys": [],
            },
        )

    def test_apply_context_tag_parameters_removes_undeclared_tag_parameters(self):
        sync = Mock()
        fetcher = ForwardQueryFetcher(sync=sync, client=Mock(), logger_=Mock())
        spec = Mock(
            parameters={
                "forward_netbox_shard_keys": [],
                "device_tag_include_tags": [],
                "device_tag_include_match": "any",
                "device_tag_exclude_tags": [],
            }
        )
        context = ForwardQueryContext(
            network_id="n1",
            snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
            snapshot_id="s1",
            device_tag_include_tags=["Core"],
            device_tag_include_match="all",
            device_tag_exclude_tags=["Branch"],
        )

        parameters = fetcher._apply_context_tag_parameters(
            spec,
            {
                "forward_netbox_shard_keys": [],
                "device_tag_unrecognized": "Core",
                "device_tag_prune_out_of_scope": True,
            },
            context,
        )

        self.assertEqual(
            parameters,
            {
                "forward_netbox_shard_keys": [],
                "device_tag_include_tags": ["Core"],
                "device_tag_include_match": "all",
                "device_tag_exclude_tags": ["Branch"],
            },
        )

    def test_query_fetch_does_not_push_context_tags_to_unparameterized_specs(self):
        sync = Mock()
        fetcher = ForwardQueryFetcher(sync=sync, client=Mock(), logger_=Mock())
        spec = Mock(
            query="foreach device in network.devices select {name: device.name}",
            run_query_id="qid-plain",
            commit_id="cid-plain",
            execution_value="qid-plain",
            parameters={"forward_netbox_shard_keys": []},
            merged_parameters=Mock(
                return_value={
                    "forward_netbox_shard_keys": [],
                    "device_tag_include_tags": ["Core", "DC"],
                    "device_tag_include_match": "all",
                    "device_tag_exclude_tags": ["Branch"],
                }
            ),
        )
        context = ForwardQueryContext(
            network_id="n1",
            snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
            snapshot_id="s1",
            device_tag_include_tags=["Core", "DC"],
            device_tag_include_match="all",
            device_tag_exclude_tags=["Branch"],
        )
        fetcher._run_nqe_query = Mock(return_value=[])

        fetcher._fetch_spec_rows(
            "dcim.device",
            spec,
            baseline=None,
            context=context,
            coalesce_fields=[["name"]],
        )

        self.assertEqual(
            fetcher._run_nqe_query.call_args.kwargs["parameters"],
            {"forward_netbox_shard_keys": []},
        )

    def test_cable_scope_requires_both_endpoints_to_be_in_scope(self):
        fetcher = ForwardQueryFetcher(sync=Mock(), client=Mock(), logger_=Mock())
        context = ForwardQueryContext(
            network_id="test-network",
            snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
            snapshot_id="snapshot-after",
            scoped_device_names={"device-a", "device-b"},
        )
        rows = [
            {
                "device": "device-a",
                "interface": "Ethernet1",
                "remote_device": "device-b",
                "remote_interface": "Ethernet2",
            },
            {
                "device": "device-a",
                "interface": "Ethernet3",
                "remote_device": "device-out-of-scope",
                "remote_interface": "Ethernet4",
            },
        ]

        kept, removed = fetcher._apply_device_tag_scope("dcim.cable", rows, context)

        self.assertEqual(kept, [rows[0]])
        self.assertEqual(removed, [rows[1]])

    def test_query_fetch_rejects_unsupported_parameters_after_merge(self):
        sync = Mock()
        fetcher = ForwardQueryFetcher(sync=sync, client=Mock(), logger_=Mock())
        spec = Mock(
            query_name="Forward IP Addresses",
            parameters={"forward_netbox_shard_keys": []},
        )

        with self.assertRaisesRegex(
            ForwardQueryError,
            r"unsupported parameter\(s\): device_tag_include_tags",
        ):
            fetcher._validate_query_parameters(
                "ipam.ipaddress",
                spec,
                {
                    "forward_netbox_shard_keys": [],
                    "device_tag_include_tags": ["Core"],
                },
            )

    def test_query_fetch_escapes_tag_scope_literals_in_scoped_query(self):
        source = ForwardSource.objects.create(
            name="tag-scope-source",
            type="saas",
            url="https://fwd.app",
            status="ready",
            parameters={
                "username": "user@example.com",
                "password": "secret",
                "verify": True,
                "network_id": "test-network",
                "device_tag_include_tags": ['Core "A"', r"Edge\Path"],
                "device_tag_include_match": "all",
                "device_tag_exclude_tags": ['Branch "B"'],
            },
        )
        sync = ForwardSync.objects.create(
            name="tag-scope-sync",
            source=source,
            parameters={"snapshot_id": LATEST_PROCESSED_SNAPSHOT},
        )

        client = Mock()
        client.get_snapshot_metrics.return_value = {}
        client.get_snapshots.return_value = []
        client.get_latest_processed_snapshot.return_value = {
            "id": "snapshot-after",
            "processedAt": "2026-03-31T12:15:00Z",
        }
        client.run_nqe_query.return_value = [
            {"name": "core-1", "site": "main dc"},
        ]
        fetcher = ForwardQueryFetcher(
            sync=sync,
            client=client,
            logger_=Mock(),
        )
        sync.get_network_id = Mock(return_value="test-network")
        sync.get_snapshot_id = Mock(return_value=LATEST_PROCESSED_SNAPSHOT)
        sync.resolve_snapshot_id = Mock(return_value="snapshot-after")
        sync.get_query_parameters = Mock(return_value={})
        sync.get_maps = Mock(return_value=[])

        context = fetcher.resolve_context()
        scoped_queries = [
            call.kwargs["query"]
            for call in client.run_nqe_query.call_args_list
            if "device.tagNames" in call.kwargs["query"]
        ]

        self.assertEqual(context.scoped_device_names, {"core-1"})
        self.assertTrue(scoped_queries)
        scoped_query = scoped_queries[0]
        self.assertIn(json.dumps('Core "A"'), scoped_query)
        self.assertIn(json.dumps(r"Edge\Path"), scoped_query)
        branch_literal = json.dumps('Branch "B"')
        self.assertIn(
            f"where !({branch_literal} in device.tagNames)",
            scoped_query,
        )

    def test_query_fetch_logs_are_not_emitted_for_parameter_passthrough(self):
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
            [{"name": "device-1"}],
            [{"name": "device-2"}],
        ]

        first = fetcher._run_nqe_query(
            spec=spec,
            context=context,
            parameters={"device_tag_include_tags": ["Core"]},
            fetch_all=True,
        )
        second = fetcher._run_nqe_query(
            spec=spec,
            context=context,
            parameters={"device_tag_include_tags": ["Core"]},
            fetch_all=True,
        )

        self.assertEqual(first, [{"name": "device-1"}])
        self.assertEqual(second, [{"name": "device-2"}])
        self.assertEqual(fetcher.client.run_nqe_query.call_count, 2)
        logger.log_info.assert_not_called()

    def test_diff_fetch_is_always_parameterless(self):
        sync = Mock()
        fetcher = ForwardQueryFetcher(sync=sync, client=Mock(), logger_=Mock())
        spec = Mock(run_query_id="qid-1", commit_id="cid-1", execution_value="qid-1")
        context = Mock(snapshot_id="after-s1")
        fetcher.client.run_nqe_diff.return_value = [
            {"changeType": "ADD", "data": {"name": "device-1"}}
        ]

        rows = fetcher._run_nqe_diff(
            spec=spec,
            context=context,
            before_snapshot_id="before-s1",
        )

        self.assertEqual(rows, [{"changeType": "ADD", "data": {"name": "device-1"}}])
        self.assertEqual(fetcher.client.run_nqe_diff.call_count, 1)
        self.assertNotIn(
            "parameters", fetcher.client.run_nqe_diff.call_args_list[0].kwargs
        )

    def test_parameterized_baseline_skips_diff_and_runs_full_async_query(self):
        logger = Mock()
        sync = Mock(parameters={}, source=Mock(parameters={}), pk=1)
        fetcher = ForwardQueryFetcher(sync=sync, client=Mock(), logger_=logger)
        spec = QuerySpec(
            model_string="dcim.interface",
            query_name="Forward Interfaces",
            query_id="Q_interfaces",
            parameters={"forward_netbox_shard_keys": []},
        )
        context = ForwardQueryContext(
            network_id="test-network",
            snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
            snapshot_id="snapshot-after",
        )
        fetcher._run_nqe_diff = Mock()
        fetcher._run_nqe_query = Mock(return_value=[])

        rows, delete_rows, sync_mode, fetch_meta = fetcher._fetch_spec_rows(
            "dcim.interface",
            spec,
            baseline=Mock(snapshot_id="snapshot-before"),
            context=context,
            coalesce_fields=[["device", "name"]],
            return_fetch_meta=True,
        )

        self.assertEqual((rows, delete_rows, sync_mode), ([], [], "full"))
        self.assertEqual(fetch_meta["fetch_mode"], "diff_fallback")
        fetcher._run_nqe_diff.assert_not_called()
        fetcher._run_nqe_query.assert_called_once()
        self.assertTrue(
            any(
                "do not accept runtime query parameters" in str(call.args[0])
                for call in logger.log_info.call_args_list
            )
        )

    def test_parameterized_baseline_fails_before_api_call_when_diff_is_required(self):
        sync = Mock(
            parameters={
                "diff_fallback_mode": ForwardDiffFallbackModeChoices.REQUIRE_DIFF
            },
            source=Mock(parameters={}),
            pk=1,
        )
        fetcher = ForwardQueryFetcher(sync=sync, client=Mock(), logger_=Mock())
        spec = QuerySpec(
            model_string="dcim.interface",
            query_name="Forward Interfaces",
            query_id="Q_interfaces",
            parameters={"forward_netbox_shard_keys": []},
        )
        context = ForwardQueryContext(
            network_id="test-network",
            snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
            snapshot_id="snapshot-after",
        )
        fetcher._run_nqe_diff = Mock()
        fetcher._run_nqe_query = Mock()

        with self.assertRaisesRegex(
            ForwardQueryError,
            "Diff execution is required.*do not accept runtime query parameters",
        ):
            fetcher._fetch_spec_rows(
                "dcim.interface",
                spec,
                baseline=Mock(snapshot_id="snapshot-before"),
                context=context,
                coalesce_fields=[["device", "name"]],
            )

        fetcher._run_nqe_diff.assert_not_called()
        fetcher._run_nqe_query.assert_not_called()

    def test_query_fetch_worker_count_uses_default_without_source_override(self):
        sync = Mock(parameters={}, source=Mock(parameters={}))
        fetcher = ForwardQueryFetcher(sync=sync, client=Mock(), logger_=Mock())

        worker_count = fetcher._query_fetch_worker_count(32)

        self.assertEqual(worker_count, DEFAULT_QUERY_FETCH_CONCURRENCY)

    def test_query_fetch_worker_count_honors_source_override(self):
        sync = Mock(
            parameters={},
            source=Mock(parameters={"query_fetch_concurrency": 6}),
        )
        fetcher = ForwardQueryFetcher(sync=sync, client=Mock(), logger_=Mock())

        worker_count = fetcher._query_fetch_worker_count(32)

        self.assertEqual(worker_count, 6)


from django.test import SimpleTestCase  # noqa: E402

from forward_netbox.utilities.sync_primitives import (  # noqa: E402
    _sorted_dependency_scope_keys,
)


class DependencyScopeKeySortTest(SimpleTestCase):
    """Routing dependency-cache scope keys carry a None VRF pk (global table)
    alongside int VRF pks under the same router/device pk. A plain sorted() then
    raised `'<' not supported between instances of 'NoneType' and 'int'` the
    moment the routing models were enabled (field report: sync failed, blank
    model, phase Sync). The None-safe key must order without raising.
    """

    def test_plain_sorted_would_raise_on_mixed_none_vrf(self):
        # Documents the original bug: a global-table peer (None) and a VRF peer
        # (int) sharing a router pk.
        with self.assertRaises(TypeError):
            sorted({(1, None), (1, 42)})
        with self.assertRaises(TypeError):
            sorted({(1, None, 5), (1, 42, 5)})

    def test_none_safe_sort_bgp_two_tuple(self):
        keys = {(1, None), (1, 42), (2, None), (2, 7)}
        ordered = _sorted_dependency_scope_keys(keys)
        self.assertEqual(ordered, [(1, None), (1, 42), (2, None), (2, 7)])

    def test_none_safe_sort_ospf_three_tuple(self):
        # (device.pk, vrf_pk|None, process_id) — None in the middle position.
        keys = {(1, None, 100), (1, 42, 100), (1, None, 200)}
        ordered = _sorted_dependency_scope_keys(keys)
        self.assertEqual(ordered, [(1, None, 100), (1, None, 200), (1, 42, 100)])

    def test_none_safe_sort_is_deterministic(self):
        keys = {(3, None), (1, 9), (1, None)}
        self.assertEqual(
            _sorted_dependency_scope_keys(keys),
            _sorted_dependency_scope_keys(set(keys)),
        )
