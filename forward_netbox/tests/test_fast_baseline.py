import json
from dataclasses import replace
from io import StringIO
from types import SimpleNamespace
from unittest.mock import patch

from core.models import ObjectChange
from dcim.models import Site
from django.contrib.auth import get_user_model
from django.core.management import call_command
from django.db import connections
from django.test import TransactionTestCase
from netbox.context import current_request
from netbox_branching.models import AppliedChange
from netbox_branching.models import Branch
from netbox_branching.models import ChangeDiff
from netbox_dlm.models import CVE
from rest_framework.test import APIRequestFactory

from forward_netbox.api.serializers import ForwardIngestionSerializer
from forward_netbox.choices import FORWARD_SUPPORTED_MODELS
from forward_netbox.choices import ForwardSyncStatusChoices
from forward_netbox.models import ForwardContributorBaseline
from forward_netbox.models import ForwardIngestion
from forward_netbox.models import ForwardSource
from forward_netbox.models import ForwardSync
from forward_netbox.models import ForwardWorkloadState
from forward_netbox.utilities.branch_budget import BranchWorkload
from forward_netbox.utilities.executor_base import ForwardExecutorBase
from forward_netbox.utilities.fast_baseline import _side_models
from forward_netbox.utilities.fast_baseline import fast_baseline_locked_decision
from forward_netbox.utilities.fast_baseline import fast_baseline_static_decision
from forward_netbox.utilities.fast_baseline import run_fast_baseline_load
from forward_netbox.utilities.fast_baseline_models import bulk_load_inventory_items
from forward_netbox.utilities.fast_baseline_models import (
    fast_baseline_workload_contract,
)
from forward_netbox.utilities.logging import SyncLogging
from forward_netbox.utilities.workload_normalization import (
    CVE_WITHOUT_IN_SCOPE_VULNERABILITY_DELETE_CONTRACT,
)
from forward_netbox.utilities.workload_state import build_state_entries
from forward_netbox.utilities.workload_state import decode_state_entries
from forward_netbox.utilities.workload_state import encode_state_entries
from forward_netbox.utilities.workload_state import PendingWorkloadState


class FastBaselineLoadTest(TransactionTestCase):
    reset_sequences = True

    @classmethod
    def _pre_setup(cls):
        current_request.set(None)
        super()._pre_setup()

    def _post_teardown(self):
        current_request.set(None)
        try:
            super()._post_teardown()
        finally:
            connections.close_all()
            current_request.set(None)

    def setUp(self):
        self.user = get_user_model().objects.create_user(username="fast-baseline-user")
        self.source = ForwardSource.objects.create(
            name="fast-baseline-source",
            type="saas",
            url="https://fwd.app",
            parameters={"network_id": "synthetic"},
        )
        model_parameters = {model: False for model in FORWARD_SUPPORTED_MODELS}
        model_parameters["dcim.site"] = True
        self.sync = ForwardSync.objects.create(
            name="fast-baseline-sync",
            source=self.source,
            user=self.user,
            auto_merge=True,
            parameters={
                **model_parameters,
                "auto_merge": True,
                "snapshot_id": "latestProcessed",
                "enable_bulk_orm": True,
                "enable_fast_baseline_load": True,
            },
        )
        self.workloads = [
            BranchWorkload(
                model_string="dcim.site",
                label="sites",
                upsert_rows=[{"name": "Baseline Site", "slug": "baseline-site"}],
                sync_mode="full",
                coalesce_fields=[["slug"], ["name"]],
            )
        ]

    def test_static_selection_fails_closed(self):
        decision = fast_baseline_static_decision(
            sync=self.sync,
            workloads=self.workloads,
        )
        self.assertTrue(decision.enabled, decision)

        self.sync.parameters["enable_fast_baseline_load"] = False
        decision = fast_baseline_static_decision(
            sync=self.sync,
            workloads=self.workloads,
        )
        self.assertFalse(decision.enabled)
        self.assertEqual(decision.reason_code, "disabled_by_default")
        self.sync.parameters["enable_fast_baseline_load"] = True

        self.sync.auto_merge = False
        decision = fast_baseline_static_decision(
            sync=self.sync,
            workloads=self.workloads,
        )
        self.assertFalse(decision.enabled)
        self.assertEqual(decision.reason_code, "auto_merge_required")

        self.sync.auto_merge = True
        delete_workload = BranchWorkload(
            model_string="dcim.site",
            label="site delete",
            delete_rows=[{"slug": "baseline-site"}],
            sync_mode="full",
        )
        decision = fast_baseline_static_decision(
            sync=self.sync,
            workloads=[delete_workload],
        )
        self.assertFalse(decision.enabled)
        self.assertEqual(decision.reason_code, "delete_rows_not_supported")

        unsupported_rows = BranchWorkload(
            model_string="dcim.interface",
            label="unsupported lag members",
            upsert_rows=[
                {
                    "device": "device-1",
                    "name": "Ethernet1",
                    "type": "1000base-t",
                    "enabled": True,
                    "lag": "Port-Channel1",
                }
            ],
            sync_mode="full",
        )
        decision = fast_baseline_static_decision(
            sync=self.sync,
            workloads=[unsupported_rows],
        )
        self.assertFalse(decision.enabled)
        self.assertEqual(decision.reason_code, "unsupported_row_contract")
        self.assertEqual(decision.context["model"], "dcim.interface")

        decision = fast_baseline_static_decision(
            sync=self.sync,
            workloads=self.workloads,
            model_results=[{"model": "dcim.site", "failure_count": 1}],
        )
        self.assertFalse(decision.enabled)
        self.assertEqual(decision.reason_code, "model_result_failure_present")

    def test_static_selection_admits_only_marked_empty_target_cve_tombstones(self):
        self.sync.parameters["netbox_dlm.cve"] = True
        marked = BranchWorkload(
            model_string="netbox_dlm.cve",
            label="derived CVE tombstones",
            upsert_rows=[{"cve_id": "CVE-2026-0001"}],
            delete_rows=[{"cve_id": "CVE-2026-0002"}],
            sync_mode="full",
            coalesce_fields=[["cve_id"]],
            derived_delete_contract=CVE_WITHOUT_IN_SCOPE_VULNERABILITY_DELETE_CONTRACT,
            derived_delete_count=1,
        )

        decision = fast_baseline_static_decision(sync=self.sync, workloads=[marked])
        self.assertTrue(decision.enabled, decision)

        mixed = replace(
            marked,
            delete_rows=[
                {"cve_id": "CVE-2026-0002"},
                {"cve_id": "CVE-2026-0002"},
            ],
        )
        decision = fast_baseline_static_decision(sync=self.sync, workloads=[mixed])
        self.assertFalse(decision.enabled)
        self.assertEqual(decision.reason_code, "delete_rows_not_supported")

    def test_relationship_adapter_contract_covers_all_ten_models(self):
        workloads = [
            BranchWorkload(
                model_string="dcim.device",
                label="device",
                upsert_rows=[{"name": "router-1"}],
            ),
            BranchWorkload(
                model_string="dcim.site",
                label="site",
                upsert_rows=[{"name": "Site A", "slug": "site-a"}],
            ),
            BranchWorkload(
                model_string="dcim.interface",
                label="interface",
                upsert_rows=[
                    {
                        "device": "router-1",
                        "name": "Ethernet1",
                        "type": "1000base-t",
                        "enabled": True,
                    },
                    {
                        "device": "router-1",
                        "name": "Port-channel1",
                        "type": "lag",
                        "enabled": True,
                    },
                    {
                        "device": "router-1",
                        "name": "Ethernet2",
                        "type": "1000base-t",
                        "enabled": True,
                        "lag": "Port-channel1",
                        "mode": "access",
                        "untagged_vlan": 100,
                    },
                ],
            ),
            BranchWorkload(
                model_string="ipam.vrf",
                label="vrf",
                upsert_rows=[
                    {
                        "name": "blue",
                        "rd": "65000:1",
                        "description": "",
                        "enforce_unique": False,
                    }
                ],
            ),
            BranchWorkload(
                model_string="dcim.module",
                label="module",
                upsert_rows=[
                    {
                        "device": "router-1",
                        "module_bay": "Slot 1",
                        "manufacturer": "Vendor",
                        "manufacturer_slug": "vendor",
                        "model": "Linecard",
                        "part_number": "LC-1",
                        "status": "active",
                    }
                ],
            ),
            BranchWorkload(
                model_string="extras.taggeditem",
                label="tag",
                upsert_rows=[
                    {
                        "device": "router-1",
                        "tag": "Feature",
                        "tag_slug": "feature",
                        "tag_color": "00ff00",
                    }
                ],
            ),
            BranchWorkload(
                model_string="ipam.vlan",
                label="vlan",
                upsert_rows=[
                    {
                        "site": "Site A",
                        "site_slug": "site-a",
                        "vid": 100,
                        "name": "Users",
                        "status": "active",
                    }
                ],
            ),
            BranchWorkload(
                model_string="ipam.fhrpgroup",
                label="fhrp",
                upsert_rows=[
                    {
                        "protocol": "hsrp",
                        "group_id": 100,
                        "name": "group-100",
                        "device": "router-1",
                        "interface": "Ethernet1",
                        "vrf": "blue",
                        "address": "192.0.2.254/24",
                        "status": "active",
                    }
                ],
            ),
            BranchWorkload(
                model_string="netbox_routing.bgppeer",
                label="peer",
                upsert_rows=[
                    {
                        "device": "router-1",
                        "vrf": "blue",
                        "local_asn": 65000,
                        "neighbor_address": "192.0.2.1",
                        "peer_asn": 65001,
                        "enabled": True,
                        "status": "active",
                    }
                ],
            ),
            BranchWorkload(
                model_string="netbox_routing.bgpaddressfamily",
                label="af",
                upsert_rows=[
                    {
                        "device": "router-1",
                        "vrf": "blue",
                        "local_asn": 65000,
                        "afi_safi": "ipv4-unicast",
                    }
                ],
            ),
            BranchWorkload(
                model_string="netbox_routing.bgppeeraddressfamily",
                label="peer af",
                upsert_rows=[
                    {
                        "device": "router-1",
                        "vrf": "blue",
                        "local_asn": 65000,
                        "neighbor_address": "192.0.2.1",
                        "peer_asn": 65001,
                        "afi_safi": "ipv4-unicast",
                        "enabled": True,
                    }
                ],
            ),
            BranchWorkload(
                model_string="netbox_routing.ospfarea",
                label="area",
                upsert_rows=[{"area_id": "0.0.0.0", "area_type": "backbone"}],
            ),
            BranchWorkload(
                model_string="netbox_routing.ospfinstance",
                label="instance",
                upsert_rows=[
                    {
                        "device": "router-1",
                        "vrf": "blue",
                        "process_id": "1",
                        "router_id": "192.0.2.10",
                    }
                ],
            ),
            BranchWorkload(
                model_string="netbox_routing.ospfinterface",
                label="ospf interface",
                upsert_rows=[
                    {
                        "device": "router-1",
                        "vrf": "blue",
                        "process_id": "1",
                        "router_id": "192.0.2.10",
                        "area_id": "0.0.0.0",
                        "area_type": "backbone",
                        "local_interface": "Ethernet1",
                    }
                ],
            ),
        ]

        enabled, reason, context = fast_baseline_workload_contract(self.sync, workloads)
        self.assertTrue(enabled, (reason, context))

        implicit_vrf = [
            replace(
                workload,
                upsert_rows=[
                    {**row, "vrf": "adapter-created-vrf"} if "vrf" in row else row
                    for row in workload.upsert_rows
                ],
            )
            for workload in workloads
            if workload.model_string != "ipam.vrf"
        ]
        enabled, reason, context = fast_baseline_workload_contract(
            self.sync, implicit_vrf
        )
        self.assertTrue(enabled, (reason, context))

        self_contained_relationship_rows = [
            workload
            for workload in workloads
            if workload.model_string
            not in {
                "netbox_routing.bgppeer",
                "netbox_routing.bgpaddressfamily",
                "netbox_routing.ospfarea",
                "netbox_routing.ospfinstance",
            }
        ]
        enabled, reason, context = fast_baseline_workload_contract(
            self.sync, self_contained_relationship_rows
        )
        self.assertTrue(enabled, (reason, context))

        broken = list(workloads)
        broken[-1] = replace(
            broken[-1],
            upsert_rows=[{**broken[-1].upsert_rows[0], "local_interface": "missing"}],
        )
        enabled, reason, context = fast_baseline_workload_contract(self.sync, broken)
        self.assertFalse(enabled)
        self.assertEqual(reason, "unsupported_row_contract")
        self.assertEqual(context["reason"], "missing_interface")

    def test_ipaddress_contract_preserves_normal_skip_and_coalesce_semantics(self):
        workloads = [
            BranchWorkload(
                model_string="dcim.device",
                label="device",
                upsert_rows=[{"name": "router-1"}],
            ),
            BranchWorkload(
                model_string="dcim.interface",
                label="interfaces",
                upsert_rows=[
                    {
                        "device": "router-1",
                        "name": name,
                        "type": "1000base-t",
                        "enabled": True,
                    }
                    for name in ("Ethernet1", "Ethernet2")
                ],
            ),
            BranchWorkload(
                model_string="ipam.ipaddress",
                label="addresses",
                upsert_rows=[
                    {
                        "device": "router-1",
                        "interface": "Ethernet1",
                        "address": "192.0.2.0/24",
                        "status": "active",
                    },
                    {
                        "device": "router-1",
                        "interface": "Ethernet1",
                        "address": "192.0.2.1/24",
                        "status": "active",
                    },
                    {
                        "device": "router-1",
                        "interface": "Ethernet2",
                        "address": "192.0.2.1/30",
                        "status": "active",
                    },
                ],
            ),
        ]

        enabled, reason, context = fast_baseline_workload_contract(self.sync, workloads)
        self.assertTrue(enabled, (reason, context))

        workloads[-1] = replace(
            workloads[-1],
            upsert_rows=[
                {
                    **workloads[-1].upsert_rows[1],
                    "interface": "not-imported",
                }
            ],
        )
        enabled, reason, context = fast_baseline_workload_contract(self.sync, workloads)
        self.assertFalse(enabled)
        self.assertEqual(reason, "unsupported_row_contract")
        self.assertEqual(context["model"], "ipam.ipaddress")

    def test_prefix_contract_admits_adapter_created_vrf(self):
        workloads = [
            BranchWorkload(
                model_string="ipam.prefix",
                label="prefixes",
                upsert_rows=[
                    {
                        "prefix": "192.0.2.0/24",
                        "vrf": "created-by-prefix-adapter",
                        "status": "active",
                    }
                ],
            )
        ]

        enabled, reason, context = fast_baseline_workload_contract(self.sync, workloads)
        self.assertTrue(enabled, (reason, context))

    def test_inventory_contract_admits_implicit_manufacturer_and_omits_module_rows(
        self,
    ):
        self.sync.parameters["dcim.module"] = True
        workloads = [
            BranchWorkload(
                model_string="dcim.device",
                label="device",
                upsert_rows=[{"name": "router-1"}],
            ),
            BranchWorkload(
                model_string="dcim.module",
                label="modules",
                upsert_rows=[],
            ),
            BranchWorkload(
                model_string="dcim.inventoryitem",
                label="inventory",
                upsert_rows=[
                    {
                        "device": "router-1",
                        "name": "Power Supply",
                        "part_id": "PSU-1",
                        "serial": "PSU-SERIAL",
                        "status": "active",
                        "discovered": True,
                        "manufacturer": "Implicit Vendor",
                        "manufacturer_slug": "implicit-vendor",
                    },
                    {
                        "device": "router-1",
                        "name": "Slot 1 line card",
                        "status": "active",
                        "discovered": True,
                        "part_type": "LINE CARD",
                        "manufacturer": "Implicit Vendor",
                        "manufacturer_slug": "implicit-vendor",
                    },
                ],
            ),
        ]

        enabled, reason, context = fast_baseline_workload_contract(self.sync, workloads)
        self.assertTrue(enabled, (reason, context))

        conflicting_manufacturer = replace(
            workloads[-1],
            upsert_rows=[
                workloads[-1].upsert_rows[0],
                {
                    **workloads[-1].upsert_rows[0],
                    "name": "Fan Tray",
                    "manufacturer_slug": "different-slug",
                },
            ],
        )
        enabled, reason, context = fast_baseline_workload_contract(
            self.sync,
            [*workloads[:-1], conflicting_manufacturer],
        )
        self.assertFalse(enabled)
        self.assertEqual(reason, "unsupported_row_contract")
        self.assertEqual(context["model"], "dcim.inventoryitem")

        duplicate_asset_tag = replace(
            workloads[-1],
            upsert_rows=[
                {**workloads[-1].upsert_rows[0], "asset_tag": "duplicate"},
                {
                    **workloads[-1].upsert_rows[0],
                    "name": "Fan Tray",
                    "asset_tag": "duplicate",
                },
            ],
        )
        enabled, reason, context = fast_baseline_workload_contract(
            self.sync,
            [*workloads[:-1], duplicate_asset_tag],
        )
        self.assertFalse(enabled)
        self.assertEqual(reason, "unsupported_row_contract")
        self.assertEqual(context["model"], "dcim.inventoryitem")

    def test_inventory_loader_creates_proven_implicit_manufacturer(self):
        from dcim.models import (
            Device,
            DeviceRole,
            DeviceType,
            InventoryItem,
            Manufacturer,
        )

        chassis_manufacturer = Manufacturer.objects.create(
            name="Chassis Vendor",
            slug="chassis-vendor",
        )
        device_type = DeviceType.objects.create(
            manufacturer=chassis_manufacturer,
            model="Router",
            slug="router",
        )
        role = DeviceRole.objects.create(name="Router", slug="router")
        site = Site.objects.create(name="Inventory Site", slug="inventory-site")
        device = Device.objects.create(
            name="router-1",
            device_type=device_type,
            role=role,
            site=site,
        )
        ingestion = SimpleNamespace()

        def ensure_manufacturer(row):
            manufacturer, _ = Manufacturer.objects.get_or_create(
                slug=row["slug"],
                defaults={"name": row["name"]},
            )
            return manufacturer

        runner = SimpleNamespace(
            sync=self.sync,
            ingestion=ingestion,
            logger=SyncLogging(),
            _ensure_manufacturer=ensure_manufacturer,
        )
        rows = [
            {
                "device": device.name,
                "name": "Power Supply",
                "part_id": "PSU-1",
                "serial": "PSU-SERIAL",
                "status": "active",
                "discovered": True,
                "manufacturer": "Implicit Vendor",
                "manufacturer_slug": "implicit-vendor",
            }
        ]

        self.assertTrue(bulk_load_inventory_items(runner, rows))
        item = InventoryItem.objects.get(device=device, name="Power Supply")
        self.assertEqual(item.manufacturer.slug, "implicit-vendor")
        self.assertTrue(Manufacturer.objects.filter(slug="implicit-vendor").exists())
        self.assertIn(Manufacturer, _side_models({"dcim.inventoryitem"}))

    def test_locked_selection_rejects_nonempty_target_and_prior_ingestion(self):
        Site.objects.create(name="Existing Site", slug="existing-site")
        from django.db import transaction

        with transaction.atomic():
            decision = fast_baseline_locked_decision(
                sync=self.sync,
                workloads=self.workloads,
            )
        self.assertFalse(decision.enabled)
        self.assertEqual(decision.reason_code, "target_table_not_empty")

        Site.objects.all().delete()
        ForwardIngestion.objects.create(sync=self.sync)
        with transaction.atomic():
            decision = fast_baseline_locked_decision(
                sync=self.sync,
                workloads=self.workloads,
            )
        self.assertFalse(decision.enabled)
        self.assertEqual(decision.reason_code, "prior_ingestion_present")

    def test_direct_baseline_preserves_durable_completion_without_branch_audit(self):
        logger = SyncLogging()
        executor = ForwardExecutorBase(
            self.sync,
            client=None,
            logger_=logger,
            user=self.user,
        )
        executor.last_model_results = [
            {"model": "dcim.site", "sync_mode": "full", "row_count": 1}
        ]
        context = SimpleNamespace(
            as_dict=lambda: {
                "snapshot_selector": "latestProcessed",
                "snapshot_id": "synthetic-baseline",
                "snapshot_info": {},
                "snapshot_metrics": {},
                "scoped_matched_tags": {},
            }
        )
        state_entries = build_state_entries(
            "dcim.site",
            self.workloads[0].upsert_rows,
            self.workloads[0].coalesce_fields,
        )
        payload, checksum = encode_state_entries(state_entries)

        def stage_contributor(ingestion, context):
            from forward_netbox.utilities.contributor_baseline import (
                ContributorRelationContract,
                ContributorRelationSeed,
                stage_contributor_baseline,
            )

            contract = ContributorRelationContract(
                model_string="dcim.site",
                map_id=None,
                contract_key="fast_baseline_site_contributor",
                query_path="/synthetic/fast_baseline_site_contributor",
                query_id="Q_fast_baseline_site",
                full_commit_id="full-commit",
                full_source_sha256="1" * 64,
                diff_query_id="Q_fast_baseline_site",
                diff_commit_id="diff-commit",
                diff_source_sha256="2" * 64,
                contract_fingerprint="3" * 64,
                reducer_id="synthetic_site",
                reducer_version=1,
                normalization_version=1,
                identity_version=1,
            )
            stage_contributor_baseline(
                ingestion,
                [
                    ContributorRelationSeed(
                        contract=contract,
                        rows=self.workloads[0].upsert_rows,
                        target_key=lambda row: row["slug"],
                    )
                ],
                network_fingerprint="4" * 64,
                map_set_fingerprint="5" * 64,
                scope_config_fingerprint="6" * 64,
                scope_membership_fingerprint="7" * 64,
                scope_state={},
            )
            return 1

        fetcher = SimpleNamespace(
            pending_workload_states=[
                PendingWorkloadState(
                    model_string="dcim.site",
                    parameter_hash="p" * 64,
                    identity_contract_hash="i" * 64,
                    payload=payload,
                    payload_checksum=checksum,
                    row_count=1,
                )
            ],
            stage_pending_contributor_baseline=stage_contributor,
        )

        ingestion, decision = run_fast_baseline_load(
            executor,
            context=context,
            workloads=self.workloads,
            fetcher=fetcher,
        )

        self.assertTrue(decision.enabled, decision)
        self.assertIsNotNone(ingestion)
        ingestion.refresh_from_db()
        self.sync.refresh_from_db()
        self.assertTrue(Site.objects.filter(slug="baseline-site").exists())
        self.assertTrue(ingestion.baseline_ready)
        self.assertIsNotNone(ingestion.merge_applied_at)
        self.assertIsNotNone(ingestion.merge_finalized_at)
        self.assertIsNone(ingestion.branch_id)
        self.assertEqual(ingestion.applied_change_count, 1)
        self.assertEqual(ingestion.created_change_count, 1)
        self.assertEqual(ingestion.failed_change_count, 0)
        self.assertEqual(self.sync.status, ForwardSyncStatusChoices.COMPLETED)
        self.assertEqual(Branch.objects.count(), 0)
        self.assertEqual(ObjectChange.objects.count(), 0)
        self.assertEqual(AppliedChange.objects.count(), 0)
        self.assertEqual(ChangeDiff.objects.count(), 0)
        workload_state = ForwardWorkloadState.objects.get(ingestion=ingestion)
        self.assertTrue(workload_state.is_current)
        self.assertEqual(workload_state.snapshot_id, "synthetic-baseline")
        contributor = ForwardContributorBaseline.objects.get(ingestion=ingestion)
        self.assertTrue(contributor.is_current)
        attestation = ingestion.snapshot_info["fast_baseline_load"]
        self.assertEqual(attestation["engine"], "direct_main_bulk_apply")
        self.assertIn("destination_object_change", attestation["omitted_evidence"])
        self.assertEqual(attestation["staged_contributor_relation_count"], 1)
        request = APIRequestFactory().get("/")
        serialized = ForwardIngestionSerializer(
            ingestion,
            context={"request": request},
        ).data
        self.assertEqual(serialized["fast_baseline_attestation"], attestation)

        from forward_netbox.views import _ingestion_log_export_payload

        exported = _ingestion_log_export_payload(ingestion, active_stage="sync")
        self.assertEqual(
            exported["ingestion"]["fast_baseline_attestation"],
            attestation,
        )

    def test_direct_baseline_omits_only_proven_cve_tombstones(self):
        self.sync.parameters["dcim.site"] = False
        self.sync.parameters["netbox_dlm.cve"] = True
        workload = BranchWorkload(
            model_string="netbox_dlm.cve",
            label="normalized CVE catalog",
            upsert_rows=[{"cve_id": "CVE-2026-0001"}],
            delete_rows=[{"cve_id": "CVE-2026-0002"}],
            sync_mode="full",
            coalesce_fields=[["cve_id"]],
            derived_delete_contract=CVE_WITHOUT_IN_SCOPE_VULNERABILITY_DELETE_CONTRACT,
            derived_delete_count=1,
        )
        state_entries = build_state_entries(
            workload.model_string,
            workload.upsert_rows,
            workload.coalesce_fields,
        )
        state_entries.update(
            build_state_entries(
                workload.model_string,
                workload.delete_rows,
                workload.coalesce_fields,
                action="delete",
            )
        )
        payload, checksum = encode_state_entries(state_entries)
        fetcher = SimpleNamespace(
            pending_workload_states=[
                PendingWorkloadState(
                    model_string=workload.model_string,
                    parameter_hash="p" * 64,
                    identity_contract_hash="i" * 64,
                    payload=payload,
                    payload_checksum=checksum,
                    row_count=2,
                )
            ],
            stage_pending_contributor_baseline=lambda ingestion, context: 0,
        )
        executor = ForwardExecutorBase(
            self.sync,
            client=None,
            logger_=SyncLogging(),
            user=self.user,
        )
        executor.last_model_results = [
            {"model": workload.model_string, "sync_mode": "full", "row_count": 2}
        ]
        context = SimpleNamespace(
            as_dict=lambda: {
                "snapshot_selector": "latestProcessed",
                "snapshot_id": "synthetic-cve-tombstone",
                "snapshot_info": {},
                "snapshot_metrics": {},
                "scoped_matched_tags": {},
            }
        )

        ingestion, decision = run_fast_baseline_load(
            executor,
            context=context,
            workloads=[workload],
            fetcher=fetcher,
        )

        self.assertTrue(decision.enabled, decision)
        self.assertEqual(
            list(CVE.objects.values_list("cve_id", flat=True)), ["CVE-2026-0001"]
        )
        ingestion.refresh_from_db()
        self.assertEqual(ingestion.applied_change_count, 1)
        self.assertEqual(ingestion.deleted_change_count, 0)
        attestation = ingestion.snapshot_info["fast_baseline_load"]
        self.assertEqual(attestation["omitted_proven_noop_deletes"], 1)
        self.assertEqual(
            attestation["statistics"]["models"]["netbox_dlm.cve"]["skipped"],
            1,
        )
        state = ForwardWorkloadState.objects.get(ingestion=ingestion)
        durable = decode_state_entries(state.payload, state.payload_checksum)
        self.assertEqual(
            {entry["action"] for entry in durable.values()}, {"upsert", "delete"}
        )
        self.assertEqual(ObjectChange.objects.count(), 0)

    def test_fault_rolls_back_target_and_ingestion(self):
        logger = SyncLogging()
        executor = ForwardExecutorBase(
            self.sync,
            client=None,
            logger_=logger,
            user=self.user,
        )
        executor.last_model_results = [
            {"model": "dcim.site", "sync_mode": "full", "row_count": 1}
        ]
        context = SimpleNamespace(
            as_dict=lambda: {
                "snapshot_selector": "latestProcessed",
                "snapshot_id": "synthetic-fault",
                "snapshot_info": {},
                "snapshot_metrics": {},
                "scoped_matched_tags": {},
            }
        )
        fetcher = SimpleNamespace(
            pending_workload_states=[],
            stage_pending_contributor_baseline=lambda ingestion, context: 0,
        )

        # The implementation imports the helper inside the function, so patch
        # the defining module as the actual fault boundary.
        with (
            patch(
                "forward_netbox.utilities.ingestion_merge._complete_post_merge_bookkeeping",
                side_effect=RuntimeError("injected finalization fault"),
            ),
            self.assertRaisesRegex(RuntimeError, "injected finalization fault"),
        ):
            run_fast_baseline_load(
                executor,
                context=context,
                workloads=self.workloads,
                fetcher=fetcher,
            )

        self.assertFalse(Site.objects.filter(slug="baseline-site").exists())
        self.assertEqual(ForwardIngestion.objects.count(), 0)
        self.assertEqual(ObjectChange.objects.count(), 0)

    def test_operator_preflight_command_reports_exact_rejection(self):
        output = StringIO()
        report = {
            "eligible": False,
            "reason_code": "competing_branch_present",
            "context": {},
            "workload_fetch_performed": True,
        }
        with patch(
            "forward_netbox.management.commands.forward_fast_baseline_preflight.fast_baseline_preflight",
            return_value=report,
        ):
            call_command(
                "forward_fast_baseline_preflight",
                sync=str(self.sync.pk),
                stdout=output,
            )

        self.assertEqual(json.loads(output.getvalue()), report)
