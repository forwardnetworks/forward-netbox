import hashlib
import json
from unittest.mock import patch

from django.contrib.auth import get_user_model
from django.test import TestCase

from forward_netbox.exceptions import ForwardQueryError
from forward_netbox.models import ForwardDeviceIdentity
from forward_netbox.models import ForwardDeviceTagClaim
from forward_netbox.models import ForwardIngestion
from forward_netbox.models import ForwardSource
from forward_netbox.models import ForwardSync
from forward_netbox.models import ForwardWorkloadState
from forward_netbox.utilities.branch_budget import BranchWorkload
from forward_netbox.utilities.workload_state import apply_durable_workload_deltas
from forward_netbox.utilities.workload_state import (
    canonical_identity_hash_scheme_errors,
)
from forward_netbox.utilities.workload_state import canonical_row_identity
from forward_netbox.utilities.workload_state import canonical_row_identity_hash
from forward_netbox.utilities.workload_state import CANONICAL_ROW_IDENTITY_HASH_SCHEME
from forward_netbox.utilities.workload_state import compare_canonical_delete_identities
from forward_netbox.utilities.workload_state import decode_state_entries
from forward_netbox.utilities.workload_state import encode_state_entries
from forward_netbox.utilities.workload_state import promote_workload_states_locked
from forward_netbox.utilities.workload_state import stage_workload_states


def _workload(
    rows,
    *,
    delete_rows=None,
    parameters=None,
    model_string="dcim.interface",
    query_name="Forward rows",
    coalesce_fields=None,
):
    coalesce_fields = coalesce_fields or {
        "dcim.device": [["name"]],
        "dcim.interface": [["device", "name"]],
        "netbox_dlm.devicesoftware": [["name"]],
        "netbox_dlm.softwareversion": [["platform_slug", "version"]],
        "netbox_dlm.vulnerability": [["cve_id", "platform_slug", "version", "name"]],
        "netbox_cisco_aci.acibridgedomain": [["fabric_name", "name"]],
    }.get(model_string, [["cve_id"]])
    return BranchWorkload(
        model_string=model_string,
        label=model_string,
        upsert_rows=list(rows),
        delete_rows=list(delete_rows or []),
        sync_mode="full",
        coalesce_fields=coalesce_fields,
        query_name=query_name,
        execution_mode="query_path",
        execution_value="org:/forward/rows",
        query_parameters=dict(
            {"include_tags": ["managed"]} if parameters is None else parameters
        ),
    )


class DurableWorkloadStateTest(TestCase):
    def setUp(self):
        user = get_user_model().objects.create_user(username="workload-state-owner")
        source = ForwardSource.objects.create(
            name="workload-state-source",
            type="saas",
            url="https://fwd.app",
            parameters={"network_id": "network"},
        )
        self.sync = ForwardSync.objects.create(
            name="workload-state-sync",
            source=source,
            user=user,
            parameters={"dcim.interface": True},
        )

    def _ingestion(self, snapshot_id):
        return ForwardIngestion.objects.create(
            sync=self.sync,
            snapshot_id=snapshot_id,
        )

    def test_compressed_state_round_trip_and_checksum_validation(self):
        entries = {
            '{"device":"router-1","name":"Ethernet1"}': {
                "action": "upsert",
                "row_hash": "hash-1",
                "row": {"device": "router-1", "name": "Ethernet1"},
            }
        }

        payload, checksum = encode_state_entries(entries)

        self.assertEqual(decode_state_entries(payload, checksum), entries)
        with self.assertRaisesMessage(ForwardQueryError, "checksum validation failed"):
            decode_state_entries(payload + b"x", checksum)
        truncated = payload[:-1]
        with self.assertRaisesMessage(ForwardQueryError, "payload is invalid"):
            decode_state_entries(truncated, hashlib.sha256(truncated).hexdigest())

    def test_fhrp_durable_identity_preserves_each_participant(self):
        group = {
            "protocol": "hsrp",
            "group_id": 10,
            "address": "192.0.2.1/32",
            "vrf": None,
        }
        first = canonical_row_identity(
            "ipam.fhrpgroup",
            {
                **group,
                "device": "router-a",
                "interface": "Vlan10",
            },
            [["protocol", "group_id", "address", "vrf"]],
        )
        second = canonical_row_identity(
            "ipam.fhrpgroup",
            {
                **group,
                "device": "router-b",
                "interface": "Vlan10",
            },
            [["protocol", "group_id", "address", "vrf"]],
        )

        self.assertNotEqual(first, second)

    def test_cable_identity_is_direction_independent(self):
        left = {
            "device": "a",
            "interface": "Ethernet1",
            "remote_device": "b",
            "remote_interface": "Ethernet2",
        }
        right = {
            "device": "b",
            "interface": "Ethernet2",
            "remote_device": "a",
            "remote_interface": "Ethernet1",
        }

        self.assertEqual(
            canonical_row_identity("dcim.cable", left, []),
            canonical_row_identity("dcim.cable", right, []),
        )

    def test_oracle_hash_is_sha256_of_production_canonical_identity(self):
        row = {"device": "router-1", "name": "Ethernet1"}
        canonical = canonical_row_identity("dcim.interface", row, [["device", "name"]])

        self.assertEqual(
            canonical_row_identity_hash("dcim.interface", row, [["device", "name"]]),
            hashlib.sha256(canonical.encode("utf-8")).hexdigest(),
        )

    def test_oracle_hash_rejects_legacy_identity_namespace(self):
        row = {"device": "router-1", "name": "Ethernet1"}
        legacy = json.dumps(
            {
                "model": "dcim.interface",
                "fields": ("device", "name"),
                "values": ["router-1", "Ethernet1"],
            },
            sort_keys=True,
            separators=(",", ":"),
            default=str,
        )
        legacy_hash = hashlib.sha256(legacy.encode("utf-8")).hexdigest()

        self.assertNotEqual(
            canonical_row_identity_hash("dcim.interface", row, [["device", "name"]]),
            legacy_hash,
        )
        self.assertEqual(
            canonical_identity_hash_scheme_errors(
                {
                    "before_full": CANONICAL_ROW_IDENTITY_HASH_SCHEME,
                    "after_full": "legacy_model_fields_values_v0",
                    "captured_diff_deletes": "",
                }
            ),
            [
                "identity_scheme_mismatch:after_full:legacy_model_fields_values_v0",
                "identity_scheme_mismatch:captured_diff_deletes:missing",
            ],
        )

    def test_oracle_hash_uses_prefix_blank_vrf_identity_contract(self):
        row = {"prefix": "192.0.2.0/24", "vrf": None}

        identity_hash = canonical_row_identity_hash(
            "ipam.prefix", row, [["prefix", "vrf"]]
        )

        self.assertEqual(len(identity_hash), 64)

    def test_delete_comparator_accepts_only_exact_canonical_match(self):
        comparison = compare_canonical_delete_identities(
            {"same", "removed"},
            {"same", "added"},
            {"removed"},
        )

        self.assertTrue(comparison["exact_match"])
        self.assertEqual(comparison["matched_delete_count"], 1)
        self.assertEqual(comparison["spurious_delete_count"], 0)
        self.assertEqual(comparison["missing_delete_count"], 0)

    def test_delete_comparator_fails_spurious_delete(self):
        comparison = compare_canonical_delete_identities(
            {"same"},
            {"same"},
            {"still-present"},
        )

        self.assertFalse(comparison["exact_match"])
        self.assertEqual(comparison["spurious_delete_count"], 1)
        self.assertEqual(comparison["missing_delete_count"], 0)

    def test_delete_comparator_fails_missing_delete(self):
        comparison = compare_canonical_delete_identities(
            {"removed"},
            set(),
            set(),
        )

        self.assertFalse(comparison["exact_match"])
        self.assertEqual(comparison["spurious_delete_count"], 0)
        self.assertEqual(comparison["missing_delete_count"], 1)

    def test_seed_then_unchanged_full_workload_stages_no_rows(self):
        rows = [{"device": "router-1", "name": "Ethernet1", "enabled": True}]
        workloads, pending, summaries = apply_durable_workload_deltas(
            self.sync,
            [_workload(rows)],
        )
        self.assertEqual(workloads[0].upsert_rows, rows)
        self.assertEqual(summaries[0]["mode"], "seed")
        ingestion = self._ingestion("snapshot-1")
        stage_workload_states(ingestion, pending)
        promote_workload_states_locked(ingestion)

        workloads, pending, summaries = apply_durable_workload_deltas(
            self.sync,
            [_workload(rows)],
        )

        self.assertEqual(workloads, [])
        self.assertEqual(len(pending), 1)
        self.assertEqual(summaries[0]["mode"], "local_delta")
        self.assertEqual(summaries[0]["upsert_rows"], 0)
        self.assertEqual(summaries[0]["delete_rows"], 0)

    def test_local_delta_derives_changed_upsert_and_missing_delete(self):
        initial = [
            {"device": "router-1", "name": "Ethernet1", "enabled": True},
            {"device": "router-1", "name": "Ethernet2", "enabled": True},
        ]
        _, pending, _ = apply_durable_workload_deltas(
            self.sync,
            [_workload(initial)],
        )
        ingestion = self._ingestion("snapshot-1")
        stage_workload_states(ingestion, pending)
        promote_workload_states_locked(ingestion)

        current = [
            {"device": "router-1", "name": "Ethernet1", "enabled": False},
            {"device": "router-1", "name": "Ethernet3", "enabled": True},
        ]
        workloads, _, summaries = apply_durable_workload_deltas(
            self.sync,
            [_workload(current)],
        )

        self.assertEqual(
            {(row["name"], row["enabled"]) for row in workloads[0].upsert_rows},
            {("Ethernet1", False), ("Ethernet3", True)},
        )
        self.assertEqual(
            workloads[0].delete_rows,
            [{"device": "router-1", "name": "Ethernet2", "enabled": True}],
        )
        self.assertEqual(summaries[0]["mode"], "local_delta")

    def test_successful_explicit_delete_is_not_repeated(self):
        deleted = {"device": "router-1", "name": "Ethernet9"}
        workloads, pending, summaries = apply_durable_workload_deltas(
            self.sync,
            [_workload([], delete_rows=[deleted])],
        )
        self.assertEqual(workloads[0].delete_rows, [deleted])
        self.assertEqual(summaries[0]["tombstone_rows"], 1)
        ingestion = self._ingestion("snapshot-1")
        stage_workload_states(ingestion, pending)
        promote_workload_states_locked(ingestion)

        workloads, _, summaries = apply_durable_workload_deltas(
            self.sync,
            [_workload([], delete_rows=[deleted])],
        )

        self.assertEqual(workloads, [])
        self.assertEqual(summaries[0]["delete_rows"], 0)
        self.assertEqual(summaries[0]["tombstone_rows"], 1)

    def test_deleted_identity_returning_to_target_is_upserted(self):
        row = {"device": "router-1", "name": "Ethernet9"}
        _, pending, _ = apply_durable_workload_deltas(
            self.sync,
            [_workload([], delete_rows=[row])],
        )
        ingestion = self._ingestion("snapshot-1")
        stage_workload_states(ingestion, pending)
        promote_workload_states_locked(ingestion)

        workloads, _, _ = apply_durable_workload_deltas(
            self.sync,
            [_workload([row])],
        )

        self.assertEqual(workloads[0].upsert_rows, [row])
        self.assertEqual(workloads[0].delete_rows, [])

    def test_peer_current_upsert_protects_shared_identity_from_delete(self):
        shared = {"device": "router-1", "name": "Ethernet1"}
        _, pending, _ = apply_durable_workload_deltas(
            self.sync,
            [_workload([shared])],
        )
        ingestion = self._ingestion("snapshot-1")
        ingestion.baseline_ready = True
        ingestion.save(update_fields=["baseline_ready"])
        stage_workload_states(ingestion, pending)
        promote_workload_states_locked(ingestion)

        peer = ForwardSync.objects.create(
            name="workload-state-peer",
            source=self.sync.source,
            user=self.sync.user,
            parameters={"dcim.interface": True},
        )
        _, peer_pending, _ = apply_durable_workload_deltas(
            peer,
            [_workload([shared])],
        )
        peer_ingestion = ForwardIngestion.objects.create(
            sync=peer,
            snapshot_id="peer-snapshot",
            baseline_ready=True,
        )
        stage_workload_states(peer_ingestion, peer_pending)
        promote_workload_states_locked(peer_ingestion)

        workloads, _, summaries = apply_durable_workload_deltas(
            self.sync,
            [_workload([])],
        )

        self.assertEqual(workloads, [])
        self.assertEqual(summaries[0]["protected_delete_rows"], 1)
        self.assertFalse(summaries[0]["unrepresented_peer"])

    def test_unseeded_completed_peer_blocks_local_deletes(self):
        row = {"device": "router-1", "name": "Ethernet1"}
        _, pending, _ = apply_durable_workload_deltas(
            self.sync,
            [_workload([row])],
        )
        ingestion = self._ingestion("snapshot-1")
        ingestion.baseline_ready = True
        ingestion.save(update_fields=["baseline_ready"])
        stage_workload_states(ingestion, pending)
        promote_workload_states_locked(ingestion)
        peer = ForwardSync.objects.create(
            name="workload-state-unseeded-peer",
            source=self.sync.source,
            user=self.sync.user,
            parameters={"dcim.interface": True},
        )
        ForwardIngestion.objects.create(
            sync=peer,
            snapshot_id="peer-snapshot",
            baseline_ready=True,
        )

        workloads, _, summaries = apply_durable_workload_deltas(
            self.sync,
            [_workload([])],
        )

        self.assertEqual(workloads, [])
        self.assertEqual(summaries[0]["protected_delete_rows"], 1)
        self.assertTrue(summaries[0]["unrepresented_peer"])

    def test_completed_peer_with_model_disabled_does_not_block_delete(self):
        row = {"device": "router-1", "name": "Ethernet1"}
        _, pending, _ = apply_durable_workload_deltas(
            self.sync,
            [_workload([row])],
        )
        ingestion = self._ingestion("snapshot-1")
        stage_workload_states(ingestion, pending)
        promote_workload_states_locked(ingestion)
        peer = ForwardSync.objects.create(
            name="workload-state-disabled-peer",
            source=self.sync.source,
            user=self.sync.user,
            parameters={"dcim.interface": False},
        )
        ForwardIngestion.objects.create(
            sync=peer,
            snapshot_id="peer-snapshot",
            baseline_ready=True,
        )

        workloads, _, summaries = apply_durable_workload_deltas(
            self.sync,
            [_workload([])],
        )

        self.assertEqual(workloads[0].delete_rows, [row])
        self.assertFalse(summaries[0]["unrepresented_peer"])

    def test_full_parameterized_and_parameterless_maps_share_durable_state(self):
        first = {"device": "router-1", "name": "Ethernet1"}
        second = {"device": "router-1", "name": "Ethernet2"}

        workloads, pending, summaries = apply_durable_workload_deltas(
            self.sync,
            [
                _workload([first], query_name="Parameterized"),
                _workload([second], parameters={}, query_name="Parameterless"),
            ],
        )

        self.assertEqual(len(workloads), 1)
        self.assertCountEqual(workloads[0].upsert_rows, [first, second])
        self.assertEqual(summaries[0]["target_rows"], 2)
        state_entries = decode_state_entries(
            pending[0].payload,
            pending[0].payload_checksum,
        )
        self.assertEqual(len(state_entries), 2)

    def test_aci_empty_snapshot_preserves_state_until_deletes_are_enabled(self):
        row = {"fabric_name": "fabric-a", "name": "bridge-domain-a"}
        model_string = "netbox_cisco_aci.acibridgedomain"
        _, pending, _ = apply_durable_workload_deltas(
            self.sync,
            [_workload([row], model_string=model_string)],
        )
        ingestion = self._ingestion("snapshot-1")
        stage_workload_states(ingestion, pending)
        promote_workload_states_locked(ingestion)

        workloads, held_pending, summaries = apply_durable_workload_deltas(
            self.sync,
            [_workload([], model_string=model_string)],
        )

        self.assertEqual(workloads, [])
        self.assertEqual(summaries[0]["protected_delete_rows"], 1)
        held_entries = decode_state_entries(
            held_pending[0].payload,
            held_pending[0].payload_checksum,
        )
        self.assertEqual(next(iter(held_entries.values()))["action"], "upsert")
        held_ingestion = self._ingestion("snapshot-2")
        stage_workload_states(held_ingestion, held_pending)
        promote_workload_states_locked(held_ingestion)

        self.sync.parameters["aci_allow_deletes"] = True
        self.sync.save(update_fields=["parameters"])
        workloads, _, _ = apply_durable_workload_deltas(
            self.sync,
            [_workload([], model_string=model_string)],
        )

        self.assertEqual(workloads[0].delete_rows, [row])

    def test_enrichment_only_software_version_state_never_derives_delete(self):
        row = {"platform_slug": "ios-xe", "version": "17.12.7"}
        _, pending, _ = apply_durable_workload_deltas(
            self.sync,
            [_workload([row], model_string="netbox_dlm.softwareversion")],
        )
        ingestion = self._ingestion("snapshot-1")
        stage_workload_states(ingestion, pending)
        promote_workload_states_locked(ingestion)

        workloads, _, summaries = apply_durable_workload_deltas(
            self.sync,
            [_workload([], model_string="netbox_dlm.softwareversion")],
        )

        self.assertEqual(
            workloads[0].delete_rows,
            [{"platform_slug": "ios-xe", "version": "17.12.7"}],
        )
        self.assertEqual(summaries[0]["delete_rows"], 1)

    def test_software_catalog_reconciliation_deletes_only_unreferenced_rows(self):
        from dcim.models import Platform
        from django.apps import apps

        SoftwareVersion = apps.get_model("netbox_dlm", "SoftwareVersion")
        platform = Platform.objects.create(name="Catalog OS", slug="catalog-os")
        SoftwareVersion.objects.create(platform=platform, version="1.0")
        SoftwareVersion.objects.create(platform=platform, version="2.0")
        current = {"platform_slug": platform.slug, "version": "2.0"}

        workloads, _, summaries = apply_durable_workload_deltas(
            self.sync,
            [_workload([current], model_string="netbox_dlm.softwareversion")],
        )

        self.assertEqual(
            workloads[0].delete_rows,
            [{"platform_slug": platform.slug, "version": "1.0"}],
        )
        self.assertEqual(summaries[0]["catalog_delete_rows"], 1)

    def test_catalog_deletes_follow_authoritative_association_deletes_same_run(self):
        from django.apps import apps
        from dcim.models import Device
        from dcim.models import DeviceRole
        from dcim.models import DeviceType
        from dcim.models import Manufacturer
        from dcim.models import Platform
        from dcim.models import Site

        DeviceSoftware = apps.get_model("netbox_dlm", "DeviceSoftware")
        SoftwareVersion = apps.get_model("netbox_dlm", "SoftwareVersion")
        CVE = apps.get_model("netbox_dlm", "CVE")
        Vulnerability = apps.get_model("netbox_dlm", "Vulnerability")
        manufacturer = Manufacturer.objects.create(
            name="Transition Vendor",
            slug="transition-vendor",
        )
        device_type = DeviceType.objects.create(
            manufacturer=manufacturer,
            model="Transition Model",
            slug="transition-model",
        )
        role = DeviceRole.objects.create(
            name="Transition Role",
            slug="transition-role",
        )
        site = Site.objects.create(name="Transition Site", slug="transition-site")
        platform = Platform.objects.create(
            name="Transition OS",
            slug="transition-os",
        )
        device = Device.objects.create(
            name="transition-device",
            device_type=device_type,
            role=role,
            site=site,
        )
        software_version = SoftwareVersion.objects.create(
            platform=platform,
            version="1.0",
        )
        cve = CVE.objects.create(cve_id="CVE-2026-21001")
        DeviceSoftware.objects.create(
            device=device,
            software_version=software_version,
        )
        Vulnerability.objects.create(
            device=device,
            software_version=software_version,
            cve=cve,
        )
        ForwardDeviceIdentity.objects.create(
            sync=self.sync,
            ingestion=self._ingestion("snapshot-0"),
            source_device_key=device.name,
            device=device,
        )
        association_row = {
            "name": device.name,
            "platform": platform.name,
            "platform_slug": platform.slug,
            "version": software_version.version,
        }
        _, baseline_pending, _ = apply_durable_workload_deltas(
            self.sync,
            [
                _workload(
                    [association_row],
                    model_string="netbox_dlm.devicesoftware",
                ),
                _workload(
                    [{**association_row, "cve_id": cve.cve_id}],
                    model_string="netbox_dlm.vulnerability",
                ),
                _workload(
                    [
                        {
                            "platform": platform.name,
                            "platform_slug": platform.slug,
                            "version": software_version.version,
                        }
                    ],
                    model_string="netbox_dlm.softwareversion",
                ),
                _workload(
                    [{"cve_id": cve.cve_id}],
                    model_string="netbox_dlm.cve",
                ),
            ],
        )
        baseline = self._ingestion("snapshot-1")
        stage_workload_states(baseline, baseline_pending)
        promote_workload_states_locked(baseline)

        workloads, _, summaries = apply_durable_workload_deltas(
            self.sync,
            [
                _workload([], model_string="netbox_dlm.devicesoftware"),
                _workload([], model_string="netbox_dlm.vulnerability"),
                _workload([], model_string="netbox_dlm.softwareversion"),
                _workload([], model_string="netbox_dlm.cve"),
            ],
        )
        by_model = {workload.model_string: workload for workload in workloads}
        by_summary = {summary["model"]: summary for summary in summaries}

        self.assertEqual(len(by_model["netbox_dlm.softwareversion"].delete_rows), 1)
        self.assertEqual(
            {
                field: by_model["netbox_dlm.softwareversion"].delete_rows[0][field]
                for field in ("platform_slug", "version")
            },
            {"platform_slug": platform.slug, "version": software_version.version},
        )
        self.assertEqual(
            by_model["netbox_dlm.cve"].delete_rows,
            [{"cve_id": cve.cve_id}],
        )
        self.assertEqual(
            by_summary["netbox_dlm.softwareversion"]["protected_delete_rows"],
            0,
        )
        self.assertEqual(
            by_summary["netbox_dlm.cve"]["protected_delete_rows"],
            0,
        )

    def test_peer_association_state_protects_catalog_transition_deletes(self):
        from django.apps import apps
        from dcim.models import Platform

        SoftwareVersion = apps.get_model("netbox_dlm", "SoftwareVersion")
        CVE = apps.get_model("netbox_dlm", "CVE")
        platform = Platform.objects.create(name="Peer OS", slug="peer-os")
        software_version = SoftwareVersion.objects.create(
            platform=platform,
            version="2.0",
        )
        cve = CVE.objects.create(cve_id="CVE-2026-21002")
        peer = ForwardSync.objects.create(
            name="workload-state-peer-associations",
            source=self.sync.source,
            user=self.sync.user,
            parameters={
                "netbox_dlm.devicesoftware": True,
                "netbox_dlm.vulnerability": True,
            },
        )
        peer_rows = [
            _workload(
                [
                    {
                        "name": "peer-device",
                        "platform_slug": platform.slug,
                        "version": software_version.version,
                    }
                ],
                model_string="netbox_dlm.devicesoftware",
            ),
            _workload(
                [
                    {
                        "name": "peer-device",
                        "cve_id": cve.cve_id,
                        "platform_slug": platform.slug,
                        "version": software_version.version,
                    }
                ],
                model_string="netbox_dlm.vulnerability",
            ),
        ]
        _, peer_pending, _ = apply_durable_workload_deltas(peer, peer_rows)
        peer_ingestion = ForwardIngestion.objects.create(
            sync=peer,
            snapshot_id="peer-snapshot",
            baseline_ready=True,
        )
        stage_workload_states(peer_ingestion, peer_pending)
        promote_workload_states_locked(peer_ingestion)

        workloads, _, summaries = apply_durable_workload_deltas(
            self.sync,
            [
                _workload([], model_string="netbox_dlm.devicesoftware"),
                _workload([], model_string="netbox_dlm.vulnerability"),
                _workload([], model_string="netbox_dlm.softwareversion"),
                _workload(
                    [],
                    delete_rows=[{"cve_id": cve.cve_id}],
                    model_string="netbox_dlm.cve",
                ),
            ],
        )

        self.assertNotIn(
            "netbox_dlm.softwareversion",
            {workload.model_string for workload in workloads},
        )
        self.assertNotIn(
            "netbox_dlm.cve",
            {workload.model_string for workload in workloads},
        )
        by_summary = {summary["model"]: summary for summary in summaries}
        self.assertEqual(
            by_summary["netbox_dlm.softwareversion"]["protected_delete_rows"],
            1,
        )
        self.assertEqual(by_summary["netbox_dlm.cve"]["protected_delete_rows"], 1)

    def _quarantine_cleared(self, device):
        """An absence streak past both default thresholds (3 runs, 72 hours)."""
        from datetime import timedelta

        from django.utils import timezone

        from forward_netbox.models import ForwardDeviceAbsence

        now = timezone.now()
        ForwardDeviceAbsence.objects.create(
            sync=self.sync,
            device=device,
            consecutive_absent_runs=3,
            first_absent_at=now - timedelta(hours=73),
            last_absent_at=now,
        )

    def test_owned_device_absent_past_quarantine_is_deleted(self):
        from dcim.models import Device
        from dcim.models import DeviceRole
        from dcim.models import DeviceType
        from dcim.models import Manufacturer
        from dcim.models import Site

        site = Site.objects.create(name="Owned Device Site", slug="owned-device-site")
        manufacturer = Manufacturer.objects.create(
            name="Owned Device Vendor", slug="owned-device-vendor"
        )
        device_type = DeviceType.objects.create(
            manufacturer=manufacturer,
            model="Owned Device Type",
            slug="owned-device-type",
        )
        role = DeviceRole.objects.create(
            name="Owned Device Role", slug="owned-device-role"
        )
        device = Device.objects.create(
            name="stale-device",
            site=site,
            device_type=device_type,
            role=role,
            status="active",
        )
        ingestion = self._ingestion("snapshot-0")
        ForwardDeviceIdentity.objects.create(
            sync=self.sync,
            ingestion=ingestion,
            source_device_key=device.name,
            device=device,
        )
        # The absence must have been CONFIRMED, repeatedly and over time, before
        # the sweep may act on it - the same quarantine Prune orphans requires.
        # This used to assert deletion on the first absence with no evidence at
        # all, which is the policy the quarantine exists to forbid.
        self._quarantine_cleared(device)

        workloads, _, summaries = apply_durable_workload_deltas(
            self.sync,
            [
                _workload(
                    [],
                    model_string="dcim.device",
                    coalesce_fields=[["name", "site"]],
                )
            ],
        )

        self.assertEqual(
            workloads[0].delete_rows,
            [{"name": "stale-device", "site": "Owned Device Site"}],
        )
        self.assertEqual(summaries[0]["ownership_delete_rows"], 1)
        self.assertEqual(summaries[0]["ownership_quarantine_held_rows"], 0)

    def test_owned_device_first_absence_is_held_by_the_quarantine(self):
        """The policy change itself: one absence is not evidence.

        No absence row exists for this device, so the quarantine holds it
        fail-closed - by its reckoning the absence has been confirmed zero
        times. The device survives, no delete workload is produced, and the
        summary says why.
        """
        from dcim.models import Device
        from dcim.models import DeviceRole
        from dcim.models import DeviceType
        from dcim.models import Manufacturer
        from dcim.models import Site

        site = Site.objects.create(name="Fresh Absence Site", slug="fresh-absence")
        manufacturer = Manufacturer.objects.create(
            name="Fresh Absence Vendor", slug="fresh-absence-vendor"
        )
        device_type = DeviceType.objects.create(
            manufacturer=manufacturer,
            model="Fresh Absence Type",
            slug="fresh-absence-type",
        )
        role = DeviceRole.objects.create(
            name="Fresh Absence Role", slug="fresh-absence-role"
        )
        device = Device.objects.create(
            name="fresh-absent-device",
            site=site,
            device_type=device_type,
            role=role,
            status="active",
        )
        ingestion = self._ingestion("snapshot-0")
        ForwardDeviceIdentity.objects.create(
            sync=self.sync,
            ingestion=ingestion,
            source_device_key=device.name,
            device=device,
        )

        workloads, _, summaries = apply_durable_workload_deltas(
            self.sync,
            [
                _workload(
                    [],
                    model_string="dcim.device",
                    coalesce_fields=[["name", "site"]],
                )
            ],
        )

        self.assertEqual(workloads, [])
        self.assertEqual(summaries[0]["ownership_delete_rows"], 0)
        self.assertEqual(summaries[0]["ownership_quarantine_held_rows"], 1)
        self.assertTrue(Device.objects.filter(pk=device.pk).exists())

    def test_a_mass_absence_is_a_fault_and_deletes_nothing(self):
        """Past the floor and the ratio, absence stops being attrition.

        Every device here has individually cleared the quarantine - that is the
        point. A query fault absents the whole fleet for three days just as
        convincingly as a decommission absents one device, so the per-device
        test cannot tell them apart and the aggregate one must.
        """
        from dcim.models import Device
        from dcim.models import DeviceRole
        from dcim.models import DeviceType
        from dcim.models import Manufacturer
        from dcim.models import Site

        from forward_netbox.utilities.scope_reconciliation import (
            SCOPE_SHRINK_REFUSAL_FLOOR,
        )

        site = Site.objects.create(name="Mass Absence Site", slug="mass-absence")
        manufacturer = Manufacturer.objects.create(
            name="Mass Absence Vendor", slug="mass-absence-vendor"
        )
        device_type = DeviceType.objects.create(
            manufacturer=manufacturer,
            model="Mass Absence Type",
            slug="mass-absence-type",
        )
        role = DeviceRole.objects.create(
            name="Mass Absence Role", slug="mass-absence-role"
        )
        ingestion = self._ingestion("snapshot-0")
        count = SCOPE_SHRINK_REFUSAL_FLOOR + 1
        for index in range(count):
            device = Device.objects.create(
                name=f"mass-absent-{index}",
                site=site,
                device_type=device_type,
                role=role,
                status="active",
            )
            ForwardDeviceIdentity.objects.create(
                sync=self.sync,
                ingestion=ingestion,
                source_device_key=device.name,
                device=device,
            )
            self._quarantine_cleared(device)

        workloads, _, summaries = apply_durable_workload_deltas(
            self.sync,
            [
                _workload(
                    [],
                    model_string="dcim.device",
                    coalesce_fields=[["name", "site"]],
                )
            ],
        )

        self.assertEqual(workloads, [])
        self.assertEqual(summaries[0]["ownership_delete_rows"], 0)
        self.assertEqual(summaries[0]["ownership_shrink_held_rows"], count)
        self.assertEqual(
            Device.objects.filter(name__startswith="mass-absent-").count(), count
        )

    def test_current_scope_claim_protects_owned_device_from_delete(self):
        from dcim.models import Device
        from dcim.models import DeviceRole
        from dcim.models import DeviceType
        from dcim.models import Manufacturer
        from dcim.models import Site
        from extras.models import Tag

        site = Site.objects.create(
            name="Claimed Device Site", slug="claimed-device-site"
        )
        manufacturer = Manufacturer.objects.create(
            name="Claimed Device Vendor", slug="claimed-device-vendor"
        )
        device_type = DeviceType.objects.create(
            manufacturer=manufacturer,
            model="Claimed Device Type",
            slug="claimed-device-type",
        )
        role = DeviceRole.objects.create(
            name="Claimed Device Role", slug="claimed-device-role"
        )
        device = Device.objects.create(
            name="claimed-device",
            site=site,
            device_type=device_type,
            role=role,
            status="active",
        )
        ingestion = self._ingestion("snapshot-0")
        ForwardDeviceIdentity.objects.create(
            sync=self.sync,
            ingestion=ingestion,
            source_device_key=device.name,
            device=device,
        )
        ForwardDeviceTagClaim.objects.create(
            sync=self.sync,
            ingestion=ingestion,
            device=device,
            tag=Tag.objects.create(name="Managed", slug="managed"),
            claim_type=ForwardDeviceTagClaim.ClaimType.SCOPE,
        )
        # Past the quarantine deliberately, so the CLAIM is what saves the
        # device - which is what this test exists to pin. Without this the
        # quarantine holds it first and the claim guard is never consulted.
        self._quarantine_cleared(device)

        workloads, _, summaries = apply_durable_workload_deltas(
            self.sync,
            [
                _workload(
                    [],
                    model_string="dcim.device",
                    coalesce_fields=[["name", "site"]],
                )
            ],
        )

        self.assertEqual(workloads, [])
        self.assertEqual(summaries[0]["protected_delete_rows"], 1)
        self.assertEqual(summaries[0]["ownership_delete_rows"], 0)

    def test_parameter_change_seeds_without_deleting_previous_scope(self):
        initial = [{"device": "router-1", "name": "Ethernet1"}]
        _, pending, _ = apply_durable_workload_deltas(
            self.sync,
            [_workload(initial)],
        )
        ingestion = self._ingestion("snapshot-1")
        stage_workload_states(ingestion, pending)
        promote_workload_states_locked(ingestion)

        replacement = [{"device": "router-2", "name": "Ethernet2"}]
        workloads, _, summaries = apply_durable_workload_deltas(
            self.sync,
            [_workload(replacement, parameters={"include_tags": ["new"]})],
        )

        self.assertEqual(workloads[0].upsert_rows, replacement)
        self.assertEqual(workloads[0].delete_rows, [])
        self.assertEqual(summaries[0]["mode"], "contract_reset")

    def test_first_dlm_baseline_deletes_legacy_rows_absent_from_target(self):
        current = [{"cve_id": "CVE-2026-0001"}]
        legacy = [
            {"cve_id": "CVE-2026-0001"},
            {"cve_id": "CVE-2026-0002"},
        ]

        with patch(
            "forward_netbox.utilities.workload_state._bootstrap_dlm_rows",
            return_value=legacy,
        ):
            workloads, _, summaries = apply_durable_workload_deltas(
                self.sync,
                [_workload(current, model_string="netbox_dlm.cve")],
            )

        self.assertEqual(workloads[0].upsert_rows, current)
        self.assertEqual(workloads[0].delete_rows, [{"cve_id": "CVE-2026-0002"}])
        self.assertEqual(summaries[0]["mode"], "seed_reconcile")
        self.assertEqual(summaries[0]["bootstrap_delete_rows"], 1)

    def test_pending_state_does_not_replace_current_until_promoted(self):
        _, first_pending, _ = apply_durable_workload_deltas(
            self.sync,
            [_workload([{"device": "router-1", "name": "Ethernet1"}])],
        )
        first = self._ingestion("snapshot-1")
        stage_workload_states(first, first_pending)
        promote_workload_states_locked(first)
        _, second_pending, _ = apply_durable_workload_deltas(
            self.sync,
            [_workload([{"device": "router-1", "name": "Ethernet2"}])],
        )
        second = self._ingestion("snapshot-2")

        stage_workload_states(second, second_pending)

        self.assertEqual(
            ForwardWorkloadState.objects.get(is_current=True).ingestion,
            first,
        )
        self.assertFalse(ForwardWorkloadState.objects.get(ingestion=second).is_current)
        promote_workload_states_locked(second)
        self.assertEqual(
            ForwardWorkloadState.objects.get(is_current=True).ingestion,
            second,
        )
