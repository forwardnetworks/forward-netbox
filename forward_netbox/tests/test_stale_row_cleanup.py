"""The operator cleanup for device rows Forward stopped returning.

Before 3.0.14 a scope change replaced the durable state and forgot the rows
Forward stopped returning on that run, so nothing ever deleted them. The
cleanup may delete such a row only when ALL of these hold:

1. its model is a device-scoped child model the sync itself may delete;
2. its device is in the sync's current scope and was fetched on the last run;
3. no current durable state holds its identity;
4. this sync created it and nobody else changed it;
5. its model's state is present, current and from the latest run, and the
   share of absent rows is not implausibly large.

Each rule has a test here that fails if the rule is removed, written from the
rule rather than from the implementation: every fixture row satisfies every
rule except the one under test.
"""

import uuid
from datetime import timedelta
from pathlib import Path
from unittest.mock import Mock
from unittest.mock import patch

from core.choices import JobStatusChoices
from core.models import Job
from core.models import ObjectChange
from dcim.models import Cable
from dcim.models import Device
from dcim.models import DeviceRole
from dcim.models import DeviceType
from dcim.models import Interface
from dcim.models import InterfaceTemplate
from dcim.models import InventoryItem
from dcim.models import Manufacturer
from dcim.models import Module
from dcim.models import ModuleBay
from dcim.models import ModuleType
from dcim.models import Site
from django.apps import apps
from django.contrib.contenttypes.models import ContentType
from django.test import RequestFactory
from django.test import TestCase
from django.utils import timezone
from extras.models import Tag
from ipam.models import IPAddress

from forward_netbox.models import ForwardDeviceIdentity
from forward_netbox.models import ForwardDeviceTagClaim
from forward_netbox.models import ForwardIngestion
from forward_netbox.models import ForwardSource
from forward_netbox.models import ForwardSync
from forward_netbox.models import ForwardWorkloadState
from forward_netbox.utilities.branch_budget import DEVICE_SHARD_MODELS
from forward_netbox.utilities.full_removal_reconciliation import (
    BASELINE_REMOVAL_MODELS,
)
from forward_netbox.utilities.stale_row_cleanup import HOLD_ATTACHED_ROWS_STAY
from forward_netbox.utilities.stale_row_cleanup import HOLD_MODULE_HAS_COMPONENTS
from forward_netbox.utilities.stale_row_cleanup import HOLD_OLDER_THAN_CHANGE_LOG
from forward_netbox.utilities.stale_row_cleanup import (
    REFUSED_CHILD_STATE_UNAVAILABLE,
)
from forward_netbox.utilities.stale_row_cleanup import (
    REFUSED_OPTION_WIDER_THAN_PREVIEW,
)
from forward_netbox.utilities.stale_row_cleanup import state_row_keys
from forward_netbox.utilities.stale_row_cleanup import STALE_ROW_CLEANUP_LEFT_OUT
from forward_netbox.utilities.stale_row_cleanup import HOLD_CHANGED_OUTSIDE_SYNC
from forward_netbox.utilities.stale_row_cleanup import HOLD_NO_CREATE_RECORD
from forward_netbox.utilities.stale_row_cleanup import HOLD_TEMPLATE_COMPONENT
from forward_netbox.utilities.stale_row_cleanup import plan_stale_row_cleanup
from forward_netbox.utilities.stale_row_cleanup import preview_stale_row_cleanup
from forward_netbox.utilities.stale_row_cleanup import REFUSED_FRACTION
from forward_netbox.utilities.stale_row_cleanup import REFUSED_GREW_SINCE_PREVIEW
from forward_netbox.utilities.stale_row_cleanup import REFUSED_NO_PREVIEW
from forward_netbox.utilities.stale_row_cleanup import REFUSED_NO_DEVICE_LIST
from forward_netbox.utilities.stale_row_cleanup import REFUSED_NO_STATE
from forward_netbox.utilities.stale_row_cleanup import REFUSED_NOT_ENABLED
from forward_netbox.utilities.stale_row_cleanup import (
    HOLD_CASCADE_REACHES_OTHER_ROWS,
)
from forward_netbox.utilities.stale_row_cleanup import HOLD_MANAGEMENT_INTERFACE
from forward_netbox.utilities.stale_row_cleanup import REFUSED_STATE_MISMATCH
from forward_netbox.utilities.stale_row_cleanup import REFUSED_STATE_ROW_WITHOUT_KEY
from forward_netbox.utilities.stale_row_cleanup import REFUSED_STATE_STALE
from forward_netbox.utilities.stale_row_cleanup import REFUSED_UNREPRESENTED_PEER
from forward_netbox.utilities.stale_row_cleanup import sync_may_delete
from forward_netbox.utilities.stale_row_cleanup import remove_stale_rows
from forward_netbox.utilities.stale_row_cleanup import STALE_ROW_CLEANUP_MODELS
from forward_netbox.utilities.stale_row_cleanup import StaleRowCleanupRefused
from forward_netbox.utilities.sync import ForwardSyncRunner
from forward_netbox.utilities.workload_state import build_state_entries
from forward_netbox.utilities.workload_state import CARRIED_ROW_HASH
from forward_netbox.utilities.workload_state import encode_state_entries

IFACE = "dcim.interface"
IP = "ipam.ipaddress"
CABLE = "dcim.cable"
INVENTORY = "dcim.inventoryitem"
MODULE = "dcim.module"
BGP_PEER = "netbox_routing.bgppeer"
BGP_AF = "netbox_routing.bgpaddressfamily"
BGP_PEER_AF = "netbox_routing.bgppeeraddressfamily"
OSPF_INSTANCE = "netbox_routing.ospfinstance"
OSPF_INTERFACE = "netbox_routing.ospfinterface"
PEERING_SESSION = "netbox_peering_manager.peeringsession"
# Every model the 3.0.14 extension added, plus the parents and children they
# interact with; the tests below switch exactly the ones they exercise on, so
# no other model's refusal leaks into a result.
EXTENDED_MODELS = (
    INVENTORY,
    MODULE,
    BGP_PEER,
    BGP_AF,
    BGP_PEER_AF,
    OSPF_INSTANCE,
    OSPF_INTERFACE,
    PEERING_SESSION,
)
COALESCE = {
    "dcim.device": [["name"]],
    "dcim.macaddress": [["mac_address"]],
    IFACE: [["device", "name"]],
    IP: [["address", "vrf"], ["address"]],
    CABLE: [["device", "interface", "remote_device", "remote_interface"]],
    INVENTORY: [
        ["device", "name", "part_id", "serial"],
        ["device", "name", "part_id"],
        ["device", "name"],
    ],
    MODULE: [["device", "module_bay"]],
    BGP_PEER: [
        ["device", "vrf", "neighbor_address"],
        ["device", "neighbor_address"],
    ],
    BGP_AF: [
        ["device", "vrf", "local_asn", "afi_safi"],
        ["device", "local_asn", "afi_safi"],
    ],
    BGP_PEER_AF: [
        ["device", "vrf", "neighbor_address", "afi_safi"],
        ["device", "neighbor_address", "afi_safi"],
    ],
    OSPF_INSTANCE: [["device", "vrf", "process_id"], ["device", "process_id"]],
    OSPF_INTERFACE: [["device", "process_id", "local_interface"]],
    PEERING_SESSION: [
        ["device", "vrf", "neighbor_address"],
        ["device", "neighbor_address"],
    ],
}


class StaleRowCleanupFixture(TestCase):
    def setUp(self):
        self.source = ForwardSource.objects.create(
            name="stale-source",
            type="saas",
            url="https://forward.example.com",
            status="ready",
            parameters={
                "network_id": "net-1",
                "device_tag_include_tags": ["in-scope"],
            },
        )
        self.sync = ForwardSync.objects.create(
            name="stale-sync",
            source=self.source,
            parameters={
                "snapshot_id": "latestProcessed",
                IFACE: True,
                IP: True,
                CABLE: True,
                "dcim.macaddress": True,
            },
        )
        self.request_id = uuid.uuid4()
        self.ingestion = ForwardIngestion.objects.create(
            sync=self.sync,
            change_request_id=self.request_id,
            merge_finalized_at=timezone.now(),
        )
        site = Site.objects.create(name="Stale Site", slug="stale-site")
        mfr = Manufacturer.objects.create(name="Stale Mfr", slug="stale-mfr")
        self.device_type = DeviceType.objects.create(
            manufacturer=mfr, model="Stale DT", slug="stale-dt"
        )
        role = DeviceRole.objects.create(name="Stale Role", slug="stale-role")
        self.tag = Tag.objects.create(name="in-scope", slug="in-scope")

        def device(name):
            return Device.objects.create(
                name=name,
                site=site,
                device_type=self.device_type,
                role=role,
                status="active",
            )

        self.edge1 = device("edge-1")
        self.edge2 = device("edge-2")
        for dev in (self.edge1, self.edge2):
            self._own(dev)
            self._scope_claim(dev)
        # The devices the last run fetched.
        self.device_state = self._state(
            "dcim.device", [{"name": "edge-1"}, {"name": "edge-2"}]
        )
        # Present in NetBox and in this sync's state, so every model has a
        # state that describes NetBox.
        self.kept1 = self._interface(self.edge1, "Ethernet1")
        self.kept2 = self._interface(self.edge2, "Ethernet1")

    # -- helpers -----------------------------------------------------------

    def _own(self, dev):
        ForwardDeviceIdentity.objects.create(
            sync=self.sync, source_device_key=dev.name, device=dev
        )

    def _scope_claim(self, dev):
        ForwardDeviceTagClaim.objects.create(
            sync=self.sync, device=dev, tag=self.tag, claim_type="scope"
        )

    def _created(self, obj, request_id=None, action="create"):
        ObjectChange.objects.create(
            request_id=request_id or self.request_id,
            user_name="forward",
            changed_object_type=ContentType.objects.get_for_model(type(obj)),
            changed_object_id=obj.pk,
            object_repr=str(obj),
            action=action,
        )

    def _interface(self, dev, name, *, created_by_sync=True):
        iface = Interface.objects.create(device=dev, name=name, type="1000base-t")
        if created_by_sync:
            self._created(iface)
        return iface

    def _state(self, model_string, rows, *, sync=None, ingestion=None, carried=()):
        entries = build_state_entries(model_string, rows, COALESCE[model_string])
        if carried:
            for identity, value in build_state_entries(
                model_string, carried, COALESCE[model_string]
            ).items():
                entries[identity] = {**value, "row_hash": CARRIED_ROW_HASH}
        payload, checksum = encode_state_entries(entries)
        return ForwardWorkloadState.objects.create(
            sync=sync or self.sync,
            ingestion=ingestion or self.ingestion,
            model_string=model_string,
            parameter_hash="p",
            identity_contract_hash="c",
            payload=payload,
            payload_checksum=checksum,
            row_count=len(entries),
            is_current=True,
        )

    def _interface_state(self, extra=(), **kwargs):
        return self._state(
            IFACE,
            [
                {"device": "edge-1", "name": "Ethernet1"},
                {"device": "edge-2", "name": "Ethernet1"},
                *extra,
            ],
            **kwargs,
        )

    def _iface_plan(self):
        return plan_stale_row_cleanup(self.sync)[IFACE]

    def _removable_pks(self):
        return set(self._iface_plan()["deletable"])

    def _preview(self):
        return Job.objects.create(
            object_type=ContentType.objects.get_for_model(ForwardSync),
            object_id=self.sync.pk,
            name=f"{self.sync.name} - preview stale device rows",
            status=JobStatusChoices.STATUS_COMPLETED,
            job_id=uuid.uuid4(),
            data=preview_stale_row_cleanup(self.sync),
        )


class StaleRowCandidateRulesTest(StaleRowCleanupFixture):
    # -- the positive case: every rule holds --------------------------------

    def test_a_forgotten_sync_created_interface_on_an_in_scope_device_is_removable(
        self,
    ):
        stale = self._interface(self.edge1, "Ethernet2")
        self._interface_state()

        self.assertEqual(self._removable_pks(), {stale.pk})

    # -- rule 1: model allowlist -------------------------------------------

    def test_only_device_scoped_models_the_sync_may_delete_are_covered(self):
        allowed = DEVICE_SHARD_MODELS & BASELINE_REMOVAL_MODELS
        self.assertTrue(set(STALE_ROW_CLEANUP_MODELS) <= allowed)
        self.assertNotIn("dcim.device", STALE_ROW_CLEANUP_MODELS)
        self.assertEqual(
            set(plan_stale_row_cleanup(self.sync)), set(STALE_ROW_CLEANUP_MODELS)
        )

    def test_a_model_the_sync_may_not_delete_is_refused_even_if_listed(self):
        from unittest.mock import patch

        with patch(
            "forward_netbox.utilities.stale_row_cleanup.STALE_ROW_CLEANUP_MODELS",
            ("dcim.device", *STALE_ROW_CLEANUP_MODELS),
        ):
            with self.assertRaises(StaleRowCleanupRefused):
                plan_stale_row_cleanup(self.sync)
        self.assertEqual(Device.objects.count(), 2)

    def test_the_guard_reads_both_allowlists_not_only_the_collectors(self):
        from unittest.mock import patch

        self.assertTrue(sync_may_delete(IFACE))
        with patch(
            "forward_netbox.utilities.stale_row_cleanup.BASELINE_REMOVAL_MODELS",
            frozenset(),
        ):
            self.assertFalse(sync_may_delete(IFACE))
        with patch("forward_netbox.utilities.branch_budget.DEVICE_SHARD_MODELS", set()):
            self.assertFalse(sync_may_delete(IFACE))

    def test_a_model_disabled_on_the_sync_is_refused(self):
        self._interface(self.edge1, "Ethernet2")
        self._interface_state()
        self.sync.parameters = {**self.sync.parameters, IFACE: False}
        self.sync.save()

        plan = self._iface_plan()

        self.assertEqual(plan["refused"], REFUSED_NOT_ENABLED)
        self.assertEqual(plan["deletable"], {})

    # -- rule 2: device in current scope and fetched ------------------------

    def test_a_device_missing_from_the_device_list_is_never_judged(self):
        outside = Device.objects.create(
            name="edge-3",
            site=self.edge1.site,
            device_type=self.device_type,
            role=self.edge1.role,
        )
        self._own(outside)  # owned, but the include tags no longer match it
        self._interface(outside, "Ethernet1")
        stale = self._interface(outside, "Ethernet2")
        self._interface_state(extra=[{"device": "edge-3", "name": "Ethernet1"}])

        self.assertNotIn(stale.pk, self._removable_pks())

    def test_a_device_marked_out_of_scope_is_never_judged(self):
        stale = self._interface(self.edge1, "Ethernet2")
        ForwardDeviceTagClaim.objects.create(
            sync=self.sync, device=self.edge1, tag=self.tag, claim_type="out_of_scope"
        )
        self._interface_state()

        self.assertNotIn(stale.pk, self._removable_pks())

    def test_a_device_another_sync_identifies_is_never_judged(self):
        other_sync = ForwardSync.objects.create(
            name="other-sync", source=self.source, parameters={}
        )
        ForwardDeviceIdentity.objects.filter(device=self.edge1).update(sync=other_sync)
        stale = self._interface(self.edge1, "Ethernet2")
        self._interface_state()

        self.assertNotIn(stale.pk, self._removable_pks())

    def test_a_device_the_last_run_did_not_return_is_never_judged(self):
        # Without scope claims or identity records (a sync with "apply device
        # scope tags" off has none), the device list is the last run's state.
        ForwardDeviceIdentity.objects.all().delete()
        ForwardDeviceTagClaim.objects.all().delete()
        self.device_state.delete()
        self._state("dcim.device", [{"name": "edge-2"}])
        stale1 = self._interface(self.edge1, "Ethernet2")
        stale2 = self._interface(self.edge2, "Ethernet2")
        self._interface_state()

        removable = self._removable_pks()

        self.assertNotIn(stale1.pk, removable)
        self.assertIn(stale2.pk, removable)

    def test_without_a_device_list_every_model_is_refused_with_a_reason(self):
        self._interface(self.edge1, "Ethernet2")
        self._interface_state()
        self.device_state.delete()

        plans = plan_stale_row_cleanup(self.sync)

        # Every model enabled on this sync (the others are refused for that).
        self.assertEqual(
            {plans[model]["refused"] for model in (IFACE, IP, CABLE)},
            {REFUSED_NO_DEVICE_LIST},
        )
        self.assertFalse(any(plan["deletable"] for plan in plans.values()))

    def test_a_device_list_from_an_older_run_is_not_used(self):
        stale = self._interface(self.edge1, "Ethernet2")
        newer = ForwardIngestion.objects.create(
            sync=self.sync,
            change_request_id=uuid.uuid4(),
            merge_finalized_at=timezone.now(),
        )
        self._interface_state(ingestion=newer)

        plan = self._iface_plan()

        self.assertEqual(plan["refused"], REFUSED_NO_DEVICE_LIST)
        self.assertNotIn(stale.pk, plan["deletable"])

    def test_uncovered_and_backfilled_devices_are_never_judged(self):
        for claim_type in ("uncovered", "backfilled"):
            ForwardDeviceTagClaim.objects.filter(device=self.edge1).exclude(
                claim_type="scope"
            ).delete()
            ForwardDeviceTagClaim.objects.create(
                sync=self.sync, device=self.edge1, tag=self.tag, claim_type=claim_type
            )
            stale = self._interface(self.edge1, f"Ethernet-{claim_type}")
            ForwardWorkloadState.objects.filter(model_string=IFACE).delete()
            self._interface_state()

            self.assertNotIn(stale.pk, self._removable_pks(), claim_type)

    def test_a_name_two_devices_share_is_never_judged(self):
        Device.objects.create(
            name="edge-1",
            site=Site.objects.create(name="Other Site", slug="other-site"),
            device_type=self.device_type,
            role=self.edge1.role,
        )
        stale = self._interface(self.edge1, "Ethernet2")
        self._interface_state()

        self.assertNotIn(stale.pk, self._removable_pks())

    def test_a_device_the_last_run_did_not_fetch_is_never_judged(self):
        # edge-2's rows are only carried (it was out of scope that run): its
        # interfaces were not Forward's to judge, so none is stale.
        stale = self._interface(self.edge2, "Ethernet2")
        self._state(
            IFACE,
            [{"device": "edge-1", "name": "Ethernet1"}],
            carried=[{"device": "edge-2", "name": "Ethernet1"}],
        )

        self.assertNotIn(stale.pk, self._removable_pks())

    # -- rule 3: absent from every current state ----------------------------

    def test_a_row_in_this_syncs_state_is_never_removable(self):
        self._interface(self.edge1, "Ethernet2")
        self._interface_state(extra=[{"device": "edge-1", "name": "Ethernet2"}])

        self.assertEqual(self._removable_pks(), set())

    def test_a_row_carried_in_the_state_is_never_removable(self):
        self._interface(self.edge1, "Ethernet2")
        self._interface_state(
            carried=[{"device": "edge-1", "name": "Ethernet2"}],
        )

        self.assertEqual(self._removable_pks(), set())

    def test_a_row_another_syncs_state_holds_is_never_removable(self):
        self._interface(self.edge1, "Ethernet2")
        self._interface_state()
        other = ForwardSync.objects.create(
            name="stale-other", source=self.source, parameters={IFACE: True}
        )
        other_ingestion = ForwardIngestion.objects.create(sync=other)
        self._state(
            IFACE,
            [{"device": "edge-1", "name": "Ethernet2"}],
            sync=other,
            ingestion=other_ingestion,
        )

        self.assertEqual(self._removable_pks(), set())

    def test_an_abbreviated_name_in_the_state_keeps_the_full_name_in_netbox(self):
        # The sync's interface lookup matches Gi0/1 to GigabitEthernet0/1; the
        # cleanup must never read that as absence.
        self._interface(self.edge1, "GigabitEthernet0/1")
        self._interface_state(extra=[{"device": "edge-1", "name": "Gi0/1"}])

        self.assertEqual(self._removable_pks(), set())

    # -- rule 4: created by this sync, changed by nobody else ---------------

    def test_a_hand_added_interface_on_an_in_scope_device_is_never_a_candidate(
        self,
    ):
        hand = self._interface(self.edge1, "Ethernet9", created_by_sync=False)
        self._interface_state()

        plan = self._iface_plan()

        self.assertNotIn(hand.pk, plan["deletable"])
        self.assertEqual(plan["held"], {HOLD_NO_CREATE_RECORD: 1})

    def test_a_row_another_ingestion_created_is_never_a_candidate(self):
        other = self._interface(self.edge1, "Ethernet9", created_by_sync=False)
        self._created(other, request_id=uuid.uuid4())
        self._interface_state()

        self.assertEqual(self._removable_pks(), set())

    def test_a_sync_created_row_someone_else_edited_is_held(self):
        edited = self._interface(self.edge1, "Ethernet9")
        self._created(edited, request_id=uuid.uuid4(), action="update")
        self._interface_state()

        plan = self._iface_plan()

        self.assertEqual(plan["deletable"], {})
        self.assertEqual(plan["held"], {HOLD_CHANGED_OUTSIDE_SYNC: 1})

    def test_a_device_type_template_interface_is_never_a_candidate(self):
        InterfaceTemplate.objects.create(
            device_type=self.device_type, name="mgmt0", type="1000base-t"
        )
        templated = self._interface(self.edge1, "mgmt0")
        self._interface_state()

        plan = self._iface_plan()

        self.assertNotIn(templated.pk, plan["deletable"])
        self.assertEqual(plan["held"], {HOLD_TEMPLATE_COMPONENT: 1})

    # -- rule 5: the whole-model refusals -----------------------------------

    def test_no_state_refuses_the_model(self):
        self._interface(self.edge1, "Ethernet2")

        plan = self._iface_plan()

        self.assertEqual(plan["refused"], REFUSED_NO_STATE)
        self.assertEqual(plan["deletable"], {})

    def test_a_state_that_is_not_current_refuses_the_model(self):
        self._interface(self.edge1, "Ethernet2")
        self._interface_state()
        ForwardWorkloadState.objects.filter(sync=self.sync, model_string=IFACE).update(
            is_current=False
        )

        self.assertEqual(self._iface_plan()["refused"], REFUSED_NO_STATE)

    def test_a_state_older_than_the_latest_run_refuses_the_model(self):
        self._interface(self.edge1, "Ethernet2")
        self._interface_state()
        newer = ForwardIngestion.objects.create(
            sync=self.sync,
            change_request_id=uuid.uuid4(),
            merge_finalized_at=timezone.now(),
        )
        # The device list is from the latest run; the interface state is not.
        self.device_state.ingestion = newer
        self.device_state.save()

        plan = self._iface_plan()

        self.assertEqual(plan["refused"], REFUSED_STATE_STALE)
        self.assertEqual(plan["deletable"], {})

    def test_too_large_an_absent_share_refuses_the_model(self):
        for index in range(2, 32):
            self._interface(self.edge1, f"Ethernet{index}")
        self._interface_state()

        plan = self._iface_plan()

        self.assertEqual(plan["refused"], REFUSED_FRACTION)
        self.assertEqual(plan["deletable"], {})


class StaleRowRefusalExtrasTest(StaleRowCleanupFixture):
    def test_another_sync_with_no_comparable_state_refuses_the_model(self):
        # The sync's own delete gate: a peer that may assert the row without a
        # state to say so.
        self._interface(self.edge1, "Ethernet2")
        self._interface_state()
        peer = ForwardSync.objects.create(
            name="stale-peer", source=self.source, parameters={IFACE: True}
        )
        ForwardIngestion.objects.create(sync=peer, baseline_ready=True)

        plan = self._iface_plan()

        self.assertEqual(plan["refused"], REFUSED_UNREPRESENTED_PEER)
        self.assertEqual(plan["deletable"], {})

    def test_a_state_row_the_cleanup_cannot_key_refuses_the_model(self):
        stale = self._interface(self.edge1, "Ethernet2")
        address = IPAddress.objects.create(address="10.0.0.9/24", assigned_object=stale)
        self._created(address)
        self._interface_state()
        self._state(IP, [{"device": "edge-1", "address": "not-an-address"}])

        plan = plan_stale_row_cleanup(self.sync)[IP]

        self.assertEqual(plan["refused"], REFUSED_STATE_ROW_WITHOUT_KEY)
        self.assertEqual(plan["deletable"], {})

    def test_a_management_step_interface_is_never_a_candidate(self):
        from forward_netbox.utilities.management_interfaces import (
            MANAGEMENT_INTERFACE_TAG_SLUG,
        )

        made = self._interface(self.edge1, "mgmt-forward")
        made.tags.add(
            Tag.objects.create(
                name=MANAGEMENT_INTERFACE_TAG_SLUG, slug=MANAGEMENT_INTERFACE_TAG_SLUG
            )
        )
        self._interface_state()

        plan = self._iface_plan()

        self.assertNotIn(made.pk, plan["deletable"])
        self.assertEqual(plan["held"], {HOLD_MANAGEMENT_INTERFACE: 1})


class StaleRowCrossModelTest(StaleRowCleanupFixture):
    def test_an_interface_carrying_an_address_that_stays_is_held(self):
        stale = self._interface(self.edge1, "Ethernet2")
        address = IPAddress.objects.create(address="10.0.0.1/24", assigned_object=stale)
        self._created(address)
        self._interface_state()
        self._state(
            IP,
            [{"device": "edge-1", "interface": "Ethernet2", "address": "10.0.0.1/24"}],
        )

        plans = plan_stale_row_cleanup(self.sync)

        self.assertEqual(plans[IP]["deletable"], {})
        self.assertNotIn(stale.pk, plans[IFACE]["deletable"])
        self.assertEqual(plans[IFACE]["held"], {HOLD_ATTACHED_ROWS_STAY: 1})

    def test_an_address_matched_by_host_is_never_removable(self):
        # Forward and NetBox may disagree on the mask; the host is the identity
        # the cleanup is willing to call absent.
        address = IPAddress.objects.create(
            address="10.0.0.1/32", assigned_object=self.kept1
        )
        self._created(address)
        self._interface_state()
        self._state(
            IP,
            [{"device": "edge-1", "interface": "Ethernet1", "address": "10.0.0.1/24"}],
        )

        self.assertEqual(plan_stale_row_cleanup(self.sync)[IP]["deletable"], {})

    def test_a_mac_written_another_way_is_never_removable(self):
        from dcim.models import MACAddress

        mac = MACAddress.objects.create(
            mac_address="00:1B:77:49:54:FD", assigned_object=self.kept1
        )
        self._created(mac)
        self._interface_state()
        self._state(
            "dcim.macaddress",
            [
                {
                    "device": "edge-1",
                    "interface": "Ethernet1",
                    "mac": "00-1b-77-49-54-fd",
                    "mac_address": "00-1b-77-49-54-fd",
                }
            ],
        )

        self.assertEqual(
            plan_stale_row_cleanup(self.sync)["dcim.macaddress"]["deletable"], {}
        )

    def test_a_stale_cable_between_in_scope_devices_is_removable(self):
        a2 = self._interface(self.edge1, "Ethernet2")
        b2 = self._interface(self.edge2, "Ethernet2")
        kept = Cable(a_terminations=[self.kept1], b_terminations=[self.kept2])
        kept.save()
        stale = Cable(a_terminations=[a2], b_terminations=[b2])
        stale.save()
        for cable in (kept, stale):
            self._created(cable)
        self._interface_state(
            extra=[
                {"device": "edge-1", "name": "Ethernet2"},
                {"device": "edge-2", "name": "Ethernet2"},
            ]
        )
        self._state(
            CABLE,
            [
                {
                    "device": "edge-1",
                    "interface": "Ethernet1",
                    "remote_device": "edge-2",
                    "remote_interface": "Ethernet1",
                }
            ],
        )

        self.assertEqual(
            set(plan_stale_row_cleanup(self.sync)[CABLE]["deletable"]), {stale.pk}
        )


class StaleRowRemovalTest(StaleRowCleanupFixture):
    def test_removal_deletes_the_candidate_and_nothing_else(self):
        stale = self._interface(self.edge1, "Ethernet2")
        hand = self._interface(self.edge1, "Ethernet9", created_by_sync=False)
        self._interface_state()
        self._preview()

        result = remove_stale_rows(self.sync)

        self.assertEqual(result["models"][IFACE]["deleted"], 1)
        self.assertFalse(Interface.objects.filter(pk=stale.pk).exists())
        for kept in (hand, self.kept1, self.kept2):
            self.assertTrue(Interface.objects.filter(pk=kept.pk).exists())

    def test_removal_refuses_without_a_preview(self):
        stale = self._interface(self.edge1, "Ethernet2")
        self._interface_state()

        with self.assertRaisesMessage(StaleRowCleanupRefused, REFUSED_NO_PREVIEW):
            remove_stale_rows(self.sync)
        self.assertTrue(Interface.objects.filter(pk=stale.pk).exists())

    def test_removal_refuses_a_model_that_grew_since_the_preview(self):
        first = self._interface(self.edge1, "Ethernet2")
        self._interface_state()
        self._preview()
        second = self._interface(self.edge1, "Ethernet3")

        result = remove_stale_rows(self.sync)

        self.assertEqual(result["models"][IFACE]["refused"], REFUSED_GREW_SINCE_PREVIEW)
        for iface in (first, second):
            self.assertTrue(Interface.objects.filter(pk=iface.pk).exists())

    def test_removal_never_deletes_a_row_the_state_holds(self):
        listed = self._interface(self.edge1, "Ethernet2")
        self._interface_state(extra=[{"device": "edge-1", "name": "Ethernet2"}])
        self._preview()

        result = remove_stale_rows(self.sync)

        self.assertEqual(result["models"][IFACE]["deleted"], 0)
        self.assertTrue(Interface.objects.filter(pk=listed.pk).exists())

    def test_removal_goes_through_the_sync_delete_adapter(self):
        stale = self._interface(self.edge1, "Ethernet2")
        self._interface_state()
        self._preview()
        calls = []

        class RecordingRunner(ForwardSyncRunner):
            def _delete_dcim_interface(self, row):
                calls.append(dict(row))
                return super()._delete_dcim_interface(row)

        result = remove_stale_rows(
            self.sync,
            runner_factory=lambda: RecordingRunner(
                sync=self.sync, ingestion=None, client=None, logger_=Mock()
            ),
        )

        self.assertEqual(calls, [{"device": "edge-1", "name": "Ethernet2"}])
        self.assertEqual(result["models"][IFACE]["deleted"], 1)
        self.assertFalse(Interface.objects.filter(pk=stale.pk).exists())

    def test_a_delete_that_resolves_another_row_is_rolled_back(self):
        stale = self._interface(self.edge1, "Ethernet2")
        self._interface_state()
        self._preview()
        kept_pk = self.kept1.pk

        class WrongRowRunner(ForwardSyncRunner):
            def _delete_dcim_interface(self, row):
                Interface.objects.filter(pk=kept_pk).delete()
                return True

        result = remove_stale_rows(
            self.sync,
            runner_factory=lambda: WrongRowRunner(
                sync=self.sync, ingestion=None, client=None, logger_=Mock()
            ),
        )

        self.assertEqual(result["models"][IFACE]["deleted"], 0)
        self.assertTrue(Interface.objects.filter(pk=stale.pk).exists())
        self.assertTrue(Interface.objects.filter(pk=kept_pk).exists())


class StaleRowCascadeTest(StaleRowCleanupFixture):
    def test_a_delete_that_would_cascade_to_a_hand_added_row_is_held(self):
        # Deleting an interface takes its FHRP assignments with it. One added
        # by hand must hold the interface, not vanish with it.
        from ipam.models import FHRPGroup
        from ipam.models import FHRPGroupAssignment

        stale = self._interface(self.edge1, "Ethernet2")
        group = FHRPGroup.objects.create(protocol="vrrp2", group_id=7)
        assignment = FHRPGroupAssignment.objects.create(
            group=group, interface=stale, priority=100
        )
        self._interface_state()
        self._preview()

        result = remove_stale_rows(self.sync)

        self.assertEqual(result["models"][IFACE]["deleted"], 0)
        self.assertEqual(
            result["models"][IFACE]["held"], {HOLD_CASCADE_REACHES_OTHER_ROWS: 1}
        )
        self.assertTrue(Interface.objects.filter(pk=stale.pk).exists())
        self.assertTrue(FHRPGroupAssignment.objects.filter(pk=assignment.pk).exists())


class StaleRowOperatorSurfaceTest(StaleRowCleanupFixture):
    def test_removal_is_refused_while_a_sync_is_running(self):
        from forward_netbox.utilities.sync_facade import enqueue_button_job
        from forward_netbox.utilities.sync_facade import JobBlockedBySyncRun

        Job.objects.create(
            object_type=ContentType.objects.get_for_model(ForwardSync),
            object_id=self.sync.pk,
            name=f"{self.sync.name} - adhoc",
            status=JobStatusChoices.STATUS_RUNNING,
            job_id=uuid.uuid4(),
        )

        with self.assertRaises(JobBlockedBySyncRun):
            enqueue_button_job(self.sync, "remove_stale_device_rows", None)

    def test_the_bundle_carries_the_cards_figures_under_its_labels(self):
        from forward_netbox.views import _stale_row_cleanup_payload
        from forward_netbox.views import STALE_ROW_CLEANUP_COLUMNS

        self._interface(self.edge1, "Ethernet2")
        self._interface(self.edge1, "Ethernet9", created_by_sync=False)
        self._interface_state()
        preview = self._preview()

        payload = _stale_row_cleanup_payload(self.sync)

        figures = {
            (figure["model"], figure["label"]): figure
            for figure in payload["ui_figures"]
        }
        for label, _key in STALE_ROW_CLEANUP_COLUMNS:
            self.assertIn((IFACE, label), figures)
        self.assertEqual(figures[(IFACE, "Removable")]["value"], 1)
        self.assertEqual(figures[(IFACE, "Not returned by Forward")]["value"], 2)
        self.assertEqual(figures[(IFACE, "Held")]["value"], 1)
        self.assertEqual(figures[(IFACE, f"Held: {HOLD_NO_CREATE_RECORD}")]["value"], 1)
        self.assertEqual(
            figures[(IFACE, "Removable")]["computed_at"],
            preview.data["generated_at"],
        )
        self.assertEqual(payload["preview_job"], preview.pk)


# --- rows the change log no longer remembers (opt-in) ----------------------


class OlderThanChangeLogFixture(StaleRowCleanupFixture):
    """A change log that begins ten days ago, and rows on either side of it."""

    def setUp(self):
        super().setUp()
        now = timezone.now()
        self.log_start = now - timedelta(days=10)
        self.before_log = now - timedelta(days=100)
        self.after_log = now - timedelta(days=5)
        # The sync was set up long before the change log's start.
        self.first_sync = now - timedelta(days=200)
        ForwardSync.objects.filter(pk=self.sync.pk).update(created=self.first_sync)
        self._retain_changes_since(self.log_start)

    def _retain_changes_since(self, when):
        """Make ``when`` the time of the oldest retained change, from anyone."""
        anchor = ObjectChange.objects.create(
            request_id=uuid.uuid4(),
            user_name="someone-else",
            changed_object_type=ContentType.objects.get_for_model(Site),
            changed_object_id=self.edge1.site_id,
            object_repr="anchor",
            action="update",
        )
        ObjectChange.objects.filter(pk=anchor.pk).update(time=when)
        self.anchor = anchor

    def _unrecorded(self, name, created, device=None):
        """An interface with no create record, made at ``created``."""
        iface = self._interface(device or self.edge1, name, created_by_sync=False)
        Interface.objects.filter(pk=iface.pk).update(created=created)
        return iface

    def _plan(self, include=True):
        return plan_stale_row_cleanup(self.sync, include_older_than_change_log=include)[
            IFACE
        ]

    def _preview_with(self, include):
        return Job.objects.create(
            object_type=ContentType.objects.get_for_model(ForwardSync),
            object_id=self.sync.pk,
            name=f"{self.sync.name} - preview stale device rows",
            status=JobStatusChoices.STATUS_COMPLETED,
            job_id=uuid.uuid4(),
            data=preview_stale_row_cleanup(
                self.sync, include_older_than_change_log=include
            ),
        )


class OlderThanChangeLogRulesTest(OlderThanChangeLogFixture):
    def test_a_row_older_than_the_oldest_retained_change_is_reached_only_by_the_option(
        self,
    ):
        old = self._unrecorded("Ethernet9", self.before_log)
        self._interface_state()

        off = self._plan(include=False)
        on = self._plan(include=True)

        self.assertNotIn(old.pk, off["deletable"])
        self.assertEqual(off["held"], {HOLD_OLDER_THAN_CHANGE_LOG: 1})
        self.assertEqual(off["older_than_change_log"], 0)
        self.assertEqual(set(on["deletable"]), {old.pk})
        self.assertEqual(on["older_than_change_log"], 1)

    def test_a_row_older_than_the_first_sync_is_never_reached(self):
        # Made by hand before the plugin was in use: no sync can have written
        # it, whatever the change log no longer says.
        hand_made = self._unrecorded("Ethernet9", self.first_sync - timedelta(days=30))
        self._interface_state()

        plan = self._plan(include=True)

        self.assertNotIn(hand_made.pk, plan["deletable"])
        self.assertEqual(plan["older_than_change_log"], 0)

    def test_a_row_newer_than_the_oldest_retained_change_is_never_reached(self):
        recent = self._unrecorded("Ethernet9", self.after_log)
        self._interface_state()

        plan = self._plan(include=True)

        self.assertNotIn(recent.pk, plan["deletable"])
        self.assertEqual(plan["held"], {HOLD_NO_CREATE_RECORD: 1})
        self.assertEqual(plan["older_than_change_log"], 0)

    def test_a_row_created_at_the_oldest_retained_change_is_not_older_than_it(self):
        edge = self._unrecorded("Ethernet9", self.log_start)
        self._interface_state()

        plan = self._plan(include=True)

        self.assertNotIn(edge.pk, plan["deletable"])
        self.assertEqual(plan["held"], {HOLD_NO_CREATE_RECORD: 1})

    def test_a_row_with_another_users_retained_update_is_never_removed(self):
        touched = self._unrecorded("Ethernet9", self.before_log)
        self._created(touched, request_id=uuid.uuid4(), action="update")
        self._interface_state()

        plan = self._plan(include=True)

        self.assertNotIn(touched.pk, plan["deletable"])
        self.assertEqual(plan["held"], {HOLD_NO_CREATE_RECORD: 1})

    def test_a_row_with_another_users_retained_create_is_never_removed(self):
        theirs = self._unrecorded("Ethernet9", self.before_log)
        self._created(theirs, request_id=uuid.uuid4(), action="create")
        self._interface_state()

        plan = self._plan(include=True)

        self.assertNotIn(theirs.pk, plan["deletable"])
        self.assertEqual(plan["held"], {HOLD_NO_CREATE_RECORD: 1})

    def test_a_retained_change_by_this_sync_alone_does_not_hold_an_older_row(self):
        old = self._unrecorded("Ethernet9", self.before_log)
        self._created(old, action="update")  # this sync's own, retained
        self._interface_state()

        self.assertEqual(set(self._plan(include=True)["deletable"]), {old.pk})

    def test_an_empty_change_log_reaches_nothing(self):
        old = self._unrecorded("Ethernet9", self.before_log)
        self._interface_state()
        ObjectChange.objects.all().delete()

        plan = self._plan(include=True)

        self.assertNotIn(old.pk, plan["deletable"])
        self.assertEqual(plan["held"], {HOLD_NO_CREATE_RECORD: 1})

    def test_a_sync_created_row_is_removable_with_or_without_the_option(self):
        mine = self._interface(self.edge1, "Ethernet9")
        self._interface_state()

        for include in (False, True):
            plan = self._plan(include=include)
            self.assertEqual(set(plan["deletable"]), {mine.pk}, include)
            self.assertEqual(plan["older_than_change_log"], 0, include)

    # -- every other rule still applies ------------------------------------

    def test_the_option_does_not_reach_a_row_a_state_still_holds(self):
        old = self._unrecorded("Ethernet9", self.before_log)
        self._interface_state(extra=[{"device": "edge-1", "name": "Ethernet9"}])

        self.assertNotIn(old.pk, self._plan(include=True)["deletable"])

    def test_the_option_does_not_reach_a_row_of_a_device_outside_the_device_list(self):
        outside = Device.objects.create(
            name="edge-3",
            site=self.edge1.site,
            device_type=self.device_type,
            role=self.edge1.role,
        )
        self._own(outside)
        old = self._unrecorded("Ethernet9", self.before_log, device=outside)
        self._interface_state(extra=[{"device": "edge-3", "name": "Ethernet1"}])

        self.assertNotIn(old.pk, self._plan(include=True)["deletable"])

    def test_the_option_does_not_reach_a_device_type_template_interface(self):
        InterfaceTemplate.objects.create(
            device_type=self.device_type, name="mgmt0", type="1000base-t"
        )
        templated = self._unrecorded("mgmt0", self.before_log)
        self._interface_state()

        plan = self._plan(include=True)

        self.assertNotIn(templated.pk, plan["deletable"])
        self.assertEqual(plan["held"], {HOLD_TEMPLATE_COMPONENT: 1})

    def test_the_option_still_keeps_an_interface_that_carries_a_staying_address(self):
        old = self._unrecorded("Ethernet9", self.before_log)
        address = IPAddress.objects.create(address="10.9.9.1/24", assigned_object=old)
        self._created(address)
        self._interface_state()
        self._state(
            IP,
            [{"device": "edge-1", "interface": "Ethernet9", "address": "10.9.9.1/24"}],
        )

        plans = plan_stale_row_cleanup(self.sync, include_older_than_change_log=True)

        self.assertNotIn(old.pk, plans[IFACE]["deletable"])
        self.assertEqual(plans[IFACE]["held"], {HOLD_ATTACHED_ROWS_STAY: 1})


class OlderThanChangeLogPreviewAndRemovalTest(OlderThanChangeLogFixture):
    def test_the_preview_records_the_choice_the_counts_and_the_change_log_point(self):
        self._unrecorded("Ethernet9", self.before_log)
        self._interface_state()

        off = preview_stale_row_cleanup(self.sync)
        on = preview_stale_row_cleanup(self.sync, include_older_than_change_log=True)

        self.assertEqual(off["options"], {"older_than_change_log": False})
        self.assertEqual(on["options"], {"older_than_change_log": True})
        self.assertEqual(on["oldest_change_logged_at"], self.log_start.isoformat())
        self.assertEqual(on["models"][IFACE]["older_than_change_log"], 1)
        self.assertEqual(on["totals"]["older_than_change_log"], 1)
        self.assertEqual(off["totals"]["older_than_change_log"], 0)
        self.assertEqual(on["totals"]["removable"], off["totals"]["removable"] + 1)

    def test_the_removal_uses_the_previews_choice_and_the_preview_without_it_removes_none(
        self,
    ):
        old = self._unrecorded("Ethernet9", self.before_log)
        self._interface_state()
        self._preview_with(False)

        result = remove_stale_rows(self.sync)

        self.assertEqual(result["options"], {"older_than_change_log": False})
        self.assertEqual(result["models"][IFACE]["deleted"], 0)
        self.assertTrue(Interface.objects.filter(pk=old.pk).exists())

    def test_the_removal_with_a_preview_that_opted_in_removes_the_older_row(self):
        old = self._unrecorded("Ethernet9", self.before_log)
        mine = self._interface(self.edge1, "Ethernet8")
        self._interface_state()
        self._preview_with(True)

        result = remove_stale_rows(self.sync)

        self.assertEqual(result["options"], {"older_than_change_log": True})
        self.assertEqual(result["models"][IFACE]["deleted"], 2)
        self.assertEqual(result["models"][IFACE]["deleted_older_than_change_log"], 1)
        self.assertEqual(result["totals"]["deleted_older_than_change_log"], 1)
        for gone in (old, mine):
            self.assertFalse(Interface.objects.filter(pk=gone.pk).exists())

    def test_the_removal_cannot_widen_the_previews_choice(self):
        old = self._unrecorded("Ethernet9", self.before_log)
        self._interface_state()
        self._preview_with(False)

        with self.assertRaisesMessage(
            StaleRowCleanupRefused, REFUSED_OPTION_WIDER_THAN_PREVIEW
        ):
            remove_stale_rows(self.sync, include_older_than_change_log=True)
        self.assertTrue(Interface.objects.filter(pk=old.pk).exists())

    def test_the_removal_can_narrow_the_previews_choice(self):
        old = self._unrecorded("Ethernet9", self.before_log)
        self._interface_state()
        self._preview_with(True)

        result = remove_stale_rows(self.sync, include_older_than_change_log=False)

        self.assertEqual(result["options"], {"older_than_change_log": False})
        self.assertEqual(result["models"][IFACE]["deleted"], 0)
        self.assertTrue(Interface.objects.filter(pk=old.pk).exists())

    def test_the_removal_is_pinned_to_the_change_log_point_the_preview_recorded(self):
        # The log is pruned between preview and removal: its oldest change is
        # now three days old, so a row from five days ago has become "older
        # than the log". The operator reviewed a preview that did not count it.
        old = self._unrecorded("Ethernet9", self.before_log)
        between = self._unrecorded("Ethernet8", self.after_log)
        self._interface_state()
        self._preview_with(True)
        ObjectChange.objects.filter(pk=self.anchor.pk).update(
            time=timezone.now() - timedelta(days=3)
        )

        result = remove_stale_rows(self.sync)

        self.assertEqual(result["models"][IFACE]["deleted"], 1)
        self.assertFalse(Interface.objects.filter(pk=old.pk).exists())
        self.assertTrue(Interface.objects.filter(pk=between.pk).exists())

    def test_a_preview_that_recorded_no_change_log_point_reaches_nothing(self):
        old = self._unrecorded("Ethernet9", self.before_log)
        self._interface_state()
        ObjectChange.objects.all().delete()
        self._preview_with(True)
        self._retain_changes_since(self.log_start)

        result = remove_stale_rows(self.sync)

        self.assertEqual(result["models"][IFACE]["deleted"], 0)
        self.assertTrue(Interface.objects.filter(pk=old.pk).exists())

    def test_the_removal_refuses_a_model_whose_option_reach_grew_since_the_preview(
        self,
    ):
        first = self._unrecorded("Ethernet9", self.before_log)
        self._interface_state()
        self._preview_with(True)
        second = self._unrecorded("Ethernet8", self.before_log)

        result = remove_stale_rows(self.sync)

        self.assertEqual(result["models"][IFACE]["refused"], REFUSED_GREW_SINCE_PREVIEW)
        for iface in (first, second):
            self.assertTrue(Interface.objects.filter(pk=iface.pk).exists())


class OlderThanChangeLogSameCountsTest(OlderThanChangeLogFixture):
    def test_the_removal_refuses_an_option_row_the_preview_did_not_list(self):
        # Same totals as the preview (one option row, one sync-created row), but
        # the option row is a different one: a row the operator never saw.
        reviewed = self._unrecorded("Ethernet9", self.before_log)
        mine = self._interface(self.edge1, "Ethernet8")
        self._interface_state()
        self._preview_with(True)
        reviewed.delete()
        unseen = self._unrecorded("Ethernet7", self.before_log)

        result = remove_stale_rows(self.sync)

        self.assertEqual(result["models"][IFACE]["refused"], REFUSED_GREW_SINCE_PREVIEW)
        for iface in (mine, unseen):
            self.assertTrue(Interface.objects.filter(pk=iface.pk).exists())

    def test_the_preview_lists_the_primary_keys_of_the_option_rows(self):
        reached = self._unrecorded("Ethernet9", self.before_log)
        self._interface(self.edge1, "Ethernet8")
        self._interface_state()

        data = preview_stale_row_cleanup(self.sync, include_older_than_change_log=True)

        self.assertEqual(data["models"][IFACE]["option_row_pks"], [reached.pk])


class OlderThanChangeLogSurfaceTest(OlderThanChangeLogFixture):
    def test_the_preview_job_stores_the_choice(self):
        from forward_netbox.jobs import _preview_stale_device_rows_work

        self._unrecorded("Ethernet9", self.before_log)
        self._interface_state()
        job = Job.objects.create(
            object_type=ContentType.objects.get_for_model(ForwardSync),
            object_id=self.sync.pk,
            name=f"{self.sync.name} - preview stale device rows",
            status=JobStatusChoices.STATUS_RUNNING,
            job_id=uuid.uuid4(),
        )

        _preview_stale_device_rows_work(job, include_older_than_change_log=True)
        job.refresh_from_db()

        self.assertEqual(job.data["options"], {"older_than_change_log": True})
        self.assertEqual(job.data["totals"]["older_than_change_log"], 1)

    def test_the_job_runners_forward_the_choice_only_when_it_is_given(self):
        from forward_netbox.jobs import PreviewStaleDeviceRowsJob
        from forward_netbox.jobs import RemoveStaleDeviceRowsJob

        job = Job.objects.create(
            object_type=ContentType.objects.get_for_model(ForwardSync),
            object_id=self.sync.pk,
            name="runner",
            status=JobStatusChoices.STATUS_RUNNING,
            job_id=uuid.uuid4(),
        )
        with patch("forward_netbox.jobs._preview_stale_device_rows_work") as work:
            PreviewStaleDeviceRowsJob(job).run()
            work.assert_called_once_with(job)
        with patch("forward_netbox.jobs._preview_stale_device_rows_work") as work:
            PreviewStaleDeviceRowsJob(job).run(include_older_than_change_log=True)
            work.assert_called_once_with(job, include_older_than_change_log=True)
        with patch("forward_netbox.jobs._remove_stale_device_rows_work") as work:
            RemoveStaleDeviceRowsJob(job).run()
            work.assert_called_once_with(job)
        with patch("forward_netbox.jobs._remove_stale_device_rows_work") as work:
            RemoveStaleDeviceRowsJob(job).run(
                preview_job_pk=4, include_older_than_change_log=True
            )
            work.assert_called_once_with(
                job, preview_job_pk=4, include_older_than_change_log=True
            )

    def test_the_page_checkbox_becomes_the_preview_jobs_parameter(self):
        from forward_netbox.views import ForwardSyncPreviewStaleDeviceRowsView
        from forward_netbox.views import ForwardSyncRemoveStaleDeviceRowsView

        factory = RequestFactory()
        preview = ForwardSyncPreviewStaleDeviceRowsView()
        self.assertEqual(
            preview.job_kwargs(
                factory.post("/", {"include_older_than_change_log": "1"})
            ),
            {"include_older_than_change_log": True},
        )
        self.assertIsNone(preview.job_kwargs(factory.post("/", {})))
        # The removal never takes the choice from the page: the preview's wins.
        self.assertEqual(
            ForwardSyncRemoveStaleDeviceRowsView().job_kwargs(
                factory.post(
                    "/", {"preview_job": "7", "include_older_than_change_log": "1"}
                )
            ),
            {"preview_job_pk": 7},
        )

    def test_the_card_offers_the_option_and_the_confirm_says_the_operator_attests(self):
        template = (
            Path(__file__).resolve().parents[1]
            / "templates"
            / "forward_netbox"
            / "forwardsync_scope_reconciliation.html"
        ).read_text()

        self.assertIn('name="include_older_than_change_log"', template)
        self.assertIn("Also remove rows older than the change log", template)
        start = template.index("forwardsync_remove_stale_device_rows")
        confirm = template[start:].split("</form>")[0]
        self.assertIn(
            "you attest that nobody created or edited those rows by hand", confirm
        )

    def test_the_bundle_carries_the_option_the_added_counts_and_the_log_point(self):
        from forward_netbox.views import _stale_row_cleanup_payload
        from forward_netbox.views import STALE_ROW_CLEANUP_COLUMNS

        self._unrecorded("Ethernet9", self.before_log)
        self._interface_state()
        preview = self._preview_with(True)

        payload = _stale_row_cleanup_payload(self.sync)

        self.assertIs(payload["older_than_change_log"], True)
        self.assertEqual(payload["removable_only_by_option"], 1)
        self.assertEqual(payload["oldest_change_logged_at"], self.log_start.isoformat())
        figures = {
            (figure["model"], figure["label"]): figure
            for figure in payload["ui_figures"]
        }
        computed = preview.data["generated_at"]
        for label, value in (
            ("Also remove rows older than the change log (preview)", True),
            ("Oldest change NetBox retained (preview)", self.log_start.isoformat()),
            ("Preview computed ... older than change log (option)", 1),
        ):
            self.assertEqual(figures[(None, label)]["value"], value, label)
            self.assertEqual(figures[(None, label)]["computed_at"], computed, label)
        self.assertIn((IFACE, "Older than change log (option)"), figures)
        self.assertEqual(figures[(IFACE, "Older than change log (option)")]["value"], 1)
        self.assertIn(
            "Older than change log (option)",
            [label for label, _ in STALE_ROW_CLEANUP_COLUMNS],
        )

    def test_the_rest_api_takes_the_choice_as_a_boolean(self):
        from django.contrib.auth import get_user_model
        from rest_framework.test import APIRequestFactory
        from rest_framework.test import force_authenticate

        from forward_netbox.api.views import ForwardSyncViewSet

        admin = get_user_model().objects.create_superuser(
            username="stale_admin", password="TestPassword123!", email="a@example.com"
        )

        def post(action_name, data):
            request = APIRequestFactory().post("/x/", data=data, format="json")
            force_authenticate(request, user=admin)
            return ForwardSyncViewSet.as_view({"post": action_name})(
                request, pk=self.sync.pk
            )

        def enqueued(action_name, suffix, data):
            real_job = Job.objects.create(
                object_type=ContentType.objects.get_for_model(ForwardSync),
                object_id=self.sync.pk,
                name=f"{self.sync.name} - {suffix}",
                status=JobStatusChoices.STATUS_COMPLETED,
                job_id=uuid.uuid4(),
            )
            with patch(
                "forward_netbox.jobs.enqueue_forward_job", return_value=real_job
            ) as enqueue:
                response = post(action_name, data)
            return response, enqueue

        response, enqueue = enqueued(
            "preview_stale_device_rows",
            "preview stale device rows",
            {"include_older_than_change_log": True},
        )
        self.assertEqual(response.status_code, 201)
        self.assertIs(enqueue.call_args.kwargs["include_older_than_change_log"], True)

        response, enqueue = enqueued(
            "preview_stale_device_rows", "preview stale device rows", {}
        )
        self.assertEqual(response.status_code, 201)
        self.assertNotIn("include_older_than_change_log", enqueue.call_args.kwargs)

        response, enqueue = enqueued(
            "remove_stale_device_rows",
            "remove stale device rows",
            {"include_older_than_change_log": True},
        )
        self.assertEqual(response.status_code, 201)
        self.assertIs(enqueue.call_args.kwargs["include_older_than_change_log"], True)

        for action_name, suffix in (
            ("preview_stale_device_rows", "preview stale device rows"),
            ("remove_stale_device_rows", "remove stale device rows"),
        ):
            response, enqueue = enqueued(
                action_name, suffix, {"include_older_than_change_log": "yes"}
            )
            self.assertEqual(response.status_code, 400, action_name)
            enqueue.assert_not_called()


# --- more models --------------------------------------------------------------


class StaleRowCoverageTest(TestCase):
    def test_every_device_shard_model_is_covered_or_named_as_left_out(self):
        covered = set(STALE_ROW_CLEANUP_MODELS)
        left_out = set(STALE_ROW_CLEANUP_LEFT_OUT)

        self.assertFalse(covered & left_out)
        self.assertLessEqual(DEVICE_SHARD_MODELS, covered | left_out)
        for model in left_out:
            with self.subTest(model=model):
                self.assertFalse(sync_may_delete(model))
        for model in covered:
            with self.subTest(model=model):
                self.assertTrue(sync_may_delete(model))

    def test_the_delete_order_puts_what_hangs_off_a_row_before_the_row(self):
        order = {model: index for index, model in enumerate(STALE_ROW_CLEANUP_MODELS)}
        for child, parent in (
            (BGP_PEER_AF, BGP_AF),
            (BGP_PEER_AF, BGP_PEER),
            (OSPF_INTERFACE, OSPF_INSTANCE),
            (BGP_PEER, IP),
            (CABLE, IFACE),
            (IP, IFACE),
            ("dcim.macaddress", IFACE),
            (OSPF_INTERFACE, IFACE),
            (INVENTORY, IFACE),
        ):
            self.assertLess(order[child], order[parent], (child, parent))


class StateRowKeyTest(TestCase):
    def test_a_bgp_address_family_matches_under_every_spelling_the_sync_accepts(self):
        stored = state_row_keys(
            BGP_AF, {"device": "edge-1", "vrf": None, "afi_safi": "ipv4-unicast"}
        )
        for spelling in ("AfiSafiType.IPV4_UNICAST", "ipv4-any", "IPV4_UNICAST"):
            with self.subTest(spelling=spelling):
                row = {"device": "edge-1", "vrf": None, "afi_safi": spelling}
                self.assertTrue(state_row_keys(BGP_AF, row) & stored)

    def test_a_bgp_address_family_in_another_vrf_is_a_different_key(self):
        default = state_row_keys(
            BGP_AF, {"device": "edge-1", "vrf": None, "afi_safi": "ipv4-unicast"}
        )
        blue = state_row_keys(
            BGP_AF, {"device": "edge-1", "vrf": "Blue", "afi_safi": "ipv4-unicast"}
        )
        shouted = state_row_keys(
            BGP_AF, {"device": "edge-1", "vrf": "BLUE", "afi_safi": "ipv4-unicast"}
        )
        self.assertFalse(default & blue)
        self.assertTrue(blue & shouted)

    def test_a_row_the_cleanup_cannot_key_has_no_keys(self):
        self.assertEqual(state_row_keys(BGP_AF, {"device": "edge-1"}), set())
        self.assertEqual(state_row_keys(BGP_PEER_AF, {"device": "edge-1"}), set())
        self.assertEqual(state_row_keys(INVENTORY, {"device": "edge-1"}), set())
        self.assertEqual(state_row_keys(MODULE, {"module_bay": "Slot 1"}), set())
        self.assertEqual(state_row_keys(OSPF_INTERFACE, {"device": "edge-1"}), set())

    def test_a_module_bay_matches_as_reported_and_as_stored(self):
        long_name = "Slot " + "x" * 70
        reported = state_row_keys(MODULE, {"device": "edge-1", "module_bay": long_name})
        stored = state_row_keys(
            MODULE, {"device": "edge-1", "module_bay": long_name.strip()[:64]}
        )
        self.assertTrue(reported & stored)
        self.assertTrue(
            state_row_keys(MODULE, {"device": "edge-1", "module_bay": "Slot 1 "})
            & state_row_keys(MODULE, {"device": "edge-1", "module_bay": "slot 1"})
        )

    def test_an_ospf_interface_matches_under_the_abbreviations_routing_accepts(self):
        stored = state_row_keys(
            OSPF_INTERFACE, {"device": "edge-1", "local_interface": "Ethernet2"}
        )
        for spelling in ("Eth2", "ethernet2", "Ethernet2"):
            with self.subTest(spelling=spelling):
                row = {"device": "edge-1", "local_interface": spelling}
                self.assertTrue(state_row_keys(OSPF_INTERFACE, row) & stored)


class ExtendedModelsFixture(StaleRowCleanupFixture):
    def _enable(self, *models):
        parameters = dict(self.sync.parameters)
        for model in EXTENDED_MODELS:
            parameters[model] = model in models
        self.sync.parameters = parameters
        self.sync.save()

    def _plan(self, model):
        return plan_stale_row_cleanup(self.sync)[model]

    def _outside_device(self):
        """A device the last run did not return (not in the device list)."""
        device = Device.objects.create(
            name="edge-3",
            site=self.edge1.site,
            device_type=self.device_type,
            role=self.edge1.role,
        )
        self._own(device)
        return device

    def _remove(self, **kwargs):
        self._preview()
        return remove_stale_rows(self.sync, **kwargs)

    def _recording_runner(self, handler_name, calls):
        sync = self.sync

        class RecordingRunner(ForwardSyncRunner):
            pass

        def handler(runner, row):
            calls.append(dict(row))
            return getattr(ForwardSyncRunner, handler_name)(runner, row)

        setattr(RecordingRunner, handler_name, handler)
        return lambda: RecordingRunner(
            sync=sync, ingestion=None, client=None, logger_=Mock()
        )


class StaleInventoryItemTest(ExtendedModelsFixture):
    def setUp(self):
        super().setUp()
        self._enable(INVENTORY)
        self.kept = self._item(self.edge1, "Fan 1")

    @staticmethod
    def _row(device, name, serial=""):
        return {
            "device": device,
            "name": name,
            "part_id": f"P-{name}",
            "serial": serial,
        }

    def _item(self, device, name, *, recorded=True, serial=""):
        item = InventoryItem.objects.create(
            device=device, name=name, part_id=f"P-{name}", serial=serial
        )
        if recorded:
            self._created(item)
        return item

    def _inventory_state(self, *extra):
        return self._state(INVENTORY, [self._row("edge-1", "Fan 1"), *extra])

    def test_a_forgotten_sync_created_item_is_removable_and_a_listed_one_is_kept(self):
        stale = self._item(self.edge1, "PSU 1")
        self._inventory_state()

        plan = self._plan(INVENTORY)

        self.assertEqual(set(plan["deletable"]), {stale.pk})
        self.assertNotIn(self.kept.pk, plan["deletable"])
        self.assertEqual(plan["netbox_rows"], 2)

    def test_a_hand_added_item_is_never_a_candidate(self):
        hand = self._item(self.edge1, "PSU 1", recorded=False)
        self._inventory_state()

        plan = self._plan(INVENTORY)

        self.assertNotIn(hand.pk, plan["deletable"])
        self.assertEqual(plan["held"], {HOLD_NO_CREATE_RECORD: 1})

    def test_an_item_whose_serial_changed_is_present_by_device_and_name(self):
        changed = self._item(self.edge1, "PSU 1", serial="NEW")
        self._inventory_state(self._row("edge-1", "PSU 1", serial="OLD"))

        self.assertNotIn(changed.pk, self._plan(INVENTORY)["deletable"])

    def test_an_item_of_a_device_the_last_run_did_not_return_is_never_judged(self):
        outside = self._outside_device()
        stale = self._item(outside, "PSU 1")
        self._inventory_state(self._row("edge-3", "Fan 1"))

        self.assertNotIn(stale.pk, self._plan(INVENTORY)["deletable"])

    def test_a_module_native_state_row_is_not_expected_in_netbox(self):
        # With modules enabled the sync never makes an inventory item of a line
        # card; twenty of them must not read as "the state does not match NetBox".
        stale = self._item(self.edge1, "PSU 1")
        cards = [
            {**self._row("edge-1", f"Card {index}"), "part_type": "LINE CARD"}
            for index in range(21)
        ]
        self._inventory_state(*cards)

        self.assertEqual(self._plan(INVENTORY)["refused"], REFUSED_STATE_MISMATCH)

        self._enable(INVENTORY, MODULE)
        plan = self._plan(INVENTORY)

        self.assertIsNone(plan["refused"])
        self.assertEqual(set(plan["deletable"]), {stale.pk})

    def test_an_interface_carrying_an_item_that_stays_is_held(self):
        stale_iface = self._interface(self.edge1, "Ethernet2")
        InventoryItem.objects.create(
            device=self.edge1, name="Optic", component=stale_iface
        )
        self._interface_state()
        self._inventory_state()

        plans = plan_stale_row_cleanup(self.sync)

        self.assertNotIn(stale_iface.pk, plans[IFACE]["deletable"])
        self.assertEqual(plans[IFACE]["held"], {HOLD_ATTACHED_ROWS_STAY: 1})

    def test_removal_goes_through_the_sync_delete_adapter(self):
        stale = self._item(self.edge1, "PSU 1", serial="S9")
        hand = self._item(self.edge1, "PSU 2", recorded=False)
        self._inventory_state()
        calls = []

        result = self._remove(
            runner_factory=self._recording_runner("_delete_dcim_inventoryitem", calls)
        )

        self.assertEqual(
            calls,
            [
                {
                    "device": "edge-1",
                    "name": "PSU 1",
                    "part_id": "P-PSU 1",
                    "serial": "S9",
                }
            ],
        )
        self.assertEqual(result["models"][INVENTORY]["deleted"], 1)
        self.assertFalse(InventoryItem.objects.filter(pk=stale.pk).exists())
        for kept in (hand, self.kept):
            self.assertTrue(InventoryItem.objects.filter(pk=kept.pk).exists())


class StaleModuleTest(ExtendedModelsFixture):
    def setUp(self):
        super().setUp()
        self._enable(MODULE)
        self.module_type = ModuleType.objects.create(
            manufacturer=self.device_type.manufacturer,
            model="Stale Line Card",
            part_number="LC-1",
        )
        self.kept = self._module(self.edge1, "Slot 1")

    @staticmethod
    def _row(device, bay):
        return {"device": device, "module_bay": bay, "model": "Stale Line Card"}

    def _module(self, device, bay_name, *, recorded=True):
        bay = ModuleBay.objects.create(device=device, name=bay_name)
        module = Module.objects.create(
            device=device, module_bay=bay, module_type=self.module_type, status="active"
        )
        if recorded:
            self._created(module)
        return module

    def _module_state(self, *extra):
        return self._state(MODULE, [self._row("edge-1", "Slot 1"), *extra])

    def test_a_forgotten_sync_created_module_is_removable_and_a_listed_one_is_kept(
        self,
    ):
        stale = self._module(self.edge1, "Slot 2")
        self._module_state()

        plan = self._plan(MODULE)

        self.assertEqual(set(plan["deletable"]), {stale.pk})
        self.assertNotIn(self.kept.pk, plan["deletable"])

    def test_a_hand_added_module_is_never_a_candidate(self):
        hand = self._module(self.edge1, "Slot 2", recorded=False)
        self._module_state()

        plan = self._plan(MODULE)

        self.assertNotIn(hand.pk, plan["deletable"])
        self.assertEqual(plan["held"], {HOLD_NO_CREATE_RECORD: 1})

    def test_a_module_whose_bay_name_was_cleaned_is_present(self):
        cleaned = self._module(self.edge1, "Slot 2")
        self._module_state(self._row("edge-1", "Slot 2 "))

        self.assertNotIn(cleaned.pk, self._plan(MODULE)["deletable"])

    def test_a_module_of_a_device_the_last_run_did_not_return_is_never_judged(self):
        outside = self._outside_device()
        stale = self._module(outside, "Slot 2")
        self._module_state(self._row("edge-3", "Slot 1"))

        self.assertNotIn(stale.pk, self._plan(MODULE)["deletable"])

    def test_a_module_with_a_component_installed_is_held(self):
        # Deleting it would delete that component, which Forward may still list.
        stale = self._module(self.edge1, "Slot 2")
        Interface.objects.create(
            device=self.edge1, name="LC2/1", type="1000base-t", module=stale
        )
        self._module_state()

        plan = self._plan(MODULE)

        self.assertNotIn(stale.pk, plan["deletable"])
        self.assertEqual(plan["held"], {HOLD_MODULE_HAS_COMPONENTS: 1})

    def test_removal_goes_through_the_sync_delete_adapter_and_leaves_the_bay(self):
        stale = self._module(self.edge1, "Slot 2")
        hand = self._module(self.edge1, "Slot 3", recorded=False)
        self._module_state()
        calls = []

        result = self._remove(
            runner_factory=self._recording_runner("_delete_dcim_module", calls)
        )

        self.assertEqual(calls, [{"device": "edge-1", "module_bay": "Slot 2"}])
        self.assertEqual(result["models"][MODULE]["deleted"], 1)
        self.assertFalse(Module.objects.filter(pk=stale.pk).exists())
        self.assertTrue(ModuleBay.objects.filter(name="Slot 2").exists())
        for kept in (hand, self.kept):
            self.assertTrue(Module.objects.filter(pk=kept.pk).exists())


class RoutingFixture(ExtendedModelsFixture):
    def setUp(self):
        if not apps.is_installed("netbox_routing"):
            self.skipTest("netbox-routing optional plugin is not installed")
        super().setUp()
        from ipam.models import ASN
        from ipam.models import RIR

        ASN.objects.create(rir=RIR.objects.create(name="Stale RIR"), asn=64512)
        self.runner = ForwardSyncRunner(
            sync=self.sync, ingestion=None, client=Mock(), logger_=Mock()
        )

    @staticmethod
    def model(name):
        return apps.get_model("netbox_routing", name)

    def _new(self, model_name, apply, *, recorded=True):
        """Run ``apply`` and return the one row of ``model_name`` it created."""
        model = self.model(model_name)
        before = set(model.objects.values_list("pk", flat=True))
        apply()
        created = model.objects.exclude(pk__in=before).get()
        if recorded:
            self._created(created)
        return created

    @staticmethod
    def af_row(device, afi="AfiSafiType.IPV4_UNICAST", vrf=None):
        return {"device": device, "vrf": vrf, "local_asn": 64512, "afi_safi": afi}

    @staticmethod
    def peer_af_row(device, host, afi="AfiSafiType.IPV4_UNICAST", vrf=None):
        return {
            "device": device,
            "vrf": vrf,
            "local_asn": 64512,
            "neighbor_address": host,
            "peer_asn": 64513,
            "enabled": True,
            "status": "active",
            "peer_type": "PeerType.EXTERNAL",
            "afi_safi": afi,
            "has_adj_rib_in": False,
            "has_adj_rib_out": True,
        }

    @staticmethod
    def peer_row(device, host, vrf=None):
        return {
            "device": device,
            "vrf": vrf,
            "local_asn": 64512,
            "neighbor_address": host,
            "peer_asn": 64513,
            "enabled": True,
            "status": "active",
        }

    @staticmethod
    def ospf_instance_row(device, process):
        return {
            "device": device,
            "vrf": None,
            "process_id": process,
            "domain": "fabric",
            "router_id": "192.0.2.254",
        }

    @classmethod
    def ospf_interface_row(cls, device, process, interface):
        return {
            **cls.ospf_instance_row(device, process),
            "area_id": "0",
            "area_type": "OspfAreaType.BACKBONE",
            "local_interface": interface,
            "remote_router_id": "192.0.2.253",
            "remote_interface_ip": "192.0.2.253/31",
            "cost": 1,
            "role": "OspfRole.DESIGNATED_ROUTER",
            "remote_device": "edge-2",
            "remote_interface": "Ethernet1",
        }


class StaleBgpAddressFamilyTest(RoutingFixture):
    def setUp(self):
        super().setUp()
        self._enable(BGP_AF)
        self.kept = self._new(
            "BGPAddressFamily",
            lambda: self.runner._apply_netbox_routing_bgpaddressfamily(
                self.af_row("edge-1")
            ),
        )

    def _address_family(self, device, afi, *, recorded=True):
        return self._new(
            "BGPAddressFamily",
            lambda: self.runner._apply_netbox_routing_bgpaddressfamily(
                self.af_row(device, afi)
            ),
            recorded=recorded,
        )

    def _af_state(self, *extra):
        return self._state(BGP_AF, [self.af_row("edge-1"), *extra])

    def test_a_forgotten_sync_created_family_is_removable_and_a_listed_one_is_kept(
        self,
    ):
        stale = self._address_family("edge-1", "AfiSafiType.IPV6_UNICAST")
        self._af_state()

        plan = self._plan(BGP_AF)

        self.assertEqual(set(plan["deletable"]), {stale.pk})
        self.assertNotIn(self.kept.pk, plan["deletable"])

    def test_a_hand_added_family_is_never_a_candidate(self):
        hand = self._address_family(
            "edge-1", "AfiSafiType.IPV6_UNICAST", recorded=False
        )
        self._af_state()

        plan = self._plan(BGP_AF)

        self.assertNotIn(hand.pk, plan["deletable"])
        self.assertEqual(plan["held"], {HOLD_NO_CREATE_RECORD: 1})

    def test_a_family_the_state_lists_under_an_alias_is_present(self):
        listed = self._address_family("edge-1", "AfiSafiType.IPV6_UNICAST")
        self._af_state(self.af_row("edge-1", "ipv6-any"))

        self.assertNotIn(listed.pk, self._plan(BGP_AF)["deletable"])

    def test_a_family_on_a_device_the_last_run_did_not_return_is_never_judged(self):
        self._outside_device()
        stale = self._address_family("edge-3", "AfiSafiType.IPV6_UNICAST")
        self._af_state(self.af_row("edge-3"))

        self.assertNotIn(stale.pk, self._plan(BGP_AF)["deletable"])

    def test_a_family_a_peer_address_family_state_still_names_is_present(self):
        self._enable(BGP_AF, BGP_PEER_AF)
        wanted = self._address_family("edge-1", "AfiSafiType.IPV6_UNICAST")
        self._af_state()
        self._state(
            BGP_PEER_AF,
            [self.peer_af_row("edge-1", "192.0.2.9", "AfiSafiType.IPV6_UNICAST")],
        )

        self.assertNotIn(wanted.pk, self._plan(BGP_AF)["deletable"])

    def test_a_family_with_a_peer_address_family_that_stays_is_held(self):
        self._enable(BGP_AF, BGP_PEER_AF)
        row = self.peer_af_row("edge-1", "192.0.2.9", "AfiSafiType.IPV6_UNICAST")
        # The peer address family writes its family too; only the family has a
        # create record, so the peer address family itself is held and stays.
        self.runner._apply_netbox_routing_bgppeeraddressfamily(row)
        stale = self.model("BGPAddressFamily").objects.get(
            address_family="ipv6-unicast"
        )
        self._created(stale)
        self._af_state()
        self._state(BGP_PEER_AF, [self.peer_af_row("edge-1", "192.0.2.1")])

        plans = plan_stale_row_cleanup(self.sync)

        self.assertNotIn(stale.pk, plans[BGP_AF]["deletable"])
        self.assertEqual(plans[BGP_AF]["held"], {HOLD_ATTACHED_ROWS_STAY: 1})
        self.assertEqual(plans[BGP_PEER_AF]["held"], {HOLD_NO_CREATE_RECORD: 1})

    def test_removal_goes_through_the_sync_delete_adapter(self):
        stale = self._address_family("edge-1", "AfiSafiType.IPV6_UNICAST")
        self._af_state()
        calls = []

        result = self._remove(
            runner_factory=self._recording_runner(
                "_delete_netbox_routing_bgpaddressfamily", calls
            )
        )

        self.assertEqual(
            calls,
            [
                {
                    "device": "edge-1",
                    "local_asn": 64512,
                    "vrf": None,
                    "afi_safi": "ipv6-unicast",
                }
            ],
        )
        self.assertEqual(result["models"][BGP_AF]["deleted"], 1)
        self.assertFalse(
            self.model("BGPAddressFamily").objects.filter(pk=stale.pk).exists()
        )
        self.assertTrue(
            self.model("BGPAddressFamily").objects.filter(pk=self.kept.pk).exists()
        )


class StaleBgpPeerAddressFamilyTest(RoutingFixture):
    def setUp(self):
        super().setUp()
        self._enable(BGP_PEER_AF)
        self.kept = self._peer_af("edge-1", "192.0.2.1")

    def _peer_af(self, device, host, afi="AfiSafiType.IPV4_UNICAST", *, recorded=True):
        return self._new(
            "BGPPeerAddressFamily",
            lambda: self.runner._apply_netbox_routing_bgppeeraddressfamily(
                self.peer_af_row(device, host, afi)
            ),
            recorded=recorded,
        )

    def _peer_af_state(self, *extra):
        return self._state(
            BGP_PEER_AF, [self.peer_af_row("edge-1", "192.0.2.1"), *extra]
        )

    def test_a_forgotten_sync_created_peer_family_is_removable_and_a_listed_one_is_kept(
        self,
    ):
        stale = self._peer_af("edge-1", "192.0.2.1", "AfiSafiType.IPV6_UNICAST")
        self._peer_af_state()

        plan = self._plan(BGP_PEER_AF)

        self.assertEqual(set(plan["deletable"]), {stale.pk})
        self.assertNotIn(self.kept.pk, plan["deletable"])

    def test_a_hand_added_peer_family_is_never_a_candidate(self):
        hand = self._peer_af(
            "edge-1", "192.0.2.1", "AfiSafiType.IPV6_UNICAST", recorded=False
        )
        self._peer_af_state()

        plan = self._plan(BGP_PEER_AF)

        self.assertNotIn(hand.pk, plan["deletable"])
        self.assertEqual(plan["held"], {HOLD_NO_CREATE_RECORD: 1})

    def test_a_peer_family_the_state_lists_under_an_alias_is_present(self):
        listed = self._peer_af("edge-1", "192.0.2.1", "AfiSafiType.IPV6_UNICAST")
        self._peer_af_state(self.peer_af_row("edge-1", "192.0.2.1", "ipv6-any"))

        self.assertNotIn(listed.pk, self._plan(BGP_PEER_AF)["deletable"])

    def test_a_peer_family_of_another_neighbour_is_not_kept_by_this_ones(self):
        other = self._peer_af("edge-1", "192.0.2.77")
        self._peer_af_state()

        self.assertIn(other.pk, self._plan(BGP_PEER_AF)["deletable"])

    def test_a_peer_family_on_a_device_the_last_run_did_not_return_is_never_judged(
        self,
    ):
        self._outside_device()
        stale = self._peer_af("edge-3", "192.0.2.1")
        self._peer_af_state(self.peer_af_row("edge-3", "192.0.2.50"))

        self.assertNotIn(stale.pk, self._plan(BGP_PEER_AF)["deletable"])

    def test_removal_goes_through_the_sync_delete_adapter(self):
        stale = self._peer_af("edge-1", "192.0.2.1", "AfiSafiType.IPV6_UNICAST")
        self._peer_af_state()
        calls = []

        result = self._remove(
            runner_factory=self._recording_runner(
                "_delete_netbox_routing_bgppeeraddressfamily", calls
            )
        )

        self.assertEqual(
            calls,
            [
                {
                    "device": "edge-1",
                    "local_asn": 64512,
                    "vrf": None,
                    "neighbor_address": "192.0.2.1",
                    "afi_safi": "ipv6-unicast",
                }
            ],
        )
        self.assertEqual(result["models"][BGP_PEER_AF]["deleted"], 1)
        model = self.model("BGPPeerAddressFamily")
        self.assertFalse(model.objects.filter(pk=stale.pk).exists())
        self.assertTrue(model.objects.filter(pk=self.kept.pk).exists())


class BgpPeerNamedByItsChildrenTest(RoutingFixture):
    def setUp(self):
        super().setUp()
        self.kept = self._new(
            "BGPPeer",
            lambda: self.runner._apply_netbox_routing_bgppeer(
                self.peer_row("edge-1", "192.0.2.1")
            ),
        )
        self.other = self._new(
            "BGPPeer",
            lambda: self.runner._apply_netbox_routing_bgppeer(
                self.peer_row("edge-1", "192.0.2.9")
            ),
        )
        self._state(BGP_PEER, [self.peer_row("edge-1", "192.0.2.1")])

    def test_a_peer_with_no_state_of_its_own_and_no_child_naming_it_is_removable(self):
        self._enable(BGP_PEER)

        self.assertEqual(set(self._plan(BGP_PEER)["deletable"]), {self.other.pk})

    def test_a_peer_a_peer_address_family_state_still_names_is_kept(self):
        self._enable(BGP_PEER, BGP_PEER_AF)
        self._state(
            BGP_PEER_AF,
            [
                self.peer_af_row("edge-1", "192.0.2.1"),
                self.peer_af_row("edge-1", "192.0.2.9"),
            ],
        )

        self.assertEqual(self._plan(BGP_PEER)["deletable"], {})

    def test_a_child_model_with_no_state_refuses_the_peer_model(self):
        self._enable(BGP_PEER, BGP_PEER_AF)

        plan = self._plan(BGP_PEER)

        self.assertEqual(plan["refused"], REFUSED_CHILD_STATE_UNAVAILABLE)
        self.assertEqual(plan["deletable"], {})

    def test_a_peering_session_state_still_naming_a_peer_keeps_it(self):
        if not apps.is_installed("netbox_peering_manager"):
            self.skipTest("netbox-peering-manager optional plugin is not installed")
        self._enable(BGP_PEER, PEERING_SESSION)
        self._state(PEERING_SESSION, [self.peer_row("edge-1", "192.0.2.9")])

        self.assertEqual(self._plan(BGP_PEER)["deletable"], {})


class StaleOspfInstanceTest(RoutingFixture):
    def setUp(self):
        super().setUp()
        self._enable(OSPF_INSTANCE)
        self.kept = self._instance("edge-1", "UNDERLAY")

    def _instance(self, device, process, *, recorded=True):
        return self._new(
            "OSPFInstance",
            lambda: self.runner._apply_netbox_routing_ospfinstance(
                self.ospf_instance_row(device, process)
            ),
            recorded=recorded,
        )

    def _instance_state(self, *extra):
        return self._state(
            OSPF_INSTANCE, [self.ospf_instance_row("edge-1", "UNDERLAY"), *extra]
        )

    def test_a_forgotten_sync_created_instance_is_removable_and_a_listed_one_is_kept(
        self,
    ):
        # Non-numeric process names are stored as a hash of the row; the key
        # must be the number the apply stored, not the text Forward reported.
        stale = self._instance("edge-1", "RETIRED")
        self._instance_state()

        plan = self._plan(OSPF_INSTANCE)

        self.assertEqual(set(plan["deletable"]), {stale.pk})
        self.assertNotIn(self.kept.pk, plan["deletable"])

    def test_numeric_process_ids_match_too(self):
        listed = self._instance("edge-1", "10")
        stale = self._instance("edge-1", "11")
        self._instance_state(self.ospf_instance_row("edge-1", "10"))

        plan = self._plan(OSPF_INSTANCE)

        self.assertEqual(set(plan["deletable"]), {stale.pk})
        self.assertNotIn(listed.pk, plan["deletable"])

    def test_a_hand_added_instance_is_never_a_candidate(self):
        hand = self._instance("edge-1", "RETIRED", recorded=False)
        self._instance_state()

        plan = self._plan(OSPF_INSTANCE)

        self.assertNotIn(hand.pk, plan["deletable"])
        self.assertEqual(plan["held"], {HOLD_NO_CREATE_RECORD: 1})

    def test_an_instance_on_a_device_the_last_run_did_not_return_is_never_judged(self):
        self._outside_device()
        stale = self._instance("edge-3", "RETIRED")
        self._instance_state(self.ospf_instance_row("edge-3", "UNDERLAY"))

        self.assertNotIn(stale.pk, self._plan(OSPF_INSTANCE)["deletable"])

    def test_an_instance_an_ospf_interface_state_still_names_is_present(self):
        self._enable(OSPF_INSTANCE, OSPF_INTERFACE)
        wanted = self._instance("edge-1", "RETIRED")
        self._instance_state()
        self._state(
            OSPF_INTERFACE,
            [self.ospf_interface_row("edge-1", "RETIRED", "Ethernet1")],
        )

        self.assertNotIn(wanted.pk, self._plan(OSPF_INSTANCE)["deletable"])

    def test_an_instance_with_an_ospf_interface_that_stays_is_held(self):
        self._enable(OSPF_INSTANCE, OSPF_INTERFACE)
        # The interface row writes its instance. The OSPF interface it also
        # wrote is listed in the state, so it stays, and the instance with it.
        self.runner._apply_netbox_routing_ospfinterface(
            self.ospf_interface_row("edge-1", "RETIRED", "Ethernet1")
        )
        stale = self.model("OSPFInstance").objects.get(
            device=self.edge1, name__contains="RETIRED"
        )
        self._created(stale)
        self._instance_state()
        self._state(
            OSPF_INTERFACE, [self.ospf_interface_row("edge-1", "UNDERLAY", "Ethernet1")]
        )

        plans = plan_stale_row_cleanup(self.sync)

        self.assertNotIn(stale.pk, plans[OSPF_INSTANCE]["deletable"])
        self.assertEqual(plans[OSPF_INSTANCE]["held"], {HOLD_ATTACHED_ROWS_STAY: 1})

    def test_removal_goes_through_the_sync_delete_adapter(self):
        stale = self._instance("edge-1", "RETIRED")
        self._instance_state()
        calls = []

        result = self._remove(
            runner_factory=self._recording_runner(
                "_delete_netbox_routing_ospfinstance", calls
            )
        )

        self.assertEqual(len(calls), 1)
        self.assertEqual(calls[0]["device"], "edge-1")
        self.assertEqual(calls[0]["process_id"], str(stale.process_id))
        self.assertEqual(result["models"][OSPF_INSTANCE]["deleted"], 1)
        self.assertFalse(
            self.model("OSPFInstance").objects.filter(pk=stale.pk).exists()
        )
        self.assertTrue(
            self.model("OSPFInstance").objects.filter(pk=self.kept.pk).exists()
        )


class StaleOspfInterfaceTest(RoutingFixture):
    def setUp(self):
        super().setUp()
        self._enable(OSPF_INTERFACE)
        self.kept = self._ospf_interface("edge-1", "Ethernet1", recorded=True)

    def _ospf_interface(self, device, interface, *, recorded=True):
        if not Interface.objects.filter(device__name=device, name=interface).exists():
            Interface.objects.create(
                device=Device.objects.get(name=device),
                name=interface,
                type="1000base-t",
            )
        return self._new(
            "OSPFInterface",
            lambda: self.runner._apply_netbox_routing_ospfinterface(
                self.ospf_interface_row(device, "UNDERLAY", interface)
            ),
            recorded=recorded,
        )

    def _ospf_state(self, *extra):
        return self._state(
            OSPF_INTERFACE,
            [self.ospf_interface_row("edge-1", "UNDERLAY", "Ethernet1"), *extra],
        )

    def test_a_forgotten_sync_created_ospf_interface_is_removable_and_a_listed_one_is_kept(
        self,
    ):
        stale = self._ospf_interface("edge-1", "Ethernet2")
        self._ospf_state()

        plan = self._plan(OSPF_INTERFACE)

        self.assertEqual(set(plan["deletable"]), {stale.pk})
        self.assertNotIn(self.kept.pk, plan["deletable"])

    def test_a_hand_added_ospf_interface_is_never_a_candidate(self):
        hand = self._ospf_interface("edge-1", "Ethernet2", recorded=False)
        self._ospf_state()

        plan = self._plan(OSPF_INTERFACE)

        self.assertNotIn(hand.pk, plan["deletable"])
        self.assertEqual(plan["held"], {HOLD_NO_CREATE_RECORD: 1})

    def test_an_abbreviated_local_interface_in_the_state_keeps_the_full_name(self):
        listed = self._ospf_interface("edge-1", "Ethernet3")
        self._ospf_state(self.ospf_interface_row("edge-1", "UNDERLAY", "Eth3"))

        self.assertNotIn(listed.pk, self._plan(OSPF_INTERFACE)["deletable"])

    def test_an_ospf_interface_on_a_device_the_last_run_did_not_return_is_never_judged(
        self,
    ):
        outside = self._outside_device()
        Interface.objects.create(device=outside, name="Ethernet2", type="1000base-t")
        stale = self._ospf_interface("edge-3", "Ethernet2")
        self._ospf_state(self.ospf_interface_row("edge-3", "UNDERLAY", "Ethernet1"))

        self.assertNotIn(stale.pk, self._plan(OSPF_INTERFACE)["deletable"])

    def test_an_interface_carrying_an_ospf_interface_that_stays_is_held(self):
        hand = self._ospf_interface("edge-1", "Ethernet2", recorded=False)
        self._ospf_state()
        self._interface_state()
        stale_iface = hand.interface
        self._created(stale_iface)

        plans = plan_stale_row_cleanup(self.sync)

        self.assertNotIn(stale_iface.pk, plans[IFACE]["deletable"])
        self.assertEqual(plans[IFACE]["held"], {HOLD_ATTACHED_ROWS_STAY: 1})

    def test_removal_takes_the_ospf_interface_before_the_interface_it_sits_on(self):
        stale = self._ospf_interface("edge-1", "Ethernet2")
        self._created(stale.interface)
        self._ospf_state()
        self._interface_state()
        calls = []

        result = self._remove(
            runner_factory=self._recording_runner(
                "_delete_netbox_routing_ospfinterface", calls
            )
        )

        self.assertEqual(calls, [{"device": "edge-1", "local_interface": "Ethernet2"}])
        self.assertEqual(result["models"][OSPF_INTERFACE]["deleted"], 1)
        self.assertEqual(result["models"][IFACE]["deleted"], 1)
        self.assertFalse(
            Interface.objects.filter(device=self.edge1, name="Ethernet2").exists()
        )
        self.assertTrue(
            self.model("OSPFInterface").objects.filter(pk=self.kept.pk).exists()
        )
