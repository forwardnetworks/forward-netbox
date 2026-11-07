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
from unittest.mock import Mock

from core.choices import JobStatusChoices
from core.models import Job
from core.models import ObjectChange
from dcim.models import Cable
from dcim.models import Device
from dcim.models import DeviceRole
from dcim.models import DeviceType
from dcim.models import Interface
from dcim.models import InterfaceTemplate
from dcim.models import Manufacturer
from dcim.models import Site
from django.contrib.contenttypes.models import ContentType
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
COALESCE = {
    "dcim.device": [["name"]],
    "dcim.macaddress": [["mac_address"]],
    IFACE: [["device", "name"]],
    IP: [["address", "vrf"], ["address"]],
    CABLE: [["device", "interface", "remote_device", "remote_interface"]],
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
