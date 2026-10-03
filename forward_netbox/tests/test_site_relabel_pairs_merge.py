"""The GUI repair for existing site-relabel duplicate pairs.

The apply-path fix (`apply_engine_bulk.py`'s `_relabel_move_candidate`) stops
a sync from creating a second device on a site relabel going forward, but the
duplicates it created before already exist. This is the one-time repair: for
each pair it can prove, keep the older device at the site Forward's device map
places the name at, and delete the newer copy.

The proof is the sync's own device map, as stored by the latest scope
reconciliation - not the device identity, which on a real estate bound the
stale copy in 218 of 221 pairs. Every test pins the negative space as hard as
the happy path: a pair this cannot prove is left completely alone.
"""

import uuid
from datetime import timedelta
from unittest.mock import Mock
from unittest.mock import patch

from core.choices import JobStatusChoices
from core.models import Job
from dcim.models import Device
from dcim.models import DeviceRole
from dcim.models import DeviceType
from dcim.models import Interface
from dcim.models import Manufacturer
from dcim.models import Site
from django.contrib.contenttypes.models import ContentType
from django.test import TestCase
from django.utils import timezone

from forward_netbox.models import ForwardDeviceIdentity
from forward_netbox.models import ForwardSource
from forward_netbox.models import ForwardSync
from forward_netbox.utilities.scope_reconciliation import merge_site_relabel_duplicates
from forward_netbox.utilities.scope_reconciliation import (
    SITE_RELABEL_HOLD_IDENTITY_OTHER,
)
from forward_netbox.utilities.scope_reconciliation import (
    SITE_RELABEL_HOLD_MANUAL_OBJECTS,
)
from forward_netbox.utilities.scope_reconciliation import (
    SITE_RELABEL_HOLD_MORE_THAN_TWO,
)
from forward_netbox.utilities.scope_reconciliation import SITE_RELABEL_HOLD_NO_REPORT
from forward_netbox.utilities.scope_reconciliation import SITE_RELABEL_HOLD_REPORT_STALE
from forward_netbox.utilities.scope_reconciliation import (
    SITE_RELABEL_HOLD_SITE_AMBIGUOUS,
)
from forward_netbox.utilities.scope_reconciliation import SITE_RELABEL_HOLD_SITE_NEITHER
from forward_netbox.utilities.scope_reconciliation import SITE_RELABEL_HOLD_SITE_UNKNOWN
from forward_netbox.utilities.scope_reconciliation import site_relabel_held_by_reason
from forward_netbox.utilities.scope_reconciliation import site_relabel_pairs


class SiteRelabelPairsTest(TestCase):
    def setUp(self):
        self.old_site = Site.objects.create(name="old-site", slug="old-site")
        self.new_site = Site.objects.create(name="new-site", slug="new-site")
        manufacturer = Manufacturer.objects.create(name="Cisco", slug="cisco")
        self.device_type = DeviceType.objects.create(
            manufacturer=manufacturer, model="model-a", slug="model-a"
        )
        self.role = DeviceRole.objects.create(
            name="role-a", slug="role-a", color="9e9e9e"
        )
        source = ForwardSource.objects.create(
            name="relabel-repair-src",
            type="saas",
            url="https://fwd.app",
            status="ready",
            parameters={"network_id": "net-1"},
        )
        self.sync = ForwardSync.objects.create(
            name="relabel-repair-sync",
            source=source,
            parameters={"snapshot_id": "latestProcessed"},
        )
        other_source = ForwardSource.objects.create(
            name="relabel-repair-other-src",
            type="saas",
            url="https://fwd.app",
            status="ready",
            parameters={"network_id": "net-2"},
        )
        self.other_sync = ForwardSync.objects.create(
            name="relabel-repair-other-sync",
            source=other_source,
            parameters={"snapshot_id": "latestProcessed"},
        )

    def _device(self, name, site):
        return Device.objects.create(
            name=name,
            site=site,
            role=self.role,
            device_type=self.device_type,
            status="active",
        )

    def _report(self, forward_sites=None, *, ambiguous=(), completed=None):
        """Store a scope report as the latest reconciliation would."""
        return Job.objects.create(
            object_type=ContentType.objects.get_for_model(ForwardSync),
            object_id=self.sync.pk,
            name=f"{self.sync.name} - scope reconciliation",
            status=JobStatusChoices.STATUS_COMPLETED,
            completed=completed or timezone.now(),
            job_id=uuid.uuid4(),
            data={
                "forward_site_id_by_device_pk": {
                    str(pk): site_pk for pk, site_pk in (forward_sites or {}).items()
                },
                "forward_site_ambiguous_device_ids": list(ambiguous),
                "forward_site_source": {"available": True, "error": "", "rows": 1},
            },
        )

    def _pair(self, name="core-sw-01", *, forward_site=None, identity=None):
        """A relabel pair: the older copy at the old site, the newer at the new.

        ``forward_site`` defaults to the new site, which is the relabel shape.
        ``identity`` is "older", "newer" or None.
        """
        older = self._device(name, self.old_site)
        newer = self._device(name, self.new_site)
        if identity is not None:
            ForwardDeviceIdentity.objects.create(
                sync=self.sync,
                source_device_key=name,
                device=older if identity == "older" else newer,
            )
        site = forward_site or self.new_site
        return older, newer, {older.pk: site.pk, newer.pk: site.pk}

    # -- detection: the shapes a real estate has ------------------------

    def test_identity_bound_to_the_older_copy_is_mergeable(self):
        # 218 of the customer's 221 pairs: the original device kept this
        # sync's identity; the relabel created an unbound copy at the new site.
        older, newer, sites = self._pair(identity="older")
        self._report(sites)

        report = site_relabel_pairs(self.sync)

        self.assertEqual(report["held"], [])
        self.assertEqual(len(report["pairs"]), 1)
        pair = report["pairs"][0]
        self.assertEqual((pair["older_pk"], pair["newer_pk"]), (older.pk, newer.pk))
        self.assertEqual(pair["forward_site_id"], self.new_site.pk)
        self.assertEqual(pair["action"], "move_older")
        self.assertEqual(pair["identity_bound"], "older")

    def test_no_identity_at_all_is_mergeable_when_the_device_map_proves_it(self):
        # The other 3: neither copy carries an identity from this sync.
        self._pair(identity=None)
        report = site_relabel_pairs(self.sync)
        self.assertEqual(report["pairs"], [])  # no report yet

        older, newer, sites = self._pair(name="edge-01", identity=None)
        self._report(
            {
                **{pk: self.new_site.pk for pk in sites},
                **{
                    d.pk: self.new_site.pk
                    for d in Device.objects.filter(name="core-sw-01")
                },
            }
        )

        report = site_relabel_pairs(self.sync)

        self.assertEqual(len(report["pairs"]), 2)
        self.assertTrue(all(p["identity_bound"] == "none" for p in report["pairs"]))

    def test_identity_bound_to_the_newer_copy_is_mergeable(self):
        _older, _newer, sites = self._pair(identity="newer")
        self._report(sites)

        report = site_relabel_pairs(self.sync)

        self.assertEqual(report["pairs"][0]["identity_bound"], "newer")

    def test_the_older_copy_already_at_forwards_site_only_deletes_the_newer(self):
        older, newer, sites = self._pair(forward_site=self.old_site, identity="older")
        self._report(sites)

        report = site_relabel_pairs(self.sync)

        self.assertEqual(report["pairs"][0]["action"], "delete_newer")

    def test_a_same_site_duplicate_is_not_a_pair_at_all(self):
        # Allowed only with different tenants; nothing changed sites.
        from tenancy.models import Tenant

        tenant = Tenant.objects.create(name="t", slug="t")
        first = self._device("core-sw-01", self.old_site)
        second = Device.objects.create(
            name="core-sw-01",
            site=self.old_site,
            role=self.role,
            device_type=self.device_type,
            status="active",
            tenant=tenant,
        )
        self._report({first.pk: self.old_site.pk, second.pk: self.old_site.pk})

        report = site_relabel_pairs(self.sync)

        self.assertEqual(report["pairs"], [])
        self.assertEqual(report["held"], [])

    # -- detection: negative space, never guess --------------------------

    def test_without_any_scope_report_every_pair_is_held(self):
        older, newer, _sites = self._pair(identity="older")

        report = site_relabel_pairs(self.sync)

        self.assertEqual(report["pairs"], [])
        held = report["held"][0]
        self.assertEqual(held["reason"], SITE_RELABEL_HOLD_NO_REPORT)
        self.assertEqual(held["members"], [older.pk, newer.pk])

    def test_a_report_older_than_a_device_is_stale(self):
        _older, _newer, sites = self._pair(identity="older")
        self._report(sites, completed=timezone.now() - timedelta(days=1))

        report = site_relabel_pairs(self.sync)

        self.assertEqual(report["held"][0]["reason"], SITE_RELABEL_HOLD_REPORT_STALE)

    def test_a_name_the_device_map_did_not_place_is_held(self):
        self._pair(identity="older")
        self._report({})

        report = site_relabel_pairs(self.sync)

        self.assertEqual(report["held"][0]["reason"], SITE_RELABEL_HOLD_SITE_UNKNOWN)

    def test_a_name_placed_at_two_sites_is_held(self):
        older, newer, _sites = self._pair(identity="older")
        self._report({}, ambiguous=[older.pk, newer.pk])

        report = site_relabel_pairs(self.sync)

        self.assertEqual(report["held"][0]["reason"], SITE_RELABEL_HOLD_SITE_AMBIGUOUS)

    def test_forward_at_a_third_site_is_held(self):
        third = Site.objects.create(name="s3", slug="s3")
        _older, _newer, sites = self._pair(forward_site=third, identity="older")
        self._report(sites)

        report = site_relabel_pairs(self.sync)

        self.assertEqual(report["held"][0]["reason"], SITE_RELABEL_HOLD_SITE_NEITHER)

    def test_identity_bound_to_a_third_device_is_held(self):
        third_site = Site.objects.create(name="s3", slug="s3")
        orphan = self._device("some-other-device", third_site)
        _older, _newer, sites = self._pair()
        ForwardDeviceIdentity.objects.create(
            sync=self.sync, source_device_key="core-sw-01", device=orphan
        )
        self._report(sites)

        report = site_relabel_pairs(self.sync)

        self.assertEqual(report["held"][0]["reason"], SITE_RELABEL_HOLD_IDENTITY_OTHER)

    def test_identity_from_a_different_sync_is_ignored(self):
        older, _newer, sites = self._pair()
        ForwardDeviceIdentity.objects.create(
            sync=self.other_sync, source_device_key="core-sw-01", device=older
        )
        self._report(sites)

        report = site_relabel_pairs(self.sync)

        self.assertEqual(report["pairs"][0]["identity_bound"], "none")

    def test_three_devices_sharing_a_name_are_held_with_every_member(self):
        third_site = Site.objects.create(name="s3", slug="s3")
        a = self._device("core-sw-01", self.old_site)
        b = self._device("core-sw-01", self.new_site)
        c = self._device("core-sw-01", third_site)
        self._report({a.pk: self.new_site.pk, b.pk: self.new_site.pk})

        report = site_relabel_pairs(self.sync)

        self.assertEqual(report["pairs"], [])
        held = report["held"][0]
        self.assertEqual(held["reason"], SITE_RELABEL_HOLD_MORE_THAN_TWO)
        self.assertEqual(held["members"], [a.pk, b.pk, c.pk])

    def test_a_real_protecting_reference_on_the_newer_device_holds_the_pair(self):
        _older, _newer, sites = self._pair(identity="older")
        self._report(sites)

        with patch(
            "forward_netbox.utilities.bulk_merge.describe_protecting_references",
            return_value=[("ipam.Service", 1)],
        ):
            report = site_relabel_pairs(self.sync)

        self.assertEqual(report["pairs"], [])
        self.assertEqual(report["held"][0]["reason"], SITE_RELABEL_HOLD_MANUAL_OBJECTS)
        self.assertEqual(report["held"][0]["blocking_models"], ["ipam.Service"])

    def test_held_reasons_come_with_a_remedy(self):
        self._pair(identity="older")

        by_reason = site_relabel_held_by_reason(site_relabel_pairs(self.sync))

        self.assertEqual(by_reason[0]["reason"], SITE_RELABEL_HOLD_NO_REPORT)
        self.assertEqual(by_reason[0]["count"], 1)
        self.assertIn("refresh Scope Reconciliation", by_reason[0]["remedy"])

    # -- the merge action itself -----------------------------------------

    def test_the_older_device_survives_at_forwards_site_with_its_primary_ip(self):
        from ipam.models import IPAddress

        older, newer, sites = self._pair(identity="older")
        interface = Interface.objects.create(
            device=older, name="mgmt0", type="1000base-t"
        )
        addr = IPAddress.objects.create(address="10.0.0.1/32")
        addr.assigned_object = interface
        addr.save()
        older.primary_ip4 = addr
        older.save()
        self._report(sites)

        result = merge_site_relabel_duplicates(self.sync)

        self.assertEqual(result["merged_count"], 1)
        self.assertEqual(result["failed_count"], 0)
        self.assertFalse(Device.objects.filter(pk=newer.pk).exists())
        older.refresh_from_db()
        self.assertEqual(older.site_id, self.new_site.pk)
        self.assertEqual(older.primary_ip4_id, addr.pk)
        self.assertTrue(
            ForwardDeviceIdentity.objects.filter(
                sync=self.sync, source_device_key="core-sw-01", device=older
            ).exists()
        )

    def test_an_identity_on_the_newer_copy_moves_to_the_older_one(self):
        older, newer, sites = self._pair(identity="newer")
        self._report(sites)

        merge_site_relabel_duplicates(self.sync)

        self.assertEqual(
            list(
                ForwardDeviceIdentity.objects.filter(sync=self.sync).values_list(
                    "source_device_key", "device_id"
                )
            ),
            [("core-sw-01", older.pk)],
        )

    def test_forwards_spelling_of_the_identity_key_is_kept(self):
        older = self._device("CORE-SW-01", self.old_site)
        newer = self._device("core-sw-01", self.new_site)
        ForwardDeviceIdentity.objects.create(
            sync=self.sync, source_device_key="Core-Sw-01", device=older
        )
        self._report({older.pk: self.new_site.pk, newer.pk: self.new_site.pk})

        merge_site_relabel_duplicates(self.sync)

        self.assertEqual(
            list(
                ForwardDeviceIdentity.objects.filter(sync=self.sync).values_list(
                    "source_device_key", "device_id"
                )
            ),
            [("Core-Sw-01", older.pk)],
        )

    def test_when_the_older_copy_is_current_the_merge_only_deletes(self):
        older, newer, sites = self._pair(forward_site=self.old_site, identity="older")
        self._report(sites)

        result = merge_site_relabel_duplicates(self.sync)

        self.assertEqual(result["merged_pairs"][0]["action"], "delete_newer")
        self.assertFalse(Device.objects.filter(pk=newer.pk).exists())
        older.refresh_from_db()
        self.assertEqual(older.site_id, self.old_site.pk)

    def test_a_held_pair_is_untouched_by_the_merge(self):
        older, newer, _sites = self._pair(identity="older")

        result = merge_site_relabel_duplicates(self.sync)

        self.assertEqual(result["merged_count"], 0)
        self.assertEqual(result["held_count"], 1)
        self.assertTrue(Device.objects.filter(pk=older.pk, site=self.old_site).exists())
        self.assertTrue(Device.objects.filter(pk=newer.pk, site=self.new_site).exists())

    def test_an_unrelated_device_is_never_touched(self):
        _older, _newer, sites = self._pair(identity="older")
        untouched = self._device("untouched-01", self.old_site)
        self._report(sites)

        merge_site_relabel_duplicates(self.sync)

        untouched.refresh_from_db()
        self.assertEqual(untouched.site_id, self.old_site.pk)

    def test_two_independent_pairs_both_merge(self):
        older_a, newer_a, sites_a = self._pair(name="core-sw-01", identity="older")
        older_b, newer_b, sites_b = self._pair(name="core-sw-02", identity=None)
        self._report({**sites_a, **sites_b})

        result = merge_site_relabel_duplicates(self.sync)

        self.assertEqual(result["merged_count"], 2)
        for older, newer in ((older_a, newer_a), (older_b, newer_b)):
            self.assertFalse(Device.objects.filter(pk=newer.pk).exists())
            older.refresh_from_db()
            self.assertEqual(older.site_id, self.new_site.pk)

    # -- routing rows on the newer device ----------------------------------

    def _ospf_interface_on(self, device):
        from forward_netbox.utilities.sync_primitives import optional_model

        label = "netbox_routing.ospfinterface"
        OSPFInstance = optional_model("netbox_routing", "OSPFInstance", label)
        OSPFArea = optional_model("netbox_routing", "OSPFArea", label)
        OSPFInterface = optional_model("netbox_routing", "OSPFInterface", label)
        interface = Interface.objects.create(
            device=device, name="Gi0/0", type="1000base-t"
        )
        instance = OSPFInstance.objects.create(
            name=f"{device.name} OSPF 1",
            router_id="10.0.0.1",
            process_id=1,
            device=device,
        )
        area = OSPFArea.objects.get_or_create(
            area_id="0.0.0.0", defaults={"area_type": "standard"}
        )[0]
        return OSPFInterface.objects.create(
            instance=instance, area=area, interface=interface
        )

    def _bgp_peer_addressed_on(self, device):
        from ipam.models import ASN
        from ipam.models import IPAddress
        from ipam.models import RIR

        from forward_netbox.utilities.sync_primitives import optional_model

        label = "netbox_routing.bgppeer"
        BGPRouter = optional_model("netbox_routing", "BGPRouter", label)
        BGPScope = optional_model("netbox_routing", "BGPScope", label)
        BGPPeer = optional_model("netbox_routing", "BGPPeer", label)
        rir = RIR.objects.get_or_create(name="rir-a", slug="rir-a")[0]
        local = ASN.objects.get_or_create(asn=65000, defaults={"rir": rir})[0]
        remote = ASN.objects.get_or_create(asn=65001, defaults={"rir": rir})[0]
        interface = Interface.objects.create(device=device, name="Lo0", type="virtual")
        address = IPAddress.objects.create(address="10.10.0.2/32", status="active")
        address.assigned_object = interface
        address.save()
        router = BGPRouter.objects.create(
            name=f"{device.name} AS65000",
            assigned_object_type=ContentType.objects.get_for_model(Device),
            assigned_object_id=device.pk,
            asn=local,
        )
        scope = BGPScope.objects.create(router=router, vrf=None)
        return BGPPeer.objects.create(
            scope=scope,
            peer=address,
            name="peer",
            remote_as=remote,
            local_as=local,
            enabled=True,
            status="active",
        )

    def test_ospf_rows_on_the_newer_device_are_released_so_the_merge_completes(self):
        from forward_netbox.utilities.sync_primitives import optional_model

        older, newer, sites = self._pair(identity="older")
        self._ospf_interface_on(newer)
        self._report(sites)

        result = merge_site_relabel_duplicates(self.sync)

        self.assertEqual(result["failed_count"], 0)
        self.assertEqual(result["merged_count"], 1)
        released = result["merged_pairs"][0]["routing_rows_released"]
        self.assertEqual(released["netbox_routing.ospfinterface"], 1)
        self.assertFalse(Device.objects.filter(pk=newer.pk).exists())
        older.refresh_from_db()
        self.assertEqual(older.site_id, self.new_site.pk)
        OSPFInterface = optional_model(
            "netbox_routing", "OSPFInterface", "netbox_routing.ospfinterface"
        )
        self.assertEqual(OSPFInterface.objects.count(), 0)

    def test_a_bgp_peer_addressed_on_the_newer_device_is_released(self):
        older, newer, sites = self._pair(identity="older")
        self._bgp_peer_addressed_on(newer)
        self._report(sites)

        result = merge_site_relabel_duplicates(self.sync)

        self.assertEqual(result["failed_count"], 0, result["failed_pairs"])
        self.assertFalse(Device.objects.filter(pk=newer.pk).exists())
        self.assertIn(
            "netbox_routing.bgppeer", result["merged_pairs"][0]["routing_rows_released"]
        )

    def test_a_non_routing_protector_refuses_and_releases_nothing(self):
        from types import SimpleNamespace

        from forward_netbox.utilities.sync_primitives import optional_model

        older, newer, sites = self._pair(identity="older")
        ospf_interface = self._ospf_interface_on(newer)
        self._report(sites)
        manual = SimpleNamespace(_meta=SimpleNamespace(label_lower="ipam.service"))

        with patch(
            "forward_netbox.utilities.scope_reconciliation._objects_protecting_device",
            return_value=[ospf_interface, manual],
        ):
            result = merge_site_relabel_duplicates(self.sync)

        self.assertEqual(result["merged_count"], 0)
        failed = result["failed_pairs"][0]
        self.assertEqual(failed["reason"], "newer_device_delete_refused")
        self.assertEqual(failed["blocking_models"], ["ipam.service"])
        self.assertTrue(Device.objects.filter(pk=newer.pk).exists())
        OSPFInterface = optional_model(
            "netbox_routing", "OSPFInterface", "netbox_routing.ospfinterface"
        )
        self.assertEqual(OSPFInterface.objects.count(), 1)
        older.refresh_from_db()
        self.assertEqual(older.site_id, self.old_site.pk)

    def test_an_object_protected_by_a_non_allowlisted_model_refuses_cleanly(self):
        from django.db.models.deletion import ProtectedError
        from types import SimpleNamespace

        from forward_netbox.utilities.scope_reconciliation import (
            _delete_releasable_object,
        )

        manual = SimpleNamespace(_meta=SimpleNamespace(label_lower="ipam.service"))
        victim = Mock()
        victim._meta = SimpleNamespace(label_lower="netbox_routing.bgppeer")
        victim.delete.side_effect = ProtectedError("blocked", [manual])

        refusal = _delete_releasable_object(victim, {})

        self.assertEqual(refusal, {"ipam.service": 1})

    def test_an_allowlisted_blocker_is_released_before_the_retry(self):
        from django.db.models.deletion import ProtectedError
        from types import SimpleNamespace

        from forward_netbox.utilities.scope_reconciliation import (
            _delete_releasable_object,
        )

        session = Mock()
        session._meta = SimpleNamespace(
            label_lower="netbox_peering_manager.peeringsession"
        )
        peer = Mock()
        peer._meta = SimpleNamespace(label_lower="netbox_routing.bgppeer")
        peer.delete.side_effect = [ProtectedError("blocked", [session]), None]

        refusal = _delete_releasable_object(peer, {})

        self.assertIsNone(refusal)
        session.delete.assert_called_once()
        self.assertEqual(peer.delete.call_count, 2)

    def test_recursion_past_the_round_limit_refuses_instead_of_looping_forever(self):
        from django.db.models.deletion import ProtectedError
        from types import SimpleNamespace

        from forward_netbox.utilities.scope_reconciliation import (
            _delete_releasable_object,
        )

        a = Mock()
        a._meta = SimpleNamespace(label_lower="netbox_routing.bgppeer")
        b = Mock()
        b._meta = SimpleNamespace(label_lower="netbox_peering_manager.peeringsession")
        # Each blocks the other forever - a pathological case the real schema
        # cannot produce, but the recursion must still terminate.
        a.delete.side_effect = ProtectedError("blocked", [b])
        b.delete.side_effect = ProtectedError("blocked", [a])

        refusal = _delete_releasable_object(a, {})

        self.assertIsNotNone(refusal)

    def _peering_session_on(self, bgp_peer):
        from forward_netbox.utilities.sync_primitives import optional_model

        PeeringSession = optional_model(
            "netbox_peering_manager",
            "PeeringSession",
            "netbox_peering_manager.peeringsession",
        )
        if PeeringSession is None:
            self.skipTest("netbox-peering-manager optional plugin is not installed")
        return PeeringSession.objects.create(bgp_peer=bgp_peer)

    def test_a_peering_session_on_the_newer_devices_peer_is_released_too(self):
        # PeeringSession PROTECTs the BGPPeer it is built from, one-to-one.
        # Deleting an allowlisted BGPPeer that still has a session used to
        # raise a bare ProtectedError the merge loop caught as a crash.
        older, newer, sites = self._pair(identity="older")
        bgp_peer = self._bgp_peer_addressed_on(newer)
        self._peering_session_on(bgp_peer)
        self._report(sites)

        result = merge_site_relabel_duplicates(self.sync)

        self.assertEqual(result["failed_count"], 0, result["failed_pairs"])
        self.assertFalse(Device.objects.filter(pk=newer.pk).exists())
        released = result["merged_pairs"][0]["routing_rows_released"]
        self.assertIn("netbox_routing.bgppeer", released)
        self.assertIn("netbox_peering_manager.peeringsession", released)

    def test_only_sync_built_routing_models_are_releasable(self):
        from forward_netbox.utilities.scope_reconciliation import (
            SITE_RELABEL_RELEASABLE_ROUTING_MODELS,
        )

        self.assertEqual(
            set(SITE_RELABEL_RELEASABLE_ROUTING_MODELS),
            {
                "netbox_routing.bgppeeraddressfamily",
                "netbox_routing.bgppeer",
                "netbox_routing.bgpscope",
                "netbox_routing.bgprouter",
                "netbox_routing.ospfinterface",
                "netbox_routing.ospfinstance",
                "netbox_peering_manager.peeringsession",
            },
        )

    # -- the last repair is visible on the page ------------------------------

    def _repair_job(self, data):
        from forward_netbox.utilities.sync_facade import BUTTON_JOB_SPECS

        suffix = BUTTON_JOB_SPECS["merge_site_relabel_duplicates"][1]
        return Job.objects.create(
            object_type=ContentType.objects.get_for_model(ForwardSync),
            object_id=self.sync.pk,
            name=f"{self.sync.name} - {suffix}",
            status=JobStatusChoices.STATUS_COMPLETED,
            completed=timezone.now(),
            job_id=uuid.uuid4(),
            data=data,
        )

    def test_the_page_payload_reports_a_refused_repair_and_what_blocked_it(self):
        from forward_netbox.views import _last_site_relabel_repair

        self._repair_job(
            {
                "merged_count": 1,
                "failed_count": 2,
                "held_count": 0,
                "merged_pairs": [
                    {"routing_rows_released": {"netbox_routing.bgppeer": 3}}
                ],
                "failed_pairs": [
                    {
                        "reason": "newer_device_delete_refused",
                        "blocking_models": ["ipam.service"],
                    },
                    {"reason": "newer_device_delete_refused", "blocking_models": None},
                ],
            }
        )

        last = _last_site_relabel_repair(self.sync)

        self.assertEqual(last["merged_count"], 1)
        self.assertEqual(last["failed_count"], 2)
        self.assertEqual(last["failed_by_reason"], [("newer_device_delete_refused", 2)])
        self.assertEqual(last["blocking_models"], [("ipam.service", 1)])
        self.assertEqual(last["routing_rows_released"], [("netbox_routing.bgppeer", 3)])

    def test_the_page_payload_has_no_last_repair_before_one_has_run(self):
        from forward_netbox.views import _last_site_relabel_repair

        self.assertIsNone(_last_site_relabel_repair(self.sync))
        self._repair_job({"error": "refused", "error_type": "X"})
        self.assertIsNone(_last_site_relabel_repair(self.sync))
