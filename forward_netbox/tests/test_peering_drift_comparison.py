# Slice seven of the adapter-only drift comparison: the peering models -
# `netbox_routing.bgppeer`, `bgpaddressfamily`, `bgppeeraddressfamily`, and
# `netbox_peering_manager.peeringsession`.
#
# These were left until last because one Forward row means the most persisted
# objects of any adapter model. A single BGP peer resolves - and the apply
# would write - two ASNs, the neighbour IPAddress, a BGPRouter, a BGPScope and
# the peer itself, and an unaudited path would write five of those six while
# "measuring".
#
# Two of them are DIRECT saves that no `runner.` shim reaches: the neighbour
# address inside `ensure_bgp_peer_ip`, and the ASN (plus the RIR beneath it)
# inside `_ensure_asn`. The first is threaded with `preview`, the second is
# overridden on the preview runner.
#
# `netbox_routing.bgprouter` and `netbox_routing.bgpscope` have NO Forward
# query of their own - they exist only as parents built while applying a peer -
# so a router this run would rewrite is drift that no model would report if the
# peer's verdict looked only at the leaf row. That is what
# `preview_routing_outcome` is for, and what the parent tests below pin.
from dcim.models import Device
from dcim.models import DeviceRole
from dcim.models import DeviceType
from dcim.models import Manufacturer
from dcim.models import Site
from django.test import TestCase
from ipam.models import ASN
from ipam.models import IPAddress
from ipam.models import RIR

from forward_netbox.utilities.drift_comparison import compare_model_rows
from forward_netbox.utilities.sync_routing_impl import bgp_peer_comments
from forward_netbox.utilities.sync_routing_impl import bgp_peer_name

BGP_PEER = "netbox_routing.bgppeer"


def _routing_models():
    from forward_netbox.utilities.sync_primitives import optional_model

    return (
        optional_model("netbox_routing", "BGPRouter", BGP_PEER),
        optional_model("netbox_routing", "BGPScope", BGP_PEER),
        optional_model("netbox_routing", "BGPPeer", BGP_PEER),
    )


class PeeringPreviewTest(TestCase):
    def setUp(self):
        site = Site.objects.create(name="P Site", slug="p-site")
        mfr = Manufacturer.objects.create(name="P Mfr", slug="p-mfr")
        dtype = DeviceType.objects.create(manufacturer=mfr, model="P DT", slug="p-dt")
        role = DeviceRole.objects.create(name="P Role", slug="p-role")
        self.device = Device.objects.create(
            name="bgp-dev", site=site, device_type=dtype, role=role, status="active"
        )
        self.rir = RIR.objects.create(name="P RIR", slug="p-rir")

    def _row(self, **extra):
        row = {
            "device": "bgp-dev",
            "local_asn": 65000,
            "peer_asn": 65001,
            "neighbor_address": "10.10.0.2",
            "enabled": True,
            "status": "active",
            "peer_type": "EXTERNAL",
        }
        row.update(extra)
        return row

    def _asns(self):
        local, _ = ASN.objects.get_or_create(asn=65000, defaults={"rir": self.rir})
        remote, _ = ASN.objects.get_or_create(asn=65001, defaults={"rir": self.rir})
        return local, remote

    def _peer_ip(self):
        return IPAddress.objects.create(address="10.10.0.2/32", status="active")

    def _existing_peer(self, *, name=None, router_name=None):
        """The whole chain a converged deployment already has."""
        from django.contrib.contenttypes.models import ContentType

        BGPRouter, BGPScope, BGPPeer = _routing_models()
        local, remote = self._asns()
        peer_ip = self._peer_ip()
        router = BGPRouter.objects.create(
            name=router_name or f"{self.device.name} AS65000",
            assigned_object_type=ContentType.objects.get_for_model(Device),
            assigned_object_id=self.device.pk,
            asn=local,
        )
        scope = BGPScope.objects.create(router=router, vrf=None)
        # Built from the apply's OWN renderers rather than from literals, so
        # a converged fixture cannot quietly stop being converged when the
        # comment or name format changes - the test would then be asserting
        # against a shape the apply no longer writes.
        peer = BGPPeer.objects.create(
            scope=scope,
            peer=peer_ip,
            name=name or bgp_peer_name(self._row()),
            remote_as=remote,
            local_as=local,
            enabled=True,
            status="active",
            description="",
            comments=bgp_peer_comments(self._row()),
        )
        return router, scope, peer

    # --- the firewall ------------------------------------------------------

    def test_a_preview_creates_no_asn_address_router_scope_or_peer(self):
        BGPRouter, BGPScope, BGPPeer = _routing_models()
        before = (
            ASN.objects.count(),
            IPAddress.objects.count(),
            RIR.objects.count(),
            BGPRouter.objects.count(),
            BGPScope.objects.count(),
            BGPPeer.objects.count(),
        )
        compare_model_rows(None, BGP_PEER, [self._row()])
        after = (
            ASN.objects.count(),
            IPAddress.objects.count(),
            RIR.objects.count(),
            BGPRouter.objects.count(),
            BGPScope.objects.count(),
            BGPPeer.objects.count(),
        )
        self.assertEqual(before, after)

    def test_a_preview_does_not_rewrite_a_drifted_peer(self):
        _, _, peer = self._existing_peer()
        compare_model_rows(None, BGP_PEER, [self._row(description="new text")])
        peer.refresh_from_db()
        self.assertEqual(peer.description, "")

    # --- the classification ------------------------------------------------

    def test_a_fully_matching_row_is_unchanged(self):
        self._existing_peer()
        result = compare_model_rows(None, BGP_PEER, [self._row()])
        self.assertEqual(result["unchanged"], 1)
        self.assertEqual(result["creates"], 0)
        self.assertEqual(result["updates"], 0)

    def test_an_absent_peer_is_a_create(self):
        result = compare_model_rows(None, BGP_PEER, [self._row()])
        self.assertEqual(result["creates"], 1)

    def test_a_drifted_peer_is_an_update(self):
        self._existing_peer()
        result = compare_model_rows(None, BGP_PEER, [self._row(description="changed")])
        self.assertEqual(result["updates"], 1)
        self.assertEqual(result["unchanged"], 0)

    def test_an_absent_asn_is_a_create_not_a_crash(self):
        """The router name reads `local_asn.asn`; a `None` there raises.

        `AttributeError` is caught by no caller, so an absent ASN would have
        killed the whole comparison rather than classifying one row.
        """
        result = compare_model_rows(None, BGP_PEER, [self._row()])
        self.assertEqual(result["creates"], 1)
        self.assertEqual(ASN.objects.count(), 0)

    def test_an_absent_neighbour_address_is_a_create(self):
        self._asns()
        result = compare_model_rows(None, BGP_PEER, [self._row()])
        self.assertEqual(result["creates"], 1)
        self.assertEqual(IPAddress.objects.count(), 0)

    def test_a_row_naming_an_absent_vrf_is_a_create_not_a_match_on_global(self):
        """Every coalesce set includes `vrf`, so `None` must not stand in.

        The apply CREATES a missing VRF and coalesces inside it. The preview
        resolves instead, so an unresolved VRF left `vrf=None` on the lookup -
        which matched the global scope built here, and reported a peer the
        apply would create as already present and unchanged.
        """
        self._existing_peer()
        result = compare_model_rows(None, BGP_PEER, [self._row(vrf="TENANT-A")])
        self.assertEqual(result["creates"], 1)
        self.assertEqual(result["unchanged"], 0)

    def test_a_row_naming_an_existing_vrf_still_resolves(self):
        from ipam.models import VRF

        VRF.objects.create(name="TENANT-A")
        result = compare_model_rows(None, BGP_PEER, [self._row(vrf="TENANT-A")])
        self.assertEqual(result["creates"], 1)
        self.assertEqual(VRF.objects.count(), 1)

    # --- the parents no other model reports --------------------------------

    def test_a_drifted_router_makes_an_otherwise_matching_peer_an_update(self):
        """`netbox_routing.bgprouter` has no query, so this is its only report.

        The peer row itself matches exactly. Only the router's name differs -
        the apply would rewrite it, and if the verdict came from the leaf row
        alone this would read `unchanged` while every run rewrote the router.
        """
        router, _, _ = self._existing_peer(router_name="stale name")
        result = compare_model_rows(None, BGP_PEER, [self._row()])
        self.assertEqual(result["updates"], 1)
        self.assertEqual(result["unchanged"], 0)
        router.refresh_from_db()
        self.assertEqual(router.name, "stale name")

    # --- rows the apply refuses -------------------------------------------

    def test_an_unknown_device_is_rejected(self):
        result = compare_model_rows(None, BGP_PEER, [self._row(device="nope")])
        self.assertEqual(result["rejected"], 1)
        self.assertEqual(result["creates"], 0)

    def test_an_unparseable_asn_is_rejected_not_a_create(self):
        """`ForwardQueryError` is not a `ForwardDataError`.

        It was escaping the row loop entirely, so one malformed ASN would have
        killed the comparison for every other row in the batch.
        """
        result = compare_model_rows(None, BGP_PEER, [self._row(local_asn="abc")])
        self.assertEqual(result["rejected"], 1)
        self.assertEqual(result["creates"], 0)

    def test_one_malformed_row_does_not_lose_the_rest_of_the_batch(self):
        self._existing_peer()
        result = compare_model_rows(
            None,
            BGP_PEER,
            [self._row(local_asn="abc"), self._row()],
        )
        self.assertEqual(result["rejected"], 1)
        self.assertEqual(result["unchanged"], 1)

    def test_a_sub_one_asn_is_rejected(self):
        result = compare_model_rows(None, BGP_PEER, [self._row(local_asn=0)])
        self.assertEqual(result["rejected"], 1)

    def test_a_row_missing_its_device_key_is_rejected(self):
        row = self._row()
        del row["device"]
        result = compare_model_rows(None, BGP_PEER, [row])
        self.assertEqual(result["rejected"], 1)

    # --- the model is actually registered ----------------------------------

    def test_the_peering_models_report_a_measurement_not_an_upper_bound(self):
        for model_string in (
            "netbox_routing.bgppeer",
            "netbox_routing.bgpaddressfamily",
            "netbox_routing.bgppeeraddressfamily",
            "netbox_peering_manager.peeringsession",
        ):
            with self.subTest(model_string=model_string):
                self.assertIsNotNone(compare_model_rows(None, model_string, []))
