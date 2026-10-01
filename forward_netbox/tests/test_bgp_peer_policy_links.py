# BGP peer address families link to the route maps and prefix lists the
# neighbor's configuration attaches. Applied through the same runner path a
# sync uses, over the cases that matter: both policy kinds, a name that is not
# imported, a per-device variant, and an existing link left alone.
from unittest.mock import Mock

from dcim.models import Device
from dcim.models import DeviceRole
from dcim.models import DeviceType
from dcim.models import Manufacturer
from dcim.models import Site
from django.apps import apps
from django.test import TestCase

from forward_netbox.models import ForwardSource
from forward_netbox.models import ForwardSync
from forward_netbox.utilities.sync import ForwardSyncRunner
from forward_netbox.utilities.sync_routing_impl import BGP_PEER_POLICY_FIELDS
from forward_netbox.utilities.sync_routing_policy import record_policy_variant_holders
from forward_netbox.utilities.sync_routing_policy import ROUTE_MAP_MODEL


class BgpPeerPolicyLinksTest(TestCase):
    def setUp(self):
        if not apps.is_installed("netbox_routing"):
            self.skipTest("netbox-routing optional plugin is not installed")
        source = ForwardSource.objects.create(
            name="pol-source",
            type="saas",
            url="https://fwd.app",
            status="ready",
            parameters={"network_id": "net-1"},
        )
        self.sync = ForwardSync.objects.create(
            name="pol-sync",
            source=source,
            parameters={"snapshot_id": "latestProcessed"},
        )
        site = Site.objects.create(name="Pol Site", slug="pol-site")
        manufacturer = Manufacturer.objects.create(name="Pol Mfr", slug="pol-mfr")
        device_type = DeviceType.objects.create(
            manufacturer=manufacturer, model="Pol DT", slug="pol-dt"
        )
        role = DeviceRole.objects.create(name="Pol Role", slug="pol-role")
        for name in ("pol-a", "pol-b"):
            Device.objects.create(
                name=name,
                site=site,
                device_type=device_type,
                role=role,
                status="active",
            )
        self.RouteMap = apps.get_model("netbox_routing", "RouteMap")
        self.PrefixList = apps.get_model("netbox_routing", "PrefixList")
        self.PeerAF = apps.get_model("netbox_routing", "BGPPeerAddressFamily")

    def _runner(self):
        return ForwardSyncRunner(
            sync=self.sync, ingestion=None, client=None, logger_=Mock()
        )

    def _row(self, **extra):
        row = {
            "device": "pol-a",
            "vrf": None,
            "local_asn": 65000,
            "router_id": "10.0.0.1",
            "neighbor_address": "10.1.1.2",
            "peer_asn": 65001,
            "peer_type": "PeerType.EXTERNAL",
            "afi_safi": "AfiSafiType.IPV4_UNICAST",
            "enabled": True,
            "status": "active",
            "has_adj_rib_in": True,
            "has_adj_rib_out": True,
        }
        row.update(extra)
        return row

    def _apply(self, runner, row):
        runner._apply_netbox_routing_bgppeeraddressfamily(row)
        return self.PeerAF.objects.get()

    def test_the_policy_fields_cover_both_kinds_in_both_directions(self):
        self.assertEqual(
            BGP_PEER_POLICY_FIELDS,
            (
                ("routemap_in", "routemap"),
                ("routemap_out", "routemap"),
                ("prefixlist_in", "prefixlist"),
                ("prefixlist_out", "prefixlist"),
            ),
        )

    def test_imported_route_maps_and_prefix_lists_are_linked(self):
        rm_in = self.RouteMap.objects.create(name="RM-IN")
        rm_out = self.RouteMap.objects.create(name="RM-OUT")
        pl_in = self.PrefixList.objects.create(name="PL-IN", family=4)
        pl_out = self.PrefixList.objects.create(name="PL-OUT", family=4)

        peer_af = self._apply(
            self._runner(),
            self._row(
                routemap_in="RM-IN",
                routemap_out="RM-OUT",
                prefixlist_in="PL-IN",
                prefixlist_out="PL-OUT",
            ),
        )

        self.assertEqual(peer_af.routemap_in_id, rm_in.pk)
        self.assertEqual(peer_af.routemap_out_id, rm_out.pk)
        self.assertEqual(peer_af.prefixlist_in_id, pl_in.pk)
        self.assertEqual(peer_af.prefixlist_out_id, pl_out.pk)

    def test_names_match_case_insensitively(self):
        rm = self.RouteMap.objects.create(name="Rm-Mixed")

        peer_af = self._apply(self._runner(), self._row(routemap_in="rm-mixed"))

        self.assertEqual(peer_af.routemap_in_id, rm.pk)

    def test_a_policy_that_is_not_imported_leaves_the_field_empty(self):
        peer_af = self._apply(self._runner(), self._row(routemap_in="NOT-IMPORTED"))

        self.assertIsNone(peer_af.routemap_in_id)

    def test_the_peer_row_still_applies_when_a_policy_is_missing(self):
        rm = self.RouteMap.objects.create(name="RM-IN")

        peer_af = self._apply(
            self._runner(), self._row(routemap_in="RM-IN", routemap_out="GONE")
        )

        self.assertEqual(peer_af.routemap_in_id, rm.pk)
        self.assertIsNone(peer_af.routemap_out_id)

    def test_a_device_holding_a_variant_links_to_that_variant(self):
        shared = self.RouteMap.objects.create(name="RM-X")
        variant = self.RouteMap.objects.create(name="RM-X@pol-b")
        runner = self._runner()
        record_policy_variant_holders(
            runner,
            ROUTE_MAP_MODEL,
            {"name": "RM-X@pol-b", "map_name": "RM-X", "holder_devices": ["pol-a"]},
        )

        peer_af = self._apply(runner, self._row(routemap_in="RM-X"))

        self.assertEqual(peer_af.routemap_in_id, variant.pk)
        self.assertNotEqual(peer_af.routemap_in_id, shared.pk)

    def test_a_device_not_holding_the_variant_links_to_the_shared_definition(self):
        shared = self.RouteMap.objects.create(name="RM-X")
        self.RouteMap.objects.create(name="RM-X@pol-b")
        runner = self._runner()
        record_policy_variant_holders(
            runner,
            ROUTE_MAP_MODEL,
            {"name": "RM-X@pol-b", "map_name": "RM-X", "holder_devices": ["pol-b"]},
        )

        peer_af = self._apply(runner, self._row(routemap_in="RM-X"))

        self.assertEqual(peer_af.routemap_in_id, shared.pk)

    def test_a_row_without_policies_does_not_clear_an_existing_link(self):
        rm = self.RouteMap.objects.create(name="RM-IN")
        runner = self._runner()
        self._apply(runner, self._row(routemap_in="RM-IN"))

        peer_af = self._apply(runner, self._row())

        self.assertEqual(peer_af.routemap_in_id, rm.pk)

    def test_a_changed_policy_is_updated_in_place(self):
        self.RouteMap.objects.create(name="RM-OLD")
        new = self.RouteMap.objects.create(name="RM-NEW")
        runner = self._runner()
        self._apply(runner, self._row(routemap_in="RM-OLD"))

        peer_af = self._apply(runner, self._row(routemap_in="RM-NEW"))

        self.assertEqual(self.PeerAF.objects.count(), 1)
        self.assertEqual(peer_af.routemap_in_id, new.pk)

    def test_repeating_the_same_row_is_a_noop(self):
        from django.db import connection
        from django.test.utils import CaptureQueriesContext

        self.RouteMap.objects.create(name="RM-IN")
        runner = self._runner()
        row = self._row(routemap_in="RM-IN")
        runner._apply_netbox_routing_bgppeeraddressfamily(row)

        with CaptureQueriesContext(connection) as queries:
            runner._apply_netbox_routing_bgppeeraddressfamily(row)

        updates = [q for q in queries if q["sql"].lstrip().upper().startswith("UPDATE")]
        self.assertEqual(updates, [])
        self.assertEqual(self.PeerAF.objects.count(), 1)
