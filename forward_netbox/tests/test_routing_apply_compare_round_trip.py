# What a sync writes, the next comparison must call unchanged.
#
# Every existing adapter-drift test builds its "already converged" fixture BY
# HAND, with ORM creates and the apply's own renderers. That proves the
# comparison agrees with a state someone described, and it cannot prove the
# comparison agrees with the state the APPLY ACTUALLY PRODUCES. A round trip
# where the apply writes one thing and the comparison expects another is
# invisible to all of them - which is how 180 permanently drifted OSPF rows
# reached a customer.
#
# So: run the real apply, then compare the same rows, and require zero drift.
# It is the weakest possible assertion and the one that matters, because a sync
# that does not converge is a sync whose drift number can never be trusted.
from unittest.mock import Mock

from dcim.models import Device
from dcim.models import DeviceRole
from dcim.models import DeviceType
from dcim.models import Interface
from dcim.models import Manufacturer
from dcim.models import Site
from django.test import TestCase

from forward_netbox.models import ForwardSource
from forward_netbox.models import ForwardSync
from forward_netbox.utilities.drift_comparison import compare_model_rows
from forward_netbox.utilities.sync import ForwardSyncRunner

BGP_PEER = "netbox_routing.bgppeer"
OSPF_INTERFACE = "netbox_routing.ospfinterface"


class RoutingRoundTripTest(TestCase):
    def setUp(self):
        self.source = ForwardSource.objects.create(
            name="rt-source",
            type="saas",
            url="https://forward.example.com",
            status="ready",
            parameters={
                "username": "u@example.com",
                "password": "p",
                "verify": True,
                "network_id": "net-1",
            },
        )
        self.sync = ForwardSync.objects.create(
            name="rt-sync",
            source=self.source,
            parameters={"snapshot_id": "latestProcessed"},
        )
        site = Site.objects.create(name="RT Site", slug="rt-site")
        mfr = Manufacturer.objects.create(name="RT Mfr", slug="rt-mfr")
        dtype = DeviceType.objects.create(manufacturer=mfr, model="RT DT", slug="rt-dt")
        role = DeviceRole.objects.create(name="RT Role", slug="rt-role")
        self.device = Device.objects.create(
            name="rt-dev", site=site, device_type=dtype, role=role, status="active"
        )
        self.interface = Interface.objects.create(
            device=self.device, name="GigabitEthernet0/0", type="1000base-t"
        )

    def _runner(self):
        return ForwardSyncRunner(
            sync=self.sync, ingestion=None, client=None, logger_=Mock()
        )

    # --- BGP peers ---------------------------------------------------------

    def _peer_row(self, **extra):
        row = {
            "device": "rt-dev",
            "vrf": None,
            "local_asn": 65000,
            "router_id": "10.0.0.1",
            "neighbor_address": "10.1.1.2",
            "peer_asn": 65001,
            "peer_type": "EBGP",
            "enabled": True,
            "status": "active",
            "description": "",
        }
        row.update(extra)
        return row

    def test_an_applied_bgp_peer_compares_unchanged(self):
        from forward_netbox.utilities.sync_routing_impl import (
            apply_netbox_routing_bgppeer,
        )

        rows = [self._peer_row()]
        apply_netbox_routing_bgppeer(self._runner(), rows[0])

        result = compare_model_rows(None, BGP_PEER, rows)

        # A create here means the comparison cannot find what the apply just
        # wrote, so the row would be reported as new on every run forever.
        self.assertEqual(result["creates"], 0)
        self.assertEqual(result["updates"], 0)

    def test_applying_twice_writes_nothing_the_second_time(self):
        from forward_netbox.utilities.sync_routing_impl import (
            apply_netbox_routing_bgppeer,
        )

        row = self._peer_row()
        apply_netbox_routing_bgppeer(self._runner(), row)
        apply_netbox_routing_bgppeer(self._runner(), row)

        result = compare_model_rows(None, BGP_PEER, [row])

        self.assertEqual(result["creates"], 0)
        self.assertEqual(result["updates"], 0)

    # --- OSPF interfaces ---------------------------------------------------

    def _ospf_row(self, **extra):
        row = {
            "device": "rt-dev",
            "process_id": "1",
            "router_id": "10.0.0.1",
            "area_id": "0.0.0.0",
            "area_type": "standard",
            "local_interface": "GigabitEthernet0/0",
        }
        row.update(extra)
        return row

    def _apply_ospf(self, rows):
        from forward_netbox.utilities.row_collapsing import collapse_rows
        from forward_netbox.utilities.sync_routing_impl import (
            apply_netbox_routing_ospfinterface,
        )

        runner = self._runner()
        for row in collapse_rows(OSPF_INTERFACE, rows):
            apply_netbox_routing_ospfinterface(runner, row)

    def test_an_applied_ospf_interface_compares_unchanged(self):
        rows = [self._ospf_row(remote_device="peer-a", remote_router_id="10.0.0.2")]
        self._apply_ospf(rows)

        result = compare_model_rows(None, OSPF_INTERFACE, rows)

        self.assertEqual(result["creates"], 0)
        self.assertEqual(result["updates"], 0)

    def test_an_applied_broadcast_segment_compares_unchanged(self):
        # The regression that reached a customer: three neighbours on one
        # interface, applied and then compared, must be zero drift.
        rows = [
            self._ospf_row(remote_device="peer-a", remote_router_id="10.0.0.2"),
            self._ospf_row(remote_device="peer-b", remote_router_id="10.0.0.3"),
            self._ospf_row(remote_device="peer-c", remote_router_id="10.0.0.4"),
        ]
        self._apply_ospf(rows)

        result = compare_model_rows(None, OSPF_INTERFACE, rows)

        self.assertEqual(result["creates"], 0)
        self.assertEqual(result["updates"], 0)
