# BGP peer round trips through the FULL apply path, over the shapes a real
# estate has: VRF-scoped sessions, neighbour addresses that already exist on
# interfaces, devices that peer with each other, IPv6, long descriptions.
#
# The existing round trip calls the peer adapter directly, so it never runs
# the dependency-cache priming the sync runs before every batch. This one
# goes through `apply_model_rows`, exactly as a sync does, and then compares.
from unittest.mock import Mock

from dcim.models import Device
from dcim.models import DeviceRole
from dcim.models import DeviceType
from dcim.models import Interface
from dcim.models import Manufacturer
from dcim.models import Site
from django.test import TestCase
from ipam.models import IPAddress
from ipam.models import VRF

from forward_netbox.models import ForwardSource
from forward_netbox.models import ForwardSync
from forward_netbox.utilities.drift_comparison import compare_model_rows
from forward_netbox.utilities.sync import ForwardSyncRunner
from forward_netbox.utilities.sync_reporting import apply_model_rows

BGP_PEER = "netbox_routing.bgppeer"


def _routing_models():
    from django.apps import apps

    return (
        apps.get_model("netbox_routing", "BGPRouter"),
        apps.get_model("netbox_routing", "BGPScope"),
        apps.get_model("netbox_routing", "BGPPeer"),
    )


class BgpPeerRoundTripVariantsTest(TestCase):
    def setUp(self):
        self.source = ForwardSource.objects.create(
            name="rtv-source",
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
            name="rtv-sync",
            source=self.source,
            parameters={"snapshot_id": "latestProcessed"},
        )
        site = Site.objects.create(name="RTV Site", slug="rtv-site")
        mfr = Manufacturer.objects.create(name="RTV Mfr", slug="rtv-mfr")
        dtype = DeviceType.objects.create(
            manufacturer=mfr, model="RTV DT", slug="rtv-dt"
        )
        role = DeviceRole.objects.create(name="RTV Role", slug="rtv-role")
        self.dev_a = Device.objects.create(
            name="rtv-a", site=site, device_type=dtype, role=role, status="active"
        )
        self.dev_b = Device.objects.create(
            name="rtv-b", site=site, device_type=dtype, role=role, status="active"
        )
        self.if_a = Interface.objects.create(
            device=self.dev_a, name="Ethernet1", type="1000base-t"
        )
        self.if_b = Interface.objects.create(
            device=self.dev_b, name="Ethernet1", type="1000base-t"
        )
        self.vrf = VRF.objects.create(name="CUST-A")

    def _runner(self):
        return ForwardSyncRunner(
            sync=self.sync, ingestion=None, client=None, logger_=Mock()
        )

    def _row(self, **extra):
        row = {
            "device": "rtv-a",
            "vrf": None,
            "local_asn": 65000,
            "router_id": "10.0.0.1",
            "neighbor_address": "10.1.1.2",
            "peer_asn": 65001,
            "peer_type": "PeerType.EXTERNAL",
            "enabled": True,
            "status": "active",
            "session_state": "SessionState.ESTABLISHED",
            "peer_device": None,
            "peer_vrf": None,
            "peer_router_id": None,
            "description": "",
            "advertised_prefixes": 12,
            "received_prefixes": 3,
        }
        row.update(extra)
        return row

    def _apply(self, rows):
        apply_model_rows(self._runner(), BGP_PEER, list(rows))

    def _assert_converged(self, rows, label):
        _, _, BGPPeer = _routing_models()
        self._apply(rows)
        peers_after_first = BGPPeer.objects.count()
        ips_after_first = IPAddress.objects.count()
        # A second apply must write nothing new.
        self._apply(rows)
        self.assertEqual(BGPPeer.objects.count(), peers_after_first, label)
        self.assertEqual(IPAddress.objects.count(), ips_after_first, label)
        result = compare_model_rows(None, BGP_PEER, list(rows))
        self.assertIsNotNone(result, label)
        self.assertEqual(
            (result["creates"], result["updates"], result["rejected"]),
            (0, 0, 0),
            f"{label}: {result}",
        )

    def test_global_neighbour_already_on_an_interface(self):
        IPAddress.objects.create(
            address="10.1.1.2/30",
            assigned_object=self.if_b,
            status="active",
        )
        self._assert_converged([self._row()], "global, /30 on interface")

    def test_vrf_neighbour_already_on_an_interface(self):
        IPAddress.objects.create(
            address="10.1.1.2/30",
            vrf=self.vrf,
            assigned_object=self.if_b,
            status="active",
        )
        self._assert_converged([self._row(vrf="CUST-A")], "vrf, /30 on interface")

    def test_vrf_neighbour_absent(self):
        self._assert_converged([self._row(vrf="CUST-A")], "vrf, absent")

    def test_global_session_with_neighbour_only_in_a_vrf(self):
        IPAddress.objects.create(
            address="10.1.1.2/30",
            vrf=self.vrf,
            assigned_object=self.if_b,
            status="active",
        )
        self._assert_converged([self._row()], "global session, ip only in vrf")

    def test_two_devices_peering_with_each_other(self):
        IPAddress.objects.create(
            address="10.1.1.1/30", assigned_object=self.if_a, status="active"
        )
        IPAddress.objects.create(
            address="10.1.1.2/30", assigned_object=self.if_b, status="active"
        )
        rows = [
            self._row(
                device="rtv-a",
                neighbor_address="10.1.1.2",
                peer_device="rtv-b",
                peer_router_id="10.0.0.2",
            ),
            self._row(
                device="rtv-b",
                router_id="10.0.0.2",
                local_asn=65001,
                peer_asn=65000,
                neighbor_address="10.1.1.1",
                peer_device="rtv-a",
                peer_router_id="10.0.0.1",
            ),
        ]
        self._assert_converged(rows, "mutual peers")

    def test_ibgp_full_mesh_shares_router_and_scope(self):
        rows = [
            self._row(
                neighbor_address="10.255.0.2",
                peer_asn=65000,
                peer_type="PeerType.INTERNAL",
            ),
            self._row(
                neighbor_address="10.255.0.3",
                peer_asn=65000,
                peer_type="PeerType.INTERNAL",
            ),
            self._row(
                neighbor_address="10.255.0.4",
                peer_asn=65000,
                peer_type="PeerType.INTERNAL",
            ),
        ]
        self._assert_converged(rows, "ibgp mesh")

    def test_ipv6_neighbour(self):
        self._assert_converged([self._row(neighbor_address="2001:db8::2")], "ipv6")

    def test_long_description_and_null_router_id(self):
        self._assert_converged(
            [self._row(description="x" * 300, router_id=None)],
            "long description",
        )

    def test_local_as_override_makes_a_second_router(self):
        rows = [
            self._row(neighbor_address="10.1.1.2"),
            self._row(neighbor_address="10.1.2.2", local_asn=64999),
        ]
        self._assert_converged(rows, "two local ASNs on one device")

    def test_same_neighbour_in_two_vrfs(self):
        rows = [
            self._row(neighbor_address="10.1.1.2"),
            self._row(neighbor_address="10.1.1.2", vrf="CUST-A"),
        ]
        self._assert_converged(rows, "same address, global and vrf")

    def test_disabled_peer(self):
        self._assert_converged(
            [
                self._row(
                    enabled=False, status="offline", session_state="SessionState.IDLE"
                )
            ],
            "disabled",
        )
