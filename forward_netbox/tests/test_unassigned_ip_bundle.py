import unittest

from dcim.models import Device
from dcim.models import DeviceRole
from dcim.models import DeviceType
from dcim.models import Manufacturer
from dcim.models import Site
from django.apps import apps
from django.contrib.contenttypes.models import ContentType
from django.test import TestCase
from ipam.models import ASN
from ipam.models import IPAddress
from ipam.models import RIR
from ipam.models import VRF

from forward_netbox.utilities.sync_primitives import optional_model
from forward_netbox.views import _interface_state_bundle_payload

NETBOX_ROUTING_INSTALLED = apps.is_installed("netbox_routing")


@unittest.skipUnless(NETBOX_ROUTING_INSTALLED, "netbox-routing is not installed")
class UnassignedIpSplitTest(TestCase):
    """An address no interface holds is not an orphan if a BGP peer needs it."""

    def setUp(self):
        site = Site.objects.create(name="U Site", slug="u-site")
        mfr = Manufacturer.objects.create(name="U Mfr", slug="u-mfr")
        dtype = DeviceType.objects.create(manufacturer=mfr, model="U DT", slug="u-dt")
        role = DeviceRole.objects.create(name="U Role", slug="u-role")
        self.device = Device.objects.create(
            name="u-dev", site=site, device_type=dtype, role=role, status="active"
        )
        rir = RIR.objects.create(name="U RIR", slug="u-rir")
        self.local, _ = ASN.objects.get_or_create(asn=65000, defaults={"rir": rir})
        self.remote, _ = ASN.objects.get_or_create(asn=65001, defaults={"rir": rir})
        model = "netbox_routing.bgppeer"
        self.BGPRouter = optional_model("netbox_routing", "BGPRouter", model)
        self.BGPScope = optional_model("netbox_routing", "BGPScope", model)
        self.BGPPeer = optional_model("netbox_routing", "BGPPeer", model)

    def peer_on(self, ip, vrf=None):
        router, _ = self.BGPRouter.objects.get_or_create(
            name="u-router",
            defaults={
                "assigned_object_type": ContentType.objects.get_for_model(Device),
                "assigned_object_id": self.device.pk,
                "asn": self.local,
            },
        )
        scope, _ = self.BGPScope.objects.get_or_create(router=router, vrf=vrf)
        return self.BGPPeer.objects.create(
            scope=scope,
            peer=ip,
            name=f"peer-{ip.pk}",
            remote_as=self.remote,
            local_as=self.local,
            enabled=True,
            status="active",
        )

    def test_a_neighbour_address_is_told_apart_from_a_real_orphan(self):
        vrf = VRF.objects.create(name="INSIDE")
        neighbour_global = IPAddress.objects.create(address="10.0.0.2/32")
        neighbour_vrf = IPAddress.objects.create(address="10.0.0.3/32", vrf=vrf)
        IPAddress.objects.create(address="10.9.9.9/32")  # nothing uses it
        self.peer_on(neighbour_global)
        self.peer_on(neighbour_vrf, vrf=vrf)

        block = _interface_state_bundle_payload()["unassigned_ip_addresses"]

        self.assertEqual(block["total"], 3)
        self.assertEqual(block["used_by_a_bgp_peer"], 2)
        self.assertEqual(block["used_by_a_bgp_peer_in_a_vrf"], 1)
        self.assertEqual(block["used_by_no_interface_and_no_bgp_peer"], 1)

    def test_an_address_two_peers_share_is_counted_once(self):
        shared = IPAddress.objects.create(address="10.0.0.2/32")
        self.peer_on(shared)
        self.peer_on(shared, vrf=VRF.objects.create(name="OTHER"))

        block = _interface_state_bundle_payload()["unassigned_ip_addresses"]

        self.assertEqual(block["used_by_a_bgp_peer"], 1)
        self.assertEqual(block["used_by_no_interface_and_no_bgp_peer"], 0)

    def test_an_address_on_an_interface_is_not_in_the_count(self):
        from dcim.models import Interface

        interface = Interface.objects.create(
            device=self.device, name="Eth1", type="1000base-t"
        )
        IPAddress.objects.create(
            address="10.1.1.1/24",
            assigned_object_type=ContentType.objects.get_for_model(Interface),
            assigned_object_id=interface.pk,
        )

        block = _interface_state_bundle_payload()["unassigned_ip_addresses"]

        self.assertEqual(block["total"], 0)
