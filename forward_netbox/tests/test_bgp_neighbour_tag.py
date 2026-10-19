import json
import unittest
from io import StringIO

from dcim.models import Interface
from django.contrib.auth import get_user_model
from django.contrib.contenttypes.models import ContentType
from django.core.management import call_command
from django.test import TestCase
from extras.models import Tag
from ipam.models import IPAddress
from ipam.models import VRF

from forward_netbox.tests.test_unassigned_ip_bundle import BgpPeerFixture
from forward_netbox.tests.test_unassigned_ip_bundle import NETBOX_ROUTING_INSTALLED
from forward_netbox.utilities.bgp_neighbour_ips import BGP_NEIGHBOUR_TAG_SLUG
from forward_netbox.utilities.bgp_neighbour_ips import tag_bgp_neighbour_ips
from forward_netbox.views import _interface_state_bundle_payload


User = get_user_model()


def tagged():
    return {
        str(address)
        for address in IPAddress.objects.filter(
            tags__slug=BGP_NEIGHBOUR_TAG_SLUG
        ).values_list("address", flat=True)
    }


def addresses(*ips):
    return {f"{ip}/32" for ip in ips}


@unittest.skipUnless(NETBOX_ROUTING_INSTALLED, "netbox-routing is not installed")
class BgpNeighbourTagTest(BgpPeerFixture, TestCase):
    """An address held only for a BGP peer carries a tag an operator can filter."""

    def test_only_unassigned_peer_addresses_are_tagged(self):
        neighbour = IPAddress.objects.create(address="10.0.0.2/32")
        in_vrf = IPAddress.objects.create(
            address="10.0.0.3/32", vrf=VRF.objects.create(name="INSIDE")
        )
        IPAddress.objects.create(address="10.9.9.9/32")  # nothing uses it
        self.peer_on(neighbour)
        self.peer_on(in_vrf, vrf=in_vrf.vrf)

        result = tag_bgp_neighbour_ips()

        self.assertEqual(result, {"added": 2, "removed": 0, "total": 2})
        self.assertEqual(tagged(), addresses("10.0.0.2", "10.0.0.3"))

    def test_a_second_run_changes_nothing(self):
        self.peer_on(IPAddress.objects.create(address="10.0.0.2/32"))
        tag_bgp_neighbour_ips()

        self.assertEqual(
            tag_bgp_neighbour_ips(), {"added": 0, "removed": 0, "total": 1}
        )

    def test_an_address_an_interface_carries_is_never_tagged(self):
        interface = Interface.objects.create(
            device=self.device, name="Eth1", type="1000base-t"
        )
        reused = IPAddress.objects.create(
            address="10.1.1.1/32",
            assigned_object_type=ContentType.objects.get_for_model(Interface),
            assigned_object_id=interface.pk,
        )
        self.peer_on(reused)

        self.assertEqual(tag_bgp_neighbour_ips()["total"], 0)
        self.assertEqual(tagged(), set())

    def test_an_address_that_gains_an_interface_loses_the_tag(self):
        neighbour = IPAddress.objects.create(address="10.0.0.2/32")
        self.peer_on(neighbour)
        tag_bgp_neighbour_ips()
        interface = Interface.objects.create(
            device=self.device, name="Eth1", type="1000base-t"
        )
        neighbour.assigned_object = interface
        neighbour.save()

        self.assertEqual(
            tag_bgp_neighbour_ips(), {"added": 0, "removed": 1, "total": 0}
        )
        self.assertEqual(tagged(), set())

    def test_an_address_whose_peer_is_deleted_loses_the_tag(self):
        neighbour = IPAddress.objects.create(address="10.0.0.2/32")
        peer = self.peer_on(neighbour)
        tag_bgp_neighbour_ips()
        peer.delete()

        self.assertEqual(
            tag_bgp_neighbour_ips(), {"added": 0, "removed": 1, "total": 0}
        )

    def test_the_tag_set_equals_the_bundle_count(self):
        for index in range(3):
            self.peer_on(
                IPAddress.objects.create(address=f"10.0.0.{index + 2}/32"),
                vrf=VRF.objects.create(name=f"v{index}"),
            )
        IPAddress.objects.create(address="10.9.9.9/32")

        tag_bgp_neighbour_ips()

        block = _interface_state_bundle_payload()["unassigned_ip_addresses"]
        self.assertEqual(len(tagged()), block["used_by_a_bgp_peer"])

    def test_the_source_address_of_a_peer_is_tagged(self):
        self.peer_on(
            IPAddress.objects.create(address="10.0.0.2/32"),
            source=IPAddress.objects.create(address="10.0.0.9/32"),
        )

        tag_bgp_neighbour_ips()

        self.assertEqual(tagged(), addresses("10.0.0.2", "10.0.0.9"))

    def test_a_run_for_a_user_is_change_logged_to_that_user(self):
        from core.models import ObjectChange

        operator = User.objects.create(username="operator")
        neighbour = IPAddress.objects.create(address="10.0.0.2/32")
        self.peer_on(neighbour)

        tag_bgp_neighbour_ips(user=operator)

        changes = ObjectChange.objects.filter(
            changed_object_type=ContentType.objects.get_for_model(IPAddress),
            changed_object_id=neighbour.pk,
            user=operator,
        )
        self.assertTrue(changes.exists())

    def test_the_command_refuses_an_unknown_user(self):
        from django.core.management.base import CommandError

        with self.assertRaises(CommandError):
            call_command("forward_tag_bgp_neighbours", user="nobody")

    def test_the_command_reports_what_it_did(self):
        self.peer_on(IPAddress.objects.create(address="10.0.0.2/32"))
        out = StringIO()

        User.objects.create(username="operator")

        call_command("forward_tag_bgp_neighbours", user="operator", stdout=out)

        payload = json.loads(out.getvalue())
        self.assertEqual(payload["added"], 1)
        self.assertTrue(payload["netbox_routing_installed"])
        self.assertEqual(payload["tag_slug"], BGP_NEIGHBOUR_TAG_SLUG)


class NoRoutingTest(TestCase):
    def test_without_netbox_routing_nothing_is_written(self):
        from unittest.mock import patch

        with patch(
            "forward_netbox.utilities.bgp_neighbour_ips.routing_installed",
            return_value=False,
        ):
            self.assertIsNone(tag_bgp_neighbour_ips())

        self.assertFalse(Tag.objects.filter(slug=BGP_NEIGHBOUR_TAG_SLUG).exists())
