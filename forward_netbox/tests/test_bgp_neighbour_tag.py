import json
import unittest
from io import StringIO

from dcim.models import Interface
from django.contrib.auth import get_user_model
from django.contrib.contenttypes.models import ContentType
from django.core.management import call_command
from django.test import TestCase
from django.urls import reverse
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


@unittest.skipUnless(NETBOX_ROUTING_INSTALLED, "netbox-routing is not installed")
class TagButtonTest(BgpPeerFixture, TestCase):
    """The tagging is a button an operator can click, not a shell command."""

    def setUp(self):
        super().setUp()
        from forward_netbox.models import ForwardSource
        from forward_netbox.models import ForwardSync

        source = ForwardSource.objects.create(
            name="btn-bgp-src",
            type="saas",
            url="https://fwd.app",
            status="ready",
            parameters={
                "username": "u@example.com",
                "password": "p",
                "verify": True,
                "network_id": "net-1",
            },
        )
        self.sync = ForwardSync.objects.create(
            name="btn-bgp-sync",
            source=source,
            parameters={"snapshot_id": "latestProcessed"},
        )
        self.peer_on(IPAddress.objects.create(address="10.0.0.2/32"))
        self.url = reverse(
            "plugins:forward_netbox:forwardsync_tag_bgp_neighbour_ips",
            args=[self.sync.pk],
        )

    def operator(self, permitted=True):
        from forward_netbox.tests.test_change_control_comments import grant
        from forward_netbox.models import ForwardSync

        user = User.objects.create(username=f"op-{permitted}")
        grant(user, ForwardSync, "view")
        if permitted:
            # Both: the click changes addresses, and NetBox reaches the sync by
            # the same action.
            grant(user, ForwardSync, "change")
            grant(user, IPAddress, "change")
        self.client.force_login(user)
        return user

    def test_the_job_tags_the_addresses_as_the_user_who_clicked(self):
        from core.models import Job
        from core.choices import JobStatusChoices
        from django.contrib.contenttypes.models import ContentType

        from forward_netbox.jobs import _tag_bgp_neighbour_ips_work
        from forward_netbox.models import ForwardSync

        user = User.objects.create(username="clicker")
        job = Job.objects.create(
            object_type=ContentType.objects.get_for_model(ForwardSync),
            object_id=self.sync.pk,
            name="btn-bgp-sync - tag BGP neighbour addresses",
            status=JobStatusChoices.STATUS_RUNNING,
            user=user,
            job_id="123e4567-e89b-12d3-a456-426614179901",
        )

        _tag_bgp_neighbour_ips_work(job)

        job.refresh_from_db()
        self.assertEqual(job.data["added"], 1)
        self.assertEqual(tagged(), addresses("10.0.0.2"))
        from core.models import ObjectChange

        self.assertTrue(
            ObjectChange.objects.filter(
                changed_object_type=ContentType.objects.get_for_model(IPAddress),
                user=user,
            ).exists()
        )

    def test_posting_queues_the_job_for_someone_who_may_change_addresses(self):
        from unittest import mock

        user = self.operator()

        with mock.patch(
            "forward_netbox.jobs.enqueue_forward_job", return_value=mock.Mock(pk=7)
        ) as enqueue:
            response = self.client.post(self.url)

        self.assertEqual(response.status_code, 302)
        enqueue.assert_called_once()
        # The job runs as, and change-logs to, the person who clicked.
        self.assertEqual(enqueue.call_args.kwargs["user"], user)
        self.assertEqual(tagged(), set())  # nothing is written by the request itself

    def test_a_user_who_may_only_change_addresses_is_not_offered_the_button(self):
        from forward_netbox.tests.test_change_control_comments import grant
        from forward_netbox.models import ForwardSync

        user = User.objects.create(username="addresses-only")
        grant(user, ForwardSync, "view")
        grant(user, IPAddress, "change")
        self.client.force_login(user)
        health = reverse(
            "plugins:forward_netbox:forwardsync_health", args=[self.sync.pk]
        )

        response = self.client.get(health)

        self.assertNotContains(response, f'<form method="post" action="{self.url}"')

    def test_posting_without_the_permission_queues_nothing(self):
        from unittest import mock

        self.operator(permitted=False)

        with mock.patch("forward_netbox.jobs.enqueue_forward_job") as enqueue:
            response = self.client.post(self.url)

        self.assertEqual(response.status_code, 403)
        enqueue.assert_not_called()

    def test_the_health_row_offers_a_button_not_a_command(self):
        from forward_netbox.utilities.explained_findings import explained_findings

        row = [
            r
            for r in explained_findings(self.sync)
            if r["finding"].startswith("IP addresses assigned to nothing")
        ][0]

        self.assertEqual(row["post_url"], self.url)
        self.assertTrue(row["post_label"])
        self.assertNotIn("manage.py", row["action"])

    def test_the_button_shows_only_to_someone_who_can_use_it(self):
        health = reverse(
            "plugins:forward_netbox:forwardsync_health", args=[self.sync.pk]
        )

        self.operator(permitted=True)
        shown = self.client.get(health)
        self.operator(permitted=False)
        hidden = self.client.get(health)

        # The label also sits in the page's embedded health JSON, so look for the
        # form itself.
        form = f'<form method="post" action="{self.url}"'
        self.assertContains(shown, form)
        self.assertNotContains(hidden, form)
