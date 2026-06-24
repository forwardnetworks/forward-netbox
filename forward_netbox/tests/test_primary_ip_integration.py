# Integration test for the in-branch primary-IP step (Mgmt_<iface> feature).
#
# Provisions a real netbox_branching branch, runs apply_primary_ip_from_mgmt_tags
# against a device whose Vlan211 interface carries an IP, and proves the resolved
# primary_ip4 stages in the branch and merges into main.
import logging
from unittest.mock import Mock

from dcim.models import Device
from dcim.models import DeviceRole
from dcim.models import DeviceType
from dcim.models import Interface
from dcim.models import Manufacturer
from dcim.models import Site
from django.contrib.auth import get_user_model
from django.contrib.contenttypes.models import ContentType
from django.test import RequestFactory
from django.test import TransactionTestCase
from django.urls import reverse
from ipam.models import IPAddress
from netbox_branching.models import Branch
from netbox_branching.utilities import activate_branch

from forward_netbox.models import ForwardSource
from forward_netbox.models import ForwardSync
from forward_netbox.utilities.primary_ip import apply_primary_ip_from_mgmt_tags


def provision_branch(*, user, name="Primary IP Branch"):
    branch = Branch(name=name)
    branch.save(provision=False)
    branch.provision(user=user)
    branch.refresh_from_db()
    return branch


class PrimaryIpFromMgmtTagIntegrationTest(TransactionTestCase):
    def setUp(self):
        self.user = get_user_model().objects.create_user(username="primary-ip-user")
        self.request = RequestFactory().get(reverse("home"))
        self.request.user = self.user
        self.logger = logging.getLogger("forward_netbox.tests.primary_ip")

        manufacturer = Manufacturer.objects.create(name="Acme", slug="acme")
        device_type = DeviceType.objects.create(
            manufacturer=manufacturer, model="Model X", slug="model-x"
        )
        role = DeviceRole.objects.create(name="Router", slug="router")
        site = Site.objects.create(name="Site 1", slug="site-1")
        self.device = Device.objects.create(
            name="r1",
            device_type=device_type,
            role=role,
            site=site,
            status="active",
        )
        self.interface = Interface.objects.create(
            device=self.device, name="Vlan211", type="virtual"
        )
        self.ip = IPAddress.objects.create(
            address="10.0.211.2/24",
            assigned_object=self.interface,
        )

        self.source = ForwardSource.objects.create(
            name="primary-ip-source",
            type="saas",
            url="https://fwd.app",
            parameters={"network_id": "net-1"},
        )
        self.sync = ForwardSync.objects.create(
            name="primary-ip-sync",
            source=self.source,
            parameters={
                "snapshot_id": "snap-1",
                "dcim.device": True,
                "set_primary_ip_from_mgmt_tag": True,
            },
        )

    def _executor(self, mgmt_tags):
        client = Mock()
        client.get_device_mgmt_tags.return_value = mgmt_tags
        return Mock(
            sync=self.sync,
            client=client,
            user=self.user,
            logger=Mock(),
        )

    def test_sets_primary_ip_and_merges_into_main(self):
        # Device starts with no primary IP.
        self.assertIsNone(self.device.primary_ip4_id)

        branch = provision_branch(user=self.user)
        executor = self._executor({"r1": ["Mgmt_Vl211"]})

        updated = apply_primary_ip_from_mgmt_tags(
            executor, branch, snapshot_id="snap-1"
        )
        self.assertEqual(updated, 1)
        executor.client.get_device_mgmt_tags.assert_called_once()

        # Staged in the branch, not yet in main.
        with activate_branch(branch):
            branched = Device.objects.get(pk=self.device.pk)
            self.assertEqual(branched.primary_ip4_id, self.ip.pk)
        self.device.refresh_from_db()
        self.assertIsNone(self.device.primary_ip4_id)

        # A device UPDATE ObjectChange was recorded in the branch carrying the
        # new primary_ip4 — i.e. the change is merge-eligible (the merge itself is
        # netbox_branching/bulk_merge's separately-tested concern).
        device_ct = ContentType.objects.get_for_model(Device)
        ocs = list(
            branch.get_unmerged_changes().filter(
                changed_object_type=device_ct,
                changed_object_id=self.device.pk,
            )
        )
        self.assertTrue(ocs, "no device ObjectChange recorded for primary_ip")
        update_ocs = [c for c in ocs if c.action == "update"]
        self.assertTrue(update_ocs, "device change was not an update")
        self.assertEqual(update_ocs[-1].postchange_data.get("primary_ip4"), self.ip.pk)

    def test_no_mgmt_tag_is_a_noop(self):
        branch = provision_branch(user=self.user, name="Primary IP NoOp")
        executor = self._executor({})
        updated = apply_primary_ip_from_mgmt_tags(
            executor, branch, snapshot_id="snap-1"
        )
        self.assertEqual(updated, 0)
        self.device.refresh_from_db()
        self.assertIsNone(self.device.primary_ip4_id)
