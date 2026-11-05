# Forward records some devices' management address on none of their interfaces
# (a Palo Alto's management port, a FortiGate modeled with no interfaces, a vdom
# with no mgmt interface). NetBox sets a primary IP only from an address on one of
# the device's interfaces. With the opt-in parameter on, such a device gets a
# tagged, management-only interface carrying the address as its primary IP.
# Real branch, synthetic data.
from unittest.mock import Mock
from unittest.mock import patch

from dcim.models import Device
from dcim.models import DeviceRole
from dcim.models import DeviceType
from dcim.models import Interface
from dcim.models import Manufacturer
from dcim.models import Site
from django.contrib.auth import get_user_model
from django.test import TransactionTestCase
from ipam.models import IPAddress
from netbox_branching.utilities import activate_branch

from forward_netbox.models import ForwardSource
from forward_netbox.models import ForwardSync
from forward_netbox.tests.test_primary_ip_integration import provision_branch
from forward_netbox.utilities.context_ip_copies import CONTEXT_COPY_TAG_SLUG
from forward_netbox.utilities.management_interfaces import (
    MANAGEMENT_INTERFACE_TAG_SLUG,
)
from forward_netbox.utilities.primary_ip import apply_primary_ip_from_mgmt_tags
from forward_netbox.utilities.primary_ip import parse_device_reason_lines

ADDRESS = "10.66.0.9"


class ManagementInterfaceTest(TransactionTestCase):
    def setUp(self):
        self.user = get_user_model().objects.create_user(username="mgmt-if-user")
        maker = Manufacturer.objects.create(name="Acme", slug="acme")
        self.kind = DeviceType.objects.create(manufacturer=maker, model="FW", slug="fw")
        self.role = DeviceRole.objects.create(name="Firewall", slug="firewall")
        self.site = Site.objects.create(name="Site 1", slug="site-1")
        self.fw = self._device("fw-1")
        Interface.objects.create(device=self.fw, name="ethernet1/1", type="1000base-t")
        self.source = ForwardSource.objects.create(
            name="mgmt-if-source",
            type="saas",
            url="https://fwd.app",
            parameters={"network_id": "net-1"},
        )
        self.sync = ForwardSync.objects.create(
            name="mgmt-if-sync",
            source=self.source,
            parameters={
                "snapshot_id": "snap-1",
                "set_primary_ip_from_forward_management_ip": True,
                "add_management_interface": True,
            },
        )

    def _device(self, name):
        return Device.objects.create(
            name=name,
            device_type=self.kind,
            role=self.role,
            site=self.site,
            status="active",
        )

    def _set(self, **parameters):
        self.sync.parameters = {**self.sync.parameters, **parameters}
        self.sync.save()

    def _run(self, management_ips, branch=None, name="Mgmt If Branch"):
        branch = branch or provision_branch(user=self.user, name=name)
        patcher = patch(
            "forward_netbox.utilities.primary_ip.get_device_management_ips",
            return_value=management_ips,
        )
        patcher.start()
        self.addCleanup(patcher.stop)
        executor = Mock(sync=self.sync, client=Mock(), user=self.user, logger=Mock())
        apply_primary_ip_from_mgmt_tags(executor, branch, snapshot_id="snap-1")
        logged = [c.args[0] for c in executor.logger.log_info.call_args_list]
        return branch, parse_device_reason_lines(logged)

    def _added(self, device):
        return Interface.objects.filter(
            device=device, tags__slug=MANAGEMENT_INTERFACE_TAG_SLUG
        )

    def test_the_device_gets_a_tagged_management_interface_carrying_its_primary_ip(
        self,
    ):
        branch, _reasons = self._run({"fw-1": [ADDRESS]})

        with activate_branch(branch):
            fresh = Device.objects.get(pk=self.fw.pk)
            primary = IPAddress.objects.get(pk=fresh.primary_ip4_id)
            self.assertEqual(str(primary.address), f"{ADDRESS}/32")
            interface = primary.assigned_object
            self.assertEqual(interface.device_id, self.fw.pk)
            self.assertEqual(interface.name, "mgmt")
            self.assertEqual(interface.type, "virtual")
            self.assertTrue(interface.mgmt_only)
            self.assertTrue(
                interface.tags.filter(slug=MANAGEMENT_INTERFACE_TAG_SLUG).exists()
            )
        # Staged in the branch, not written to main.
        self.assertFalse(self._added(self.fw).exists())
        self.assertIsNone(Device.objects.get(pk=self.fw.pk).primary_ip4_id)

    def test_off_by_default_nothing_is_added_and_the_reason_stays(self):
        self._set(add_management_interface=False)

        branch, reasons = self._run({"fw-1": [ADDRESS]})

        with activate_branch(branch):
            self.assertFalse(self._added(self.fw).exists())
            self.assertIsNone(Device.objects.get(pk=self.fw.pk).primary_ip4_id)
        self.assertEqual(reasons[self.fw.pk][0], "no-interface")

    def test_a_rerun_adds_nothing_more(self):
        branch, _reasons = self._run({"fw-1": [ADDRESS]})
        with activate_branch(branch):
            first = Device.objects.get(pk=self.fw.pk).primary_ip4_id

        self._run({"fw-1": [ADDRESS]}, branch=branch)

        with activate_branch(branch):
            self.assertEqual(self._added(self.fw).count(), 1)
            self.assertEqual(
                IPAddress.objects.filter(address__net_host=ADDRESS).count(), 1
            )
            self.assertEqual(Device.objects.get(pk=self.fw.pk).primary_ip4_id, first)

    def test_an_interface_named_mgmt_the_sync_did_not_add_is_never_used(self):
        Interface.objects.create(device=self.fw, name="mgmt", type="1000base-t")

        branch, reasons = self._run({"fw-1": [ADDRESS]})

        with activate_branch(branch):
            self.assertFalse(IPAddress.objects.filter(address__net_host=ADDRESS))
            self.assertIsNone(Device.objects.get(pk=self.fw.pk).primary_ip4_id)
        self.assertEqual(reasons[self.fw.pk][0], "management-interface-name-taken")

    def test_an_address_netbox_holds_unassigned_is_not_duplicated(self):
        IPAddress.objects.create(address=f"{ADDRESS}/24")

        branch, reasons = self._run({"fw-1": [ADDRESS]})

        with activate_branch(branch):
            self.assertEqual(
                IPAddress.objects.filter(address__net_host=ADDRESS).count(), 1
            )
            self.assertFalse(self._added(self.fw).exists())
        self.assertEqual(
            reasons[self.fw.pk][0], "management-interface-address-elsewhere"
        )

    def test_two_devices_naming_one_address_both_get_a_primary_ip(self):
        # A chassis and its context, both modeled with no interface carrying the
        # address: the first gets the row, the second a copy on its own interface.
        self._set(give_contexts_their_own_management_ip=True)
        ctx = self._device("fw-1-ctx")

        branch, _reasons = self._run({"fw-1": [ADDRESS], "fw-1-ctx": [ADDRESS]})

        with activate_branch(branch):
            for device in (self.fw, ctx):
                self.assertIsNotNone(Device.objects.get(pk=device.pk).primary_ip4_id)
            copy = IPAddress.objects.get(
                pk=Device.objects.get(pk=ctx.pk).primary_ip4_id
            )
            self.assertTrue(copy.tags.filter(slug=CONTEXT_COPY_TAG_SLUG).exists())
            self.assertEqual(copy.assigned_object.device_id, ctx.pk)

    def test_without_copies_the_second_device_reads_as_sharing_the_address(self):
        ctx = self._device("fw-1-ctx")

        branch, reasons = self._run({"fw-1": [ADDRESS], "fw-1-ctx": [ADDRESS]})

        with activate_branch(branch):
            self.assertIsNotNone(Device.objects.get(pk=self.fw.pk).primary_ip4_id)
            self.assertIsNone(Device.objects.get(pk=ctx.pk).primary_ip4_id)
        self.assertEqual(reasons[ctx.pk], ("shared", self.fw.pk))

    def test_a_context_missing_the_chassis_interface_gets_one_named_like_it(self):
        # A vdom: the chassis holds the address on an interface the context lacks.
        self._set(give_contexts_their_own_management_ip=True)
        held_on = Interface.objects.create(
            device=self.fw, name="port-mgmt", type="1000base-t"
        )
        original = IPAddress.objects.create(
            address=f"{ADDRESS}/24", assigned_object=held_on
        )
        ctx = self._device("fw-1-vdom")
        Interface.objects.create(device=ctx, name="port9", type="1000base-t")

        branch, _reasons = self._run({"fw-1": [ADDRESS], "fw-1-vdom": [ADDRESS]})

        with activate_branch(branch):
            copy = IPAddress.objects.get(
                pk=Device.objects.get(pk=ctx.pk).primary_ip4_id
            )
            self.assertNotEqual(copy.pk, original.pk)
            self.assertEqual(copy.assigned_object.name, "port-mgmt")
            self.assertTrue(
                copy.assigned_object.tags.filter(
                    slug=MANAGEMENT_INTERFACE_TAG_SLUG
                ).exists()
            )
            self.assertEqual(
                Device.objects.get(pk=self.fw.pk).primary_ip4_id, original.pk
            )

    def test_without_the_setting_the_context_keeps_its_reason(self):
        self._set(
            give_contexts_their_own_management_ip=True, add_management_interface=False
        )
        held_on = Interface.objects.create(
            device=self.fw, name="port-mgmt", type="1000base-t"
        )
        IPAddress.objects.create(address=f"{ADDRESS}/24", assigned_object=held_on)
        ctx = self._device("fw-1-vdom")

        branch, reasons = self._run({"fw-1": [ADDRESS], "fw-1-vdom": [ADDRESS]})

        with activate_branch(branch):
            self.assertFalse(self._added(ctx).exists())
        self.assertEqual(reasons[ctx.pk][0], "context-copy-no-matching-interface")

    def test_a_primary_ip_set_by_hand_is_kept(self):
        own = Interface.objects.get(device=self.fw, name="ethernet1/1")
        by_hand = IPAddress.objects.create(address="10.77.0.1/24", assigned_object=own)
        Device.objects.filter(pk=self.fw.pk).update(primary_ip4=by_hand)

        branch, _reasons = self._run({"fw-1": [ADDRESS]})

        with activate_branch(branch):
            self.assertEqual(
                Device.objects.get(pk=self.fw.pk).primary_ip4_id, by_hand.pk
            )
            self.assertFalse(self._added(self.fw).exists())

    def test_a_forward_name_with_no_netbox_device_is_not_counted_as_left(self):
        self._set(add_management_interface=False)
        branch = provision_branch(user=self.user, name="Count Branch")
        patcher = patch(
            "forward_netbox.utilities.primary_ip.get_device_management_ips",
            return_value={"fw-1": [ADDRESS], "not-in-netbox": ["10.66.0.10"]},
        )
        patcher.start()
        self.addCleanup(patcher.stop)
        executor = Mock(sync=self.sync, client=Mock(), user=self.user, logger=Mock())

        apply_primary_ip_from_mgmt_tags(executor, branch, snapshot_id="snap-1")

        logged = [c.args[0] for c in executor.logger.log_info.call_args_list]
        summary = next(line for line in logged if "left without a primary IP" in line)
        self.assertTrue(summary.split(":", 1)[1].strip().startswith("1 device(s)"))

    def test_a_failed_address_leaves_no_interface_behind(self):
        with patch(
            "ipam.models.IPAddress.full_clean", side_effect=ValueError("refused")
        ):
            branch, reasons = self._run({"fw-1": [ADDRESS]})

        with activate_branch(branch):
            self.assertFalse(self._added(self.fw).exists())
            self.assertIsNone(Device.objects.get(pk=self.fw.pk).primary_ip4_id)
        self.assertEqual(reasons[self.fw.pk][0], "management-interface-failed")

    def test_the_added_rows_are_branch_changes_the_merge_carries(self):
        from django.contrib.contenttypes.models import ContentType

        branch, _reasons = self._run({"fw-1": [ADDRESS]})

        changes = branch.get_unmerged_changes()
        for model in (Interface, IPAddress):
            created = changes.filter(
                changed_object_type=ContentType.objects.get_for_model(model),
                action="create",
            )
            self.assertEqual(created.count(), 1, model.__name__)
        pointed = [
            change
            for change in changes.filter(
                changed_object_type=ContentType.objects.get_for_model(Device),
                changed_object_id=self.fw.pk,
                action="update",
            )
            if (change.postchange_data or {}).get("primary_ip4")
        ]
        self.assertEqual(len(pointed), 1)

    def test_a_context_rerun_adds_nothing_more(self):
        self._set(give_contexts_their_own_management_ip=True)
        held_on = Interface.objects.create(
            device=self.fw, name="port-mgmt", type="1000base-t"
        )
        IPAddress.objects.create(address=f"{ADDRESS}/24", assigned_object=held_on)
        ctx = self._device("fw-1-vdom")
        ips = {"fw-1": [ADDRESS], "fw-1-vdom": [ADDRESS]}
        branch, _reasons = self._run(ips)

        self._run(ips, branch=branch)

        with activate_branch(branch):
            self.assertEqual(self._added(ctx).count(), 1)
            self.assertEqual(
                IPAddress.objects.filter(address__net_host=ADDRESS).count(), 2
            )
