from dcim.models import Device
from dcim.models import DeviceRole
from dcim.models import DeviceType
from dcim.models import Interface
from dcim.models import Manufacturer
from dcim.models import Site
from django.test import TestCase
from ipam.models import IPAddress

from forward_netbox.utilities.primary_ip import _branch_interface_ips


class PrimaryIpDuplicateNamesTest(TestCase):
    """A name shared by two devices must not silently pick the wrong copy."""

    def setUp(self):
        manufacturer = Manufacturer.objects.create(name="Acme", slug="acme")
        self.device_type = DeviceType.objects.create(
            manufacturer=manufacturer, model="Model X", slug="model-x"
        )
        self.role = DeviceRole.objects.create(name="Router", slug="router")
        self.old_site = Site.objects.create(name="Old", slug="old")
        self.new_site = Site.objects.create(name="New", slug="new")

    def _device(self, name, site):
        return Device.objects.create(
            name=name,
            device_type=self.device_type,
            role=self.role,
            site=site,
            status="active",
        )

    def _address(self, device, interface_name, address):
        interface = Interface.objects.create(
            device=device, name=interface_name, type="virtual"
        )
        return IPAddress.objects.create(address=address, assigned_object=interface)

    def test_a_unique_name_resolves_as_before(self):
        device = self._device("r1", self.old_site)
        self._address(device, "Vlan211", "10.0.211.2/24")

        devices, interface_ips, lookup = _branch_interface_ips(["r1"])

        self.assertEqual(devices["r1"].pk, device.pk)
        self.assertEqual(interface_ips["r1"], {"Vlan211": ["10.0.211.2/24"]})
        self.assertIn(("r1", "Vlan211", "10.0.211.2/24"), lookup)

    def test_the_copy_that_holds_the_addresses_is_chosen(self):
        older = self._device("r1", self.old_site)
        newer = self._device("r1", self.new_site)
        self._address(newer, "Vlan211", "10.0.211.2/24")
        Interface.objects.create(device=older, name="Vlan211", type="virtual")

        devices, interface_ips, _lookup = _branch_interface_ips(["r1"])

        self.assertEqual(devices["r1"].pk, newer.pk)
        self.assertEqual(interface_ips["r1"], {"Vlan211": ["10.0.211.2/24"]})

    def test_the_choice_does_not_depend_on_which_copy_is_older(self):
        newer_first = self._device("r1", self.new_site)
        older_second = self._device("r1", self.old_site)
        self._address(older_second, "Vlan211", "10.0.211.2/24")

        devices, _interface_ips, _lookup = _branch_interface_ips(["r1"])

        self.assertEqual(devices["r1"].pk, older_second.pk)
        self.assertNotEqual(devices["r1"].pk, newer_first.pk)

    def test_two_copies_with_addresses_stay_unresolved(self):
        first = self._device("r1", self.old_site)
        second = self._device("r1", self.new_site)
        self._address(first, "Vlan211", "10.0.211.2/24")
        self._address(second, "Vlan211", "10.0.211.3/24")

        devices, interface_ips, lookup = _branch_interface_ips(["r1"])

        self.assertNotIn("r1", devices)
        self.assertNotIn("r1", interface_ips)
        self.assertEqual(lookup, {})

    def test_two_copies_with_no_addresses_stay_unresolved(self):
        self._device("r1", self.old_site)
        self._device("r1", self.new_site)

        devices, _interface_ips, _lookup = _branch_interface_ips(["r1"])

        self.assertNotIn("r1", devices)

    def test_an_unrelated_name_is_unaffected_by_a_duplicate(self):
        self._device("dup", self.old_site)
        self._device("dup", self.new_site)
        solo = self._device("solo", self.old_site)
        self._address(solo, "Lo0", "10.9.9.9/32")

        devices, interface_ips, _lookup = _branch_interface_ips(["dup", "solo"])

        self.assertEqual(set(devices), {"solo"})
        self.assertEqual(interface_ips["solo"], {"Lo0": ["10.9.9.9/32"]})
