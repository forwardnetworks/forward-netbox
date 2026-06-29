from dcim.models import Device
from dcim.models import DeviceRole
from dcim.models import DeviceType
from dcim.models import Manufacturer
from dcim.models import Site
from django.test import TestCase

from forward_netbox.utilities.vsys_parent import link_vsys_parents
from forward_netbox.utilities.vsys_parent import PARENT_DEVICE_CF


class VsysParentTest(TestCase):
    def setUp(self):
        mfr = Manufacturer.objects.create(name="Palo Alto", slug="palo-alto")
        self.dt = DeviceType.objects.create(
            manufacturer=mfr, model="PA-5250", slug="pa-5250"
        )
        self.role = DeviceRole.objects.create(name="Firewall", slug="firewall")
        self.site = Site.objects.create(name="DC1", slug="dc1")

    def _device(self, name):
        return Device.objects.create(
            name=name, device_type=self.dt, role=self.role, site=self.site
        )

    def _rows(self, pairs):
        return lambda sync, client: [{"name": n, "parent": p} for n, p in pairs]

    def test_links_virtual_device_to_parent(self):
        chassis = self._device("fw-chassis-a")
        vsys = self._device("fw-chassis-a_vsys3_APP")
        result = link_vsys_parents(
            None, fetch_rows=self._rows([(vsys.name, chassis.name)])
        )
        self.assertEqual(result["linked"], 1)
        self.assertEqual(result["orphan_parent"], 0)
        vsys.refresh_from_db()
        chassis.refresh_from_db()
        self.assertEqual(vsys.custom_field_data.get(PARENT_DEVICE_CF), chassis.pk)
        # The physical chassis itself is never linked.
        self.assertIn(chassis.custom_field_data.get(PARENT_DEVICE_CF), (None, ""))

    def test_orphan_parent_not_linked(self):
        # vsys present in NetBox, but its physical chassis is not collected.
        vsys = self._device("fw-chassis-c_vsys1")
        result = link_vsys_parents(
            None, fetch_rows=self._rows([(vsys.name, "fw-chassis-c")])
        )
        self.assertEqual(result["linked"], 0)
        self.assertEqual(result["orphan_parent"], 1)
        vsys.refresh_from_db()
        self.assertIn(vsys.custom_field_data.get(PARENT_DEVICE_CF), (None, ""))

    def test_idempotent(self):
        chassis = self._device("fw-a")
        vsys = self._device("fw-a_vsys1")
        rows = self._rows([(vsys.name, chassis.name)])
        first = link_vsys_parents(None, fetch_rows=rows)
        second = link_vsys_parents(None, fetch_rows=rows)
        self.assertEqual(first["linked"], 1)
        self.assertEqual(second["linked"], 0)
        self.assertEqual(second["already"], 1)

    def test_self_heal_clears_stale_link(self):
        chassis = self._device("fw-b")
        vsys = self._device("fw-b_vsys1")
        link_vsys_parents(None, fetch_rows=self._rows([(vsys.name, chassis.name)]))
        vsys.refresh_from_db()
        self.assertEqual(vsys.custom_field_data.get(PARENT_DEVICE_CF), chassis.pk)
        # Next run: it is no longer reported as a virtual context → link cleared.
        result = link_vsys_parents(None, fetch_rows=self._rows([]))
        self.assertEqual(result["cleared"], 1)
        vsys.refresh_from_db()
        self.assertIsNone(vsys.custom_field_data.get(PARENT_DEVICE_CF))
