# The interface lookup that cost a customer four minutes of SQL in one model.
#
# `bulk_orm_apply_macaddress` resolves every row's interface before it can
# classify anything. That lookup used to build one
# `Q(device__name=..., name__in=[...])` branch per device and OR up to 500 of
# them together, joining `dcim_device` by NAME. Postgres cannot use an index
# for that shape, so each chunk scanned the interface table whole - 360,771
# rows on the estate that reported it - once per chunk of 500 pairs.
#
# The reported cost was `dcim.macaddress at 297643 ms in 756 queries, 247134 ms
# of it in SQL`, inside a comparison that took seventeen minutes overall.
#
# Two properties are pinned here. The lookup must issue a number of queries
# that scales with DEVICES rather than with pairs, and it must still return
# exactly the pairs asked for - the name filter is a superset across the chunk,
# so dropping the pair check would silently match an interface on the wrong
# device.
from dcim.models import Device
from dcim.models import DeviceRole
from dcim.models import DeviceType
from dcim.models import Interface
from dcim.models import Manufacturer
from dcim.models import Site
from django.db import connection
from django.test import TestCase
from django.test.utils import CaptureQueriesContext

from forward_netbox.utilities.apply_engine_bulk import _interfaces_by_device_and_name


class InterfaceLookupQueryShapeTest(TestCase):
    @classmethod
    def setUpTestData(cls):
        site = Site.objects.create(name="Q Site", slug="q-site")
        mfr = Manufacturer.objects.create(name="Q Mfr", slug="q-mfr")
        dtype = DeviceType.objects.create(manufacturer=mfr, model="Q DT", slug="q-dt")
        role = DeviceRole.objects.create(name="Q Role", slug="q-role")
        cls.devices = {}
        for index in range(6):
            name = f"q-dev-{index}"
            device = Device.objects.create(
                name=name, site=site, device_type=dtype, role=role, status="active"
            )
            cls.devices[name] = device
            for port in range(4):
                Interface.objects.create(
                    device=device, name=f"Ethernet{port}", type="1000base-t"
                )

    def _pairs(self):
        return {(name, f"Ethernet{port}") for name in self.devices for port in range(4)}

    def test_every_requested_pair_is_returned(self):
        found = _interfaces_by_device_and_name(
            self._pairs(), devices_by_name=self.devices
        )
        self.assertEqual(set(found), self._pairs())

    def test_an_interface_on_the_wrong_device_is_not_returned(self):
        # Every device here has an `Ethernet0`. Asking for one device's must
        # not return another's, which is exactly what a superset name filter
        # would do without the pair check.
        found = _interfaces_by_device_and_name(
            {("q-dev-0", "Ethernet0")}, devices_by_name=self.devices
        )
        self.assertEqual(set(found), {("q-dev-0", "Ethernet0")})
        self.assertEqual(found[("q-dev-0", "Ethernet0")].device.name, "q-dev-0")

    def test_a_name_that_does_not_exist_is_simply_absent(self):
        found = _interfaces_by_device_and_name(
            {("q-dev-0", "Ethernet99")}, devices_by_name=self.devices
        )
        self.assertEqual(found, {})

    def test_a_device_outside_the_resolved_set_is_skipped(self):
        found = _interfaces_by_device_and_name(
            {("q-dev-0", "Ethernet0"), ("unknown-dev", "Ethernet0")},
            devices_by_name=self.devices,
        )
        self.assertEqual(set(found), {("q-dev-0", "Ethernet0")})

    def test_the_query_count_scales_with_devices_not_pairs(self):
        # Twenty-four pairs across six devices, all inside one device chunk.
        # The old shape issued one query per chunk of 500 PAIRS, each an
        # OR-tree; this must be a single indexed query.
        with CaptureQueriesContext(connection) as captured:
            _interfaces_by_device_and_name(self._pairs(), devices_by_name=self.devices)
        self.assertEqual(len(captured.captured_queries), 1)

    def test_the_query_filters_on_device_id_rather_than_matching_a_name(self):
        with CaptureQueriesContext(connection) as captured:
            _interfaces_by_device_and_name(self._pairs(), devices_by_name=self.devices)
        sql = captured.captured_queries[0]["sql"]
        where = sql.split(" WHERE ", 1)[1]
        # The predicate is what decides whether an index can be used. A device
        # NAME in it is the old OR-tree shape, which could not be indexed.
        self.assertIn('"dcim_interface"."device_id" IN', where)
        self.assertNotIn('"dcim_device"."name"', where)
        self.assertNotIn(" OR ", where)

    def test_the_result_is_not_sorted_for_a_dict_that_discards_order(self):
        # NetBox orders Interface by device name then naturalized name under a
        # collation. Nothing here reads the order, and the sort is charged per
        # chunk on the full result set.
        with CaptureQueriesContext(connection) as captured:
            _interfaces_by_device_and_name(self._pairs(), devices_by_name=self.devices)
        self.assertNotIn("ORDER BY", captured.captured_queries[0]["sql"])

    def test_no_pairs_asks_nothing(self):
        with CaptureQueriesContext(connection) as captured:
            self.assertEqual(
                _interfaces_by_device_and_name(set(), devices_by_name={}), {}
            )
        self.assertEqual(len(captured.captured_queries), 0)
