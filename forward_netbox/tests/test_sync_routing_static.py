# Static routes: parsing a configured line, and the shared-route semantics of
# the adapter (one object per VRF/prefix/next hop, every configuring device
# attached, attributes from the device sorting first, deletion only of the
# sync's own route once its last device leaves).
from unittest import TestCase as PlainTestCase
from unittest.mock import Mock

from dcim.models import Device
from dcim.models import DeviceRole
from dcim.models import DeviceType
from dcim.models import Manufacturer
from dcim.models import Site
from django.apps import apps
from django.db import connection
from django.test import TestCase
from django.test.utils import CaptureQueriesContext

from forward_netbox.models import ForwardSource
from forward_netbox.models import ForwardSync
from forward_netbox.utilities.sync import ForwardSyncRunner
from forward_netbox.utilities.sync_reporting import STATIC_ROUTE_UNREADABLE_REASON
from forward_netbox.utilities.sync_routing_static import (
    apply_netbox_routing_staticroute,
)
from forward_netbox.utilities.sync_routing_static import (
    delete_netbox_routing_staticroute,
)
from forward_netbox.utilities.sync_routing_static import parse_static_route_args
from forward_netbox.utilities.sync_routing_static import STATIC_ROUTE_MARKER
from forward_netbox.utilities.sync_routing_static import STATIC_ROUTE_MODEL
from forward_netbox.utilities.sync_routing_static import UnreadableStaticRoute


class ParseStaticRouteTest(PlainTestCase):
    def _parse(self, args, family="ip"):
        return parse_static_route_args(family, args)

    def test_ios_address_and_netmask_with_a_next_hop(self):
        parsed = self._parse("10.0.0.0 255.255.255.0 10.1.1.1")

        self.assertEqual(parsed["prefix"], "10.0.0.0/24")
        self.assertEqual(parsed["next_hop"], "10.1.1.1")
        self.assertIsNone(parsed["interface"])
        self.assertIsNone(parsed["distance"])

    def test_ios_distance_and_tag_after_the_next_hop(self):
        parsed = self._parse("0.0.0.0 0.0.0.0 7.130.96.3 220 tag 500")

        self.assertEqual(parsed["prefix"], "0.0.0.0/0")
        self.assertEqual(parsed["distance"], 220)
        self.assertEqual(parsed["tag"], 500)

    def test_track_is_ignored_and_does_not_become_a_distance(self):
        parsed = self._parse("10.0.0.0 255.0.0.0 10.1.1.1 10 track 600")

        self.assertEqual(parsed["distance"], 10)
        self.assertIsNone(parsed["tag"])

    def test_an_interface_with_a_next_hop_keeps_only_the_next_hop(self):
        parsed = self._parse("10.0.0.0 255.0.0.0 GigabitEthernet0/0 10.1.1.1 5")

        self.assertEqual(parsed["next_hop"], "10.1.1.1")
        self.assertIsNone(parsed["interface"])
        self.assertEqual(parsed["distance"], 5)

    def test_an_interface_alone_is_the_interface_next_hop(self):
        parsed = self._parse("7.6.0.0/15 Null0")

        self.assertIsNone(parsed["next_hop"])
        self.assertEqual(parsed["interface"], "Null0")
        self.assertEqual(parsed["prefix"], "7.6.0.0/15")

    def test_nxos_name_tag_and_distance_in_any_order(self):
        parsed = self._parse("10.0.0.0/8 10.1.1.1 name TO-CORE tag 100 200")

        self.assertEqual(parsed["name"], "TO-CORE")
        self.assertEqual(parsed["tag"], 100)
        self.assertEqual(parsed["distance"], 200)

    def test_permanent_is_recognised(self):
        self.assertTrue(
            self._parse("10.0.0.0 255.0.0.0 10.1.1.1 permanent")["permanent"]
        )
        self.assertFalse(self._parse("10.0.0.0/8 10.1.1.1")["permanent"])

    def test_host_bits_in_the_destination_are_normalised(self):
        self.assertEqual(self._parse("10.0.0.1/24 10.1.1.1")["prefix"], "10.0.0.0/24")

    def test_ipv6(self):
        parsed = self._parse("2001:db8::/32 2001:db8:1::1", family="ipv6")

        self.assertEqual(parsed["prefix"], "2001:db8::/32")
        self.assertEqual(parsed["next_hop"], "2001:db8:1::1")

    def test_a_distance_above_255_is_dropped_not_stored(self):
        self.assertIsNone(self._parse("10.0.0.0/8 10.1.1.1 300")["distance"])

    def test_a_long_name_is_cut_to_the_models_limit(self):
        parsed = self._parse(f"10.0.0.0/8 10.1.1.1 name {'x' * 80}")

        self.assertEqual(len(parsed["name"]), 50)

    def test_lines_that_cannot_be_read_raise_with_a_reason(self):
        for args, reason in (
            ("", "empty"),
            ("garbage", "destination"),
            ("10.0.0.0/8", "no_next_hop"),
            ("10.0.0.0 255.0.0.0", "no_next_hop"),
            ("999.0.0.0/8 10.1.1.1", "destination"),
        ):
            with self.assertRaises(UnreadableStaticRoute) as caught:
                self._parse(args)
            self.assertEqual(caught.exception.reason, reason, args)

    def test_a_next_hop_of_the_wrong_family_is_refused(self):
        with self.assertRaises(UnreadableStaticRoute) as caught:
            self._parse("10.0.0.0/8 2001:db8::1")

        self.assertEqual(caught.exception.reason, "family_mismatch")


class StaticRouteAdapterTest(TestCase):
    def setUp(self):
        if not apps.is_installed("netbox_routing"):
            self.skipTest("netbox-routing optional plugin is not installed")
        source = ForwardSource.objects.create(
            name="sr-source",
            type="saas",
            url="https://fwd.app",
            status="ready",
            parameters={"network_id": "net-1"},
        )
        self.sync = ForwardSync.objects.create(
            name="sr-sync", source=source, parameters={"snapshot_id": "latestProcessed"}
        )
        site = Site.objects.create(name="SR Site", slug="sr-site")
        manufacturer = Manufacturer.objects.create(name="SR Mfr", slug="sr-mfr")
        device_type = DeviceType.objects.create(
            manufacturer=manufacturer, model="SR DT", slug="sr-dt"
        )
        role = DeviceRole.objects.create(name="SR Role", slug="sr-role")
        self.devices = {
            name: Device.objects.create(
                name=name,
                site=site,
                device_type=device_type,
                role=role,
                status="active",
            )
            for name in ("sr-a", "sr-b", "sr-c")
        }
        self.StaticRoute = apps.get_model("netbox_routing", "StaticRoute")

    def _runner(self):
        return ForwardSyncRunner(
            sync=self.sync, ingestion=None, client=None, logger_=Mock()
        )

    def _row(self, device="sr-a", args="10.0.0.0/8 10.1.1.1", vrf=None, family="ip"):
        return {
            "device": device,
            "os": "OS.NXOS",
            "vrf": vrf,
            "family": family,
            "args": args,
        }

    def _devices_of(self, route):
        return sorted(route.devices.values_list("name", flat=True))

    def test_a_route_is_created_with_its_device(self):
        route = apply_netbox_routing_staticroute(self._runner(), self._row())

        self.assertEqual(str(route.prefix), "10.0.0.0/8")
        self.assertEqual(str(route.next_hop), "10.1.1.1")
        self.assertEqual(self._devices_of(route), ["sr-a"])
        self.assertIn(STATIC_ROUTE_MARKER, route.comments)

    def test_the_same_route_on_two_devices_is_one_object_with_both(self):
        runner = self._runner()
        apply_netbox_routing_staticroute(runner, self._row("sr-a"))
        apply_netbox_routing_staticroute(runner, self._row("sr-b"))

        self.assertEqual(self.StaticRoute.objects.count(), 1)
        self.assertEqual(
            self._devices_of(self.StaticRoute.objects.get()), ["sr-a", "sr-b"]
        )

    def test_a_global_route_is_not_duplicated_by_the_nullable_key(self):
        runner = self._runner()
        for _ in range(3):
            apply_netbox_routing_staticroute(runner, self._row())

        self.assertEqual(self.StaticRoute.objects.count(), 1)

    def test_the_same_prefix_in_a_vrf_is_a_separate_route(self):
        runner = self._runner()
        apply_netbox_routing_staticroute(runner, self._row())
        apply_netbox_routing_staticroute(runner, self._row(vrf="CUST"))

        self.assertEqual(self.StaticRoute.objects.count(), 2)
        self.assertEqual(self.StaticRoute.objects.filter(vrf__name="CUST").count(), 1)

    def test_different_next_hops_are_different_routes(self):
        runner = self._runner()
        apply_netbox_routing_staticroute(runner, self._row(args="10.0.0.0/8 10.1.1.1"))
        apply_netbox_routing_staticroute(runner, self._row(args="10.0.0.0/8 10.1.1.2"))

        self.assertEqual(self.StaticRoute.objects.count(), 2)

    def test_an_interface_only_route_is_keyed_by_the_interface(self):
        runner = self._runner()
        apply_netbox_routing_staticroute(runner, self._row(args="10.0.0.0/8 Null0"))
        apply_netbox_routing_staticroute(runner, self._row(args="10.0.0.0/8 Null0"))
        apply_netbox_routing_staticroute(runner, self._row(args="10.0.0.0/8 Vlan9"))

        routes = self.StaticRoute.objects.order_by("interface_next_hop")
        self.assertEqual([r.interface_next_hop for r in routes], ["Null0", "Vlan9"])
        self.assertTrue(all(r.next_hop is None for r in routes))

    def test_the_device_sorting_first_owns_the_attributes(self):
        runner = self._runner()
        apply_netbox_routing_staticroute(
            runner, self._row("sr-b", args="10.0.0.0/8 10.1.1.1 200")
        )
        apply_netbox_routing_staticroute(
            runner, self._row("sr-a", args="10.0.0.0/8 10.1.1.1 5")
        )

        route = self.StaticRoute.objects.get()
        self.assertEqual(route.metric, 5)

    def test_the_result_does_not_depend_on_the_order_rows_arrive_in(self):
        runner = self._runner()
        apply_netbox_routing_staticroute(
            runner, self._row("sr-a", args="10.0.0.0/8 10.1.1.1 5")
        )
        apply_netbox_routing_staticroute(
            runner, self._row("sr-b", args="10.0.0.0/8 10.1.1.1 200")
        )

        self.assertEqual(self.StaticRoute.objects.get().metric, 5)

    def test_a_non_owner_row_adds_its_device_without_changing_attributes(self):
        runner = self._runner()
        apply_netbox_routing_staticroute(
            runner, self._row("sr-a", args="10.0.0.0/8 10.1.1.1 name KEEP")
        )
        apply_netbox_routing_staticroute(
            runner, self._row("sr-b", args="10.0.0.0/8 10.1.1.1 name OTHER")
        )

        route = self.StaticRoute.objects.get()
        self.assertEqual(route.name, "KEEP")
        self.assertEqual(self._devices_of(route), ["sr-a", "sr-b"])

    def test_an_unreadable_line_is_skipped_and_counted(self):
        runner = self._runner()

        result = apply_netbox_routing_staticroute(runner, self._row(args="garbage"))

        self.assertIs(result, False)
        self.assertEqual(self.StaticRoute.objects.count(), 0)
        self.assertEqual(
            runner._aggregated_skip_warning_counts.get(
                (STATIC_ROUTE_MODEL, STATIC_ROUTE_UNREADABLE_REASON)
            ),
            1,
        )

    def test_repeating_a_row_writes_nothing(self):
        runner = self._runner()
        row = self._row(args="10.0.0.0/8 10.1.1.1 name X tag 5 7")
        apply_netbox_routing_staticroute(runner, row)

        with CaptureQueriesContext(connection) as queries:
            apply_netbox_routing_staticroute(runner, row)

        writes = [
            q["sql"]
            for q in queries
            if q["sql"].lstrip().upper().startswith(("UPDATE", "INSERT", "DELETE"))
        ]
        self.assertEqual(writes, [], "\n".join(writes))

    # -- removal -------------------------------------------------------------

    def test_removing_one_device_leaves_the_route_for_the_others(self):
        runner = self._runner()
        apply_netbox_routing_staticroute(runner, self._row("sr-a"))
        apply_netbox_routing_staticroute(runner, self._row("sr-b"))

        changed = delete_netbox_routing_staticroute(runner, self._row("sr-a"))

        self.assertTrue(changed)
        self.assertEqual(self._devices_of(self.StaticRoute.objects.get()), ["sr-b"])

    def test_the_last_device_leaving_deletes_the_syncs_own_route(self):
        runner = self._runner()
        apply_netbox_routing_staticroute(runner, self._row("sr-a"))

        delete_netbox_routing_staticroute(runner, self._row("sr-a"))

        self.assertEqual(self.StaticRoute.objects.count(), 0)

    def test_a_route_made_by_hand_is_never_deleted(self):
        runner = self._runner()
        apply_netbox_routing_staticroute(runner, self._row("sr-a"))
        self.StaticRoute.objects.update(comments="Added by an operator.")

        delete_netbox_routing_staticroute(runner, self._row("sr-a"))

        route = self.StaticRoute.objects.get()
        self.assertEqual(self._devices_of(route), [])

    def test_removing_a_device_that_was_never_attached_changes_nothing(self):
        runner = self._runner()
        apply_netbox_routing_staticroute(runner, self._row("sr-a"))

        changed = delete_netbox_routing_staticroute(runner, self._row("sr-c"))

        self.assertFalse(changed)
        self.assertEqual(self._devices_of(self.StaticRoute.objects.get()), ["sr-a"])

    def test_removing_a_route_that_does_not_exist_changes_nothing(self):
        self.assertFalse(
            delete_netbox_routing_staticroute(
                self._runner(), self._row("sr-a", args="192.0.2.0/24 192.0.2.1")
            )
        )

    def test_an_unreadable_row_is_never_a_delete(self):
        runner = self._runner()
        apply_netbox_routing_staticroute(runner, self._row("sr-a"))

        self.assertFalse(
            delete_netbox_routing_staticroute(runner, self._row("sr-a", args="garbage"))
        )
        self.assertEqual(self.StaticRoute.objects.count(), 1)
