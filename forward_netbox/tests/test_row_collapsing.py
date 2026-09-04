# Collapsing rows that write one object, and the properties that make it safe.
#
# The collapse exists because some Forward queries report one row per
# OBSERVATION while the apply upserts one object per IDENTITY. Three properties
# have to hold or it trades one churn for another:
#
#   - a single-observation row must come through untouched, or every
#     unaffected interface on the estate rewrites for nothing;
#   - the merged text must not depend on the order Forward returned rows in;
#   - a genuine difference must still survive the merge.
from django.test import SimpleTestCase

from forward_netbox.utilities.row_collapsing import collapse_rows
from forward_netbox.utilities.sync_routing_impl import COLLAPSED_COMMENTS_KEY
from forward_netbox.utilities.sync_routing_impl import ospf_interface_comments

OSPF_INTERFACE = "netbox_routing.ospfinterface"
OSPF_AREA = "netbox_routing.ospfarea"


def _row(**extra):
    row = {
        "device": "dev-1",
        "process_id": "1",
        "area_id": "0.0.0.0",
        "area_type": "standard",
        "local_interface": "GigabitEthernet0/0",
        "cost": 10,
    }
    row.update(extra)
    return row


class AModelWithNoCollapserIsUntouchedTest(SimpleTestCase):
    def test_rows_pass_straight_through(self):
        rows = [_row(), _row(device="dev-2")]
        self.assertIs(collapse_rows("dcim.device", rows), rows)


class OneNeighbourIsNotAGroupTest(SimpleTestCase):
    """The common case must be byte-identical or the whole estate rewrites."""

    def test_a_single_row_is_returned_unchanged(self):
        row = _row(remote_device="peer-a", remote_router_id="10.0.0.2")
        collapsed = collapse_rows(OSPF_INTERFACE, [row])
        self.assertEqual(collapsed, [row])
        self.assertNotIn(COLLAPSED_COMMENTS_KEY, collapsed[0])

    def test_its_comments_are_what_they_always_were(self):
        row = _row(remote_device="peer-a", remote_router_id="10.0.0.2")
        collapsed = collapse_rows(OSPF_INTERFACE, [row])
        self.assertEqual(
            ospf_interface_comments(collapsed[0]), ospf_interface_comments(row)
        )

    def test_two_interfaces_stay_two_rows(self):
        rows = [_row(), _row(local_interface="GigabitEthernet0/1")]
        self.assertEqual(len(collapse_rows(OSPF_INTERFACE, rows)), 2)

    def test_the_same_interface_on_two_devices_stays_two_rows(self):
        rows = [_row(), _row(device="dev-2")]
        self.assertEqual(len(collapse_rows(OSPF_INTERFACE, rows)), 2)


class NeighboursOnOneInterfaceBecomeOneRowTest(SimpleTestCase):
    def _neighbours(self):
        return [
            _row(remote_device="peer-b", remote_router_id="10.0.0.3"),
            _row(remote_device="peer-a", remote_router_id="10.0.0.2"),
        ]

    def test_they_collapse_to_one(self):
        self.assertEqual(len(collapse_rows(OSPF_INTERFACE, self._neighbours())), 1)

    def test_every_neighbour_survives_the_merge(self):
        collapsed = collapse_rows(OSPF_INTERFACE, self._neighbours())[0]
        comments = ospf_interface_comments(collapsed)
        for value in ("peer-a", "peer-b", "10.0.0.2", "10.0.0.3"):
            self.assertIn(value, comments)

    def test_the_merge_does_not_depend_on_the_order_forward_returned(self):
        rows = self._neighbours()
        forwards = collapse_rows(OSPF_INTERFACE, rows)[0]
        backwards = collapse_rows(OSPF_INTERFACE, list(reversed(rows)))[0]
        # An unsorted merge would swap a churn that recurs for one that
        # recurs whenever Forward's ordering moves, which is worse: it would
        # look intermittent.
        self.assertEqual(
            ospf_interface_comments(forwards), ospf_interface_comments(backwards)
        )

    def test_the_interfaces_own_fields_are_kept(self):
        collapsed = collapse_rows(OSPF_INTERFACE, self._neighbours())[0]
        self.assertEqual(collapsed["local_interface"], "GigabitEthernet0/0")
        self.assertEqual(collapsed["device"], "dev-1")

    def test_an_alias_and_its_expansion_are_the_same_interface(self):
        rows = [
            _row(local_interface="gi0/0", remote_device="peer-a"),
            _row(local_interface="GigabitEthernet0/0", remote_device="peer-b"),
        ]
        # They resolve to one NetBox interface, so they must collapse to one
        # row - grouping on the raw string would leave them fighting.
        self.assertEqual(len(collapse_rows(OSPF_INTERFACE, rows)), 1)

    def test_a_changed_neighbour_changes_the_merged_text(self):
        before = collapse_rows(OSPF_INTERFACE, self._neighbours())[0]
        after = collapse_rows(
            OSPF_INTERFACE,
            [
                _row(remote_device="peer-b", remote_router_id="10.0.0.3"),
                _row(remote_device="peer-a", remote_router_id="10.0.0.9"),
            ],
        )[0]
        self.assertNotEqual(
            ospf_interface_comments(before), ospf_interface_comments(after)
        )


class AnAreaIsOneObjectHoweverManyDevicesReportItTest(SimpleTestCase):
    def test_rows_for_one_area_collapse(self):
        rows = [_row(device="dev-1"), _row(device="dev-2"), _row(device="dev-3")]
        self.assertEqual(len(collapse_rows(OSPF_AREA, rows)), 1)

    def test_distinct_areas_stay_distinct(self):
        rows = [_row(), _row(area_id="0.0.0.1")]
        self.assertEqual(len(collapse_rows(OSPF_AREA, rows)), 2)

    def test_a_disagreement_resolves_the_same_way_every_run(self):
        # Two devices reporting different types for one area is a data
        # conflict. Whatever it means, the two must not take turns winning and
        # report drift forever.
        rows = [_row(area_type="stub"), _row(device="dev-2", area_type="nssa")]
        first = collapse_rows(OSPF_AREA, rows)[0]
        second = collapse_rows(OSPF_AREA, list(reversed(rows)))[0]
        self.assertEqual(first["area_type"], second["area_type"])
