"""A routing policy definition is in scope when any in-scope device holds it.

The prefix-list, community-list and route-map maps are catalogues: one
definition is shared by every device configured with it, and each row names
only a representative - the lowest holder across the whole network. The
device-tag scope used to test that one representative, so a definition used by
in-scope devices was dropped whenever its lowest holder was out of scope. The
queries now attach every holder to each definition's lowest-sequence row
(`scope_holders`), and the scope keeps the whole definition when any holder is
in scope.
"""

from unittest.mock import Mock

from django.test import SimpleTestCase

from forward_netbox.utilities.forward_api import LATEST_PROCESSED_SNAPSHOT
from forward_netbox.utilities.query_fetch import ForwardQueryContext
from forward_netbox.utilities.query_fetch import ForwardQueryFetcher
from forward_netbox.utilities.query_fetch_execution import (
    _catalogue_definitions_in_scope,
)
from forward_netbox.utilities.query_fetch_execution import ROUTING_CATALOGUE_MODELS


def _context(scoped):
    return ForwardQueryContext(
        network_id="n",
        snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
        snapshot_id="s",
        device_tag_include_tags=["Prod"],
        scoped_device_names=set(scoped),
    )


def _fetcher():
    return ForwardQueryFetcher(sync=Mock(), client=Mock(), logger_=Mock())


def _entry(name, seq, *, rep, scope_holders=None, holder_devices=None):
    return {
        "name": name,
        "sequence": seq,
        "device": rep,
        "holder_devices": list(holder_devices or []),
        "scope_holders": list(scope_holders or []),
    }


class CatalogueScopeTest(SimpleTestCase):
    def test_a_shared_definition_with_an_out_of_scope_representative_is_kept(self):
        # `a-edge` is the lowest holder network-wide and is out of scope;
        # `core-1` holds the same definition and is in scope.
        rows = [
            _entry("PL-MGMT", 5, rep="a-edge", scope_holders=["a-edge", "core-1"]),
            _entry("PL-MGMT", 10, rep="a-edge"),
        ]
        for model in sorted(ROUTING_CATALOGUE_MODELS):
            filtered, removed = _fetcher()._apply_device_tag_scope(
                model, rows, _context({"core-1"})
            )
            self.assertEqual(filtered, rows, model)
            self.assertEqual(removed, [], model)

    def test_a_definition_only_out_of_scope_devices_hold_is_removed_whole(self):
        rows = [
            _entry("PL-LAB", 5, rep="lab-1", scope_holders=["lab-1", "lab-2"]),
            _entry("PL-LAB", 10, rep="lab-1"),
        ]
        filtered, removed = _fetcher()._apply_device_tag_scope(
            "netbox_routing.prefixlistentry", rows, _context({"core-1"})
        )
        self.assertEqual(filtered, [])
        self.assertEqual(removed, rows)

    def test_a_published_query_without_scope_holders_keeps_the_old_behaviour(self):
        # A stale org copy has only `device` and, for a variant,
        # `holder_devices`: it still works, knowing only those.
        rows = [
            {"name": "PL-A", "sequence": 5, "device": "core-1"},
            {"name": "PL-B", "sequence": 5, "device": "a-edge"},
            {
                "name": "PL-C@a-edge",
                "sequence": 5,
                "device": "a-edge",
                "holder_devices": ["a-edge", "core-1"],
            },
        ]
        filtered, _removed = _fetcher()._apply_device_tag_scope(
            "netbox_routing.communitylistentry", rows, _context({"core-1"})
        )
        self.assertEqual([row["name"] for row in filtered], ["PL-A", "PL-C@a-edge"])

    def test_holders_are_collected_across_every_row_of_a_definition(self):
        rows = [
            _entry("RM-1", 10, rep="a-edge"),
            _entry("RM-1", 20, rep="a-edge", scope_holders=["a-edge", "core-9"]),
        ]
        self.assertEqual(_catalogue_definitions_in_scope(rows, {"core-9"}), {"RM-1"})

    def test_other_models_still_scope_by_row_device(self):
        rows = [{"device": "a-edge", "name": "Ethernet1", "scope_holders": ["core-1"]}]
        filtered, removed = _fetcher()._apply_device_tag_scope(
            "dcim.interface", rows, _context({"core-1"})
        )
        self.assertEqual(filtered, [])
        self.assertEqual(removed, rows)
