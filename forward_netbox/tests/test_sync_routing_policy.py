# Routing policy: prefix lists, community lists and route maps, parsed from
# device configuration because Forward has no structured policy model.
#
# Two things are pinned here that the destination model forces. netbox-routing
# names policy objects globally, so the queries build a catalogue: the
# definition most devices share owns the bare name and each divergent one is
# `<name>@<device>`; rows arrive carrying that stored `name`, and two devices
# defining `TO-DMZ` differently never overwrite each other.
# And netbox-routing 0.4.3's `PrefixListEntry.clean()` rejects the normal
# `ge X le Y` pair, so the adapter predicts that and stores the entry without
# bounds rather than letting the merge reject it on every run.
#
# The parse fixtures are syntax examples captured from real IOS-XE, NX-OS and
# EOS configurations with names and addresses replaced - never inventory.
from unittest.mock import Mock

from dcim.models import Device
from dcim.models import DeviceRole
from dcim.models import DeviceType
from dcim.models import Manufacturer
from dcim.models import Site
from django.test import SimpleTestCase
from django.test import TestCase

from forward_netbox.models import ForwardSource
from forward_netbox.models import ForwardSync
from forward_netbox.utilities.drift_comparison import compare_model_rows
from forward_netbox.utilities.sync import ForwardSyncRunner
from forward_netbox.utilities.sync_routing_policy import (
    apply_netbox_routing_communitylistentry,
)
from forward_netbox.utilities.sync_routing_policy import (
    apply_netbox_routing_prefixlistentry,
)
from forward_netbox.utilities.sync_routing_policy import (
    apply_netbox_routing_routemapentry,
)
from forward_netbox.utilities.sync_routing_policy import COMMUNITY_LIST_MODEL
from forward_netbox.utilities.sync_routing_policy import community_value
from forward_netbox.utilities.sync_routing_policy import (
    delete_netbox_routing_communitylistentry,
)
from forward_netbox.utilities.sync_routing_policy import (
    delete_netbox_routing_prefixlistentry,
)
from forward_netbox.utilities.sync_routing_policy import (
    delete_netbox_routing_routemapentry,
)
from forward_netbox.utilities.sync_routing_policy import EXPANDED_COMMUNITY_LIST_REASON
from forward_netbox.utilities.sync_routing_policy import NON_NUMERIC_COMMUNITY_REASON
from forward_netbox.utilities.sync_routing_policy import parse_route_map_clauses
from forward_netbox.utilities.sync_routing_policy import policy_object_name
from forward_netbox.utilities.sync_routing_policy import prefix_length_bounds
from forward_netbox.utilities.sync_routing_policy import PREFIX_LIST_MODEL
from forward_netbox.utilities.sync_routing_policy import ROUTE_MAP_MODEL
from forward_netbox.utilities.sync_routing_policy import (
    UNREPRESENTABLE_PREFIX_BOUNDS_REASON,
)


def _routing(model_name):
    from forward_netbox.utilities.sync_primitives import optional_model

    return optional_model("netbox_routing", model_name, "netbox_routing.routemapentry")


class PrefixLengthBoundsTest(SimpleTestCase):
    """What netbox-routing 0.4.3 will and will not validate."""

    def test_no_bounds_is_representable(self):
        self.assertEqual(prefix_length_bounds("10.0.0.0/8"), (None, None, True))

    def test_le_alone_longer_than_the_prefix_is_kept(self):
        self.assertEqual(prefix_length_bounds("0.0.0.0/0", le=32), (None, 32, True))

    def test_eq_becomes_an_equal_pair(self):
        # NX-OS `eq 32`; the plugin's check is strictly `ge < le`, so equal passes.
        self.assertEqual(prefix_length_bounds("0.0.0.0/0", eq=32), (32, 32, True))

    def test_the_normal_ge_below_le_pair_is_not_representable(self):
        # `ip prefix-list X seq 5 permit 10.0.0.0/8 ge 24 le 32` - the most
        # common IOS form - is rejected by the plugin's clean().
        self.assertEqual(
            prefix_length_bounds("10.0.0.0/8", ge=24, le=32), (None, None, False)
        )

    def test_a_bound_not_longer_than_the_prefix_is_not_representable(self):
        self.assertEqual(
            prefix_length_bounds("10.0.0.0/24", le=24), (None, None, False)
        )

    def test_a_v6_boundary_is_128(self):
        self.assertEqual(prefix_length_bounds("2001:db8::/32", le=64), (None, 64, True))
        self.assertEqual(
            prefix_length_bounds("2001:db8::/32", le=129), (None, None, False)
        )


class CommunityValueTest(SimpleTestCase):
    def test_numeric_forms_are_accepted(self):
        self.assertEqual(community_value("64101:102"), "64101:102")
        self.assertEqual(community_value("65000:1:2"), "65000:1:2")
        self.assertEqual(community_value('"65004:17100"'), "65004:17100")

    def test_well_known_names_and_patterns_are_not(self):
        self.assertIsNone(community_value("no-export"))
        self.assertIsNone(community_value("internet"))
        self.assertIsNone(community_value("65...:[14]0010"))
        self.assertIsNone(community_value(""))


class RouteMapClauseParserTest(SimpleTestCase):
    """Clause shapes captured from IOS-XE, NX-OS and EOS route maps."""

    def test_match_prefix_list_keys_on_the_full_clause_head(self):
        parsed = parse_route_map_clauses(["match ip address prefix-list PL-OUT"])
        self.assertEqual(parsed["match"], {"ip_address_prefix_list": ["PL-OUT"]})
        self.assertIsNone(parsed["set"])

    def test_match_as_path_takes_the_list_name_as_a_value(self):
        # `as-path` is a sub-verb only for `set as-path prepend`.
        parsed = parse_route_map_clauses(["match as-path LOCAL_ONLY"])
        self.assertEqual(parsed["match"], {"as_path": ["LOCAL_ONLY"]})

    def test_set_as_path_prepend_keys_on_prepend(self):
        parsed = parse_route_map_clauses(["set as-path prepend 65000 65000"])
        self.assertEqual(parsed["set"], {"as_path_prepend": ["65000", "65000"]})

    def test_qualifiers_become_flags(self):
        parsed = parse_route_map_clauses(
            ["match community CL-A exact-match", "set community 64101:102 additive"]
        )
        self.assertEqual(
            parsed["match"], {"community": ["CL-A"], "community_exact_match": True}
        )
        self.assertEqual(
            parsed["set"], {"community": ["64101:102"], "community_additive": True}
        )

    def test_continue_and_description_are_fields_not_json(self):
        parsed = parse_route_map_clauses(["continue 1600", "description Send routes"])
        self.assertEqual(parsed["flow_control"], 1600)
        self.assertEqual(parsed["description"], "Send routes")
        self.assertIsNone(parsed["match"])

    def test_unknown_clauses_are_kept_verbatim(self):
        parsed = parse_route_map_clauses(["on-match next"])
        self.assertEqual(parsed["match"], {"other": ["on-match next"]})

    def test_an_empty_stanza_is_empty(self):
        parsed = parse_route_map_clauses([])
        self.assertEqual(
            parsed,
            {"match": None, "set": None, "flow_control": None, "description": ""},
        )


class RoutingPolicyAdapterTest(TestCase):
    def setUp(self):
        self.source = ForwardSource.objects.create(
            name="pol-source",
            type="saas",
            url="https://forward.example.com",
            status="ready",
            parameters={
                "username": "u@example.com",
                "password": "p",
                "verify": True,
                "network_id": "net-1",
            },
        )
        self.sync = ForwardSync.objects.create(
            name="pol-sync",
            source=self.source,
            parameters={"snapshot_id": "latestProcessed"},
        )
        site = Site.objects.create(name="Pol Site", slug="pol-site")
        mfr = Manufacturer.objects.create(name="Pol Mfr", slug="pol-mfr")
        dtype = DeviceType.objects.create(
            manufacturer=mfr, model="Pol DT", slug="pol-dt"
        )
        role = DeviceRole.objects.create(name="Pol Role", slug="pol-role")
        for name in ("pol-a", "pol-b"):
            Device.objects.create(
                name=name, site=site, device_type=dtype, role=role, status="active"
            )

    def _runner(self):
        return ForwardSyncRunner(
            sync=self.sync, ingestion=None, client=None, logger_=Mock()
        )

    # --- prefix lists ------------------------------------------------------

    def _pl_row(self, **extra):
        row = {
            "name": "PL-OUT",
            "device": "pol-a",
            "device_count": 3,
            "list_name": "PL-OUT",
            "family": 4,
            "sequence": 10,
            "action": "permit",
            "prefix": "10.0.0.0/8",
            "ge": None,
            "le": None,
            "eq": None,
            "os": "OS.IOS_XE",
        }
        row.update(extra)
        return row

    def test_a_prefix_list_entry_creates_its_list_and_custom_prefix(self):
        entry = apply_netbox_routing_prefixlistentry(
            self._runner(), self._pl_row(le=32)
        )

        self.assertEqual(entry.prefix_list.name, "PL-OUT")
        self.assertEqual(entry.prefix_list.family, 4)
        self.assertEqual(
            entry.prefix_list.description,
            "PL-OUT defined identically on 3 device(s), e.g. pol-a (Forward)",
        )
        self.assertEqual(str(entry.assigned_prefix.prefix), "10.0.0.0/8")
        self.assertEqual((entry.ge, entry.le), (None, 32))
        self.assertEqual(entry.action, "permit")

    def test_a_divergent_variant_is_its_own_list_with_a_variant_description(self):
        # The query names the variant `PL-OUT@pol-b`; the adapter stores it
        # beside the canonical `PL-OUT` rather than over it.
        runner = self._runner()
        apply_netbox_routing_prefixlistentry(runner, self._pl_row(prefix="10.0.0.0/8"))
        apply_netbox_routing_prefixlistentry(
            runner,
            self._pl_row(
                name="PL-OUT@pol-b",
                device="pol-b",
                device_count=1,
                prefix="192.168.0.0/16",
            ),
        )

        PrefixList = _routing("PrefixList")
        self.assertEqual(
            sorted(PrefixList.objects.values_list("name", flat=True)),
            ["PL-OUT", "PL-OUT@pol-b"],
        )
        variant = PrefixList.objects.get(name="PL-OUT@pol-b")
        self.assertEqual(
            variant.description,
            "PL-OUT: variant defined identically on 1 device(s), e.g. pol-b (Forward)",
        )
        # A shared prefix value is one CustomPrefix; a different one is another.
        self.assertEqual(_routing("CustomPrefix").objects.count(), 2)

    def test_ge_below_le_is_stored_without_bounds_and_rolled_up(self):
        runner = self._runner()
        entry = apply_netbox_routing_prefixlistentry(runner, self._pl_row(ge=24, le=32))

        self.assertEqual((entry.ge, entry.le), (None, None))
        self.assertEqual(entry.description, "10.0.0.0/8 ge 24 le 32")
        self.assertEqual(
            runner._aggregated_skip_warning_counts.get(
                (PREFIX_LIST_MODEL, UNREPRESENTABLE_PREFIX_BOUNDS_REASON)
            ),
            1,
        )
        # The stored row passes the plugin's own validation, which is the point.
        entry.full_clean()

    def test_eq_is_stored_as_an_equal_pair_and_validates(self):
        entry = apply_netbox_routing_prefixlistentry(
            self._runner(), self._pl_row(prefix="0.0.0.0/0", eq=32)
        )
        self.assertEqual((entry.ge, entry.le), (32, 32))
        entry.full_clean()

    def test_an_applied_prefix_list_entry_compares_unchanged(self):
        rows = [self._pl_row(le=32), self._pl_row(sequence=20, prefix="172.16.0.0/12")]
        runner = self._runner()
        for row in rows:
            apply_netbox_routing_prefixlistentry(runner, row)

        result = compare_model_rows(None, PREFIX_LIST_MODEL, rows)

        self.assertEqual(result["creates"], 0)
        self.assertEqual(result["updates"], 0)

    def test_deleting_the_last_entry_removes_the_empty_list(self):
        runner = self._runner()
        apply_netbox_routing_prefixlistentry(runner, self._pl_row())
        apply_netbox_routing_prefixlistentry(runner, self._pl_row(sequence=20))

        self.assertTrue(delete_netbox_routing_prefixlistentry(runner, self._pl_row()))
        self.assertEqual(_routing("PrefixList").objects.count(), 1)
        self.assertTrue(
            delete_netbox_routing_prefixlistentry(runner, self._pl_row(sequence=20))
        )
        self.assertEqual(_routing("PrefixList").objects.count(), 0)

    def test_deleting_an_unknown_entry_is_a_no_op(self):
        self.assertFalse(
            delete_netbox_routing_prefixlistentry(self._runner(), self._pl_row())
        )

    # --- community lists ---------------------------------------------------

    def _cl_row(self, **extra):
        row = {
            "name": "CL-A",
            "device": "pol-a",
            "device_count": 2,
            "list_name": "CL-A",
            "form": "standard",
            "sequence": None,
            "action": "permit",
            "community": "64101:102",
            "os": "OS.IOS_XE",
        }
        row.update(extra)
        return row

    def test_a_community_list_entry_creates_its_list_and_community(self):
        entry = apply_netbox_routing_communitylistentry(self._runner(), self._cl_row())

        self.assertEqual(entry.community_list.name, "CL-A")
        self.assertEqual(entry.community.community, "64101:102")
        self.assertEqual(entry.action, "permit")

    def test_a_community_value_is_shared_across_lists(self):
        runner = self._runner()
        apply_netbox_routing_communitylistentry(runner, self._cl_row())
        apply_netbox_routing_communitylistentry(
            runner, self._cl_row(name="CL-A@pol-b", device="pol-b", device_count=1)
        )

        self.assertEqual(_routing("Community").objects.count(), 1)
        self.assertEqual(_routing("CommunityList").objects.count(), 2)

    def test_an_expanded_list_is_skipped_and_rolled_up(self):
        runner = self._runner()
        outcome = apply_netbox_routing_communitylistentry(
            runner, self._cl_row(form="expanded", community="65...:[14]0010")
        )

        self.assertIs(outcome, False)
        self.assertEqual(_routing("CommunityList").objects.count(), 0)
        self.assertEqual(
            runner._aggregated_skip_warning_counts.get(
                (COMMUNITY_LIST_MODEL, EXPANDED_COMMUNITY_LIST_REASON)
            ),
            1,
        )

    def test_a_well_known_community_is_skipped_and_rolled_up(self):
        runner = self._runner()
        outcome = apply_netbox_routing_communitylistentry(
            runner, self._cl_row(community="no-export")
        )

        self.assertIs(outcome, False)
        self.assertEqual(
            runner._aggregated_skip_warning_counts.get(
                (COMMUNITY_LIST_MODEL, NON_NUMERIC_COMMUNITY_REASON)
            ),
            1,
        )

    def test_an_applied_community_list_entry_compares_unchanged(self):
        rows = [self._cl_row(), self._cl_row(community="64101:103")]
        runner = self._runner()
        for row in rows:
            apply_netbox_routing_communitylistentry(runner, row)

        result = compare_model_rows(None, COMMUNITY_LIST_MODEL, rows)

        self.assertEqual(result["creates"], 0)
        self.assertEqual(result["updates"], 0)

    def test_deleting_the_last_entry_removes_the_empty_community_list(self):
        runner = self._runner()
        apply_netbox_routing_communitylistentry(runner, self._cl_row())

        self.assertTrue(
            delete_netbox_routing_communitylistentry(runner, self._cl_row())
        )
        self.assertEqual(_routing("CommunityList").objects.count(), 0)
        # The community value is a fleet-wide catalogue row; it stays.
        self.assertEqual(_routing("Community").objects.count(), 1)

    # --- route maps --------------------------------------------------------

    def _rm_row(self, **extra):
        row = {
            "name": "RM-OUT",
            "device": "pol-a",
            "device_count": 5,
            "map_name": "RM-OUT",
            "sequence": 10,
            "action": "permit",
            "clauses": [
                "match ip address prefix-list PL-OUT",
                "set community 64101:102 additive",
            ],
            "os": "OS.NXOS",
        }
        row.update(extra)
        return row

    def test_a_route_map_entry_creates_its_map_and_carries_the_clauses(self):
        entry = apply_netbox_routing_routemapentry(self._runner(), self._rm_row())

        self.assertEqual(entry.route_map.name, "RM-OUT")
        self.assertEqual(entry.sequence, 10)
        self.assertEqual(entry.match, {"ip_address_prefix_list": ["PL-OUT"]})
        self.assertEqual(
            entry.set, {"community": ["64101:102"], "community_additive": True}
        )
        self.assertIsNone(entry.flow_control)

    def test_continue_lands_in_flow_control(self):
        entry = apply_netbox_routing_routemapentry(
            self._runner(), self._rm_row(clauses=["continue 20", "description x"])
        )
        self.assertEqual(entry.flow_control, 20)
        self.assertEqual(entry.description, "x")
        self.assertIsNone(entry.match)

    def test_a_changed_clause_is_an_update_not_a_duplicate(self):
        runner = self._runner()
        apply_netbox_routing_routemapentry(runner, self._rm_row())
        apply_netbox_routing_routemapentry(
            runner, self._rm_row(clauses=["match tag 12345"])
        )

        RouteMapEntry = _routing("RouteMapEntry")
        self.assertEqual(RouteMapEntry.objects.count(), 1)
        self.assertEqual(RouteMapEntry.objects.get().match, {"tag": ["12345"]})

    def test_an_applied_route_map_entry_compares_unchanged(self):
        rows = [self._rm_row(), self._rm_row(sequence=20, action="deny", clauses=[])]
        runner = self._runner()
        for row in rows:
            apply_netbox_routing_routemapentry(runner, row)

        result = compare_model_rows(None, ROUTE_MAP_MODEL, rows)

        self.assertEqual(result["creates"], 0)
        self.assertEqual(result["updates"], 0)

    def test_deleting_the_last_entry_removes_the_empty_route_map(self):
        runner = self._runner()
        apply_netbox_routing_routemapentry(runner, self._rm_row())

        self.assertTrue(delete_netbox_routing_routemapentry(runner, self._rm_row()))
        self.assertEqual(_routing("RouteMap").objects.count(), 0)

    def test_the_stored_name_is_the_catalogue_name_the_query_decided(self):
        self.assertEqual(policy_object_name({"name": "TO-DMZ@leaf-1"}), "TO-DMZ@leaf-1")
        self.assertEqual(policy_object_name({}), "")
