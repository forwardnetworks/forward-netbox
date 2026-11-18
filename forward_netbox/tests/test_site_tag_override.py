# Forward knows a device by its location; NetBox sites are usually coarser. A
# device tagged NetBox_Site_<name> goes to the site <name>, and an untagged device
# keeps its location. The rule has to be the same in every place that decides a
# site: the shared NQE helper, the maps that use it, and the ad-hoc queries the
# plugin builds in Python. A customer whose data centre is split across many
# Forward locations hit exactly this: sites that would not line up with their
# inventory system.
import re
from pathlib import Path

from django.test import SimpleTestCase

from forward_netbox.utilities import query_fetch_execution
from forward_netbox.utilities import scope_reconciliation
from forward_netbox.utilities.query_execution_contract import declared_query_parameters
from forward_netbox.utilities.site_tag import site_name_for
from forward_netbox.utilities.site_tag import site_name_nqe
from forward_netbox.utilities.site_tag import site_tag_nqe_let
from forward_netbox.utilities.site_tag import SITE_TAG_PREFIX

QUERIES = Path(__file__).resolve().parents[1] / "queries"
DOCS = Path(__file__).resolve().parents[2] / "docs"


def _squash(text):
    """Whitespace collapsed, and none kept beside a bracket: layout is not logic."""
    text = re.sub(r"\s+", " ", text).strip()
    return re.sub(r"\s*([()])\s*", r"\1", text)


class SiteRuleTest(SimpleTestCase):
    def test_an_untagged_device_keeps_its_location_lowercased(self):
        self.assertEqual(site_name_for([], "Hall_Mgmt"), "hall_mgmt")

    def test_a_site_tag_overrides_the_location(self):
        self.assertEqual(
            site_name_for(["Prod", f"{SITE_TAG_PREFIX}Campus-Production"], "Hall_Mgmt"),
            "campus-production",
        )

    def test_several_locations_become_one_site_through_the_tag(self):
        tag = [f"{SITE_TAG_PREFIX}Campus-Production"]

        names = {
            site_name_for(tag, location) for location in ("Hall_Mgmt", "Hall_Fab1")
        }

        self.assertEqual(names, {"campus-production"})

    def test_two_site_tags_take_the_first_alphabetically_in_any_order(self):
        forward = [f"{SITE_TAG_PREFIX}b-site", f"{SITE_TAG_PREFIX}a-site"]

        self.assertEqual(site_name_for(forward, "x"), "a-site")
        self.assertEqual(site_name_for(list(reversed(forward)), "x"), "a-site")

    def test_a_tag_with_nothing_after_the_prefix_is_not_a_site_tag(self):
        self.assertEqual(site_name_for([SITE_TAG_PREFIX], "Lab"), "lab")

    def test_the_prefix_is_case_sensitive(self):
        self.assertEqual(site_name_for(["netbox_site_other"], "Lab"), "lab")

    def test_no_location_and_no_tag_is_unknown(self):
        self.assertEqual(site_name_for([], None), "unknown")
        self.assertEqual(site_name_for([], ""), "unknown")


class SharedHelperTest(SimpleTestCase):
    def setUp(self):
        self.helper = (QUERIES / "netbox_utilities.nqe").read_text()

    def test_the_helper_is_exported_with_the_documented_prefix(self):
        for name in (
            "siteTagName",
            "siteNameFor",
            "deviceSiteName",
            "siteRuleMatches",
            "siteRuleRank",
            "siteRuleSite",
            "siteNameForRules",
            "deviceSiteNameRules",
        ):
            self.assertIn(f"export {name}(", self.helper)
        self.assertIn(f'matches(tag, "{SITE_TAG_PREFIX}*")', self.helper)
        self.assertIn(f'replace(tag, "{SITE_TAG_PREFIX}", "")', self.helper)

    def test_the_python_embedding_spells_the_tag_lookup_like_the_helper(self):
        body = self.helper[self.helper.index("export siteTagName(") :]
        body = body[: body.index(";")]
        helper_lookup = _squash(body.split("=", 1)[1]).replace(
            "tag_names", "device.tagNames"
        )
        python_lookup = _squash(site_tag_nqe_let().split("=", 1)[1])

        self.assertEqual(python_lookup, helper_lookup)


class MapsUseTheHelperTest(SimpleTestCase):
    def _text(self, name):
        return (QUERIES / f"{name}.nqe").read_text()

    def test_the_maps_that_name_a_site_use_the_helper_not_their_own_rule(self):
        for name in (
            "forward_devices",
            "forward_devices_with_netbox_aliases",
            "forward_vlans",
            "forward_locations",
        ):
            with self.subTest(query=name):
                text = self._text(name)
                self.assertIn("deviceSiteNameRules(device, site_rules)", text)
                self.assertNotIn("toLowerCase(device.locationName)", text)

    def test_snmp_endpoints_follow_the_same_rule(self):
        for name in ("forward_devices", "forward_devices_with_netbox_aliases"):
            with self.subTest(query=name):
                text = self._text(name)
                self.assertIn("siteNameForRules(site_rules, endpoint.tagNames", text)
                self.assertNotIn("toLowerCase(endpoint.locationName)", text)

    def test_the_locations_map_emits_one_row_per_site_not_per_location(self):
        text = self._text("forward_locations")

        self.assertIn("group device as devices by site_name as site_name", text)
        self.assertNotIn(
            "foreach location in network.locations\nwhere location.name in", text
        )

    def test_aci_fabric_naming_is_left_alone(self):
        # A fabric is not a site: it is named from the APIC's location, and
        # re-deriving it from a site tag would split or merge real fabrics.
        for path in QUERIES.glob("forward_aci_*.nqe"):
            self.assertNotIn("deviceSiteName", path.read_text(), path.name)


class PythonQueriesTest(SimpleTestCase):
    def test_the_inline_queries_embed_the_rule_instead_of_copying_the_old_one(self):
        for module in (scope_reconciliation, query_fetch_execution):
            with self.subTest(module=module.__name__):
                source = Path(module.__file__).read_text()
                self.assertIn("site_tag_nqe_let()", source)
                self.assertIn("site_name_nqe(", source)
                self.assertNotIn("toLowerCase(device.locationName)", source)

    def test_the_embedded_expression_falls_back_the_way_each_caller_needs(self):
        self.assertIn('else ""', site_name_nqe(fallback='""'))
        self.assertTrue(site_name_nqe().endswith('else "unknown"'))
        self.assertIn(f'"{SITE_TAG_PREFIX}*"', site_tag_nqe_let("endpoint"))
        self.assertIn("endpoint.tagNames", site_tag_nqe_let("endpoint"))


class DocumentedTest(SimpleTestCase):
    def test_the_convention_is_documented(self):
        text = (DOCS / "02_Reference" / "site-tag-override.md").read_text()

        self.assertIn(SITE_TAG_PREFIX, text)
        self.assertIn("first alphabetically", text)


SITE_RULES_TYPE = (
    "site_rules: List<{ kind: String, pattern: String, site: String, rank: Number }>"
)
SITE_NAMING_MAPS = (
    "forward_devices",
    "forward_devices_with_netbox_aliases",
    "forward_vlans",
    "forward_locations",
)


class SiteRulesParameterTest(SimpleTestCase):
    """Only the maps that name a site take the rules. `netbox_utilities` is copied into
    about thirty queries, and a query that does not declare a parameter rejects it."""

    def _declared(self, path):
        """The parameter names the plugin itself reads from the @query signature."""
        declared = declared_query_parameters(path.read_text())
        return [] if declared is None else [parameter.name for parameter in declared]

    def test_the_maps_that_name_a_site_declare_the_rules_as_records(self):
        for name in SITE_NAMING_MAPS:
            with self.subTest(query=name):
                declared = declared_query_parameters(
                    (QUERIES / f"{name}.nqe").read_text()
                )
                types = {parameter.name: parameter.nqe_type for parameter in declared}
                self.assertEqual(
                    types.get("site_rules"),
                    "List<{ kind: String, pattern: String, site: String, rank: Number }>",
                    "the plugin reads the record type from the query signature",
                )

    def test_no_other_shipped_query_declares_the_rules(self):
        for path in sorted(QUERIES.glob("*.nqe")):
            if path.stem in SITE_NAMING_MAPS or path.stem == "netbox_utilities":
                continue
            with self.subTest(query=path.name):
                self.assertNotIn("site_rules", self._declared(path))

    def test_the_vlan_helpers_pass_the_rules_down_to_where_the_site_is_named(self):
        text = (QUERIES / "forward_vlans.nqe").read_text()

        for call in (
            "ni_vlan_rows(device_tag_include_tags, device_tag_include_match, device_tag_exclude_tags, site_rules)",
            "iface_vlan_rows(device_tag_include_tags, device_tag_include_match, device_tag_exclude_tags, site_rules)",
        ):
            self.assertIn(call, text)
        self.assertEqual(text.count("deviceSiteNameRules(device, site_rules)"), 2)

    def test_the_locations_map_keeps_a_device_that_only_a_rule_places(self):
        text = (QUERIES / "forward_locations.nqe").read_text()

        self.assertIn("isPresent(rule_site)", text)

    def test_the_old_helpers_keep_their_shape_for_customers_own_queries(self):
        helper = (QUERIES / "netbox_utilities.nqe").read_text()

        self.assertIn("export deviceSiteName(device: Device) =", helper)
        self.assertIn(
            "export siteNameFor(tag_names: List<String>, location_name: String)", helper
        )
