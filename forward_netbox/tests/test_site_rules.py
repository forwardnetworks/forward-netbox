# An operator maps Forward devices to NetBox sites in the source settings: devices
# whose Forward location matches a pattern, or that carry a tag, go to a named site.
# The rule list has to be stored, validated and sent to the queries the same way
# everywhere, so the parsing and validation live in one place and the tests pin what
# an operator can and cannot save.
from pathlib import Path

from django.test import SimpleTestCase

from forward_netbox.utilities.site_tag import format_site_rules
from forward_netbox.utilities.site_tag import normalize_site_rules
from forward_netbox.utilities.site_tag import parse_site_rules_text
from forward_netbox.utilities.site_tag import query_site_rules
from forward_netbox.utilities.site_tag import site_helper_nqe
from forward_netbox.utilities.site_tag import site_slug
from forward_netbox.utilities.site_tag import with_site_rules
from forward_netbox.utilities.site_tag import SiteRuleError

QUERIES = Path(__file__).resolve().parents[1] / "queries"


def rule(kind="location", pattern="Hall*", site="campus"):
    return {"kind": kind, "pattern": pattern, "site": site}


class NormalizeTest(SimpleTestCase):
    def test_a_valid_list_is_kept_in_order_with_text_trimmed(self):
        rules = normalize_site_rules(
            [rule(pattern="  Hall* ", site=" campus "), rule("tag", "Prod", "core")]
        )

        self.assertEqual(
            rules,
            [rule(pattern="Hall*", site="campus"), rule("tag", "Prod", "core")],
        )

    def test_nothing_set_is_an_empty_list(self):
        self.assertEqual(normalize_site_rules(None), [])
        self.assertEqual(normalize_site_rules([]), [])

    def test_a_list_of_rules_is_required(self):
        for bad in ("Hall* -> campus", {"kind": "tag"}, 5):
            with self.subTest(bad=bad), self.assertRaises(SiteRuleError):
                normalize_site_rules(bad)

    def test_each_rule_is_an_object_with_exactly_the_three_keys(self):
        for bad in (
            "text",
            {"kind": "tag", "pattern": "x"},
            {**rule(), "extra": 1},
            {**rule(), "rank": 3},
        ):
            with self.subTest(bad=bad), self.assertRaises(SiteRuleError):
                normalize_site_rules([bad])

    def test_the_kind_is_location_or_tag(self):
        with self.assertRaises(SiteRuleError):
            normalize_site_rules([rule(kind="device")])

    def test_blank_pattern_or_site_is_refused(self):
        for bad in (rule(pattern="  "), rule(site=""), rule(site="   ")):
            with self.subTest(bad=bad), self.assertRaises(SiteRuleError):
                normalize_site_rules([bad])

    def test_a_lone_star_location_pattern_is_refused(self):
        # It would put every device in one site.
        with self.assertRaises(SiteRuleError):
            normalize_site_rules([rule(pattern="*")])
        normalize_site_rules([rule(pattern="a*")])

    def test_a_site_name_that_has_no_slug_is_refused(self):
        with self.assertRaises(SiteRuleError):
            normalize_site_rules([rule(site="---")])

    def test_the_same_match_twice_is_refused(self):
        with self.assertRaises(SiteRuleError):
            normalize_site_rules([rule(pattern="Hall*"), rule(pattern="hall*")])
        with self.assertRaises(SiteRuleError):
            normalize_site_rules([rule("tag", "Prod"), rule("tag", "Prod", "other")])

    def test_a_tag_is_matched_exactly_so_case_makes_a_different_tag(self):
        normalize_site_rules([rule("tag", "Prod"), rule("tag", "prod", "other")])

    def test_two_site_names_with_the_same_slug_are_refused(self):
        with self.assertRaises(SiteRuleError):
            normalize_site_rules([rule(site="DC 1"), rule(pattern="B*", site="dc-1")])

    def test_one_site_named_twice_the_same_way_is_fine(self):
        normalize_site_rules([rule(), rule(pattern="B*")])

    def test_at_most_a_hundred_rules(self):
        many = [rule(pattern=f"Hall{i}*", site=f"s{i}") for i in range(100)]
        self.assertEqual(len(normalize_site_rules(many)), 100)
        many.append(rule(pattern="Extra*", site="s-extra"))
        with self.assertRaises(SiteRuleError):
            normalize_site_rules(many)

    def test_a_message_names_the_rule_number(self):
        with self.assertRaisesRegex(SiteRuleError, "Rule 2"):
            normalize_site_rules([rule(), rule(kind="nope")])


class QueryRulesTest(SimpleTestCase):
    def test_rank_is_the_position_so_it_is_never_stored(self):
        sent = query_site_rules(
            [rule(pattern="A*"), rule("tag", "Prod", "Core"), rule(pattern="B*")]
        )

        self.assertEqual([r["rank"] for r in sent], [1, 2, 3])
        self.assertEqual(
            set(sent[0]), {"kind", "pattern", "site", "rank"}, "the record NQE declares"
        )

    def test_the_site_is_lowercased_like_every_site_name_the_sync_makes(self):
        self.assertEqual(
            query_site_rules([rule(site="Campus-East")])[0]["site"], "campus-east"
        )

    def test_a_location_pattern_is_lowercased_and_a_tag_is_left_exact(self):
        sent = query_site_rules([rule(pattern="Hall*"), rule("tag", "Prod", "core")])

        self.assertEqual(sent[0]["pattern"], "hall*")
        self.assertEqual(sent[1]["pattern"], "Prod")

    def test_no_rules_sends_an_empty_list(self):
        self.assertEqual(query_site_rules(None), [])
        self.assertEqual(query_site_rules([]), [])


class SlugTest(SimpleTestCase):
    """The same rule the shared NQE `slugify` applies, so a rule's site name and the
    site the query names agree."""

    def test_lowercase_runs_of_other_characters_become_one_dash(self):
        self.assertEqual(site_slug("DC 1"), "dc-1")
        self.assertEqual(site_slug("Hall__A  East"), "hall-a-east")

    def test_an_ampersand_reads_as_and(self):
        self.assertEqual(site_slug("R&D"), "r-and-d")

    def test_dashes_at_the_ends_are_trimmed(self):
        self.assertEqual(site_slug("--x--"), "x")
        self.assertEqual(site_slug("---"), "")

    def test_characters_outside_ascii_letters_and_digits_are_dropped(self):
        self.assertEqual(site_slug("Zürich 2"), "z-rich-2")


class TextRoundTripTest(SimpleTestCase):
    def test_one_rule_per_line_and_it_round_trips(self):
        rules = [
            rule(pattern="Hall A*", site="Campus East"),
            rule("tag", "Prod", "core"),
        ]

        text = format_site_rules(rules)

        self.assertEqual(text, "location Hall A* -> Campus East\ntag Prod -> core")
        self.assertEqual(parse_site_rules_text(text), rules)

    def test_blank_lines_and_surrounding_space_are_ignored(self):
        self.assertEqual(
            parse_site_rules_text("\n  location Hall* -> campus  \n\n"),
            [rule(pattern="Hall*", site="campus")],
        )

    def test_a_line_that_is_not_kind_pattern_arrow_site_names_its_number(self):
        for bad in ("Hall* -> campus", "location Hall*", "location -> campus"):
            with self.subTest(bad=bad), self.assertRaisesRegex(SiteRuleError, "Line 1"):
                parse_site_rules_text(bad)

    def test_nothing_typed_is_no_rules(self):
        self.assertEqual(parse_site_rules_text(""), [])
        self.assertEqual(parse_site_rules_text(None), [])
        self.assertEqual(format_site_rules(None), "")


class InlineQueryTest(SimpleTestCase):
    """The queries the plugin builds in Python cannot import netbox_utilities, so they
    embed the same helper block. The rules reach them as a declared parameter, never
    pasted into the text (operator text in NQE would be an injection)."""

    NAMES = (
        "siteTagName",
        "siteNameFor",
        "deviceSiteName",
        "siteRuleMatches",
        "siteRuleRank",
        "siteRuleSite",
        "siteNameForRulesElse",
        "siteNameForRules",
        "deviceSiteNameRules",
    )

    def test_the_embedded_helpers_are_the_ones_in_the_shared_module(self):
        embedded = site_helper_nqe()

        for name in self.NAMES:
            with self.subTest(helper=name):
                self.assertRegex(embedded, rf"(?m)^{name}\(")

    def test_nothing_in_the_embedded_block_is_exported(self):
        self.assertNotIn("export ", site_helper_nqe())

    def test_the_block_is_the_text_of_the_shared_module_apart_from_export(self):
        shared = (QUERIES / "netbox_utilities.nqe").read_text()
        block = shared[
            shared.index("// BEGIN site naming helpers") : shared.index(
                "// END site naming helpers"
            )
        ]

        self.assertEqual(
            site_helper_nqe().strip(),
            block.split("\n", 1)[1].replace("export ", "").strip(),
        )

    def test_a_query_is_wrapped_so_it_declares_the_rules_and_ends_with_a_semicolon(
        self,
    ):
        wrapped = with_site_rules("foreach device in network.devices\nselect { n: 1 }")

        self.assertIn(
            "@query\nf(site_rules: List<{ kind: String, pattern: String, site: String, rank: Number }>) =",
            wrapped,
        )
        self.assertTrue(wrapped.rstrip().endswith("select { n: 1 };"))
        self.assertIn("siteNameForRulesElse(", wrapped)

    def test_the_rules_are_never_pasted_into_the_text(self):
        wrapped = with_site_rules("foreach device in network.devices\nselect { n: 1 }")

        self.assertNotIn("Hall", wrapped)
