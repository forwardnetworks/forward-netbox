# The site rules an operator saves on the Forward source have to reach exactly the
# queries that name a site, in every filter mode, and no other query: a query that
# does not declare a parameter rejects it. These tests pin how the rules get from
# the source to the query parameters.
from pathlib import Path
from unittest.mock import Mock
from unittest.mock import patch

from django.test import SimpleTestCase
from django.test import TestCase

from forward_netbox.exceptions import ForwardClientError
from forward_netbox.exceptions import ForwardQueryError
from forward_netbox.models import ForwardSource
from forward_netbox.models import ForwardSync
from forward_netbox.utilities import query_registry
from forward_netbox.utilities.query_fetch_execution import ForwardQueryContext
from forward_netbox.utilities.query_fetch_execution import ForwardQueryFetcher
from forward_netbox.utilities.query_registry import QuerySpec
from forward_netbox.utilities.site_tag import normalize_site_rules

QUERIES = Path(__file__).resolve().parents[1] / "queries"
SITE_NAMING_MAPS = (
    "forward_devices",
    "forward_devices_with_netbox_aliases",
    "forward_vlans",
    "forward_locations",
)
RULES = normalize_site_rules(
    [
        {"kind": "location", "pattern": "Hall*", "site": "Campus"},
        {"kind": "tag", "pattern": "Prod", "site": "core"},
    ]
)


class RegistryDefaultsTest(SimpleTestCase):
    def test_the_maps_that_name_a_site_default_the_rules_to_none(self):
        for name in SITE_NAMING_MAPS:
            with self.subTest(query=name):
                defaults = query_registry._default_query_parameters(f"{name}.nqe")
                self.assertEqual(defaults["site_rules"], [])

    def test_no_other_shipped_query_gets_the_rules(self):
        # netbox_utilities is copied into about thirty queries; reading the compiled
        # text for the name would give all of them a parameter they do not declare.
        for path in sorted(QUERIES.glob("*.nqe")):
            if path.stem in SITE_NAMING_MAPS or path.stem == "netbox_utilities":
                continue
            with self.subTest(query=path.name):
                self.assertNotIn(
                    "site_rules", query_registry._default_query_parameters(path.name)
                )

    def test_the_list_of_site_naming_queries_is_the_queries_that_declare_the_rules(
        self,
    ):
        declared = {
            path.name
            for path in QUERIES.glob("*.nqe")
            if "site_rules" in query_registry._default_query_parameters(path.name)
        }

        self.assertEqual(declared, set(query_registry.SITE_NAMING_QUERY_FILES))

    def test_the_default_is_one_the_old_query_retry_may_drop(self):
        self.assertEqual(
            query_registry.LEGACY_SAFE_PARAMETER_DEFAULTS["site_rules"], []
        )


class _SourceCase(TestCase):
    def setUp(self):
        self.source = ForwardSource.objects.create(
            name="rules-source",
            type="saas",
            url="https://fwd.app",
            status="ready",
            parameters={"network_id": "test-network"},
        )
        self.sync = ForwardSync.objects.create(
            name="rules-sync",
            source=self.source,
            parameters={"snapshot_id": "latestProcessed"},
        )
        self.fetcher = ForwardQueryFetcher(
            sync=self.sync, client=Mock(), logger_=Mock()
        )

    def _context(self, **kwargs):
        return ForwardQueryContext(
            network_id="test-network",
            snapshot_selector="latestProcessed",
            snapshot_id="snapshot-1",
            **kwargs,
        )

    def _spec(self, parameters):
        return QuerySpec(
            model_string="dcim.site",
            query_name="Forward Locations",
            query="@query f() = []",
            parameters=parameters,
        )


class InjectionTest(_SourceCase):
    def test_the_rules_are_sent_with_their_position_as_rank(self):
        spec = self._spec({"forward_netbox_shard_keys": [], "site_rules": []})

        sent = self.fetcher._apply_context_tag_parameters(
            spec, {"forward_netbox_shard_keys": []}, self._context(site_rules=RULES)
        )

        self.assertEqual(
            sent["site_rules"],
            [
                {"kind": "location", "pattern": "hall*", "site": "campus", "rank": 1},
                {"kind": "tag", "pattern": "Prod", "site": "core", "rank": 2},
            ],
        )

    def test_no_rules_sends_an_empty_list_because_forward_needs_the_parameter(self):
        spec = self._spec({"site_rules": []})

        sent = self.fetcher._apply_context_tag_parameters(spec, {}, self._context())

        self.assertEqual(sent["site_rules"], [])

    def test_a_query_that_does_not_declare_the_rules_is_not_sent_them(self):
        spec = self._spec({"forward_netbox_shard_keys": []})

        sent = self.fetcher._apply_context_tag_parameters(
            spec, {"forward_netbox_shard_keys": []}, self._context(site_rules=RULES)
        )

        self.assertNotIn("site_rules", sent)

    def test_the_rules_are_sent_alongside_the_device_tag_parameters(self):
        # The source's device-tag filter mode must not decide whether site rules
        # reach the query: the tag parameters take their own path.
        spec = self._spec(
            {
                "device_tag_include_tags": [],
                "device_tag_include_match": "any",
                "device_tag_exclude_tags": [],
                "site_rules": [],
            }
        )

        sent = self.fetcher._apply_context_tag_parameters(
            spec,
            {},
            self._context(
                site_rules=RULES,
                device_tag_include_tags=["Prod"],
                device_tag_include_match="all",
            ),
        )

        self.assertEqual(len(sent["site_rules"]), 2)
        self.assertEqual(sent["device_tag_include_tags"], ["Prod"])

    def test_the_context_reports_the_rules(self):
        self.assertEqual(self._context(site_rules=RULES).as_dict()["site_rules"], RULES)


class ResolveContextTest(_SourceCase):
    def _resolve(self):
        # The snapshot lookups are not what these tests are about.
        fetcher = ForwardQueryFetcher(sync=self.sync, client=Mock(), logger_=Mock())
        fetcher._resolve_snapshot_info = Mock(return_value={})
        self.sync.get_network_id = Mock(return_value="test-network")
        self.sync.get_snapshot_id = Mock(return_value="latestProcessed")
        self.sync.resolve_snapshot_id = Mock(return_value="snapshot-after")
        self.sync.get_query_parameters = Mock(return_value={})
        self.sync.get_maps = Mock(return_value=[])
        return fetcher

    def test_the_rules_on_the_source_reach_the_context(self):
        self.source.parameters["site_rules"] = RULES
        self.source.save(update_fields=["parameters"])

        context = self._resolve().resolve_context()

        self.assertEqual(context.site_rules, RULES)

    def test_a_source_with_no_rules_has_an_empty_list(self):
        self.assertEqual(self._resolve().resolve_context().site_rules, [])

    def test_changed_rules_are_a_different_context_not_a_cached_one(self):
        fetcher = self._resolve()
        first = fetcher.resolve_context()
        self.source.parameters["site_rules"] = RULES
        self.source.save(update_fields=["parameters"])

        second = fetcher.resolve_context()

        self.assertEqual(first.site_rules, [])
        self.assertEqual(second.site_rules, RULES)

    def test_rules_saved_by_hand_that_are_not_valid_stop_the_sync_with_the_reason(self):
        self.source.parameters["site_rules"] = [
            {"kind": "location", "pattern": "*", "site": "campus"}
        ]
        self.source.save(update_fields=["parameters"])

        with self.assertRaisesRegex(ForwardQueryError, "site rules.*Rule 1"):
            self._resolve().resolve_context()


class StaleQueryAtRuntimeTest(_SourceCase):
    """An org copy of a query that predates the rules: Forward rejects the parameter."""

    REJECTION = "Provided argument, 'site_rules' is not a parameter to the given query."

    def _run(self, parameters, run_nqe_query):
        spec = QuerySpec(
            model_string="dcim.site",
            query_name="Forward Locations",
            query="@query f() = []",
            parameters={"site_rules": []},
        )
        with patch(
            "forward_netbox.utilities.query_fetch_execution.run_nqe_query",
            run_nqe_query,
        ):
            return self.fetcher._run_nqe_query(
                spec=spec,
                context=self._context(),
                parameters=parameters,
            )

    def test_no_rules_the_query_is_retried_without_the_parameter(self):
        calls = []

        def run(client, **kwargs):
            calls.append(dict(kwargs["parameters"]))
            if "site_rules" in kwargs["parameters"]:
                raise ForwardClientError(self.REJECTION)
            return [{"name": "campus"}]

        rows = self._run({"scope": [], "site_rules": []}, run)

        self.assertEqual(rows, [{"name": "campus"}])
        self.assertEqual(calls, [{"scope": [], "site_rules": []}, {"scope": []}])

    def test_the_run_remembers_so_it_does_not_ask_twice(self):
        calls = []

        def run(client, **kwargs):
            calls.append(dict(kwargs["parameters"]))
            if "site_rules" in kwargs["parameters"]:
                raise ForwardClientError(self.REJECTION)
            return []

        self._run({"scope": [], "site_rules": []}, run)
        calls.clear()
        self._run({"scope": [], "site_rules": []}, run)

        self.assertEqual(calls, [{"scope": []}])

    def test_rules_that_are_set_stop_the_sync_and_say_what_to_do(self):
        def run(client, **kwargs):
            raise ForwardClientError(self.REJECTION)

        with self.assertRaisesRegex(
            ForwardQueryError, "site mapping rules.*Publish Bundled Queries"
        ):
            self._run({"scope": [], "site_rules": [{"kind": "tag"}]}, run)

    def test_any_other_rejected_parameter_is_still_an_error(self):
        def run(client, **kwargs):
            raise ForwardClientError(
                "Provided argument, 'other' is not a parameter to the given query."
            )

        with self.assertRaises(ForwardClientError):
            self._run({"scope": [], "site_rules": []}, run)


class SyncQueryParametersTest(_SourceCase):
    """`ForwardSync.get_query_parameters` feeds the older single-stage path and every
    contract preflight, so the rules must be in it whatever the filter mode."""

    def _params(self, **source):
        self.source.parameters.update(source)
        self.source.save(update_fields=["parameters"])
        return self.sync.get_query_parameters()

    def test_no_rules_adds_nothing(self):
        self.assertEqual(self._params(), {})

    def test_rules_are_included_in_the_default_filter_mode(self):
        sent = self._params(site_rules=RULES)

        self.assertEqual([rule["rank"] for rule in sent["site_rules"]], [1, 2])
        self.assertEqual(sent["site_rules"][0]["site"], "campus")

    def test_rules_are_included_alongside_the_query_parameters_filter_mode(self):
        sent = self._params(
            site_rules=RULES,
            device_tag_filter_mode="query_parameters",
            device_tag_include_tags=["Prod"],
        )

        self.assertEqual(sent["device_tag_include_tags"], ["Prod"])
        self.assertEqual(len(sent["site_rules"]), 2)

    def test_saved_rules_that_are_not_valid_stop_with_the_reason(self):
        with self.assertRaisesRegex(Exception, "site rules.*Rule 1"):
            self._params(site_rules=[{"kind": "tag", "pattern": "", "site": "x"}])


class _Stop(Exception):
    """Raised by a patched run_nqe_query to capture the query and stop the caller."""


class InlineQueriesTest(_SourceCase):
    """The queries the plugin builds in Python. With no rules they are the 3.0.16 text
    and send no parameter; with rules they use the shared helpers and receive the
    rules as a declared parameter, never written into the text."""

    def _scoped(self, site_rules):
        captured = {}

        def run(client, **kwargs):
            captured.update(kwargs)
            return []

        self.fetcher._warn_if_scope_all_backfilled = Mock()
        with patch("forward_netbox.utilities.query_fetch_execution.run_nqe_query", run):
            self.fetcher._resolve_scoped_tag_scope(
                network_id="n",
                snapshot_id="s",
                include_tags=["Prod"],
                exclude_tags=[],
                include_match="any",
                site_rules=site_rules,
            )
        return captured

    def _reconciliation(self, rules):
        from forward_netbox.utilities import scope_reconciliation

        captured = {}

        def run(client, **kwargs):
            captured.update(kwargs)
            raise _Stop

        self.source.parameters["site_rules"] = rules
        self.source.parameters["device_tag_include_tags"] = ["Prod"]
        self.source.save(update_fields=["parameters"])
        self.sync.get_network_id = Mock(return_value="n")
        self.sync.resolve_snapshot_id = Mock(return_value="s")
        with patch.object(scope_reconciliation, "run_nqe_query", run), patch.object(
            ForwardSource, "get_client", Mock(return_value=Mock())
        ):
            with self.assertRaises(_Stop):
                scope_reconciliation.compute_scope_reconciliation(self.sync)
        return captured

    def test_the_tag_scope_query_with_no_rules_is_the_old_text_with_no_parameter(self):
        call = self._scoped([])

        self.assertNotIn("parameters", call)
        self.assertIn("let site_tag = min(", call["query"])
        self.assertNotIn("@query", call["query"])

    def test_the_tag_scope_query_with_rules_names_the_site_through_the_helpers(self):
        call = self._scoped(RULES)

        self.assertIn("deviceSiteNameRules(device, site_rules)", call["query"])
        self.assertIn("@query\nf(site_rules:", call["query"])
        self.assertEqual(
            [rule["rank"] for rule in call["parameters"]["site_rules"]], [1, 2]
        )

    def test_the_rules_are_not_written_into_the_query_text(self):
        call = self._scoped(RULES)

        self.assertNotIn("Hall", call["query"])
        self.assertNotIn("hall*", call["query"])

    def test_the_reconciliation_query_with_no_rules_is_the_old_text(self):
        call = self._reconciliation([])

        self.assertNotIn("parameters", call)
        self.assertNotIn("@query", call["query"])

    def test_the_reconciliation_query_with_rules_has_no_location_when_nothing_places_it(
        self,
    ):
        # The reconciliation compares locations to NetBox sites and uses an empty
        # string, not "unknown", for a device that has neither.
        call = self._reconciliation(RULES)

        self.assertIn("siteNameForRulesElse(site_rules, device.tagNames", call["query"])
        self.assertTrue(call["query"].rstrip().rstrip(";").rstrip().endswith("}"))
        self.assertIn('else "", "")', call["query"])
        self.assertEqual(len(call["parameters"]["site_rules"]), 2)

    def test_saved_rules_that_are_not_valid_stop_the_reconciliation_with_the_reason(
        self,
    ):
        from forward_netbox.utilities import scope_reconciliation

        self.source.parameters["site_rules"] = [
            {"kind": "location", "pattern": "*", "site": "x"}
        ]
        self.source.save(update_fields=["parameters"])
        self.sync.get_network_id = Mock(return_value="n")
        self.sync.resolve_snapshot_id = Mock(return_value="s")

        with patch.object(
            ForwardSource, "get_client", Mock(return_value=Mock())
        ), self.assertRaisesRegex(ValueError, "site rules.*Rule 1"):
            scope_reconciliation.compute_scope_reconciliation(self.sync)


class ReconciliationReportTest(_SourceCase):
    """The Scope page reads how many devices each rule places from the report."""

    def _report(self, rules, rows):
        from forward_netbox.utilities import scope_reconciliation

        calls = []

        def run(client, **kwargs):
            calls.append(kwargs)
            return list(rows) if len(calls) == 1 else []

        if rules:
            self.source.parameters["site_rules"] = rules
        self.source.parameters["device_tag_include_tags"] = ["Prod"]
        self.source.save(update_fields=["parameters"])
        self.sync.get_network_id = Mock(return_value="n")
        self.sync.resolve_snapshot_id = Mock(return_value="s")
        with patch.object(scope_reconciliation, "run_nqe_query", run), patch.object(
            ForwardSource, "get_client", Mock(return_value=Mock())
        ):
            report = scope_reconciliation.compute_scope_reconciliation(self.sync)
        return report, calls[0]

    def _row(self, name, rank, tags=("Prod",)):
        return {
            "name": name,
            "completed": True,
            "reason": "completed",
            "tagNames": list(tags),
            "location": "somewhere",
            "siteRuleRank": rank,
        }

    def test_the_report_counts_the_devices_each_rule_places(self):
        report, _call = self._report(
            RULES,
            [
                self._row("a", 1),
                self._row("b", 1),
                self._row("c", 2),
                self._row("d", 0),
            ],
        )

        summary = report["site_rules"]
        self.assertTrue(summary["available"])
        self.assertEqual([r["devices"] for r in summary["rules"]], [2, 1])
        self.assertEqual(summary["by_location"], 1)

    def test_the_rules_query_asks_for_each_devices_matching_rank(self):
        _report, call = self._report(RULES, [self._row("a", 1)])

        self.assertIn("let rule_rank = siteRuleRank(site_rules", call["query"])
        self.assertIn("siteRuleRank: if isPresent(rule_rank)", call["query"])

    def test_with_no_rules_the_report_says_so_and_the_query_is_the_old_one(self):
        report, call = self._report([], [self._row("a", 0)])

        self.assertFalse(report["site_rules"]["available"])
        self.assertNotIn("siteRuleRank", call["query"])


class StaleLegacyQueryTest(_SourceCase):
    """An old org copy bound by path that also lacks an older default parameter.

    3.0.16 rescued it by retrying with no parameters. Dropping `site_rules` must lead
    into that same rescue, not around it."""

    def test_dropping_the_rules_then_still_gets_the_legacy_retry(self):
        calls = []

        def run(client, **kwargs):
            calls.append(dict(kwargs["parameters"]))
            if "site_rules" in kwargs["parameters"]:
                raise ForwardClientError(
                    "Provided argument, 'site_rules' is not a parameter to the given query."
                )
            if kwargs["parameters"]:
                raise ForwardClientError(
                    "Provided argument, 'forward_netbox_shard_keys' is not a parameter "
                    "to the given query."
                )
            return [{"name": "campus"}]

        spec = QuerySpec(
            model_string="dcim.site",
            query_name="Forward Locations",
            query_path="/Root/forward_locations",
            query_repository="org",
            resolved_query_id="Q_locations",
            parameters={"forward_netbox_shard_keys": [], "site_rules": []},
        )
        with patch("forward_netbox.utilities.query_fetch_execution.run_nqe_query", run):
            rows = self.fetcher._run_nqe_query(
                spec=spec,
                context=self._context(),
                parameters={"forward_netbox_shard_keys": [], "site_rules": []},
            )

        self.assertEqual(rows, [{"name": "campus"}])
        self.assertEqual(
            calls,
            [
                {"forward_netbox_shard_keys": [], "site_rules": []},
                {"forward_netbox_shard_keys": []},
                {},
            ],
        )
