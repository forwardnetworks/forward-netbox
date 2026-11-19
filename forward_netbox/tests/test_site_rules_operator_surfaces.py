# A refusal about the site rules has to reach the operator on every surface, not only
# the NetBox job page. The plugin's own Logs tab and the support bundle redact warning
# text because a map's name is the customer's own word, so the plugin writes a second,
# value-free warning built only from its own vocabulary (the model, the reason label
# and fixed advice) that the redaction lets through. This also covers the audit notes
# on the same surfaces: multi-label reasons in the bundle, the Require diff advice, and
# site rules together with a sync that requires the incremental diff.
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import Mock
from unittest.mock import patch

from django.test import SimpleTestCase

from forward_netbox.exceptions import ForwardClientError
from forward_netbox.exceptions import ForwardQueryError
from forward_netbox.tests.test_query_execution_contract import _context
from forward_netbox.tests.test_query_execution_contract import _eligible_spec
from forward_netbox.utilities.diagnostics import _sanitize_log_rows
from forward_netbox.utilities.diagnostics import failure_reason
from forward_netbox.utilities.diagnostics import model_failure_summary
from forward_netbox.utilities.diagnostics import safe_site_rules_message
from forward_netbox.utilities.diagnostics import SAFE_WARNING_LOG_MESSAGE
from forward_netbox.utilities.query_execution_contract import query_source_sha256
from forward_netbox.utilities.query_fetch_execution import ForwardQueryFetcher
from forward_netbox.utilities.query_registry import QuerySpec
from forward_netbox.utilities.validation import ForwardValidationRunner

RULES = [{"kind": "location", "pattern": "hall*", "site": "campus"}]


def _warning_row(message):
    return {"level": "warning", "message": message}


def _shown(message):
    """What the plugin's Logs tab and the log export show for a warning row."""
    return _sanitize_log_rows([_warning_row(message)])[0]["message"]


def _custom(**overrides):
    values = {
        "built_in": False,
        "query_name": "Operator Sites",
        "contract_key": "",
        "parameters": {"scope": []},
    }
    values.update(overrides)
    return replace(_eligible_spec(), **values)


class TheLogsTabShowsTheValueFreeWarningTest(SimpleTestCase):
    def test_the_value_free_warning_survives_the_redaction(self):
        message = safe_site_rules_message("dcim.site", "site-rules-custom-map")

        self.assertEqual(_shown(message), message)

    def test_it_names_the_model_the_reason_and_what_to_do_and_no_map(self):
        message = safe_site_rules_message("dcim.device", "site-rules-custom-map")

        self.assertIn("dcim.device", message)
        self.assertIn("site-rules-custom-map", message)
        self.assertIn("Enable the bundled map", message)

    def test_every_cause_has_its_own_advice(self):
        published = safe_site_rules_message("dcim.site", "site-rules-query-rejected")
        diff = safe_site_rules_message("dcim.site", "site-rules-require-diff")

        self.assertIn("Publish Bundled Queries", published)
        self.assertNotIn("Publish Bundled Queries", diff)
        self.assertIn("Allow full fallback", diff)

    def test_the_full_message_with_the_maps_name_is_still_redacted(self):
        full = (
            "Site mapping rules are set on the Forward source, but the map Operator "
            "Sites for dcim.site runs a custom query that does not take them."
        )

        self.assertEqual(_shown(full), SAFE_WARNING_LOG_MESSAGE)

    def test_a_lookalike_with_other_words_is_redacted(self):
        # Only the exact sentences the plugin builds pass; the prefix alone is not
        # a way to put a customer's words through.
        forged = safe_site_rules_message("dcim.site", "site-rules-custom-map")
        forged += " Operator Sites at Hall 1."

        self.assertEqual(_shown(forged), SAFE_WARNING_LOG_MESSAGE)
        self.assertEqual(
            _shown("Site mapping rules cannot be applied for Operator Sites (x)."),
            SAFE_WARNING_LOG_MESSAGE,
        )


class EachRefusalLogsTheValueFreeWarningTest(SimpleTestCase):
    def _fetcher(self, spec, *, require_diff=False, rules=RULES, model="dcim.site"):
        sync = SimpleNamespace(
            pk=1,
            parameters=({"diff_fallback_mode": "require_diff"} if require_diff else {}),
            source=SimpleNamespace(parameters={}),
        )
        logger = Mock()
        fetcher = ForwardQueryFetcher(sync=sync, client=Mock(), logger_=logger)
        fetcher._drop_unavailable_integration_models = Mock(return_value=[model])
        fetcher._resolve_specs_for_models = Mock(return_value=({model: [spec]}, {}))
        fetcher._scope_for_spec = Mock(return_value=None)
        fetcher._incremental_baseline_for_specs = Mock(return_value=None)

        def parameters_for(spec, context, scope):
            parameters = dict(spec.parameters)
            if "site_rules" in parameters:
                from forward_netbox.utilities.site_tag import query_site_rules

                parameters["site_rules"] = query_site_rules(context.site_rules)
            return parameters

        fetcher._query_parameters_for_scope = Mock(side_effect=parameters_for)
        context = _context(maps=[], site_rules=rules)
        fetcher._build_workload_jobs(context, model_strings=[model])
        return [str(call.args[0]) for call in logger.log_warning.call_args_list]

    def test_a_custom_map_logs_it(self):
        logged = self._fetcher(_custom())

        self.assertIn(
            safe_site_rules_message("dcim.site", "site-rules-custom-map"), logged
        )

    def test_a_bundled_query_that_predates_the_rules_logs_it(self):
        old = (
            "@query\nf(scope: List<String>) =\nforeach value in scope\n"
            "select {name: value, slug: value}\n"
        )
        spec = _eligible_spec(
            parameters={"scope": [], "site_rules": []},
            full_query_source=old,
            full_source_sha256=query_source_sha256(old),
        )

        logged = self._fetcher(spec)

        self.assertIn(
            safe_site_rules_message("dcim.site", "site-rules-query-not-declared"),
            logged,
        )

    def test_the_runtime_rejection_logs_it_once(self):
        sync = SimpleNamespace(
            pk=1, parameters={}, source=SimpleNamespace(parameters={})
        )
        logger = Mock()
        fetcher = ForwardQueryFetcher(sync=sync, client=Mock(), logger_=logger)
        spec = QuerySpec(
            model_string="dcim.site",
            query_name="Operator Sites",
            query="@query f() = []",
            parameters={"site_rules": []},
        )

        def reject(client, **kwargs):
            raise ForwardClientError(
                "Provided argument, 'site_rules' is not a parameter to the given query."
            )

        with patch(
            "forward_netbox.utilities.query_fetch_execution.run_nqe_query", reject
        ):
            for _part in range(2):
                try:
                    fetcher._run_nqe_query(
                        spec=spec,
                        context=_context(site_rules=RULES),
                        parameters={"site_rules": [{"kind": "tag"}]},
                    )
                except ForwardQueryError:
                    pass

        logged = [str(call.args[0]) for call in logger.log_warning.call_args_list]
        safe = safe_site_rules_message("dcim.site", "site-rules-query-rejected")
        self.assertEqual(logged.count(safe), 1)


class SiteRulesWithRequireDiffTest(SimpleTestCase):
    """Site rules need a full fetch (the diff revisions cannot carry them), and a sync
    set to Require diff refuses any full fetch. The two cannot both be honoured, so
    say so and stop, rather than let some models fail and others apply."""

    helper = EachRefusalLogsTheValueFreeWarningTest()

    def _plan(self, *, require_diff, rules):
        logged = self.helper._fetcher(
            _custom(parameters={"scope": [], "site_rules": []}),
            require_diff=require_diff,
            rules=rules,
        )
        return logged

    def test_rules_with_require_diff_stops_the_run_and_says_what_to_change(self):
        logged = self._plan(require_diff=True, rules=RULES)

        self.assertIn(
            safe_site_rules_message("dcim.site", "site-rules-require-diff"), logged
        )
        self.assertTrue(any("Require diff" in text for text in logged))
        self.assertTrue(any("Allow full fallback" in text for text in logged))

    def test_without_rules_require_diff_is_untouched(self):
        logged = self._plan(require_diff=True, rules=[])

        self.assertFalse([text for text in logged if "ite mapping rules" in text])

    def test_rules_with_the_default_fallback_mode_are_untouched(self):
        logged = self._plan(require_diff=False, rules=RULES)

        self.assertNotIn(
            safe_site_rules_message("dcim.site", "site-rules-require-diff"), logged
        )

    def test_the_refusal_has_its_own_label(self):
        message = (
            "Site mapping rules are set and need a full fetch of dcim.site, but this "
            "sync's diff fallback mode is `Require diff`."
        )

        self.assertIn(
            "site-rules-require-diff",
            failure_reason(ForwardQueryError(message)).split("+"),
        )


class BundleKeepsEveryLabelTest(SimpleTestCase):
    def test_a_reason_of_two_labels_is_kept_whole(self):
        # The published-query refusal is two labels in one sentence; the bundle
        # turned any `+` reason into "redacted" while the page showed it.
        summary = model_failure_summary(
            [
                {
                    "model": "dcim.site",
                    "failure_count": 1,
                    "failure_exception": "ForwardQueryError",
                    "failure_reason": "site-rules-query-not-declared+unsafe-full-contract",
                }
            ]
        )

        self.assertEqual(
            summary[0]["reason"], "site-rules-query-not-declared+unsafe-full-contract"
        )

    def test_a_reason_with_anything_else_in_it_is_still_redacted(self):
        for bad in ("a+b c", "a+/etc/passwd", "a++b", "+a", "a+"):
            with self.subTest(bad=bad):
                summary = model_failure_summary(
                    [
                        {
                            "model": "dcim.site",
                            "failure_count": 1,
                            "failure_exception": "X",
                            "failure_reason": bad,
                        }
                    ]
                )

                self.assertEqual(summary[0]["reason"], "redacted")


class RequireDiffAdviceTest(SimpleTestCase):
    """The Require diff paragraph offers `Allow full fallback` as the way to recover.
    A site-rules block happens whatever the fallback mode, so it must not claim that."""

    def _reasons(self, results, *, require_diff):
        runner = ForwardValidationRunner.__new__(ForwardValidationRunner)
        runner.sync = SimpleNamespace(
            get_model_strings=lambda: ["dcim.site", "dcim.device"],
            parameters={"diff_fallback_mode": "require_diff"} if require_diff else {},
        )
        return runner._required_query_failure_reasons(results)

    def test_a_site_rules_block_alone_does_not_get_the_recovery_paragraph(self):
        reasons = self._reasons(
            [
                {
                    "model": "dcim.site",
                    "failure_count": 1,
                    "failure_reason": "site-rules-custom-map",
                }
            ],
            require_diff=True,
        )

        self.assertFalse([text for text in reasons if "recover automatically" in text])

    def test_another_query_failure_still_gets_it(self):
        # A device query failure with a child model that depends on it is a block of
        # its own, and for that one `Allow full fallback` is a real way to recover.
        runner = ForwardValidationRunner.__new__(ForwardValidationRunner)
        runner.sync = SimpleNamespace(
            get_model_strings=lambda: ["dcim.device", "dcim.interface"],
            parameters={"diff_fallback_mode": "require_diff"},
        )

        reasons = runner._required_query_failure_reasons(
            [{"model": "dcim.device", "failure_count": 1, "failure_reason": "timeout"}]
        )

        self.assertTrue([text for text in reasons if "recover automatically" in text])

    def test_both_together_keep_the_paragraph_for_the_other_failure(self):
        runner = ForwardValidationRunner.__new__(ForwardValidationRunner)
        runner.sync = SimpleNamespace(
            get_model_strings=lambda: ["dcim.device", "dcim.interface", "dcim.site"],
            parameters={"diff_fallback_mode": "require_diff"},
        )

        reasons = runner._required_query_failure_reasons(
            [
                {
                    "model": "dcim.device",
                    "failure_count": 1,
                    "failure_reason": "timeout",
                },
                {
                    "model": "dcim.site",
                    "failure_count": 1,
                    "failure_reason": "site-rules-custom-map",
                },
            ]
        )

        self.assertTrue([text for text in reasons if "recover automatically" in text])

    def test_the_require_diff_cause_has_its_own_advice(self):
        reasons = self._reasons(
            [
                {
                    "model": "dcim.site",
                    "failure_count": 1,
                    "failure_reason": "site-rules-require-diff",
                }
            ],
            require_diff=True,
        )
        text = " ".join(reasons)

        self.assertIn("Allow full fallback", text)
        self.assertIn("dcim.site", text)
        self.assertNotIn("Publish Bundled Queries", text)


class SmokeFetchPathTest(SimpleTestCase):
    """The smoke-test fetch (`_query_jobs`) plans its own jobs and had no custom-map
    check: with rules set it would have shown a custom map's devices unruled."""

    def _jobs(self, spec, rules):
        sync = SimpleNamespace(
            pk=1, parameters={}, source=SimpleNamespace(parameters={})
        )
        logger = Mock()
        fetcher = ForwardQueryFetcher(sync=sync, client=Mock(), logger_=logger)
        fetcher._drop_unavailable_integration_models = Mock(return_value=["dcim.site"])
        fetcher._resolve_specs_for_models = Mock(
            return_value=({"dcim.site": [spec]}, {})
        )
        fetcher._query_parameters_for_scope = Mock(
            side_effect=lambda spec, context, scope: dict(spec.parameters)
        )
        fetcher._record_model_failure = Mock()
        jobs = fetcher._query_jobs(
            _context(maps=[], site_rules=rules), model_strings=["dcim.site"]
        )
        return (
            fetcher,
            jobs,
            [str(c.args[0]) for c in logger.log_warning.call_args_list],
        )

    def test_a_custom_map_with_rules_set_plans_no_job_and_says_why(self):
        fetcher, jobs, logged = self._jobs(_custom(), RULES)

        self.assertEqual(jobs, [])
        fetcher._record_model_failure.assert_called_once()
        self.assertIn("Operator Sites", " ".join(logged))
        self.assertIn(
            safe_site_rules_message("dcim.site", "site-rules-custom-map"), logged
        )

    def test_with_no_rules_the_same_map_is_planned(self):
        fetcher, jobs, _logged = self._jobs(_custom(), [])

        self.assertEqual(len(jobs), 1)
        fetcher._record_model_failure.assert_not_called()

    def test_a_bundled_map_is_planned(self):
        fetcher, jobs, _logged = self._jobs(_eligible_spec(), RULES)

        self.assertEqual(len(jobs), 1)
        fetcher._record_model_failure.assert_not_called()


class TheTwoStandaloneModulesAgreeTest(SimpleTestCase):
    """`diagnostics` and the execution contract are each loaded on their own (by the
    standalone scripts tests and by the release preflight), so neither can import the
    other at load time and they keep the same reason codes as plain strings."""

    def test_the_reason_codes_are_the_same_strings(self):
        from forward_netbox.utilities import diagnostics
        from forward_netbox.utilities import query_execution_contract as contract

        for name in (
            "SITE_RULES_NOT_DECLARED_REASON",
            "SITE_RULES_CUSTOM_MAP_REASON",
            "SITE_RULES_REQUIRE_DIFF_REASON",
        ):
            with self.subTest(code=name):
                self.assertEqual(getattr(contract, name), getattr(diagnostics, name))

    def test_the_hint_is_the_advice_for_each_code(self):
        from forward_netbox.utilities import diagnostics
        from forward_netbox.utilities.query_execution_contract import (
            contract_reason_hint,
        )

        for code, advice in diagnostics.SITE_RULES_ADVICE.items():
            with self.subTest(code=code):
                self.assertEqual(contract_reason_hint(code), advice)

    def test_a_code_with_no_advice_has_no_hint(self):
        from forward_netbox.utilities.query_execution_contract import (
            contract_reason_hint,
        )

        self.assertEqual(contract_reason_hint("unsafe_full_parameters"), "")


class AdviceForTheRealCauseTest(SimpleTestCase):
    """A custom device map refused for site rules also makes dcim.device fail, and
    with a child model enabled that read as 'the device query failed' with advice
    (Allow full fallback, Publish Bundled Queries) that fixes nothing here."""

    def _reasons(self, reason):
        runner = ForwardValidationRunner.__new__(ForwardValidationRunner)
        runner.sync = SimpleNamespace(
            get_model_strings=lambda: ["dcim.device", "dcim.interface"],
            parameters={"diff_fallback_mode": "require_diff"},
        )
        return runner._required_query_failure_reasons(
            [{"model": "dcim.device", "failure_count": 1, "failure_reason": reason}]
        )

    def test_a_site_rules_refusal_is_not_also_reported_as_the_device_query_failing(
        self,
    ):
        reasons = self._reasons("site-rules-custom-map")

        self.assertEqual(len(reasons), 1)
        self.assertIn("Site mapping rules", reasons[0])
        self.assertFalse([t for t in reasons if "depend on device coverage" in t])
        self.assertFalse([t for t in reasons if "recover automatically" in t])

    def test_a_real_device_query_failure_is_still_reported(self):
        reasons = self._reasons("timeout")

        self.assertTrue([t for t in reasons if "depend on device coverage" in t])
        self.assertTrue([t for t in reasons if "recover automatically" in t])


class RequireDiffPreflightIsAboutTheQueryTest(SimpleTestCase):
    """The conflict is between Require diff and a query that takes the rules. A
    device map that names no site (an ACI inventory) is not affected."""

    helper = EachRefusalLogsTheValueFreeWarningTest()

    def test_a_bundled_device_map_that_declares_no_rules_is_not_blocked(self):
        spec = _eligible_spec(
            model_string="dcim.device",
            parameters={"scope": []},
        )

        logged = self.helper._fetcher(
            spec, require_diff=True, rules=RULES, model="dcim.device"
        )

        self.assertFalse([text for text in logged if "need a full fetch" in text])

    def test_a_bundled_map_that_declares_the_rules_is_blocked(self):
        source = (
            "@query\nf(scope: List<String>, site_rules: "
            "List<{ kind: String, pattern: String, site: String, rank: Number }>) =\n"
            "foreach value in scope\nselect {name: value, slug: value}\n"
        )
        spec = _eligible_spec(
            parameters={"scope": [], "site_rules": []},
            full_query_source=source,
            full_source_sha256=query_source_sha256(source),
        )

        logged = self.helper._fetcher(spec, require_diff=True, rules=RULES)

        self.assertTrue([text for text in logged if "need a full fetch" in text])
