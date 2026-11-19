# Site rules reach a query only if it declares them. A bundled map that predates the
# rules is refused by the execution contract; a CUSTOM map (an operator's own query,
# bound by ID or stored as text) takes only the parameters it was saved with, so the
# contract never sees `site_rules` for it and it would run with the rules silently
# ignored, putting its devices at their Forward location. The preflight has to stop
# the whole run before anything is applied, as it does for an unsafe contract.
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import Mock

from django.test import SimpleTestCase

from forward_netbox.exceptions import ForwardQueryError
from forward_netbox.tests.test_query_execution_contract import _context
from forward_netbox.tests.test_query_execution_contract import _eligible_spec
from forward_netbox.utilities.query_execution_contract import query_source_sha256
from forward_netbox.utilities.query_fetch_execution import ForwardQueryFetcher
from forward_netbox.utilities.site_tag import query_site_rules

RULES = [{"kind": "location", "pattern": "hall*", "site": "campus"}]


def _custom(**overrides):
    values = {
        "built_in": False,
        "query_name": "Operator Sites",
        "contract_key": "",
        "parameters": {"scope": []},
    }
    values.update(overrides)
    return replace(_eligible_spec(), **values)


class CustomMapsAndSiteRulesTest(SimpleTestCase):
    def _plan(self, spec, *, rules, model_string="dcim.site"):
        sync = SimpleNamespace(
            pk=1, parameters={}, source=SimpleNamespace(parameters={})
        )
        fetcher = ForwardQueryFetcher(sync=sync, client=Mock(), logger_=Mock())
        fetcher._drop_unavailable_integration_models = Mock(return_value=[model_string])
        fetcher._resolve_specs_for_models = Mock(
            return_value=({model_string: [spec]}, {})
        )
        fetcher._scope_for_spec = Mock(return_value=None)
        fetcher._incremental_baseline_for_specs = Mock(return_value=None)
        # Like the real one, start from the map's own parameters.
        fetcher._query_parameters_for_scope = Mock(
            side_effect=lambda spec, context, scope: dict(spec.parameters)
        )
        fetcher._record_model_failure = Mock()
        jobs = fetcher._build_workload_jobs(
            _context(maps=[], site_rules=rules), model_strings=[model_string]
        )
        return fetcher, jobs

    def test_a_custom_site_map_with_rules_set_stops_the_run(self):
        fetcher, jobs = self._plan(_custom(), rules=RULES)

        self.assertEqual(jobs, [])
        fetcher._record_model_failure.assert_called_once()

    def test_the_refusal_names_the_map_and_how_to_fix_it(self):
        fetcher, _jobs = self._plan(_custom(), rules=RULES)

        message = str(fetcher._record_model_failure.call_args.args[3])
        self.assertIn("Operator Sites", message)
        self.assertIn("site mapping rules", message.lower())
        self.assertIn("custom query", message)

    def test_devices_and_vlans_are_site_naming_models_too(self):
        for model_string in ("dcim.device", "ipam.vlan"):
            with self.subTest(model=model_string):
                spec = _custom(model_string=model_string)
                fetcher, jobs = self._plan(spec, rules=RULES, model_string=model_string)

                self.assertEqual(jobs, [])
                fetcher._record_model_failure.assert_called_once()

    def test_the_same_map_with_no_rules_is_planned_as_it_always_was(self):
        fetcher, jobs = self._plan(_custom(), rules=[])

        self.assertEqual(len(jobs), 1)
        fetcher._record_model_failure.assert_not_called()

    def test_a_custom_map_that_declares_the_rules_is_allowed(self):
        # The operator's own query declares `site_rules` in its text and is saved
        # with the parameter, so the rules can reach it.
        source = (
            "@query\nf(scope: List<String>, site_rules: "
            "List<{ kind: String, pattern: String, site: String, rank: Number }>) =\n"
            "foreach value in scope\nselect {name: value, slug: value}\n"
        )
        spec = _custom(
            parameters={"scope": [], "site_rules": []},
            full_query_source=source,
            full_source_sha256=query_source_sha256(source),
        )

        fetcher, jobs = self._plan(spec, rules=RULES)

        fetcher._record_model_failure.assert_not_called()
        self.assertEqual(len(jobs), 1)

    def test_a_bundled_map_is_left_to_the_execution_contract(self):
        fetcher, _jobs = self._plan(_eligible_spec(), rules=RULES)

        for call in fetcher._record_model_failure.call_args_list:
            self.assertNotIn("site mapping rules", str(call.args[3]))

    def test_a_custom_map_for_a_model_that_names_no_site_is_unaffected(self):
        spec = _custom(model_string="dcim.manufacturer")

        fetcher, jobs = self._plan(spec, rules=RULES, model_string="dcim.manufacturer")

        fetcher._record_model_failure.assert_not_called()
        self.assertEqual(len(jobs), 1)


class WhatTheOperatorSeesTest(SimpleTestCase):
    """The refusal is only useful if the map's name and the fix reach the operator:
    the log keeps the first words of an exception and the model result keeps a
    reason label, so both are asserted, not just the raw exception."""

    def _logged(self, spec, rules=RULES):
        sync = SimpleNamespace(
            pk=1, parameters={}, source=SimpleNamespace(parameters={})
        )
        logger = Mock()
        fetcher = ForwardQueryFetcher(sync=sync, client=Mock(), logger_=logger)
        fetcher._drop_unavailable_integration_models = Mock(return_value=["dcim.site"])
        fetcher._resolve_specs_for_models = Mock(
            return_value=({"dcim.site": [spec]}, {})
        )
        fetcher._scope_for_spec = Mock(return_value=None)
        fetcher._incremental_baseline_for_specs = Mock(return_value=None)

        def parameters_for(spec, context, scope):
            # Like the real one: the map's own parameters, with the context's
            # rules injected into a query that declares them.
            parameters = dict(spec.parameters)
            if "site_rules" in parameters:
                parameters["site_rules"] = query_site_rules(context.site_rules)
            return parameters

        fetcher._query_parameters_for_scope = Mock(side_effect=parameters_for)
        fetcher._build_workload_jobs(
            _context(maps=[], site_rules=rules), model_strings=["dcim.site"]
        )
        return " ".join(str(call.args[0]) for call in logger.log_warning.call_args_list)

    def test_the_log_names_the_custom_map_and_says_what_to_do(self):
        logged = self._logged(_custom())

        self.assertIn("Operator Sites", logged)
        self.assertIn("custom query", logged)
        self.assertIn("Enable the bundled map", logged)

    def test_the_log_names_the_bundled_map_that_predates_the_rules(self):
        old_source = (
            "@query\nf(scope: List<String>) =\nforeach value in scope\n"
            "select {name: value, slug: value}\n"
        )
        spec = _eligible_spec(
            parameters={"scope": [], "site_rules": []},
            full_query_source=old_source,
            full_source_sha256=query_source_sha256(old_source),
        )

        logged = self._logged(spec)

        self.assertIn("Forward Sites", logged)
        self.assertIn("Publish Bundled Queries", logged)

    def test_the_summary_does_not_blame_an_unsafe_contract_for_a_custom_map(self):
        logged = self._logged(_custom())

        self.assertNotIn("has an unsafe full contract", logged)

    def test_each_site_rules_refusal_has_its_own_reason_label(self):
        from forward_netbox.utilities.diagnostics import failure_reason

        for message, label in (
            (
                "Site mapping rules are set on the Forward source, but the map X for "
                "dcim.site runs a custom query that does not take them.",
                "site-rules-custom-map",
            ),
            (
                "The query for dcim.site rejected the site mapping rules (Forward: "
                "'site_rules' is not a parameter).",
                "site-rules-query-rejected",
            ),
            (
                "Execution contract preflight rejected an unsafe full contract for "
                "dcim.site: site_rules_not_declared.",
                "site-rules-query-not-declared",
            ),
        ):
            with self.subTest(label=label):
                self.assertIn(
                    label, failure_reason(ForwardQueryError(message)).split("+")
                )


class LabelsAreNotPushedOutTest(SimpleTestCase):
    """A reason label list is capped, and the validation block reads the site-rules
    label from it, so a map whose name holds words matched earlier must not push the
    label out and let the run through."""

    def test_the_site_rules_label_survives_other_matching_words(self):
        from forward_netbox.utilities.diagnostics import failure_reason

        message = (
            "Site mapping rules are set on the Forward source, but the map "
            "license-timeout-forbidden-unauthorized for dcim.site runs a custom query "
            "that does not take them."
        )

        self.assertIn(
            "site-rules-custom-map",
            failure_reason(ForwardQueryError(message)).split("+"),
        )


class RuntimeWarningIsLoggedOnceTest(SimpleTestCase):
    def test_a_map_rejected_by_every_part_of_a_split_fetch_is_logged_once(self):
        from unittest.mock import patch

        from forward_netbox.exceptions import ForwardClientError
        from forward_netbox.utilities.query_registry import QuerySpec

        sync = SimpleNamespace(
            pk=1, parameters={}, source=SimpleNamespace(parameters={})
        )
        logger = Mock()
        fetcher = ForwardQueryFetcher(sync=sync, client=Mock(), logger_=logger)
        spec = QuerySpec(
            model_string="dcim.site",
            query_name="Forward Locations",
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
            for _part in range(3):
                try:
                    fetcher._run_nqe_query(
                        spec=spec,
                        context=_context(site_rules=RULES),
                        parameters={"site_rules": [{"kind": "tag"}]},
                    )
                except ForwardQueryError:
                    pass

        warnings = [
            call.args[0]
            for call in logger.log_warning.call_args_list
            if "rejected the site mapping rules" in str(call.args[0])
        ]
        self.assertEqual(len(warnings), 1)
