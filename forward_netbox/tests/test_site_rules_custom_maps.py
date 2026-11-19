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

from forward_netbox.tests.test_query_execution_contract import _context
from forward_netbox.tests.test_query_execution_contract import _eligible_spec
from forward_netbox.utilities.query_execution_contract import query_source_sha256
from forward_netbox.utilities.query_fetch_execution import ForwardQueryFetcher

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
