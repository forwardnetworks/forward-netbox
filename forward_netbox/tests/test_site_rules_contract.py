# The site rules are a new parameter on the queries that name a site. The plugin
# sends every parameter the bundled query declares to whatever query a map is bound
# to, and Forward rejects one the query does not declare. So an operator who has not
# set any rules must never notice the new parameter (not in a refusal, not in the
# durable hash), and an operator who HAS set rules must never have them silently
# ignored by a query that cannot honour them.
from types import SimpleNamespace

from django.test import SimpleTestCase

from forward_netbox.tests.test_workload_state import _workload
from forward_netbox.utilities.query_execution_contract import query_source_sha256
from forward_netbox.utilities.query_execution_contract import resolve_execution_contract
from forward_netbox.utilities.query_execution_contract import scope_config_fingerprint
from forward_netbox.utilities.query_registry import QuerySpec
from forward_netbox.utilities.tier3_reducers import scope_state_from_context
from forward_netbox.utilities.workload_state import _legacy_parameter_hash
from forward_netbox.utilities.workload_state import _parameter_hash

RECORD = "List<{ kind: String, pattern: String, site: String, rank: Number }>"
UPDATED_SOURCE = f"""
@query
f(scope: List<String>, site_rules: {RECORD}) =
foreach value in scope
select {{name: value, slug: value}}
"""
OLD_SOURCE = """
@query
f(scope: List<String>) =
foreach value in scope
select {name: value, slug: value}
"""
RULES = [{"kind": "location", "pattern": "hall*", "site": "campus", "rank": 1}]


def _spec(source, **overrides):
    values = {
        "model_string": "dcim.site",
        "query_name": "Forward Sites",
        "query_id": "Q_site_query",
        "query_repository": "org",
        "resolved_query_path": "/Root/forward_locations",
        "commit_id": None,
        "map_id": 7,
        "built_in": True,
        "contract_key": "forward_locations",
        "full_query_source": source,
        "full_source_sha256": query_source_sha256(source),
        "parameters": {"scope": []},
        "coalesce_fields": (("slug",), ("name",)),
    }
    values.update(overrides)
    return QuerySpec(**values)


class QueryThatDeclaresTheRulesTest(SimpleTestCase):
    def test_no_rules_are_sent_as_an_empty_list_and_run(self):
        contract = resolve_execution_contract(
            _spec(UPDATED_SOURCE),
            effective_parameters={"scope": [], "site_rules": []},
        )

        self.assertTrue(contract.full_eligible)
        self.assertEqual(contract.full_effective_parameters["site_rules"], [])

    def test_rules_run_on_the_full_path(self):
        contract = resolve_execution_contract(
            _spec(UPDATED_SOURCE),
            effective_parameters={"scope": [], "site_rules": RULES},
        )

        self.assertTrue(contract.full_eligible)
        self.assertEqual(contract.full_effective_parameters["site_rules"], RULES)

    def test_with_rules_set_the_incremental_diff_is_not_used(self):
        # The diff revisions take no parameters, so they would name sites without
        # the rules and move devices back to their location.
        contract = resolve_execution_contract(
            _spec(UPDATED_SOURCE),
            effective_parameters={"scope": [], "site_rules": RULES},
        )

        self.assertFalse(contract.diff_eligible)
        self.assertEqual(contract.diff_reason_code, "site_rules_full_fetch")

    def test_with_no_rules_the_diff_decision_is_what_it_was(self):
        contract = resolve_execution_contract(
            _spec(UPDATED_SOURCE),
            effective_parameters={"scope": [], "site_rules": []},
        )

        self.assertNotEqual(contract.diff_reason_code, "site_rules_full_fetch")


class QueryThatDoesNotDeclareTheRulesTest(SimpleTestCase):
    """A pinned or older query that predates the parameter."""

    def test_empty_rules_are_left_out_so_the_old_query_still_runs(self):
        contract = resolve_execution_contract(
            _spec(OLD_SOURCE),
            effective_parameters={"scope": [], "site_rules": []},
        )

        self.assertTrue(contract.full_eligible)
        self.assertNotIn("site_rules", contract.full_effective_parameters)

    def test_set_rules_are_refused_not_dropped(self):
        contract = resolve_execution_contract(
            _spec(OLD_SOURCE),
            effective_parameters={"scope": [], "site_rules": RULES},
        )

        self.assertFalse(contract.full_eligible)
        self.assertEqual(contract.full_reason_code, "site_rules_not_declared")

    def test_a_query_that_never_got_the_parameter_is_unaffected(self):
        contract = resolve_execution_contract(
            _spec(OLD_SOURCE), effective_parameters={"scope": []}
        )

        self.assertTrue(contract.full_eligible)
        self.assertEqual(dict(contract.full_effective_parameters), {"scope": []})


class DurableHashTest(SimpleTestCase):
    """A source with no rules must hash exactly as it did before the parameter."""

    def _device_workloads(self, **extra):
        parameters = {"forward_netbox_shard_keys": ["router-1"], **extra}
        return [
            _workload(
                [{"name": "router-1"}],
                model_string="dcim.device",
                parameters=parameters,
            )
        ]

    def _site_workloads(self, **extra):
        return [
            _workload(
                [{"name": "a", "slug": "a"}], model_string="dcim.site", parameters=extra
            )
        ]

    def test_empty_rules_do_not_change_the_scope_independent_hash(self):
        self.assertEqual(
            _parameter_hash(self._device_workloads()),
            _parameter_hash(self._device_workloads(site_rules=[])),
        )

    def test_empty_rules_do_not_change_the_whole_network_hash(self):
        self.assertEqual(
            _parameter_hash(self._site_workloads()),
            _parameter_hash(self._site_workloads(site_rules=[])),
        )
        self.assertEqual(
            _legacy_parameter_hash(self._site_workloads()),
            _legacy_parameter_hash(self._site_workloads(site_rules=[])),
        )

    def test_rules_change_the_hash_so_a_rule_change_is_a_new_contract(self):
        self.assertNotEqual(
            _parameter_hash(self._device_workloads()),
            _parameter_hash(self._device_workloads(site_rules=RULES)),
        )
        self.assertNotEqual(
            _parameter_hash(self._site_workloads()),
            _parameter_hash(self._site_workloads(site_rules=RULES)),
        )

    def test_a_different_rule_is_a_different_hash(self):
        other = [{**RULES[0], "site": "elsewhere"}]

        self.assertNotEqual(
            _parameter_hash(self._site_workloads(site_rules=RULES)),
            _parameter_hash(self._site_workloads(site_rules=other)),
        )


class ScopeFingerprintTest(SimpleTestCase):
    """The scope fingerprint decides whether an earlier run is comparable."""

    def _context(self, **kwargs):
        return SimpleNamespace(network_id="n", **kwargs)

    def test_no_rules_fingerprint_as_before_the_rules_existed(self):
        self.assertEqual(
            scope_config_fingerprint(self._context()),
            scope_config_fingerprint(self._context(site_rules=[])),
        )
        self.assertNotIn("site_rules", scope_state_from_context(self._context()))

    def test_rules_change_the_fingerprint_and_the_scope_state(self):
        rules = [{"kind": "location", "pattern": "Hall*", "site": "campus"}]

        self.assertNotEqual(
            scope_config_fingerprint(self._context()),
            scope_config_fingerprint(self._context(site_rules=rules)),
        )
        self.assertEqual(
            scope_state_from_context(self._context(site_rules=rules))["site_rules"],
            rules,
        )

    def test_the_order_of_the_rules_is_part_of_the_fingerprint(self):
        first = {"kind": "location", "pattern": "A*", "site": "a"}
        second = {"kind": "location", "pattern": "B*", "site": "b"}

        self.assertNotEqual(
            scope_config_fingerprint(self._context(site_rules=[first, second])),
            scope_config_fingerprint(self._context(site_rules=[second, first])),
        )
