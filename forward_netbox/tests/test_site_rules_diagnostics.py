# The diagnostics queries are raw NQE that declares no parameters. The site rules ride
# in the context's query parameters (so every map's preflight sees them), and Forward
# rejects a parameter a query does not declare, so the diagnostics must not be handed
# them: the bundle's diagnostic figures would vanish for every source that sets rules.
from types import SimpleNamespace
from unittest.mock import Mock
from unittest.mock import patch

from django.test import SimpleTestCase

from forward_netbox.utilities import query_diagnostics

RULES = [{"kind": "location", "pattern": "hall*", "site": "campus", "rank": 1}]


class DiagnosticsParametersTest(SimpleTestCase):
    def _sent(self, runner):
        context = SimpleNamespace(
            network_id="n",
            snapshot_id="s",
            query_parameters={"site_rules": RULES, "device_tag_include_tags": ["Prod"]},
        )
        fetcher = SimpleNamespace(client=Mock(), logger=Mock(), sync=Mock())
        with patch.object(
            query_diagnostics, "run_nqe_query", return_value=[]
        ) as run_nqe_query:
            runner(fetcher, context)
        return run_nqe_query.call_args.kwargs["parameters"]

    def test_the_ip_assignment_diagnostic_is_not_sent_the_rules(self):
        sent = self._sent(query_diagnostics.run_ipaddress_unassignable_diagnostic)

        self.assertNotIn("site_rules", sent)

    def test_the_routing_import_diagnostic_is_not_sent_the_rules(self):
        sent = self._sent(query_diagnostics.run_routing_import_diagnostic)

        self.assertNotIn("site_rules", sent)

    def test_the_aci_readiness_diagnostic_is_not_sent_the_rules(self):
        sent = self._sent(query_diagnostics.run_aci_source_readiness)

        self.assertNotIn("site_rules", sent)

    def test_every_other_parameter_is_still_passed_on_as_before(self):
        sent = self._sent(query_diagnostics.run_ipaddress_unassignable_diagnostic)

        self.assertEqual(sent, {"device_tag_include_tags": ["Prod"]})
