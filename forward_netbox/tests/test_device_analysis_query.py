import re

from django.test import SimpleTestCase

from forward_netbox.utilities.query_registry import read_builtin_query_source


class DeviceAnalysisQueryTest(SimpleTestCase):
    """Guard the device-analysis NQE against the 2.2.3 regression: a bare
    ``let x = foreach ... select ...`` is INVALID NQE — the live Forward engine
    returns HTTP 400 (``extraneous input 'foreach'``) and the whole
    refresh-device-analysis job errors. The local NQE linter does NOT catch this
    (it reported 0 errors on the broken query), so this source-level test is the
    real guard. A ``foreach`` used as a value must be wrapped in parens / a call.
    """

    def test_cve_ids_comprehension_is_parenthesized(self):
        source = read_builtin_query_source("forward_device_analysis.nqe")
        self.assertIn("(foreach c in device.cveFindings", source)

    def test_no_bare_foreach_in_let_binding(self):
        source = read_builtin_query_source("forward_device_analysis.nqe")
        # A `let <name> =` immediately followed by a bare `foreach` is the invalid
        # pattern; wrapped forms (`(foreach`, `length(foreach`) are fine.
        bare = re.findall(r"let\s+\w+\s*=\s*\n\s*foreach\b", source)
        self.assertEqual(bare, [], f"bare foreach in a let binding: {bare}")
