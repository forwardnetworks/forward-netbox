import re

from django.test import SimpleTestCase

from forward_netbox.utilities.query_registry import QUERY_DIR
from forward_netbox.utilities.query_registry import read_builtin_query_source

# `nqe-lsp-validate` lints two constructs clean that fail at LIVE Forward
# execution:
#   (a) a bare `let x = foreach ...` used as a value (2.2.3,
#       forward_device_analysis.nqe): `NQE_RUNTIME_ERROR: extraneous input
#       'foreach'`. Must be wrapped in parens/a call.
#   (b) `regexMatches(str, re`...`)` used directly as a boolean (2.9.7,
#       forward_ip_addresses_ipv4.nqe): it returns `List<Match>`, not `Bool`,
#       so `where regexMatches(...)` fails with `NQE_RUNTIME_ERROR: Expression
#       is not a Bool`. Must be wrapped, e.g. `!isEmpty(regexMatches(...))`.
# This test generalizes test_device_analysis_query.py's single-file guard to
# every builtin `.nqe` file, so a THIRD occurrence of either class never
# ships silently again.
#
# forward_hsrp_groups.nqe's `foreach group in ...` (8 sites) is a live-
# confirmed-correct linter false positive in the OPPOSITE direction (lint
# says error, live returns real rows) — it is neither a bare-value `foreach`
# nor a `regexMatches` boolean use, so it needs no exclusion here. See its
# own header comment and docs/00_Project_Knowledge/validation-matrix.md.

BARE_FOREACH_VALUE_RE = re.compile(r"let\s+\w+\s*=\s*foreach\b")
BOOL_CONTEXT_REGEXMATCHES_RE = re.compile(
    r"(?:\bwhere\b|\bif\b|&&|\|\|)\s+regexMatches\("
)

ALL_QUERY_FILES = sorted(path.name for path in QUERY_DIR.glob("*.nqe"))


class NqeLintBlindSpotRegexesTest(SimpleTestCase):
    """Prove the regexes actually catch the historical bad patterns, so
    narrowing them later can't silently defang this guard."""

    def test_bare_foreach_regex_catches_the_2_2_3_pattern(self):
        planted = "let cveIds =\n  foreach c in device.cveFindings select c.cveId"
        self.assertEqual(
            len(BARE_FOREACH_VALUE_RE.findall(planted)),
            1,
        )

    def test_bare_foreach_regex_ignores_wrapped_forms(self):
        planted = "let cveIds = (foreach c in device.cveFindings select c.cveId)"
        self.assertEqual(BARE_FOREACH_VALUE_RE.findall(planted), [])

    def test_bool_regexmatches_regex_catches_the_2_9_7_pattern(self):
        planted = "where regexMatches(hostIpRaw, re`^[0-9]+$`)"
        self.assertEqual(
            len(BOOL_CONTEXT_REGEXMATCHES_RE.findall(planted)),
            1,
        )

    def test_bool_regexmatches_regex_ignores_the_fixed_form(self):
        planted = "where !isEmpty(regexMatches(hostIpRaw, re`^[0-9]+$`))"
        self.assertEqual(BOOL_CONTEXT_REGEXMATCHES_RE.findall(planted), [])

    def test_bool_regexmatches_regex_ignores_foreach_iteration(self):
        planted = "foreach m in regexMatches(sysDescr, re`v(\\d+)`) select m.data"
        self.assertEqual(BOOL_CONTEXT_REGEXMATCHES_RE.findall(planted), [])


class NqeLintBlindSpotCorpusTest(SimpleTestCase):
    """Run both regexes against every builtin `.nqe` file."""

    def test_no_bare_foreach_value_in_any_query(self):
        for filename in ALL_QUERY_FILES:
            source = read_builtin_query_source(filename)
            findings = BARE_FOREACH_VALUE_RE.findall(source)
            self.assertEqual(
                findings,
                [],
                f"{filename}: bare `foreach` used as a value (wrap it in "
                f"parens or a call): {findings}",
            )

    def test_no_regexmatches_used_as_bare_boolean_in_any_query(self):
        for filename in ALL_QUERY_FILES:
            source = read_builtin_query_source(filename)
            findings = BOOL_CONTEXT_REGEXMATCHES_RE.findall(source)
            self.assertEqual(
                findings,
                [],
                f"{filename}: `regexMatches(...)` used directly as a "
                f"boolean (it returns List<Match>, not Bool - wrap with "
                f"!isEmpty(...) or similar): {findings}",
            )


class NqeLintBlindSpotNamedRegressionsTest(SimpleTestCase):
    """Named fixtures for the files most likely to regress, so a future
    failure names the file precisely rather than "some file in the corpus"."""

    def test_hsrp_groups_has_neither_bug_class(self):
        source = read_builtin_query_source("forward_hsrp_groups.nqe")
        self.assertEqual(BARE_FOREACH_VALUE_RE.findall(source), [])
        self.assertEqual(BOOL_CONTEXT_REGEXMATCHES_RE.findall(source), [])

    def test_aci_static_port_bindings_has_neither_bug_class(self):
        source = read_builtin_query_source("forward_aci_static_port_bindings.nqe")
        self.assertEqual(BARE_FOREACH_VALUE_RE.findall(source), [])
        self.assertEqual(BOOL_CONTEXT_REGEXMATCHES_RE.findall(source), [])

    def test_ip_addresses_ipv4_has_neither_bug_class(self):
        source = read_builtin_query_source("forward_ip_addresses_ipv4.nqe")
        self.assertEqual(BARE_FOREACH_VALUE_RE.findall(source), [])
        self.assertEqual(BOOL_CONTEXT_REGEXMATCHES_RE.findall(source), [])
