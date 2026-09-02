# Two gaps the failure-reason work recorded and left open.
#
# 1. The reason catalogue is matched against MESSAGE TEXT, so a message
#    reworded by a future change silently stops resolving to its slug and falls
#    back to `unrecognized-fetch-failure`. Visible rather than silent, but
#    nothing asserted the needles still match anything the plugin says.
# 2. `Job.error` sentences composed with NO exception exported as
#    `<redacted diagnostic>` - plugin-authored sentences interpolating a
#    plugin-defined enum, so safe in full, but carrying no classifier for the
#    recovery to find.
import ast
import re
from pathlib import Path

from django.test import SimpleTestCase

from forward_netbox.choices import ForwardSyncStatusChoices
from forward_netbox.utilities import diagnostics
from forward_netbox.utilities.diagnostics import REDACTED_DIAGNOSTIC
from forward_netbox.utilities.diagnostics import safe_job_error_summary

# Where the plugin-authored needles begin. Everything before this slug comes
# from a transport library's wording, which this repository does not control
# and cannot assert against its own source.
_FIRST_PLUGIN_AUTHORED_SLUG = "license-tier-denied"


def _plugin_authored_needles():
    rules = list(diagnostics._FAILURE_REASON_RULES)
    start = next(
        index
        for index, (slug, _needle) in enumerate(rules)
        if slug == _FIRST_PLUGIN_AUTHORED_SLUG
    )
    return rules[start:]


def _plugin_message_literals():
    """Every string literal the plugin composes, as the runtime sees it.

    Parsed rather than grepped. Python merges implicit concatenation - the
    `"... is not " f"contractually safe ({code})."` shape these messages are
    written in - into ONE constant, so a needle spanning a line break is
    present at runtime and invisible to a text search. Two slugs looked dead
    for exactly that reason while their messages were intact.

    f-string parts are kept separately: an interpolated value sits between
    them at runtime, so a needle cannot span one there either.
    """
    root = Path(diagnostics.__file__).parent.parent
    literals = []
    for path in sorted(root.rglob("*.py")):
        parts = set(path.parts)
        if "tests" in parts or "migrations" in parts or path.name == "diagnostics.py":
            continue
        try:
            tree = ast.parse(path.read_text(encoding="utf-8"))
        except SyntaxError:  # pragma: no cover - a file we cannot read is not evidence
            continue
        for node in ast.walk(tree):
            if isinstance(node, ast.Constant) and isinstance(node.value, str):
                literals.append(node.value)
    return [re.sub(r"\s+", " ", literal.casefold()) for literal in literals]


class EveryPluginAuthoredNeedleStillMatchesTest(SimpleTestCase):
    """A slug whose needle matches nothing is a rename nobody noticed."""

    def test_each_needle_appears_in_a_message_the_plugin_composes(self):
        literals = _plugin_message_literals()
        # The needle is matched against the casefolded exception text, so it is
        # compared casefolded here too, with whitespace collapsed on both sides
        # because these messages are wrapped across source lines.
        missing = [
            slug
            for slug, needle in _plugin_authored_needles()
            if not any(re.sub(r"\s+", " ", needle) in literal for literal in literals)
        ]
        self.assertEqual(
            missing,
            [],
            "these failure-reason slugs match no message the plugin composes "
            "any more, so the reasons they name now fall back to "
            f"`unrecognized-fetch-failure`: {missing}",
        )

    def test_the_readers_find_something(self):
        # A reader that silently found nothing would pass the test above.
        needles = _plugin_authored_needles()
        self.assertGreater(len(needles), 10)
        self.assertEqual(needles[0][0], _FIRST_PLUGIN_AUTHORED_SLUG)
        self.assertGreater(len(_plugin_message_literals()), 1000)

    def test_implicit_concatenation_is_joined_the_way_the_runtime_joins_it(self):
        # The bug this reader was written around: `"a " f"b{x}"` is ONE
        # constant at runtime, so a needle spanning that break is present.
        self.assertTrue(
            any(
                "not contractually safe" in literal
                for literal in _plugin_message_literals()
            )
        )


class ExceptionFreeJobErrorsReadBackTest(SimpleTestCase):
    """Plugin-authored sentences with no classifier are safe in full."""

    def test_a_sync_status_sentence_survives_readback(self):
        for status, _label, _colour in ForwardSyncStatusChoices.CHOICES:
            message = f"Forward sync ended with status {status}."
            self.assertEqual(safe_job_error_summary(message), message, status)

    def test_every_shape_compiled(self):
        """Zero shapes redacts everything, silently and plausibly.

        The allowlist drops any shape whose enum it cannot read, which is
        right when a plugin is absent and indistinguishable from having got
        the enum's API wrong - `ChoiceSet.values` is a method, and reading it
        as a list dropped every pattern. Assert the count, not just that one
        sentence survives.
        """
        from forward_netbox.utilities.diagnostics import safe_job_error_shape_count

        self.assertEqual(safe_job_error_shape_count(), 3)

    def test_an_enum_value_outside_the_choice_set_is_redacted(self):
        # The distinction the character class could not make.
        self.assertEqual(
            safe_job_error_summary("Forward sync ended with status finished."),
            REDACTED_DIAGNOSTIC,
        )

    def test_the_interrupted_merge_sentence_survives_readback(self):
        message = (
            "Forward merge cannot be retried after the interrupted job; "
            "the authoritative branch state is missing."
        )
        self.assertEqual(safe_job_error_summary(message), message)

    def test_a_reworded_sentence_is_redacted_rather_than_admitted(self):
        # The safe direction: the allowlist pins whole wording, so a change to
        # the message stops matching instead of widening what gets through.
        self.assertEqual(
            safe_job_error_summary("Forward sync ended with status failed today."),
            REDACTED_DIAGNOSTIC,
        )

    def test_the_interpolated_value_cannot_carry_customer_data(self):
        # The shape is built from the enum's own members, so it admits exactly
        # the sentences the plugin can compose. `x` is here because the first
        # spelling used `[a-z_]+` and admitted it - a lowercase token this code
        # did not write - which an existing harness test caught before the tag.
        for value in ("Mgmt_Vl211", "leaf-101", "10.0.0.1", "tenant name", "x"):
            self.assertEqual(
                safe_job_error_summary(f"Forward sync ended with status {value}."),
                REDACTED_DIAGNOSTIC,
                value,
            )

    def test_an_ordinary_classified_error_is_unchanged(self):
        self.assertEqual(
            safe_job_error_summary("Forward sync failed (ForwardQueryError)."),
            "Job failed (ForwardQueryError).",
        )

    def test_an_arbitrary_message_is_still_redacted(self):
        self.assertEqual(
            safe_job_error_summary("something went wrong on leaf-101"),
            REDACTED_DIAGNOSTIC,
        )
