"""A silently disabled fast path must say so.

Three subsystems refuse to run unless the installed plugin set exactly matches
a validated tuple: the COPY/SQL apply engine, the set-based merge, and the fast
baseline. Failing closed is correct — their SQL is generated against a known
schema — but until now the consequence was invisible. Installing any NetBox
plugin this release has not validated costs a deployment all three, and for the
fast baseline that is a first sync taking hours instead of minutes, with no
error raised and nothing in the UI that mentions it. The reason codes existed
in the decision objects; nothing displayed them.

This was found while adding an unrelated optional integration: the plugin was
added to the runtime and six fast-baseline tests began failing with
`unsupported_runtime_tuple`. A customer gets no such signal — only a slow sync.

The negative space is the important half. A warning that fires on a healthy
runtime is a warning operators learn to scroll past, so the silent case is
asserted first and hardest.
"""

from unittest.mock import patch

from django.test import SimpleTestCase

from forward_netbox.utilities.apply_engine_decision import (
    COPY_SQL_SUPPORTED_PLUGIN_APPS,
)
from forward_netbox.utilities.health import _fast_path_runtime_check


class AHealthyRuntimeSaysNothingTest(SimpleTestCase):
    """The common case must produce no row at all."""

    def test_the_validated_plugin_set_raises_no_warning(self):
        with patch(
            "django.conf.settings.PLUGINS", sorted(COPY_SQL_SUPPORTED_PLUGIN_APPS)
        ):
            self.assertIsNone(
                _fast_path_runtime_check(),
                "a validated runtime must not warn; a check that always fires "
                "is a check operators stop reading",
            )


class AnUnvalidatedPluginIsNamedTest(SimpleTestCase):
    """The actionable part is WHICH plugin, not that a tuple mismatched."""

    def test_an_extra_plugin_is_named_and_the_cost_is_stated(self):
        with patch(
            "django.conf.settings.PLUGINS",
            sorted(COPY_SQL_SUPPORTED_PLUGIN_APPS | {"some_other_plugin"}),
        ):
            check = _fast_path_runtime_check()

        self.assertIsNotNone(check)
        self.assertEqual(check["status"], "warn")
        message = check["message"]
        self.assertIn("some_other_plugin", message)
        self.assertIn("COPY/SQL", message)
        self.assertIn("set-based merge", message)
        # The consequence, in words an operator can act on rather than a
        # reason code.
        self.assertIn("hours", message)
        # And the reassurance that matters: this is slow, not broken.
        self.assertIn("still succeed", message)

    def test_a_missing_validated_plugin_is_also_named(self):
        reduced = sorted(COPY_SQL_SUPPORTED_PLUGIN_APPS - {"netbox_dlm"})
        with patch("django.conf.settings.PLUGINS", reduced):
            check = _fast_path_runtime_check()

        self.assertIsNotNone(check)
        self.assertIn("netbox_dlm", check["message"])
        self.assertIn("not installed", check["message"])

    def test_the_check_survives_a_broken_fast_baseline_probe(self):
        """A diagnostic must never be the thing that breaks the page."""
        with patch(
            "django.conf.settings.PLUGINS",
            sorted(COPY_SQL_SUPPORTED_PLUGIN_APPS | {"another_plugin"}),
        ), patch(
            "forward_netbox.utilities.fast_baseline._runtime_decision",
            side_effect=RuntimeError("probe exploded"),
        ):
            check = _fast_path_runtime_check()

        self.assertIsNotNone(check)
        self.assertIn("another_plugin", check["message"])
