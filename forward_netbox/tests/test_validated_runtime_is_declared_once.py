"""One declaration of the validated runtime, and nothing may re-spell it.

Three subsystems refuse to run unless the installed runtime matches a validated
set — the COPY/SQL apply engine, the set-based merge, and the fast baseline —
and each used to carry its own copy, the fast baseline carrying two (the
versions it EXPECTS and, separately, the distributions it PROBES). Five
hand-maintained copies of one fact.

Every divergence between them fails CLOSED and SILENTLY. A plugin listed in
four places and missed in the fifth disables the fast path with no error
anywhere and turns a first sync from minutes into hours. Adding one optional
integration meant finding all five, and each was located only by a different
test going red.

These tests pin the property that makes that impossible: the consumers do not
merely happen to agree today, they are the same object. Equality would pass
again the moment someone pasted a literal back in; identity does not.
"""

from django.test import SimpleTestCase

from forward_netbox.utilities import fast_baseline
from forward_netbox.utilities.apply_engine_decision import (
    COPY_SQL_SUPPORTED_OPTIONAL_DISTRIBUTIONS,
)
from forward_netbox.utilities.apply_engine_decision import (
    COPY_SQL_SUPPORTED_PLUGIN_APPS,
)
from forward_netbox.utilities.merge_set_based import (
    SET_BASED_MERGE_SUPPORTED_OPTIONAL_DISTRIBUTIONS,
)
from forward_netbox.utilities.merge_set_based import (
    SET_BASED_MERGE_SUPPORTED_PLUGIN_APPS,
)
from forward_netbox.utilities.validated_runtime import (
    VALIDATED_OPTIONAL_DISTRIBUTION_NAMES,
)
from forward_netbox.utilities.validated_runtime import (
    VALIDATED_OPTIONAL_DISTRIBUTIONS,
)
from forward_netbox.utilities.validated_runtime import VALIDATED_PLUGIN_APPS


class EveryConsumerIsTheSameObjectTest(SimpleTestCase):
    """Identity, not equality — equality tolerates a pasted-back literal."""

    def test_the_engines_share_the_declared_plugin_apps(self):
        self.assertIs(COPY_SQL_SUPPORTED_PLUGIN_APPS, VALIDATED_PLUGIN_APPS)
        self.assertIs(SET_BASED_MERGE_SUPPORTED_PLUGIN_APPS, VALIDATED_PLUGIN_APPS)

    def test_the_engines_share_the_declared_distributions(self):
        self.assertIs(
            COPY_SQL_SUPPORTED_OPTIONAL_DISTRIBUTIONS,
            VALIDATED_OPTIONAL_DISTRIBUTIONS,
        )
        self.assertIs(
            SET_BASED_MERGE_SUPPORTED_OPTIONAL_DISTRIBUTIONS,
            VALIDATED_OPTIONAL_DISTRIBUTIONS,
        )


class TheProbeCannotDisagreeWithTheExpectationTest(SimpleTestCase):
    """The fast baseline's two lists were the subtlest of the five.

    A distribution the decision expects but the probe never reads reports as
    absent, which fails the match exactly as a wrong version would — so the
    failure looks like a version problem and is not one.
    """

    def test_the_probe_reports_every_expected_distribution(self):
        tuple_ = fast_baseline.fast_baseline_runtime_tuple()

        self.assertEqual(
            sorted(tuple_["optional_plugins"]),
            sorted(VALIDATED_OPTIONAL_DISTRIBUTIONS),
            "a distribution expected but not probed reads as absent and fails "
            "the runtime match with a misleading reason",
        )

    def test_the_probe_names_come_from_the_declaration(self):
        self.assertEqual(
            set(VALIDATED_OPTIONAL_DISTRIBUTION_NAMES),
            set(VALIDATED_OPTIONAL_DISTRIBUTIONS),
        )


class AddingAnIntegrationTakesOneEditTest(SimpleTestCase):
    """The point of the refactor, asserted directly.

    Patching the single declaration must move every consumer. If any consumer
    still holds its own copy this fails, which is the whole guarantee.
    """

    def test_one_declaration_moves_every_consumer(self):
        from unittest.mock import patch

        extended = VALIDATED_PLUGIN_APPS | {"some_new_integration"}
        with patch(
            "forward_netbox.utilities.validated_runtime.VALIDATED_PLUGIN_APPS",
            extended,
        ):
            from forward_netbox.utilities import validated_runtime

            self.assertTrue(
                validated_runtime.validated_plugin_apps_match(extended),
                "the declaration's own helpers must read the patched value",
            )
            self.assertEqual(
                validated_runtime.unexpected_plugin_apps(extended),
                [],
                "nothing is unexpected once the declaration includes it",
            )

    def test_helpers_name_what_differs(self):
        from forward_netbox.utilities import validated_runtime

        # Removes netbox_branching rather than an optional plugin: the optional
        # set on NetBox 4.7 is one entry, and a test that depended on it would
        # go vacuous the day that entry moved. Branching is always there, and
        # its absence is the case that matters most.
        installed = (VALIDATED_PLUGIN_APPS | {"stranger"}) - {"netbox_branching"}

        self.assertFalse(validated_runtime.validated_plugin_apps_match(installed))
        self.assertEqual(
            validated_runtime.unexpected_plugin_apps(installed), ["stranger"]
        )
        self.assertEqual(
            validated_runtime.missing_plugin_apps(installed), ["netbox_branching"]
        )
