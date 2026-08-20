"""`PreviewRunner` must answer everything the priming and classification ask of it.

2.8.7 made `compare_model_rows` prime its dependency caches, which was the right
fix and shipped with the wrong test. The tests exercised `dcim.interface`, whose
priming touches only caches `PreviewRunner` already seeded, so they passed while
the routing primers - which ask the runner for `BGPRouter`, `BGPScope`,
`OSPFInstance` and `OSPFArea` through `_optional_model` - had no stand-in at all.

Every deployment with `netbox_routing` installed then lost its entire dependency
preview to a bare `AttributeError`. One missing method, and because the preview
is a single job over all models, one model's missing attribute takes down the
measurement for every model.

A per-model test cannot close that, because the gap is exactly the models the
author did not think to enumerate. So the contract is asserted structurally
instead: whatever `sync_primitives` reads off a runner, `PreviewRunner` must
provide. That check does not need to know which models exist, which is the
property the previous tests lacked.
"""

import re
from pathlib import Path

from django.test import SimpleTestCase
from django.test import TestCase

from forward_netbox.utilities import sync_primitives
from forward_netbox.utilities.drift_comparison import PreviewRunner
from forward_netbox.utilities.drift_comparison import compare_model_rows


class PreviewRunnerSatisfiesThePrimingContractTest(SimpleTestCase):
    """Structural: no attribute the primitives read may be missing."""

    def test_every_runner_attribute_the_primitives_read_exists(self):
        source = Path(sync_primitives.__file__).read_text(encoding="utf-8")
        referenced = set(re.findall(r"runner\.(_[A-Za-z0-9_]+)", source))
        self.assertTrue(
            referenced,
            "the scan found no `runner._x` reads at all, so it is no longer "
            "checking anything - fix the pattern rather than the assertion",
        )

        runner = PreviewRunner()
        missing = sorted(name for name in referenced if not hasattr(runner, name))

        self.assertEqual(
            missing,
            [],
            "PreviewRunner is missing attributes that sync_primitives reads off "
            "a runner. Every one is an AttributeError waiting for the first "
            "deployment whose models reach that code path, and it takes the "
            "whole preview with it, not just one model.",
        )

    def test_optional_model_matches_the_real_runner_for_an_absent_plugin(self):
        """Absence raises `ForwardQueryError`, and that is the correct contract.

        Not `None`. `_prime_optional_dependency_cache` catches exactly that
        exception and returns `{}`, so the "optional" in optional-plugin is
        handled by the caller, not by the lookup. The preview must therefore
        behave as the real runner does rather than be quietly more forgiving -
        a delegate that returned `None` here would hand a `None` model to a
        primer that expects a class.
        """
        from forward_netbox.exceptions import ForwardQueryError

        runner = PreviewRunner()
        with self.assertRaises(ForwardQueryError):
            runner._optional_model(
                "not_a_real_plugin_app", "NotAModel", "not_a_real_plugin_app.notamodel"
            )


class RoutingPreviewSurvivesPrimingTest(TestCase):
    """Behavioural: the path that actually broke, exercised end to end."""

    def test_a_routing_model_preview_does_not_raise(self):
        # Whether netbox_routing is installed decides whether this compares or
        # reports "no comparison"; neither outcome may be an exception, and the
        # exception is what shipped.
        result = compare_model_rows(
            None,
            "netbox_routing.bgppeer",
            [{"device": "no-such-device.invalid", "remote_address": "192.0.2.10"}],
        )
        self.assertTrue(result is None or isinstance(result, dict))

    def test_every_contracted_model_can_be_offered_rows_without_raising(self):
        """The enumeration the previous tests were missing.

        Not an assertion about drift - most of these return `None` for "no
        comparison" and that is correct. The assertion is only that offering a
        model to the preview never raises, which is the failure mode that took
        out a deployment's whole report.
        """
        from forward_netbox.utilities.sync_contracts import MODEL_SYNC_CONTRACTS

        failures = []
        for model_string in sorted(MODEL_SYNC_CONTRACTS):
            try:
                compare_model_rows(None, model_string, [])
            except Exception as exc:  # noqa: BLE001 - the point is to catch any
                failures.append(f"{model_string}: {type(exc).__name__}: {exc}")

        self.assertEqual(failures, [], "\n".join(failures))
