# The predict stub's contract.
#
# Predict is not live, is licence-gated when it ships, and is limited today.
# The workflow must be correct without it, so these pin the two things that
# make its absence safe: it never raises, and "we could not ask" never renders
# as "we asked and it was fine".
from types import SimpleNamespace

from django.test import SimpleTestCase

from forward_netbox.change_control.predict import predict_change
from forward_netbox.change_control.predict import PredictOutcome
from forward_netbox.change_control.predict import render_predict_panel


class TheStubAnswersRatherThanRaisesTest(SimpleTestCase):
    def test_predict_returns_unavailable_today(self):
        outcome = predict_change(
            source=SimpleNamespace(parameters={}),
            snapshot_id="1",
            devices=(),
            criteria=(),
        )

        self.assertEqual(outcome.status, PredictOutcome.UNAVAILABLE)
        self.assertFalse(outcome.is_answer)

    def test_the_reason_says_the_verdict_does_not_depend_on_it(self):
        # An operator reading this must not conclude the change cannot be
        # verified. The verdict comes from the post-change snapshot, which is
        # available on every licence tier.
        outcome = predict_change(source=SimpleNamespace(parameters={}), snapshot_id="1")

        self.assertIn("verified", outcome.reason)
        self.assertIn("post-change snapshot", outcome.reason)


class ANonAnswerNeverRendersAsSuccessTest(SimpleTestCase):
    def test_unavailable_is_informational_not_an_error(self):
        panel = render_predict_panel(
            PredictOutcome(status=PredictOutcome.UNAVAILABLE, reason="not licensed")
        )

        self.assertEqual(panel["level"], "info")
        self.assertIn("not available", panel["heading"])
        self.assertEqual(panel["verdict"], "")

    def test_unsupported_is_distinct_from_unavailable(self):
        # "We could not ask" and "we asked and it could not say" mean different
        # things to someone deciding whether to approve.
        unavailable = render_predict_panel(
            PredictOutcome(status=PredictOutcome.UNAVAILABLE)
        )
        unsupported = render_predict_panel(
            PredictOutcome(status=PredictOutcome.UNSUPPORTED)
        )

        self.assertNotEqual(unavailable["heading"], unsupported["heading"])

    def test_every_panel_is_marked_advisory(self):
        for status in (
            PredictOutcome.UNAVAILABLE,
            PredictOutcome.UNSUPPORTED,
            PredictOutcome.ANSWERED,
        ):
            with self.subTest(status=status):
                panel = render_predict_panel(PredictOutcome(status=status))
                self.assertTrue(panel["advisory"])


class ThePredictFlagIsOffByDefaultTest(SimpleTestCase):
    """Off unless a deployment opts in, and the three cases stay distinct.

    "Not enabled here", "not licensed" and "not live yet" mean different things
    to someone deciding whether to approve, and collapsing them into one greyed
    panel is how a reader concludes the change was checked when it was not.
    """

    def test_a_source_with_no_parameters_is_off(self):
        from forward_netbox.change_control.predict import predict_enabled

        self.assertFalse(predict_enabled(SimpleNamespace(parameters={})))

    def test_a_missing_key_is_off_rather_than_permission(self):
        # No data migration: every existing source simply lacks the key.
        from forward_netbox.change_control.predict import predict_enabled

        self.assertFalse(
            predict_enabled(SimpleNamespace(parameters={"something_else": True}))
        )

    def test_none_parameters_is_off(self):
        from forward_netbox.change_control.predict import predict_enabled

        self.assertFalse(predict_enabled(SimpleNamespace(parameters=None)))

    def test_the_flag_turns_it_on(self):
        from forward_netbox.change_control.predict import predict_enabled

        self.assertTrue(
            predict_enabled(SimpleNamespace(parameters={"enable_predict": True}))
        )

    def test_disabled_says_it_was_not_asked(self):
        outcome = predict_change(source=SimpleNamespace(parameters={}), snapshot_id="1")

        self.assertEqual(outcome.status, PredictOutcome.UNAVAILABLE)
        self.assertIn("not enabled on this Forward source", outcome.reason)
        self.assertIn("was not asked", outcome.reason)

    def test_enabled_but_upstream_missing_says_so_differently(self):
        # An operator who turned it ON deserves to know the answer is
        # upstream, not a setting they missed.
        outcome = predict_change(
            source=SimpleNamespace(parameters={"enable_predict": True}),
            snapshot_id="1",
        )

        self.assertEqual(outcome.status, PredictOutcome.UNAVAILABLE)
        self.assertIn("enabled for this source", outcome.reason)
        self.assertIn("licensed separately", outcome.reason)
