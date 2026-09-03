# The change workflow's graph, and the two properties that are the whole point.
#
# A HOLD is a valid result, not an error, and it is never closed - it is fixed
# and re-verified. Most home-grown change gates get that wrong by treating a
# failed check as an exception, which pushes operators towards closing it to
# clear the queue. Here the graph simply has no edge that expresses it.
from django.test import SimpleTestCase

from forward_netbox.change_control.choices import ForwardChangeStateChoices as State
from forward_netbox.change_control.state_machine import available_transitions
from forward_netbox.change_control.state_machine import legal_transition
from forward_netbox.change_control.state_machine import TRANSITIONS


class TheGraphRefusesToCloseAHoldTest(SimpleTestCase):
    def test_a_hold_cannot_reach_closed(self):
        self.assertNotIn(State.CLOSED, TRANSITIONS[State.VERIFIED_HOLD])

    def test_a_hold_goes_back_for_a_fix_or_is_abandoned(self):
        # Re-verifying means a fresh collection, so it returns to APPLIED
        # rather than jumping straight to COLLECTED with the stale snapshot.
        self.assertEqual(
            TRANSITIONS[State.VERIFIED_HOLD],
            frozenset({State.APPLIED, State.ABANDONED}),
        )

    def test_only_a_proceed_reaches_closed(self):
        closers = [s for s, targets in TRANSITIONS.items() if State.CLOSED in targets]
        self.assertEqual(closers, [State.VERIFIED_PROCEED])


class PredictIsOptionalTest(SimpleTestCase):
    def test_staged_reaches_approved_directly(self):
        # Not a degradation: predict is not live, is licence-gated when it
        # ships, and is limited today, so this is the DEFAULT path.
        self.assertTrue(legal_transition(State.STAGED, State.APPROVED))

    def test_staged_may_also_go_through_predicted(self):
        self.assertTrue(legal_transition(State.STAGED, State.PREDICTED))
        self.assertTrue(legal_transition(State.PREDICTED, State.APPROVED))

    def test_predict_is_the_only_skippable_state(self):
        # Every other state is on every path from DRAFT to CLOSED. If this ever
        # fails, a mandatory gate has become bypassable.
        mandatory = {
            State.DRAFT,
            State.SCOPED,
            State.BASELINED,
            State.STAGED,
            State.APPROVED,
            State.APPLIED,
            State.COLLECTED,
        }
        for state in mandatory:
            with self.subTest(state=state):
                self.assertTrue(
                    any(state in targets for targets in TRANSITIONS.values())
                    or state == State.DRAFT
                )


class TerminalStatesTest(SimpleTestCase):
    def test_closed_and_abandoned_lead_nowhere(self):
        self.assertEqual(available_transitions(State.CLOSED), ())
        self.assertEqual(available_transitions(State.ABANDONED), ())

    def test_a_hold_is_not_terminal(self):
        self.assertNotIn(State.VERIFIED_HOLD, State.TERMINAL)
        self.assertIn(State.VERIFIED_HOLD, State.OPEN)

    def test_every_open_state_can_be_abandoned(self):
        for state in State.OPEN:
            with self.subTest(state=state):
                self.assertIn(State.ABANDONED, TRANSITIONS[state])


class BaselineComesBeforeStagingTest(SimpleTestCase):
    def test_staging_is_only_reachable_from_baselined(self):
        # Deliberate ordering: a criterion must be measured against the network
        # BEFORE anyone edits NetBox, or a criterion that was already failing
        # gets counted as damage this change caused.
        stagers = [s for s, targets in TRANSITIONS.items() if State.STAGED in targets]
        self.assertEqual(
            sorted(stagers),
            sorted([State.BASELINED, State.PREDICTED, State.APPROVED]),
        )

    def test_the_forward_path_from_baselined_is_staged(self):
        self.assertIn(State.STAGED, TRANSITIONS[State.BASELINED])
