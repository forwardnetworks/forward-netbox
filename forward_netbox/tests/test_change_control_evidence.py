# The regression-flip comparison, which is why evidence is recorded at a
# baseline as well as after the change.
#
# Without a before phase, a criterion that was ALREADY failing is counted as
# damage this change caused, and the gate holds a change that broke nothing.
from django.test import SimpleTestCase

from forward_netbox.change_control.evidence import BLOCKING_FLIPS
from forward_netbox.change_control.evidence import classify_flip
from forward_netbox.change_control.evidence import FIX
from forward_netbox.change_control.evidence import PRE_EXISTING
from forward_netbox.change_control.evidence import PRESERVED
from forward_netbox.change_control.evidence import REGRESSION


class TheFourFlipsTest(SimpleTestCase):
    def test_pass_then_fail_is_a_regression(self):
        self.assertEqual(classify_flip(True, False), REGRESSION)

    def test_fail_then_pass_is_a_fix(self):
        self.assertEqual(classify_flip(False, True), FIX)

    def test_fail_then_fail_is_pre_existing(self):
        # The case the baseline exists for. This must NOT block: the change
        # neither caused it nor was asked to fix it.
        self.assertEqual(classify_flip(False, False), PRE_EXISTING)

    def test_pass_then_pass_is_preserved(self):
        self.assertEqual(classify_flip(True, True), PRESERVED)


class OnlyARegressionBlocksTest(SimpleTestCase):
    def test_exactly_one_flip_blocks(self):
        self.assertEqual(BLOCKING_FLIPS, frozenset({REGRESSION}))

    def test_pre_existing_failure_does_not_block(self):
        self.assertNotIn(PRE_EXISTING, BLOCKING_FLIPS)

    def test_a_fix_does_not_block(self):
        self.assertNotIn(FIX, BLOCKING_FLIPS)

    def test_preserved_state_does_not_block(self):
        self.assertNotIn(PRESERVED, BLOCKING_FLIPS)


class NotMeasuredIsNotAPassTest(SimpleTestCase):
    """A missing phase must stay a third answer all the way to the verdict.

    Coercing it to a pass is precisely how an empty comparison once read as a
    successful one, which cost a release to find and another to explain.
    """

    def test_a_missing_before_phase_has_no_flip(self):
        self.assertEqual(classify_flip(None, True), "")

    def test_a_missing_after_phase_has_no_flip(self):
        self.assertEqual(classify_flip(True, None), "")

    def test_both_missing_has_no_flip(self):
        self.assertEqual(classify_flip(None, None), "")


class TheTwoFamiliesBlockDifferentlyTest(SimpleTestCase):
    """Acceptance and state preservation ask different questions.

    Acceptance asserts the change ACHIEVED ITS INTENT, so what matters is the
    after state on its own: a criterion still failing means the change did not
    work, and letting that through because it was already failing would verify
    a change that accomplished nothing.

    State preservation asserts nothing ELSE moved, so a pre-existing failure is
    genuinely not this change's business.
    """

    # NOT `_outcome`: unittest.TestCase sets an internal `_outcome`
    # attribute during run(), which shadows a method of that name and
    # fails with "'_Outcome' object is not callable".
    def _make_outcome(self, family, flip):
        from types import SimpleNamespace

        from forward_netbox.change_control.evidence import CriterionOutcome

        return CriterionOutcome(
            criterion=SimpleNamespace(name="c", family=family),
            before_passed=None,
            after_passed=None,
            flip=flip,
            blocking=True,
        )

    def test_a_failing_acceptance_criterion_blocks_even_if_pre_existing(self):
        from forward_netbox.change_control.choices import (
            ForwardCriterionFamilyChoices,
        )

        outcome = self._make_outcome(
            ForwardCriterionFamilyChoices.ACCEPTANCE, PRE_EXISTING
        )

        self.assertTrue(outcome.blocks)
        self.assertIn("did not achieve", outcome.block_reason)

    def test_a_pre_existing_state_preservation_failure_does_not_block(self):
        from forward_netbox.change_control.choices import (
            ForwardCriterionFamilyChoices,
        )

        outcome = self._make_outcome(
            ForwardCriterionFamilyChoices.STATE_PRESERVATION, PRE_EXISTING
        )

        self.assertFalse(outcome.blocks)

    def test_a_regression_blocks_in_both_families(self):
        from forward_netbox.change_control.choices import (
            ForwardCriterionFamilyChoices,
        )

        for family in (
            ForwardCriterionFamilyChoices.ACCEPTANCE,
            ForwardCriterionFamilyChoices.STATE_PRESERVATION,
        ):
            with self.subTest(family=family):
                self.assertTrue(self._make_outcome(family, REGRESSION).blocks)

    def test_a_fix_never_blocks(self):
        from forward_netbox.change_control.choices import (
            ForwardCriterionFamilyChoices,
        )

        for family in (
            ForwardCriterionFamilyChoices.ACCEPTANCE,
            ForwardCriterionFamilyChoices.STATE_PRESERVATION,
        ):
            with self.subTest(family=family):
                self.assertFalse(self._make_outcome(family, FIX).blocks)
