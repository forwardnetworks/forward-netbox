# How many approvals a gate needs, and who counts.
#
# Before this module, ForwardChangePolicy was inert: nothing read
# min_approvals, nothing evaluated a rule, and any single approving row opened
# the gate. An approval policy nobody enforces is worse than none, because the
# page implies a control that is not there.
from types import SimpleNamespace

from django.test import SimpleTestCase
from django.test import TestCase

from forward_netbox.change_control.choices import ForwardReviewPhaseChoices
from forward_netbox.change_control.policy import DEFAULT_REQUIRED_APPROVALS
from forward_netbox.change_control.policy import distinct_approvers
from forward_netbox.change_control.policy import required_approvals
from forward_netbox.change_control.policy import stale_approvals


class AnUnmatchedChangeIsNotEasierToMergeTest(TestCase):
    def test_no_policy_still_requires_an_approval(self):
        # The failure mode this guards: adding a policy to cover one estate
        # quietly leaving everything else ungoverned.
        change = SimpleNamespace(
            devices=SimpleNamespace(select_related=lambda *a: []),
        )

        self.assertEqual(
            required_approvals(change, ForwardReviewPhaseChoices.PRE),
            DEFAULT_REQUIRED_APPROVALS,
        )
        self.assertEqual(
            required_approvals(change, ForwardReviewPhaseChoices.POST),
            DEFAULT_REQUIRED_APPROVALS,
        )


class SelfApprovalIsNotApprovalTest(SimpleTestCase):
    def _change(self, reviews, requester_id=7):
        return SimpleNamespace(
            requester_id=requester_id,
            reviews=SimpleNamespace(filter=lambda **kw: reviews),
        )

    def test_the_requester_does_not_count(self):
        reviews = [SimpleNamespace(reviewer_id=7)]

        self.assertEqual(
            distinct_approvers(self._change(reviews), ForwardReviewPhaseChoices.PRE),
            set(),
        )

    def test_one_reviewer_counts_once(self):
        # The uniqueness constraint stops this at write time; counting
        # distinctly means a bypass of that constraint still cannot inflate.
        reviews = [SimpleNamespace(reviewer_id=3), SimpleNamespace(reviewer_id=3)]

        self.assertEqual(
            distinct_approvers(self._change(reviews), ForwardReviewPhaseChoices.PRE),
            {3},
        )

    def test_a_review_with_no_reviewer_does_not_count(self):
        reviews = [SimpleNamespace(reviewer_id=None)]

        self.assertEqual(
            distinct_approvers(self._change(reviews), ForwardReviewPhaseChoices.PRE),
            set(),
        )


class TheTwoGatesAnchorToDifferentEvidenceTest(SimpleTestCase):
    def _change(self, reviews, **kw):
        base = dict(
            branch_last_change_time="t1",
            before_snapshot_id="snap-before",
            after_snapshot_id="snap-after",
            verdict="proceed",
        )
        base.update(kw)
        return SimpleNamespace(
            reviews=SimpleNamespace(filter=lambda **f: reviews), **base
        )

    def test_a_pre_approval_is_voided_by_a_new_baseline(self):
        review = SimpleNamespace(
            branch_change_time="t1",
            baseline_snapshot_id="snap-old",
            after_snapshot_id="",
            verdict="",
        )
        change = self._change([review])

        self.assertEqual(len(stale_approvals(change, ForwardReviewPhaseChoices.PRE)), 1)

    def test_a_post_approval_is_voided_by_re_verification(self):
        # The reviewer signed off on specific evidence; a newer snapshot is
        # different evidence.
        review = SimpleNamespace(
            branch_change_time="t1",
            baseline_snapshot_id="snap-before",
            after_snapshot_id="snap-older",
            verdict="proceed",
        )
        change = self._change([review])

        self.assertEqual(
            len(stale_approvals(change, ForwardReviewPhaseChoices.POST)), 1
        )

    def test_a_post_approval_is_voided_by_the_verdict_changing(self):
        review = SimpleNamespace(
            branch_change_time="t1",
            baseline_snapshot_id="snap-before",
            after_snapshot_id="snap-after",
            verdict="hold",
        )
        change = self._change([review])

        self.assertEqual(
            len(stale_approvals(change, ForwardReviewPhaseChoices.POST)), 1
        )

    def test_matching_anchors_are_not_stale(self):
        review = SimpleNamespace(
            branch_change_time="t1",
            baseline_snapshot_id="snap-before",
            after_snapshot_id="snap-after",
            verdict="proceed",
        )
        change = self._change([review])

        self.assertEqual(stale_approvals(change, ForwardReviewPhaseChoices.PRE), [])
        self.assertEqual(stale_approvals(change, ForwardReviewPhaseChoices.POST), [])
