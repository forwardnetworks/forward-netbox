from uuid import uuid4

from django.contrib.auth import get_user_model
from users.models import Group

from forward_netbox.change_control.choices import ForwardChangeStateChoices as State
from forward_netbox.change_control.choices import ForwardChangeVerdictChoices
from forward_netbox.change_control.choices import ForwardReviewPhaseChoices as Phase
from forward_netbox.change_control.state_machine import closure_check
from forward_netbox.change_control.state_machine import plan_approval_check
from forward_netbox.models import ForwardChangeApprovalRule
from forward_netbox.models import ForwardChangeDevice
from forward_netbox.models import ForwardChangePolicy
from forward_netbox.models import ForwardChangePolicyRule
from forward_netbox.models import ForwardChangeReview
from forward_netbox.tests.test_change_control_actions import ChangeFixture

User = get_user_model()


class RuleFixture(ChangeFixture):
    """A reviewer rule says WHO must approve and how many; a policy scope says WHEN.

    A policy with no scope rules applies to every change, so these tests use one and
    add the scope only where the scope is the subject.
    """

    def setUp(self):
        super().setUp()
        self.policy = ForwardChangePolicy.objects.create(name=f"p-{uuid4().hex[:6]}")
        self.group = Group.objects.create(name=f"architects-{uuid4().hex[:6]}")
        self.alice = self.user("alice", self.group)
        self.bob = self.user("bob", self.group)
        self.mallory = self.user("mallory")
        self.change_ = self.change(
            State.STAGED, branch=self.branch, before_snapshot_id="base"
        )

    def user(self, name, group=None):
        user = User.objects.create(username=f"{name}-{uuid4().hex[:5]}")
        if group is not None:
            user.groups.add(group)
        return user

    def rule(self, **kwargs):
        rule = ForwardChangeApprovalRule.objects.create(
            policy=self.policy, name=f"r-{uuid4().hex[:5]}", **kwargs
        )
        return rule

    def approve(self, user, change=None, phase=Phase.PRE, **anchors):
        change = change or self.change_
        if phase == Phase.PRE:
            anchors.setdefault("baseline_snapshot_id", change.before_snapshot_id)
        else:
            anchors.setdefault("after_snapshot_id", change.after_snapshot_id)
            anchors.setdefault("verdict", change.verdict)
        return ForwardChangeReview.objects.create(
            change=change, reviewer=user, phase=phase, approved=True, **anchors
        )

    def plan_blockers(self):
        return " ".join(plan_approval_check(self.change_).blockers)


class EligibleReviewersTest(RuleFixture):
    def test_groups_and_named_reviewers_are_both_eligible(self):
        rule = self.rule()
        rule.groups.add(self.group)
        rule.reviewers.add(self.mallory)

        self.assertEqual(
            rule.eligible_user_ids(),
            {self.alice.pk, self.bob.pk, self.mallory.pk},
        )

    def test_a_rule_marked_any_reviewer_lets_anyone_count(self):
        # None, not an empty set: an empty set would mean nobody is eligible.
        self.assertIsNone(self.rule(any_reviewer=True).eligible_user_ids())

    def test_a_rule_with_no_groups_or_users_is_open_to_nobody(self):
        self.assertEqual(self.rule().eligible_user_ids(), set())


class MinimumReviewsFromAGroupTest(RuleFixture):
    def test_only_members_of_the_group_count_toward_the_minimum(self):
        rule = self.rule(min_reviews=2)
        rule.groups.add(self.group)
        self.approve(self.alice)
        self.approve(self.mallory)

        blockers = self.plan_blockers()

        self.assertIn(rule.name, blockers)
        self.assertIn("1 more", blockers)

    def test_two_members_satisfy_it(self):
        rule = self.rule(min_reviews=2)
        rule.groups.add(self.group)
        self.approve(self.alice)
        self.approve(self.bob)

        self.assertTrue(plan_approval_check(self.change_).allowed)

    def test_the_blocker_names_who_may_review(self):
        rule = self.rule(min_reviews=1)
        rule.groups.add(self.group)

        blockers = self.plan_blockers()

        self.assertIn(self.group.name, blockers)


class NamedReviewersTest(RuleFixture):
    def test_an_approval_from_someone_not_named_does_not_satisfy_the_rule(self):
        rule = self.rule()
        rule.reviewers.add(self.alice)
        self.approve(self.mallory)

        self.assertFalse(plan_approval_check(self.change_).allowed)

    def test_an_approval_from_the_named_reviewer_does(self):
        rule = self.rule()
        rule.reviewers.add(self.alice)
        self.approve(self.alice)

        self.assertTrue(plan_approval_check(self.change_).allowed)


class AnyReviewerRuleTest(RuleFixture):
    def test_the_minimum_is_a_count_of_distinct_reviewers(self):
        self.rule(min_reviews=2, any_reviewer=True)
        self.approve(self.alice)
        self.assertFalse(plan_approval_check(self.change_).allowed)

        self.approve(self.mallory)
        self.assertTrue(plan_approval_check(self.change_).allowed)


class DeletedEligibilityTest(RuleFixture):
    """A rule whose reviewers are gone must block, not quietly open to everyone."""

    def test_deleting_the_only_named_reviewer_does_not_open_the_rule(self):
        rule = self.rule()
        rule.reviewers.add(self.alice)
        self.alice.delete()
        self.approve(self.mallory)

        self.assertFalse(plan_approval_check(self.change_).allowed)
        self.assertIn("nobody", self.plan_blockers())

    def test_deleting_the_only_group_does_not_open_the_rule(self):
        rule = self.rule()
        rule.groups.add(self.group)
        self.group.delete()
        self.approve(self.mallory)

        self.assertFalse(plan_approval_check(self.change_).allowed)


class WhatDoesNotCountTest(RuleFixture):
    def test_a_deactivated_reviewers_approval_does_not_count(self):
        rule = self.rule(min_reviews=1)
        rule.reviewers.add(self.alice)
        self.approve(self.alice)
        type(self.alice).objects.filter(pk=self.alice.pk).update(is_active=False)

        self.assertFalse(plan_approval_check(self.change_).allowed)

    def test_a_stale_approval_does_not_count_toward_a_rule(self):
        rule = self.rule(min_reviews=1)
        rule.reviewers.add(self.alice)
        self.approve(self.alice, baseline_snapshot_id="an-earlier-baseline")

        blockers = self.plan_blockers()

        self.assertFalse(plan_approval_check(self.change_).allowed)
        self.assertIn(rule.name, blockers)

    def test_the_requesters_own_approval_never_counts(self):
        # The policy minimum is 0 so that only the rule can be what refuses.
        ForwardChangePolicy.objects.filter(pk=self.policy.pk).update(
            min_pre_approvals=0
        )
        rule = self.rule(min_reviews=1)
        rule.reviewers.add(self.requester)
        self.approve(self.requester)

        self.assertFalse(plan_approval_check(self.change_).allowed)
        self.assertIn(rule.name, self.plan_blockers())


class WhichPoliciesApplyTest(RuleFixture):
    def test_a_rule_on_a_policy_that_does_not_apply_is_ignored(self):
        # The policy is scoped to a site the change does not touch.
        from dcim.models import Site

        elsewhere = Site.objects.create(
            name=f"elsewhere-{uuid4().hex[:5]}", slug=f"elsewhere-{uuid4().hex[:5]}"
        )
        ForwardChangePolicyRule.objects.create(policy=self.policy, site=elsewhere)
        rule = self.rule(min_reviews=2)
        rule.reviewers.add(self.alice)
        ForwardChangeDevice.objects.create(
            change=self.change_, device=self.device(), forward_device_key="d1"
        )
        self.approve(self.mallory)

        self.assertTrue(plan_approval_check(self.change_).allowed)

    def test_the_rules_of_every_applicable_policy_must_hold(self):
        other = ForwardChangePolicy.objects.create(name=f"o-{uuid4().hex[:6]}")
        first = self.rule(min_reviews=1)
        first.reviewers.add(self.alice)
        second = ForwardChangeApprovalRule.objects.create(
            policy=other, name="second", min_reviews=1
        )
        second.reviewers.add(self.bob)
        self.approve(self.alice)

        self.assertFalse(plan_approval_check(self.change_).allowed)
        self.assertIn("second", self.plan_blockers())

    def test_a_disabled_policy_asks_for_nothing(self):
        rule = self.rule(min_reviews=3)
        rule.reviewers.add(self.alice)
        ForwardChangePolicy.objects.filter(pk=self.policy.pk).update(enabled=False)
        self.approve(self.mallory)

        self.assertTrue(plan_approval_check(self.change_).allowed)


class RulesApplyAtBothGatesTest(RuleFixture):
    def test_a_rule_holds_the_closure_too(self):
        rule = self.rule(min_reviews=2)
        rule.groups.add(self.group)
        change = self.change(
            State.VERIFIED_PROCEED,
            branch=None,
            after_snapshot_id="after",
            verdict=ForwardChangeVerdictChoices.PROCEED,
        )
        self.approve(self.alice, change=change, phase=Phase.POST)

        blocked = closure_check(change)
        self.assertFalse(blocked.allowed)

        self.approve(self.bob, change=change, phase=Phase.POST)
        self.assertTrue(closure_check(change).allowed)
