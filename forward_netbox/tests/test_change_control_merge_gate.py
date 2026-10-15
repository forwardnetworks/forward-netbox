from uuid import uuid4

from django.apps import apps
from django.contrib.auth import get_user_model
from django.db.models import ProtectedError
from django.test import TestCase
from netbox_branching.choices import BranchStatusChoices
from netbox_branching.models import Branch
from netbox_branching.utilities import supports_branching

from forward_netbox.change_control.branch_gate import change_control_branching_resolver
from forward_netbox.change_control.branch_gate import merge_gate
from forward_netbox.change_control.choices import ForwardChangeStateChoices as State
from forward_netbox.change_control.choices import ForwardChangeVerdictChoices
from forward_netbox.change_control.choices import ForwardReviewPhaseChoices
from forward_netbox.models import ForwardChange
from forward_netbox.models import ForwardChangeCriterion
from forward_netbox.models import ForwardChangeDevice
from forward_netbox.models import ForwardChangeEvidence
from forward_netbox.models import ForwardChangePolicy
from forward_netbox.models import ForwardChangePolicyRule
from forward_netbox.models import ForwardChangeReview
from forward_netbox.models import ForwardSource

User = get_user_model()


class MergeGateTest(TestCase):
    """A branch tied to a change merges only when the change may be closed.

    The branch merges at CLOSE: a closure approval on top of a verified PROCEED.
    Merging it any other way, from the Branching page or its API, would be
    exactly the merge-on-approval step this design exists to prevent. A branch
    with no change (a sync ingestion branch, an ordinary working branch) is not
    this gate's business.
    """

    def setUp(self):
        self.source = ForwardSource.objects.create(
            name=f"gate-{uuid4().hex[:8]}",
            type="saas",
            url="https://fwd.app",
            status="ready",
            parameters={
                "username": "u@example.com",
                "password": "p",
                "verify": True,
                "network_id": "net-1",
            },
        )
        self.requester = User.objects.create(username=f"req-{uuid4().hex[:6]}")
        self.reviewer = User.objects.create(username=f"rev-{uuid4().hex[:6]}")
        self.branch = self._branch()

    def _branch(self):
        return Branch.objects.create(
            name=f"gate-{uuid4().hex[:10]}",
            schema_id=f"gate_{uuid4().hex[:10]}",
            status=BranchStatusChoices.READY,
        )

    def _change(self, state, **extra):
        values = {
            "source": self.source,
            "title": "Move the uplink",
            "state": state,
            "branch": self.branch,
            "requester": self.requester,
            "before_snapshot_id": "snap-before",
            "after_snapshot_id": "snap-after",
            "verdict": ForwardChangeVerdictChoices.PROCEED,
        }
        values.update(extra)
        return ForwardChange.objects.create(**values)

    def _approve_closure(self, change, **anchors):
        values = {
            "after_snapshot_id": change.after_snapshot_id,
            "verdict": change.verdict,
        }
        values.update(anchors)
        return ForwardChangeReview.objects.create(
            change=change,
            reviewer=self.reviewer,
            phase=ForwardReviewPhaseChoices.POST,
            approved=True,
            **values,
        )

    def test_a_branch_with_no_change_is_not_gated(self):
        self.assertTrue(merge_gate(self.branch))

    def test_an_unsaved_branch_is_not_gated(self):
        # `filter(branch_id=None)` would match every change that has no branch yet.
        self._change(State.STAGED, branch=None)

        self.assertTrue(merge_gate(Branch(name="not-saved")))

    def test_a_change_that_has_not_verified_blocks_the_merge(self):
        self._change(State.STAGED)

        verdict = merge_gate(self.branch)

        self.assertFalse(verdict)
        self.assertIn("verified PROCEED", verdict.message)

    def test_a_verified_change_without_a_closure_approval_blocks_the_merge(self):
        self._change(State.VERIFIED_PROCEED)

        verdict = merge_gate(self.branch)

        self.assertFalse(verdict)
        self.assertIn("approval", verdict.message.lower())

    def test_a_verified_change_with_a_closure_approval_may_merge(self):
        change = self._change(State.VERIFIED_PROCEED)
        self._approve_closure(change)

        self.assertTrue(merge_gate(self.branch))

    def test_the_requesters_own_approval_does_not_open_the_gate(self):
        change = self._change(State.VERIFIED_PROCEED)
        ForwardChangeReview.objects.create(
            change=change,
            reviewer=self.requester,
            phase=ForwardReviewPhaseChoices.POST,
            approved=True,
            after_snapshot_id=change.after_snapshot_id,
            verdict=change.verdict,
        )

        self.assertFalse(merge_gate(self.branch))

    def test_an_approval_of_older_evidence_does_not_open_the_gate(self):
        change = self._change(State.VERIFIED_PROCEED)
        self._approve_closure(change, after_snapshot_id="an-earlier-snapshot")

        self.assertFalse(merge_gate(self.branch))

    def test_a_pre_change_approval_does_not_stand_in_for_the_closure(self):
        change = self._change(State.VERIFIED_PROCEED)
        ForwardChangeReview.objects.create(
            change=change,
            reviewer=self.reviewer,
            phase=ForwardReviewPhaseChoices.PRE,
            approved=True,
        )

        self.assertFalse(merge_gate(self.branch))

    def test_a_held_change_cannot_merge(self):
        self._change(State.VERIFIED_HOLD, verdict=ForwardChangeVerdictChoices.HOLD)

        self.assertFalse(merge_gate(self.branch))

    def test_an_abandoned_change_cannot_merge_and_says_why(self):
        self._change(State.ABANDONED)

        verdict = merge_gate(self.branch)

        self.assertFalse(verdict)
        self.assertIn("abandoned", verdict.message.lower())

    def test_a_closed_change_is_not_blocked(self):
        self._change(State.CLOSED)

        self.assertTrue(merge_gate(self.branch))

    def test_the_gate_is_what_a_real_merge_asks(self):
        # `Branch.merge()` raises unless `can_merge` passes, and `can_merge` runs
        # the registered pre-action checks, so registering the gate is what makes
        # the Branching UI, its REST API and its merge job all refuse. A dry run
        # does not ask, so a reviewer can still preview the merge.
        self._change(State.STAGED)
        branch = Branch.objects.get(pk=self.branch.pk)

        verdict = branch.can_merge

        self.assertFalse(verdict)
        # This gate's own words, so a refusal from any other check cannot pass.
        self.assertIn("is not ready for its branch to merge", verdict.message)

    def test_deleting_a_change_cannot_free_its_live_branch(self):
        # With the change gone the gate finds nothing to refuse and the branch
        # merges with no verification and no approval: the exact merge-before-close
        # this design exists to prevent.
        change = self._change(State.STAGED)

        with self.assertRaises(ProtectedError):
            change.delete()

        self.assertTrue(ForwardChange.objects.filter(pk=change.pk).exists())
        self.assertFalse(merge_gate(self.branch))

    def test_a_bulk_delete_is_held_to_the_same_rule(self):
        change = self._change(State.VERIFIED_PROCEED)

        with self.assertRaises(ProtectedError):
            ForwardChange.objects.filter(pk=change.pk).delete()

    def test_a_finished_or_branchless_change_may_be_deleted(self):
        closed = self._change(State.CLOSED)
        closed.delete()
        abandoned = self._change(State.ABANDONED)
        abandoned.delete()
        no_branch = self._change(State.DRAFT, branch=None)
        no_branch.delete()

        self.assertFalse(ForwardChange.objects.exists())

    def test_the_gate_is_registered_once(self):
        registered = Branch._preaction_validators["merge"]

        self.assertEqual(
            [func for func in registered if func is merge_gate],
            [merge_gate],
        )


class BranchingResolverTest(TestCase):
    """Change records live on main, never in a branch.

    They are change-logged, so branching would otherwise treat them as
    branch-aware. A reviewer who approves while the change's branch is active
    would then write the approval into the branch schema, where the gate (reading
    main) never sees it, and the approval would merge along with the branch.
    """

    CHANGE_MODELS = (
        ForwardChange,
        ForwardChangeDevice,
        ForwardChangeCriterion,
        ForwardChangeEvidence,
        ForwardChangeReview,
        ForwardChangePolicy,
        ForwardChangePolicyRule,
    )

    def test_every_change_control_model_is_kept_on_main(self):
        for model in self.CHANGE_MODELS:
            with self.subTest(model=model.__name__):
                self.assertFalse(supports_branching(model))

    def test_a_change_control_model_added_later_is_covered_too(self):
        # The exemption matches on the model-name prefix, so Phase 1's comments,
        # replies and validation tables are exempt without anyone remembering to
        # extend a list.
        models = [
            model
            for model in apps.get_app_config("forward_netbox").get_models()
            if model._meta.model_name.startswith("forwardchange")
        ]

        self.assertGreaterEqual(len(models), len(self.CHANGE_MODELS))
        for model in models:
            with self.subTest(model=model.__name__):
                self.assertFalse(supports_branching(model))

    def test_the_resolver_defers_on_every_other_model(self):
        # None means "not mine": the first non-None answer wins, and answering
        # for a model that is not ours would reroute it for everyone.
        self.assertIsNone(change_control_branching_resolver(ForwardSource))
        self.assertIsNone(change_control_branching_resolver(User))
