from datetime import timedelta
from unittest.mock import patch
from uuid import uuid4

from dcim.models import Device
from dcim.models import DeviceRole
from dcim.models import DeviceType
from dcim.models import Manufacturer
from dcim.models import Site
from django.contrib.auth import get_user_model
from django.contrib.contenttypes.models import ContentType
from django.core.exceptions import ValidationError
from django.db import ProgrammingError
from django.test import TestCase
from django.utils import timezone
from extras.models import JournalEntry
from netbox_branching.choices import BranchStatusChoices
from netbox_branching.models import Branch

from forward_netbox.change_control.actions import attest_applied
from forward_netbox.change_control.actions import BRANCH_UNREADABLE
from forward_netbox.change_control.actions import close_change
from forward_netbox.change_control.actions import record_review
from forward_netbox.change_control.actions import review_anchor
from forward_netbox.change_control.actions import sync_branch_state
from forward_netbox.change_control.actions import transition
from forward_netbox.change_control.choices import ForwardChangeStateChoices as State
from forward_netbox.change_control.choices import ForwardChangeVerdictChoices
from forward_netbox.change_control.choices import ForwardReviewPhaseChoices
from forward_netbox.change_control.status import review_status
from forward_netbox.models import ForwardChange
from forward_netbox.models import ForwardChangeCriterion
from forward_netbox.models import ForwardChangeDevice
from forward_netbox.models import ForwardChangeReview
from forward_netbox.models import ForwardSource

User = get_user_model()


class ChangesAt:
    """A stand-in for `Branch.get_changes()` with one change at `when`, or none."""

    def __init__(self, when=None):
        self.when = when

    def order_by(self, *_):
        return self

    def first(self):
        if self.when is None:
            return None

        class _Change:
            time = self.when

        return _Change()


class ChangeFixture(TestCase):
    def setUp(self):
        # A test branch has no schema; the branch's own edits are not the subject
        # here, so it has none unless a test says otherwise.
        patcher = patch.object(Branch, "get_changes", return_value=ChangesAt())
        patcher.start()
        self.addCleanup(patcher.stop)
        self.source = ForwardSource.objects.create(
            name=f"act-{uuid4().hex[:8]}",
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
        self.branch = Branch.objects.create(
            name=f"act-{uuid4().hex[:10]}",
            schema_id=f"act_{uuid4().hex[:10]}",
            status=BranchStatusChoices.READY,
        )

    def change(self, state=State.DRAFT, **extra):
        values = {
            "source": self.source,
            "title": "Move the uplink",
            "state": state,
            "requester": self.requester,
        }
        values.update(extra)
        return ForwardChange.objects.create(**values)

    def device(self):
        suffix = uuid4().hex[:6]
        site = Site.objects.create(name=f"site-{suffix}", slug=f"site-{suffix}")
        mfr = Manufacturer.objects.create(name=f"m-{suffix}", slug=f"m-{suffix}")
        dtype = DeviceType.objects.create(
            manufacturer=mfr, model=f"t-{suffix}", slug=f"t-{suffix}"
        )
        role = DeviceRole.objects.create(name=f"r-{suffix}", slug=f"r-{suffix}")
        return Device.objects.create(
            name=f"dev-{suffix}", site=site, device_type=dtype, role=role
        )

    def review(self, change, phase, approved=True, **anchors):
        return ForwardChangeReview.objects.create(
            change=change,
            reviewer=self.reviewer,
            phase=phase,
            approved=approved,
            **anchors,
        )


class TransitionTest(ChangeFixture):
    def test_a_legal_transition_moves_the_state_and_is_journalled(self):
        change = self.change(State.DRAFT)

        outcome = transition(change, State.ABANDONED, self.requester)

        self.assertTrue(outcome.ok)
        change.refresh_from_db()
        self.assertEqual(change.state, State.ABANDONED)
        entries = JournalEntry.objects.filter(
            assigned_object_type=ContentType.objects.get_for_model(ForwardChange),
            assigned_object_id=change.pk,
        )
        self.assertEqual(entries.count(), 1)
        entry = entries.get()
        self.assertEqual(entry.created_by, self.requester)
        self.assertIn(State.DRAFT, entry.comments)
        self.assertIn(State.ABANDONED, entry.comments)

    def test_a_blocked_transition_changes_nothing_and_says_what_is_missing(self):
        change = self.change(State.DRAFT)

        outcome = transition(change, State.SCOPED, self.requester)

        self.assertFalse(outcome.ok)
        self.assertTrue(outcome.blockers)
        change.refresh_from_db()
        self.assertEqual(change.state, State.DRAFT)
        self.assertFalse(
            JournalEntry.objects.filter(
                assigned_object_type=ContentType.objects.get_for_model(ForwardChange),
                assigned_object_id=change.pk,
            )
        )

    def test_an_edge_the_graph_does_not_have_is_refused(self):
        change = self.change(State.DRAFT)

        outcome = transition(change, State.APPROVED, self.requester)

        self.assertFalse(outcome.ok)
        change.refresh_from_db()
        self.assertEqual(change.state, State.DRAFT)

    def test_a_finished_change_goes_nowhere(self):
        change = self.change(State.CLOSED)

        outcome = transition(change, State.DRAFT, self.requester)

        self.assertFalse(outcome.ok)

    def test_scoping_needs_devices_and_a_blocking_criterion(self):
        change = self.change(State.DRAFT)
        ForwardChangeDevice.objects.create(
            change=change, device=self.device(), forward_device_key="dev-1"
        )
        ForwardChangeCriterion.objects.create(
            change=change, name="uplink up", blocking=True
        )

        outcome = transition(change, State.SCOPED, self.requester)

        self.assertTrue(outcome.ok, outcome.blockers)
        change.refresh_from_db()
        self.assertEqual(change.state, State.SCOPED)

    def test_attesting_the_apply_records_who_and_when_and_moves_the_change(self):
        change = self.change(State.APPROVED)
        when = timezone.now() - timedelta(minutes=5)

        outcome = attest_applied(
            change, self.requester, applied_at=when, ref="CHG-1234"
        )

        self.assertTrue(outcome.ok, outcome.blockers)
        change.refresh_from_db()
        self.assertEqual(change.state, State.APPLIED)
        self.assertEqual(change.applied_by, self.requester)
        self.assertEqual(change.applied_at, when)
        self.assertEqual(change.applied_ref, "CHG-1234")

    def test_a_change_is_closed_by_merging_its_branch_not_by_a_button(self):
        # CLOSED is where the branch merges. Setting the state without the merge
        # would let the page say "closed" over a branch that never merged, and
        # the gate treats CLOSED as already merged.
        change = self.change(
            State.VERIFIED_PROCEED,
            branch=self.branch,
            after_snapshot_id="after",
            verdict=ForwardChangeVerdictChoices.PROCEED,
        )
        self.review(
            change,
            ForwardReviewPhaseChoices.POST,
            after_snapshot_id="after",
            verdict=ForwardChangeVerdictChoices.PROCEED,
        )

        refused = transition(change, State.CLOSED, self.requester)
        self.assertFalse(refused.ok)
        self.assertIn("merge", " ".join(refused.blockers).lower())

        Branch.objects.filter(pk=self.branch.pk).update(
            status=BranchStatusChoices.MERGED
        )
        change.refresh_from_db()
        allowed = transition(change, State.CLOSED, self.requester)
        self.assertTrue(allowed.ok, allowed.blockers)


class BranchStateTest(ChangeFixture):
    """Approvals are anchored to the branch; the anchor needs a writer."""

    def test_the_branch_name_and_last_change_time_are_recorded(self):
        change = self.change(State.STAGED, branch=self.branch)
        when = timezone.now()

        with patch.object(Branch, "get_changes", return_value=ChangesAt(when)):
            sync_branch_state(change)

        change.refresh_from_db()
        self.assertEqual(change.branch_name, self.branch.name)
        self.assertEqual(change.branch_last_change_time, when)

    def test_a_branch_edited_after_approval_makes_the_approval_stale(self):
        approved_at = timezone.now() - timedelta(hours=2)
        change = self.change(
            State.STAGED,
            branch=self.branch,
            before_snapshot_id="base",
            branch_last_change_time=approved_at,
        )
        self.review(
            change,
            ForwardReviewPhaseChoices.PRE,
            branch_change_time=approved_at,
            baseline_snapshot_id="base",
        )
        with patch.object(Branch, "get_changes", return_value=ChangesAt(approved_at)):
            first = transition(change, State.APPROVED, self.requester)
        self.assertTrue(first.ok, first.blockers)
        ForwardChange.objects.filter(pk=change.pk).update(state=State.STAGED)
        change.refresh_from_db()

        with patch.object(
            Branch,
            "get_changes",
            return_value=ChangesAt(timezone.now()),
        ):
            second = transition(change, State.APPROVED, self.requester)

        self.assertFalse(second.ok)
        self.assertIn("stale", " ".join(second.blockers).lower())


class RecordReviewTest(ChangeFixture):
    """The anchor a review records must be the branch as it is now."""

    def staged(self):
        return self.change(
            State.STAGED,
            branch=self.branch,
            before_snapshot_id="base",
        )

    def test_a_first_approval_is_not_stale_because_nobody_synced_the_anchor(self):
        # The branch has edits and the stored anchor is still empty. Recording the
        # anchor from the stale field would make every first approval stale, so
        # every change would need a second round for a branch nobody touched.
        change = self.staged()
        when = timezone.now() - timedelta(hours=1)

        with patch.object(Branch, "get_changes", return_value=ChangesAt(when)):
            reviewed = record_review(change, self.reviewer, True, "looks right")
            moved = transition(change, State.APPROVED, self.requester)

        self.assertTrue(reviewed.ok, reviewed.message)
        self.assertEqual(reviewed.phase, ForwardReviewPhaseChoices.PRE)
        self.assertTrue(moved.ok, moved.blockers)

    def test_the_requester_cannot_approve_their_own_change(self):
        change = self.staged()

        outcome = record_review(change, self.requester, True, "")

        self.assertFalse(outcome.ok)
        self.assertFalse(change.reviews.exists())

    def test_the_gate_is_derived_from_the_state(self):
        change = self.change(
            State.VERIFIED_PROCEED,
            branch=self.branch,
            after_snapshot_id="after",
            verdict=ForwardChangeVerdictChoices.PROCEED,
        )

        outcome = record_review(change, self.reviewer, True, "")

        self.assertEqual(outcome.phase, ForwardReviewPhaseChoices.POST)

    def test_a_branch_edited_after_the_closure_approval_cannot_close(self):
        # Edits made after the plan was approved, during apply or verification,
        # would otherwise merge without anyone having reviewed them.
        before = timezone.now() - timedelta(hours=2)
        change = self.change(
            State.VERIFIED_PROCEED,
            branch=self.branch,
            after_snapshot_id="after",
            verdict=ForwardChangeVerdictChoices.PROCEED,
        )
        with patch.object(Branch, "get_changes", return_value=ChangesAt(before)):
            record_review(change, self.reviewer, True, "")

        with patch.object(
            Branch, "get_changes", return_value=ChangesAt(timezone.now())
        ):
            outcome = close_change(change, self.requester)

        self.assertFalse(outcome.ok)
        self.assertIn("stale", " ".join(outcome.blockers).lower())


class RobustnessTest(ChangeFixture):
    def test_an_illegal_edge_does_not_touch_the_branch(self):
        change = self.change(State.STAGED, branch=self.branch)

        with patch.object(
            Branch, "get_changes", side_effect=AssertionError("queried the branch")
        ):
            outcome = transition(change, State.CLOSED, self.requester)

        self.assertFalse(outcome.ok)
        self.assertIn("does not lead", " ".join(outcome.blockers))

    def test_an_unreadable_branch_leaves_the_anchor_alone_and_says_so(self):
        change = self.change(State.STAGED, branch=self.branch, before_snapshot_id="b")

        with patch.object(Branch, "get_changes", side_effect=ProgrammingError("gone")):
            readable = sync_branch_state(change)

        self.assertFalse(readable)
        change.refresh_from_db()
        self.assertIsNone(change.branch_last_change_time)

    def test_an_approval_is_refused_when_the_branch_cannot_be_read(self):
        # Passing over edits it never saw would defeat the point of the anchor.
        change = self.change(State.STAGED, branch=self.branch, before_snapshot_id="b")
        self.review(
            change,
            ForwardReviewPhaseChoices.PRE,
            baseline_snapshot_id="b",
        )

        with patch.object(Branch, "get_changes", side_effect=ProgrammingError("gone")):
            outcome = transition(change, State.APPROVED, self.requester)

        self.assertFalse(outcome.ok)
        self.assertIn(BRANCH_UNREADABLE, outcome.blockers)

    def test_a_change_whose_branch_was_deleted_cannot_be_closed_by_state(self):
        change = self.change(
            State.VERIFIED_PROCEED,
            after_snapshot_id="after",
            verdict=ForwardChangeVerdictChoices.PROCEED,
        )
        self.review(
            change,
            ForwardReviewPhaseChoices.POST,
            after_snapshot_id="after",
            verdict=ForwardChangeVerdictChoices.PROCEED,
        )

        outcome = transition(change, State.CLOSED, self.requester)

        self.assertFalse(outcome.ok)
        self.assertIn("no longer exists", " ".join(outcome.blockers))

    def test_a_change_deleted_underneath_a_transition_is_reported_not_raised(self):
        change = self.change(State.DRAFT)
        ForwardChange.objects.filter(pk=change.pk).delete()

        outcome = transition(change, State.ABANDONED, self.requester)

        self.assertFalse(outcome.ok)

    def test_a_refused_attestation_records_nothing(self):
        change = self.change(State.DRAFT)

        outcome = attest_applied(change, self.requester, ref="CHG-1")

        self.assertFalse(outcome.ok)
        change.refresh_from_db()
        self.assertIsNone(change.applied_at)
        self.assertIsNone(change.applied_by)
        self.assertEqual(change.applied_ref, "")


class ReviewDecisionTest(ChangeFixture):
    def staged(self):
        return self.change(State.STAGED, branch=self.branch, before_snapshot_id="b")

    def test_a_reviewer_changing_their_mind_updates_one_review(self):
        change = self.staged()

        record_review(change, self.reviewer, False, "not yet")
        record_review(change, self.reviewer, True, "fixed")

        reviews = change.reviews.filter(reviewer=self.reviewer)
        self.assertEqual(reviews.count(), 1)
        self.assertTrue(reviews.get().approved)

    def test_the_requester_cannot_reject_their_own_change_either(self):
        change = self.staged()

        outcome = record_review(change, self.requester, False, "")

        self.assertFalse(outcome.ok)
        self.assertFalse(change.reviews.exists())

    def test_an_approval_of_what_the_reviewer_saw_is_recorded(self):
        change = self.staged()
        seen = review_anchor(change)

        outcome = record_review(change, self.reviewer, True, "", anchor=seen)

        self.assertTrue(outcome.ok, outcome.message)

    def test_an_approval_of_something_that_moved_since_is_refused(self):
        # The anchor is what the reviewer saw when they opened the page, not what
        # exists when they click. An edit in between must not be approved unseen.
        change = self.staged()
        seen = review_anchor(change)

        with patch.object(
            Branch, "get_changes", return_value=ChangesAt(timezone.now())
        ):
            outcome = record_review(change, self.reviewer, True, "", anchor=seen)

        self.assertFalse(outcome.ok)
        self.assertIn("moved", outcome.message)
        self.assertFalse(change.reviews.exists())

    def test_the_anchor_changes_with_each_thing_a_reviewer_signs_off_on(self):
        change = self.change(State.STAGED)
        base = review_anchor(change)

        for field, value in (
            ("branch_last_change_time", timezone.now()),
            ("before_snapshot_id", "another"),
            ("after_snapshot_id", "another"),
            ("verdict", ForwardChangeVerdictChoices.HOLD),
            ("state", State.APPROVED),
        ):
            with self.subTest(field=field):
                changed = ForwardChange(
                    **{
                        "source": self.source,
                        "title": "t",
                        "state": change.state,
                        "branch_last_change_time": change.branch_last_change_time,
                        "before_snapshot_id": change.before_snapshot_id,
                        "after_snapshot_id": change.after_snapshot_id,
                        "verdict": change.verdict,
                        field: value,
                    }
                )
                self.assertNotEqual(review_anchor(changed), base)


class ReviewStatusTest(ChangeFixture):
    """The status NetBox's own change-management plugin reports, derived.

    There is one lifecycle; this is a read-only view of it that uses the same
    six words, so an event rule or script written against that product keeps
    working.
    """

    def status(self, state, **extra):
        return review_status(self.change(state, **extra))

    def test_a_change_being_drawn_up_is_a_draft(self):
        for state in (State.DRAFT, State.SCOPED, State.BASELINED):
            with self.subTest(state=state):
                self.assertEqual(self.status(state), "draft")

    def test_a_staged_change_waits_for_review(self):
        self.assertEqual(self.status(State.STAGED), "needs-review")
        self.assertEqual(self.status(State.PREDICTED), "needs-review")

    def test_a_rejection_asks_for_changes(self):
        change = self.change(State.STAGED)
        self.review(change, ForwardReviewPhaseChoices.PRE, approved=False)

        self.assertEqual(review_status(change), "changes-requested")

    def test_an_approved_plan_is_approved_through_verification(self):
        for state in (State.APPROVED, State.APPLIED, State.COLLECTED):
            with self.subTest(state=state):
                self.assertEqual(self.status(state), "approved")

    def test_a_failed_verification_asks_for_changes(self):
        # A hold must not hide behind "approved" from anything filtering on status.
        self.assertEqual(self.status(State.VERIFIED_HOLD), "changes-requested")

    def test_a_staged_change_with_enough_approvals_reads_approved(self):
        change = self.change(State.STAGED, branch=self.branch, before_snapshot_id="b")
        self.review(
            change,
            ForwardReviewPhaseChoices.PRE,
            baseline_snapshot_id="b",
        )

        self.assertEqual(review_status(change), "approved")

    def test_a_verified_change_waits_for_its_closure_approval(self):
        change = self.change(
            State.VERIFIED_PROCEED,
            after_snapshot_id="after",
            verdict=ForwardChangeVerdictChoices.PROCEED,
        )
        self.assertEqual(review_status(change), "needs-review")

        self.review(
            change,
            ForwardReviewPhaseChoices.POST,
            after_snapshot_id="after",
            verdict=ForwardChangeVerdictChoices.PROCEED,
        )
        self.assertEqual(review_status(change), "approved")

    def test_closed_is_completed_and_abandoned_is_rejected(self):
        self.assertEqual(self.status(State.CLOSED), "completed")
        self.assertEqual(self.status(State.ABANDONED), "rejected")


class RequestFieldsTest(ChangeFixture):
    def test_priority_defaults_to_medium_and_stays_between_one_and_five(self):
        change = self.change()
        self.assertEqual(change.priority, 3)
        for good in (1, 5):
            change.priority = good
            change.full_clean()

        for bad in (0, 6):
            with self.subTest(priority=bad):
                change.priority = bad
                with self.assertRaises(ValidationError):
                    change.full_clean()

    def test_a_summary_is_optional(self):
        change = self.change()

        self.assertEqual(change.summary, "")
