from unittest.mock import patch

from django.db import ProgrammingError

from extras.models import JournalEntry
from netbox_branching.choices import BranchStatusChoices
from netbox_branching.jobs import MergeBranchJob
from netbox_branching.models import Branch
from netbox_branching.signals import post_merge

from forward_netbox.change_control.actions import BRANCH_UNREADABLE
from forward_netbox.change_control.actions import close_change
from forward_netbox.change_control.actions import close_when_the_branch_merges
from forward_netbox.change_control.choices import ForwardChangeStateChoices as State
from forward_netbox.change_control.choices import ForwardChangeVerdictChoices
from forward_netbox.change_control.choices import ForwardReviewPhaseChoices
from forward_netbox.models import ForwardChangeReview
from forward_netbox.tests.test_change_control_actions import ChangeFixture
from forward_netbox.tests.test_change_control_actions import ChangesAt


class _Something:
    def exists(self):
        return True


class CloseFixture(ChangeFixture):
    def setUp(self):
        super().setUp()
        # A branch with edits to merge, unless a test says it has none.
        patcher = patch.object(
            Branch, "get_unmerged_changes", return_value=_Something()
        )
        patcher.start()
        self.addCleanup(patcher.stop)

    def verified_change(self, **extra):
        change = self.change(
            State.VERIFIED_PROCEED,
            branch=self.branch,
            after_snapshot_id="after",
            verdict=ForwardChangeVerdictChoices.PROCEED,
            **extra,
        )
        self.review(
            change,
            ForwardReviewPhaseChoices.POST,
            after_snapshot_id="after",
            verdict=ForwardChangeVerdictChoices.PROCEED,
        )
        return change


class CloseChangeTest(CloseFixture):
    """Closing a change asks the Branching plugin to merge its branch.

    The merge is its job, not ours, and it happens in the background. The change
    is CLOSED when that merge has happened, not when it was asked for.
    """

    def test_a_closable_change_asks_for_its_branch_to_be_merged(self):
        change = self.verified_change()

        with patch.object(MergeBranchJob, "enqueue") as enqueue:
            outcome = close_change(change, self.requester)

        self.assertTrue(outcome.ok, outcome.blockers)
        enqueue.assert_called_once()
        kwargs = enqueue.call_args.kwargs
        self.assertEqual(kwargs["instance"].pk, self.branch.pk)
        self.assertEqual(kwargs["user"], self.requester)
        self.assertTrue(kwargs["commit"])

    def test_asking_does_not_close_the_change_yet(self):
        change = self.verified_change()

        with patch.object(MergeBranchJob, "enqueue"):
            close_change(change, self.requester)

        change.refresh_from_db()
        self.assertEqual(change.state, State.VERIFIED_PROCEED)
        self.assertTrue(
            JournalEntry.objects.filter(
                assigned_object_id=change.pk, comments__icontains="merge"
            ).exists()
        )

    def test_a_change_that_may_not_close_asks_for_nothing(self):
        change = self.change(State.STAGED, branch=self.branch)

        with patch.object(MergeBranchJob, "enqueue") as enqueue:
            outcome = close_change(change, self.requester)

        self.assertFalse(outcome.ok)
        self.assertTrue(outcome.blockers)
        enqueue.assert_not_called()

    def test_a_change_without_its_closure_approval_asks_for_nothing(self):
        change = self.change(
            State.VERIFIED_PROCEED,
            branch=self.branch,
            after_snapshot_id="after",
            verdict=ForwardChangeVerdictChoices.PROCEED,
        )

        with patch.object(MergeBranchJob, "enqueue") as enqueue:
            outcome = close_change(change, self.requester)

        self.assertFalse(outcome.ok)
        enqueue.assert_not_called()


class CloseWithNothingToMergeTest(CloseFixture):
    def test_a_change_with_no_branch_cannot_be_closed(self):
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

        outcome = close_change(change, self.requester)

        self.assertFalse(outcome.ok)
        self.assertIn("no branch", " ".join(outcome.blockers).lower())

    def test_a_forward_only_change_with_no_netbox_edits_closes_directly(self):
        # Merging an empty branch does nothing and never marks it merged, so a
        # change that only needed Forward's verdict could otherwise never close.
        change = self.verified_change()

        class _Nothing:
            def exists(self):
                return False

        with patch.object(
            Branch, "get_unmerged_changes", return_value=_Nothing()
        ), patch.object(MergeBranchJob, "enqueue") as enqueue:
            outcome = close_change(change, self.requester)

        self.assertTrue(outcome.ok, outcome.blockers)
        enqueue.assert_not_called()
        change.refresh_from_db()
        self.assertEqual(change.state, State.CLOSED)


class ClosedByTheMergeTest(CloseFixture):
    """The merge itself closes the change, however the merge was started.

    A branch merged from the Branching page after the closure approval, or by its
    REST API, passes the gate; leaving its change at VERIFIED_PROCEED forever would
    make the record disagree with the network.
    """

    def _merge(self, branch, user):
        Branch.objects.filter(pk=branch.pk).update(status=BranchStatusChoices.MERGED)
        branch.refresh_from_db()
        post_merge.send(sender=Branch, branch=branch, user=user)

    def test_a_merge_closes_a_verified_change_and_journals_it(self):
        change = self.verified_change()

        self._merge(self.branch, self.reviewer)

        change.refresh_from_db()
        self.assertEqual(change.state, State.CLOSED)
        entry = JournalEntry.objects.filter(assigned_object_id=change.pk).latest(
            "created"
        )
        self.assertEqual(entry.created_by, self.reviewer)
        self.assertIn(State.CLOSED, entry.comments)

    def test_a_branch_with_no_change_is_left_alone(self):
        with self.assertNoLogs("forward_netbox.change_control", level="ERROR"):
            self._merge(self.branch, self.reviewer)

    def test_a_merge_closes_the_change_even_if_an_approval_has_gone_stale_since(self):
        # The gate passed when the merge started; the merge has happened. The
        # record must say so rather than staying at VERIFIED_PROCEED forever.
        change = self.verified_change()
        ForwardChangeReview.objects.filter(change=change).update(
            after_snapshot_id="an-earlier-snapshot"
        )

        self._merge(self.branch, self.reviewer)

        change.refresh_from_db()
        self.assertEqual(change.state, State.CLOSED)

    def test_a_change_not_yet_verified_is_not_closed_by_a_stray_merge(self):
        # Reaching here means a merge got past the gate some other way (the
        # plugin's own sync merge, a direct call). The record must not claim a
        # closure that never happened properly.
        change = self.change(State.STAGED, branch=self.branch)

        self._merge(self.branch, self.reviewer)

        change.refresh_from_db()
        self.assertEqual(change.state, State.STAGED)


class CloseFailureTest(CloseFixture):
    def test_an_unreadable_branch_asks_for_nothing(self):
        change = self.verified_change()

        with patch.object(Branch, "get_changes", side_effect=ProgrammingError("x")):
            with patch.object(MergeBranchJob, "enqueue") as enqueue:
                outcome = close_change(change, self.requester)

        self.assertFalse(outcome.ok)
        self.assertIn(BRANCH_UNREADABLE, outcome.blockers)
        enqueue.assert_not_called()

    def test_a_failed_enqueue_is_raised_and_nothing_is_recorded_as_requested(self):
        change = self.verified_change()

        with patch.object(MergeBranchJob, "enqueue", side_effect=RuntimeError("down")):
            with self.assertRaises(RuntimeError):
                close_change(change, self.requester)

        self.assertFalse(
            JournalEntry.objects.filter(
                assigned_object_id=change.pk, comments__icontains="requested"
            ).exists()
        )

    def test_the_merge_hook_never_raises_and_logs_what_went_wrong(self):
        self.verified_change()

        with patch(
            "forward_netbox.change_control.actions._journal",
            side_effect=RuntimeError("boom"),
        ):
            with self.assertLogs("forward_netbox.change_control", "ERROR"):
                close_when_the_branch_merges(Branch, self.branch, self.reviewer)
