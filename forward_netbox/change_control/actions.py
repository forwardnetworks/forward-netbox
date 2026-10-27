# Moving a change through its lifecycle.
#
# The state machine says whether a transition may happen and what is missing; this
# is the one place that makes it happen. Every move is checked, written under a row
# lock, and journalled, so the history of a change reads as a sequence of who moved
# it and why it was allowed.
import logging
from dataclasses import dataclass
from dataclasses import field

from django.db import DatabaseError
from django.db import transaction
from django.utils import timezone
from extras.choices import JournalEntryKindChoices
from extras.models import JournalEntry
from netbox_branching.choices import BranchStatusChoices
from netbox_branching.utilities import deactivate_branch

from .choices import ForwardChangeStateChoices as State
from .choices import ForwardReviewPhaseChoices as Phase
from .state_machine import check_transition
from .state_machine import closure_check
from .state_machine import legal_transition

logger = logging.getLogger("forward_netbox.change_control")


@dataclass(frozen=True)
class TransitionOutcome:
    """Whether a change moved, where it is now, and what is missing if it did not."""

    ok: bool
    state: str
    blockers: tuple = field(default_factory=tuple)

    def __bool__(self):
        return self.ok


@dataclass(frozen=True)
class ReviewOutcome:
    ok: bool
    phase: str = ""
    message: str = ""

    def __bool__(self):
        return self.ok


def _lock(change):
    """The change's row, locked, or None if it no longer exists."""
    from ..models import ForwardChange

    return ForwardChange.objects.select_for_update().filter(pk=change.pk).first()


def _journal(change, user, text):
    # JournalEntry is branch-aware: written with a branch active it would land in
    # that branch's schema and merge along with it. The journal belongs on main.
    with deactivate_branch():
        JournalEntry.objects.create(
            assigned_object=change,
            created_by=user,
            kind=JournalEntryKindChoices.KIND_INFO,
            comments=text,
        )


BRANCH_UNREADABLE = "Could not read the changes on the change's branch; try again."


def sync_branch_state(change):
    """Record the branch's name and the time of its newest change on the change.

    Approvals are anchored to that time: a branch edited after a reviewer signed
    makes the approval stale, which is only detectable if somebody keeps the anchor
    current. Only a READY branch is read: any other status has nothing left that
    could merge. Returns False when a READY branch could not be read, so a caller
    that is about to rely on the anchor can refuse instead of passing over edits it
    never saw. (The branch has its own connection, so a failed read does not
    disturb the caller's transaction.)
    """
    from ..models import ForwardChange

    if not change.branch_id:
        return True
    branch = change.branch
    updates = {}
    readable = True
    if change.branch_name != branch.name:
        updates["branch_name"] = branch.name
    if branch.status == BranchStatusChoices.READY:
        try:
            newest = branch.get_changes().order_by("-time").first()
        except DatabaseError:
            logger.warning(
                "Could not read the changes on branch %s; its anchor is unchanged.",
                branch,
                exc_info=True,
            )
            readable = False
        else:
            newest_time = newest.time if newest else None
            if newest_time != change.branch_last_change_time:
                updates["branch_last_change_time"] = newest_time
    if updates:
        ForwardChange.objects.filter(pk=change.pk).update(**updates)
        for name, value in updates.items():
            setattr(change, name, value)
    return readable


def review_anchor(change) -> str:
    """An opaque token for what a reviewer is shown: the state and its evidence.

    Rendered into the review form and posted back, so an approval is recorded only
    if nothing the reviewer saw has moved since they opened the page. Without it
    the anchor would be taken when they click, and an edit made in between would be
    approved unseen.
    """
    import hashlib

    parts = (
        change.state,
        (
            change.branch_last_change_time.isoformat()
            if change.branch_last_change_time
            else ""
        ),
        change.before_snapshot_id,
        change.after_snapshot_id,
        change.verdict,
    )
    return hashlib.sha256("|".join(parts).encode()).hexdigest()[:24]


def _closing_needs_the_merge(change):
    """CLOSED is where the branch merges, so it cannot be set by itself.

    Setting the state without the merge would let the page say "closed" over a
    branch that never merged.
    """
    branch = change.branch
    if branch is None:
        return (
            "The change's branch no longer exists, so there is nothing to merge "
            "and the change cannot be closed by a merge.",
        )
    if branch.status == BranchStatusChoices.MERGED:
        return ()
    return (
        "A change is closed by merging its branch: merge the branch to close "
        "the change. Setting the state alone would leave the branch unmerged.",
    )


def _apply(change, target, user, note=""):
    result = check_transition(change, target)
    if not result.allowed:
        return TransitionOutcome(False, change.state, tuple(result.blockers))
    previous = change.state
    change.state = target
    change.save(update_fields=["state", "last_updated"])
    _journal(
        change,
        user,
        f"State changed from {previous} to {target}.{' ' + note if note else ''}",
    )
    from .events import announce_transition

    announce_transition(change, target, user)
    if target in State.TERMINAL and change.predict_change_set_id:
        # Forward keeps a change set's predicted snapshots out of its retention
        # until the set is deleted. After the transaction commits, so the network
        # call never holds the change's row lock, and best-effort so Forward being
        # down never blocks closing a change.
        transaction.on_commit(lambda pk=change.pk: _discard_prediction_for(pk))
    return TransitionOutcome(True, target)


def _discard_prediction_for(change_pk):
    from ..models import ForwardChange
    from .predict import discard_prediction

    change = ForwardChange.objects.filter(pk=change_pk).first()
    if change is not None:
        discard_prediction(change)


def transition(change, target, user):
    """Move `change` to `target` if the state machine allows it; say why not if not."""
    with transaction.atomic():
        locked = _lock(change)
        if locked is None:
            return TransitionOutcome(
                False, change.state, ("The change no longer exists.",)
            )
        # The edge first: an illegal request is answered without touching the branch.
        if not legal_transition(locked.state, target):
            result = check_transition(locked, target)
            return TransitionOutcome(False, locked.state, tuple(result.blockers))
        if target == State.CLOSED:
            blockers = _closing_needs_the_merge(locked)
            if blockers:
                return TransitionOutcome(False, locked.state, blockers)
        if target == State.APPROVED and not sync_branch_state(locked):
            return TransitionOutcome(False, locked.state, (BRANCH_UNREADABLE,))
        outcome = _apply(locked, target, user)
    change.refresh_from_db()
    if outcome.ok and target == State.STAGED:
        from .queueing import validate_on_submission

        try:
            validate_on_submission(change, user)
        except Exception:  # noqa: BLE001 - staging is not undone by a queueing problem
            logger.exception("Could not queue validation for change %s", change.pk)
    return outcome


def attest_applied(change, user, applied_at=None, ref=""):
    """Record that the change was applied, then move it to APPLIED.

    Nothing here can observe a network being changed, so this is a human claim,
    recorded with who made it and when. If the change may not move to APPLIED,
    nothing is recorded.
    """
    with transaction.atomic():
        locked = _lock(change)
        if locked is None:
            return TransitionOutcome(
                False, change.state, ("The change no longer exists.",)
            )
        locked.applied_at = applied_at or timezone.now()
        locked.applied_by = user
        locked.applied_ref = ref
        locked.save(update_fields=["applied_at", "applied_by", "applied_ref"])
        outcome = _apply(locked, State.APPLIED, user)
        if not outcome.ok:
            transaction.set_rollback(True)
    change.refresh_from_db()
    return outcome


def record_review(change, reviewer, approved, comment="", anchor=None):
    """Record one reviewer's decision at whichever gate the change is at.

    The anchors are captured here, from the change as it is at this moment and
    after the branch's anchor has been refreshed, rather than accepted from a
    request: a reviewer approves the evidence they were shown. Recording the stored
    anchor without refreshing it would make every first approval stale.
    """
    with transaction.atomic():
        locked = _lock(change)
        if locked is None:
            return ReviewOutcome(False, message="The change no longer exists.")
        if locked.requester_id and locked.requester_id == reviewer.pk:
            return ReviewOutcome(
                False,
                message=(
                    "You raised this change, so your review does not count "
                    "towards it."
                ),
            )
        # The gate is derived from the state, not chosen by the reviewer: an
        # approval recorded while the change is still STAGED would sign off on
        # evidence that does not exist yet.
        if locked.state in State.TERMINAL:
            return ReviewOutcome(
                False, message="This change is finished, so it takes no more reviews."
            )
        phase = Phase.POST if locked.state == State.VERIFIED_PROCEED else Phase.PRE
        if not sync_branch_state(locked):
            return ReviewOutcome(False, message=BRANCH_UNREADABLE)
        if anchor is not None and anchor != review_anchor(locked):
            return ReviewOutcome(
                False,
                message=(
                    "The change moved since you opened this page (its branch, a "
                    "snapshot or the verdict changed). Reload it and review "
                    "what is there now."
                ),
            )
        from ..models import ForwardChangeReview

        ForwardChangeReview.objects.update_or_create(
            change=locked,
            reviewer=reviewer,
            phase=phase,
            defaults={
                "approved": approved,
                "comment": (comment or "").strip(),
                "branch_change_time": locked.branch_last_change_time,
                "baseline_snapshot_id": locked.before_snapshot_id,
                "after_snapshot_id": locked.after_snapshot_id,
                "verdict": locked.verdict,
            },
        )
        if not approved:
            from .events import CHANGES_REQUESTED
            from .events import announce

            announce(locked, CHANGES_REQUESTED, reviewer, phase=phase)
    return ReviewOutcome(True, phase)


def close_change(change, user):
    """Ask the Branching plugin to merge the change's branch, which closes it.

    The merge is that plugin's job and runs in the background; the change is CLOSED
    when the merge has happened (`close_when_the_branch_merges`), not when it was
    asked for. Nothing is asked unless the change may be closed, and the branch's
    anchor is refreshed first so an edit made after the closure approval is caught.
    A branch with no NetBox changes has nothing to merge (Branching returns without
    marking it merged), so a change that only needed Forward's verdict closes
    directly.
    """
    from netbox_branching.jobs import MergeBranchJob

    with transaction.atomic():
        locked = _lock(change)
        if locked is None:
            return TransitionOutcome(
                False, change.state, ("The change no longer exists.",)
            )
        if not sync_branch_state(locked):
            return TransitionOutcome(False, locked.state, (BRANCH_UNREADABLE,))
        result = closure_check(locked)
        if not result.allowed:
            return TransitionOutcome(False, locked.state, tuple(result.blockers))
        branch = locked.branch
        if branch is None:
            return TransitionOutcome(
                False, locked.state, ("The change has no branch to merge.",)
            )
        if branch.status != BranchStatusChoices.READY:
            return TransitionOutcome(
                False,
                locked.state,
                (f"The branch is {branch.status}, not ready to merge.",),
            )
        if not branch.get_unmerged_changes().exists():
            outcome = _apply(
                locked,
                State.CLOSED,
                user,
                note="The branch holds no NetBox changes, so there was nothing to merge.",
            )
            change.refresh_from_db()
            return outcome
    MergeBranchJob.enqueue(instance=branch, user=user, commit=True)
    _journal(change, user, f"Merge of branch {branch.name} requested.")
    change.refresh_from_db()
    return TransitionOutcome(True, change.state)


def close_when_the_branch_merges(sender, branch, user, **kwargs):
    """Close the change whose branch has just merged, however the merge started.

    A merge from the Branching page or its REST API passes the gate once the closure
    approval is in; leaving its change at VERIFIED_PROCEED would make the record
    disagree with the network. The merge has happened and the gate passed when it
    started, so this records that rather than re-judging it: re-running the closure
    check could only produce a false refusal and leave a merged change open.

    A change in any other state is left alone: a merge that reached it some other
    way (the plugin's own sync merge, a direct call) must not be recorded as a
    proper closure. Never raises: failing here would not undo the merge.
    """
    from ..models import ForwardChange

    try:
        with transaction.atomic():
            change = (
                ForwardChange.objects.select_for_update()
                .filter(branch_id=branch.pk)
                .first()
            )
            if change is None or change.state != State.VERIFIED_PROCEED:
                return
            previous = change.state
            change.state = State.CLOSED
            change.save(update_fields=["state", "last_updated"])
            _journal(
                change,
                user,
                f"State changed from {previous} to {State.CLOSED}: "
                f"branch {branch.name} merged.",
            )
            from .events import announce_transition

            announce_transition(change, State.CLOSED, user)
            if change.predict_change_set_id:
                transaction.on_commit(lambda pk=change.pk: _discard_prediction_for(pk))
    except Exception:  # noqa: BLE001 - see the docstring
        logger.exception("Could not close the change for merged branch %s", branch)
