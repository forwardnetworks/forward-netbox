# The transitions, and what each one demands before it will happen.
#
# Pure functions over the database: no request object, no client, no job. A
# transition returns a TransitionResult rather than raising, because "this
# cannot proceed yet, and here is precisely what is missing" is the normal
# case in a change workflow, not an error condition.
from dataclasses import dataclass
from dataclasses import field

from .choices import ForwardChangeStateChoices as State
from .choices import ForwardReviewPhaseChoices


@dataclass(frozen=True)
class TransitionResult:
    """Whether a transition may happen, and what is missing when it may not.

    `blockers` are operator-facing sentences. They are the entire value of this
    module: a gate that says "not ready" without saying what is missing gets
    turned off.
    """

    allowed: bool
    blockers: tuple[str, ...] = ()
    context: dict = field(default_factory=dict)

    def __bool__(self):
        return self.allowed


# Every legal edge. PREDICTED has two ways in and two ways out because it is
# optional; STAGED -> APPROVED is a first-class transition, not a fallback.
#
# VERIFIED_HOLD deliberately has no edge to CLOSED. A hold is fixed and
# re-verified - it goes back to APPLIED to await a fresh collection, or is
# abandoned. Closing a hold is the exact mistake this vocabulary exists to
# prevent, so the graph refuses to express it.
TRANSITIONS: dict[str, frozenset[str]] = {
    State.DRAFT: frozenset({State.SCOPED, State.ABANDONED}),
    State.SCOPED: frozenset({State.BASELINED, State.DRAFT, State.ABANDONED}),
    State.BASELINED: frozenset({State.STAGED, State.SCOPED, State.ABANDONED}),
    State.STAGED: frozenset({State.PREDICTED, State.APPROVED, State.ABANDONED}),
    State.PREDICTED: frozenset({State.APPROVED, State.STAGED, State.ABANDONED}),
    State.APPROVED: frozenset({State.APPLIED, State.STAGED, State.ABANDONED}),
    State.APPLIED: frozenset({State.COLLECTED, State.ABANDONED}),
    State.COLLECTED: frozenset(
        {State.VERIFIED_PROCEED, State.VERIFIED_HOLD, State.ABANDONED}
    ),
    State.VERIFIED_PROCEED: frozenset({State.CLOSED, State.ABANDONED}),
    State.VERIFIED_HOLD: frozenset({State.APPLIED, State.ABANDONED}),
    State.CLOSED: frozenset(),
    State.ABANDONED: frozenset(),
}


def legal_transition(current: str, target: str) -> bool:
    """Whether the graph has this edge at all, before any evidence is read."""
    return target in TRANSITIONS.get(current, frozenset())


def available_transitions(current: str) -> tuple[str, ...]:
    return tuple(sorted(TRANSITIONS.get(current, frozenset())))


def check_transition(change, target: str) -> TransitionResult:
    """Whether `change` may move to `target`, and what is missing if not."""
    current = change.state
    if current == target:
        return TransitionResult(False, (f"The change is already {target}.",))
    if not legal_transition(current, target):
        return TransitionResult(
            False,
            (
                f"{current} does not lead to {target}. "
                f"From here: {', '.join(available_transitions(current)) or 'nowhere'}.",
            ),
        )
    checker = _ENTRY_CHECKS.get(target)
    if checker is None:
        return TransitionResult(True)
    return checker(change)


def _scoped(change) -> TransitionResult:
    """Every device must be one Forward knows, and something must be asserted."""
    blockers = []
    devices = list(change.devices.all())
    if not devices:
        blockers.append("Add at least one device to the change's scope.")
    unresolved = [d for d in devices if not d.forward_device_key]
    if unresolved:
        names = ", ".join(sorted(str(d.device) for d in unresolved[:5]))
        blockers.append(
            f"{len(unresolved)} device(s) have no Forward identity for this "
            f"source, so Forward cannot be asked about them: {names}."
        )
    if not change.criteria.filter(blocking=True).exists():
        blockers.append(
            "Add at least one blocking acceptance criterion. A change with "
            "nothing asserted cannot be verified, only asserted to have "
            "happened."
        )
    return TransitionResult(not blockers, tuple(blockers))


def _baselined(change) -> TransitionResult:
    """Criteria must BIND before anything is measured against them.

    Binding is resolution to a concrete `query_id` at a concrete `commit_id`.
    An unbound criterion would be measured against whatever the query happens
    to say later, which is how a moving assertion silently passes.
    """
    blockers = []
    unbound = [c for c in change.criteria.all() if not (c.query_id and c.commit_id)]
    if unbound:
        names = ", ".join(sorted(c.name for c in unbound[:5]))
        blockers.append(
            f"{len(unbound)} criterion(s) are not bound to a committed query "
            f"version: {names}."
        )
    if not change.before_snapshot_id:
        blockers.append(
            "Pin a baseline snapshot. A selector is not enough: the id is "
            "recorded literally so the same snapshot is used at verify."
        )
    return TransitionResult(not blockers, tuple(blockers))


def _staged(change) -> TransitionResult:
    blockers = []
    if not change.branch_id:
        blockers.append("Create the NetBox branch holding the intended changes.")
    if not change.before_snapshot_id:
        blockers.append("Baseline the change before staging it.")
    return TransitionResult(not blockers, tuple(blockers))


def _approval_gate(change, phase, what) -> TransitionResult:
    """Shared by both gates: enough distinct, non-stale, non-self approvals.

    `what` names the evidence the reviewer signed off on, so the blocker reads
    as an instruction rather than a code.
    """
    from .policy import distinct_approvers
    from .policy import required_approvals
    from .policy import stale_approvals

    blockers = []
    needed = required_approvals(change, phase)
    approvers = distinct_approvers(change, phase)
    stale = stale_approvals(change, phase)

    if len(approvers) < needed:
        shortfall = needed - len(approvers)
        blockers.append(
            f"{shortfall} more approval(s) needed ({len(approvers)} of "
            f"{needed}). Approvals are counted per distinct reviewer, and the "
            f"requester's own does not count."
        )
    if stale:
        blockers.append(
            f"{len(stale)} approval(s) are stale: {what} moved after they were "
            f"given, so they approved a different change."
        )
    return TransitionResult(not blockers, tuple(blockers))


def _approved(change) -> TransitionResult:
    """The PRE gate: approving the plan, anchored to the branch and baseline.

    Both anchors, because a change can be re-baselined without touching the
    branch and the reviewer would have seen evidence from the old snapshot.
    """
    return _approval_gate(
        change,
        ForwardReviewPhaseChoices.PRE,
        "the branch or the baseline",
    )


def _applied(change) -> TransitionResult:
    """The one transition with no computed evidence, and it says so.

    Nothing here can observe that a network engineer pushed configuration. The
    attestation is a human claim, recorded as one, and the page must not
    present it as a measurement.
    """
    blockers = []
    if not change.applied_at:
        blockers.append(
            "Record when the change was applied. Nothing here can observe "
            "that, so it is attested rather than measured."
        )
    if not change.applied_by:
        blockers.append("Record who attested the change was applied.")
    return TransitionResult(not blockers, tuple(blockers))


def _collected(change) -> TransitionResult:
    """A snapshot that is not the baseline and post-dates the apply.

    The plugin waits; it has no way to ask Forward to collect. The
    per-device timing check is the gate's real content and lives in
    `gates.py`, because it needs the client.
    """
    blockers = []
    if not change.after_snapshot_id:
        blockers.append("No post-change snapshot has been pinned yet.")
    elif change.after_snapshot_id == change.before_snapshot_id:
        blockers.append(
            "The post-change snapshot is the baseline. Verifying a change "
            "against the snapshot taken before it would pass by construction."
        )
    return TransitionResult(not blockers, tuple(blockers))


def _closed(change) -> TransitionResult:
    """The POST gate. CLOSE is where the branch merges, so it gets its own.

    A PROCEED verdict opens this gate; it does not walk through it. The
    machine says the network looks right, and a human still decides to merge -
    which is the consequential, irreversible half.

    Anchored to the after-snapshot and the verdict: re-verifying against a
    newer snapshot, or the verdict changing, voids a closure sign-off.
    """
    if change.state != State.VERIFIED_PROCEED:
        return TransitionResult(
            False, ("Only a change that verified PROCEED may be closed.",)
        )
    return _approval_gate(
        change,
        ForwardReviewPhaseChoices.POST,
        "the post-change snapshot or the verdict",
    )


_ENTRY_CHECKS = {
    State.SCOPED: _scoped,
    State.BASELINED: _baselined,
    State.STAGED: _staged,
    State.APPROVED: _approved,
    State.APPLIED: _applied,
    State.COLLECTED: _collected,
    State.CLOSED: _closed,
}
