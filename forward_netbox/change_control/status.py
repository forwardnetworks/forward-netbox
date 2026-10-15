# The status NetBox's own change-management plugin reports, derived.
#
# There is one lifecycle (the state machine), and approval already sits inside
# it. This is a read-only view of that lifecycle in the six words that product
# uses, so an event rule, filter or script written against it keeps working. It
# is never stored and never set, so it cannot disagree with the state.
from .choices import ForwardChangeStateChoices as State
from .choices import ForwardReviewPhaseChoices
from .state_machine import closure_check
from .state_machine import plan_approval_check

DRAFT = "draft"
NEEDS_REVIEW = "needs-review"
CHANGES_REQUESTED = "changes-requested"
APPROVED = "approved"
COMPLETED = "completed"
REJECTED = "rejected"

STATUSES = (DRAFT, NEEDS_REVIEW, CHANGES_REQUESTED, APPROVED, COMPLETED, REJECTED)

_DRAWING_UP = frozenset({State.DRAFT, State.SCOPED, State.BASELINED})
_AWAITING_PLAN_REVIEW = frozenset({State.STAGED, State.PREDICTED})
_PLAN_APPROVED = frozenset({State.APPROVED, State.APPLIED, State.COLLECTED})


def _rejected(change, phase) -> bool:
    return change.reviews.filter(phase=phase, approved=False).exists()


def review_status(change) -> str:
    state = change.state
    if state == State.CLOSED:
        return COMPLETED
    if state == State.ABANDONED:
        return REJECTED
    if state in _DRAWING_UP:
        return DRAFT
    if state in _AWAITING_PLAN_REVIEW:
        if _rejected(change, ForwardReviewPhaseChoices.PRE):
            return CHANGES_REQUESTED
        return APPROVED if plan_approval_check(change).allowed else NEEDS_REVIEW
    if state in _PLAN_APPROVED:
        return APPROVED
    if state == State.VERIFIED_HOLD:
        # A failed verification must not hide behind "approved" from anything
        # filtering on status: it is waiting for a fix.
        return CHANGES_REQUESTED
    if state == State.VERIFIED_PROCEED:
        if _rejected(change, ForwardReviewPhaseChoices.POST):
            return CHANGES_REQUESTED
        return APPROVED if closure_check(change).allowed else NEEDS_REVIEW
    return DRAFT
