# Which branch a change may be tied to, and when it may change.
#
# One rule for the form and the REST serializer, so the API cannot link what the
# page refuses.
from .choices import ForwardChangeStateChoices as State

BRANCH_EDITABLE_STATES = frozenset({State.DRAFT, State.SCOPED, State.BASELINED})


def branch_blocker(change, branch):
    """Why `change` may not be given `branch` (None clears it), or ""."""
    from ..models import ForwardChange
    from ..models import ForwardIngestion

    current = change.branch_id if change is not None and change.pk else None
    pk = branch.pk if branch is not None else None
    if pk == current:
        return ""
    if branch is not None:
        if ForwardIngestion.objects.filter(branch_id=pk).exists():
            return "This branch belongs to a Forward sync and cannot back a change."
        if (
            ForwardChange.objects.filter(branch_id=pk)
            .exclude(pk=getattr(change, "pk", None))
            .exists()
        ):
            return "This branch already backs another change."
    if change is not None and change.pk and change.state not in BRANCH_EDITABLE_STATES:
        return "The branch can only change before the change is staged."
    return ""
