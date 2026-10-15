# The merge gate, and keeping change records out of branches.
#
# The branch merges at CLOSE, not at APPROVE (see `__init__.py`). That is only
# true if nothing else can merge it first, so the Branching plugin is asked, at
# its own extension points, to refuse.
#
# `Branch.merge()` raises unless `can_merge` passes, and `can_merge` runs every
# registered pre-action check. A real merge from the Branching page, its REST API
# or its merge job therefore all stop here, with the message shown to the user. A
# dry run (`commit=False`) does not ask, so a reviewer can still preview the
# merge. The `pre_merge` signal is deliberately not used: it fires on dry runs too,
# so raising from it would block previews, and it surfaces only as a failed job
# after the user has been told the merge is running.
from django.db.models import ProtectedError
from netbox_branching.utilities import BranchActionIndicator

from .choices import ForwardChangeStateChoices as State
from .actions import BRANCH_UNREADABLE
from .actions import sync_branch_state
from .state_machine import closure_check

# Change records live on main. They are change-logged, so Branching would treat
# them as branch-aware by default; an approval written while the change's branch
# is active would land in the branch schema, where the gate (reading main) never
# sees it, and it would then merge along with the branch. Matched on the model-name
# prefix so a model added later (comments, validation tables) is covered without
# anyone remembering to extend a list.
CHANGE_CONTROL_PREFIX = "forwardchange"


def change_control_branching_resolver(model):
    """False for a change-control model; None (not mine) for everything else.

    The first non-None answer wins across every registered resolver, so
    answering for a model that is not ours would reroute it for everyone.
    """
    meta = model._meta
    if meta.app_label == "forward_netbox" and meta.model_name.startswith(
        CHANGE_CONTROL_PREFIX
    ):
        return False
    return None


def merge_gate(branch):
    """May this branch merge? A branch with no change is not this gate's business."""
    from ..models import ForwardChange

    if branch.pk is None:
        return BranchActionIndicator(True)
    change = ForwardChange.objects.filter(branch_id=branch.pk).first()
    if change is None:
        return BranchActionIndicator(True)
    if change.state == State.CLOSED:
        # Closing merges the branch (or finds nothing to merge), so a READY branch
        # under a CLOSED change was reverted, or never held anything. Passing it
        # would let it be edited and merged with no approval at all.
        return BranchActionIndicator(
            False,
            f"The change {change} is closed. Its branch cannot be merged "
            "without raising a new change.",
        )
    if change.state == State.ABANDONED:
        return BranchActionIndicator(
            False,
            f"The change {change} was abandoned, so its branch must not be merged.",
        )
    # The anchor first, but only where it matters: an edit made after the closure
    # approval must be seen, and a branch that cannot be read must not be passed.
    if change.state == State.VERIFIED_PROCEED and not sync_branch_state(change):
        return BranchActionIndicator(False, BRANCH_UNREADABLE)
    result = closure_check(change)
    if result.allowed:
        return BranchActionIndicator(True)
    return BranchActionIndicator(
        False,
        f"The change {change} is not ready for its branch to merge. "
        + " ".join(result.blockers),
    )


def ensure_change_may_be_deleted(change):
    """Refuse to delete a change whose branch could still merge.

    With the change gone the gate finds nothing to refuse, and the branch would
    merge with no verification and no approval. That holds for an abandoned change
    as much as a live one (abandoning needs no approval), and for a closed change
    whose branch was reverted. Deleting is allowed once the branch can no longer
    merge: it is merged or archived, or it is gone.
    """
    from netbox_branching.choices import BranchStatusChoices
    from netbox_branching.models import Branch

    if not change.branch_id:
        return
    # Read fresh: a cached related object can be a stale view of the branch.
    status = (
        Branch.objects.filter(pk=change.branch_id).values_list("status", flat=True).first()
    )
    if status is not None and status not in (
        BranchStatusChoices.MERGED,
        BranchStatusChoices.ARCHIVED,
    ):
        advice = (
            "Archive or delete the branch first."
            if change.state in State.TERMINAL
            else "Merge, archive or delete the branch first."
        )
        raise ProtectedError(
            f"The change {change} still holds a branch that could merge. {advice}",
            {change},
        )


_registered = False


def register_branch_gate():
    """Register the resolver, the merge check and the close hook. Idempotent."""
    global _registered
    if _registered:
        return
    from netbox_branching.models import Branch
    from netbox_branching.utilities import register_branching_resolver

    from netbox_branching.signals import post_merge

    from .actions import close_when_the_branch_merges

    register_branching_resolver(change_control_branching_resolver)
    Branch.register_preaction_check(merge_gate, "merge")
    post_merge.connect(
        close_when_the_branch_merges,
        dispatch_uid="forward_change_close_when_branch_merges",
        weak=False,
    )
    _registered = True
