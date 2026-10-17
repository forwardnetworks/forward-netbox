# Adding, editing, binding and deleting a change's criteria.
#
# A criterion is the assertion a change is judged against, so it may not move
# once something has been measured against it. It is editable only while the
# change is still being planned and only while no evidence exists for it; editing
# the query path of a bound criterion drops its binding, because the pinned
# version belonged to the old path.
from contextlib import contextmanager
from dataclasses import dataclass
from dataclasses import field

from django.db import transaction

from .actions import _journal
from .actions import _lock
from .choices import ForwardChangeStateChoices as State
from .criteria import bind_criterion

# Planning only. BASELINED is out: entering it required every criterion to be bound,
# and STAGED has no way back, so a criterion added or re-pathed after that would sit
# unbound and unmeasured forever. Edit from SCOPED, then baseline.
EDITABLE_STATES = frozenset({State.DRAFT, State.SCOPED})


@dataclass(frozen=True)
class CriterionOutcome:
    ok: bool
    message: str = ""
    problems: tuple = field(default_factory=tuple)

    def __bool__(self):
        return self.ok


def _refuse(message):
    return CriterionOutcome(False, message)


def change_blocker(change):
    """Why this change's criteria cannot change now, or "" if they can."""
    if change.state not in EDITABLE_STATES:
        return (
            "Criteria can only change while the change is being planned (draft or "
            "scoped). Move it back to scoped to change them; once baselined or "
            "staged the assertion would move under what was measured and reviewed."
        )
    return ""


def criterion_blocker(criterion):
    """Why this criterion cannot be edited, rebound or deleted, or ""."""
    reason = change_blocker(criterion.change)
    if reason:
        return reason
    if criterion.pk and criterion.evidence.exists():
        return (
            "Evidence has already been recorded against this criterion; changing "
            "it now would change what the recorded results mean."
        )
    return ""


def clear_binding_if_path_changed(criterion, previous_path):
    """Drop the pinned version when the query path it was pinned for changed."""
    if previous_path != criterion.query_path:
        criterion.query_id = ""
        criterion.commit_id = ""
        criterion.source_sha256 = ""


def bind(change, criterion_pk, user, *, client=None):
    """Pin a criterion to a committed query version, resolved from its path."""
    with transaction.atomic():
        locked = _lock(change)
        if locked is None:
            return _refuse("The change no longer exists.")
        criterion = locked.criteria.filter(pk=criterion_pk).first()
        if criterion is None:
            return _refuse("That criterion is not part of this change.")
        reason = criterion_blocker(criterion)
        if reason:
            return _refuse(reason)
        if client is None:
            try:
                client = locked.source.get_client()
            except Exception as exc:  # noqa: BLE001
                return _refuse(f"The Forward source could not be used: {exc}")
        result = bind_criterion(criterion, client)
        if not result.bound:
            return _refuse(f"Not bound: {result.error}")
        criterion.query_id = result.query_id
        criterion.commit_id = result.commit_id
        criterion.source_sha256 = result.source_sha256
        criterion.save(update_fields=["query_id", "commit_id", "source_sha256"])
        _journal(
            locked,
            user,
            f"Bound criterion '{criterion.name}' to a committed query version.",
        )
    return CriterionOutcome(True, f"Bound '{criterion.name}'.")


def delete(change, criterion_pk, user):
    with transaction.atomic():
        locked = _lock(change)
        if locked is None:
            return _refuse("The change no longer exists.")
        criterion = locked.criteria.filter(pk=criterion_pk).first()
        if criterion is None:
            return _refuse("That criterion is not part of this change.")
        reason = criterion_blocker(criterion)
        if reason:
            return _refuse(reason)
        name = criterion.name
        criterion.delete()
        _journal(locked, user, f"Removed criterion '{name}'.")
    return CriterionOutcome(True, f"Removed '{name}'.")


@contextmanager
def locked_for_edit(change_id, criterion_pk=None):
    """Hold the change's row and re-check the freeze against what is stored now.

    The REST API and the form both check the rules before they write, and a
    staging or an evidence write can land in between. Yields the locked change; a
    refusal is raised as `CriterionRefused` with the reason.
    """
    from ..models import ForwardChange
    from ..models import ForwardChangeCriterion

    with transaction.atomic():
        locked = ForwardChange.objects.select_for_update().filter(pk=change_id).first()
        if locked is None:
            raise CriterionRefused("The change no longer exists.")
        if criterion_pk:
            probe = ForwardChangeCriterion.objects.filter(
                pk=criterion_pk, change=locked
            ).first()
            if probe is None:
                raise CriterionRefused("That criterion is not part of this change.")
        else:
            probe = ForwardChangeCriterion(change=locked)
        reason = criterion_blocker(probe)
        if reason:
            raise CriterionRefused(reason)
        yield locked


class CriterionRefused(Exception):
    """A criterion edit the rules refuse; the message says why."""
