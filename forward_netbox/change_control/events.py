# Telling people and systems what happened to a change.
#
# Two channels, both best effort: NetBox notifications for the people who need to
# act (reviewers when a change is submitted, the requester when it moves), and
# NetBox event rules (webhooks, scripts) for everything else. Nothing here can
# fail a transition: the state has already changed, so a broken webhook or a
# deleted user must not undo or hide it.
import logging

from django.db import transaction
from django.utils.translation import gettext_lazy as _
from netbox.events import EVENT_TYPE_KIND_DANGER
from netbox.events import EVENT_TYPE_KIND_SUCCESS
from netbox.events import EVENT_TYPE_KIND_WARNING
from netbox.events import EventType
from netbox.registry import registry

from .choices import ForwardChangeStateChoices as State

logger = logging.getLogger("forward_netbox.change_control")

SUBMITTED = "forward_change_submitted"
APPROVED = "forward_change_approved"
CHANGES_REQUESTED = "forward_change_changes_requested"
VERIFIED = "forward_change_verified"
CLOSED = "forward_change_closed"
ABANDONED = "forward_change_abandoned"
COMMENT_ADDED = "forward_change_comment_added"
VALIDATION_COMPLETED = "forward_change_validation_completed"

EVENT_TYPES = (
    EventType(SUBMITTED, _("Forward change submitted for review")),
    EventType(APPROVED, _("Forward change approved"), kind=EVENT_TYPE_KIND_SUCCESS),
    EventType(
        CHANGES_REQUESTED,
        _("Forward change: changes requested"),
        kind=EVENT_TYPE_KIND_WARNING,
    ),
    EventType(VERIFIED, _("Forward change verified")),
    EventType(CLOSED, _("Forward change closed"), kind=EVENT_TYPE_KIND_SUCCESS),
    EventType(ABANDONED, _("Forward change abandoned"), kind=EVENT_TYPE_KIND_DANGER),
    EventType(COMMENT_ADDED, _("Forward change comment added")),
    EventType(VALIDATION_COMPLETED, _("Forward change validation completed")),
)

_BY_STATE = {
    State.STAGED: SUBMITTED,
    State.APPROVED: APPROVED,
    State.VERIFIED_PROCEED: VERIFIED,
    State.VERIFIED_HOLD: VERIFIED,
    State.CLOSED: CLOSED,
    State.ABANDONED: ABANDONED,
}


def register_event_types():
    """Register the event types once; safe to call again."""
    for event in EVENT_TYPES:
        if event.name not in registry["event_types"]:
            event.register()


def payload(change, **extra):
    """What an event rule receives: identity and status, never evidence or rows."""
    from .status import review_status

    data = {
        "id": change.pk,
        "ref": change.ref,
        "title": change.title,
        "state": change.state,
        "status": review_status(change),
        "verdict": change.verdict,
        "priority": change.priority,
        "branch": change.branch_name,
        "display": str(change),
        "requester": change.requester.username if change.requester_id else None,
        "has_workspace": bool(change.workspace_network_id),
    }
    data.update(extra)
    return data


def _reviewer_ids(change):
    """Users who could review this change, or an empty set when any user may.

    A rule open to any reviewer names nobody to tell, and telling everyone is not
    what "any reviewer" means.
    """
    from .policy import applicable_policies

    ids = set()
    for policy in applicable_policies(change):
        for rule in policy.approval_rules.all():
            eligible = rule.eligible_user_ids()
            if eligible:
                ids |= set(eligible)
    return ids


def recipients(change, event_name, actor=None, extra_users=(), notify_actor=False):
    """The user ids to notify; not the person who did it, unless asked."""
    ids = set(extra_users)
    if change.requester_id:
        ids.add(change.requester_id)
    if event_name == SUBMITTED or (
        event_name == VERIFIED and change.state == State.VERIFIED_PROCEED
    ):
        ids |= _reviewer_ids(change)
    if actor is not None:
        actor_pk = getattr(actor, "pk", actor)
        if notify_actor:
            ids.add(actor_pk)
        else:
            ids.discard(actor_pk)
    return ids


def _can_view(user, change):
    """Whether this user may open the change: the notification links to it."""
    from ..models import ForwardChange

    return ForwardChange.objects.restrict(user, "view").filter(pk=change.pk).exists()


def _notify(change, event_name, user_ids):
    from django.contrib.auth import get_user_model
    from django.contrib.contenttypes.models import ContentType
    from django.utils import timezone
    from extras.models import Notification

    if not user_ids:
        return
    content_type = ContentType.objects.get_for_model(type(change))
    for user in get_user_model().objects.filter(pk__in=user_ids, is_active=True):
        # Only people who could open the change are told its title.
        if not _can_view(user, change):
            continue
        Notification.objects.update_or_create(
            object_type=content_type,
            object_id=change.pk,
            user=user,
            defaults={
                "event_type": event_name,
                "object_repr": str(change)[:200],
                "read": None,
                "created": timezone.now(),
            },
        )


def _fire_event_rules(change, event_name, user, data):
    from core.models import ObjectType
    from extras.events import process_event_rules
    from extras.models import EventRule

    object_type = ObjectType.objects.get_for_model(type(change))
    rules = EventRule.objects.filter(
        event_types__contains=[event_name], enabled=True, object_types=object_type
    )
    if not rules:
        return
    process_event_rules(
        event_rules=rules,
        object_type=object_type,
        event={
            "event_type": event_name,
            "data": data,
            "username": user.username if user else None,
        },
    )


def announce(
    change, event_name, user=None, *, extra_users=(), notify_actor=False, **extra
):
    """Notify the people involved and fire event rules, once the change is committed.

    All of it happens after the surrounding transaction commits and is guarded, so a
    failed query, a deleted user or a broken webhook can neither abort that
    transaction nor undo the transition it reports. Where there is no surrounding
    transaction it runs at once.
    """
    change_pk = change.pk
    user_pk = getattr(user, "pk", None)
    extra_users = tuple(extra_users)
    transaction.on_commit(
        lambda: _deliver(
            change_pk, event_name, user_pk, extra_users, notify_actor, extra
        )
    )


def _deliver(change_pk, event_name, user_pk, extra_users, notify_actor, extra):
    from django.contrib.auth import get_user_model

    from ..models import ForwardChange

    try:
        change = (
            ForwardChange.objects.select_related("requester")
            .filter(pk=change_pk)
            .first()
        )
        if change is None:
            return
        user = get_user_model().objects.filter(pk=user_pk).first() if user_pk else None
        data = payload(change, **extra)
    except Exception:  # noqa: BLE001
        logger.exception("Could not prepare %s for change %s", event_name, change_pk)
        return
    try:
        with transaction.atomic():
            _notify(
                change,
                event_name,
                recipients(change, event_name, user, extra_users, notify_actor),
            )
    except Exception:  # noqa: BLE001
        logger.exception(
            "Could not notify about %s on change %s", event_name, change_pk
        )
    try:
        _fire_event_rules(change, event_name, user, data)
    except Exception:  # noqa: BLE001
        logger.exception(
            "Event rules for %s on change %s failed", event_name, change_pk
        )


def announce_transition(change, target, user=None):
    """The event for a change reaching `target`, if that state has one."""
    event_name = _BY_STATE.get(target)
    if event_name:
        announce(change, event_name, user)
