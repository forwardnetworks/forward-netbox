# Comments on a change, replies to them, and resolving them.
#
# Mirrors the documented behaviour of NetBox's own change management: comments can
# be replied to and resolved, resolving needs the `change` permission on comments,
# and only the author edits or deletes their own words.
from dataclasses import dataclass

from django.db import transaction
from django.utils import timezone

from .choices import ForwardChangeStateChoices as State

RESOLVE_PERMISSION = "forward_netbox.change_forwardchangecomment"


@dataclass(frozen=True)
class CommentOutcome:
    ok: bool
    message: str = ""
    comment: object = None

    def __bool__(self):
        return self.ok


def _refuse(message):
    return CommentOutcome(False, message)


def _clean(body):
    return (body or "").strip()


def add_comment(change, author, body):
    from ..models import ForwardChangeComment

    body = _clean(body)
    if not body:
        return _refuse("A comment cannot be empty.")
    if change.state in State.TERMINAL:
        return _refuse("This change is finished, so it takes no more comments.")
    comment = ForwardChangeComment.objects.create(
        change=change, author=author, body=body
    )
    return CommentOutcome(True, comment=comment)


def reply_to_comment(comment, author, body):
    from ..models import ForwardChangeCommentReply

    body = _clean(body)
    if not body:
        return _refuse("A reply cannot be empty.")
    if comment.change.state in State.TERMINAL:
        return _refuse("This change is finished, so it takes no more replies.")
    reply = ForwardChangeCommentReply.objects.create(
        comment=comment, author=author, body=body
    )
    return CommentOutcome(True, comment=reply)


def _owner_only(item, user, what):
    if item.author_id is None or item.author_id != getattr(user, "pk", None):
        return _refuse(f"Only the author may change or delete their {what}.")
    return None


def edit_comment(comment, user, body):
    refused = _owner_only(comment, user, "comment")
    # `is not None`: a refusal is falsy by design (`__bool__` is `ok`), so
    # `if refused:` would skip every refusal.
    if refused is not None:
        return refused
    body = _clean(body)
    if not body:
        return _refuse("A comment cannot be empty.")
    comment.body = body
    comment.save(update_fields=["body", "last_updated"])
    return CommentOutcome(True, comment=comment)


def delete_comment(comment, user):
    refused = _owner_only(comment, user, "comment")
    # `is not None`: a refusal is falsy by design (`__bool__` is `ok`), so
    # `if refused:` would skip every refusal.
    if refused is not None:
        return refused
    if comment.replies.exists():
        # Deleting it would cascade to other people's replies, and only an author
        # may remove their own words.
        return _refuse(
            "This comment has replies, so it cannot be deleted. Resolve it instead."
        )
    comment.delete()
    return CommentOutcome(True)


def edit_reply(reply, user, body):
    refused = _owner_only(reply, user, "reply")
    # `is not None`: a refusal is falsy by design (`__bool__` is `ok`), so
    # `if refused:` would skip every refusal.
    if refused is not None:
        return refused
    body = _clean(body)
    if not body:
        return _refuse("A reply cannot be empty.")
    reply.body = body
    reply.save(update_fields=["body", "last_updated"])
    return CommentOutcome(True, comment=reply)


def delete_reply(reply, user):
    refused = _owner_only(reply, user, "reply")
    # `is not None`: a refusal is falsy by design (`__bool__` is `ok`), so
    # `if refused:` would skip every refusal.
    if refused is not None:
        return refused
    reply.delete()
    return CommentOutcome(True)


def set_resolved(comment, user, resolved):
    """Resolve or unresolve a comment; needs the `change` permission on comments."""
    if not user.has_perm(RESOLVE_PERMISSION, comment):
        return _refuse("You do not have permission to resolve or unresolve comments.")
    with transaction.atomic():
        comment.resolved = bool(resolved)
        comment.resolved_by = user if resolved else None
        comment.resolved_at = timezone.now() if resolved else None
        comment.save(
            update_fields=["resolved", "resolved_by", "resolved_at", "last_updated"]
        )
    return CommentOutcome(True, comment=comment)
