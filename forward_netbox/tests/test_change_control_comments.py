from django.contrib.contenttypes.models import ContentType
from users.models import ObjectPermission

from forward_netbox.change_control.choices import ForwardChangeStateChoices as State
from forward_netbox.change_control.comments import add_comment
from forward_netbox.change_control.comments import delete_comment
from forward_netbox.change_control.comments import delete_reply
from forward_netbox.change_control.comments import edit_comment
from forward_netbox.change_control.comments import edit_reply
from forward_netbox.change_control.comments import reply_to_comment
from forward_netbox.change_control.comments import set_resolved
from forward_netbox.models import ForwardChangeComment
from forward_netbox.models import ForwardChangeCommentReply
from forward_netbox.tests.test_change_control_actions import ChangeFixture


def grant(user, model, *actions):
    """Give `user` the NetBox object permission for `actions` on `model`."""
    permission = ObjectPermission.objects.create(
        name=f"{user.username}-{model.__name__}-{'-'.join(actions)}",
        actions=list(actions),
    )
    permission.object_types.add(ContentType.objects.get_for_model(model))
    permission.users.add(user)


class CommentFixture(ChangeFixture):
    def setUp(self):
        super().setUp()
        self.open_change = self.change(State.STAGED, branch=self.branch)

    def comment(self, author=None, body="Is the second uplink drained first?"):
        return add_comment(self.open_change, author or self.reviewer, body).comment


class AddingCommentsTest(CommentFixture):
    def test_a_comment_is_recorded_with_its_author(self):
        outcome = add_comment(self.open_change, self.reviewer, "  Looks fine.  ")

        self.assertTrue(outcome.ok, outcome.message)
        comment = ForwardChangeComment.objects.get()
        self.assertEqual(comment.author, self.reviewer)
        self.assertEqual(comment.body, "Looks fine.")
        self.assertFalse(comment.resolved)

    def test_an_empty_comment_is_refused(self):
        outcome = add_comment(self.open_change, self.reviewer, "   ")

        self.assertFalse(outcome.ok)
        self.assertFalse(ForwardChangeComment.objects.exists())

    def test_a_finished_change_takes_no_more_comments(self):
        closed = self.change(State.CLOSED)

        outcome = add_comment(closed, self.reviewer, "Too late.")

        self.assertFalse(outcome.ok)
        self.assertFalse(ForwardChangeComment.objects.exists())

    def test_the_change_counts_its_comments(self):
        self.comment()
        self.comment(body="Second")

        self.assertEqual(self.open_change.comment_count, 2)


class RepliesTest(CommentFixture):
    def test_a_reply_hangs_off_its_comment(self):
        comment = self.comment()

        outcome = reply_to_comment(comment, self.requester, "Yes, drained first.")

        self.assertTrue(outcome.ok, outcome.message)
        reply = ForwardChangeCommentReply.objects.get()
        self.assertEqual(reply.comment, comment)
        self.assertEqual(reply.author, self.requester)

    def test_an_empty_reply_is_refused(self):
        outcome = reply_to_comment(self.comment(), self.requester, "")

        self.assertFalse(outcome.ok)
        self.assertFalse(ForwardChangeCommentReply.objects.exists())


class OnlyTheAuthorEditsOrDeletesTest(CommentFixture):
    """A reviewer's words are theirs: nobody else may rewrite or remove them."""

    def test_the_author_may_edit_their_comment(self):
        comment = self.comment()

        outcome = edit_comment(comment, self.reviewer, "Reworded.")

        self.assertTrue(outcome.ok, outcome.message)
        comment.refresh_from_db()
        self.assertEqual(comment.body, "Reworded.")

    def test_someone_else_may_not_edit_it(self):
        comment = self.comment()

        outcome = edit_comment(comment, self.requester, "Rewritten by someone else.")

        self.assertFalse(outcome.ok)
        comment.refresh_from_db()
        self.assertNotEqual(comment.body, "Rewritten by someone else.")

    def test_someone_else_may_not_delete_it(self):
        comment = self.comment()

        outcome = delete_comment(comment, self.requester)

        self.assertFalse(outcome.ok)
        self.assertTrue(ForwardChangeComment.objects.filter(pk=comment.pk).exists())

    def test_a_comment_with_replies_cannot_be_deleted(self):
        # Deleting it would take other people's replies with it.
        comment = self.comment()
        reply_to_comment(comment, self.requester, "Yes, drained first.")

        outcome = delete_comment(comment, self.reviewer)

        self.assertFalse(outcome.ok)
        self.assertTrue(ForwardChangeComment.objects.filter(pk=comment.pk).exists())
        self.assertTrue(ForwardChangeCommentReply.objects.exists())

    def test_the_author_may_delete_it(self):
        comment = self.comment()

        outcome = delete_comment(comment, self.reviewer)

        self.assertTrue(outcome.ok, outcome.message)
        self.assertFalse(ForwardChangeComment.objects.exists())

    def test_replies_follow_the_same_rule(self):
        comment = self.comment()
        reply_to_comment(comment, self.requester, "Reply.")
        reply = ForwardChangeCommentReply.objects.get()

        self.assertFalse(edit_reply(reply, self.reviewer, "Not yours.").ok)
        self.assertFalse(delete_reply(reply, self.reviewer).ok)
        self.assertTrue(edit_reply(reply, self.requester, "Mine.").ok)
        self.assertTrue(delete_reply(reply, self.requester).ok)


class ResolvingTest(CommentFixture):
    """Resolving needs the `change` permission on comments, as NetBox's own change
    management does: it settles a conversation, so it is not for everyone who can
    comment."""

    def test_a_user_without_the_permission_cannot_resolve(self):
        comment = self.comment()

        outcome = set_resolved(comment, self.requester, True)

        self.assertFalse(outcome.ok)
        comment.refresh_from_db()
        self.assertFalse(comment.resolved)

    def test_a_user_with_it_resolves_and_the_resolution_is_attributed(self):
        comment = self.comment()
        grant(self.requester, ForwardChangeComment, "change")

        outcome = set_resolved(comment, self.requester, True)

        self.assertTrue(outcome.ok, outcome.message)
        comment.refresh_from_db()
        self.assertTrue(comment.resolved)
        self.assertEqual(comment.resolved_by, self.requester)
        self.assertIsNotNone(comment.resolved_at)

    def test_a_permission_limited_to_other_comments_does_not_resolve_this_one(self):
        comment = self.comment()
        other = self.comment(body="A different comment.")
        permission = ObjectPermission.objects.create(
            name="resolve-only-the-other",
            actions=["change"],
            constraints={"pk": other.pk},
        )
        permission.object_types.add(
            ContentType.objects.get_for_model(ForwardChangeComment)
        )
        permission.users.add(self.requester)

        self.assertFalse(set_resolved(comment, self.requester, True).ok)
        self.assertTrue(set_resolved(other, self.requester, True).ok)

    def test_unresolving_clears_the_attribution(self):
        comment = self.comment()
        grant(self.requester, ForwardChangeComment, "change")
        set_resolved(comment, self.requester, True)

        outcome = set_resolved(comment, self.requester, False)

        self.assertTrue(outcome.ok, outcome.message)
        comment.refresh_from_db()
        self.assertFalse(comment.resolved)
        self.assertIsNone(comment.resolved_by)
        self.assertIsNone(comment.resolved_at)
