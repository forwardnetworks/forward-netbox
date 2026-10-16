from django.urls import reverse

from forward_netbox.change_control.choices import ForwardChangeStateChoices as State
from forward_netbox.forms import ForwardChangeApprovalRuleForm
from forward_netbox.models import ForwardChangeApprovalRule
from forward_netbox.models import ForwardChangeComment
from forward_netbox.models import ForwardChangeCommentReply
from forward_netbox.models import ForwardChangePolicy
from forward_netbox.tests.test_change_control_actions import ChangeFixture
from forward_netbox.tests.test_change_control_comments import grant


class CommentViewsTest(ChangeFixture):
    """The endpoints only collect the request; the rules live in the service."""

    def setUp(self):
        super().setUp()
        self.change_ = self.change(State.STAGED)
        grant(self.reviewer, type(self.change_), "view")
        self.client.force_login(self.reviewer)

    def _comment_url(self):
        return reverse(
            "plugins:forward_netbox:forwardchange_comment", args=[self.change_.pk]
        )

    def test_a_user_with_the_permission_adds_a_comment(self):
        grant(self.reviewer, ForwardChangeComment, "add")

        response = self.client.post(self._comment_url(), {"body": "Looks fine."})

        self.assertEqual(response.status_code, 302)
        comment = ForwardChangeComment.objects.get()
        self.assertEqual(comment.author, self.reviewer)
        self.assertEqual(comment.change, self.change_)

    def test_a_user_without_the_permission_cannot(self):
        response = self.client.post(self._comment_url(), {"body": "Sneaky."})

        self.assertEqual(response.status_code, 403)
        self.assertFalse(ForwardChangeComment.objects.exists())

    def test_a_reply_hangs_off_its_comment(self):
        grant(self.reviewer, ForwardChangeComment, "add")
        grant(self.reviewer, ForwardChangeCommentReply, "add")
        comment = ForwardChangeComment.objects.create(
            change=self.change_, author=self.requester, body="Question?"
        )
        url = reverse(
            "plugins:forward_netbox:forwardchange_comment_reply",
            args=[self.change_.pk, comment.pk],
        )

        response = self.client.post(url, {"body": "Answer."})

        self.assertEqual(response.status_code, 302)
        self.assertEqual(ForwardChangeCommentReply.objects.get().comment, comment)

    def test_a_reply_needs_its_own_permission(self):
        grant(self.reviewer, ForwardChangeComment, "add")
        comment = ForwardChangeComment.objects.create(
            change=self.change_, author=self.requester, body="Question?"
        )
        url = reverse(
            "plugins:forward_netbox:forwardchange_comment_reply",
            args=[self.change_.pk, comment.pk],
        )

        response = self.client.post(url, {"body": "Answer."})

        self.assertEqual(response.status_code, 403)
        self.assertFalse(ForwardChangeCommentReply.objects.exists())

    def test_resolving_goes_through_the_service_permission(self):
        comment = ForwardChangeComment.objects.create(
            change=self.change_, author=self.requester, body="Question?"
        )
        url = reverse(
            "plugins:forward_netbox:forwardchange_comment_resolve",
            args=[self.change_.pk, comment.pk],
        )

        self.client.post(url, {"resolved": "1"})
        comment.refresh_from_db()
        self.assertFalse(comment.resolved)

        grant(self.reviewer, ForwardChangeComment, "change")
        self.client.post(url, {"resolved": "1"})
        comment.refresh_from_db()
        self.assertTrue(comment.resolved)

    def test_someone_else_cannot_delete_a_comment_through_the_view(self):
        comment = ForwardChangeComment.objects.create(
            change=self.change_, author=self.requester, body="Mine."
        )
        url = reverse(
            "plugins:forward_netbox:forwardchange_comment_delete",
            args=[self.change_.pk, comment.pk],
        )

        self.client.post(url)

        self.assertTrue(ForwardChangeComment.objects.filter(pk=comment.pk).exists())

    def test_a_comment_of_another_change_is_not_reachable_through_this_one(self):
        other = self.change(State.STAGED)
        comment = ForwardChangeComment.objects.create(
            change=other, author=self.reviewer, body="Elsewhere."
        )
        url = reverse(
            "plugins:forward_netbox:forwardchange_comment_delete",
            args=[self.change_.pk, comment.pk],
        )

        response = self.client.post(url)

        self.assertEqual(response.status_code, 404)
        self.assertTrue(ForwardChangeComment.objects.filter(pk=comment.pk).exists())


class PagesShowThemTest(ChangeFixture):
    def setUp(self):
        super().setUp()
        self.admin = type(self.reviewer).objects.create_superuser(
            username="root-admin", password="x", email="a@example.com"
        )
        self.client.force_login(self.admin)

    def test_the_change_page_lists_its_comments(self):
        change = self.change(State.STAGED, branch=None)
        ForwardChangeComment.objects.create(
            change=change, author=self.reviewer, body="Drain the uplink first."
        )

        response = self.client.get(change.get_absolute_url())

        self.assertContains(response, "Drain the uplink first.")

    def test_the_policy_page_lists_its_reviewer_rules(self):
        policy = ForwardChangePolicy.objects.create(name="p-pages")
        ForwardChangeApprovalRule.objects.create(
            policy=policy, name="architects-sign-off", any_reviewer=True
        )

        response = self.client.get(policy.get_absolute_url())

        self.assertContains(response, "architects-sign-off")


class RuleViewsTest(ChangeFixture):
    def setUp(self):
        super().setUp()
        admin = type(self.reviewer).objects.create_superuser(
            username="root-admin-2", password="x", email="b@example.com"
        )
        self.client.force_login(admin)

    def test_the_add_page_renders_with_its_policy_preselected(self):
        policy = ForwardChangePolicy.objects.create(name="p-add")

        response = self.client.get(
            reverse("plugins:forward_netbox:forwardchangeapprovalrule_add"),
            {"policy": policy.pk},
        )

        self.assertEqual(response.status_code, 200)

    def test_a_rule_can_be_created_through_the_view(self):
        policy = ForwardChangePolicy.objects.create(name="p-post")

        response = self.client.post(
            reverse("plugins:forward_netbox:forwardchangeapprovalrule_add"),
            {
                "policy": policy.pk,
                "name": "any-two",
                "min_reviews": 2,
                "any_reviewer": "on",
            },
        )

        self.assertEqual(response.status_code, 302)
        self.assertEqual(ForwardChangeApprovalRule.objects.get().min_reviews, 2)


class RuleFormTest(ChangeFixture):
    def _form(self, **data):
        policy = ForwardChangePolicy.objects.create(name="p-form")
        base = {"policy": policy.pk, "name": "r", "min_reviews": 1}
        base.update(data)
        return ForwardChangeApprovalRuleForm(data=base)

    def test_a_rule_naming_nobody_is_refused(self):
        form = self._form()

        self.assertFalse(form.is_valid())
        self.assertIn("any reviewer", str(form.errors))

    def test_a_rule_open_to_any_reviewer_is_accepted(self):
        self.assertTrue(self._form(any_reviewer=True).is_valid())
