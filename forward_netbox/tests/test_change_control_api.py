from django.urls import reverse
from rest_framework.test import APIClient
from users.models import Token

from forward_netbox.change_control.choices import ForwardChangeStateChoices as State
from forward_netbox.models import ForwardChangeApprovalRule
from forward_netbox.models import ForwardChangeComment
from forward_netbox.models import ForwardChangeCriterion
from forward_netbox.models import ForwardChangeDevice
from forward_netbox.models import ForwardChangePolicy
from forward_netbox.models import ForwardIngestion
from forward_netbox.models import ForwardSync
from forward_netbox.tests.test_change_control_actions import ChangeFixture
from forward_netbox.tests.test_change_control_comments import grant


class ApiFixture(ChangeFixture):
    def setUp(self):
        super().setUp()
        self.change_ = self.change(State.SCOPED)
        self.token = Token.objects.create(user=self.reviewer, write_enabled=True)
        self.api = APIClient()
        self.api.force_authenticate(user=self.reviewer, token=self.token)

    def can(self, model, *actions):
        for action in actions:
            grant(self.reviewer, model, action)

    def url(self, name, *args):
        return reverse(f"plugins-api:forward_netbox-api:{name}", args=args)


class ChangeApiTest(ApiFixture):
    def test_a_change_is_listed_with_its_derived_status(self):
        self.can(type(self.change_), "view")

        response = self.api.get(self.url("forwardchange-list"))

        self.assertEqual(response.status_code, 200)
        row = response.json()["results"][0]
        self.assertEqual(row["state"]["value"], "scoped")
        self.assertEqual(row["status"], "draft")

    def test_state_cannot_be_written_through_the_serializer(self):
        self.can(type(self.change_), "view", "change")

        response = self.api.patch(
            self.url("forwardchange-detail", self.change_.pk),
            {"state": "closed", "title": "renamed"},
            format="json",
        )

        self.change_.refresh_from_db()
        self.assertEqual(self.change_.state, State.SCOPED)
        self.assertEqual(self.change_.title, "renamed", response.content)

    def test_a_branch_a_sync_owns_is_refused(self):
        self.can(type(self.change_), "view", "change")
        sync = ForwardSync.objects.create(name="s-api", source=self.source)
        ForwardIngestion.objects.create(sync=sync, branch=self.branch)

        response = self.api.patch(
            self.url("forwardchange-detail", self.change_.pk),
            {"branch": self.branch.pk},
            format="json",
        )

        self.assertEqual(response.status_code, 400)

    def test_transition_refuses_a_move_a_person_cannot_make(self):
        self.can(type(self.change_), "view", "change")
        self.change_.state = State.COLLECTED
        self.change_.save()

        response = self.api.post(
            self.url("forwardchange-transition", self.change_.pk),
            {"target": "verified-proceed"},
            format="json",
        )

        self.assertEqual(response.status_code, 409)
        self.change_.refresh_from_db()
        self.assertEqual(self.change_.state, State.COLLECTED)

    def test_transition_reports_what_is_missing(self):
        self.can(type(self.change_), "view", "change")

        response = self.api.post(
            self.url("forwardchange-transition", self.change_.pk),
            {"target": "baselined"},
            format="json",
        )

        self.assertEqual(response.status_code, 409)
        self.assertTrue(response.json()["reasons"])

    def test_an_action_needs_the_change_permission(self):
        self.can(type(self.change_), "view")

        response = self.api.post(
            self.url("forwardchange-devices-add", self.change_.pk),
            {"names": ["x"]},
            format="json",
        )

        self.assertEqual(response.status_code, 403)

    def test_devices_are_added_through_the_same_service(self):
        self.can(type(self.change_), "view", "change")
        device = self.device()

        response = self.api.post(
            self.url("forwardchange-devices-add", self.change_.pk),
            {"names": [device.name.upper(), device.name]},
            format="json",
        )

        self.assertEqual(response.status_code, 200, response.content)
        self.assertEqual(ForwardChangeDevice.objects.get().device, device)

    def test_a_comment_needs_its_own_permission(self):
        self.can(type(self.change_), "view", "change")

        response = self.api.post(
            self.url("forwardchange-comment", self.change_.pk),
            {"body": "hello"},
            format="json",
        )
        self.assertEqual(response.status_code, 403)

        self.can(ForwardChangeComment, "add")
        # Permissions are cached on the user object, so authenticate afresh.
        fresh = type(self.reviewer).objects.get(pk=self.reviewer.pk)
        self.api.force_authenticate(user=fresh, token=self.token)
        response = self.api.post(
            self.url("forwardchange-comment", self.change_.pk),
            {"body": "hello"},
            format="json",
        )
        self.assertEqual(response.status_code, 200, response.content)


class CriterionApiTest(ApiFixture):
    def setUp(self):
        super().setUp()
        self.can(ForwardChangeCriterion, "view", "add", "change", "delete")

    def payload(self, **extra):
        values = {
            "change": self.change_.pk,
            "name": "no-isolated",
            "query_path": "/q",
            "family": "acceptance",
            "expectation": "no-rows",
        }
        values.update(extra)
        return values

    def test_a_criterion_is_created_while_planning(self):
        response = self.api.post(
            self.url("forwardchangecriterion-list"), self.payload(), format="json"
        )

        self.assertEqual(response.status_code, 201, response.content)

    def test_it_is_refused_once_the_change_is_staged(self):
        self.change_.state = State.STAGED
        self.change_.save()

        response = self.api.post(
            self.url("forwardchangecriterion-list"), self.payload(), format="json"
        )

        self.assertEqual(response.status_code, 400)
        self.assertFalse(ForwardChangeCriterion.objects.exists())

    def test_the_pinned_version_cannot_be_written(self):
        response = self.api.post(
            self.url("forwardchangecriterion-list"),
            self.payload(query_id="forged", commit_id="forged"),
            format="json",
        )

        self.assertEqual(response.status_code, 201, response.content)
        criterion = ForwardChangeCriterion.objects.get()
        self.assertEqual((criterion.query_id, criterion.commit_id), ("", ""))

    def test_a_new_path_drops_the_binding(self):
        criterion = ForwardChangeCriterion.objects.create(
            change=self.change_,
            name="c",
            query_path="/a",
            query_id="q",
            commit_id="c",
        )

        response = self.api.patch(
            self.url("forwardchangecriterion-detail", criterion.pk),
            {"query_path": "/b"},
            format="json",
        )

        self.assertEqual(response.status_code, 200, response.content)
        criterion.refresh_from_db()
        self.assertEqual(criterion.query_id, "")

    def test_deleting_one_with_evidence_is_refused(self):
        from forward_netbox.models import ForwardChangeEvidence

        criterion = ForwardChangeCriterion.objects.create(
            change=self.change_, name="c", query_path="/a"
        )
        ForwardChangeEvidence.objects.create(
            criterion=criterion, phase="before", passed=True, snapshot_id="s"
        )

        response = self.api.delete(
            self.url("forwardchangecriterion-detail", criterion.pk)
        )

        self.assertEqual(response.status_code, 400)
        self.assertTrue(ForwardChangeCriterion.objects.filter(pk=criterion.pk).exists())


class ApprovalRuleApiTest(ApiFixture):
    def test_a_rule_that_names_nobody_is_refused(self):
        self.can(ForwardChangeApprovalRule, "view", "add")
        policy = ForwardChangePolicy.objects.create(name="p-api")

        response = self.api.post(
            self.url("forwardchangeapprovalrule-list"),
            {"policy": policy.pk, "name": "nobody", "min_reviews": 1},
            format="json",
        )

        self.assertEqual(response.status_code, 400)

    def test_an_open_rule_is_accepted(self):
        self.can(ForwardChangeApprovalRule, "view", "add")
        policy = ForwardChangePolicy.objects.create(name="p-api2")

        response = self.api.post(
            self.url("forwardchangeapprovalrule-list"),
            {
                "policy": policy.pk,
                "name": "anyone",
                "min_reviews": 1,
                "any_reviewer": True,
            },
            format="json",
        )

        self.assertEqual(response.status_code, 201, response.content)


class AuditFixesApiTest(ApiFixture):
    def test_a_criterion_cannot_be_moved_to_another_change(self):
        self.can(ForwardChangeCriterion, "view", "change")
        criterion = ForwardChangeCriterion.objects.create(
            change=self.change_, name="c", query_path="/a"
        )
        other = self.change(State.DRAFT)

        response = self.api.patch(
            self.url("forwardchangecriterion-detail", criterion.pk),
            {"change": other.pk},
            format="json",
        )

        self.assertEqual(response.status_code, 400)
        criterion.refresh_from_db()
        self.assertEqual(criterion.change_id, self.change_.pk)

    def test_a_baselined_changes_criterion_cannot_be_moved_out(self):
        self.can(ForwardChangeCriterion, "view", "change")
        self.change_.state = State.STAGED
        self.change_.save()
        criterion = ForwardChangeCriterion.objects.create(
            change=self.change_, name="c", query_path="/a"
        )
        draft = self.change(State.DRAFT)

        response = self.api.patch(
            self.url("forwardchangecriterion-detail", criterion.pk),
            {"change": draft.pk},
            format="json",
        )

        self.assertEqual(response.status_code, 400)

    def test_a_review_without_a_decision_is_refused_not_recorded_as_a_rejection(self):
        self.can(type(self.change_), "view", "approve")
        self.change_.state = State.STAGED
        self.change_.save()

        response = self.api.post(
            self.url("forwardchange-review", self.change_.pk),
            {"comment": "lgtm", "anchor": "x"},
            format="json",
        )

        self.assertEqual(response.status_code, 400)
        self.assertFalse(self.change_.reviews.exists())

    def test_the_review_anchor_is_published_and_a_wrong_one_is_refused(self):
        self.can(type(self.change_), "view", "approve")
        self.change_.state = State.STAGED
        self.change_.save()

        response = self.api.post(
            self.url("forwardchange-review", self.change_.pk),
            {"decision": "approve", "anchor": "stale"},
            format="json",
        )

        self.assertEqual(response.status_code, 409)
        self.assertFalse(self.change_.reviews.exists())

    def test_the_evidence_endpoint_lists(self):
        from forward_netbox.models import ForwardChangeEvidence

        self.can(ForwardChangeEvidence, "view")
        criterion = ForwardChangeCriterion.objects.create(
            change=self.change_, name="c", query_path="/a"
        )
        ForwardChangeEvidence.objects.create(
            criterion=criterion, phase="before", passed=True, snapshot_id="s"
        )

        response = self.api.get(self.url("forwardchangeevidence-list"))

        self.assertEqual(response.status_code, 200, response.content)
        self.assertEqual(response.json()["count"], 1)

    def test_a_change_can_be_created_with_the_callers_permission(self):
        self.can(type(self.change_), "view", "add")

        response = self.api.post(
            self.url("forwardchange-list"),
            {"title": "via api", "source": self.source.pk},
            format="json",
        )

        self.assertEqual(response.status_code, 201, response.content)
        self.assertEqual(response.json()["requester"]["id"], self.reviewer.pk)

    def test_a_change_the_user_may_not_see_is_not_found_by_an_action(self):
        # An object-level constraint on `change` that excludes this change.
        from users.models import ObjectPermission
        from django.contrib.contenttypes.models import ContentType

        permission = ObjectPermission.objects.create(
            name="only-other", actions=["change", "view"], constraints={"title": "nope"}
        )
        permission.object_types.add(
            ContentType.objects.get_for_model(type(self.change_))
        )
        permission.users.add(self.reviewer)

        response = self.api.post(
            self.url("forwardchange-devices-resolve", self.change_.pk), format="json"
        )

        self.assertEqual(response.status_code, 404)

    def test_a_review_of_a_finished_change_is_refused(self):
        from forward_netbox.change_control.actions import record_review

        self.change_.state = State.CLOSED
        self.change_.save()

        outcome = record_review(self.change_, self.reviewer, True)

        self.assertFalse(outcome.ok)
