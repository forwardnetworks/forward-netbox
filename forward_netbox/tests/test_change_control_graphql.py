import json

from django.urls import reverse

from forward_netbox.change_control.choices import ForwardChangeStateChoices as State
from forward_netbox.models import ForwardChangeCriterion
from forward_netbox.models import ForwardChangeValidationPolicy
from forward_netbox.models import ForwardChangeValidationResult
from forward_netbox.models import ForwardChangeValidationRule
from forward_netbox.models import ForwardChangeValidationRun
from forward_netbox.tests.test_change_control_actions import ChangeFixture
from forward_netbox.tests.test_change_control_comments import grant


class GraphQLTest(ChangeFixture):
    def setUp(self):
        super().setUp()
        self.change_ = self.change(State.SCOPED, title="Via graphql", ref="CHG-1")
        for model in (
            type(self.change_),
            ForwardChangeCriterion,
            ForwardChangeValidationPolicy,
            ForwardChangeValidationRule,
            ForwardChangeValidationRun,
            ForwardChangeValidationResult,
        ):
            grant(self.reviewer, model, "view")
        self.client.force_login(self.reviewer)

    def query(self, text):
        response = self.client.post(
            reverse("graphql"),
            data=json.dumps({"query": text}),
            content_type="application/json",
        )
        self.assertEqual(response.status_code, 200, response.content)
        return response.json()

    def test_a_change_is_listed_and_filtered_by_state(self):
        body = self.query(
            '{ forward_change_list(filters: {state: {exact: "scoped"}}) '
            "{ id title ref state priority } }"
        )

        self.assertNotIn("errors", body)
        rows = body["data"]["forward_change_list"]
        self.assertEqual([r["title"] for r in rows], ["Via graphql"])

    def test_a_filter_that_matches_nothing_returns_nothing(self):
        body = self.query(
            '{ forward_change_list(filters: {state: {exact: "closed"}}) { id } }'
        )

        self.assertEqual(body["data"]["forward_change_list"], [])

    def test_a_criterion_reaches_its_change(self):
        ForwardChangeCriterion.objects.create(
            change=self.change_, name="c", query_path="/q"
        )

        body = self.query("{ forward_change_criterion_list { name change { title } } }")

        self.assertNotIn("errors", body)
        self.assertEqual(
            body["data"]["forward_change_criterion_list"][0]["change"]["title"],
            "Via graphql",
        )

    def test_validation_runs_results_and_policies_are_queryable(self):
        policy = ForwardChangeValidationPolicy.objects.create(name="pol")
        ForwardChangeValidationRule.objects.create(
            policy=policy, name="r", engine="predict"
        )
        run = ForwardChangeValidationRun.objects.create(
            change=self.change_, policy=policy, status="completed", score=90
        )
        ForwardChangeValidationResult.objects.create(
            run=run, engine="check", check_name="c", status="pass"
        )

        body = self.query(
            "{ forward_change_validation_policy_list { name } "
            "forward_change_validation_rule_list { name policy { name } } "
            "forward_change_validation_run_list { score status change { id } } "
            "forward_change_validation_result_list { check_name status } }"
        )

        self.assertNotIn("errors", body)
        data = body["data"]
        self.assertEqual(
            data["forward_change_validation_policy_list"][0]["name"], "pol"
        )
        self.assertEqual(data["forward_change_validation_run_list"][0]["score"], 90)
        self.assertEqual(
            data["forward_change_validation_result_list"][0]["status"], "pass"
        )

    def test_a_user_without_view_permission_sees_nothing(self):
        stranger = type(self.reviewer).objects.create(username="gql-stranger")
        self.client.force_login(stranger)

        body = self.query("{ forward_change_list { id } }")

        self.assertEqual(body["data"]["forward_change_list"], [])

    def test_results_need_their_own_view_permission(self):
        run = ForwardChangeValidationRun.objects.create(change=self.change_)
        ForwardChangeValidationResult.objects.create(
            run=run, engine="check", check_name="c", status="pass"
        )
        other = type(self.reviewer).objects.create(username="run-only")
        grant(other, ForwardChangeValidationRun, "view")
        self.client.force_login(other)

        body = self.query("{ forward_change_validation_result_list { id } }")

        self.assertEqual(body["data"]["forward_change_validation_result_list"], [])

    def test_no_evidence_type_or_field_is_exposed(self):
        body = self.query("{ __schema { types { name } } }")
        names = {t["name"] for t in body["data"]["__schema"]["types"]}

        self.assertFalse([n for n in names if "Evidence" in n], names)
        change_type = self.query(
            '{ __type(name: "ForwardChangeType") { fields { name } } }'
        )
        fields = {f["name"] for f in change_type["data"]["__type"]["fields"]}
        self.assertNotIn("evidence", fields)

    def test_lists_can_be_paginated(self):
        for n in range(3):
            self.change(State.SCOPED, title=f"page-{n}")

        body = self.query("{ forward_change_list(pagination: {limit: 2}) { id } }")

        self.assertNotIn("errors", body)
        self.assertEqual(len(body["data"]["forward_change_list"]), 2)
