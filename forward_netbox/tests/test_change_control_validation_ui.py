import json
from unittest import mock

from django.urls import reverse

from forward_netbox.change_control.choices import ForwardChangeStateChoices as State
from forward_netbox.change_control.packs import export_policy
from forward_netbox.change_control.packs import import_policy
from forward_netbox.change_control.packs import install_pack
from forward_netbox.change_control.packs import PACKS
from forward_netbox.change_control.validation import rule_parameter_problems
from forward_netbox.forms import ForwardChangeValidationRuleForm
from forward_netbox.models import ForwardChangeDevice
from forward_netbox.models import ForwardChangeValidationPolicy
from forward_netbox.models import ForwardChangeValidationResult
from forward_netbox.models import ForwardChangeValidationRule
from forward_netbox.models import ForwardChangeValidationRun
from forward_netbox.tests.test_change_control_actions import ChangeFixture
from forward_netbox.tests.test_change_control_api import ApiFixture
from forward_netbox.tests.test_change_control_comments import grant
from forward_netbox.views import _change_control_bundle_payload


class ParameterRulesTest(ChangeFixture):
    def test_each_engine_names_what_is_missing(self):
        self.assertTrue(rule_parameter_problems("nqe", {}))
        self.assertTrue(
            rule_parameter_problems(
                "nqe", {"query_path": "/q", "expectation": "no-diff"}
            )
        )
        self.assertFalse(rule_parameter_problems("nqe", {"query_path": "/q"}))
        self.assertTrue(rule_parameter_problems("check", {"check_types": "isolation"}))
        self.assertFalse(rule_parameter_problems("check", {}))
        self.assertTrue(rule_parameter_problems("predict", {"x": 1}))
        self.assertTrue(rule_parameter_problems("nqe", []))

    def test_the_form_refuses_bad_parameters(self):
        policy = ForwardChangeValidationPolicy.objects.create(name="p")
        form = ForwardChangeValidationRuleForm(
            data={
                "policy": policy.pk,
                "name": "r",
                "engine": "nqe",
                "severity": "error",
                "enabled": "on",
                "parameters": "{}",
            }
        )

        self.assertFalse(form.is_valid())


class PackTest(ChangeFixture):
    def test_every_shipped_pack_installs_and_exports_back_the_same(self):
        for key in PACKS:
            policy, problems = install_pack(key)
            self.assertEqual(problems, [], key)
            exported = export_policy(policy)
            self.assertEqual(
                sorted(r["name"] for r in exported["rules"]),
                sorted(r["name"] for r in PACKS[key]["rules"]),
            )

    def test_a_pack_cannot_be_installed_twice_under_one_name(self):
        install_pack("connectivity")

        policy, problems = install_pack("connectivity")

        self.assertIsNone(policy)
        self.assertIn("already exists", problems[0])

    def test_an_unknown_pack_is_refused(self):
        self.assertTrue(install_pack("nope")[1])

    def test_import_names_every_problem_and_creates_nothing(self):
        policy, problems = import_policy(
            {
                "name": "bad",
                "min_score": 500,
                "scope": {"sites": ["no-such-site"]},
                "rules": [
                    {"name": "a", "engine": "nqe", "parameters": {}},
                    {"name": "b", "engine": "wat"},
                ],
            }
        )

        self.assertIsNone(policy)
        text = " ".join(problems)
        for needle in ("min_score", "no-such-site", "query_path", "unknown engine"):
            self.assertIn(needle, text)
        self.assertFalse(
            ForwardChangeValidationPolicy.objects.filter(name="bad").exists()
        )

    def test_scope_round_trips_by_slug(self):
        device = self.device()
        policy = ForwardChangeValidationPolicy.objects.create(name="scoped")
        policy.sites.add(device.site)
        data = export_policy(policy)
        policy.delete()

        again, problems = import_policy(data)

        self.assertEqual(problems, [])
        self.assertEqual(list(again.sites.all()), [device.site])


class ValidationPagesTest(ChangeFixture):
    def setUp(self):
        super().setUp()
        self.change_ = self.change(
            State.SCOPED, workspace_network_id="ws", before_snapshot_id="s1"
        )
        ForwardChangeDevice.objects.create(
            change=self.change_, device=self.device(), forward_device_key="sw"
        )
        for model in (
            type(self.change_),
            ForwardChangeValidationPolicy,
            ForwardChangeValidationRule,
            ForwardChangeValidationRun,
        ):
            for action in ("view", "add", "change", "delete"):
                grant(self.reviewer, model, action)
        self.client.force_login(self.reviewer)
        self.policy = ForwardChangeValidationPolicy.objects.create(name="pol")
        ForwardChangeValidationRule.objects.create(
            policy=self.policy, name="r", engine="predict"
        )

    def test_the_policy_list_and_detail_render(self):
        list_url = reverse("plugins:forward_netbox:forwardchangevalidationpolicy_list")
        detail = reverse(
            "plugins:forward_netbox:forwardchangevalidationpolicy",
            args=[self.policy.pk],
        )

        self.assertEqual(self.client.get(list_url).status_code, 200)
        response = self.client.get(detail)
        self.assertContains(response, "Add rule")

    def test_the_rule_add_page_renders(self):
        url = reverse("plugins:forward_netbox:forwardchangevalidationrule_add")
        self.assertEqual(
            self.client.get(f"{url}?policy={self.policy.pk}").status_code, 200
        )

    def test_deleting_a_policy_and_a_rule_from_the_ui_does_not_crash(self):
        rule = self.policy.rules.get()
        rule_url = reverse(
            "plugins:forward_netbox:forwardchangevalidationrule_delete", args=[rule.pk]
        )
        policy_url = reverse(
            "plugins:forward_netbox:forwardchangevalidationpolicy_delete",
            args=[self.policy.pk],
        )

        self.assertEqual(self.client.post(rule_url, {"confirm": True}).status_code, 302)
        self.assertEqual(
            self.client.post(policy_url, {"confirm": True}).status_code, 302
        )
        self.assertFalse(ForwardChangeValidationPolicy.objects.exists())

    def test_the_run_page_renders_with_its_results(self):
        run = ForwardChangeValidationRun.objects.create(
            change=self.change_, status="completed", score=90
        )
        ForwardChangeValidationResult.objects.create(
            run=run, engine="check", check_name="c", status="skip", message="m"
        )

        response = self.client.get(run.get_absolute_url())

        self.assertContains(response, "never a pass")

    def test_the_change_page_shows_the_validation_card_and_blockers(self):
        self.policy.require_passed_run = True
        self.policy.save()

        response = self.client.get(self.change_.get_absolute_url())

        self.assertContains(response, "Run validation")
        self.assertContains(response, "requires a completed validation run")

    def test_the_validate_button_queues_a_run(self):
        with mock.patch("forward_netbox.jobs.ChangeValidationJob.enqueue") as queued:
            response = self.client.post(
                reverse(
                    "plugins:forward_netbox:forwardchange_validate",
                    args=[self.change_.pk],
                )
            )

        self.assertEqual(response.status_code, 302)
        queued.assert_called_once()
        self.assertEqual(self.change_.validation_runs.count(), 1)

    def test_the_verify_button_queues_a_job_instead_of_running_inline(self):
        self.change_.state = State.APPLIED
        self.change_.save()

        with mock.patch("forward_netbox.jobs.ChangeVerifyJob.enqueue") as queued:
            self.client.post(
                reverse(
                    "plugins:forward_netbox:forwardchange_verify",
                    args=[self.change_.pk],
                )
            )

        queued.assert_called_once()

    def test_retrigger_runs_the_same_policy_again(self):
        run = ForwardChangeValidationRun.objects.create(
            change=self.change_, policy=self.policy, status="completed"
        )

        with mock.patch("forward_netbox.jobs.ChangeValidationJob.enqueue") as queued:
            self.client.post(
                reverse(
                    "plugins:forward_netbox:forwardchangevalidationrun_retrigger",
                    args=[run.pk],
                )
            )

        queued.assert_called_once()

    def test_the_pack_page_installs_a_policy(self):
        url = reverse("plugins:forward_netbox:forwardchangevalidationpolicy_packs")
        self.assertEqual(self.client.get(url).status_code, 200)

        response = self.client.post(url, {"pack": "connectivity"})

        self.assertEqual(response.status_code, 302)
        self.assertTrue(
            ForwardChangeValidationPolicy.objects.filter(name="Connectivity").exists()
        )


class ValidationApiTest(ApiFixture):
    def setUp(self):
        super().setUp()
        self.change_.workspace_network_id = "ws"
        self.change_.before_snapshot_id = "s1"
        self.change_.save()
        self.policy = ForwardChangeValidationPolicy.objects.create(name="pol")
        ForwardChangeValidationRule.objects.create(
            policy=self.policy, name="r", engine="predict"
        )

    def test_validate_queues_a_run_through_the_service(self):
        self.can(type(self.change_), "view", "change")

        with mock.patch("forward_netbox.jobs.ChangeValidationJob.enqueue"):
            response = self.api.post(
                self.url("forwardchange-validate", self.change_.pk), {}, format="json"
            )

        self.assertEqual(response.status_code, 200, response.content)
        self.assertTrue(response.json()["run_id"])

    def test_validate_reports_why_it_cannot_start(self):
        self.can(type(self.change_), "view", "change")
        self.change_.workspace_network_id = ""
        self.change_.save()

        response = self.api.post(
            self.url("forwardchange-validate", self.change_.pk), {}, format="json"
        )

        self.assertEqual(response.status_code, 409)

    def test_a_rule_with_bad_parameters_is_refused(self):
        self.can(ForwardChangeValidationRule, "view", "add")

        response = self.api.post(
            self.url("forwardchangevalidationrule-list"),
            {"policy": self.policy.pk, "name": "x", "engine": "nqe", "parameters": {}},
            format="json",
        )

        self.assertEqual(response.status_code, 400)

    def test_export_then_import_round_trips(self):
        self.can(ForwardChangeValidationPolicy, "view", "add")
        self.can(ForwardChangeValidationRule, "view", "add")
        exported = self.api.get(
            self.url("forwardchangevalidationpolicy-export", self.policy.pk)
        ).json()
        exported["name"] = "copy"

        response = self.api.post(
            self.url("forwardchangevalidationpolicy-import-policy"),
            json.dumps(exported),
            content_type="application/json",
        )

        self.assertEqual(response.status_code, 201, response.content)
        self.assertEqual(
            ForwardChangeValidationPolicy.objects.get(name="copy").rules.count(), 1
        )

    def test_import_needs_the_rule_permission_too(self):
        self.can(ForwardChangeValidationPolicy, "view", "add")

        response = self.api.post(
            self.url("forwardchangevalidationpolicy-import-policy"),
            {"name": "x"},
            format="json",
        )

        self.assertEqual(response.status_code, 403)

    def test_install_pack(self):
        self.can(ForwardChangeValidationPolicy, "view", "add")
        self.can(ForwardChangeValidationRule, "view", "add")

        response = self.api.post(
            self.url("forwardchangevalidationpolicy-install-pack"),
            {"pack": "intent"},
            format="json",
        )

        self.assertEqual(response.status_code, 201, response.content)

    def test_runs_and_results_are_listed(self):
        self.can(ForwardChangeValidationRun, "view")
        self.can(ForwardChangeValidationResult, "view")
        run = ForwardChangeValidationRun.objects.create(change=self.change_)
        ForwardChangeValidationResult.objects.create(
            run=run, engine="check", check_name="c", status="pass"
        )

        self.assertEqual(
            self.api.get(self.url("forwardchangevalidationrun-list")).status_code, 200
        )
        self.assertEqual(
            self.api.get(self.url("forwardchangevalidationresult-list")).json()[
                "count"
            ],
            1,
        )


class BundleTest(ChangeFixture):
    def test_the_bundle_section_carries_counts_and_no_customer_text(self):
        change = self.change(State.SCOPED, title="SECRET-TITLE", ref="SECRET-REF")
        ForwardChangeDevice.objects.create(change=change, device=self.device())
        policy = ForwardChangeValidationPolicy.objects.create(name="SECRET-POLICY")
        run = ForwardChangeValidationRun.objects.create(
            change=change, status="failed", error="SECRET-ERROR"
        )
        ForwardChangeValidationResult.objects.create(
            run=run,
            engine="nqe",
            check_name="SECRET-CHECK",
            device_name="SECRET-DEV",
            status="fail",
            message="SECRET-MSG",
        )

        payload = _change_control_bundle_payload()
        text = json.dumps(payload)

        self.assertEqual(payload["changes"]["by_state"], {"scoped": 1})
        self.assertEqual(payload["scope"]["device_rows_without_forward_name"], 1)
        self.assertEqual(payload["validation"]["failed_run_pks"], [run.pk])
        self.assertEqual(payload["validation"]["results_by_status"], {"fail": 1})
        self.assertNotIn("SECRET", text)
        self.assertEqual(policy.pk, ForwardChangeValidationPolicy.objects.get().pk)
