from unittest import mock

from django.contrib.auth import get_user_model
from django.contrib.contenttypes.models import ContentType
from django.test import RequestFactory
from extras.models import EventRule
from extras.models import Notification
from extras.models import Webhook
from netbox.registry import registry

from forward_netbox.change_control import events
from forward_netbox.change_control.actions import record_review
from forward_netbox.change_control.actions import transition
from forward_netbox.change_control.choices import ForwardChangeStateChoices as State
from forward_netbox.change_control.comments import add_comment
from forward_netbox.models import ForwardChange
from forward_netbox.models import ForwardChangeApprovalRule
from forward_netbox.models import ForwardChangeDevice
from forward_netbox.models import ForwardChangePolicy
from forward_netbox.template_content import ForwardChangeBranchPanel
from forward_netbox.tests.test_change_control_actions import ChangeFixture

User = get_user_model()


class EventTypesTest(ChangeFixture):
    def test_every_event_type_is_registered(self):
        for event in events.EVENT_TYPES:
            self.assertIn(event.name, registry["event_types"])

    def test_registering_again_is_harmless(self):
        events.register_event_types()
        events.register_event_types()


def allow_viewing(user):
    """Give `user` the right to open changes (a notification links to one)."""
    from users.models import ObjectPermission

    permission = ObjectPermission.objects.create(
        name=f"view-{user.pk}", actions=["view"]
    )
    permission.object_types.add(ContentType.objects.get_for_model(ForwardChange))
    permission.users.add(user)


class NotificationTest(ChangeFixture):
    def setUp(self):
        super().setUp()
        self.actor = User.objects.create(username="actor")
        self.policy = ForwardChangePolicy.objects.create(name="ev-policy")
        rule = ForwardChangeApprovalRule.objects.create(
            policy=self.policy, name="named", min_reviews=1
        )
        rule.reviewers.add(self.reviewer)
        for user in (self.reviewer, self.requester, self.actor):
            allow_viewing(user)

    def notified(self, user, change):
        return Notification.objects.filter(
            user=user,
            object_type=ContentType.objects.get_for_model(ForwardChange),
            object_id=change.pk,
        )

    def staged_change(self):
        change = self.change(
            State.BASELINED,
            branch=self.branch,
            before_snapshot_id="b",
            workspace_network_id="ws",
        )
        ForwardChangeDevice.objects.create(
            change=change, device=self.device(), forward_device_key="sw"
        )
        return change

    def test_submitting_a_change_notifies_the_reviewers_but_not_the_actor(self):
        change = self.staged_change()

        with self.captureOnCommitCallbacks(execute=True):
            outcome = transition(change, State.STAGED, self.actor)

        self.assertTrue(outcome.ok, outcome.blockers)
        self.assertEqual(
            self.notified(self.reviewer, change).get().event_type, events.SUBMITTED
        )
        self.assertFalse(self.notified(self.actor, change).exists())
        # The requester hears too, as the actor was someone else.
        self.assertTrue(self.notified(self.requester, change).exists())

    def test_nothing_is_sent_until_the_transaction_commits(self):
        change = self.staged_change()

        transition(change, State.STAGED, self.actor)  # callbacks not run yet

        self.assertFalse(Notification.objects.exists())

    def test_a_rejection_tells_the_requester(self):
        change = self.change(State.STAGED, branch=self.branch)

        with self.captureOnCommitCallbacks(execute=True):
            record_review(change, self.reviewer, approved=False, comment="no")

        self.assertEqual(
            self.notified(self.requester, change).get().event_type,
            events.CHANGES_REQUESTED,
        )

    def test_an_approval_alone_notifies_nobody(self):
        change = self.change(State.STAGED, branch=self.branch)

        with self.captureOnCommitCallbacks(execute=True):
            record_review(change, self.reviewer, approved=True)

        self.assertFalse(self.notified(self.requester, change).exists())

    def test_a_comment_tells_the_requester_and_earlier_commenters(self):
        change = self.change(State.STAGED)
        with self.captureOnCommitCallbacks(execute=True):
            add_comment(change, self.reviewer, "first")

        with self.captureOnCommitCallbacks(execute=True):
            add_comment(change, self.actor, "second")

        self.assertTrue(self.notified(self.requester, change).exists())
        self.assertTrue(self.notified(self.reviewer, change).exists())
        self.assertFalse(self.notified(self.actor, change).exists())

    def test_a_user_who_cannot_view_the_change_is_not_told_about_it(self):
        change = self.change(State.STAGED, branch=self.branch, title="Private")
        stranger = User.objects.create(username="stranger")
        ForwardChangeApprovalRule.objects.get().reviewers.add(stranger)

        with self.captureOnCommitCallbacks(execute=True):
            transition_outcome = record_review(change, self.reviewer, approved=False)

        self.assertTrue(transition_outcome.ok)
        self.assertFalse(Notification.objects.filter(user=stranger).exists())

    def test_an_inactive_user_is_not_notified(self):
        self.requester.is_active = False
        self.requester.save()
        change = self.change(State.STAGED, branch=self.branch)

        with self.captureOnCommitCallbacks(execute=True):
            record_review(change, self.reviewer, approved=False)

        self.assertFalse(self.notified(self.requester, change).exists())

    def test_a_database_error_while_notifying_never_undoes_the_transition(self):
        from django.db import DatabaseError

        change = self.staged_change()

        with mock.patch(
            "forward_netbox.change_control.events._notify",
            side_effect=DatabaseError("statement timeout"),
        ):
            with self.captureOnCommitCallbacks(execute=True):
                outcome = transition(change, State.STAGED, self.actor)

        self.assertTrue(outcome.ok)
        change.refresh_from_db()
        self.assertEqual(change.state, State.STAGED)

    def test_the_person_who_started_a_validation_run_hears_it_finished(self):
        from forward_netbox.change_control.choices import (
            ForwardValidationEngineChoices as Engine,
        )
        from forward_netbox.change_control.validation import execute_run
        from forward_netbox.change_control.validation import start_run
        from forward_netbox.models import ForwardChangeValidationPolicy
        from forward_netbox.models import ForwardChangeValidationRule

        change = self.change(
            State.SCOPED, workspace_network_id="ws", before_snapshot_id="s1"
        )
        ForwardChangeDevice.objects.create(
            change=change, device=self.device(), forward_device_key="sw"
        )
        policy = ForwardChangeValidationPolicy.objects.create(name="v")
        ForwardChangeValidationRule.objects.create(
            policy=policy, name="r", engine=Engine.PREDICT
        )
        run, _ = start_run(change, self.reviewer)

        with self.captureOnCommitCallbacks(execute=True):
            execute_run(run, api=object())

        self.assertEqual(
            self.notified(self.reviewer, change).get().event_type,
            events.VALIDATION_COMPLETED,
        )


class EventRuleTest(ChangeFixture):
    def make_rule(self, event_type):
        webhook = Webhook.objects.create(
            name=f"hook-{event_type}", payload_url="https://example.invalid/"
        )
        rule = EventRule.objects.create(
            name=f"rule-{event_type}",
            event_types=[event_type],
            action_type="webhook",
            action_object_type=ContentType.objects.get_for_model(Webhook),
            action_object_id=webhook.pk,
        )
        from core.models import ObjectType

        rule.object_types.add(ObjectType.objects.get_for_model(ForwardChange))
        return rule

    def test_an_event_rule_receives_the_payload_after_commit(self):
        self.make_rule(events.CHANGES_REQUESTED)
        change = self.change(State.STAGED, branch=self.branch, title="Move it")

        with mock.patch("extras.events.process_event_rules") as process:
            with self.captureOnCommitCallbacks(execute=True):
                record_review(change, self.reviewer, approved=False)

        process.assert_called_once()
        call = process.call_args.kwargs
        self.assertEqual(call["event"]["event_type"], events.CHANGES_REQUESTED)
        self.assertEqual(call["event"]["data"]["id"], change.pk)
        self.assertEqual(call["event"]["data"]["title"], "Move it")
        self.assertEqual(call["event"]["username"], self.reviewer.username)

    def test_a_rule_for_a_different_event_type_is_not_fired(self):
        self.make_rule(events.APPROVED)  # not the event that happens
        change = self.change(State.STAGED, branch=self.branch)

        with mock.patch("extras.events.process_event_rules") as process:
            with self.captureOnCommitCallbacks(execute=True):
                record_review(change, self.reviewer, approved=False)

        process.assert_not_called()

    def test_a_disabled_rule_is_not_fired(self):
        rule = self.make_rule(events.CHANGES_REQUESTED)
        rule.enabled = False
        rule.save()
        change = self.change(State.STAGED, branch=self.branch)

        with mock.patch("extras.events.process_event_rules") as process:
            with self.captureOnCommitCallbacks(execute=True):
                record_review(change, self.reviewer, approved=False)

        process.assert_not_called()

    def test_a_rule_for_another_object_type_is_not_fired(self):
        rule = self.make_rule(events.CHANGES_REQUESTED)
        rule.object_types.clear()
        from core.models import ObjectType

        rule.object_types.add(ObjectType.objects.get_for_model(Webhook))
        change = self.change(State.STAGED, branch=self.branch)

        with mock.patch("extras.events.process_event_rules") as process:
            with self.captureOnCommitCallbacks(execute=True):
                record_review(change, self.reviewer, approved=False)

        process.assert_not_called()

    def test_a_broken_event_rule_does_not_break_the_review(self):
        self.make_rule(events.CHANGES_REQUESTED)
        change = self.change(State.STAGED, branch=self.branch)

        with mock.patch(
            "extras.events.process_event_rules",
            side_effect=RuntimeError("webhook down"),
        ):
            with self.captureOnCommitCallbacks(execute=True):
                outcome = record_review(change, self.reviewer, approved=False)

        self.assertTrue(outcome.ok)

    def test_netbox_really_runs_a_notification_rule_on_our_event(self):
        # Not mocked: proves the call into NetBox's event machinery is the right
        # shape, using a rule whose action is NetBox's own notification group.
        from extras.models import NotificationGroup

        group = NotificationGroup.objects.create(name="watchers")
        watcher = User.objects.create(username="watcher")
        group.users.add(watcher)
        rule = EventRule.objects.create(
            name="notify-watchers",
            event_types=[events.CHANGES_REQUESTED],
            action_type="notification",
            action_object_type=ContentType.objects.get_for_model(NotificationGroup),
            action_object_id=group.pk,
        )
        from core.models import ObjectType

        rule.object_types.add(ObjectType.objects.get_for_model(ForwardChange))
        change = self.change(State.STAGED, branch=self.branch, title="Real path")

        with self.captureOnCommitCallbacks(execute=True):
            record_review(change, self.reviewer, approved=False)

        got = Notification.objects.filter(user=watcher)
        self.assertEqual(got.count(), 1)
        self.assertEqual(got.get().event_type, events.CHANGES_REQUESTED)


class BranchPanelTest(ChangeFixture):
    def render(self, branch, user=None, view=True):
        request = RequestFactory().get("/")
        request.user = user or self.reviewer
        if view:
            allow_viewing(request.user)
        panel = ForwardChangeBranchPanel(
            {"object": branch, "request": request, "perms": mock.MagicMock()}
        )
        return panel.right_page()

    def test_a_branch_with_a_change_shows_its_state_and_why_it_cannot_merge(self):
        self.change(State.STAGED, branch=self.branch, title="Backed by this branch")

        html = self.render(self.branch)

        self.assertIn("Backed by this branch", html)
        self.assertIn("not ready for its branch to merge", html)

    def test_a_branch_with_no_change_shows_nothing(self):
        self.assertEqual(self.render(self.branch), "")

    def test_a_user_who_may_not_view_changes_sees_nothing(self):
        self.change(State.STAGED, branch=self.branch, title="Hidden")

        self.assertEqual(self.render(self.branch, view=False), "")

    def test_a_gate_that_blows_up_does_not_break_the_branch_page(self):
        self.change(State.STAGED, branch=self.branch, title="Backed")

        with mock.patch(
            "forward_netbox.change_control.branch_gate.merge_gate",
            side_effect=RuntimeError("boom"),
        ):
            html = self.render(self.branch)

        self.assertIn("could not be read", html)


class CapabilitiesTest(ChangeFixture):
    class Denied(Exception):
        def __init__(self, kind, detail):
            super().__init__("denied")
            self.denial = type("D", (), {"kind": kind, "detail": detail})()

    class Api:
        def __init__(self, result=None, error=None):
            self.result, self.error = result, error

        def capabilities(self, network_id):
            if self.error:
                raise self.error
            return self.result

    def setUp(self):
        super().setUp()
        from django.core.cache import cache

        cache.clear()

    def test_a_refusal_is_reported_with_its_reason_not_guessed(self):
        from forward_netbox.change_control.capabilities import _probe

        def refuse():
            error = RuntimeError("wrapped")
            error.__cause__ = self.Denied(
                "license", "NetworkOperation.MANAGE_CHANGE_SETS"
            )
            raise error

        self.assertEqual(
            _probe(refuse),
            {
                "available": False,
                "reason": "license: NetworkOperation.MANAGE_CHANGE_SETS",
            },
        )

    def test_an_unrecognised_failure_is_unknown_not_unavailable(self):
        from forward_netbox.change_control.capabilities import _probe

        result = _probe(lambda: (_ for _ in ()).throw(RuntimeError("timeout")))

        self.assertIsNone(result["available"])

    def test_success_is_available(self):
        from forward_netbox.change_control.capabilities import _probe

        self.assertEqual(_probe(lambda: None), {"available": True, "reason": ""})

    def test_the_cache_is_per_network(self):
        from forward_netbox.change_control.capabilities import _cache_key

        before = _cache_key(self.source)
        self.source.parameters = {**self.source.parameters, "network_id": "another"}

        self.assertNotEqual(before, _cache_key(self.source))

    def test_the_result_is_cached_and_a_failure_to_probe_is_unknown(self):
        from forward_netbox.change_control.capabilities import cached_capabilities
        from forward_netbox.change_control.capabilities import probe_capabilities

        found = probe_capabilities(
            self.source, api=self.Api(error=RuntimeError("down"))
        )

        self.assertIsNone(found["predict"]["available"])
        self.assertEqual(cached_capabilities(self.source), found)

    def test_predict_says_why_when_forward_refused_it(self):
        from forward_netbox.change_control.capabilities import probe_capabilities
        from forward_netbox.change_control.predict import PredictOutcome
        from forward_netbox.change_control.predict import predict_change

        probe_capabilities(
            self.source,
            api=self.Api(
                {
                    "predict": {
                        "available": False,
                        "reason": "license: MANAGE_CHANGE_SETS",
                    },
                    "ai": {"available": False, "reason": "org_setting: AI_ALLOWED"},
                }
            ),
        )

        outcome = predict_change(source=self.source, snapshot_id="s")

        self.assertEqual(outcome.status, PredictOutcome.UNAVAILABLE)
        self.assertIn("MANAGE_CHANGE_SETS", outcome.reason)

    def test_predict_available_is_unsupported_until_a_proposal_can_be_authored(self):
        from forward_netbox.change_control.capabilities import probe_capabilities
        from forward_netbox.change_control.predict import PredictOutcome
        from forward_netbox.change_control.predict import predict_change

        probe_capabilities(
            self.source,
            api=self.Api(
                {
                    "predict": {"available": True, "reason": ""},
                    "ai": {"available": True, "reason": ""},
                }
            ),
        )

        outcome = predict_change(source=self.source, snapshot_id="s")

        self.assertEqual(outcome.status, PredictOutcome.UNSUPPORTED)
        self.assertFalse(outcome.is_answer)
