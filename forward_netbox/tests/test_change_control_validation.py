from unittest import mock

from forward_netbox.change_control.choices import ForwardChangeStateChoices as State
from forward_netbox.change_control.choices import (
    ForwardValidationEngineChoices as Engine,
)
from forward_netbox.change_control.choices import (
    ForwardValidationResultStatusChoices as Status,
)
from forward_netbox.change_control.choices import (
    ForwardValidationRunStatusChoices as RunStatus,
)
from forward_netbox.change_control.choices import (
    ForwardValidationSeverityChoices as Severity,
)
from forward_netbox.change_control.queueing import enqueue_validation
from forward_netbox.change_control.state_machine import plan_approval_check
from forward_netbox.change_control.validation import execute_run
from forward_netbox.change_control.validation import policies_for
from forward_netbox.change_control.validation import run_validation
from forward_netbox.change_control.validation import score_results
from forward_netbox.change_control.validation import start_run
from forward_netbox.change_control.validation import validation_blockers
from forward_netbox.models import ForwardChangeDevice
from forward_netbox.models import ForwardChangeValidationPolicy
from forward_netbox.models import ForwardChangeValidationRule
from forward_netbox.tests.test_change_control_actions import ChangeFixture


class FakeApi:
    def __init__(self, rows=None, checks=None, resolved=("q1", "c1"), error=None):
        self.rows = rows if rows is not None else []
        self._checks = checks or []
        self.resolved = resolved
        self.error = error
        self.queries = []
        self.check_calls = []

    def resolve_query(self, path, commit_id=None):
        if self.error:
            raise self.error
        return self.resolved

    def run_query(self, query_id, commit_id, network_id, snapshot_id):
        self.queries.append((query_id, commit_id, network_id, snapshot_id))
        return self.rows

    def checks(self, snapshot_id):
        self.check_calls.append(snapshot_id)
        return self._checks


def check(name, status, type_="Isolation", violations=0):
    return {
        "name": name,
        "type": type_,
        "status": status,
        "priority": "HIGH",
        "violations": violations,
    }


class ValidationFixture(ChangeFixture):
    def setUp(self):
        super().setUp()
        self.change_ = self.change(
            State.SCOPED,
            workspace_network_id="ws-1",
            before_snapshot_id="snap-before",
        )
        self.device_ = self.device()
        ForwardChangeDevice.objects.create(
            change=self.change_, device=self.device_, forward_device_key="sw"
        )
        self.policy = ForwardChangeValidationPolicy.objects.create(name="p")

    def rule(self, engine, name="r", severity=Severity.ERROR, **parameters):
        return ForwardChangeValidationRule.objects.create(
            policy=self.policy,
            name=name,
            engine=engine,
            severity=severity,
            parameters=parameters,
        )


class PolicyScopeTest(ValidationFixture):
    def test_a_policy_with_no_scope_applies_to_every_change(self):
        self.assertEqual(policies_for(self.change_), [self.policy])

    def test_a_disabled_policy_does_not_apply(self):
        self.policy.enabled = False
        self.policy.save()

        self.assertEqual(policies_for(self.change_), [])

    def test_a_scoped_policy_applies_only_when_a_device_matches(self):
        self.policy.sites.add(self.device_.site)
        self.assertEqual(policies_for(self.change_), [self.policy])

        other = self.device().site
        self.policy.sites.clear()
        self.policy.sites.add(other)
        self.assertEqual(policies_for(self.change_), [])

    def test_every_named_kind_must_match(self):
        self.policy.sites.add(self.device_.site)
        self.policy.device_roles.add(self.device().role)  # a different role

        self.assertEqual(policies_for(self.change_), [])


class StartRunTest(ValidationFixture):
    def test_a_change_without_a_workspace_is_refused(self):
        self.rule(Engine.PREDICT)
        self.change_.workspace_network_id = ""
        self.change_.save()

        run, reason = start_run(self.change_, self.requester)

        self.assertIsNone(run)
        self.assertIn("workspace", reason)

    def test_a_change_without_a_snapshot_is_refused(self):
        self.rule(Engine.PREDICT)
        self.change_.before_snapshot_id = ""
        self.change_.save()

        self.assertIsNone(start_run(self.change_, self.requester)[0])

    def test_no_rules_is_refused(self):
        run, reason = start_run(self.change_, self.requester)

        self.assertIsNone(run)
        self.assertIn("rules", reason)

    def test_a_second_run_is_refused_while_one_is_live(self):
        self.rule(Engine.PREDICT)
        first, _ = start_run(self.change_, self.requester)

        second, reason = start_run(self.change_, self.requester)

        self.assertIsNotNone(first)
        self.assertIsNone(second)
        self.assertIn("in progress", reason)

    def test_a_run_pins_the_snapshots_the_change_has_now(self):
        self.rule(Engine.PREDICT)

        run, _ = start_run(self.change_, self.requester)

        self.assertEqual(run.snapshot_id, "snap-before")
        self.assertEqual(run.before_snapshot_id, "snap-before")

    def test_after_collection_the_after_snapshot_is_validated(self):
        self.rule(Engine.PREDICT)
        self.change_.state = State.COLLECTED
        self.change_.after_snapshot_id = "snap-after"
        self.change_.save()

        run, _ = start_run(self.change_, self.requester)

        self.assertEqual(run.snapshot_id, "snap-after")


class NqeEngineTest(ValidationFixture):
    def run_rule(self, api, **params):
        self.rule(Engine.NQE, query_path="/q", **params)
        outcome = run_validation(self.change_, self.requester, api=api)
        from forward_netbox.models import ForwardChangeValidationRun

        return outcome, ForwardChangeValidationRun.objects.get(pk=outcome.run_id)

    def test_no_rows_passes_a_no_rows_expectation(self):
        api = FakeApi(rows=[])

        outcome, run = self.run_rule(api)

        self.assertTrue(outcome.ok)
        self.assertEqual(run.results.get().status, Status.PASS)
        self.assertEqual(run.score, 100)
        self.assertEqual(api.queries, [("q1", "c1", "ws-1", "snap-before")])

    def test_rows_fail_per_device_with_counts_only(self):
        api = FakeApi(
            rows=[
                {"device": "sw-a", "secret": "x"},
                {"device": "sw-a", "secret": "y"},
                {"device": "sw-b"},
            ]
        )

        _outcome, run = self.run_rule(api)

        results = {r.device_name: r for r in run.results.all()}
        self.assertEqual(set(results), {"sw-a", "sw-b"})
        self.assertEqual(results["sw-a"].row_count, 2)
        self.assertTrue(all(r.status == Status.FAIL for r in results.values()))
        self.assertNotIn("secret", " ".join(r.message for r in results.values()))
        self.assertEqual(run.score, 100 - 2 * Severity.PENALTY[Severity.ERROR])

    def test_rows_without_a_device_column_fail_once(self):
        _outcome, run = self.run_rule(FakeApi(rows=[{"x": 1}, {"x": 2}]))

        result = run.results.get()
        self.assertEqual(
            (result.status, result.row_count, result.device_name), (Status.FAIL, 2, "")
        )

    def test_some_rows_expectation(self):
        _o, run = self.run_rule(FakeApi(rows=[]), expectation="some-rows")
        self.assertEqual(run.results.get().status, Status.FAIL)

    def test_an_unknown_expectation_is_an_error_not_a_pass(self):
        _o, run = self.run_rule(FakeApi(rows=[]), expectation="no-diff")
        self.assertEqual(run.results.get().status, Status.ERROR)

    def test_a_forward_failure_is_one_error_result_and_the_run_completes(self):
        _o, run = self.run_rule(FakeApi(error=RuntimeError("down")))

        self.assertEqual(run.status, RunStatus.COMPLETED)
        result = run.results.get()
        self.assertEqual(result.status, Status.ERROR)
        self.assertIn("down", result.message)
        self.assertLess(run.score, 100)

    def test_an_unresolvable_path_is_an_error(self):
        _o, run = self.run_rule(FakeApi(resolved=("", "")))
        self.assertEqual(run.results.get().status, Status.ERROR)


class CheckEngineTest(ValidationFixture):
    def run_checks(self, checks, **params):
        self.rule(Engine.CHECK, **params)
        outcome = run_validation(
            self.change_, self.requester, api=FakeApi(checks=checks)
        )
        from forward_netbox.models import ForwardChangeValidationRun

        return ForwardChangeValidationRun.objects.get(pk=outcome.run_id)

    def test_check_statuses_map_and_none_is_never_a_pass(self):
        run = self.run_checks(
            [
                check("a", "PASS"),
                check("b", "FAIL", violations=3),
                check("c", "ERROR"),
                check("d", "NONE"),
                check("e", "REQUIRES_ADDITIONAL_SNAPSHOT_PROCESSING"),
            ]
        )

        got = {r.check_name: r.status for r in run.results.all()}
        self.assertEqual(
            got,
            {
                "a": Status.PASS,
                "b": Status.FAIL,
                "c": Status.ERROR,
                "d": Status.SKIP,
                "e": Status.SKIP,
            },
        )
        self.assertEqual(run.results.get(check_name="b").row_count, 3)

    def test_names_and_types_narrow_the_checks(self):
        run = self.run_checks(
            [check("a", "PASS", "Isolation"), check("b", "FAIL", "NQE")],
            check_types=["isolation"],
        )

        self.assertEqual([r.check_name for r in run.results.all()], ["a"])

    def test_no_matching_check_is_a_skip_that_says_how_many_exist(self):
        run = self.run_checks([check("a", "PASS", "NQE")], check_types=["isolation"])

        result = run.results.get()
        self.assertEqual(result.status, Status.SKIP)
        self.assertIn("1 check", result.message)
        self.assertIsNone(run.score)  # nothing measured is no score, not 100

    def test_an_empty_workspace_is_a_skip_not_a_pass(self):
        run = self.run_checks([])

        self.assertEqual(run.results.get().status, Status.SKIP)
        self.assertIsNone(run.score)


class PredictAndScoreTest(ValidationFixture):
    def test_predict_reports_skip_and_the_run_has_no_score(self):
        self.rule(Engine.PREDICT)

        outcome = run_validation(self.change_, self.requester, api=FakeApi())

        from forward_netbox.models import ForwardChangeValidationRun

        run = ForwardChangeValidationRun.objects.get(pk=outcome.run_id)
        self.assertEqual(run.results.get().status, Status.SKIP)
        self.assertIsNone(run.score)

    def test_penalty_depends_on_the_rules_severity(self):
        class R:
            def __init__(self, status, severity):
                self.status, self.severity = status, severity

        results = [
            R(Status.FAIL, Severity.CRITICAL),
            R(Status.ERROR, Severity.WARNING),
            R(Status.PASS, Severity.CRITICAL),
            R(Status.SKIP, Severity.CRITICAL),
        ]

        self.assertEqual(score_results(results), 100 - 25 - 3)

    def test_the_score_never_goes_below_zero(self):
        class R:
            status, severity = Status.FAIL, Severity.CRITICAL

        self.assertEqual(score_results([R() for _ in range(10)]), 0)


class GateTest(ValidationFixture):
    def ask(self, **fields):
        for key, value in fields.items():
            setattr(self.policy, key, value)
        self.policy.save()

    def completed_run(self, passes=1, fails=0, errors=0, skips=0, policy=None, **pins):
        """A completed run whose results came from `policy`'s rules."""
        from forward_netbox.models import ForwardChangeValidationResult
        from forward_netbox.models import ForwardChangeValidationRun

        policy = policy or self.policy
        rule, _ = ForwardChangeValidationRule.objects.get_or_create(
            policy=policy,
            name="gate-rule",
            defaults={"engine": Engine.CHECK, "severity": Severity.CRITICAL},
        )
        values = {
            "change": self.change_,
            "status": RunStatus.COMPLETED,
            "snapshot_id": "snap-before",
            "before_snapshot_id": "snap-before",
            "after_snapshot_id": "",
        }
        values.update(pins)
        run = ForwardChangeValidationRun.objects.create(**values)
        for status, count in (
            (Status.PASS, passes),
            (Status.FAIL, fails),
            (Status.ERROR, errors),
            (Status.SKIP, skips),
        ):
            for n in range(count):
                ForwardChangeValidationResult.objects.create(
                    run=run,
                    rule=rule,
                    engine=Engine.CHECK,
                    check_name=f"{status}-{n}",
                    status=status,
                    severity=Severity.CRITICAL,
                )
        return run

    def test_a_policy_that_asks_for_nothing_adds_nothing(self):
        self.assertEqual(validation_blockers(self.change_), [])

    def test_a_required_run_must_exist(self):
        self.ask(require_passed_run=True)

        self.assertTrue(validation_blockers(self.change_))

    def test_a_current_run_with_enough_score_passes(self):
        self.ask(min_score=80)
        self.completed_run(passes=3)

        self.assertEqual(validation_blockers(self.change_), [])

    def test_a_low_score_blocks(self):
        self.ask(min_score=80)
        self.completed_run(passes=1, fails=1)  # 100 - 25 = 75

        self.assertIn("75", validation_blockers(self.change_)[0])

    def test_an_unanswered_rule_blocks_whatever_the_score(self):
        self.ask(require_passed_run=True)
        self.completed_run(passes=5, errors=1)

        self.assertIn("could not be answered", validation_blockers(self.change_)[0])

    def test_a_run_goes_stale_when_the_baseline_moves(self):
        self.ask(require_passed_run=True)
        self.completed_run()
        self.change_.before_snapshot_id = "snap-rebaselined"
        self.change_.save()

        self.assertIn("out of date", validation_blockers(self.change_)[0])

    def test_a_run_that_measured_nothing_cannot_pass(self):
        self.ask(require_passed_run=True)
        self.completed_run(passes=0, skips=2)

        self.assertIn("measured nothing", validation_blockers(self.change_)[0])

    def test_a_failed_run_does_not_count(self):
        self.ask(require_passed_run=True)
        self.completed_run(status=RunStatus.FAILED)

        self.assertTrue(validation_blockers(self.change_))

    def test_a_run_of_another_policy_does_not_stand_in_for_the_one_that_asks(self):
        strict = ForwardChangeValidationPolicy.objects.create(
            name="strict", min_score=90
        )
        lenient = ForwardChangeValidationPolicy.objects.create(name="lenient")
        self.completed_run(policy=lenient)

        blockers = validation_blockers(self.change_)

        self.assertEqual(len(blockers), 1)
        self.assertIn("strict", blockers[0])
        self.assertTrue(strict.pk)

    def test_a_run_covering_every_policy_is_scored_per_policy(self):
        strict = ForwardChangeValidationPolicy.objects.create(
            name="strict", min_score=90
        )
        run = self.completed_run(policy=strict, passes=0, fails=1)  # 75 for strict
        from forward_netbox.models import ForwardChangeValidationResult

        # The same run also ran an easy policy's rule and passed it.
        easy_rule = ForwardChangeValidationRule.objects.create(
            policy=self.policy, name="easy", engine=Engine.CHECK
        )
        ForwardChangeValidationResult.objects.create(
            run=run,
            rule=easy_rule,
            engine=Engine.CHECK,
            check_name="x",
            status=Status.PASS,
        )

        blockers = validation_blockers(self.change_)

        self.assertEqual(len(blockers), 1)
        self.assertIn("strict", blockers[0])
        self.assertIn("75", blockers[0])

    def test_the_plan_gate_includes_the_validation_blocker(self):
        self.ask(require_passed_run=True)

        result = plan_approval_check(self.change_)

        self.assertFalse(result.allowed)
        self.assertTrue(any("validation" in b.lower() for b in result.blockers))


class QueueingTest(ValidationFixture):
    def test_enqueue_creates_the_run_and_queues_the_job(self):
        self.rule(Engine.PREDICT)

        with mock.patch("forward_netbox.jobs.ChangeValidationJob.enqueue") as queued:
            outcome = enqueue_validation(self.change_, self.requester)

        self.assertTrue(outcome.ok)
        queued.assert_called_once()
        self.assertEqual(queued.call_args.kwargs["run_id"], outcome.run_id)

    def test_nothing_is_queued_when_the_run_cannot_start(self):
        with mock.patch("forward_netbox.jobs.ChangeValidationJob.enqueue") as queued:
            outcome = enqueue_validation(self.change_, self.requester)

        self.assertFalse(outcome.ok)
        queued.assert_not_called()

    def test_staging_queues_a_run_when_a_policy_asks_for_one(self):
        from forward_netbox.change_control.actions import transition

        self.rule(Engine.PREDICT)
        self.policy.run_on_submission = True
        self.policy.save()
        change = self.change(
            State.BASELINED,
            branch=self.branch,
            workspace_network_id="ws-2",
            before_snapshot_id="b",
        )
        ForwardChangeDevice.objects.create(
            change=change, device=self.device(), forward_device_key="x"
        )

        with mock.patch("forward_netbox.jobs.ChangeValidationJob.enqueue") as queued:
            outcome = transition(change, State.STAGED, self.requester)

        self.assertTrue(outcome.ok, outcome.blockers)
        queued.assert_called_once()

    def test_a_queueing_failure_does_not_undo_staging(self):
        from forward_netbox.change_control.actions import transition

        self.rule(Engine.PREDICT)
        self.policy.run_on_submission = True
        self.policy.save()
        change = self.change(
            State.BASELINED,
            branch=self.branch,
            workspace_network_id="ws-2",
            before_snapshot_id="b",
        )
        ForwardChangeDevice.objects.create(
            change=change, device=self.device(), forward_device_key="x"
        )

        with mock.patch(
            "forward_netbox.jobs.ChangeValidationJob.enqueue",
            side_effect=RuntimeError("redis down"),
        ):
            outcome = transition(change, State.STAGED, self.requester)

        self.assertTrue(outcome.ok)
        change.refresh_from_db()
        self.assertEqual(change.state, State.STAGED)


class ExecuteRunTest(ValidationFixture):
    def test_a_crash_outside_the_rules_fails_the_run_with_a_reason(self):
        self.rule(Engine.PREDICT)
        run, _ = start_run(self.change_, self.requester)

        with mock.patch(
            "forward_netbox.change_control.validation.policies_for",
            side_effect=RuntimeError("boom"),
        ):
            execute_run(run, api=FakeApi())

        run.refresh_from_db()
        self.assertEqual(run.status, RunStatus.FAILED)
        self.assertIn("boom", run.error)


class HardeningTest(ValidationFixture):
    def test_a_named_check_forward_does_not_have_is_an_error_not_silence(self):
        self.rule(Engine.CHECK, check_names=["present", "absent"])

        outcome = run_validation(
            self.change_, self.requester, api=FakeApi(checks=[check("present", "PASS")])
        )

        from forward_netbox.models import ForwardChangeValidationRun

        run = ForwardChangeValidationRun.objects.get(pk=outcome.run_id)
        got = {r.check_name: r.status for r in run.results.all()}
        self.assertEqual(got, {"present": Status.PASS, "absent": Status.ERROR})

    def test_only_a_policy_that_is_on_and_applies_may_be_run_by_itself(self):
        self.rule(Engine.PREDICT)
        other_site = self.device().site
        outsider = ForwardChangeValidationPolicy.objects.create(name="outsider")
        outsider.sites.add(other_site)
        ForwardChangeValidationRule.objects.create(
            policy=outsider, name="r", engine=Engine.PREDICT
        )

        run, reason = start_run(self.change_, self.requester, policy=outsider)

        self.assertIsNone(run)
        self.assertIn("does not apply", reason)

    def test_a_run_whose_job_never_ran_is_closed_as_failed(self):
        from datetime import timedelta

        from django.utils import timezone

        from forward_netbox.models import ForwardChangeValidationRun

        self.rule(Engine.PREDICT)
        stuck = ForwardChangeValidationRun.objects.create(change=self.change_)
        ForwardChangeValidationRun.objects.filter(pk=stuck.pk).update(
            created=timezone.now() - timedelta(hours=1)
        )

        run, reason = start_run(self.change_, self.requester)

        self.assertIsNotNone(run, reason)
        stuck.refresh_from_db()
        self.assertEqual(stuck.status, RunStatus.FAILED)

    def test_a_queue_that_is_down_fails_the_run_and_says_so(self):
        self.rule(Engine.PREDICT)

        with mock.patch(
            "forward_netbox.jobs.ChangeValidationJob.enqueue",
            side_effect=RuntimeError("redis down"),
        ):
            outcome = enqueue_validation(self.change_, self.requester)

        from forward_netbox.models import ForwardChangeValidationRun

        self.assertFalse(outcome.ok)
        self.assertEqual(
            ForwardChangeValidationRun.objects.get(pk=outcome.run_id).status,
            RunStatus.FAILED,
        )
        # ...and the change can be validated again straight away.
        with mock.patch("forward_netbox.jobs.ChangeValidationJob.enqueue"):
            self.assertTrue(enqueue_validation(self.change_, self.requester).ok)

    def test_a_job_delivered_twice_runs_once(self):
        self.rule(Engine.PREDICT)
        run, _ = start_run(self.change_, self.requester)

        execute_run(run, api=FakeApi())
        execute_run(run, api=FakeApi())

        self.assertEqual(run.results.count(), 1)
