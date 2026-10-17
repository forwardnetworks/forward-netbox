from datetime import timedelta

from django.utils import timezone

from forward_netbox.change_control.choices import ForwardChangeStateChoices as State
from forward_netbox.change_control.choices import ForwardChangeVerdictChoices as Verdict
from forward_netbox.change_control.choices import (
    ForwardCriterionExpectationChoices as Expect,
)
from forward_netbox.change_control.choices import ForwardEvidencePhaseChoices as Phase
from forward_netbox.change_control.runner import record_evidence
from forward_netbox.change_control.runner import verify_change
from forward_netbox.models import ForwardChangeCriterion
from forward_netbox.models import ForwardChangeDevice
from forward_netbox.models import ForwardChangeEvidence
from forward_netbox.tests.test_change_control_actions import ChangeFixture


class FakeQueryApi:
    """Answers queries and device collection from a script keyed by snapshot."""

    def __init__(self, rows=None, collected=None):
        self.rows = rows or {}  # (query_id, snapshot_id) -> rows
        self.collected = collected or {}  # snapshot_id -> {device: datetime}
        self.queries = []

    def run_query(self, query_id, commit_id, network_id, snapshot_id):
        self.queries.append((query_id, commit_id, network_id, snapshot_id))
        return list(self.rows.get((query_id, snapshot_id), []))

    def device_collection(self, network_id, snapshot_id):
        return dict(self.collected.get(snapshot_id, {}))


class Scoped(ChangeFixture):
    def setUp(self):
        super().setUp()
        self.applied = timezone.now() - timedelta(hours=1)
        self.change_ = self.change(
            State.APPLIED,
            branch=None,
            before_snapshot_id="base",
            after_snapshot_id="after",
            applied_at=self.applied,
            workspace_network_id="ws-1",
        )
        ForwardChangeDevice.objects.create(
            change=self.change_, device=self.device(), forward_device_key="sw-a"
        )

    def criterion(self, name="no loops", expectation=Expect.NO_ROWS, bound=True, **kw):
        values = {
            "change": self.change_,
            "name": name,
            "expectation": expectation,
            "query_path": f"/q/{name}",
        }
        if bound:
            values.update(query_id=f"q-{name}", commit_id="c1")
        values.update(kw)
        return ForwardChangeCriterion.objects.create(**values)

    def fresh(self):
        return {"after": {"sw-a": self.applied + timedelta(minutes=30)}}


class RecordingEvidenceTest(Scoped):
    def test_a_bound_criterion_is_run_in_the_workspace_at_the_pinned_snapshot(self):
        criterion = self.criterion()
        api = FakeQueryApi()

        outcome = record_evidence(self.change_, Phase.BEFORE, api=api)

        self.assertTrue(outcome.ok, outcome.message)
        self.assertEqual(api.queries, [("q-no loops", "c1", "ws-1", "base")])
        row = ForwardChangeEvidence.objects.get(criterion=criterion)
        self.assertEqual(
            (row.phase, row.snapshot_id, row.passed), ("before", "base", True)
        )

    def test_evidence_keeps_the_shape_but_never_the_rows(self):
        criterion = self.criterion()
        api = FakeQueryApi(rows={("q-no loops", "base"): [{"device": "secret-name"}]})

        record_evidence(self.change_, Phase.BEFORE, api=api)

        row = ForwardChangeEvidence.objects.get(criterion=criterion)
        self.assertFalse(row.passed)
        self.assertEqual(row.row_count, 1)
        self.assertEqual(row.shape["columns"], ["device"])
        self.assertNotIn("secret-name", str(row.shape))

    def test_an_unbound_criterion_is_skipped_and_named(self):
        self.criterion(name="bound")
        self.criterion(name="loose", bound=False)

        outcome = record_evidence(self.change_, Phase.BEFORE, api=FakeQueryApi())

        self.assertTrue(outcome.ok)
        self.assertEqual(ForwardChangeEvidence.objects.count(), 1)
        self.assertIn("loose", outcome.message)

    def test_a_change_with_no_workspace_never_runs_against_the_whole_network(self):
        self.criterion()
        self.change_.workspace_network_id = ""
        self.change_.save()
        api = FakeQueryApi()

        outcome = record_evidence(self.change_, Phase.BEFORE, api=api)

        self.assertFalse(outcome.ok)
        self.assertEqual(api.queries, [])

    def test_a_no_diff_criterion_with_no_baseline_is_not_measured_not_passed(self):
        self.criterion(name="stable", expectation=Expect.NO_DIFF)
        self.change_.before_snapshot_id = ""
        self.change_.save()

        outcome = record_evidence(self.change_, Phase.AFTER, api=FakeQueryApi())

        self.assertTrue(outcome.ok)
        self.assertFalse(ForwardChangeEvidence.objects.exists())
        self.assertIn("stable", outcome.message)

    def test_evidence_from_another_snapshot_or_query_version_does_not_count(self):
        from forward_netbox.change_control.evidence import criterion_outcomes

        criterion = self.criterion()
        ForwardChangeEvidence.objects.create(
            criterion=criterion,
            phase=Phase.BEFORE,
            passed=True,
            snapshot_id="some-older-snapshot",
            commit_id="c1",
        )
        ForwardChangeEvidence.objects.create(
            criterion=criterion,
            phase=Phase.AFTER,
            passed=True,
            snapshot_id="after",
            commit_id="a-different-version",
        )

        (outcome,) = criterion_outcomes(self.change_)

        self.assertIsNone(outcome.before_passed)
        self.assertIsNone(outcome.after_passed)

    def test_a_phase_with_no_pinned_snapshot_records_nothing(self):
        self.criterion()
        self.change_.before_snapshot_id = ""
        self.change_.save()

        outcome = record_evidence(self.change_, Phase.BEFORE, api=FakeQueryApi())

        self.assertFalse(outcome.ok)
        self.assertFalse(ForwardChangeEvidence.objects.exists())

    def test_running_it_again_replaces_rather_than_duplicates(self):
        self.criterion()
        record_evidence(self.change_, Phase.BEFORE, api=FakeQueryApi())
        record_evidence(self.change_, Phase.BEFORE, api=FakeQueryApi())

        self.assertEqual(ForwardChangeEvidence.objects.count(), 1)

    def test_a_no_diff_criterion_after_compares_against_the_baselines_rows(self):
        criterion = self.criterion(name="stable", expectation=Expect.NO_DIFF)
        same = [{"d": "x"}]
        api = FakeQueryApi(
            rows={("q-stable", "base"): same, ("q-stable", "after"): same}
        )

        record_evidence(self.change_, Phase.AFTER, api=api)

        self.assertTrue(ForwardChangeEvidence.objects.get(criterion=criterion).passed)
        self.assertEqual({q[3] for q in api.queries}, {"base", "after"})

    def test_a_query_that_fails_is_reported_and_writes_no_evidence(self):
        self.criterion()

        class Boom(FakeQueryApi):
            def run_query(self, *a, **k):
                raise RuntimeError("nqe down")

        outcome = record_evidence(self.change_, Phase.BEFORE, api=Boom())

        self.assertFalse(outcome.ok)
        self.assertIn("nqe down", outcome.message)
        self.assertFalse(ForwardChangeEvidence.objects.exists())


class VerifyingTest(Scoped):
    def _api(self, before_rows=None, after_rows=None):
        rows = {
            ("q-no loops", "base"): before_rows or [],
            ("q-no loops", "after"): after_rows or [],
        }
        return FakeQueryApi(rows=rows, collected=self.fresh())

    def test_a_change_that_fixes_something_verifies_proceed(self):
        self.criterion()
        before = self._api(before_rows=[{"loop": 1}])
        record_evidence(self.change_, Phase.BEFORE, api=before)

        outcome = verify_change(self.change_, self.requester, api=self._api())

        self.assertTrue(outcome.ok, outcome.reasons)
        self.change_.refresh_from_db()
        self.assertEqual(self.change_.state, State.VERIFIED_PROCEED)
        self.assertEqual(self.change_.verdict, Verdict.PROCEED)

    def test_a_change_after_which_nothing_moved_is_held_with_a_question(self):
        # Identical before and after means Forward saw no change: either it has
        # not landed yet or it changed nothing the criteria ask about.
        self.criterion()
        record_evidence(self.change_, Phase.BEFORE, api=self._api())

        outcome = verify_change(self.change_, self.requester, api=self._api())

        self.change_.refresh_from_db()
        self.assertEqual(self.change_.state, State.VERIFIED_HOLD)
        self.assertIn("no change", " ".join(outcome.reasons))

    def test_a_regression_holds_the_change_with_its_reasons(self):
        self.criterion()
        record_evidence(self.change_, Phase.BEFORE, api=self._api())

        outcome = verify_change(
            self.change_, self.requester, api=self._api(after_rows=[{"loop": 1}])
        )

        self.change_.refresh_from_db()
        self.assertEqual(self.change_.state, State.VERIFIED_HOLD)
        self.assertEqual(self.change_.verdict, Verdict.HOLD)
        self.assertTrue(outcome.reasons)

    def test_a_device_collected_before_the_apply_holds_the_change(self):
        self.criterion()
        record_evidence(self.change_, Phase.BEFORE, api=self._api())
        api = self._api()
        api.collected = {"after": {"sw-a": self.applied - timedelta(hours=3)}}

        verify_change(self.change_, self.requester, api=api)

        self.change_.refresh_from_db()
        self.assertEqual(self.change_.state, State.VERIFIED_HOLD)

    def test_a_device_missing_from_the_after_snapshot_holds_the_change(self):
        self.criterion()
        record_evidence(self.change_, Phase.BEFORE, api=self._api())
        api = self._api()
        api.collected = {"after": {}}

        verify_change(self.change_, self.requester, api=api)

        self.change_.refresh_from_db()
        self.assertEqual(self.change_.state, State.VERIFIED_HOLD)

    def test_a_held_device_set_writes_no_after_evidence(self):
        # A criterion evaluated over an incomplete snapshot reads as a
        # measurement; the gate short-circuits before any row is written.
        self.criterion()
        record_evidence(self.change_, Phase.BEFORE, api=self._api())
        api = self._api()
        api.collected = {"after": {}}

        verify_change(self.change_, self.requester, api=api)

        self.assertFalse(
            ForwardChangeEvidence.objects.filter(phase=Phase.AFTER).exists()
        )

    def test_a_verdict_computed_on_a_snapshot_that_moved_is_not_saved(self):
        self.criterion()
        record_evidence(
            self.change_, Phase.BEFORE, api=self._api(before_rows=[{"x": 1}])
        )
        change = self.change_
        api = self._api()
        real = api.device_collection

        def moving(network_id, snapshot_id):
            # Another verify re-pinned the snapshot while this one was reading.
            type(change).objects.filter(pk=change.pk).update(after_snapshot_id="newer")
            return real(network_id, snapshot_id)

        api.device_collection = moving

        outcome = verify_change(change, self.requester, api=api)

        self.assertFalse(outcome.ok)
        change.refresh_from_db()
        self.assertEqual(change.verdict, "")
        self.assertEqual(change.state, State.COLLECTED)

    def test_a_change_with_no_workspace_is_not_verified(self):
        self.criterion()
        self.change_.workspace_network_id = ""
        self.change_.save()

        outcome = verify_change(self.change_, self.requester, api=self._api())

        self.assertFalse(outcome.ok)
        self.assertIn("workspace", outcome.message)

    def test_a_change_not_yet_applied_is_not_verified(self):
        self.criterion()
        pending = self.change(State.STAGED, branch=None, workspace_network_id="ws-1")

        outcome = verify_change(pending, self.requester, api=self._api())

        self.assertFalse(outcome.ok)
        pending.refresh_from_db()
        self.assertEqual(pending.state, State.STAGED)
