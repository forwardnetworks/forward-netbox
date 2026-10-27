from datetime import timedelta
from types import SimpleNamespace
from unittest.mock import create_autospec
from unittest.mock import patch
from uuid import uuid4

from core.models import ObjectType
from dcim.models import Platform
from django.utils import timezone
from extras.models import JournalEntry
from forward_sdk._sync.services.change_sets import ChangeSetHandle
from forward_sdk._sync.services.change_sets import ChangeSetsService
from forward_sdk.errors import ForwardAPIError
from forward_sdk.errors import ForwardPermissionError
from forward_sdk.errors import ForwardServerError

from forward_netbox.change_control import predict
from forward_netbox.change_control.actions import transition
from forward_netbox.change_control.choices import ForwardChangeStateChoices as State
from forward_netbox.change_control.predict import discard_prediction
from forward_netbox.change_control.predict import prediction_is_stale
from forward_netbox.change_control.predict import PredictOutcome
from forward_netbox.change_control.predict import proposal_digest
from forward_netbox.change_control.predict import refresh_prediction
from forward_netbox.change_control.predict import save_proposal
from forward_netbox.change_control.predict import start_prediction
from forward_netbox.change_control.state_machine import check_transition
from forward_netbox.change_control.state_machine import closure_check
from forward_netbox.change_control.workspace import ForwardWorkspaceApi
from forward_netbox.models import ForwardChangeDevice
from forward_netbox.models import ForwardChangeProposal
from forward_netbox.tests.test_change_control_actions import ChangeFixture
from forward_netbox.utilities.forward_client_errors import translate_client_exception

SECRET = "snmp-server community hunter2-secret RO"


def _translated(sdk_error):
    """The error as the plugin raises it: translated, with the SDK's error as cause."""
    error = translate_client_exception(sdk_error)
    error.__cause__ = sdk_error
    return error


def _denied(text):
    """A real 403 as the SDK raises it, translated the way the client does."""
    return _translated(
        ForwardPermissionError("request failed with HTTP 403", status=403, text=text)
    )


class PredictApi:
    """Stands in for Forward's change sets: records calls, answers from a script."""

    def __init__(self, *, snapshots=None, command_errors=0, fail_at=None, error=None):
        self.calls = []
        self.deleted = []
        self.snapshot_rows = (
            [{"id": "base-1", "state": "PROCESSED"}] if snapshots is None else snapshots
        )
        self.command_errors = command_errors
        self.fail_at = fail_at
        self.error = error or RuntimeError("boom " + SECRET)
        self.delete_fails = False
        self.delete_error = None

    def _step(self, name):
        self.calls.append(name)
        if self.fail_at == name:
            raise self.error

    def create_change_set(self, network_id, *, name, snapshot_id):
        self._step("create")
        self.created_with = (network_id, name, snapshot_id)
        return "cs-1"

    def validate_commands(self, network_id, change_set_id, device_name, commands):
        self._step("validate")
        return self.command_errors

    def set_commands(self, network_id, change_set_id, device_name, commands):
        self._step("set")
        self.staged = getattr(self, "staged", {})
        self.staged[device_name] = commands

    def commit(self, network_id, change_set_id, note):
        self._step("commit")

    def predict(self, network_id, change_set_id, note):
        self._step("predict")
        return "snap-p"

    def delete_change_set(self, network_id, change_set_id):
        self.calls.append("delete")
        if self.delete_error is not None:
            raise self.delete_error
        if self.delete_fails:
            raise RuntimeError("cannot delete")
        self.deleted.append(change_set_id)

    def snapshots(self, network_id):
        return list(self.snapshot_rows)


class PredictFixture(ChangeFixture):
    def setUp(self):
        super().setUp()
        self.change_ = self.change(
            State.STAGED,
            branch=self.branch,
            workspace_network_id="ws-1",
            before_snapshot_id="base-1",
        )
        for name in ("sw-a", "sw-b"):
            ForwardChangeDevice.objects.create(
                change=self.change_, device=self.device(), forward_device_key=name
            )
        self.source.parameters = {**self.source.parameters, "enable_predict": True}
        self.source.save()
        patcher = patch.object(
            predict, "_open_api", side_effect=lambda change, api: (api, None)
        )
        patcher.start()
        self.addCleanup(patcher.stop)

    def propose(self, **bodies):
        outcome = save_proposal(
            self.change_, self.requester, bodies or {"sw-a": SECRET, "sw-b": "no shut"}
        )
        self.assertTrue(outcome.ok, outcome.message)
        self.change_.refresh_from_db()

    def journal_text(self):
        return " ".join(
            JournalEntry.objects.filter(assigned_object_id=self.change_.pk).values_list(
                "comments", flat=True
            )
        )


class ProposalTest(PredictFixture):
    def test_commands_are_stored_per_device_and_an_empty_text_clears_one(self):
        self.propose()
        self.assertEqual(
            predict.proposal_entries(self.change_),
            {"sw-a": SECRET, "sw-b": "no shut"},
        )

        save_proposal(self.change_, self.requester, {"sw-b": "  \n"})

        self.assertEqual(predict.proposal_entries(self.change_), {"sw-a": SECRET})

    def test_adding_commands_needs_the_source_to_have_opted_in_but_clearing_does_not(
        self,
    ):
        self.propose()
        self.source.parameters = {**self.source.parameters, "enable_predict": False}
        self.source.save()
        self.change_.refresh_from_db()

        added = save_proposal(self.change_, self.requester, {"sw-a": "x"})
        cleared = save_proposal(self.change_, self.requester, {"sw-a": ""})

        self.assertFalse(added.ok)
        self.assertIn("Predict is off", added.message)
        self.assertTrue(cleared.ok)

    def test_a_device_that_is_not_in_the_change_cannot_have_an_entry(self):
        outcome = save_proposal(self.change_, self.requester, {"sw-z": "x"})

        self.assertFalse(outcome.ok)
        self.assertIn("sw-z", outcome.message)
        self.assertEqual(ForwardChangeProposal.objects.count(), 0)

    def test_a_proposal_cannot_be_edited_once_the_change_is_approved(self):
        self.change_.state = State.APPROVED
        self.change_.save()

        outcome = save_proposal(self.change_, self.requester, {"sw-a": "x"})

        self.assertFalse(outcome.ok)
        self.assertIn("cannot be edited", outcome.message)

    def test_the_commands_never_reach_the_journal_or_the_change_log(self):
        from core.models import ObjectChange
        from django.test import RequestFactory
        from netbox.context_managers import event_tracking

        request = RequestFactory().get("/")
        request.user = self.requester
        request.id = uuid4()
        # Change logging only records inside event_tracking, so without it this
        # test would pass whatever the model did.
        with event_tracking(request):
            self.propose()
            self.change_.predict_status = PredictOutcome.PENDING
            self.change_.predict_digest = proposal_digest(self.change_)
            self.change_.save()

        recorded = ObjectChange.objects.filter(request_id=request.id)
        self.assertTrue(recorded.exists(), "the control: change logging did record")
        for row in recorded:
            self.assertNotIn("hunter2", str(row.postchange_data))
            self.assertNotIn("hunter2", str(row.prechange_data))
        self.assertFalse(
            recorded.filter(
                changed_object_type=ObjectType.objects.get_for_model(
                    ForwardChangeProposal
                )
            ).exists()
        )
        self.assertNotIn("hunter2", self.journal_text())
        self.assertIn("2 device(s) with commands", self.journal_text())

    def test_the_digest_changes_with_the_proposal_and_never_holds_the_text(self):
        self.propose()
        first = proposal_digest(self.change_)

        save_proposal(self.change_, self.requester, {"sw-b": "shut"})

        self.assertNotEqual(first, proposal_digest(self.change_))
        self.assertNotIn("hunter2", first)
        self.assertEqual(len(first), 64)


class StartPredictionTest(PredictFixture):
    def start(self, api, **kwargs):
        kwargs.setdefault("confirmed", True)
        outcome = start_prediction(self.change_, self.requester, api=api, **kwargs)
        self.change_.refresh_from_db()
        return outcome

    def test_it_needs_the_operators_confirmation(self):
        self.propose()
        api = PredictApi()

        outcome = self.start(api, confirmed=False)

        self.assertFalse(outcome.ok)
        self.assertEqual(api.calls, [])

    def test_it_creates_validates_stages_commits_then_predicts_in_that_order(self):
        self.propose()
        api = PredictApi()

        outcome = self.start(api)

        self.assertTrue(outcome.ok, outcome.message)
        self.assertEqual(
            api.calls,
            ["create", "validate", "set", "validate", "set", "commit", "predict"],
        )
        self.assertEqual(api.created_with[0], "ws-1")
        self.assertEqual(api.created_with[2], "base-1")
        self.assertEqual(api.staged, {"sw-a": SECRET, "sw-b": "no shut"})

    def test_it_records_the_ids_and_marks_the_prediction_pending(self):
        self.propose()

        self.start(PredictApi())

        self.assertEqual(self.change_.predict_change_set_id, "cs-1")
        self.assertEqual(self.change_.predict_snapshot_id, "snap-p")
        self.assertEqual(self.change_.predict_status, PredictOutcome.PENDING)
        self.assertEqual(self.change_.predict_digest, proposal_digest(self.change_))
        self.assertIsNotNone(self.change_.predict_started_at)
        self.assertNotIn("hunter2", self.journal_text())

    def test_it_is_refused_without_a_workspace_baseline_or_commands(self):
        api = PredictApi()
        self.assertIn("no commands", self.start(api).message)

        self.propose()
        self.change_.before_snapshot_id = ""
        self.change_.save()
        self.assertIn("baseline", self.start(api).message)
        self.assertEqual(api.calls, [])

    def test_it_is_refused_once_the_change_is_approved(self):
        self.propose()
        self.change_.state = State.APPROVED
        self.change_.save()

        self.assertFalse(self.start(PredictApi()).ok)

    def test_a_firewall_is_refused_before_anything_is_created(self):
        self.propose()
        pan = Platform.objects.create(name="PAN-OS", slug="pan-os")
        device = self.change_.devices.get(forward_device_key="sw-a").device
        device.platform = pan
        device.save()
        api = PredictApi()

        outcome = self.start(api)

        self.assertFalse(outcome.ok)
        self.assertIn("sw-a", outcome.message)
        self.assertEqual(api.calls, [])

    def test_command_errors_remove_the_change_set_and_stage_nothing(self):
        self.propose()
        api = PredictApi(command_errors=2)

        outcome = self.start(api)

        self.assertFalse(outcome.ok)
        self.assertEqual(api.deleted, ["cs-1"])
        self.assertNotIn("set", api.calls)
        self.assertEqual(self.change_.predict_change_set_id, "")
        self.assertEqual(self.change_.predict_status, PredictOutcome.FAILED)
        self.assertNotIn("hunter2", self.change_.predict_reason)

    def test_a_failure_after_create_deletes_the_set_it_made(self):
        self.propose()
        api = PredictApi(fail_at="predict")

        outcome = self.start(api)

        self.assertFalse(outcome.ok)
        self.assertEqual(api.deleted, ["cs-1"])
        self.assertEqual(self.change_.predict_change_set_id, "")
        self.assertEqual(self.change_.predict_status, PredictOutcome.FAILED)

    def test_a_failure_records_the_class_never_forwards_text(self):
        self.propose()
        api = PredictApi(fail_at="commit")

        self.start(api)

        self.assertIn("RuntimeError", self.change_.predict_reason)
        self.assertNotIn("hunter2", self.change_.predict_reason)

    def test_a_set_forward_will_not_delete_stays_recorded_so_it_can_be_retried(self):
        self.propose()
        api = PredictApi(fail_at="commit")
        api.delete_fails = True

        self.start(api)

        self.assertEqual(self.change_.predict_change_set_id, "cs-1")

    def test_a_refusal_says_which_kind_and_is_unavailable_not_failed(self):
        cases = {
            "Unlicensed operation: NetworkOperation.MANAGE_CHANGE_SETS": "licence",
            "Missing permission: NetworkOperation.MANAGE_CHANGE_SETS": "permission",
            "PREDICT_MODELING is off for your organization": "not enabled",
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                self.change_.predict_status = ""
                self.change_.save()
                self.propose()
                api = PredictApi(fail_at="create", error=_denied(text))

                outcome = self.start(api)

                self.assertFalse(outcome.ok)
                self.assertEqual(
                    self.change_.predict_status, PredictOutcome.UNAVAILABLE
                )
                self.assertIn(expected, self.change_.predict_reason)

    def test_a_403_with_no_stated_cause_is_refused_without_claiming_one(self):
        self.propose()
        api = PredictApi(fail_at="create", error=_denied("Forbidden"))

        self.start(api)

        self.assertEqual(self.change_.predict_status, PredictOutcome.UNAVAILABLE)
        self.assertIn("without saying why", self.change_.predict_reason)

    def test_a_server_error_is_a_failure_not_a_refusal(self):
        self.propose()
        error = _translated(
            ForwardServerError("HTTP 500", status=500, text="oops " + SECRET)
        )
        api = PredictApi(fail_at="create", error=error)

        self.start(api)

        self.assertEqual(self.change_.predict_status, PredictOutcome.FAILED)
        self.assertNotIn("hunter2", self.change_.predict_reason)

    def test_it_is_refused_server_side_when_the_source_has_not_opted_in(self):
        self.propose()
        self.source.parameters = {**self.source.parameters, "enable_predict": False}
        self.source.save()
        self.change_.refresh_from_db()
        api = PredictApi()

        outcome = self.start(api)

        self.assertFalse(outcome.ok)
        self.assertIn("off for this source", outcome.message)
        self.assertEqual(api.calls, [])

    def test_a_baseline_the_workspace_no_longer_lists_is_refused_and_leaves_no_claim(
        self,
    ):
        self.propose()
        api = PredictApi(snapshots=[{"id": "other", "state": "PROCESSED"}])

        outcome = self.start(api)

        self.assertFalse(outcome.ok)
        self.assertIn("baseline", outcome.message)
        self.assertNotIn("create", api.calls)
        self.assertNotEqual(self.change_.predict_status, PredictOutcome.PENDING)

    def test_a_baseline_still_processing_is_refused(self):
        self.propose()
        api = PredictApi(snapshots=[{"id": "base-1", "state": "PROCESSING"}])

        outcome = self.start(api)

        self.assertFalse(outcome.ok)
        self.assertNotIn("create", api.calls)

    def test_a_refused_start_restores_the_page_as_it_was(self):
        self.propose()
        self.change_.predict_change_set_id = "cs-old"
        self.change_.predict_status = PredictOutcome.ANSWERED
        self.change_.predict_reason = "earlier"
        self.change_.save()
        api = PredictApi()
        api.delete_fails = True

        self.start(api)

        self.assertEqual(self.change_.predict_status, PredictOutcome.ANSWERED)
        self.assertEqual(self.change_.predict_reason, "earlier")

    def test_a_change_set_forward_says_does_not_exist_is_gone_not_stuck(self):
        self.propose()
        self.change_.predict_change_set_id = "cs-old"
        self.change_.save()
        gone = _translated(ForwardAPIError("not found", status=404, text="no"))
        api = PredictApi()
        api.delete_error = gone

        outcome = self.start(api)

        self.assertTrue(outcome.ok, outcome.message)
        self.assertEqual(self.change_.predict_change_set_id, "cs-1")

    def test_a_second_request_while_one_is_pending_is_refused(self):
        self.propose()
        self.start(PredictApi())
        api = PredictApi()

        outcome = self.start(api)

        self.assertFalse(outcome.ok)
        self.assertIn("already in progress", outcome.message)
        self.assertEqual(api.calls, [])

    def test_a_claim_that_never_reached_forward_stops_blocking(self):
        self.propose()
        self.change_.predict_status = PredictOutcome.PENDING
        self.change_.predict_started_at = timezone.now() - timedelta(hours=1)
        self.change_.save()

        self.assertTrue(self.start(PredictApi()).ok)

    def test_repeating_deletes_the_earlier_change_set_first(self):
        self.propose()
        self.change_.predict_change_set_id = "cs-old"
        self.change_.predict_status = PredictOutcome.ANSWERED
        self.change_.save()
        api = PredictApi()

        self.start(api)

        self.assertEqual(api.deleted, ["cs-old"])
        self.assertEqual(api.calls[:2], ["delete", "create"])

    def test_it_does_not_start_another_if_the_earlier_one_cannot_be_removed(self):
        self.propose()
        self.change_.predict_change_set_id = "cs-old"
        self.change_.save()
        api = PredictApi()
        api.delete_fails = True

        outcome = self.start(api)

        self.assertFalse(outcome.ok)
        self.assertNotIn("create", api.calls)
        self.assertEqual(self.change_.predict_change_set_id, "cs-old")


class RefreshPredictionTest(PredictFixture):
    def pending(self):
        self.propose()
        start_prediction(self.change_, self.requester, confirmed=True, api=PredictApi())
        self.change_.refresh_from_db()

    def refresh(self, rows):
        outcome = refresh_prediction(
            self.change_, self.requester, api=PredictApi(snapshots=rows)
        )
        self.change_.refresh_from_db()
        return outcome

    def test_a_processed_snapshot_is_an_answer_and_moves_no_state(self):
        self.pending()

        outcome = self.refresh([{"id": "snap-p", "state": "PROCESSED"}])

        self.assertTrue(outcome.ok)
        self.assertEqual(self.change_.predict_status, PredictOutcome.ANSWERED)
        # An answer never moves the change: the state is in the review anchor.
        self.assertEqual(self.change_.state, State.STAGED)
        self.assertIn("snap-p", self.change_.predict_reason)

    def test_a_failed_snapshot_is_a_failure_that_names_the_state(self):
        self.pending()

        outcome = self.refresh([{"id": "snap-p", "state": "FAILED"}])

        self.assertFalse(outcome.ok)
        self.assertEqual(self.change_.predict_status, PredictOutcome.FAILED)
        self.assertIn("FAILED", self.change_.predict_reason)
        self.assertEqual(self.change_.state, State.STAGED)

    def test_a_snapshot_still_processing_stays_pending_and_is_not_an_answer(self):
        self.pending()

        outcome = self.refresh([{"id": "snap-p", "state": "PROCESSING"}])

        self.assertTrue(outcome.ok)
        self.assertIn("Still processing", outcome.message)
        self.assertEqual(self.change_.predict_status, PredictOutcome.PENDING)

    def test_an_unlisted_snapshot_stays_pending_then_fails_after_a_long_wait(self):
        self.pending()

        outcome = self.refresh([])
        self.assertTrue(outcome.ok)
        self.assertEqual(self.change_.predict_status, PredictOutcome.PENDING)

        self.change_.predict_started_at = timezone.now() - timedelta(hours=3)
        self.change_.save()
        self.refresh([])
        self.assertEqual(self.change_.predict_status, PredictOutcome.FAILED)

    def test_nothing_pending_means_nothing_to_refresh(self):
        self.assertFalse(self.refresh([]).ok)

    def test_a_proposal_edited_after_the_prediction_reads_as_stale(self):
        self.pending()
        self.refresh([{"id": "snap-p", "state": "PROCESSED"}])
        self.assertFalse(prediction_is_stale(self.change_))

        save_proposal(self.change_, self.requester, {"sw-b": "shutdown"})

        self.assertTrue(prediction_is_stale(self.change_))


class StuckStartTest(PredictFixture):
    def test_a_start_that_never_finished_is_failed_once_its_claim_expires(self):
        self.propose()
        self.change_.predict_status = PredictOutcome.PENDING
        self.change_.predict_started_at = timezone.now() - timedelta(hours=1)
        self.change_.save()

        outcome = refresh_prediction(self.change_, self.requester, api=PredictApi())

        self.assertFalse(outcome.ok)
        self.change_.refresh_from_db()
        self.assertEqual(self.change_.predict_status, PredictOutcome.FAILED)

    def test_a_recent_unfinished_start_is_left_alone(self):
        self.propose()
        self.change_.predict_status = PredictOutcome.PENDING
        self.change_.predict_started_at = timezone.now()
        self.change_.save()

        outcome = refresh_prediction(self.change_, self.requester, api=PredictApi())

        self.assertTrue(outcome.ok)
        self.change_.refresh_from_db()
        self.assertEqual(self.change_.predict_status, PredictOutcome.PENDING)

    def test_an_entry_for_a_device_that_left_the_change_can_still_be_cleared(self):
        self.propose()
        self.change_.devices.get(forward_device_key="sw-b").delete()

        refused = save_proposal(self.change_, self.requester, {"sw-b": "new text"})
        cleared = save_proposal(self.change_, self.requester, {"sw-b": ""})

        self.assertFalse(refused.ok)
        self.assertTrue(cleared.ok)
        self.assertEqual(predict.proposal_entries(self.change_), {"sw-a": SECRET})

    def test_a_start_that_loses_its_claim_deletes_what_it_made(self):
        self.propose()

        class Takeover(PredictApi):
            def predict(inner, network_id, change_set_id, note):
                # Another start takes the page while Forward is being asked.
                type(self.change_).objects.filter(pk=self.change_.pk).update(
                    predict_started_at=timezone.now() + timedelta(seconds=5)
                )
                return super().predict(network_id, change_set_id, note)

        api = Takeover()

        outcome = start_prediction(
            self.change_, self.requester, confirmed=True, api=api
        )

        self.assertFalse(outcome.ok)
        self.assertEqual(api.deleted, ["cs-1"])
        self.change_.refresh_from_db()
        self.assertEqual(self.change_.predict_snapshot_id, "")

    def test_a_start_that_loses_its_claim_before_recording_the_set_deletes_it(self):
        self.propose()

        class Takeover(PredictApi):
            def create_change_set(inner, network_id, *, name, snapshot_id):
                type(self.change_).objects.filter(pk=self.change_.pk).update(
                    predict_started_at=timezone.now() + timedelta(seconds=5)
                )
                return super().create_change_set(
                    network_id, name=name, snapshot_id=snapshot_id
                )

        api = Takeover()

        outcome = start_prediction(
            self.change_, self.requester, confirmed=True, api=api
        )

        self.assertFalse(outcome.ok)
        self.assertEqual(api.deleted, ["cs-1"])
        self.assertNotIn("set", api.calls)

    def test_a_start_for_a_change_closed_meanwhile_deletes_what_it_made(self):
        self.propose()

        class Closer(PredictApi):
            def predict(inner, network_id, change_set_id, note):
                type(self.change_).objects.filter(pk=self.change_.pk).update(
                    state=State.ABANDONED
                )
                return super().predict(network_id, change_set_id, note)

        api = Closer()

        outcome = start_prediction(
            self.change_, self.requester, confirmed=True, api=api
        )

        self.assertFalse(outcome.ok)
        self.assertEqual(api.deleted, ["cs-1"])


class CleanupTest(PredictFixture):
    def with_set(self, state=State.STAGED):
        self.change_.state = state
        self.change_.predict_change_set_id = "cs-9"
        self.change_.predict_snapshot_id = "snap-9"
        self.change_.save()

    def test_discard_deletes_the_set_and_clears_the_ids(self):
        self.with_set()
        self.change_.predict_status = PredictOutcome.PENDING
        self.change_.predict_reason = "in progress"
        self.change_.save()
        api = PredictApi()

        self.assertTrue(discard_prediction(self.change_, api=api))

        self.assertEqual(api.deleted, ["cs-9"])
        self.change_.refresh_from_db()
        self.assertEqual(self.change_.predict_change_set_id, "")
        self.assertEqual(self.change_.predict_snapshot_id, "")
        # The panel does not keep describing a prediction that no longer exists.
        self.assertEqual(self.change_.predict_status, "")
        self.assertEqual(self.change_.predict_reason, "")

    def test_discard_keeps_the_id_when_forward_will_not_delete(self):
        self.with_set()
        api = PredictApi()
        api.delete_fails = True

        self.assertFalse(discard_prediction(self.change_, api=api))

        self.change_.refresh_from_db()
        self.assertEqual(self.change_.predict_change_set_id, "cs-9")

    def test_discard_with_nothing_to_delete_is_a_no_op(self):
        api = PredictApi()

        self.assertTrue(discard_prediction(self.change_, api=api))
        self.assertEqual(api.calls, [])

    def test_abandoning_a_change_deletes_its_change_set_after_commit(self):
        self.with_set()
        api = PredictApi()
        with patch.object(
            predict, "_open_api", side_effect=lambda change, a: (api, None)
        ):
            with self.captureOnCommitCallbacks(execute=True):
                outcome = transition(self.change_, State.ABANDONED, self.requester)

        self.assertTrue(outcome.ok)
        self.assertEqual(api.deleted, ["cs-9"])

    def test_abandoning_succeeds_even_when_forward_cannot_be_reached(self):
        self.with_set()
        api = PredictApi()
        api.delete_fails = True
        with patch.object(
            predict, "_open_api", side_effect=lambda change, a: (api, None)
        ):
            with self.captureOnCommitCallbacks(execute=True):
                outcome = transition(self.change_, State.ABANDONED, self.requester)

        self.assertTrue(outcome.ok)
        self.change_.refresh_from_db()
        self.assertEqual(self.change_.state, State.ABANDONED)

    def test_deleting_a_change_deletes_its_change_set_only_after_the_commit(self):
        self.change_.branch = None
        self.change_.save()
        self.with_set()
        api = PredictApi()
        with patch.object(
            predict, "_open_api", side_effect=lambda change, a: (api, None)
        ):
            with self.captureOnCommitCallbacks(execute=False) as callbacks:
                self.change_.delete()
            self.assertEqual(api.deleted, [])
            for callback in callbacks:
                callback()

        self.assertEqual(api.deleted, ["cs-9"])

    def test_closing_through_the_branch_merge_also_deletes_the_change_set(self):
        from forward_netbox.change_control.actions import close_when_the_branch_merges

        self.with_set(State.VERIFIED_PROCEED)
        api = PredictApi()
        with patch.object(
            predict, "_open_api", side_effect=lambda change, a: (api, None)
        ):
            with self.captureOnCommitCallbacks(execute=True):
                close_when_the_branch_merges(
                    None, SimpleNamespace(pk=self.branch.pk, name="b"), self.requester
                )

        self.change_.refresh_from_db()
        self.assertEqual(self.change_.state, State.CLOSED)
        self.assertEqual(api.deleted, ["cs-9"])

    def test_deleting_a_change_with_no_prediction_asks_forward_nothing(self):
        self.change_.branch = None
        self.change_.save()
        api = PredictApi()
        with patch.object(
            predict, "_open_api", side_effect=lambda change, a: (api, None)
        ):
            self.change_.delete()

        self.assertEqual(api.calls, [])


class QuarantineTest(PredictFixture):
    """A prediction must never become a decision."""

    def twin(self):
        from netbox_branching.models import Branch

        branch = Branch.objects.create(
            name=f"twin-{uuid4().hex[:10]}",
            schema_id=f"twin_{uuid4().hex[:10]}",
            status=self.branch.status,
        )
        return self.change(
            State.STAGED,
            branch=branch,
            workspace_network_id="ws-1",
            before_snapshot_id="base-1",
        )

    def test_an_answered_prediction_changes_no_gate_outcome(self):
        plain = self.change_
        predicted = self.twin()
        predicted.predict_status = PredictOutcome.ANSWERED
        predicted.predict_reason = "Forward predicted this change."
        predicted.predict_pre_verdict = "pass"
        predicted.predict_snapshot_id = "snap-p"
        predicted.predict_change_set_id = "cs-1"
        predicted.save()

        for target in (State.APPROVED, State.ABANDONED, State.APPLIED):
            with self.subTest(target=target):
                a = check_transition(plain, target)
                b = check_transition(predicted, target)
                self.assertEqual((a.allowed, a.blockers), (b.allowed, b.blockers))
        a, b = closure_check(plain), closure_check(predicted)
        self.assertEqual((a.allowed, a.blockers), (b.allowed, b.blockers))

    def test_a_failed_or_pending_prediction_blocks_nothing_either(self):
        plain = self.change_
        for status in (PredictOutcome.PENDING, PredictOutcome.FAILED):
            with self.subTest(status=status):
                other = self.twin()
                other.predict_status = status
                other.save()
                a = check_transition(plain, State.APPROVED)
                b = check_transition(other, State.APPROVED)
                self.assertEqual((a.allowed, a.blockers), (b.allowed, b.blockers))


class WorkspaceApiTest(ChangeFixture):
    """The wrapper calls the SDK's real service methods with the SDK's argument names."""

    def api(self):
        service = create_autospec(ChangeSetsService, instance=True)
        handle = create_autospec(ChangeSetHandle, instance=True)
        service.handle.return_value = handle
        service.create.return_value = SimpleNamespace(id=42)
        handle.predict.return_value = SimpleNamespace(id=77)
        handle.validate_commands.return_value = SimpleNamespace(
            command_errors=[{"line": 1}]
        )
        api = ForwardWorkspaceApi.__new__(ForwardWorkspaceApi)
        api.client = SimpleNamespace(
            _call_sdk=lambda fn, *a, **k: fn(*a, **k),
            _sdk_client=SimpleNamespace(change_sets=service),
        )
        return api, service, handle

    def test_every_call_matches_the_sdk_signatures(self):
        api, service, handle = self.api()

        self.assertEqual(
            api.create_change_set("ws-1", name="n", snapshot_id="base-1"), "42"
        )
        service.create.assert_called_once_with(
            "n", snapshot_id="base-1", network_id="ws-1"
        )
        self.assertEqual(api.validate_commands("ws-1", "42", "sw-a", "x"), 1)
        api.set_commands("ws-1", "42", "sw-a", "x")
        api.commit("ws-1", "42", "note")
        self.assertEqual(api.predict("ws-1", "42", "note"), "77")
        api.delete_change_set("ws-1", "42")

        service.handle.assert_called_with("42", network_id="ws-1")
        handle.set_commands.assert_called_once_with("sw-a", "x")
        handle.commit.assert_called_once_with("note")
        handle.predict.assert_called_once_with("note")
        service.delete.assert_called_once_with(["42"], network_id="ws-1")


class ValidationQuarantineTest(PredictFixture):
    def test_a_predict_rule_gives_a_prediction_no_say_in_the_validation_gate(self):
        from forward_netbox.change_control.validation import _predict
        from forward_netbox.change_control.validation import Status
        from forward_netbox.change_control.validation import validation_blockers
        from forward_netbox.models import ForwardChangeValidationPolicy
        from forward_netbox.models import ForwardChangeValidationRule

        policy = ForwardChangeValidationPolicy.objects.create(
            name="needs-a-run", require_passed_run=True
        )
        rule = ForwardChangeValidationRule.objects.create(
            policy=policy, name="predict", engine="predict"
        )
        before = validation_blockers(self.change_)
        self.assertTrue(before, "the control: the policy does block without a run")

        self.change_.predict_status = PredictOutcome.ANSWERED
        self.change_.predict_pre_verdict = "pass"
        self.change_.predict_snapshot_id = "snap-p"
        self.change_.save()

        self.assertEqual(validation_blockers(self.change_), before)
        self.assertEqual(
            [r.status for r in _predict(rule, SimpleNamespace())], [Status.SKIP]
        )
