"""A merge failure must record enough to act on, and nothing more.

Merge issues persisted only the exception class name and an empty `raw_data`.
Four `IntegrityError` rows then blocked a customer's baseline for a day, and the
constraint they violated was recoverable from nowhere — not the GUI, the API,
the CLI, or a support bundle. The list view compounded it by truncating the
message and offering no way to open a row.

The opposite failure is just as real: this repository deliberately keeps
customer data out of persisted diagnostics. Constraint, table, column and field
names are schema identifiers the plugin defines. The key *values* a Postgres
DETAIL line embeds are not, and are not captured.
"""

from django.core.exceptions import ValidationError
from django.db import IntegrityError
from django.test import SimpleTestCase
from django.test import TestCase
from django.urls import reverse

from forward_netbox.utilities.diagnostics import structured_failure_diagnosis


class _Diag:
    def __init__(self, **values):
        for key, value in values.items():
            setattr(self, key, value)


def _integrity_error(**diag):
    cause = Exception("duplicate key value violates unique constraint")
    cause.diag = _Diag(**diag)
    error = IntegrityError("duplicate key")
    error.__cause__ = cause
    return error


class StructuredFailureDiagnosisTest(SimpleTestCase):
    def test_a_unique_violation_names_the_constraint(self):
        # The exact question that could not be answered for a day.
        diagnosis = structured_failure_diagnosis(
            _integrity_error(
                constraint_name="netbox_dlm_cve_cve_id_key",
                table_name="netbox_dlm_cve",
                column_name="",
            )
        )
        self.assertEqual(diagnosis["exception_type"], "IntegrityError")
        self.assertEqual(diagnosis["constraint_name"], "netbox_dlm_cve_cve_id_key")
        self.assertEqual(diagnosis["table_name"], "netbox_dlm_cve")

    def test_a_validation_error_names_the_offending_fields(self):
        diagnosis = structured_failure_diagnosis(
            ValidationError({"name": ["too long"], "site": ["required"]})
        )
        self.assertEqual(diagnosis["invalid_fields"], ["name", "site"])

    def test_validation_messages_are_not_captured(self):
        # Messages quote the submitted value, so only field names are kept.
        diagnosis = structured_failure_diagnosis(
            ValidationError({"name": ["'customer-device-01' is already used"]})
        )
        self.assertNotIn("customer-device-01", str(diagnosis))

    def test_a_value_bearing_diag_field_is_not_captured(self):
        # psycopg exposes message_detail with the key VALUE; it must not land
        # in a persisted record.
        diagnosis = structured_failure_diagnosis(
            _integrity_error(
                constraint_name="some_key",
                message_detail="Key (name)=(customer-device-01) already exists.",
            )
        )
        self.assertNotIn("customer-device-01", str(diagnosis))

    def test_a_non_identifier_value_is_rejected(self):
        # Anything that is not a plain schema token is dropped rather than
        # persisted on the assumption it is safe.
        diagnosis = structured_failure_diagnosis(
            _integrity_error(constraint_name="Key (name)=(customer-device-01)")
        )
        self.assertNotIn("constraint_name", diagnosis)

    def test_a_plain_exception_still_yields_its_type(self):
        self.assertEqual(
            structured_failure_diagnosis(RuntimeError("boom")),
            {"exception_type": "RuntimeError"},
        )

    def test_a_missing_cause_does_not_raise(self):
        self.assertEqual(
            structured_failure_diagnosis(IntegrityError("no cause")),
            {"exception_type": "IntegrityError"},
        )


class IngestionIssueDetailRouteTest(TestCase):
    def test_an_issue_is_reachable(self):
        # The list truncates the message and shows only the exception class, so
        # without a detail route a failure could not be inspected at all.
        self.assertEqual(
            reverse("plugins:forward_netbox:forwardingestionissue", kwargs={"pk": 1}),
            "/plugins/forward/ingestion-issue/1/",
        )


class SyncPhaseDiagnosisTest(TestCase):
    """The sync phase must record the constraint too, not just merge.

    2.6.6 gave merge issues a schema-level diagnosis and left the sync recorder
    persisting an exception class name only. A customer's terminating
    `dcim.module` IntegrityError therefore read "row processing failed
    (IntegrityError)" with an empty coalesce context — naming neither the
    constraint nor the row — and the sync that died could not be diagnosed from
    the GUI, the API or a support bundle. Sync-phase failures are the common
    case, so this was the larger half of the gap.
    """

    def _runner(self, ingestion):
        from types import SimpleNamespace

        return SimpleNamespace(
            ingestion=ingestion,
            logger=SimpleNamespace(
                log_info=lambda *a, **k: None,
                log_failure=lambda *a, **k: None,
                log_warning=lambda *a, **k: None,
            ),
            _recorded_issue_ids=set(),
            _dependency_skip_issue_counts={},
            _dependency_skip_issue_samples={},
            DEPENDENCY_SKIP_ISSUE_DETAIL_LIMIT=5,
        )

    def _ingestion(self):
        from forward_netbox.models import (
            ForwardIngestion,
            ForwardSource,
            ForwardSync,
        )

        source = ForwardSource.objects.create(
            name="diagnosis-source", url="https://fwd.example.invalid"
        )
        sync = ForwardSync.objects.create(name="diagnosis-sync", source=source)
        return ForwardIngestion.objects.create(sync=sync)

    def _record(self, exception):
        from forward_netbox.utilities.sync_reporting import record_issue

        ingestion = self._ingestion()
        return record_issue(
            self._runner(ingestion),
            "dcim.module",
            "ignored",
            {"device": "x", "module_bay": "y"},
            exception=exception,
        )

    def test_the_message_names_the_violated_constraint(self):
        issue = self._record(
            _integrity_error(
                constraint_name="dcim_module_module_bay_id_key",
                table_name="dcim_module",
                column_name="",
            )
        )

        self.assertIn("dcim_module_module_bay_id_key", issue.message)
        self.assertIn("IntegrityError", issue.message)

    def test_raw_data_carries_the_structured_diagnosis(self):
        issue = self._record(
            _integrity_error(
                constraint_name="dcim_module_module_bay_id_key",
                table_name="dcim_module",
                column_name="",
            )
        )

        self.assertEqual(
            issue.raw_data["constraint_name"], "dcim_module_module_bay_id_key"
        )
        self.assertEqual(issue.raw_data["table_name"], "dcim_module")
        # The row shape is still present alongside it.
        self.assertEqual(issue.raw_data["type"], "mapping")
        self.assertEqual(sorted(issue.raw_data["fields"]), ["device", "module_bay"])

    def test_a_validation_error_names_the_invalid_fields(self):
        issue = self._record(ValidationError({"module_bay": ["already occupied"]}))

        self.assertIn("module_bay", issue.message)
        self.assertEqual(issue.raw_data["invalid_fields"], ["module_bay"])

    def test_no_constraint_leaves_the_message_as_before(self):
        # An exception with nothing to add must not gain empty punctuation.
        issue = self._record(RuntimeError("boom"))

        self.assertEqual(
            issue.message, "dcim.module row processing failed (RuntimeError)."
        )

    def test_submitted_values_are_never_persisted(self):
        issue = self._record(
            ValidationError({"module_bay": ["'Bay 3' on device core-1 is taken"]})
        )

        self.assertNotIn("core-1", str(issue.raw_data))
        self.assertNotIn("core-1", issue.message)


class TerminalSyncFailureDiagnosisTest(TestCase):
    """The failure that stops a sync must be the most explicable row, not the least.

    A terminating sync failure recorded a blank model, empty coalesce fields and
    an empty `raw_data` — so the one issue that explains why a run died said
    only "Forward ingestion failed (IntegrityError)". An operator could not tell
    which constraint from the GUI, the API or a support bundle, only from server
    logs, which is exactly what the UI is supposed to spare them.
    """

    def _fail_with(self, exc):
        from forward_netbox.models import (
            ForwardIngestion,
            ForwardSource,
            ForwardSync,
        )
        from forward_netbox.utilities.sync_orchestration import (
            _record_forward_sync_failure,
        )

        source = ForwardSource.objects.create(
            name="terminal-source", url="https://fwd.example.invalid"
        )
        sync = ForwardSync.objects.create(name="terminal-sync", source=source)
        ingestion = ForwardIngestion.objects.create(sync=sync)
        _record_forward_sync_failure(sync, None, None, ingestion, exc)
        return ingestion.issues.latest("pk")

    def test_the_terminating_constraint_is_named(self):
        issue = self._fail_with(
            _integrity_error(
                constraint_name="dcim_module_module_bay_id_key",
                table_name="dcim_module",
                column_name="",
            )
        )

        self.assertIn("dcim_module_module_bay_id_key", issue.message)
        self.assertEqual(
            issue.raw_data["constraint_name"], "dcim_module_module_bay_id_key"
        )
        self.assertEqual(issue.raw_data["table_name"], "dcim_module")

    def test_a_plain_exception_still_records_its_classifier(self):
        issue = self._fail_with(RuntimeError("boom"))

        self.assertEqual(issue.raw_data, {"exception_type": "RuntimeError"})
        self.assertNotIn("boom", issue.message)
        self.assertNotIn("boom", str(issue.raw_data))
