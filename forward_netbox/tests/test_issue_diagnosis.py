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
