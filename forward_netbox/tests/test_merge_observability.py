import signal
from unittest.mock import patch

from django.test import SimpleTestCase

from forward_netbox.utilities.merge_observability import (
    ForwardMergeSignalError,
    failure_evidence,
    rq_process_failure_evidence,
)


class MergeProcessEvidenceTest(SimpleTestCase):
    def test_rq_waitpid_signal_is_structured(self):
        with patch(
            "forward_netbox.utilities.merge_observability._rq_exception_text",
            return_value=(
                "Work-horse terminated unexpectedly; waitpid returned 9 (signal 9)"
            ),
        ):
            evidence = rq_process_failure_evidence(object())

        self.assertEqual(evidence["failure_kind"], "signal")
        self.assertEqual(evidence["exception_type"], "WorkHorseKilledError")
        self.assertEqual(evidence["process_wait_status"], 9)
        self.assertEqual(evidence["process_signal"], 9)
        self.assertEqual(evidence["process_signal_name"], "SIGKILL")

    def test_missing_parent_worker_evidence_is_explicitly_unknown(self):
        with patch(
            "forward_netbox.utilities.merge_observability._rq_exception_text",
            return_value="",
        ):
            evidence = rq_process_failure_evidence(object())

        self.assertEqual(evidence["failure_kind"], "unknown_termination")
        self.assertEqual(evidence["exception_type"], "UnknownProcessTermination")
        self.assertNotIn("process_signal", evidence)

    def test_rq_waitpid_exit_status_is_decoded(self):
        with patch(
            "forward_netbox.utilities.merge_observability._rq_exception_text",
            return_value="Work-horse terminated; waitpid returned 1792",
        ):
            evidence = rq_process_failure_evidence(object())

        self.assertEqual(evidence["failure_kind"], "process_exit")
        self.assertEqual(evidence["process_wait_status"], 1792)
        self.assertEqual(evidence["process_exit_code"], 7)
        self.assertIsNone(evidence["process_signal"])

    def test_catchable_signal_records_type_number_and_name(self):
        evidence = failure_evidence(ForwardMergeSignalError(signal.SIGTERM))

        self.assertEqual(evidence["failure_kind"], "signal")
        self.assertEqual(evidence["exception_type"], "ForwardMergeSignalError")
        self.assertEqual(evidence["process_signal"], signal.SIGTERM)
        self.assertEqual(evidence["process_signal_name"], "SIGTERM")

    def test_system_exit_records_exit_code_and_exception_type(self):
        evidence = failure_evidence(SystemExit(23))

        self.assertEqual(evidence["failure_kind"], "process_exit")
        self.assertEqual(evidence["exception_type"], "SystemExit")
        self.assertEqual(evidence["process_exit_code"], 23)
