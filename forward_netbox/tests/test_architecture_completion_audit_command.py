import json
import os
import tempfile
from io import StringIO
from pathlib import Path

from django.core.management import call_command
from django.test import TestCase


class ForwardArchitectureCompletionAuditCommandTest(TestCase):
    def test_completion_audit_reports_bulk_orm_coverage_and_external_gaps(self):
        stream = StringIO()
        call_command(
            "forward_architecture_completion_audit",
            "--runtime-evidence",
            "docs/03_Plans/evidence/does-not-exist.json",
            stdout=stream,
        )
        payload = json.loads(stream.getvalue())

        checks = {item["id"]: item for item in payload["checks"]}
        self.assertEqual(
            checks["bulk_orm_classification_gaps_clear"]["status"],
            "completed",
        )
        self.assertEqual(
            checks["bulk_orm_model_eligibility_coverage_complete"]["status"],
            "completed",
        )
        self.assertEqual(
            checks["model_contract_registry_complete"]["status"],
            "completed",
        )
        self.assertEqual(
            checks["bulk_orm_safe_set_present"]["status"],
            "completed",
        )
        self.assertEqual(
            checks["bulk_orm_parity_plan_present"]["status"],
            "completed",
        )
        self.assertEqual(
            checks["bulk_orm_candidate_parity_tests_complete"]["status"],
            "completed",
        )
        self.assertEqual(
            checks["compatibility_cache_prune_task_present"]["status"],
            "completed",
        )
        self.assertEqual(
            checks["validation_docs_include_compatibility_prune_evidence"]["status"],
            "completed",
        )
        self.assertEqual(
            checks["support_bundle_reports_compatibility_cache_status"]["status"],
            "completed",
        )
        self.assertEqual(
            checks["field_scale_runtime_matrix_verified"]["status"],
            "needs_external_evidence",
        )
        self.assertEqual(
            checks["destructive_runtime_worker_kill_evidence_verified"]["status"],
            "needs_external_evidence",
        )
        self.assertEqual(
            checks["runtime_fallback_reduction_evidence_verified"]["status"],
            "needs_external_evidence",
        )
        self.assertEqual(
            checks["scheduler_overlap_readiness_evidence_verified"]["status"],
            "needs_external_evidence",
        )

    def test_completion_audit_is_cwd_independent(self):
        stream = StringIO()
        original_cwd = os.getcwd()
        with tempfile.TemporaryDirectory():
            try:
                os.chdir(tempfile.gettempdir())
                call_command("forward_architecture_completion_audit", stdout=stream)
            finally:
                os.chdir(original_cwd)

        payload = json.loads(stream.getvalue())
        checks = {item["id"]: item for item in payload["checks"]}
        self.assertEqual(
            checks["architecture_gate_enforced_in_ci"]["status"],
            "completed",
        )
        self.assertTrue(payload["summary"]["all_repo_checks_green"])

    def test_completion_audit_uses_runtime_evidence_file_when_fresh_and_passed(self):
        with tempfile.TemporaryDirectory():
            repo_root = Path(__file__).resolve().parents[2]
            rel_path = "docs/03_Plans/evidence/runtime-evidence-test.json"
            abs_path = repo_root / rel_path
            abs_path.parent.mkdir(parents=True, exist_ok=True)
            abs_path.write_text(
                json.dumps(
                    {
                        "generated_at": "2099-01-01T00:00:00+00:00",
                        "checks": {
                            "field_scale_runtime_matrix_verified": {
                                "status": "passed",
                                "evidence": {"run_matrix": "ok"},
                            },
                            "destructive_runtime_worker_kill_evidence_verified": {
                                "status": "passed",
                                "evidence": {"chaos": "ok"},
                            },
                            "runtime_fallback_reduction_verified": {
                                "status": "passed",
                                "evidence": {"fallback": "ok"},
                            },
                            "scheduler_overlap_readiness_verified": {
                                "status": "passed",
                                "evidence": {"scheduler": "ok"},
                            },
                        },
                    }
                )
                + "\n",
                encoding="utf-8",
            )

            stream = StringIO()
            try:
                call_command(
                    "forward_architecture_completion_audit",
                    "--runtime-evidence",
                    rel_path,
                    stdout=stream,
                )
                payload = json.loads(stream.getvalue())
            finally:
                abs_path.unlink(missing_ok=True)

        checks = {item["id"]: item for item in payload["checks"]}
        self.assertEqual(
            checks["field_scale_runtime_matrix_verified"]["status"],
            "completed",
        )
        self.assertEqual(
            checks["destructive_runtime_worker_kill_evidence_verified"]["status"],
            "completed",
        )
        self.assertEqual(
            checks["runtime_fallback_reduction_evidence_verified"]["status"],
            "completed",
        )
        self.assertEqual(
            checks["scheduler_overlap_readiness_evidence_verified"]["status"],
            "completed",
        )

    def test_completion_audit_writes_relative_output_under_repo_root(self):
        repo_root = Path(__file__).resolve().parents[2]
        rel_path = "docs/03_Plans/evidence/test-completion-audit-output.json"
        output_path = repo_root / rel_path
        output_path.unlink(missing_ok=True)
        try:
            call_command(
                "forward_architecture_completion_audit",
                "--runtime-evidence",
                "docs/03_Plans/evidence/does-not-exist.json",
                "--output-json",
                rel_path,
            )
            payload = json.loads(output_path.read_text(encoding="utf-8"))
        finally:
            output_path.unlink(missing_ok=True)

        self.assertIn("checks", payload)
        self.assertEqual(payload["summary"]["failed"], 0)
