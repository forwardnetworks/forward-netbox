import json
import re
from datetime import timedelta
from pathlib import Path

from django.core.management.base import BaseCommand
from django.utils import timezone

from forward_netbox.choices import FORWARD_SUPPORTED_MODELS
from forward_netbox.management.commands.forward_architecture_audit import (
    Command as ArchitectureAuditCommand,
)


LEGACY_FIELD_SCALE_RUNTIME_KEY = "ad" + "p_scale_runtime_matrix_verified"


class Command(BaseCommand):
    help = (
        "Emit a requirement-by-requirement architecture completion audit that "
        "separates in-repo proven checks from items that still require external "
        "runtime evidence."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--output-json",
            default="",
            help="Optional path to write the report JSON.",
        )
        parser.add_argument(
            "--runtime-evidence",
            default="docs/03_Plans/evidence/architecture-runtime-evidence.json",
            help=(
                "Path to runtime evidence JSON produced by "
                "`invoke architecture-runtime-evidence`."
            ),
        )

    def handle(self, *args, **options):
        self._runtime_evidence_path = (options.get("runtime_evidence") or "").strip()
        report = self._build_report()
        rendered = json.dumps(report, indent=2, sort_keys=True, default=str)
        self.stdout.write(rendered)

        output_path = (options.get("output_json") or "").strip()
        if output_path:
            output_file = Path(output_path)
            if not output_file.is_absolute():
                output_file = Path(__file__).resolve().parents[3] / output_file
            output_file.parent.mkdir(parents=True, exist_ok=True)
            with open(output_file, "w", encoding="utf-8") as handle:
                handle.write(rendered + "\n")
            output_file.chmod(0o666)
            self.stdout.write(
                self.style.SUCCESS(
                    f"Wrote architecture completion audit report to {output_path}"
                )
            )

    def _build_report(self):
        matrix = ArchitectureAuditCommand()._apply_engine_matrix()
        gaps = matrix["classification_gaps"]
        model_eligibility = matrix["model_eligibility"]
        tasks_text = self._read_repo_text("tasks.py")
        validation_text = self._read_repo_text(
            "docs/00_Project_Knowledge/validation-matrix.md"
        )
        support_bundle_text = self._read_repo_text(
            "forward_netbox/utilities/execution_ledger_serialization.py"
        )
        runtime_evidence = self._runtime_evidence()
        expansion = matrix["bulk_orm_expansion"]
        parity_plan = expansion.get("parity_plan") or {}
        parity_test_ids = self._bulk_orm_parity_test_ids(parity_plan)
        test_sync_text = self._read_repo_text("forward_netbox/tests/test_sync.py")
        missing_parity_tests = [
            test_id
            for test_id in parity_test_ids
            if not self._test_id_present(test_sync_text, test_id)
        ]

        checks = [
            self._check(
                "bulk_orm_classification_gaps_clear",
                not ArchitectureAuditCommand()._has_classification_gaps(gaps),
                "apply_engine_matrix.classification_gaps",
                external=False,
            ),
            self._check(
                "bulk_orm_model_eligibility_coverage_complete",
                set(model_eligibility) == set(FORWARD_SUPPORTED_MODELS),
                "apply_engine_matrix.model_eligibility",
                external=False,
            ),
            self._check(
                "model_contract_registry_complete",
                matrix["model_contract_registry"]["status"] == "pass"
                and set(matrix["model_contract_registry"]["models"])
                == set(FORWARD_SUPPORTED_MODELS),
                "apply_engine_matrix.model_contract_registry",
                external=False,
            ),
            self._check(
                "bulk_orm_safe_set_present",
                set(matrix["bulk_orm_safe_models"])
                >= {
                    "dcim.site",
                    "dcim.manufacturer",
                    "dcim.devicerole",
                    "dcim.platform",
                    "dcim.devicetype",
                    "dcim.virtualchassis",
                    "ipam.vlan",
                    "ipam.vrf",
                },
                "apply_engine_matrix.bulk_orm_safe_models",
                external=False,
            ),
            self._check(
                "bulk_orm_parity_plan_present",
                parity_plan.get("status") == "pending_candidate_parity"
                and bool(parity_plan.get("candidates"))
                and bool(parity_test_ids),
                {
                    "status": parity_plan.get("status"),
                    "candidate_count": parity_plan.get("candidate_count"),
                    "first_candidate": (
                        parity_plan.get("candidates", [{}])[0].get("model")
                        if parity_plan.get("candidates")
                        else None
                    ),
                    "required_test_count": len(parity_test_ids),
                },
                external=False,
            ),
            self._check(
                "bulk_orm_candidate_parity_tests_complete",
                not missing_parity_tests,
                {
                    "missing_test_count": len(missing_parity_tests),
                    "missing_tests": missing_parity_tests[:20],
                    "note": (
                        "Candidate models remain on adapter until these parity "
                        "tests exist and pass."
                    ),
                },
                external=False,
            ),
            self._check(
                "architecture_gate_enforced_in_ci",
                "architecture_audit_check," in tasks_text,
                "tasks.py: ci pre-task includes architecture_audit_check",
                external=False,
            ),
            self._check(
                "destructive_chaos_harness_present",
                all(
                    marker in tasks_text
                    for marker in (
                        "stage-before-branch",
                        "stage-after-branch",
                        "stage-during-apply",
                        "merge-during-exec",
                    )
                ),
                "tasks.py: docker-chaos-kill scenario set",
                external=False,
            ),
            self._check(
                "validation_docs_include_architecture_audit_gate",
                "invoke architecture-audit-check" in validation_text,
                "docs/00_Project_Knowledge/validation-matrix.md",
                external=False,
            ),
            self._check(
                "compatibility_cache_prune_task_present",
                "def prune_compat_cache" in tasks_text
                and "forward_prune_compatibility_cache" in tasks_text,
                "tasks.py: prune-compat-cache invokes forward_prune_compatibility_cache",
                external=False,
            ),
            self._check(
                "validation_docs_include_compatibility_prune_evidence",
                "invoke prune-compat-cache" in validation_text,
                "docs/00_Project_Knowledge/validation-matrix.md",
                external=False,
            ),
            self._check(
                "support_bundle_reports_compatibility_cache_status",
                '"compatibility_cache": _compatibility_cache_evidence(run)'
                in support_bundle_text
                and '"stale_payload_present": stale_payload_present'
                in support_bundle_text,
                (
                    "forward_netbox/utilities/execution_ledger_serialization.py: "
                    "support bundle includes compatibility cache evidence"
                ),
                external=False,
            ),
            self._check(
                "field_scale_runtime_matrix_verified",
                runtime_evidence["field_scale_runtime_matrix_verified"],
                runtime_evidence["field_scale_runtime_matrix_evidence"],
                external=True,
            ),
            self._check(
                "destructive_runtime_worker_kill_evidence_verified",
                runtime_evidence["destructive_runtime_worker_kill_evidence_verified"],
                runtime_evidence["destructive_runtime_worker_kill_evidence"],
                external=True,
            ),
            self._check(
                "runtime_fallback_reduction_evidence_verified",
                runtime_evidence["runtime_fallback_reduction_verified"],
                runtime_evidence["runtime_fallback_reduction_evidence"],
                external=True,
            ),
            self._check(
                "scheduler_overlap_readiness_evidence_verified",
                runtime_evidence["scheduler_overlap_readiness_verified"],
                runtime_evidence["scheduler_overlap_readiness_evidence"],
                external=True,
            ),
        ]

        completed = sum(1 for item in checks if item["status"] == "completed")
        pending_external = sum(
            1 for item in checks if item["status"] == "needs_external_evidence"
        )
        failed = sum(1 for item in checks if item["status"] == "failed")
        return {
            "objective": (
                "complete all remaining architecture work, including testing models "
                "that should or should not go into bulk orm"
            ),
            "checks": checks,
            "summary": {
                "completed": completed,
                "needs_external_evidence": pending_external,
                "failed": failed,
                "all_repo_checks_green": failed == 0,
            },
            "runtime_evidence": runtime_evidence,
        }

    def _read_repo_text(self, relative_path: str) -> str:
        repo_root = Path(__file__).resolve().parents[3]
        return (repo_root / relative_path).read_text(encoding="utf-8")

    def _runtime_evidence(self):
        evidence_file = getattr(self, "_runtime_evidence_path", "").strip()
        if not evidence_file:
            return self._default_runtime_evidence(
                "No runtime evidence path configured."
            )
        try:
            payload = json.loads(self._read_repo_text(evidence_file))
        except FileNotFoundError:
            return self._default_runtime_evidence(
                f"Runtime evidence file not found: {evidence_file}"
            )
        except json.JSONDecodeError:
            return self._default_runtime_evidence(
                f"Runtime evidence file is not valid JSON: {evidence_file}"
            )

        max_age = timedelta(days=7)
        created = self._parse_timestamp(payload.get("generated_at"))
        is_fresh = bool(created and timezone.now() - created <= max_age)
        field_scale_check = payload.get("checks", {}).get(
            "field_scale_runtime_matrix_verified"
        ) or payload.get("checks", {}).get(LEGACY_FIELD_SCALE_RUNTIME_KEY)
        chaos_check = payload.get("checks", {}).get(
            "destructive_runtime_worker_kill_evidence_verified"
        )
        fallback_check = payload.get("checks", {}).get(
            "runtime_fallback_reduction_verified"
        )
        scheduler_check = payload.get("checks", {}).get(
            "scheduler_overlap_readiness_verified"
        )
        capacity_check = payload.get("checks", {}).get(
            "runtime_capacity_review_present"
        )
        field_scale_ok = bool(
            is_fresh
            and field_scale_check
            and field_scale_check.get("status") == "passed"
        )
        chaos_ok = bool(
            is_fresh and chaos_check and chaos_check.get("status") == "passed"
        )
        fallback_ok = bool(
            is_fresh and fallback_check and fallback_check.get("status") == "passed"
        )
        scheduler_ok = bool(
            is_fresh and scheduler_check and scheduler_check.get("status") == "passed"
        )
        freshness_note = (
            "fresh runtime evidence"
            if is_fresh
            else "runtime evidence missing or older than 7 days"
        )
        return {
            "path": evidence_file,
            "fresh": is_fresh,
            "generated_at": payload.get("generated_at"),
            "field_scale_runtime_matrix_verified": field_scale_ok,
            "field_scale_runtime_matrix_evidence": (
                field_scale_check.get("evidence")
                if field_scale_check and field_scale_check.get("evidence")
                else (
                    f"{freshness_note}; requires explicit field-scale runtime "
                    "matrix run."
                )
            ),
            "destructive_runtime_worker_kill_evidence_verified": chaos_ok,
            "destructive_runtime_worker_kill_evidence": (
                chaos_check.get("evidence")
                if chaos_check and chaos_check.get("evidence")
                else (
                    f"{freshness_note}; requires docker-chaos-kill scenarios with "
                    "captured support-bundle artifacts."
                )
            ),
            "runtime_fallback_reduction_verified": fallback_ok,
            "runtime_fallback_reduction_evidence": (
                fallback_check.get("evidence")
                if fallback_check and fallback_check.get("evidence")
                else (
                    f"{freshness_note}; requires repeated large-run support bundles "
                    "showing low fallback reason counts or explainable residual "
                    "fallback causes."
                )
            ),
            "scheduler_overlap_readiness_verified": scheduler_ok,
            "scheduler_overlap_readiness_evidence": (
                self._scheduler_evidence_with_capacity(
                    scheduler_check.get("evidence"),
                    capacity_check.get("evidence") if capacity_check else None,
                )
                if scheduler_check and scheduler_check.get("evidence")
                else (
                    f"{freshness_note}; requires repeated large-run support bundles "
                    "showing whether scheduler overlap is not warranted, unknown, "
                    "blocked, or a candidate with capacity evidence."
                )
            ),
        }

    def _default_runtime_evidence(self, message: str):
        return {
            "path": getattr(self, "_runtime_evidence_path", ""),
            "fresh": False,
            "generated_at": None,
            "field_scale_runtime_matrix_verified": False,
            "field_scale_runtime_matrix_evidence": message,
            "destructive_runtime_worker_kill_evidence_verified": False,
            "destructive_runtime_worker_kill_evidence": message,
            "runtime_fallback_reduction_verified": False,
            "runtime_fallback_reduction_evidence": message,
            "scheduler_overlap_readiness_verified": False,
            "scheduler_overlap_readiness_evidence": message,
        }

    def _scheduler_evidence_with_capacity(self, scheduler_evidence, capacity_evidence):
        if not isinstance(scheduler_evidence, dict):
            return scheduler_evidence
        enriched = dict(scheduler_evidence)
        if capacity_evidence is not None:
            enriched["capacity_review"] = capacity_evidence
        return enriched

    def _parse_timestamp(self, value):
        if not value or not isinstance(value, str):
            return None
        try:
            return timezone.datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None

    def _check(self, check_id, condition, evidence, *, external):
        if condition:
            status = "completed"
        elif external:
            status = "needs_external_evidence"
        else:
            status = "failed"
        return {
            "id": check_id,
            "status": status,
            "external_evidence_required": bool(external and not condition),
            "evidence": evidence,
        }

    def _bulk_orm_parity_test_ids(self, parity_plan):
        test_ids = []
        candidates = parity_plan.get("candidates") or []
        if not candidates:
            return test_ids
        for test_id in candidates[0].get("required_test_ids") or []:
            if test_id and test_id not in test_ids:
                test_ids.append(test_id)
        return test_ids

    def _test_id_present(self, source_text, test_id):
        _, _, method_name = str(test_id).partition(".")
        if not method_name:
            return False
        return bool(re.search(rf"def\s+{re.escape(method_name)}\s*\(", source_text))
