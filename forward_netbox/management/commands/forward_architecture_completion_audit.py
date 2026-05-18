import json
from datetime import timedelta
from pathlib import Path

from django.core.management.base import BaseCommand
from django.utils import timezone

from forward_netbox.choices import FORWARD_SUPPORTED_MODELS
from forward_netbox.management.commands.forward_architecture_audit import (
    Command as ArchitectureAuditCommand,
)


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
            with open(output_path, "w", encoding="utf-8") as handle:
                handle.write(rendered + "\n")
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
        runtime_evidence = self._runtime_evidence()

        checks = [
            self._check(
                "bulk_orm_classification_gaps_clear",
                all(not items for items in gaps.values()),
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
                "bulk_orm_safe_set_present",
                set(matrix["bulk_orm_safe_models"])
                == {
                    "dcim.site",
                    "dcim.manufacturer",
                    "dcim.devicerole",
                    "dcim.platform",
                    "dcim.devicetype",
                    "ipam.vlan",
                    "ipam.vrf",
                },
                "apply_engine_matrix.bulk_orm_safe_models",
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
                "adp_scale_runtime_matrix_verified",
                runtime_evidence["adp_scale_runtime_matrix_verified"],
                runtime_evidence["adp_scale_runtime_matrix_evidence"],
                external=True,
            ),
            self._check(
                "destructive_runtime_worker_kill_evidence_verified",
                runtime_evidence["destructive_runtime_worker_kill_evidence_verified"],
                runtime_evidence["destructive_runtime_worker_kill_evidence"],
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
        adp_check = payload.get("checks", {}).get("adp_scale_runtime_matrix_verified")
        chaos_check = payload.get("checks", {}).get(
            "destructive_runtime_worker_kill_evidence_verified"
        )
        adp_ok = bool(is_fresh and adp_check and adp_check.get("status") == "passed")
        chaos_ok = bool(
            is_fresh and chaos_check and chaos_check.get("status") == "passed"
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
            "adp_scale_runtime_matrix_verified": adp_ok,
            "adp_scale_runtime_matrix_evidence": (
                adp_check.get("evidence")
                if adp_check and adp_check.get("evidence")
                else f"{freshness_note}; requires explicit ORG scale runtime matrix run."
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
        }

    def _default_runtime_evidence(self, message: str):
        return {
            "path": getattr(self, "_runtime_evidence_path", ""),
            "fresh": False,
            "generated_at": None,
            "adp_scale_runtime_matrix_verified": False,
            "adp_scale_runtime_matrix_evidence": message,
            "destructive_runtime_worker_kill_evidence_verified": False,
            "destructive_runtime_worker_kill_evidence": message,
        }

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
