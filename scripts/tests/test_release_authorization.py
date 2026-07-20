from __future__ import annotations

import importlib.util
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

_SPEC = importlib.util.spec_from_file_location(
    "release_authorization",
    Path(__file__).resolve().parents[1] / "check_release_authorization.py",
)
release_authorization = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(release_authorization)


class ReleaseAuthorizationTest(unittest.TestCase):
    VALID_EVIDENCE = {
        "final-tree-full-gate": (
            "`rtk env FORWARD_NETBOX_DOCKER_PROJECT=forward-netbox-release-gate "
            "FORWARD_NETBOX_POSTGRES_DATA_PATH=netbox-postgres-data "
            "FORWARD_NETBOX_WORKER_AUTORELOAD=0 NETBOX_VER=v4.6.5 "
            "FORWARD_NETBOX_HOST_PORT=18080 NETBOX_URL=http://127.0.0.1:18080 "
            "invoke ci` passed: 1343 tests, 0 failures."
        ),
        "exact-runtime-artifact": (
            "`rtk env FORWARD_NETBOX_DOCKER_PROJECT=forward-netbox-release-gate "
            "FORWARD_NETBOX_POSTGRES_DATA_PATH=netbox-postgres-data "
            "FORWARD_NETBOX_WORKER_AUTORELOAD=0 NETBOX_VER=v4.6.5 "
            "invoke artifact-test` passed: NetBox 4.6.5, Branching 1.1.1, "
            "Python 3.14, and SBOM validation; 0 errors."
        ),
        "scale-and-failure": (
            "`rtk env FORWARD_NETBOX_DOCKER_PROJECT=forward-netbox-upgrade26 "
            "FORWARD_NETBOX_POSTGRES_DATA_PATH=netbox-postgres-data "
            "FORWARD_NETBOX_WORKER_AUTORELOAD=0 NETBOX_VER=v4.6.5 "
            "invoke scale-soak --runs 3` passed 3 runs and "
            "`rtk env FORWARD_NETBOX_DOCKER_PROJECT=forward-netbox-release-gate "
            "FORWARD_NETBOX_POSTGRES_DATA_PATH=netbox-postgres-data "
            "FORWARD_NETBOX_WORKER_AUTORELOAD=0 NETBOX_VER=v4.6.5 "
            "invoke scenario-test` passed 28 failure scenarios, 0 failures and "
            "`rtk env FORWARD_NETBOX_DOCKER_PROJECT=forward-netbox-release-gate "
            "FORWARD_NETBOX_POSTGRES_DATA_PATH=netbox-postgres-data "
            "FORWARD_NETBOX_WORKER_AUTORELOAD=0 NETBOX_VER=v4.6.5 "
            "invoke bulk-merge-retry-scale-test` passed 20,005-row replay with "
            "a 1,512-second 1M-row projection under the 2,400-second limit."
        ),
        "ui-validation": (
            "`rtk env FORWARD_NETBOX_DOCKER_PROJECT=forward-netbox-release-gate "
            "FORWARD_NETBOX_POSTGRES_DATA_PATH=netbox-postgres-data "
            "FORWARD_NETBOX_WORKER_AUTORELOAD=0 NETBOX_VER=v4.6.5 "
            "FORWARD_NETBOX_HOST_PORT=18081 NETBOX_URL=http://127.0.0.1:18081 "
            "invoke playwright-test` passed 14 desktop and mobile checks, 0 failures."
        ),
        "ownership-audit": (
            "`rtk docker compose exec netbox python manage.py "
            "forward_ownership_audit --fail-on-inconsistent "
            "--require-no-open-branches` passed: 0 inconsistencies, 0 open branches."
        ),
        "customer-equivalent-acceptance": (
            "`rtk env FORWARD_NETBOX_DOCKER_PROJECT=forward-netbox-upgrade26 "
            "FORWARD_NETBOX_POSTGRES_DATA_PATH=netbox-postgres-data "
            "FORWARD_NETBOX_WORKER_AUTORELOAD=0 NETBOX_VER=v4.6.5 "
            "invoke sync-release-gate --sync-ids 51` passed sync id 51: 0 blockers, "
            "0 warnings, 0 errors."
        ),
        "independent-review": (
            "`rtk git diff --check` passed; independent reviewer inspected 112 files "
            "and reported 0 blockers."
        ),
    }

    def _plan(
        self,
        checked=True,
        evidence=None,
        evidence_overrides=None,
        base_commit=None,
    ):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        path = Path(temporary.name) / "release-plan.md"
        marker = "x" if checked else " "
        evidence_overrides = dict(evidence_overrides or {})
        entries = "\n".join(
            f"- [{marker}] `{evidence_id}` - "
            f"{evidence_overrides.get(evidence_id, evidence if evidence is not None else self.VALID_EVIDENCE[evidence_id])}"
            for evidence_id in sorted(release_authorization.REQUIRED_EVIDENCE_IDS)
        )
        base_line = (
            f"- Evidence base commit: `{base_commit}`\n\n" if base_commit else ""
        )
        path.write_text(
            f"# Release\n\n## Release Authorization\n\n{base_line}"
            f"{entries}\n\n## Rollback\n",
            encoding="utf-8",
        )
        return path

    def test_accepts_complete_evidenced_authorization(self):
        result = release_authorization.check_release_authorization(self._plan())

        self.assertEqual(
            set(result["authorized_evidence_ids"]),
            release_authorization.REQUIRED_EVIDENCE_IDS,
        )

    def test_rejects_unchecked_evidence(self):
        with self.assertRaisesRegex(ValueError, "unchecked"):
            release_authorization.check_release_authorization(self._plan(checked=False))

    def test_rejects_placeholder_evidence(self):
        with self.assertRaisesRegex(ValueError, "placeholder_evidence"):
            release_authorization.check_release_authorization(
                self._plan(evidence="pending")
            )

    def test_rejects_prospective_checklist_language(self):
        with self.assertRaisesRegex(ValueError, "placeholder_evidence"):
            release_authorization.check_release_authorization(
                self._plan(
                    evidence="`rtk invoke ci` must pass 1343 tests before release."
                )
            )

    def test_rejects_vague_outcome_without_command_or_count(self):
        with self.assertRaisesRegex(ValueError, "placeholder_evidence"):
            release_authorization.check_release_authorization(
                self._plan(evidence="All release checks passed successfully.")
            )

    def test_rejects_command_without_retrospective_outcome(self):
        with self.assertRaisesRegex(ValueError, "placeholder_evidence"):
            release_authorization.check_release_authorization(
                self._plan(evidence="`rtk invoke ci` produced 1343 test results.")
            )

    def test_rejects_repeated_meaningless_command_for_every_evidence_class(self):
        with self.assertRaisesRegex(ValueError, "placeholder_evidence"):
            release_authorization.check_release_authorization(
                self._plan(evidence="`printf 1` passed: 1 check, 0 failures.")
            )

    def test_rejects_echoed_canonical_command_for_every_evidence_class(self):
        for evidence_id, evidence in self.VALID_EVIDENCE.items():
            echoed = evidence.replace("`rtk ", "`rtk echo ")
            with self.subTest(evidence_id=evidence_id), self.assertRaisesRegex(
                ValueError, "placeholder_evidence"
            ):
                release_authorization.check_release_authorization(
                    self._plan(evidence_overrides={evidence_id: echoed})
                )

    def test_rejects_invoke_help_noops(self):
        for evidence_id in (
            "final-tree-full-gate",
            "exact-runtime-artifact",
            "scale-and-failure",
            "ui-validation",
            "customer-equivalent-acceptance",
        ):
            evidence = self.VALID_EVIDENCE[evidence_id].replace(
                "`rtk invoke",
                "`rtk invoke",
                1,
            )
            first_command_end = evidence.index("`")
            command_end = evidence.index("`", first_command_end + 1)
            evidence = evidence[:command_end] + " --help" + evidence[command_end:]
            with self.subTest(evidence_id=evidence_id), self.assertRaisesRegex(
                ValueError, "placeholder_evidence"
            ):
                release_authorization.check_release_authorization(
                    self._plan(evidence_overrides={evidence_id: evidence})
                )

    def test_rejects_scale_evidence_without_retry_performance_gate(self):
        evidence = self.VALID_EVIDENCE["scale-and-failure"]
        marker = (
            " and `rtk env FORWARD_NETBOX_DOCKER_PROJECT=forward-netbox-release-gate"
        )
        evidence = evidence[: evidence.rindex(marker)] + "."

        with self.assertRaisesRegex(ValueError, "placeholder_evidence"):
            release_authorization.check_release_authorization(
                self._plan(evidence_overrides={"scale-and-failure": evidence})
            )

    def test_rejects_scale_evidence_with_fewer_than_three_soak_runs(self):
        evidence = self.VALID_EVIDENCE["scale-and-failure"].replace(
            "invoke scale-soak --runs 3",
            "invoke scale-soak --runs 1",
        )

        with self.assertRaisesRegex(ValueError, "placeholder_evidence"):
            release_authorization.check_release_authorization(
                self._plan(evidence_overrides={"scale-and-failure": evidence})
            )

    def test_rejects_echoed_ownership_audit(self):
        evidence = self.VALID_EVIDENCE["ownership-audit"].replace(
            "exec netbox python manage.py",
            "exec netbox echo python manage.py",
        )
        with self.assertRaisesRegex(ValueError, "placeholder_evidence"):
            release_authorization.check_release_authorization(
                self._plan(evidence_overrides={"ownership-audit": evidence})
            )

    def test_rejects_execution_changing_environment_prefixes(self):
        for evidence_id, evidence in self.VALID_EVIDENCE.items():
            unsafe = evidence.replace("`rtk ", "`rtk env PATH=/missing ")
            with self.subTest(evidence_id=evidence_id), self.assertRaisesRegex(
                ValueError, "placeholder_evidence"
            ):
                release_authorization.check_release_authorization(
                    self._plan(evidence_overrides={evidence_id: unsafe})
                )

    def test_rejects_mismatched_release_ports(self):
        evidence = self.VALID_EVIDENCE["final-tree-full-gate"].replace(
            "NETBOX_URL=http://127.0.0.1:18080",
            "NETBOX_URL=http://127.0.0.1:18081",
        )
        with self.assertRaisesRegex(ValueError, "placeholder_evidence"):
            release_authorization.check_release_authorization(
                self._plan(evidence_overrides={"final-tree-full-gate": evidence})
            )

    def test_rejects_reserved_isolated_project_as_release_target(self):
        evidence = self.VALID_EVIDENCE["exact-runtime-artifact"].replace(
            "forward-netbox-release-gate",
            "forward-netbox-artifact-test",
        )
        with self.assertRaisesRegex(ValueError, "placeholder_evidence"):
            release_authorization.check_release_authorization(
                self._plan(evidence_overrides={"exact-runtime-artifact": evidence})
            )

    def test_rejects_contradictory_failed_outcome(self):
        with self.assertRaisesRegex(ValueError, "placeholder_evidence"):
            release_authorization.check_release_authorization(
                self._plan(
                    evidence_overrides={
                        "final-tree-full-gate": (
                            "`rtk invoke ci` failed 99 checks, but passed 1 check, "
                            "0 failures."
                        )
                    }
                )
            )

    def test_requires_expected_evidence_base_commit(self):
        base_commit = "a" * 40
        result = release_authorization.check_release_authorization(
            self._plan(base_commit=base_commit),
            expected_base_commit=base_commit,
            evidence_commit="b" * 40,
        )

        self.assertEqual(result["evidence_base_commit"], base_commit)
        self.assertEqual(result["evidence_commit"], "b" * 40)

        with self.assertRaisesRegex(ValueError, "evidence base commit"):
            release_authorization.check_release_authorization(
                self._plan(),
                expected_base_commit=base_commit,
            )

    def test_git_binding_requires_evidence_only_child_commit(self):
        base_commit = "a" * 40
        head_commit = "b" * 40
        with tempfile.TemporaryDirectory() as temp_dir:
            repo_root = Path(temp_dir)
            plan = repo_root / "docs/03_Plans/active/release.md"
            plan.parent.mkdir(parents=True)
            plan.write_text("evidence\n", encoding="utf-8")

            def capture(*args):
                if args == ("rev-parse", "HEAD"):
                    return head_commit
                if args == ("rev-list", "--parents", "-n", "1", head_commit):
                    return f"{head_commit} {base_commit}"
                if args == ("diff", "--name-only", base_commit, head_commit):
                    return "docs/03_Plans/active/release.md"
                if args == ("status", "--porcelain"):
                    return ""
                raise AssertionError(args)

            with patch.object(
                release_authorization, "REPO_ROOT", repo_root
            ), patch.object(release_authorization, "_git_capture", side_effect=capture):
                self.assertEqual(
                    release_authorization.release_evidence_commit_binding(plan),
                    (base_commit, head_commit),
                )


if __name__ == "__main__":
    unittest.main()
