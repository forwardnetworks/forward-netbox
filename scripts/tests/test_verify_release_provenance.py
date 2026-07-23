from __future__ import annotations

import copy
import importlib.util
import os
import subprocess
import sys
import tempfile
import unittest
import urllib.parse
from contextlib import redirect_stdout
from io import StringIO
from pathlib import Path
from unittest.mock import patch


_SPEC = importlib.util.spec_from_file_location(
    "release_provenance",
    Path(__file__).resolve().parents[1] / "verify_release_provenance.py",
)
provenance = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(provenance)


class ReleaseProvenanceTest(unittest.TestCase):
    prior_release_commit = "1" * 40
    anchor_commit = "2" * 40
    release_commit = "a" * 40
    production_commit = "b" * 40
    anchor_candidate = "9" * 40
    production_candidate = "c" * 40
    evidence_candidate = "d" * 40

    def _git(self, *arguments):
        responses = {
            ("cat-file", "-t", "refs/tags/v2.6.0"): "tag",
            ("rev-parse", "refs/tags/v2.6.0^{commit}"): self.release_commit,
            ("rev-parse", "refs/remotes/origin/main"): self.release_commit,
            (
                "merge-base",
                "--is-ancestor",
                self.release_commit,
                self.release_commit,
            ): "",
            (
                "rev-list",
                "--parents",
                "-n",
                "1",
                self.release_commit,
            ): f"{self.release_commit} {self.production_commit}",
            (
                "diff",
                "--name-only",
                self.production_commit,
                self.release_commit,
            ): "docs/03_Plans/active/2026-07-18-release-2.6.0-scope-convergence.md",
            (
                "cat-file",
                "-t",
                f"refs/tags/{provenance.PRIOR_RELEASE_TAG}",
            ): "tag",
            (
                "rev-parse",
                f"refs/tags/{provenance.PRIOR_RELEASE_TAG}^{{commit}}",
            ): self.prior_release_commit,
            (
                "merge-base",
                "--is-ancestor",
                self.prior_release_commit,
                self.release_commit,
            ): "",
            (
                "rev-list",
                "--first-parent",
                "--reverse",
                f"{self.prior_release_commit}..{self.release_commit}",
            ): (
                f"{provenance.PRIOR_POST_RELEASE_DOC_COMMIT}\n"
                f"{self.anchor_commit}\n{self.production_commit}\n"
                f"{self.release_commit}"
            ),
            (
                "rev-list",
                "--parents",
                "-n",
                "1",
                provenance.PRIOR_POST_RELEASE_DOC_COMMIT,
            ): (
                f"{provenance.PRIOR_POST_RELEASE_DOC_COMMIT} "
                f"{self.prior_release_commit}"
            ),
            (
                "diff",
                "--name-only",
                self.prior_release_commit,
                provenance.PRIOR_POST_RELEASE_DOC_COMMIT,
            ): "docs/03_Plans/completed/2026-07-16-live-acceptance-followup.md",
            (
                "rev-list",
                "--parents",
                "-n",
                "1",
                self.anchor_commit,
            ): f"{self.anchor_commit} {provenance.PRIOR_POST_RELEASE_DOC_COMMIT}",
            (
                "diff",
                "--name-only",
                provenance.PRIOR_POST_RELEASE_DOC_COMMIT,
                self.anchor_commit,
            ): "\n".join(provenance.BOOTSTRAP_REQUIRED_FILES),
        }
        return responses[arguments]

    @staticmethod
    def _path_parts(path):
        parsed = urllib.parse.urlsplit(path)
        query = urllib.parse.parse_qs(parsed.query)
        return parsed.path, int(query.get("page", ["1"])[0]), query

    def _github(self, path, _token):
        endpoint, page, query = self._path_parts(path)
        commits = {
            self.anchor_commit: provenance.PRIOR_POST_RELEASE_DOC_COMMIT,
            self.production_commit: self.anchor_commit,
            self.release_commit: self.production_commit,
        }
        if endpoint.startswith("commits/") and endpoint.count("/") == 1:
            commit = endpoint.split("/")[1]
            if commit in commits:
                return {
                    "commit": {"verification": {"verified": True}},
                    "parents": [{"sha": commits[commit]}],
                }

        pull_data = {
            self.anchor_commit: (9, "2026-07-20T08:00:00Z"),
            self.production_commit: (10, "2026-07-20T10:00:00Z"),
            self.release_commit: (11, "2026-07-20T12:00:00Z"),
        }
        for commit, (number, merged_at) in pull_data.items():
            if endpoint == f"commits/{commit}/pulls":
                if page > 1:
                    return []
                return [
                    {
                        "number": number,
                        "merged_at": merged_at,
                        "base": {"ref": "main"},
                        "merge_commit_sha": commit,
                    }
                ]

        candidates = {
            9: self.anchor_candidate,
            10: self.production_candidate,
            11: self.evidence_candidate,
        }
        merged_at = {
            9: "2026-07-20T08:00:00Z",
            10: "2026-07-20T10:00:00Z",
            11: "2026-07-20T12:00:00Z",
        }
        for number, candidate in candidates.items():
            if endpoint == f"pulls/{number}":
                return {
                    "number": number,
                    "merged_at": merged_at[number],
                    "head": {"sha": candidate},
                }

        status_runs = {
            self.production_candidate: (201, 10),
            self.evidence_candidate: (202, 11),
        }
        for candidate, (run_id, _pull_number) in status_runs.items():
            if endpoint == f"commits/{candidate}/statuses":
                if page > 1:
                    return []
                return [
                    {
                        "id": run_id,
                        "context": provenance.TRUSTED_STATUS_CONTEXT,
                        "state": "success",
                        "creator": {"login": provenance.TRUSTED_STATUS_CREATOR},
                        "target_url": (
                            "https://github.com/forwardnetworks/forward-netbox/"
                            f"actions/runs/{run_id}"
                        ),
                    }
                ]

        if (
            endpoint
            == "actions/workflows/.github%2Fworkflows%2Ftrusted-sensitive-pr.yml"
        ):
            return {
                "id": 3,
                "path": provenance.TRUSTED_SCANNER_WORKFLOW,
                "state": "active",
            }
        for candidate, (run_id, pull_number) in status_runs.items():
            if endpoint == f"actions/runs/{run_id}":
                return {
                    "id": run_id,
                    "workflow_id": 3,
                    "path": provenance.TRUSTED_SCANNER_WORKFLOW,
                    "event": "pull_request_target",
                    "status": "completed",
                    "conclusion": "success",
                    "pull_requests": [
                        {
                            "number": pull_number,
                            "head": {"sha": candidate},
                            "base": {"ref": "main"},
                        }
                    ],
                }

        workflow_paths = dict(enumerate(provenance.REQUIRED_WORKFLOWS, 1))
        for workflow_id, workflow_path in workflow_paths.items():
            encoded = urllib.parse.quote(workflow_path, safe="")
            if endpoint == f"actions/workflows/{encoded}":
                return {"id": workflow_id, "path": workflow_path, "state": "active"}
            if endpoint == f"actions/workflows/{workflow_id}/runs":
                commit = query["head_sha"][0]
                return {
                    "workflow_runs": [
                        {
                            "id": 1000 + workflow_id,
                            "workflow_id": workflow_id,
                            "path": workflow_path,
                            "head_sha": commit,
                            "head_branch": "main",
                            "event": "push",
                            "status": "completed",
                            "conclusion": "success",
                        }
                    ]
                }
        raise AssertionError(path)

    def _verify(self, *, github=None, git=None):
        with (
            patch.object(provenance, "_git_capture", side_effect=git or self._git),
            patch.object(
                provenance, "_github_json", side_effect=github or self._github
            ),
        ):
            return provenance.verify_release_provenance("v2.6.0", "token")

    def test_accepts_reviewed_bootstrap_and_release_lineage(self):
        result = self._verify()

        self.assertEqual(result["release_commit"], self.release_commit)
        self.assertEqual(result["production_commit"], self.production_commit)
        self.assertEqual(result["security_bootstrap_commit"], self.anchor_commit)
        self.assertEqual(
            result["reviewed_commits"],
            [self.anchor_commit, self.production_commit, self.release_commit],
        )

    def test_main_does_not_log_provenance_evidence_or_token(self):
        secret = "secret-provenance-evidence"
        output = StringIO()
        argv = [
            "verify_release_provenance.py",
            "--tag",
            "v2.6.0",
        ]
        with (
            patch.dict(os.environ, {"GH_TOKEN": secret}, clear=True),
            patch.object(sys, "argv", argv),
            patch.object(
                provenance,
                "verify_release_provenance",
                return_value={"untrusted_evidence": secret},
            ),
            redirect_stdout(output),
        ):
            self.assertEqual(provenance.main(), 0)

        self.assertEqual(output.getvalue(), "Release provenance verification passed.\n")
        self.assertNotIn(secret, output.getvalue())

    def test_controls_only_cli_requires_release_status_and_redacts_evidence(self):
        secret = "secret-control-evidence"
        output = StringIO()
        argv = [
            "verify_release_provenance.py",
            "--controls-only",
        ]
        with (
            patch.dict(os.environ, {"GH_TOKEN": secret}, clear=True),
            patch.object(sys, "argv", argv),
            patch.object(
                provenance,
                "verify_github_release_controls",
                return_value={"untrusted_evidence": secret},
            ) as verify,
            redirect_stdout(output),
        ):
            self.assertEqual(provenance.main(), 0)

        verify.assert_called_once_with(secret)
        self.assertEqual(
            output.getvalue(),
            "GitHub release controls verification passed.\n",
        )
        self.assertNotIn(secret, output.getvalue())

    def test_accepts_tagged_release_when_main_advanced(self):
        advanced_main = "e" * 40

        def git(*arguments):
            if arguments == ("rev-parse", "refs/remotes/origin/main"):
                return advanced_main
            if arguments == (
                "merge-base",
                "--is-ancestor",
                self.release_commit,
                advanced_main,
            ):
                return ""
            return self._git(*arguments)

        result = self._verify(git=git)

        self.assertEqual(result["release_commit"], self.release_commit)

    def test_direct_control_commit_skips_unavailable_historical_workflow_runs(self):
        commit = "f" * 40

        with (
            patch.object(provenance, "_github_pages", return_value=[]),
            patch.object(
                provenance,
                "_git_capture",
                return_value="docs/03_Plans/completed/security-controls.md",
            ),
        ):
            self.assertTrue(
                provenance._require_merged_main_pr(
                    commit,
                    "token",
                    allow_direct_control_commit=True,
                )
            )

    def test_direct_control_commit_rejects_runtime_plugin_code(self):
        commit = "f" * 40

        with (
            patch.object(provenance, "_github_pages", return_value=[]),
            patch.object(
                provenance,
                "_git_capture",
                return_value="forward_netbox/models.py",
            ),
        ):
            with self.assertRaises(provenance.ProvenanceError):
                provenance._require_merged_main_pr(
                    commit,
                    "token",
                    allow_direct_control_commit=True,
                )

    def test_rejects_tagged_release_diverged_from_main(self):
        advanced_main = "e" * 40

        def git(*arguments):
            if arguments == ("rev-parse", "refs/remotes/origin/main"):
                return advanced_main
            if arguments == (
                "merge-base",
                "--is-ancestor",
                self.release_commit,
                advanced_main,
            ):
                raise subprocess.CalledProcessError(1, ["git", *arguments])
            return self._git(*arguments)

        with self.assertRaisesRegex(provenance.ProvenanceError, "ancestor"):
            self._verify(git=git)

    def test_tag_only_push_survives_real_remote_main_advance(self):
        def run(repository: Path | None, *arguments: str) -> str:
            command = ["git"]
            if repository is not None:
                command.extend(["-C", str(repository)])
            command.extend(arguments)
            return subprocess.run(
                command,
                check=True,
                capture_output=True,
                text=True,
            ).stdout.strip()

        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            origin = root / "origin.git"
            tagger = root / "tagger"
            advancer = root / "advancer"
            run(None, "init", "--bare", "--initial-branch=main", str(origin))
            run(None, "clone", str(origin), str(tagger))
            run(tagger, "config", "user.name", "Release Tagger")
            run(tagger, "config", "user.email", "tagger@example.invalid")
            (tagger / "release.txt").write_text("release\n", encoding="utf-8")
            run(tagger, "add", "release.txt")
            run(tagger, "commit", "-m", "release")
            release_commit = run(tagger, "rev-parse", "HEAD")
            run(tagger, "push", "-u", "origin", "main")

            run(None, "clone", str(origin), str(advancer))
            run(advancer, "config", "user.name", "Main Advancer")
            run(advancer, "config", "user.email", "advancer@example.invalid")
            (advancer / "next.txt").write_text("next\n", encoding="utf-8")
            run(advancer, "add", "next.txt")
            run(advancer, "commit", "-m", "advance main")
            advanced_main = run(advancer, "rev-parse", "HEAD")
            run(advancer, "push", "origin", "main")

            run(
                tagger,
                "tag",
                "-a",
                "v2.6.0",
                "-m",
                "Forward NetBox 2.6.0",
                release_commit,
            )
            run(tagger, "push", "origin", "refs/tags/v2.6.0")
            run(
                tagger,
                "fetch",
                "origin",
                "main:refs/remotes/origin/main",
            )

            with patch.object(provenance, "REPO_ROOT", tagger):
                self.assertEqual(
                    provenance._require_release_on_main_lineage(release_commit),
                    advanced_main,
                )
            self.assertEqual(
                run(
                    None,
                    "--git-dir",
                    str(origin),
                    "rev-parse",
                    "refs/tags/v2.6.0^{commit}",
                ),
                release_commit,
            )

    def test_rejects_untrusted_candidate_status(self):
        def github(path, token):
            payload = self._github(path, token)
            if path.startswith(f"commits/{self.evidence_candidate}/statuses?"):
                payload[0]["creator"]["login"] = "attacker"
            return payload

        with self.assertRaisesRegex(provenance.ProvenanceError, "authenticated"):
            self._verify(github=github)

    def test_rejects_same_bot_status_from_another_workflow(self):
        def github(path, token):
            payload = self._github(path, token)
            if path == "actions/runs/202":
                payload["workflow_id"] = 999
                payload["path"] = ".github/workflows/forged.yml"
            return payload

        with self.assertRaisesRegex(provenance.ProvenanceError, "trusted run"):
            self._verify(github=github)

    def test_rejects_status_run_for_another_pull_request(self):
        def github(path, token):
            payload = self._github(path, token)
            if path == "actions/runs/202":
                payload["pull_requests"][0]["number"] = 999
            return payload

        with self.assertRaisesRegex(provenance.ProvenanceError, "exact pull"):
            self._verify(github=github)

    def test_rejects_wrong_workflow_identity(self):
        def github(path, token):
            payload = self._github(path, token)
            if path.startswith("actions/workflows/1/runs?"):
                payload["workflow_runs"][0]["path"] = ".github/workflows/fake.yml"
            return payload

        with self.assertRaisesRegex(provenance.ProvenanceError, "no exact"):
            self._verify(github=github)

    def test_rejects_runtime_changes_in_security_bootstrap(self):
        def git(*arguments):
            result = self._git(*arguments)
            if arguments == (
                "diff",
                "--name-only",
                provenance.PRIOR_POST_RELEASE_DOC_COMMIT,
                self.anchor_commit,
            ):
                return "\n".join(
                    (*provenance.BOOTSTRAP_REQUIRED_FILES, "forward_netbox/models.py")
                )
            return result

        with self.assertRaisesRegex(provenance.ProvenanceError, "runtime changes"):
            self._verify(git=git)

    def test_rejects_non_plan_evidence_commit(self):
        def git(*arguments):
            result = self._git(*arguments)
            if arguments == (
                "diff",
                "--name-only",
                self.production_commit,
                self.release_commit,
            ):
                return "forward_netbox/models.py"
            return result

        with self.assertRaisesRegex(provenance.ProvenanceError, "unexpected path"):
            self._verify(git=git)


class GitHubReleaseControlsTest(unittest.TestCase):

    @staticmethod
    def _ruleset(name, target, pattern, rules, bypass):
        return {
            "name": name,
            "target": target,
            "source_type": "Repository",
            "source": provenance.GITHUB_REPOSITORY,
            "enforcement": "active",
            "conditions": {"ref_name": {"include": [pattern], "exclude": []}},
            "rules": rules,
            "bypass_actors": bypass,
        }

    def _payloads(self):
        statuses = [
            {"context": context, "integration_id": integration_id}
            for context, integration_id in provenance.BASE_REQUIRED_STATUS_CHECKS
        ]
        statuses.append(
            {
                "context": provenance.TRUSTED_STATUS_CONTEXT,
                "integration_id": provenance.GITHUB_ACTIONS_APP_ID,
            }
        )
        main = self._ruleset(
            provenance.MAIN_RULESET_NAME,
            "branch",
            "refs/heads/main",
            [
                {"type": "deletion"},
                {"type": "non_fast_forward"},
                {"type": "required_linear_history"},
                {
                    "type": "pull_request",
                    "parameters": {
                        "required_approving_review_count": 0,
                        "dismiss_stale_reviews_on_push": False,
                        "require_code_owner_review": False,
                        "require_last_push_approval": False,
                        "required_review_thread_resolution": True,
                        "allowed_merge_methods": ["squash"],
                    },
                },
                {
                    "type": "required_status_checks",
                    "parameters": {
                        "strict_required_status_checks_policy": True,
                        "do_not_enforce_on_create": False,
                        "required_status_checks": statuses,
                    },
                },
            ],
            [],
        )
        rulesets = {
            provenance.MAIN_RULESET_NAME: main,
            provenance.VERSION_TAG_INTEGRITY_RULESET: self._ruleset(
                provenance.VERSION_TAG_INTEGRITY_RULESET,
                "tag",
                "refs/tags/v*",
                [{"type": "deletion"}, {"type": "non_fast_forward"}],
                [],
            ),
        }
        for ruleset_id, ruleset in enumerate(rulesets.values(), 1):
            ruleset["id"] = ruleset_id
        environment = {
            "can_admins_bypass": False,
            "deployment_branch_policy": {
                "protected_branches": False,
                "custom_branch_policies": True,
            },
            "protection_rules": [],
        }
        return {
            "repository": {
                "allow_auto_merge": True,
                "allow_merge_commit": False,
                "allow_squash_merge": True,
                "delete_branch_on_merge": True,
            },
            "actions": {"enabled": True, "sha_pinning_required": True},
            "rulesets": rulesets,
            "environment": environment,
        }

    def _github(self, payloads):
        def github(path, _token):
            endpoint, page, _query = ReleaseProvenanceTest._path_parts(path)
            if endpoint == "":
                return copy.deepcopy(payloads["repository"])
            if endpoint == "actions/permissions":
                return copy.deepcopy(payloads["actions"])
            if endpoint == "rulesets":
                if page > 1:
                    return []
                return [
                    {
                        "id": ruleset["id"],
                        "name": name,
                        "source_type": "Repository",
                        "source": provenance.GITHUB_REPOSITORY,
                    }
                    for name, ruleset in payloads["rulesets"].items()
                ]
            if endpoint.startswith("rulesets/"):
                ruleset_id = int(endpoint.split("/")[1])
                return copy.deepcopy(
                    next(
                        ruleset
                        for ruleset in payloads["rulesets"].values()
                        if ruleset["id"] == ruleset_id
                    )
                )
            if endpoint.startswith("environments/") and endpoint.endswith(
                "/deployment-branch-policies"
            ):
                return {
                    "total_count": 1,
                    "branch_policies": [{"name": "v*", "type": "tag"}],
                }
            if endpoint.startswith("environments/"):
                name = endpoint.split("/")[1]
                return {"name": name, **copy.deepcopy(payloads["environment"])}
            raise AssertionError(path)

        return github

    def _verify(self, payloads=None):
        current = payloads or self._payloads()
        with patch.object(
            provenance,
            "_github_json",
            side_effect=self._github(current),
        ):
            return provenance.verify_github_release_controls("token")

    def test_accepts_complete_live_release_controls(self):
        result = self._verify()

        self.assertEqual(result["main_ruleset"], provenance.MAIN_RULESET_NAME)
        self.assertIn(provenance.TRUSTED_STATUS_CONTEXT, result["required_statuses"])

    def test_rejects_missing_trusted_status_for_release(self):
        payloads = self._payloads()
        main = payloads["rulesets"][provenance.MAIN_RULESET_NAME]
        statuses = next(
            rule for rule in main["rules"] if rule["type"] == "required_status_checks"
        )["parameters"]["required_status_checks"]
        statuses[:] = [
            status
            for status in statuses
            if status["context"] != provenance.TRUSTED_STATUS_CONTEXT
        ]

        with self.assertRaisesRegex(provenance.ProvenanceError, "authenticated"):
            self._verify(payloads)

    def test_rejects_environment_admin_bypass(self):
        payloads = self._payloads()
        payloads["environment"]["can_admins_bypass"] = True

        with self.assertRaisesRegex(provenance.ProvenanceError, "administrator"):
            self._verify(payloads)

    def test_rejects_environment_approval_gate(self):
        payloads = self._payloads()
        payloads["environment"]["protection_rules"] = [{"type": "required_reviewers"}]

        with self.assertRaisesRegex(provenance.ProvenanceError, "approval gate"):
            self._verify(payloads)

    def test_rejects_missing_version_tag_integrity_ruleset(self):
        payloads = self._payloads()
        del payloads["rulesets"][provenance.VERSION_TAG_INTEGRITY_RULESET]

        with self.assertRaisesRegex(provenance.ProvenanceError, "not unique"):
            self._verify(payloads)

    def test_rejects_retired_version_tag_creation_ruleset(self):
        payloads = self._payloads()
        ruleset = self._ruleset(
            provenance.RETIRED_VERSION_TAG_CREATION_RULESET,
            "tag",
            "refs/tags/v*",
            [{"type": "creation"}],
            [],
        )
        ruleset["id"] = 99
        payloads["rulesets"][provenance.RETIRED_VERSION_TAG_CREATION_RULESET] = ruleset

        with self.assertRaisesRegex(provenance.ProvenanceError, "remains active"):
            self._verify(payloads)


if __name__ == "__main__":
    unittest.main()
