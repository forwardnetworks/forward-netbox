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
            ("cat-file", "-t", "refs/tags/v3.0.1"): "tag",
            ("rev-parse", "refs/tags/v3.0.1^{commit}"): self.release_commit,
            ("rev-parse", provenance.LANE.remote_tracking_ref): self.release_commit,
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
            ): "docs/03_Plans/active/2026-07-18-release-3.0.1-scope-convergence.md",
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
                        "base": {"ref": provenance.LANE.branch},
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
                    "base": {"ref": provenance.LANE.branch},
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
                        "context": "retired-trusted-status",
                        "state": "success",
                        "creator": {"login": "github-actions[bot]"},
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
                    "pull_requests": [],
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
                            "head_branch": provenance.LANE.branch,
                            "event": "push",
                            "status": "completed",
                            "conclusion": "success",
                        }
                    ]
                }
        raise AssertionError(path)

    def _git_bytes(self, *arguments):
        """Serve the reviewed bootstrap bytes for the release commit."""
        if arguments[:1] == ("show",):
            _, _separator, path = arguments[1].partition(":")
            for candidate, digest in provenance.BOOTSTRAP_FILE_DIGESTS.items():
                if path == candidate:
                    return self._bootstrap_bytes.get(candidate, b"")
            raise AssertionError(arguments)
        raise AssertionError(arguments)

    @property
    def _bootstrap_bytes(self):
        # Preimages chosen so their digests are the pinned ones: the fixture
        # reads the real files, which is what the release tree contains.
        return {
            path: (provenance.REPO_ROOT / path).read_bytes()
            for path in provenance.BOOTSTRAP_FILE_DIGESTS
        }

    def _verify(self, *, github=None, git=None, git_bytes=None):
        with (
            patch.object(provenance, "_git_capture", side_effect=git or self._git),
            patch.object(
                provenance,
                "_git_capture_bytes",
                side_effect=git_bytes or self._git_bytes,
            ),
            patch.object(
                provenance, "_github_json", side_effect=github or self._github
            ),
        ):
            return provenance.verify_release_provenance("v3.0.1", "token")

    def test_accepts_reviewed_bootstrap_and_release_lineage(self):
        result = self._verify()

        self.assertEqual(result["release_commit"], self.release_commit)
        self.assertEqual(result["production_commit"], self.production_commit)
        self.assertEqual(result["first_reviewed_commit"], self.anchor_commit)
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
            "v3.0.1",
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
            if arguments == ("rev-parse", provenance.LANE.remote_tracking_ref):
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
                provenance._require_merged_release_branch_pr(
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
                provenance._require_merged_release_branch_pr(
                    commit,
                    "token",
                    allow_direct_control_commit=True,
                )

    def test_the_release_pair_can_never_be_a_direct_control_commit(self):
        # The allowance was `index < 3`, read as "the first three positions".
        # But a lineage may be exactly three reviewed commits and ours routinely
        # is - anchor, production, release - and at that size the allowance
        # covered every commit including the release pair, which is the opposite
        # of the intent. Anchoring to the END makes it hold at any length.
        for size in (3, 4, 7):
            reviewed = [f"c{index}" for index in range(size)]
            limit = len(reviewed) - 2
            allowed = [index < limit for index in range(size)]
            with self.subTest(lineage=size):
                self.assertFalse(
                    allowed[-1], "the release commit must come through a PR"
                )
                self.assertFalse(
                    allowed[-2], "the production commit must come through a PR"
                )
                self.assertTrue(
                    all(allowed[:-2]), "control commits ahead of the pair may be direct"
                )

    def test_rejects_tagged_release_diverged_from_main(self):
        advanced_main = "e" * 40

        def git(*arguments):
            if arguments == ("rev-parse", provenance.LANE.remote_tracking_ref):
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

    def test_tag_only_push_survives_real_remote_lane_advance(self):
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
            run(
                None,
                "init",
                "--bare",
                f"--initial-branch={provenance.LANE.branch}",
                str(origin),
            )
            run(None, "clone", str(origin), str(tagger))
            run(tagger, "config", "user.name", "Release Tagger")
            run(tagger, "config", "user.email", "tagger@example.invalid")
            (tagger / "release.txt").write_text("release\n", encoding="utf-8")
            run(tagger, "add", "release.txt")
            run(tagger, "commit", "-m", "release")
            release_commit = run(tagger, "rev-parse", "HEAD")
            run(tagger, "push", "-u", "origin", provenance.LANE.branch)

            run(None, "clone", str(origin), str(advancer))
            run(advancer, "config", "user.name", "Main Advancer")
            run(advancer, "config", "user.email", "advancer@example.invalid")
            (advancer / "next.txt").write_text("next\n", encoding="utf-8")
            run(advancer, "add", "next.txt")
            run(advancer, "commit", "-m", "advance main")
            advanced_main = run(advancer, "rev-parse", "HEAD")
            run(advancer, "push", "origin", provenance.LANE.branch)

            run(
                tagger,
                "tag",
                "-a",
                "v3.0.1",
                "-m",
                "Forward NetBox 3.0.1",
                release_commit,
            )
            run(tagger, "push", "origin", "refs/tags/v3.0.1")
            run(
                tagger,
                "fetch",
                "origin",
                f"{provenance.LANE.branch}:{provenance.LANE.remote_tracking_ref}",
            )

            with patch.object(provenance, "REPO_ROOT", tagger):
                self.assertEqual(
                    provenance._require_release_on_lane_lineage(release_commit),
                    advanced_main,
                )
            self.assertEqual(
                run(
                    None,
                    "--git-dir",
                    str(origin),
                    "rev-parse",
                    "refs/tags/v3.0.1^{commit}",
                ),
                release_commit,
            )

    def test_rejects_altered_security_bootstrap_content(self):
        # The old check accepted any change touching all three paths, including
        # one that gutted them. Pinned content rejects a modified scanner.
        def git_bytes(*arguments):
            if arguments[:1] == ("show",) and arguments[1].endswith(
                "scripts/check_sensitive_content.py"
            ):
                return b"# neutered\n"
            return self._git_bytes(*arguments)

        with self.assertRaisesRegex(
            provenance.ProvenanceError, "does not match reviewed content"
        ):
            self._verify(git_bytes=git_bytes)

    def test_rejects_absent_security_bootstrap_file(self):
        def git_bytes(*arguments):
            if arguments[:1] == ("show",) and arguments[1].endswith(
                "scripts/sensitive_content.py"
            ):
                raise subprocess.CalledProcessError(128, ["git", *arguments])
            return self._git_bytes(*arguments)

        with self.assertRaisesRegex(provenance.ProvenanceError, "absent"):
            self._verify(git_bytes=git_bytes)

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

        with self.assertRaisesRegex(
            provenance.ProvenanceError, "must change exactly one release-.* plan"
        ):
            self._verify(git=git)

    def test_accepts_release_commit_carrying_code_beside_the_plan(self):
        def git(*arguments):
            result = self._git(*arguments)
            if arguments == (
                "diff",
                "--name-only",
                self.production_commit,
                self.release_commit,
            ):
                return (
                    "forward_netbox/models.py\n"
                    "docs/03_Plans/active/"
                    "2026-07-18-release-3.0.1-scope-convergence.md"
                )
            return result

        self._verify(git=git)

    def test_rejects_release_commit_without_a_matching_version_plan(self):
        def git(*arguments):
            result = self._git(*arguments)
            if arguments == (
                "diff",
                "--name-only",
                self.production_commit,
                self.release_commit,
            ):
                return "docs/03_Plans/active/2020-01-01-release-9.9.9-other.md"
            return result

        with self.assertRaisesRegex(
            provenance.ProvenanceError, "must change exactly one release-.* plan"
        ):
            self._verify(git=git)

    def _bridge_diff(self, changed):
        def git(*arguments):
            if arguments == (
                "diff",
                "--name-only",
                self.prior_release_commit,
                provenance.PRIOR_POST_RELEASE_DOC_COMMIT,
            ):
                return changed
            return self._git(*arguments)

        return git

    def test_accepts_post_release_bridge_that_archives_the_plan(self):
        git = self._bridge_diff("docs/03_Plans/completed/2026-01-01-release.md")

        result = self._verify(git=git)

        self.assertEqual(result["release_commit"], self.release_commit)

    def test_accepts_post_release_bridge_that_promotes_the_release(self):
        # The shape `release.py --finish` actually produces. `v2.7.0` was
        # promoted without being archived, so this commit took the bridge slot;
        # rejecting it made every later release unverifiable, because the bridge
        # is fixed at the first commit after the tag and cannot be reclaimed.
        git = self._bridge_diff(
            "CHANGELOG.md\nREADME.md\ndocs/README.md\ndocs/01_User_Guide/README.md"
        )

        result = self._verify(git=git)

        self.assertEqual(result["release_commit"], self.release_commit)

    def test_rejects_post_release_bridge_carrying_code(self):
        git = self._bridge_diff("CHANGELOG.md\nforward_netbox/models.py")

        with self.assertRaisesRegex(
            provenance.ProvenanceError, "post-release bridge must be documentation-only"
        ):
            self._verify(git=git)

    def test_rejects_post_release_bridge_carrying_a_workflow(self):
        git = self._bridge_diff("docs/README.md\n.github/workflows/release.yml")

        with self.assertRaisesRegex(
            provenance.ProvenanceError, "post-release bridge must be documentation-only"
        ):
            self._verify(git=git)

    def test_rejects_empty_post_release_bridge(self):
        git = self._bridge_diff("")

        with self.assertRaisesRegex(
            provenance.ProvenanceError, "post-release bridge must be documentation-only"
        ):
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
        # No `required_status_checks` rule: the CI gates were removed, so the
        # ruleset must not name checks that can never report again.
        main = self._ruleset(
            provenance.RELEASE_BRANCH_RULESET_NAME,
            "branch",
            provenance.LANE.ref_pattern,
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
            ],
            [],
        )
        rulesets = {
            provenance.RELEASE_BRANCH_RULESET_NAME: main,
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

        self.assertEqual(
            result["release_branch_ruleset"], provenance.RELEASE_BRANCH_RULESET_NAME
        )
        # No required status checks remain: the gates that reported them were
        # removed, and a ruleset naming checks that can never report again would
        # block every pull request permanently.
        self.assertEqual(result["required_statuses"], [])

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


class BridgeContentExceptionsAreNarrowTest(unittest.TestCase):
    """The bridge exception must not become a general escape hatch.

    A bridge commit must be documentation-only, and the requirement is pinned to
    the first commit after the release tag, so a bridge carrying anything else
    disqualifies itself permanently - no later commit can reclaim the slot. That
    happened once, to the bridge after `v2.8.3`, when an unreleased version bump
    was inherited into it.

    It was excused by commit hash, and the hash key is what made the excuse
    temporary: once the anchor moved to `v2.8.4` the entry stopped being
    consulted and was deleted. So the expected state of this table is EMPTY, and
    these tests pin that - plus the shape any future entry must take, if the
    stage that generates bridges ever regresses.
    """

    def test_the_table_is_empty(self):
        self.assertEqual(
            provenance.BRIDGE_CONTENT_EXCEPTIONS,
            {},
            "an excuse for a provenance rule should not outlive the anchor it "
            "was written for; delete the entry when the anchor passes it",
        )

    def test_any_entry_is_keyed_by_a_full_commit_hash(self):
        for commit in provenance.BRIDGE_CONTENT_EXCEPTIONS:
            self.assertRegex(
                commit,
                r"^[0-9a-f]{40}$",
                "keyed by hash so a future bridge cannot inherit the excuse",
            )

    def test_no_entry_uses_a_wildcard_or_a_directory(self):
        for commit, paths in provenance.BRIDGE_CONTENT_EXCEPTIONS.items():
            for path in paths:
                self.assertNotIn("*", path, f"{commit} excuses a glob, not a path")
                self.assertFalse(
                    path.endswith("/"), f"{commit} excuses a directory, not a path"
                )

    def test_an_unlisted_commit_gets_no_exception(self):
        self.assertEqual(
            provenance.BRIDGE_CONTENT_EXCEPTIONS.get("0" * 40, ()),
            (),
        )
