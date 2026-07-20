from __future__ import annotations

import importlib.util
import unittest
import urllib.parse
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
                f"refs/tags/{provenance.TRUSTED_ANCHOR_TAG}",
            ): "tag",
            (
                "rev-parse",
                f"refs/tags/{provenance.TRUSTED_ANCHOR_TAG}^{{commit}}",
            ): self.anchor_commit,
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
                self.anchor_commit,
            ): "",
            (
                "rev-list",
                "--first-parent",
                "--reverse",
                f"{self.prior_release_commit}..{self.anchor_commit}",
            ): f"{provenance.PRIOR_POST_RELEASE_DOC_COMMIT}\n{self.anchor_commit}",
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
                self.anchor_commit,
                self.release_commit,
                "--",
                *provenance.TRUSTED_RELEASE_FILES,
            ): "",
            (
                "merge-base",
                "--is-ancestor",
                self.anchor_commit,
                self.release_commit,
            ): "",
            (
                "rev-list",
                "--first-parent",
                "--reverse",
                f"{self.anchor_commit}..{self.release_commit}",
            ): f"{self.production_commit}\n{self.release_commit}",
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
            if endpoint == f"pulls/{number}/reviews":
                if page > 1:
                    return []
                return [
                    {
                        "id": number,
                        "state": "APPROVED",
                        "user": {"login": "brandonheller"},
                        "commit_id": candidate,
                        "submitted_at": f"2026-07-20T0{number - 2}:00:00Z",
                    }
                ]

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
        with patch.object(
            provenance, "_git_capture", side_effect=git or self._git
        ), patch.object(provenance, "_github_json", side_effect=github or self._github):
            return provenance.verify_release_provenance(
                "v2.6.0", "brandonheller", "token"
            )

    def test_accepts_reviewed_bootstrap_and_release_lineage(self):
        result = self._verify()

        self.assertEqual(result["release_commit"], self.release_commit)
        self.assertEqual(result["production_commit"], self.production_commit)
        self.assertEqual(result["trusted_anchor"], self.anchor_commit)
        self.assertEqual(
            result["reviewed_commits"],
            [self.anchor_commit, self.production_commit, self.release_commit],
        )

    def test_accepts_reviewed_anchor_candidate_before_tag_creation(self):
        with patch.object(
            provenance,
            "_git_capture",
            side_effect=self._git,
        ), patch.object(
            provenance,
            "_github_json",
            side_effect=self._github,
        ):
            result = provenance.verify_trusted_anchor_candidate(
                self.anchor_commit,
                "brandonheller",
                "token",
            )

        self.assertEqual(result["trusted_anchor"], self.anchor_commit)
        self.assertEqual(result["pull_request"], 9)

    def test_rejects_stale_review(self):
        def github(path, token):
            payload = self._github(path, token)
            if path.startswith("pulls/11/reviews?") and payload:
                payload[0]["commit_id"] = "f" * 40
            return payload

        with self.assertRaisesRegex(provenance.ProvenanceError, "final SHA"):
            self._verify(github=github)

    def test_reads_all_review_pages_before_accepting_latest_state(self):
        def github(path, token):
            endpoint, page, _query = self._path_parts(path)
            if endpoint == "pulls/11/reviews" and page == 1:
                return [
                    {
                        "id": review_id,
                        "state": "APPROVED",
                        "user": {"login": "brandonheller"},
                        "commit_id": self.evidence_candidate,
                        "submitted_at": "2026-07-20T11:00:00Z",
                    }
                    for review_id in range(1, 101)
                ]
            if endpoint == "pulls/11/reviews" and page == 2:
                return [
                    {
                        "id": 101,
                        "state": "CHANGES_REQUESTED",
                        "user": {"login": "brandonheller"},
                        "commit_id": self.evidence_candidate,
                        "submitted_at": "2026-07-20T11:30:00Z",
                    }
                ]
            return self._github(path, token)

        with self.assertRaisesRegex(provenance.ProvenanceError, "current approval"):
            self._verify(github=github)

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

    def test_rejects_trusted_controller_change_after_bootstrap(self):
        def git(*arguments):
            result = self._git(*arguments)
            if arguments[:5] == (
                "diff",
                "--name-only",
                self.anchor_commit,
                self.release_commit,
                "--",
            ):
                return ".github/workflows/release.yml"
            return result

        with self.assertRaisesRegex(provenance.ProvenanceError, "changed after"):
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


if __name__ == "__main__":
    unittest.main()
