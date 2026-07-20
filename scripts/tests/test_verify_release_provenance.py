from __future__ import annotations

import importlib.util
import unittest
from pathlib import Path
from unittest.mock import patch


_SPEC = importlib.util.spec_from_file_location(
    "release_provenance",
    Path(__file__).resolve().parents[1] / "verify_release_provenance.py",
)
provenance = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(provenance)


class ReleaseProvenanceTest(unittest.TestCase):
    release_commit = "a" * 40
    production_commit = "b" * 40
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
        }
        return responses[arguments]

    def _github(self, path, _token):
        if path == f"commits/{self.production_commit}":
            return {
                "commit": {"verification": {"verified": True}},
                "parents": [{"sha": "e" * 40}],
            }
        if path == f"commits/{self.release_commit}":
            return {
                "commit": {"verification": {"verified": True}},
                "parents": [{"sha": self.production_commit}],
            }
        if path == f"commits/{self.production_commit}/pulls?per_page=100":
            return [
                {
                    "number": 10,
                    "merged_at": "2026-07-20T10:00:00Z",
                    "base": {"ref": "main"},
                    "merge_commit_sha": self.production_commit,
                }
            ]
        if path == f"commits/{self.release_commit}/pulls?per_page=100":
            return [
                {
                    "number": 11,
                    "merged_at": "2026-07-20T12:00:00Z",
                    "base": {"ref": "main"},
                    "merge_commit_sha": self.release_commit,
                }
            ]
        if path == "pulls/10":
            return {
                "number": 10,
                "merged_at": "2026-07-20T10:00:00Z",
                "head": {"sha": self.production_candidate},
            }
        if path == "pulls/11":
            return {
                "number": 11,
                "merged_at": "2026-07-20T12:00:00Z",
                "head": {"sha": self.evidence_candidate},
            }
        if path == "pulls/10/reviews?per_page=100":
            return [
                {
                    "id": 1,
                    "state": "APPROVED",
                    "user": {"login": "brandonheller"},
                    "commit_id": self.production_candidate,
                    "submitted_at": "2026-07-20T09:00:00Z",
                }
            ]
        if path == "pulls/11/reviews?per_page=100":
            return [
                {
                    "id": 2,
                    "state": "APPROVED",
                    "user": {"login": "brandonheller"},
                    "commit_id": self.evidence_candidate,
                    "submitted_at": "2026-07-20T11:00:00Z",
                }
            ]
        if path in {
            f"commits/{self.production_candidate}/status?per_page=100",
            f"commits/{self.evidence_candidate}/status?per_page=100",
        }:
            return {
                "statuses": [
                    {
                        "id": 4,
                        "context": provenance.TRUSTED_STATUS_CONTEXT,
                        "state": "success",
                        "creator": {"login": provenance.TRUSTED_STATUS_CREATOR},
                    }
                ]
            }
        if path == "actions/workflows/.github%2Fworkflows%2Fci.yml":
            return {
                "id": 1,
                "path": ".github/workflows/ci.yml",
                "state": "active",
            }
        if path == "actions/workflows/.github%2Fworkflows%2Fcodeql.yml":
            return {
                "id": 2,
                "path": ".github/workflows/codeql.yml",
                "state": "active",
            }
        if path.startswith("actions/workflows/") and "/runs?" in path:
            workflow_id = int(path.split("/")[2])
            commit = (
                self.production_commit
                if f"head_sha={self.production_commit}" in path
                else self.release_commit
            )
            workflow_path = provenance.REQUIRED_WORKFLOWS[workflow_id - 1]
            return {
                "workflow_runs": [
                    {
                        "id": 10 + workflow_id,
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

    def test_accepts_two_reviewed_main_pull_requests(self):
        with patch.object(provenance, "_git_capture", side_effect=self._git), patch.object(
            provenance, "_github_json", side_effect=self._github
        ):
            result = provenance.verify_release_provenance(
                "v2.6.0", "brandonheller", "token"
            )

        self.assertEqual(result["release_commit"], self.release_commit)
        self.assertEqual(result["production_commit"], self.production_commit)

    def test_rejects_stale_review(self):
        def github(path, token):
            payload = self._github(path, token)
            if path == "pulls/11/reviews?per_page=100":
                payload[0]["commit_id"] = "f" * 40
            return payload

        with patch.object(provenance, "_git_capture", side_effect=self._git), patch.object(
            provenance, "_github_json", side_effect=github
        ), self.assertRaisesRegex(provenance.ProvenanceError, "final SHA"):
            provenance.verify_release_provenance(
                "v2.6.0", "brandonheller", "token"
            )

    def test_rejects_untrusted_candidate_status(self):
        def github(path, token):
            payload = self._github(path, token)
            if path == f"commits/{self.evidence_candidate}/status?per_page=100":
                payload["statuses"][0]["creator"]["login"] = "attacker"
            return payload

        with patch.object(provenance, "_git_capture", side_effect=self._git), patch.object(
            provenance, "_github_json", side_effect=github
        ), self.assertRaisesRegex(provenance.ProvenanceError, "authenticated"):
            provenance.verify_release_provenance(
                "v2.6.0", "brandonheller", "token"
            )

    def test_rejects_wrong_workflow_identity(self):
        def github(path, token):
            payload = self._github(path, token)
            if path.startswith("actions/workflows/1/runs?"):
                payload["workflow_runs"][0]["path"] = ".github/workflows/fake.yml"
            return payload

        with patch.object(provenance, "_git_capture", side_effect=self._git), patch.object(
            provenance, "_github_json", side_effect=github
        ), self.assertRaisesRegex(provenance.ProvenanceError, "no exact"):
            provenance.verify_release_provenance(
                "v2.6.0", "brandonheller", "token"
            )

    def test_rejects_non_plan_evidence_commit(self):
        def git(*arguments):
            result = self._git(*arguments)
            if arguments[:2] == ("diff", "--name-only"):
                return "forward_netbox/models.py"
            return result

        with patch.object(provenance, "_git_capture", side_effect=git), patch.object(
            provenance, "_github_json", side_effect=self._github
        ), self.assertRaisesRegex(provenance.ProvenanceError, "unexpected path"):
            provenance.verify_release_provenance(
                "v2.6.0", "brandonheller", "token"
            )


if __name__ == "__main__":
    unittest.main()
