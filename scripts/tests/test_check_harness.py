from __future__ import annotations

import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import scripts.check_harness as check_harness


class CheckHarnessPlanLifecycleTest(unittest.TestCase):
    def test_high_risk_change_without_plan_fails(self):
        failures = []

        with patch.object(
            check_harness,
            "_changed_files",
            return_value=["forward_netbox/utilities/sync.py"],
        ):
            check_harness._check_plan_lifecycle(failures)

        self.assertEqual(len(failures), 1)
        self.assertIn("high-risk changes require a plan file", failures[0])
        self.assertIn("forward_netbox/utilities/sync.py", failures[0])

    def test_high_risk_change_with_plan_passes(self):
        failures = []

        with patch.object(
            check_harness,
            "_changed_files",
            return_value=[
                "forward_netbox/utilities/sync.py",
                "docs/03_Plans/completed/change-record.md",
            ],
        ):
            check_harness._check_plan_lifecycle(failures)

        self.assertEqual(failures, [])

    def test_low_risk_change_without_plan_passes(self):
        failures = []

        with patch.object(
            check_harness,
            "_changed_files",
            return_value=["docs/01_User_Guide/configuration.md"],
        ):
            check_harness._check_plan_lifecycle(failures)

        self.assertEqual(failures, [])


class CheckHarnessPlanDirectoryTest(unittest.TestCase):
    def test_plan_directory_requires_all_standard_headings(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            repo_root = Path(temp_dir)
            plan_dir = repo_root / "docs/03_Plans/active"
            plan_dir.mkdir(parents=True)
            (plan_dir / "incomplete.md").write_text(
                "# Incomplete Plan\n\n## Goal\n\nDo the thing.\n",
                encoding="utf-8",
            )
            failures = []

            with patch.object(check_harness, "REPO_ROOT", repo_root):
                check_harness._check_plan_directory(
                    failures,
                    "docs/03_Plans/active",
                )

        self.assertGreaterEqual(len(failures), 1)
        self.assertIn("must include plan heading", failures[0])

    def test_plan_directory_ignores_readme(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            repo_root = Path(temp_dir)
            plan_dir = repo_root / "docs/03_Plans/active"
            plan_dir.mkdir(parents=True)
            (plan_dir / "README.md").write_text("short readme\n", encoding="utf-8")
            failures = []

            with patch.object(check_harness, "REPO_ROOT", repo_root):
                check_harness._check_plan_directory(
                    failures,
                    "docs/03_Plans/active",
                )

        self.assertEqual(failures, [])


class CheckHarnessGitHubDiffTest(unittest.TestCase):
    def test_github_changed_files_uses_commit_file_lists(self):
        event = {
            "commits": [
                {
                    "added": ["docs/new.md"],
                    "modified": ["scripts/check_harness.py"],
                    "removed": ["docs/old.md"],
                }
            ]
        }

        with tempfile.NamedTemporaryFile("w", encoding="utf-8") as event_file:
            json.dump(event, event_file)
            event_file.flush()

            with patch.dict(os.environ, {"GITHUB_EVENT_PATH": event_file.name}):
                changed_files = check_harness._github_changed_files()

        self.assertEqual(
            changed_files,
            ["docs/new.md", "docs/old.md", "scripts/check_harness.py"],
        )

    def test_github_changed_files_uses_push_sha_diff_when_available(self):
        event = {
            "before": "abc123",
            "after": "def456",
            "commits": [{"modified": ["fallback.py"]}],
        }

        with tempfile.NamedTemporaryFile("w", encoding="utf-8") as event_file:
            json.dump(event, event_file)
            event_file.flush()

            with (
                patch.dict(os.environ, {"GITHUB_EVENT_PATH": event_file.name}),
                patch.object(
                    check_harness,
                    "_git_names",
                    return_value=["scripts/check_harness.py"],
                ) as git_names,
            ):
                changed_files = check_harness._github_changed_files()

        self.assertEqual(changed_files, ["scripts/check_harness.py"])
        git_names.assert_called_once_with("diff", "--name-only", "abc123", "def456")

    def test_github_changed_files_skips_zero_before_sha(self):
        event = {
            "before": "0000000000000000000000000000000000000000",
            "after": "def456",
            "commits": [{"modified": ["scripts/check_harness.py"]}],
        }

        with tempfile.NamedTemporaryFile("w", encoding="utf-8") as event_file:
            json.dump(event, event_file)
            event_file.flush()

            with (
                patch.dict(os.environ, {"GITHUB_EVENT_PATH": event_file.name}),
                patch.object(check_harness, "_git_names") as git_names,
            ):
                changed_files = check_harness._github_changed_files()

        self.assertEqual(changed_files, ["scripts/check_harness.py"])
        git_names.assert_not_called()


if __name__ == "__main__":
    unittest.main()
