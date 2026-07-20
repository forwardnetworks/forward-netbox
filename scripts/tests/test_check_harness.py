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


class CheckHarnessDevelopmentSecretBoundaryTest(unittest.TestCase):
    def _check(self, files, tracked):
        with tempfile.TemporaryDirectory() as temp_dir:
            repo_root = Path(temp_dir)
            for relative_path, content in files.items():
                path = repo_root / relative_path
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text(content, encoding="utf-8")
            failures = []

            def git_names(*args):
                if args == ("ls-files", "--cached"):
                    return list(tracked)
                if args == ("ls-files", "--deleted"):
                    return []
                return []

            with (
                patch.object(check_harness, "REPO_ROOT", repo_root),
                patch.object(check_harness, "_git_names", side_effect=git_names),
            ):
                check_harness._check_development_secret_boundary(failures)
        return failures

    def test_rejects_tracked_secret_file_and_assignment(self):
        failures = self._check(
            {
                "development/.env": "NETBOX_VER=v4.6.5\n",
                "development/env/netbox.env": "DB_PASSWORD=example\n",
            },
            ["development/.env", "development/env/netbox.env"],
        )

        self.assertEqual(len(failures), 2)
        self.assertTrue(any("must not be tracked" in failure for failure in failures))
        self.assertTrue(any("secret assignment" in failure for failure in failures))

    def test_generated_secret_compose_contract_passes(self):
        files = {
            "development/env/netbox.env": "DB_HOST=postgres\n",
            "development/env/postgres.env": "POSTGRES_DB=netbox\n",
            "development/docker-compose.yml": (
                "services:\n"
                "  netbox:\n"
                "    secrets: [api_token_pepper_1, db_password, redis_password, "
                "secret_key]\n"
                "  postgres:\n"
                "    environment:\n"
                "      POSTGRES_PASSWORD_FILE: /run/secrets/db_password\n"
                "  redis:\n"
                "    command: [sh, -ec, 'cat /run/secrets/redis_password']\n"
                "secrets:\n"
                "  api_token_pepper_1: {}\n"
                "  db_password: {}\n"
                "  redis_password: {}\n"
                "  secret_key: {}\n"
            ),
            ".github/workflows/ci.yml": (
                "steps:\n"
                "  - run: python scripts/generate_development_secrets.py\n"
                "  - run: docker compose --project-name forward-netbox build\n"
            ),
            ".dockerignore": "development/secrets\n",
        }
        failures = self._check(files, files)

        self.assertEqual(failures, [])


class CheckHarnessTrustedPrivateFetchTest(unittest.TestCase):
    def _check(self, workflow_text):
        with tempfile.TemporaryDirectory() as temp_dir:
            repo_root = Path(temp_dir)
            path = repo_root / ".github/workflows/trusted-sensitive-pr.yml"
            path.parent.mkdir(parents=True)
            path.write_text(workflow_text, encoding="utf-8")
            failures = []
            with patch.object(check_harness, "REPO_ROOT", repo_root):
                check_harness._check_trusted_private_fetch(failures)
        return failures

    def test_authenticated_exact_head_fetch_passes(self):
        failures = self._check(
            "GH_TOKEN: ${{ secrets.GITHUB_TOKEN }}\n"
            'extraheader="http.https://github.com/.extraheader"\n'
            'git -c "${extraheader}=AUTHORIZATION: basic ${auth_header}" fetch\n'
            'test "$(git rev-parse FETCH_HEAD)" = "${PR_HEAD_SHA}"\n'
        )

        self.assertEqual(failures, [])

    def test_unauthenticated_fetch_fails(self):
        failures = self._check(
            "git fetch https://github.com/example/repo.git\n"
            'test "$(git rev-parse FETCH_HEAD)" = "${PR_HEAD_SHA}"\n'
        )

        self.assertEqual(len(failures), 3)


if __name__ == "__main__":
    unittest.main()
