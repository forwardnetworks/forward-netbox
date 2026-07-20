from __future__ import annotations

import json
import os
import tempfile
import unittest
from datetime import date
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


class CheckHarnessKnowledgeTest(unittest.TestCase):
    def test_agents_entrypoint_rejects_monolithic_manual(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            repo_root = Path(temp_dir)
            (repo_root / "AGENTS.md").write_text(
                "\n".join(["instruction"] * 121),
                encoding="utf-8",
            )
            failures = []

            with patch.object(check_harness, "REPO_ROOT", repo_root):
                check_harness._check_agents_entrypoint(failures)

        self.assertEqual(len(failures), 1)
        self.assertIn("concise repository map", failures[0])

    def test_knowledge_freshness_accepts_recent_review(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            repo_root = Path(temp_dir)
            relative_path = "docs/alignment.md"
            path = repo_root / relative_path
            path.parent.mkdir(parents=True)
            path.write_text("Last reviewed: 2026-07-18\n", encoding="utf-8")
            failures = []

            with (
                patch.object(check_harness, "REPO_ROOT", repo_root),
                patch.object(
                    check_harness,
                    "KNOWLEDGE_FRESHNESS_DAYS",
                    {relative_path: 90},
                ),
            ):
                check_harness._check_knowledge_freshness(
                    failures,
                    today=date(2026, 7, 18),
                )

        self.assertEqual(failures, [])

    def test_knowledge_freshness_rejects_missing_or_stale_review(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            repo_root = Path(temp_dir)
            docs_dir = repo_root / "docs"
            docs_dir.mkdir()
            (docs_dir / "missing.md").write_text("# Missing\n", encoding="utf-8")
            (docs_dir / "stale.md").write_text(
                "Last reviewed: 2026-01-01\n",
                encoding="utf-8",
            )
            failures = []

            with (
                patch.object(check_harness, "REPO_ROOT", repo_root),
                patch.object(
                    check_harness,
                    "KNOWLEDGE_FRESHNESS_DAYS",
                    {"docs/missing.md": 90, "docs/stale.md": 90},
                ),
            ):
                check_harness._check_knowledge_freshness(
                    failures,
                    today=date(2026, 7, 18),
                )

        self.assertEqual(len(failures), 2)
        self.assertIn("Last reviewed", failures[0])
        self.assertIn("review is stale", failures[1])

    def test_knowledge_freshness_rejects_invalid_calendar_date(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            repo_root = Path(temp_dir)
            relative_path = "docs/alignment.md"
            path = repo_root / relative_path
            path.parent.mkdir(parents=True)
            path.write_text("Last reviewed: 2026-99-99\n", encoding="utf-8")
            failures = []

            with (
                patch.object(check_harness, "REPO_ROOT", repo_root),
                patch.object(
                    check_harness,
                    "KNOWLEDGE_FRESHNESS_DAYS",
                    {relative_path: 90},
                ),
            ):
                check_harness._check_knowledge_freshness(
                    failures,
                    today=date(2026, 7, 18),
                )

        self.assertEqual(len(failures), 1)
        self.assertIn("invalid review date", failures[0])


class CheckHarnessRuntimeRetirementTest(unittest.TestCase):
    def test_retired_runtime_path_fails(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            repo_root = Path(temp_dir)
            runtime_file = repo_root / "forward_netbox/utilities/sync.py"
            runtime_file.parent.mkdir(parents=True)
            runtime_file.write_text(
                'state = payload.get("_execution_progress")\n',
                encoding="utf-8",
            )
            failures = []

            with patch.object(check_harness, "REPO_ROOT", repo_root):
                check_harness._check_retired_runtime_paths(failures)

        self.assertEqual(len(failures), 1)
        self.assertIn("forward_netbox/utilities/sync.py:1", failures[0])
        self.assertIn("retired persisted execution progress", failures[0])

    def test_migration_cleanup_and_tests_are_excluded(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            repo_root = Path(temp_dir)
            for relative_path in (
                "forward_netbox/migrations/0042_cleanup.py",
                "forward_netbox/tests/test_cleanup.py",
            ):
                path = repo_root / relative_path
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text(
                    'parameters.pop("device_tag_include", None)\n',
                    encoding="utf-8",
                )
            runtime_file = repo_root / "forward_netbox/models.py"
            runtime_file.write_text(
                'parameters.get("device_tag_include_tags", [])\n',
                encoding="utf-8",
            )
            failures = []

            with patch.object(check_harness, "REPO_ROOT", repo_root):
                check_harness._check_retired_runtime_paths(failures)

        self.assertEqual(failures, [])

    def test_retired_paths_fail_in_queries_package_and_workflow_surfaces(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            repo_root = Path(temp_dir)
            files = {
                "forward_netbox/queries/retired.nqe": "column_filters = []\n",
                "pyproject.toml": 'package_names = ["netbox-routing"]\n',
                ".github/workflows/ci.yml": "JOBRESULT_RETENTION: 30\n",
            }
            for relative_path, content in files.items():
                path = repo_root / relative_path
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text(content, encoding="utf-8")
            failures = []

            with patch.object(check_harness, "REPO_ROOT", repo_root):
                check_harness._check_retired_runtime_paths(failures)

        self.assertEqual(len(failures), 3)
        self.assertTrue(any("retired.nqe:1" in failure for failure in failures))
        self.assertTrue(any("pyproject.toml:1" in failure for failure in failures))
        self.assertTrue(any("ci.yml:1" in failure for failure in failures))


class CheckHarnessComposeHealthProbeTest(unittest.TestCase):
    def _check(self, compose_text):
        with tempfile.TemporaryDirectory() as temp_dir:
            repo_root = Path(temp_dir)
            path = repo_root / "development/docker-compose.yml"
            path.parent.mkdir(parents=True)
            path.write_text(compose_text, encoding="utf-8")
            failures = []
            with patch.object(check_harness, "REPO_ROOT", repo_root):
                check_harness._check_compose_health_probe(failures)
        return failures

    def test_exact_login_probe_passes(self):
        failures = self._check(
            "services:\n"
            "  netbox:\n"
            "    healthcheck:\n"
            "      test: 'curl -f http://localhost:8000/login/ || exit 1'\n"
        )

        self.assertEqual(failures, [])

    def test_comment_cannot_mask_incorrect_probe(self):
        failures = self._check(
            "# curl -f http://localhost:8000/login/ || exit 1\n"
            "services:\n"
            "  netbox:\n"
            "    healthcheck:\n"
            "      test: 'curl -f http://localhost:8000/api/ || exit 1'\n"
        )

        self.assertEqual(len(failures), 1)
        self.assertIn("services.netbox.healthcheck.test must equal", failures[0])

    def test_missing_probe_fails(self):
        failures = self._check("services:\n  netbox: {}\n")

        self.assertEqual(len(failures), 1)
        self.assertIn("no parseable netbox health probe", failures[0])


class CheckHarnessWorkerAutoreloadTest(unittest.TestCase):
    def _check(self, compose_text):
        with tempfile.TemporaryDirectory() as temp_dir:
            repo_root = Path(temp_dir)
            path = repo_root / "development/docker-compose.yml"
            path.parent.mkdir(parents=True)
            path.write_text(compose_text, encoding="utf-8")
            failures = []
            with patch.object(check_harness, "REPO_ROOT", repo_root):
                check_harness._check_worker_autoreload_contract(failures)
        return failures

    def test_container_runtime_expansion_passes(self):
        failures = self._check(
            "services:\n"
            "  netbox-worker:\n"
            "    environment:\n"
            "      FORWARD_NETBOX_WORKER_AUTORELOAD: "
            '"${FORWARD_NETBOX_WORKER_AUTORELOAD:-1}"\n'
            "    command:\n"
            "      - sh\n"
            "      - -lc\n"
            '      - \'if [ "$${FORWARD_NETBOX_WORKER_AUTORELOAD:-1}" = "1" ]; '
            "then true; fi'\n"
        )

        self.assertEqual(failures, [])

    def test_host_expansion_fails(self):
        failures = self._check(
            "services:\n"
            "  netbox-worker:\n"
            "    environment:\n"
            "      FORWARD_NETBOX_WORKER_AUTORELOAD: '0'\n"
            "    command:\n"
            "      - sh\n"
            "      - -lc\n"
            '      - \'if [ "${FORWARD_NETBOX_WORKER_AUTORELOAD:-1}" = "1" ]; '
            "then true; fi'\n"
        )

        self.assertEqual(len(failures), 2)


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
                "development/env/netbox.env": (
                    "DB_PASSWORD=example\nRQ_DEFAULT_TIMEOUT=7200\n"
                ),
            },
            ["development/.env", "development/env/netbox.env"],
        )

        self.assertEqual(len(failures), 2)
        self.assertTrue(any("must not be tracked" in failure for failure in failures))
        self.assertTrue(any("secret assignment" in failure for failure in failures))

    def test_generated_secret_compose_contract_passes(self):
        files = {
            "development/env/netbox.env": (
                "DB_HOST=postgres\nRQ_DEFAULT_TIMEOUT=7200\n"
            ),
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

    def test_rejects_short_or_duplicate_worker_timeout(self):
        files = {
            "development/env/netbox.env": (
                "RQ_DEFAULT_TIMEOUT=300\nRQ_DEFAULT_TIMEOUT=7200\n"
            ),
        }

        failures = self._check(files, files)

        self.assertEqual(len(failures), 1)
        self.assertIn("RQ_DEFAULT_TIMEOUT=7200", failures[0])

    def test_missing_worker_environment_is_rejected(self):
        failures = self._check({}, [])

        self.assertEqual(len(failures), 1)
        self.assertIn("development/env/netbox.env must exist", failures[0])


class CheckHarnessDevelopmentLoggingBoundaryTest(unittest.TestCase):
    def _check(self, files):
        with tempfile.TemporaryDirectory() as temp_dir:
            repo_root = Path(temp_dir)
            for relative_path, content in files.items():
                path = repo_root / relative_path
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text(content, encoding="utf-8")
            failures = []
            with patch.object(check_harness, "REPO_ROOT", repo_root):
                check_harness._check_development_logging_boundary(failures)
        return failures

    def test_info_logging_contract_passes(self):
        failures = self._check(
            {
                "development/configuration/logging.py": (
                    "from os import environ\n"
                    'LOGLEVEL = environ.get("LOGLEVEL", "INFO")\n'
                    "LOGGING = {\n"
                    "    'handlers': {name: {'level': LOGLEVEL} for name in "
                    "('console', 'netbox_file', 'forward_file')},\n"
                    "    'loggers': {name: {'level': LOGLEVEL} for name in "
                    "('django', 'django_auth_ldap', 'netbox', "
                    "'netbox_branching', 'forward_netbox')},\n"
                    "}\n"
                ),
                "development/docker-compose.override.yml": (
                    "services:\n"
                    "  netbox:\n"
                    "    environment: {LOGLEVEL: INFO}\n"
                    "  netbox-worker:\n"
                    "    environment: {LOGLEVEL: INFO}\n"
                ),
            }
        )

        self.assertEqual(failures, [])

    def test_debug_worker_or_handler_is_rejected(self):
        failures = self._check(
            {
                "development/configuration/logging.py": (
                    "from os import environ\n"
                    'LOGLEVEL = environ.get("LOGLEVEL", "DEBUG")\n'
                    "LOGGING = {\n"
                    "    'handlers': {name: {'level': "
                    "('DEBUG' if name == 'forward_file' else LOGLEVEL)} for name in "
                    "('console', 'netbox_file', 'forward_file')},\n"
                    "    'loggers': {name: {'level': LOGLEVEL} for name in "
                    "('django', 'django_auth_ldap', 'netbox', "
                    "'netbox_branching', 'forward_netbox')},\n"
                    "}\n"
                ),
                "development/docker-compose.override.yml": (
                    "services:\n"
                    "  netbox:\n"
                    "    environment: {LOGLEVEL: INFO}\n"
                    "  netbox-worker:\n"
                    "    environment: {LOGLEVEL: DEBUG}\n"
                ),
            }
        )

        self.assertEqual(len(failures), 4)

    def test_missing_logging_files_are_rejected(self):
        failures = self._check({})

        self.assertEqual(len(failures), 2)

    def test_hardcoded_info_levels_are_rejected(self):
        failures = self._check(
            {
                "development/configuration/logging.py": (
                    "from os import environ\n"
                    'LOGLEVEL = environ.get("LOGLEVEL", "INFO")\n'
                    "LOGGING = {\n"
                    "    'handlers': {name: {'level': 'INFO'} for name in "
                    "('console', 'netbox_file', 'forward_file')},\n"
                    "    'loggers': {name: {'level': 'INFO'} for name in "
                    "('django', 'django_auth_ldap', 'netbox', "
                    "'netbox_branching', 'forward_netbox')},\n"
                    "}\n"
                ),
                "development/docker-compose.override.yml": (
                    "services:\n"
                    "  netbox:\n"
                    "    environment: {LOGLEVEL: INFO}\n"
                    "  netbox-worker:\n"
                    "    environment: {LOGLEVEL: INFO}\n"
                ),
            }
        )

        self.assertEqual(len(failures), 2)
        self.assertTrue(all("LOGLEVEL=WARNING" in failure for failure in failures))


class CheckHarnessGardeningDependencyTest(unittest.TestCase):
    def _check(self, workflow_text):
        with tempfile.TemporaryDirectory() as temp_dir:
            repo_root = Path(temp_dir)
            path = repo_root / ".github/workflows/harness-gardening.yml"
            path.parent.mkdir(parents=True)
            path.write_text(workflow_text, encoding="utf-8")
            failures = []
            with patch.object(check_harness, "REPO_ROOT", repo_root):
                check_harness._check_harness_gardening_dependency(failures)
        return failures

    def test_dependency_before_harness_passes(self):
        failures = self._check(
            "jobs:\n"
            "  audit:\n"
            "    steps:\n"
            "      - run: python -m pip install --disable-pip-version-check PyYAML==6.0.3\n"
            "      - run: python scripts/check_harness.py\n"
        )

        self.assertEqual(failures, [])

    def test_missing_dependency_fails(self):
        failures = self._check(
            "jobs:\n"
            "  audit:\n"
            "    steps:\n"
            "      - run: python scripts/check_harness.py\n"
        )

        self.assertEqual(len(failures), 1)
        self.assertIn("must install PyYAML 6.0.3", failures[0])

    def test_dependency_after_harness_fails(self):
        failures = self._check(
            "jobs:\n"
            "  audit:\n"
            "    steps:\n"
            "      - run: python scripts/check_harness.py\n"
            "      - run: python -m pip install --disable-pip-version-check PyYAML==6.0.3\n"
        )

        self.assertEqual(len(failures), 1)
        self.assertIn("before the harness check", failures[0])


class CheckHarnessSensitiveGuardTest(unittest.TestCase):
    CI_WORKFLOW = """jobs:
  validate:
    steps:
      - uses: actions/checkout@example
        with:
          fetch-depth: 0
      - run: python scripts/check_sensitive_content.py --protected-history --require-baseline-env
      - run: python scripts/check_sensitive_content.py --git-files
      - if: github.event_name == 'push'
        env:
          FORWARD_SENSITIVE_PATTERNS: ${{ secrets.FORWARD_SENSITIVE_PATTERNS }}
          FORWARD_SENSITIVE_HISTORY_BASELINE: ${{ vars.FORWARD_SENSITIVE_HISTORY_BASELINE }}
        run: python scripts/check_sensitive_content.py --git-files --protected-history --require-env-patterns --require-baseline-env
"""
    RELEASE_WORKFLOW = """permissions:
  actions: read
  contents: read
  pull-requests: read
  statuses: read
jobs:
  validate:
    steps:
      - uses: actions/checkout@example
        with:
          fetch-depth: 0
      - run: git fetch origin refs/tags/v2.5.11 && python scripts/verify_release_provenance.py --tag v2.6.0
      - env:
          FORWARD_SENSITIVE_PATTERNS: ${{ secrets.FORWARD_SENSITIVE_PATTERNS }}
          FORWARD_SENSITIVE_HISTORY_BASELINE: ${{ vars.FORWARD_SENSITIVE_HISTORY_BASELINE }}
        run: python scripts/check_sensitive_content.py --git-files --protected-history --require-env-patterns --require-baseline-env
"""
    TRUSTED_WORKFLOW = """\"on\":
  pull_request_target:
    types: [opened, reopened, synchronize]
permissions:
  statuses: write
jobs:
  sensitive-content:
    steps:
      - uses: actions/checkout@example
        with:
          fetch-depth: 0
          persist-credentials: false
          ref: ${{ github.event.pull_request.base.sha }}
      - run: git fetch origin \"pull/${PR_NUMBER}/head\"
      - id: scan
        env:
          FORWARD_SENSITIVE_PATTERNS: ${{ secrets.FORWARD_SENSITIVE_PATTERNS }}
          FORWARD_SENSITIVE_HISTORY_BASELINE: ${{ vars.FORWARD_SENSITIVE_HISTORY_BASELINE }}
        run: python scripts/check_sensitive_content.py --rev-list base..head --git-tree head --ref-name branch --require-env-patterns --require-baseline-env
      - if: always()
        env:
          GH_TOKEN: ${{ secrets.GITHUB_TOKEN }}
          SCAN_OUTCOME: ${{ steps.scan.outcome }}
          RUN_ID: ${{ github.run_id }}
        run: echo Trusted sensitive-content scan target_url actions/runs/${RUN_ID}
"""
    TASKS = """def sensitive_check(context):
    context.run(f\"python scripts/check_sensitive_content.py\")
    context.run(f\"python scripts/check_sensitive_content.py --protected-history\")
"""

    def _check(self, *, ci=None, release=None, tasks=None):
        with tempfile.TemporaryDirectory() as temp_dir:
            repo_root = Path(temp_dir)
            files = {
                ".github/workflows/ci.yml": ci or self.CI_WORKFLOW,
                ".github/workflows/release.yml": release or self.RELEASE_WORKFLOW,
                ".github/workflows/trusted-sensitive-pr.yml": self.TRUSTED_WORKFLOW,
                "tasks.py": tasks or self.TASKS,
            }
            for relative_path, content in files.items():
                path = repo_root / relative_path
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text(content, encoding="utf-8")
            failures = []
            with patch.object(check_harness, "REPO_ROOT", repo_root):
                check_harness._check_sensitive_guard_wiring(failures)
        return failures

    def test_complete_sensitive_guard_wiring_passes(self):
        self.assertEqual(self._check(), [])

    def test_shallow_checkout_fails(self):
        failures = self._check(
            ci=self.CI_WORKFLOW.replace("fetch-depth: 0", "fetch-depth: 1")
        )

        self.assertTrue(any("fetch-depth: 0" in failure for failure in failures))

    def test_missing_release_secret_enforcement_fails(self):
        failures = self._check(
            release=self.RELEASE_WORKFLOW.replace(" --require-env-patterns", "")
        )

        self.assertTrue(
            any("--require-env-patterns" in failure for failure in failures)
        )

    def test_missing_release_provenance_permission_fails(self):
        failures = self._check(
            release=self.RELEASE_WORKFLOW.replace("  actions: read\n", "")
        )

        self.assertTrue(any("actions: read" in failure for failure in failures))

    def test_missing_task_history_scan_fails(self):
        failures = self._check(
            tasks='context.run(f"python scripts/check_sensitive_content.py")\n'
        )

        self.assertTrue(any("--protected-history" in failure for failure in failures))

    def test_disabled_scanner_step_fails(self):
        failures = self._check(
            release=self.RELEASE_WORKFLOW.replace(
                "      - env:\n",
                "      - if: false\n        env:\n",
            )
        )

        self.assertTrue(
            any("must not be conditional" in failure for failure in failures)
        )

    def test_untrusted_environment_provenance_fails(self):
        failures = self._check(
            release=self.RELEASE_WORKFLOW.replace(
                "${{ vars.FORWARD_SENSITIVE_HISTORY_BASELINE }}",
                "candidate-value",
            )
        )

        self.assertTrue(any("trusted settings" in failure for failure in failures))

    def test_missing_candidate_status_permission_fails(self):
        trusted = self.TRUSTED_WORKFLOW.replace("  statuses: write\n", "")
        with tempfile.TemporaryDirectory() as temp_dir:
            repo_root = Path(temp_dir)
            files = {
                ".github/workflows/ci.yml": self.CI_WORKFLOW,
                ".github/workflows/release.yml": self.RELEASE_WORKFLOW,
                ".github/workflows/trusted-sensitive-pr.yml": trusted,
                "tasks.py": self.TASKS,
            }
            for relative_path, content in files.items():
                path = repo_root / relative_path
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text(content, encoding="utf-8")
            failures = []
            with patch.object(check_harness, "REPO_ROOT", repo_root):
                check_harness._check_sensitive_guard_wiring(failures)

        self.assertTrue(any("statuses: write" in failure for failure in failures))

    def test_conditional_candidate_status_fails(self):
        trusted = self.TRUSTED_WORKFLOW.replace("if: always()", "if: success()")
        with tempfile.TemporaryDirectory() as temp_dir:
            repo_root = Path(temp_dir)
            files = {
                ".github/workflows/ci.yml": self.CI_WORKFLOW,
                ".github/workflows/release.yml": self.RELEASE_WORKFLOW,
                ".github/workflows/trusted-sensitive-pr.yml": trusted,
                "tasks.py": self.TASKS,
            }
            for relative_path, content in files.items():
                path = repo_root / relative_path
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text(content, encoding="utf-8")
            failures = []
            with patch.object(check_harness, "REPO_ROOT", repo_root):
                check_harness._check_sensitive_guard_wiring(failures)

        self.assertTrue(any("if: always()" in failure for failure in failures))


class CheckHarnessReleaseToolchainTest(unittest.TestCase):
    LOCK = """build==1.5.0 \\
    --hash=sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
pip==26.1.2 \\
    --hash=sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb
"""
    RELEASE = """jobs:
  validate:
    steps:
      - run: python -m pip install --require-hashes --requirement requirements-release.txt
  build:
    steps:
      - run: python -m pip install --require-hashes --requirement requirements-release.txt
"""
    CI = """jobs:
  validate:
    steps:
      - run: python -m pip install --require-hashes --requirement requirements-release.txt
"""

    def _check(self, *, lock=None, release=None, ci=None):
        with tempfile.TemporaryDirectory() as temp_dir:
            repo_root = Path(temp_dir)
            files = {
                "requirements-release.txt": lock or self.LOCK,
                ".github/workflows/release.yml": release or self.RELEASE,
                ".github/workflows/ci.yml": ci or self.CI,
            }
            for relative_path, content in files.items():
                path = repo_root / relative_path
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text(content, encoding="utf-8")
            failures = []
            with patch.object(check_harness, "REPO_ROOT", repo_root):
                check_harness._check_release_toolchain_lock(failures)
        return failures

    def test_hash_locked_toolchain_passes(self):
        self.assertEqual(self._check(), [])

    def test_unpinned_entry_fails(self):
        failures = self._check(lock=self.LOCK.replace("build==1.5.0", "build>=1.5"))

        self.assertTrue(any("exact versions" in failure for failure in failures))

    def test_missing_hash_fails(self):
        failures = self._check(
            lock=self.LOCK.replace(
                "    --hash=sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\n",
                "",
            )
        )

        self.assertTrue(any("SHA-256 hashes" in failure for failure in failures))

    def test_mutable_release_install_fails(self):
        failures = self._check(
            release=self.RELEASE + "\n# pip install --upgrade build\n"
        )

        self.assertTrue(any("mutable latest" in failure for failure in failures))


class CheckHarnessStandardReleaseTagFlowTest(unittest.TestCase):
    RELEASE = """\
ensure_release_tag(tag, head_commit)
_verify_live_release_controls()
"--controls-only"
"tag",
"-a",
"push", "origin", f"refs/tags/{tag}"
"ls-remote", "--tags", "origin", f"refs/tags/{tag}^{{}}"
"""
    PROVENANCE = """\
PRIOR_RELEASE_TAG = "v2.5.11"
BOOTSTRAP_REQUIRED_FILES
BASE_REQUIRED_STATUS_CHECKS
TRUSTED_STATUS_CONTEXT
operation.add_argument("--controls-only", action="store_true")
"merge-base", "--is-ancestor", release_commit, current_main
"""

    def _check(self, *, release=None, provenance=None):
        with tempfile.TemporaryDirectory() as temp_dir:
            repo_root = Path(temp_dir)
            files = {
                "scripts/release.py": release or self.RELEASE,
                "scripts/verify_release_provenance.py": (provenance or self.PROVENANCE),
            }
            for relative_path, content in files.items():
                path = repo_root / relative_path
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text(content, encoding="utf-8")
            failures = []
            with patch.object(check_harness, "REPO_ROOT", repo_root):
                check_harness._check_standard_release_tag_flow(failures)
        return failures

    def test_standard_release_tag_flow_passes(self):
        self.assertEqual(self._check(), [])

    def test_missing_remote_target_verification_fails(self):
        release = self.RELEASE.replace(
            '"ls-remote", "--tags", "origin", f"refs/tags/{tag}^{{}}"\n',
            "",
        )

        self.assertTrue(
            any("ls-remote" in failure for failure in self._check(release=release))
        )

    def test_retired_app_controller_fails(self):
        release = self.RELEASE + "\nRELEASE_CONTROL_APP_ID\n"

        self.assertTrue(
            any(
                "retired release controller" in failure
                for failure in self._check(release=release)
            )
        )

    def test_retired_anchor_fails(self):
        provenance = self.PROVENANCE + "\nsecurity-bootstrap-2.6\n"

        self.assertTrue(
            any(
                "retired release controller" in failure
                for failure in self._check(provenance=provenance)
            )
        )


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
