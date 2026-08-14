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
                "development/.env": "NETBOX_VER=v4.6.6\n",
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
PRIOR_RELEASE_TAG = "v2.7.0"
BOOTSTRAP_REQUIRED_FILES
BOOTSTRAP_FILE_DIGESTS
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


class ReleaseAnchorTracksCurrentReleaseTest(unittest.TestCase):
    """Two post-release steps that were repeatedly skipped, tied together.

    Advancing `PRIOR_RELEASE_TAG` and promoting the shipped release in the
    compatibility table are both silent when omitted. A stale anchor grows the
    reviewed commit range until an expired run burns a release at the tag; a
    stale table told operators `2.6.9` was current while two later releases were
    already published. After a release the two name the same version, so they
    can check each other.
    """

    PROVENANCE = 'PRIOR_RELEASE_TAG = "{tag}"\n'
    TABLE = (
        "| Plugin Release | NetBox Version | Status |\n"
        "| --- | --- | --- |\n"
        "| `{current}` | `4.6.x` | Current release; shipped. |\n"
        "| `v2.6.9` | `4.6.x` | Superseded by `{current}`; shipped. |\n"
    )

    def _run(self, *, tag, current, table=None, unpublished=None):
        failures = []
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "scripts").mkdir()
            provenance = self.PROVENANCE.format(tag=tag)
            if unpublished is not None:
                listed = ", ".join(f'"{entry}"' for entry in unpublished)
                provenance = f"UNPUBLISHED_RELEASE_TAGS = ({listed})\n" + provenance
            (root / "scripts/verify_release_provenance.py").write_text(
                provenance, encoding="utf-8"
            )
            (root / "README.md").write_text(
                table if table is not None else self.TABLE.format(current=current),
                encoding="utf-8",
            )
            with patch.object(check_harness, "REPO_ROOT", root):
                check_harness._check_release_anchor_tracks_current_release(failures)
        return failures

    def test_anchor_matching_the_current_release_passes(self):
        self.assertEqual(self._run(tag="v2.7.0", current="v2.7.0"), [])

    def test_stale_anchor_is_reported_with_both_remedies(self):
        # The exact shape that burned v2.6.10: the anchor left behind while the
        # table moved on.
        failures = self._run(tag="v2.6.10", current="v2.7.0")
        self.assertEqual(len(failures), 1)
        self.assertIn("v2.6.10", failures[0])
        self.assertIn("v2.7.0", failures[0])
        self.assertIn("advance the anchor", failures[0])
        self.assertIn("promote the shipped release", failures[0])

    def test_unpromoted_table_is_reported(self):
        # The other direction: the anchor advanced but the release was never
        # promoted, so the table still calls an older version current.
        failures = self._run(tag="v2.7.0", current="v2.6.12")
        self.assertEqual(len(failures), 1)
        self.assertIn("does not match the current release", failures[0])

    def test_an_anchor_on_a_tag_that_never_published_is_accepted(self):
        # A tag refused at the publish gate is still a valid anchor - it exists
        # and is annotated - but it must never be promoted in the table, so the
        # two legitimately disagree. v2.7.8 is the case that forced this.
        self.assertEqual(
            self._run(tag="v2.7.8", current="v2.7.6", unpublished=("v2.7.8",)),
            [],
        )

    def test_an_unlisted_anchor_is_reported_even_when_others_are_listed(self):
        # The excuse is per-tag. Recording one unpublished tag must not blanket-
        # accept every anchor that disagrees with the table, or the forgotten
        # promotion this check exists for comes back.
        failures = self._run(tag="v2.7.9", current="v2.7.6", unpublished=("v2.7.8",))
        self.assertEqual(len(failures), 1)
        self.assertIn("does not match the current release", failures[0])
        self.assertIn("UNPUBLISHED_RELEASE_TAGS", failures[0])

    def test_a_table_without_exactly_one_current_release_is_reported(self):
        # The promotion step edits this column, so a botched edit is the likely
        # way it breaks; taking the first match would hide it.
        table = (
            "| `v2.7.0` | `4.6.x` | Current release; shipped. |\n"
            "| `v2.6.12` | `4.6.x` | Current release; shipped. |\n"
        )
        failures = self._run(tag="v2.7.0", current="v2.7.0", table=table)
        self.assertEqual(len(failures), 1)
        self.assertIn("exactly one current release", failures[0])

    def test_a_missing_anchor_constant_is_reported(self):
        failures = self._run(tag="not-a-tag", current="v2.7.0")
        self.assertEqual(len(failures), 1)
        self.assertIn("PRIOR_RELEASE_TAG", failures[0])


class PostReleaseBridgeIsDocumentationOnlyTest(unittest.TestCase):
    """The bridge slot is not reclaimable, so its shape is checked before a tag.

    `v2.7.0` was promoted without being archived first, so the promotion commit
    took the slot the verifier reserves for a documentation-only bridge. The
    bridge is defined as the first first-parent commit after the tag, so no
    later commit could reclaim it: every release after `v2.7.0` was unverifiable
    until the rule was widened. Nothing failed at the time because the harness
    only checked which release the anchor named, never what the bridge changed.

    The rule itself is deliberately not restated here - these cases exercise the
    verifier's own `_is_documentation_path` through the harness check.
    """

    BRIDGE = "b" * 40
    PROVENANCE = (
        'PRIOR_RELEASE_TAG = "v2.7.0"\n' f'PRIOR_POST_RELEASE_DOC_COMMIT = "{BRIDGE}"\n'
    )

    def _run(self, changed, *, provenance=None):
        failures = []
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "scripts").mkdir()
            (root / "scripts/verify_release_provenance.py").write_text(
                self.PROVENANCE if provenance is None else provenance,
                encoding="utf-8",
            )
            with (
                patch.object(check_harness, "REPO_ROOT", root),
                patch.object(
                    check_harness,
                    "_git_names",
                    return_value=list(changed),
                ) as git_names,
            ):
                check_harness._check_post_release_bridge_is_documentation_only(failures)
                self.calls = git_names.call_args_list
        return failures

    def test_documentation_only_bridge_passes(self):
        failures = self._run(
            [
                "CHANGELOG.md",
                "README.md",
                "docs/01_User_Guide/README.md",
                "docs/03_Plans/completed/2026-08-03-release-2.7.1.md",
            ]
        )

        self.assertEqual(failures, [])
        self.assertEqual(
            self.calls[0].args,
            ("diff", "--name-only", "v2.7.0", self.BRIDGE),
        )

    def test_bridge_carrying_plugin_code_fails(self):
        failures = self._run(["CHANGELOG.md", "forward_netbox/utilities/sync.py"])

        self.assertEqual(len(failures), 1)
        self.assertIn("forward_netbox/utilities/sync.py", failures[0])
        # Only the disqualifying paths are named; the documentation it also
        # carried is not what has to be removed.
        self.assertNotIn("CHANGELOG.md", failures[0])
        self.assertIn("cannot be reclaimed", failures[0])

    def test_bridge_carrying_a_workflow_fails(self):
        failures = self._run([".github/workflows/release.yml"])

        self.assertEqual(len(failures), 1)
        self.assertIn(".github/workflows/release.yml", failures[0])
        self.assertIn("cannot be reclaimed", failures[0])

    def test_an_unreadable_or_empty_bridge_fails_closed(self):
        # `all()` is vacuously true on an empty sequence, and `_git_names`
        # returns nothing when git fails, so silence must not read as a pass.
        failures = self._run([])

        self.assertEqual(len(failures), 1)
        self.assertIn("no changed paths", failures[0])

    def test_a_missing_bridge_constant_is_reported(self):
        failures = self._run(
            ["CHANGELOG.md"],
            provenance='PRIOR_RELEASE_TAG = "v2.7.0"\n',
        )

        self.assertEqual(len(failures), 1)
        self.assertIn("PRIOR_POST_RELEASE_DOC_COMMIT", failures[0])


class TemplateCommentsAreParseableTest(unittest.TestCase):
    """A `{# #}` comment that wraps is not a comment - Django prints it.

    Two of these reached a customer's UI, one of them in a panel shipped the day
    before. Nothing in the suite could see it: the template rendered, the view
    returned 200, and 2085 tests passed with the text on screen.
    """

    def _failures(self, template_body):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            template_dir = root / "forward_netbox" / "templates" / "forward_netbox"
            template_dir.mkdir(parents=True)
            (template_dir / "page.html").write_text(template_body, encoding="utf-8")
            original = check_harness.REPO_ROOT
            check_harness.REPO_ROOT = root
            try:
                failures = []
                check_harness._check_template_comments_are_parseable(failures)
                return failures
            finally:
                check_harness.REPO_ROOT = original

    def test_a_wrapped_comment_is_reported(self):
        failures = self._failures(
            "<div>\n  {# this explanation is long enough that it\n"
            "     wrapped onto a second line #}\n  <p>body</p>\n</div>\n"
        )

        self.assertEqual(len(failures), 1)
        self.assertIn("page.html:2", failures[0])
        self.assertIn("comment", failures[0])

    def test_a_single_line_comment_is_accepted(self):
        failures = self._failures("<div>\n  {# short and closed #}\n</div>\n")

        self.assertEqual(failures, [])

    def test_a_comment_block_is_accepted(self):
        failures = self._failures(
            "<div>\n  {% comment %}\n  wrapped prose is fine here\n"
            "  {% endcomment %}\n</div>\n"
        )

        self.assertEqual(failures, [])

    def test_a_second_comment_on_the_line_is_still_checked(self):
        """The closed one must not vouch for the open one beside it."""
        failures = self._failures(
            "<div>\n  {# closed #} {# but this one runs on\n"
            "     to the next line #}\n</div>\n"
        )

        self.assertEqual(len(failures), 1)
        self.assertIn("page.html:2", failures[0])

    def test_a_template_with_no_comments_is_accepted(self):
        failures = self._failures("<div><p>body</p></div>\n")

        self.assertEqual(failures, [])
