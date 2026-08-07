#!/usr/bin/env python3
from __future__ import annotations

import importlib.util
import json
import os
import re
import runpy
import subprocess
import sys
from datetime import date
from datetime import datetime
from pathlib import Path

import yaml


REPO_ROOT = Path(__file__).resolve().parents[1]
FORBIDDEN_TRACKED_DEVELOPMENT_ENV_FILES = {
    "development/.env",
    "development/env/redis.env",
}
FORBIDDEN_DEVELOPMENT_SECRET_ASSIGNMENT = re.compile(
    r"^(?:API_TOKEN_PEPPER_\d+|DB_PASSWORD|POSTGRES_PASSWORD|"
    r"REDIS(?:_CACHE)?_PASSWORD|SECRET_KEY)\s*=",
    re.MULTILINE,
)

REQUIRED_PATHS = [
    ".sensitive-binary-allowlist",
    ".sensitive-history-baseline",
    "AGENTS.md",
    "ARCHITECTURE.md",
    "docs/00_Project_Knowledge/README.md",
    "docs/00_Project_Knowledge/architecture.md",
    "docs/00_Project_Knowledge/agent-workflow.md",
    "docs/00_Project_Knowledge/code-boundary-map.md",
    "docs/00_Project_Knowledge/validation-matrix.md",
    "docs/00_Project_Knowledge/release-playbook.md",
    "docs/00_Project_Knowledge/local-docker-workflow.md",
    "docs/00_Project_Knowledge/harness-engineering-alignment.md",
    "docs/00_Project_Knowledge/quality-score.md",
    "docs/03_Plans/active/README.md",
    "docs/03_Plans/completed/README.md",
    "docs/03_Plans/plan-template.md",
    "scripts/tests/test_check_harness.py",
    "scripts/check_release_authorization.py",
    "scripts/build_reproducible_distribution.py",
    "scripts/verify_release_provenance.py",
    "scripts/check_sensitive_content.py",
    "scripts/sensitive_content.py",
    "scripts/tests/test_release_authorization.py",
    "scripts/tests/test_build_reproducible_distribution.py",
    "scripts/tests/test_verify_release_provenance.py",
    "scripts/tests/test_sensitive_content.py",
    "requirements-release.in",
    "requirements-release.txt",
]

AGENTS_ENTRYPOINT_MAX_LINES = 120
KNOWLEDGE_FRESHNESS_DAYS = {
    "docs/00_Project_Knowledge/harness-engineering-alignment.md": 90,
    "docs/00_Project_Knowledge/quality-score.md": 90,
}
EXPECTED_NETBOX_HEALTHCHECK = "curl -f http://localhost:8000/login/ || exit 1"
EXPECTED_HARNESS_DEPENDENCY_COMMAND = (
    "python -m pip install --disable-pip-version-check PyYAML==6.0.3"
)
EXPECTED_HARNESS_CHECK_COMMAND = "python scripts/check_harness.py"
EXPECTED_DEVELOPMENT_RQ_DEFAULT_TIMEOUT = "7200"
EXPECTED_DEVELOPMENT_LOG_LEVEL = "INFO"

RUNTIME_SOURCE_SUFFIXES = {".html", ".js", ".nqe", ".py"}
RUNTIME_SOURCE_EXCLUDED_DIRECTORIES = {"migrations", "tests"}
RETIREMENT_CONFIGURATION_SUFFIXES = {
    "",
    ".env",
    ".py",
    ".toml",
    ".txt",
    ".yaml",
    ".yml",
}
RETIREMENT_CONFIGURATION_ROOTS = ("development", ".github/workflows")
RETIREMENT_CONFIGURATION_FILES = ("tasks.py", "pyproject.toml", "constraints.txt")
RETIRED_RUNTIME_PATTERNS = {
    r"\b_execution_progress\b": "retired persisted execution progress",
    r"\bget_execution_display_state\b": "retired execution-state display adapter",
    r"\bset_execution_progress\b": "retired execution progress writer",
    r"\bset_runtime_phase\b": "retired runtime-phase compatibility shim",
    r"\bfetch_column_filters\b": "retired column-filter fetch mode",
    r"\bcolumn_filters\b": "retired column-filter query contract",
    r"['\"]device_tag_include['\"]": "retired singular include-tag key",
    r"['\"]device_tag_exclude['\"]": "retired singular exclude-tag key",
    r"\bJOBRESULT_RETENTION\b": "retired job-retention environment alias",
    r"\bLOGIN_REQUIRED\b": "retired NetBox login setting",
    r"PluginConfig\s*=\s*object": "retired PluginConfig import fallback",
    r"_CoreSyncError\s*=\s*Exception": "retired sync-error import fallback",
    r"\b_load_cached_diagnostic_result\b": "retired diagnostic cache reader",
    r"\b_store_cached_diagnostic_result\b": "retired diagnostic cache writer",
    r"\blegacy_endpoint_device_types\b": "retired endpoint diagnostic key",
    r"['\"]forward_sync_": "retired Django-cache job-result key",
    r"\bpackage_names\b": "retired optional-plugin package aliases",
    r"\binstalled_package_name\b": "retired optional-plugin package detection alias",
    r"\bnetbox_aci_plugin\b": "retired Cisco ACI package alias",
    r"\bnetbox-aci-plugin\b": "retired Cisco ACI distribution alias",
}

PLAN_REQUIRED_HEADINGS = [
    "## Goal",
    "## Constraints",
    "## Touched Surfaces",
    "## Approach",
    "## Validation",
    "## Rollback",
    "## Decision Log",
]

HIGH_RISK_PATHS = [
    ".github/workflows/",
    "pyproject.toml",
    "tasks.py",
    "scripts/",
    "forward_netbox/models.py",
    "forward_netbox/forms.py",
    "forward_netbox/views.py",
    "forward_netbox/api/",
    "forward_netbox/jobs.py",
    "forward_netbox/queries/",
    "forward_netbox/utilities/",
    "forward_netbox/management/commands/",
]

PLAN_PATHS = [
    "docs/03_Plans/active/",
    "docs/03_Plans/completed/",
]

REQUIRED_TEXT = {
    "AGENTS.md": [
        "ARCHITECTURE.md",
        "Agent Workflow",
        "invoke harness-check",
        "invoke harness-test",
        "sensitive",
    ],
    "ARCHITECTURE.md": [
        "Production Boundaries",
        "Ownership Control Plane",
        "Non-Negotiable Constraints",
    ],
    "docs/00_Project_Knowledge/validation-matrix.md": [
        "invoke harness-check",
        "invoke harness-test",
        "invoke playwright-test",
        "invoke lint",
        "invoke check",
        "invoke scenario-test",
        "invoke test",
        "invoke docs",
        "scripts/check_sensitive_content.py --protected-history",
        "EPGs, contracts, and static port bindings are excluded from 2.6",
    ],
    "docs/01_User_Guide/upgrade.md": [
        "every upgrade from a pre-2.6 release must run **Publish Bundled",
    ],
    "docs/00_Project_Knowledge/agent-workflow.md": [
        "Choose The Lane",
        "Before Editing",
        "Before Commit",
        "invoke harness-test",
        "invoke playwright-test",
    ],
    "docs/00_Project_Knowledge/code-boundary-map.md": [
        "Forward API Boundary",
        "Branch Execution Boundary",
        "NetBox Adapter Boundary",
    ],
    "docs/00_Project_Knowledge/harness-engineering-alignment.md": [
        "Repository knowledge",
        "Application legibility",
        "Architecture enforcement",
        "Entropy control",
    ],
    "docs/00_Project_Knowledge/release-playbook.md": [
        "GitHub CI",
        "PyPI",
        "twine",
        "invoke harness-test",
        "invoke playwright-test",
    ],
    "docs/03_Plans/plan-template.md": [
        "Goal",
        "Validation",
        "Rollback",
        "Decision Log",
    ],
    ".github/workflows/release.yml": [
        "fetch-depth: 0",
        "refs/tags/v2.7.4",
        "verify_release_provenance.py",
        "--git-files",
        "--protected-history",
        "--require-env-patterns --require-baseline-env",
        "FORWARD_SENSITIVE_HISTORY_BASELINE",
        "--require-hashes",
        "requirements-release.txt",
        "scripts/build_reproducible_distribution.py",
    ],
    ".github/CODEOWNERS": [
        "@captainpacket",
        "/.github/",
        "/scripts/",
        "/.sensitive-binary-allowlist",
        "/.sensitive-history-baseline",
    ],
}


def _check_agents_entrypoint(failures: list[str]) -> None:
    path = REPO_ROOT / "AGENTS.md"
    if not path.exists():
        return
    line_count = len(path.read_text(encoding="utf-8").splitlines())
    if line_count > AGENTS_ENTRYPOINT_MAX_LINES:
        failures.append(
            "AGENTS.md must remain a concise repository map: "
            f"{line_count} lines exceeds {AGENTS_ENTRYPOINT_MAX_LINES}"
        )


def _check_knowledge_freshness(
    failures: list[str],
    *,
    today: date | None = None,
) -> None:
    today = today or date.today()
    for relative_path, max_age_days in KNOWLEDGE_FRESHNESS_DAYS.items():
        path = REPO_ROOT / relative_path
        if not path.exists():
            continue
        text = path.read_text(encoding="utf-8")
        match = re.search(r"^Last reviewed:\s*(\d{4}-\d{2}-\d{2})\s*$", text, re.M)
        if match is None:
            failures.append(f"{relative_path} must include 'Last reviewed: YYYY-MM-DD'")
            continue
        try:
            reviewed = datetime.strptime(match.group(1), "%Y-%m-%d").date()
        except ValueError:
            failures.append(
                f"{relative_path} has an invalid review date: {match.group(1)}"
            )
            continue
        age_days = (today - reviewed).days
        if age_days < 0:
            failures.append(
                f"{relative_path} has a future review date: {reviewed.isoformat()}"
            )
        elif age_days > max_age_days:
            failures.append(
                f"{relative_path} review is stale: {age_days} days old "
                f"(maximum {max_age_days})"
            )


def _check_retired_runtime_paths(failures: list[str]) -> None:
    runtime_root = REPO_ROOT / "forward_netbox"
    paths = []
    if runtime_root.exists():
        for path in sorted(runtime_root.rglob("*")):
            if not path.is_file() or path.suffix not in RUNTIME_SOURCE_SUFFIXES:
                continue
            relative_path = path.relative_to(runtime_root)
            if any(
                part in RUNTIME_SOURCE_EXCLUDED_DIRECTORIES
                for part in relative_path.parts[:-1]
            ):
                continue
            paths.append(path)
    for relative_root in RETIREMENT_CONFIGURATION_ROOTS:
        root = REPO_ROOT / relative_root
        if not root.exists():
            continue
        paths.extend(
            path
            for path in sorted(root.rglob("*"))
            if path.is_file() and path.suffix in RETIREMENT_CONFIGURATION_SUFFIXES
        )
    paths.extend(
        path
        for relative_path in RETIREMENT_CONFIGURATION_FILES
        if (path := REPO_ROOT / relative_path).is_file()
    )

    for path in dict.fromkeys(paths):
        text = path.read_text(encoding="utf-8")
        for pattern, description in RETIRED_RUNTIME_PATTERNS.items():
            match = re.search(pattern, text)
            if match is None:
                continue
            line = text.count("\n", 0, match.start()) + 1
            failures.append(
                f"{path.relative_to(REPO_ROOT)}:{line} contains {description}"
            )


def _git_names(*args: str) -> list[str]:
    try:
        result = subprocess.run(
            ["git", *args],
            cwd=REPO_ROOT,
            check=True,
            capture_output=True,
            text=True,
        )
    except subprocess.CalledProcessError:
        return []
    return [line.strip() for line in result.stdout.splitlines() if line.strip()]


def _is_zero_sha(value: str | None) -> bool:
    return bool(value) and set(value) == {"0"}


def _commit_files_from_event(event: dict) -> list[str]:
    files: set[str] = set()
    for commit in event.get("commits", []):
        for key in ("added", "modified", "removed"):
            files.update(commit.get(key, []))
    return sorted(files)


def _github_changed_files() -> list[str]:
    event_path = os.environ.get("GITHUB_EVENT_PATH")
    if not event_path:
        return []

    try:
        event = json.loads(Path(event_path).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return []

    pull_request = event.get("pull_request") or {}
    if pull_request:
        base_sha = pull_request.get("base", {}).get("sha")
        head_sha = pull_request.get("head", {}).get("sha")
        if base_sha and head_sha:
            changed_files = _git_names("diff", "--name-only", base_sha, head_sha)
            if changed_files:
                return changed_files

    before_sha = event.get("before")
    after_sha = event.get("after")
    if before_sha and after_sha and not _is_zero_sha(before_sha):
        changed_files = _git_names("diff", "--name-only", before_sha, after_sha)
        if changed_files:
            return changed_files

    changed_files = _commit_files_from_event(event)
    if changed_files:
        return changed_files

    if after_sha:
        return _git_names("diff-tree", "--no-commit-id", "--name-only", "-r", after_sha)

    return []


def _local_changed_files() -> list[str]:
    changed_files = set(_git_names("diff", "--name-only", "HEAD"))
    changed_files.update(_git_names("ls-files", "--others", "--exclude-standard"))
    return sorted(changed_files)


def _changed_files() -> list[str]:
    if os.environ.get("GITHUB_ACTIONS") == "true":
        changed_files = _github_changed_files()
        if changed_files:
            return sorted(set(changed_files))
    return _local_changed_files()


def _is_plan_file(path: str) -> bool:
    if not path.endswith(".md") or path.endswith("/README.md"):
        return False
    return any(path.startswith(plan_path) for plan_path in PLAN_PATHS)


def _is_high_risk_path(path: str) -> bool:
    return any(
        path == high_risk_path or path.startswith(high_risk_path)
        for high_risk_path in HIGH_RISK_PATHS
    )


def _check_plan_directory(failures: list[str], relative_directory: str) -> None:
    directory = REPO_ROOT / relative_directory
    if not directory.exists():
        return
    for path in sorted(directory.glob("*.md")):
        if path.name == "README.md":
            continue
        text = path.read_text(encoding="utf-8")
        for heading in PLAN_REQUIRED_HEADINGS:
            if heading not in text:
                failures.append(
                    f"{path.relative_to(REPO_ROOT)} must include plan heading: {heading}"
                )


def _check_plan_lifecycle(failures: list[str]) -> None:
    changed_files = _changed_files()
    if not changed_files:
        return

    high_risk_files = sorted(path for path in changed_files if _is_high_risk_path(path))
    if not high_risk_files or any(_is_plan_file(path) for path in changed_files):
        return

    formatted_files = ", ".join(high_risk_files[:8])
    if len(high_risk_files) > 8:
        formatted_files = f"{formatted_files}, ..."
    failures.append(
        "high-risk changes require a plan file in docs/03_Plans/active/ "
        f"or docs/03_Plans/completed/ in the same diff: {formatted_files}"
    )


def _commit_files(sha: str) -> list[str]:
    return _git_names("diff-tree", "--no-commit-id", "--name-only", "-r", sha)


def _check_per_commit_plan_lifecycle(failures: list[str], base: str) -> None:
    """Simulate the push-event gate: every commit in base..HEAD that touches a
    high-risk path must also touch a plan file in that SAME commit.

    The GitHub push check, for a new branch, evaluates only the tip commit
    (diff-tree of after_sha), so a high-risk file and its plan must share a commit.
    Validating every commit this way guarantees the push passes regardless of how
    GitHub computes the diff — and catches it before pushing (no failed-CI email).
    """
    shas = _git_names("rev-list", f"{base}..HEAD")
    for sha in shas:
        files = _commit_files(sha)
        high_risk = [path for path in files if _is_high_risk_path(path)]
        if high_risk and not any(_is_plan_file(path) for path in files):
            failures.append(
                f"commit {sha[:10]} changes high-risk paths without a plan file in "
                f"the same commit: {', '.join(high_risk[:6])}"
            )


# A gate in the tag-triggered publish workflow can only fail after an immutable
# tag exists, and the tag ruleset forbids moving or deleting one.
PUBLISH_FORBIDDEN_INVOKE_TASKS = ("artifact-upgrade-test",)


def _check_publish_gate_placement(failures: list[str]) -> None:
    """Long-running validation gates must run before the tag, not after it.

    `artifact-upgrade-test` was wired into `release.yml`, where the earliest it
    can run is after the tag has been pushed. It failed twice for one-line
    reasons — a tagless checkout, then resolving a version that was tagged but
    never published — and each failure permanently consumed a version number,
    because `v*` tags cannot be deleted or moved.

    The defects were trivial; the placement is what made them expensive.

    The gate moved again when the CI workflows were removed: it now has to run
    in the local `invoke ci` flow, because that is the only thing left that runs
    before a tag exists. Removing `.github/workflows/ci.yml` without moving it
    would have left the upgrade path validated nowhere, which is the same defect
    in a new place.
    """
    release_path = ".github/workflows/release.yml"
    release = REPO_ROOT / release_path
    if release.exists():
        text = release.read_text(encoding="utf-8")
        for task in PUBLISH_FORBIDDEN_INVOKE_TASKS:
            if f"invoke {task}" in text:
                failures.append(
                    f"{release_path} runs `{task}`, which can then only fail "
                    "after the release tag exists and cannot be moved. Run it in "
                    "the local `ci` task, where it blocks the release instead."
                )
    tasks_path = REPO_ROOT / "tasks.py"
    if tasks_path.exists():
        text = tasks_path.read_text(encoding="utf-8")
        pre_list = text.rsplit("@task(", 1)[-1].split("def ci(", 1)[0]
        for task in PUBLISH_FORBIDDEN_INVOKE_TASKS:
            if task.replace("-", "_") not in pre_list:
                failures.append(
                    f"tasks.py `ci` no longer runs `{task}`; with the CI "
                    "workflows removed the upgrade path would be validated "
                    "nowhere before publication."
                )


def _check_compose_health_probe(failures: list[str]) -> None:
    relative_path = "development/docker-compose.yml"
    path = REPO_ROOT / relative_path
    if not path.exists():
        return
    try:
        rendered = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        actual = rendered["services"]["netbox"]["healthcheck"]["test"]
    except (KeyError, TypeError, yaml.YAMLError) as exc:
        failures.append(f"{relative_path} has no parseable netbox health probe: {exc}")
        return
    if actual != EXPECTED_NETBOX_HEALTHCHECK:
        failures.append(
            f"{relative_path} services.netbox.healthcheck.test must equal "
            f"{EXPECTED_NETBOX_HEALTHCHECK!r}; got {actual!r}"
        )


def _check_worker_autoreload_contract(failures: list[str]) -> None:
    relative_path = "development/docker-compose.yml"
    path = REPO_ROOT / relative_path
    if not path.exists():
        return
    try:
        rendered = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        worker = rendered["services"]["netbox-worker"]
        environment = worker["environment"]
        command = "\n".join(str(part) for part in worker["command"])
    except (KeyError, TypeError, yaml.YAMLError) as exc:
        failures.append(
            f"{relative_path} has no parseable worker autoreload contract: {exc}"
        )
        return
    expected_environment = "${FORWARD_NETBOX_WORKER_AUTORELOAD:-1}"
    if environment.get("FORWARD_NETBOX_WORKER_AUTORELOAD") != expected_environment:
        failures.append(
            f"{relative_path} must pass the worker autoreload setting into the container"
        )
    expected_runtime_expansion = "$${FORWARD_NETBOX_WORKER_AUTORELOAD:-1}"
    if expected_runtime_expansion not in command:
        failures.append(
            f"{relative_path} must defer worker autoreload expansion to the container"
        )


def _check_development_secret_boundary(failures: list[str]) -> None:
    tracked = set(_git_names("ls-files", "--cached")) - set(
        _git_names("ls-files", "--deleted")
    )
    for relative_path in sorted(FORBIDDEN_TRACKED_DEVELOPMENT_ENV_FILES & tracked):
        failures.append(
            f"development credential file must not be tracked: {relative_path}"
        )

    for relative_path in sorted(tracked):
        if not (
            relative_path.startswith("development/env/")
            or relative_path == "development/.env.example"
        ):
            continue
        path = REPO_ROOT / relative_path
        if not path.is_file():
            continue
        match = FORBIDDEN_DEVELOPMENT_SECRET_ASSIGNMENT.search(
            path.read_text(encoding="utf-8")
        )
        if match:
            line = path.read_text(encoding="utf-8").count("\n", 0, match.start()) + 1
            failures.append(
                f"{relative_path}:{line} must not contain a tracked secret assignment"
            )

    netbox_env_path = REPO_ROOT / "development/env/netbox.env"
    if not netbox_env_path.is_file():
        failures.append(
            "development/env/netbox.env must exist with the release worker timeout"
        )
    else:
        timeout_values = re.findall(
            r"^RQ_DEFAULT_TIMEOUT=(\d+)\s*$",
            netbox_env_path.read_text(encoding="utf-8"),
            re.MULTILINE,
        )
        if timeout_values != [EXPECTED_DEVELOPMENT_RQ_DEFAULT_TIMEOUT]:
            failures.append(
                "development/env/netbox.env must set exactly one "
                f"RQ_DEFAULT_TIMEOUT={EXPECTED_DEVELOPMENT_RQ_DEFAULT_TIMEOUT}"
            )

    compose_path = REPO_ROOT / "development/docker-compose.yml"
    if compose_path.is_file():
        try:
            compose = yaml.safe_load(compose_path.read_text(encoding="utf-8")) or {}
            declared = set(compose["secrets"])
            netbox_secrets = compose["services"]["netbox"]["secrets"]
            postgres = compose["services"]["postgres"]
            redis = compose["services"]["redis"]
        except (KeyError, TypeError, yaml.YAMLError) as exc:
            failures.append(
                "development/docker-compose.yml must define parseable development "
                f"secrets: {exc}"
            )
        else:
            required = {
                "api_token_pepper_1",
                "db_password",
                "redis_password",
                "secret_key",
            }
            if declared != required:
                failures.append(
                    "development/docker-compose.yml must declare exactly the four "
                    "generated development secrets"
                )
            if (
                "db_password" not in netbox_secrets
                or "secret_key" not in netbox_secrets
            ):
                failures.append(
                    "netbox must mount generated database and application secrets"
                )
            if postgres.get("environment", {}).get("POSTGRES_PASSWORD_FILE") != (
                "/run/secrets/db_password"
            ):
                failures.append("postgres must read its password from db_password")
            redis_command = "\n".join(str(part) for part in redis.get("command", []))
            if "/run/secrets/redis_password" not in redis_command:
                failures.append("redis must read its password from redis_password")

    dockerignore_path = REPO_ROOT / ".dockerignore"
    if dockerignore_path.is_file() and "development/secrets" not in {
        line.strip()
        for line in dockerignore_path.read_text(encoding="utf-8").splitlines()
    }:
        failures.append(".dockerignore must exclude development/secrets")


def _check_development_logging_boundary(failures: list[str]) -> None:
    logging_path = REPO_ROOT / "development/configuration/logging.py"
    if not logging_path.is_file():
        failures.append("development/configuration/logging.py must exist")
    else:
        logging_text = logging_path.read_text(encoding="utf-8")
        expected_default = (
            f'LOGLEVEL = environ.get("LOGLEVEL", "{EXPECTED_DEVELOPMENT_LOG_LEVEL}")'
        )
        if logging_text.count(expected_default) != 1:
            failures.append(
                "development/configuration/logging.py must default LOGLEVEL to "
                f"{EXPECTED_DEVELOPMENT_LOG_LEVEL}"
            )
        previous_loglevel = os.environ.get("LOGLEVEL")
        try:
            for expected_level in (EXPECTED_DEVELOPMENT_LOG_LEVEL, "WARNING"):
                os.environ["LOGLEVEL"] = expected_level
                try:
                    logging_config = runpy.run_path(str(logging_path))["LOGGING"]
                except Exception as exc:
                    failures.append(
                        "development/configuration/logging.py must define a loadable "
                        f"LOGGING dictionary: {exc}"
                    )
                    break
                for group, names in {
                    "handlers": ("console", "netbox_file", "forward_file"),
                    "loggers": (
                        "django",
                        "django_auth_ldap",
                        "netbox",
                        "netbox_branching",
                        "forward_netbox",
                    ),
                }.items():
                    configured = logging_config.get(group, {})
                    if any(
                        configured.get(name, {}).get("level") != expected_level
                        for name in names
                    ):
                        failures.append(
                            "development/configuration/logging.py required "
                            f"{group} must honor LOGLEVEL={expected_level}"
                        )
        finally:
            if previous_loglevel is None:
                os.environ.pop("LOGLEVEL", None)
            else:
                os.environ["LOGLEVEL"] = previous_loglevel

    override_path = REPO_ROOT / "development/docker-compose.override.yml"
    if not override_path.is_file():
        failures.append("development/docker-compose.override.yml must exist")
        return
    try:
        override = yaml.safe_load(override_path.read_text(encoding="utf-8")) or {}
        services = override["services"]
        levels = {
            service: services[service]["environment"]["LOGLEVEL"]
            for service in ("netbox", "netbox-worker")
        }
    except (KeyError, TypeError, yaml.YAMLError) as exc:
        failures.append(
            "development/docker-compose.override.yml must define parseable "
            f"NetBox log levels: {exc}"
        )
        return
    if set(levels.values()) != {EXPECTED_DEVELOPMENT_LOG_LEVEL}:
        failures.append(
            "development/docker-compose.override.yml must set netbox and "
            f"netbox-worker LOGLEVEL to {EXPECTED_DEVELOPMENT_LOG_LEVEL}"
        )


def _workflow_steps(relative_path: str, job_name: str) -> list[dict]:
    path = REPO_ROOT / relative_path
    if not path.exists():
        return []
    rendered = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    steps = rendered.get("jobs", {}).get(job_name, {}).get("steps", [])
    return [step for step in steps if isinstance(step, dict)]


def _workflow(relative_path: str) -> dict:
    path = REPO_ROOT / relative_path
    if not path.exists():
        return {}
    return yaml.safe_load(path.read_text(encoding="utf-8")) or {}


def _check_release_toolchain_lock(failures: list[str]) -> None:
    lock_path = REPO_ROOT / "requirements-release.txt"
    if not lock_path.exists():
        return
    lines = lock_path.read_text(encoding="utf-8").splitlines()
    entries: list[tuple[int, str]] = []
    for index, line in enumerate(lines):
        if line and not line[0].isspace() and not line.startswith("#"):
            entries.append((index, line))
    if not entries:
        failures.append("requirements-release.txt must contain pinned packages")
        return
    for position, (index, line) in enumerate(entries):
        if not re.fullmatch(r"[A-Za-z0-9_.-]+==[^\\\s]+ \\", line):
            failures.append(
                "requirements-release.txt entries must use exact versions: "
                f"line {index + 1}"
            )
            continue
        next_index = (
            entries[position + 1][0] if position + 1 < len(entries) else len(lines)
        )
        if not any("--hash=sha256:" in item for item in lines[index + 1 : next_index]):
            failures.append(
                "requirements-release.txt entries must carry SHA-256 hashes: "
                f"line {index + 1}"
            )

    release_text = (REPO_ROOT / ".github/workflows/release.yml").read_text(
        encoding="utf-8"
    )
    if "pip install --upgrade" in release_text:
        failures.append("release workflow must not install mutable latest tooling")
    if release_text.count("--require-hashes") < 2:
        failures.append("release workflow must hash-lock validation and build tooling")


def _check_release_anchor_tracks_current_release(failures: list[str]) -> None:
    """The provenance anchor must name the release the table calls current.

    Two post-release steps are easy to skip because nothing failed when they
    were: advancing `PRIOR_RELEASE_TAG`, and promoting the shipped release in
    the compatibility table. Skipping either is silent at the time and expensive
    later - a stale anchor grows the reviewed commit range until GitHub expires
    a run and burns a release at the tag, which is what happened to `v2.6.10`,
    and a stale table told operators for two releases that `2.6.9` was current
    while `2.6.12` and `2.7.0` sat as candidates.

    Both were previously pinned by hand, including a literal copy of the anchor
    in this file, so the checks moved only when someone remembered to move them.
    Tying the two together removes the hand-maintained copy and makes skipping
    either step fail here instead of at a tag months later.
    """
    provenance_path = REPO_ROOT / "scripts/verify_release_provenance.py"
    readme_path = REPO_ROOT / "README.md"
    if not provenance_path.exists() or not readme_path.exists():
        return

    anchor_match = re.search(
        r'^PRIOR_RELEASE_TAG = "(?P<tag>v[0-9]+\.[0-9]+\.[0-9]+)"$',
        provenance_path.read_text(encoding="utf-8"),
        re.MULTILINE,
    )
    if anchor_match is None:
        failures.append(
            "release provenance must pin PRIOR_RELEASE_TAG to a vX.Y.Z release tag"
        )
        return

    current = re.findall(
        r"^\| `(?P<version>v[0-9]+\.[0-9]+\.[0-9]+)` \|[^|]*\| Current release;",
        readme_path.read_text(encoding="utf-8"),
        re.MULTILINE,
    )
    if len(current) != 1:
        failures.append(
            "README release table must name exactly one current release, found "
            f"{len(current)}"
        )
        return

    if anchor_match.group("tag") != current[0]:
        failures.append(
            "release provenance anchor "
            f"{anchor_match.group('tag')} does not match the current release "
            f"{current[0]} in the README table: advance the anchor after a "
            "release, or promote the shipped release in the table"
        )


def _documentation_bridge_rule():
    """Return the release verifier's own post-release bridge path rule.

    The rule is loaded from the module beside this one rather than restated
    here. A second copy drifts the moment either side moves, and a harness
    check that passes while the verifier fails is worse than no check at all -
    the entire value of checking early is that the two agree.
    """
    path = Path(__file__).resolve().parent / "verify_release_provenance.py"
    spec = importlib.util.spec_from_file_location(
        "forward_netbox_release_provenance_rule",
        path,
    )
    if spec is None or spec.loader is None:
        raise ImportError(f"cannot load release provenance rule from {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module._is_documentation_path


def _check_post_release_bridge_is_documentation_only(failures: list[str]) -> None:
    """The commit the anchor pins must be a documentation-only bridge.

    `_require_prior_release_bridge` requires the first first-parent commit after
    `PRIOR_RELEASE_TAG` - the bridge - to touch documentation only. That slot is
    fixed by definition: the bridge *is* the first commit after the tag, so once
    a commit carrying anything else lands there no later commit can reclaim it,
    and the anchor cannot be re-pointed past it without reintroducing the
    expiring-review-range problem that spent `v2.6.10` and `v2.6.11`.

    `v2.7.0` was promoted without first being archived, so the promotion commit
    took the slot. Every release after it became unverifiable and `2.7.1` was
    blocked until the rule was widened - and widening made that ordering
    non-fatal, not correct. Nothing caught it at the time:
    `_check_release_anchor_tracks_current_release` asserts only that the anchor
    names the current release, never what the bridge contains, and the
    re-anchoring pull request pinned a commit that could never satisfy the
    check with nothing re-running the verifier.

    Running the verifier's own rule against the pinned pairing on every harness
    run moves that failure to where it is still one commit away from being
    fixed, instead of to a tag that cannot be moved or deleted.
    """
    provenance_path = REPO_ROOT / "scripts/verify_release_provenance.py"
    if not provenance_path.exists():
        return

    text = provenance_path.read_text(encoding="utf-8")
    tag_match = re.search(
        r'^PRIOR_RELEASE_TAG = "(?P<tag>v[0-9]+\.[0-9]+\.[0-9]+)"$',
        text,
        re.MULTILINE,
    )
    bridge_match = re.search(
        r'^PRIOR_POST_RELEASE_DOC_COMMIT = "(?P<commit>[0-9a-f]{40})"$',
        text,
        re.MULTILINE,
    )
    if tag_match is None or bridge_match is None:
        failures.append(
            "release provenance must pin PRIOR_RELEASE_TAG and a 40-character "
            "PRIOR_POST_RELEASE_DOC_COMMIT so the post-release bridge shape can "
            "be checked before a tag depends on it"
        )
        return

    tag = tag_match.group("tag")
    bridge = bridge_match.group("commit")

    try:
        is_documentation_path = _documentation_bridge_rule()
    except Exception as exc:
        failures.append(
            "release provenance bridge rule is not loadable, so the post-release "
            f"bridge cannot be checked: {exc}"
        )
        return

    changed = _git_names("diff", "--name-only", tag, bridge)
    if not changed:
        failures.append(
            f"post-release bridge {bridge[:10]} against {tag} lists no changed "
            "paths: check out the full history and tags (fetch-depth: 0) or "
            "correct the anchor. An unreadable or empty bridge is not evidence "
            "that the bridge is documentation-only"
        )
        return

    disqualifying = sorted(path for path in changed if not is_documentation_path(path))
    if disqualifying:
        failures.append(
            f"post-release bridge {bridge[:10]} after {tag} is not "
            "documentation-only; these paths disqualify it: "
            f"{', '.join(disqualifying)}. The bridge is the first first-parent "
            "commit after the tag, so this slot cannot be reclaimed by a later "
            "commit and the anchor cannot be re-pointed past it: the close-out "
            "must land its documentation commit (archive, then promote) as the "
            "first commit after the tag"
        )


def _check_standard_release_tag_flow(failures: list[str]) -> None:
    paths = {
        "release": REPO_ROOT / "scripts/release.py",
        "provenance": REPO_ROOT / "scripts/verify_release_provenance.py",
    }
    if any(not path.exists() for path in paths.values()):
        return
    texts = {name: path.read_text(encoding="utf-8") for name, path in paths.items()}
    for fragment in (
        "ensure_release_tag(tag, head_commit)",
        "_verify_live_release_controls()",
        '"--controls-only"',
        '"tag",',
        '"-a",',
        '"push", "origin", f"refs/tags/{tag}"',
        '"ls-remote",',
        'f"refs/tags/{tag}^{{}}"',
    ):
        if fragment not in texts["release"]:
            failures.append(f"standard release tag flow must contain: {fragment}")
    for fragment in (
        "PRIOR_RELEASE_TAG = ",
        "BOOTSTRAP_REQUIRED_FILES",
        "BOOTSTRAP_FILE_DIGESTS",
        'operation.add_argument("--controls-only", action="store_true")',
        '"merge-base", "--is-ancestor", release_commit, current_main',
    ):
        if fragment not in texts["provenance"]:
            failures.append(f"release provenance must contain: {fragment}")
    for fragment in (
        "trusted-tag.yml",
        "authorize_trusted_tag",
        "RELEASE_CONTROL_APP",
        "RELEASE_TAG_DEPLOY_KEY",
        "security-bootstrap-2.6",
    ):
        if any(fragment in text for text in texts.values()):
            failures.append(f"retired release controller remains: {fragment}")


def main() -> int:
    import argparse

    parser = argparse.ArgumentParser(description="Forward NetBox harness check.")
    parser.add_argument(
        "--base",
        help=(
            "Validate every commit in <base>..HEAD against the push-event plan "
            "gate (use before pushing, e.g. --base origin/main)."
        ),
    )
    args = parser.parse_args()

    failures: list[str] = []

    for relative_path in REQUIRED_PATHS:
        path = REPO_ROOT / relative_path
        if not path.exists():
            failures.append(f"missing required harness file: {relative_path}")

    for relative_path, required_fragments in REQUIRED_TEXT.items():
        path = REPO_ROOT / relative_path
        if not path.exists():
            continue
        text = path.read_text(encoding="utf-8")
        for fragment in required_fragments:
            if fragment not in text:
                failures.append(
                    f"{relative_path} must mention required fragment: {fragment}"
                )

    _check_plan_directory(failures, "docs/03_Plans/active")
    _check_plan_directory(failures, "docs/03_Plans/completed")
    _check_plan_lifecycle(failures)
    _check_agents_entrypoint(failures)
    _check_knowledge_freshness(failures)
    _check_retired_runtime_paths(failures)
    _check_compose_health_probe(failures)
    _check_worker_autoreload_contract(failures)
    _check_development_secret_boundary(failures)
    _check_development_logging_boundary(failures)
    _check_release_toolchain_lock(failures)
    _check_release_anchor_tracks_current_release(failures)
    _check_post_release_bridge_is_documentation_only(failures)
    _check_standard_release_tag_flow(failures)
    _check_publish_gate_placement(failures)
    if args.base:
        _check_per_commit_plan_lifecycle(failures, args.base)

    if failures:
        print("Harness check failed:")
        for failure in failures:
            print(f"- {failure}")
        return 1

    print("Harness check passed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
