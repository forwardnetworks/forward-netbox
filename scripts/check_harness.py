#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import re
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
    ".github/workflows/harness-gardening.yml",
    ".github/workflows/codeql.yml",
    ".github/workflows/trusted-sensitive-pr.yml",
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
FORBIDDEN_TRACKED_DEVELOPMENT_ENV_FILES = {
    "development/.env",
    "development/env/redis.env",
}
FORBIDDEN_DEVELOPMENT_SECRET_ASSIGNMENT = re.compile(
    r"^(?:API_TOKEN_PEPPER_\d+|DB_PASSWORD|POSTGRES_PASSWORD|"
    r"REDIS(?:_CACHE)?_PASSWORD|SECRET_KEY)\s*=",
    re.MULTILINE,
)

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
    ".github/workflows/ci.yml": [
        # Version-agnostic: Dependabot bumps the action major, and pinning a
        # version here would red every such PR + main after merge.
        "actions/checkout@",
        "actions/setup-python@",
        "FORCE_JAVASCRIPT_ACTIONS_TO_NODE24",
        "contents: read",
        "Run harness tests",
        "Run NetBox database migrations",
        "Run synthetic scenario tests",
        "fetch-depth: 0",
        "--require-baseline-env",
        "--require-env-patterns",
        "FORWARD_SENSITIVE_HISTORY_BASELINE",
    ],
    ".github/workflows/release.yml": [
        "fetch-depth: 0",
        "refs/tags/v2.5.11",
        "verify_release_provenance.py",
        "--git-files",
        "--protected-history",
        "--require-env-patterns --require-baseline-env",
        "FORWARD_SENSITIVE_HISTORY_BASELINE",
        "--require-hashes",
        "requirements-release.txt",
        "scripts/build_reproducible_distribution.py",
    ],
    ".github/workflows/harness-gardening.yml": [
        "schedule:",
        "scripts/check_harness.py",
        "test_check_harness.py",
    ],
    ".github/workflows/trusted-sensitive-pr.yml": [
        "pull_request_target:",
        "persist-credentials: false",
        "github.event.pull_request.base.sha",
        "statuses: write",
        "Trusted sensitive-content scan",
        "target_url",
        "actions/runs/",
        "--git-tree",
        "--ref-name",
        "--require-env-patterns --require-baseline-env",
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

    workflow_path = REPO_ROOT / ".github/workflows/ci.yml"
    if workflow_path.is_file():
        workflow_text = workflow_path.read_text(encoding="utf-8")
        generator = "python scripts/generate_development_secrets.py"
        compose_build = "docker compose --project-name forward-netbox"
        if generator not in workflow_text or workflow_text.index(
            generator
        ) > workflow_text.index(compose_build):
            failures.append(
                "CI must generate development secrets before Docker Compose"
            )

    dockerignore_path = REPO_ROOT / ".dockerignore"
    if dockerignore_path.is_file() and "development/secrets" not in {
        line.strip()
        for line in dockerignore_path.read_text(encoding="utf-8").splitlines()
    }:
        failures.append(".dockerignore must exclude development/secrets")


def _check_trusted_private_fetch(failures: list[str]) -> None:
    path = REPO_ROOT / ".github/workflows/trusted-sensitive-pr.yml"
    if not path.is_file():
        return
    text = path.read_text(encoding="utf-8")
    required = (
        "GH_TOKEN: ${{ secrets.GITHUB_TOKEN }}",
        'extraheader="http.https://github.com/.extraheader"',
        '"${extraheader}=AUTHORIZATION: basic ${auth_header}"',
        'test "$(git rev-parse FETCH_HEAD)" = "${PR_HEAD_SHA}"',
    )
    for fragment in required:
        if fragment not in text:
            failures.append(
                ".github/workflows/trusted-sensitive-pr.yml must authenticate "
                f"private candidate fetches and bind FETCH_HEAD: missing {fragment}"
            )


def _check_harness_gardening_dependency(failures: list[str]) -> None:
    relative_path = ".github/workflows/harness-gardening.yml"
    path = REPO_ROOT / relative_path
    if not path.exists():
        return
    try:
        rendered = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        steps = rendered["jobs"]["audit"]["steps"]
        commands = [step.get("run") for step in steps if isinstance(step, dict)]
        dependency_index = commands.index(EXPECTED_HARNESS_DEPENDENCY_COMMAND)
        harness_index = commands.index(EXPECTED_HARNESS_CHECK_COMMAND)
    except (KeyError, TypeError, ValueError, yaml.YAMLError) as exc:
        failures.append(
            f"{relative_path} must install PyYAML 6.0.3 before the harness check: {exc}"
        )
        return
    if dependency_index >= harness_index:
        failures.append(
            f"{relative_path} must install PyYAML 6.0.3 before the harness check"
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


def _check_sensitive_guard_wiring(failures: list[str]) -> None:
    try:
        ci_steps = _workflow_steps(".github/workflows/ci.yml", "validate")
        release_workflow = _workflow(".github/workflows/release.yml")
        release_steps = _workflow_steps(".github/workflows/release.yml", "validate")
        trusted_workflow = _workflow(".github/workflows/trusted-sensitive-pr.yml")
        trusted_steps = _workflow_steps(
            ".github/workflows/trusted-sensitive-pr.yml",
            "sensitive-content",
        )
    except yaml.YAMLError as exc:
        failures.append(f"sensitive-content workflows must parse as YAML: {exc}")
        return

    for relative_path, steps in (
        (".github/workflows/ci.yml", ci_steps),
        (".github/workflows/release.yml", release_steps),
        (".github/workflows/trusted-sensitive-pr.yml", trusted_steps),
    ):
        checkout_steps = [
            step
            for step in steps
            if str(step.get("uses", "")).startswith("actions/checkout@")
        ]
        if (
            not checkout_steps
            or checkout_steps[0].get("with", {}).get("fetch-depth") != 0
        ):
            failures.append(f"{relative_path} sensitive scan requires fetch-depth: 0")

    def normalized_commands(steps: list[dict]) -> str:
        raw = "\n".join(str(step.get("run", "")) for step in steps)
        return " ".join(raw.replace("\\\n", " ").split())

    ci_commands = normalized_commands(ci_steps)
    release_commands = normalized_commands(release_steps)
    trusted_commands = normalized_commands(trusted_steps)
    required_ci_fragments = (
        "check_sensitive_content.py --protected-history",
        "check_sensitive_content.py --git-files",
        "--require-baseline-env",
        "--require-env-patterns",
    )
    for fragment in required_ci_fragments:
        if fragment not in ci_commands:
            failures.append(f".github/workflows/ci.yml must execute {fragment}")
    for fragment in (
        "refs/tags/v2.5.11",
        "verify_release_provenance.py",
        "--tag",
        "check_sensitive_content.py --git-files --protected-history",
        "--require-env-patterns --require-baseline-env",
    ):
        if fragment not in release_commands:
            failures.append(f".github/workflows/release.yml must execute {fragment}")
    release_permissions = release_workflow.get("permissions", {})
    for permission in ("actions", "contents", "pull-requests", "statuses"):
        if not isinstance(release_permissions, dict) or (
            release_permissions.get(permission) != "read"
        ):
            failures.append(
                f".github/workflows/release.yml must grant {permission}: read"
            )

    for fragment in (
        "pull/${PR_NUMBER}/head",
        "check_sensitive_content.py --rev-list",
        "--git-tree",
        "--ref-name",
        "--require-env-patterns --require-baseline-env",
        "actions/runs/",
    ):
        if fragment not in trusted_commands:
            failures.append(
                f".github/workflows/trusted-sensitive-pr.yml must execute {fragment}"
            )

    trusted_events = trusted_workflow.get("on", {})
    target = (
        trusted_events.get("pull_request_target", {})
        if isinstance(trusted_events, dict)
        else {}
    )
    if set(target.get("types", [])) != {"opened", "reopened", "synchronize"}:
        failures.append(
            ".github/workflows/trusted-sensitive-pr.yml must use only the reviewed "
            "pull_request_target event types"
        )
    permissions = trusted_workflow.get("permissions", {})
    if not isinstance(permissions, dict) or permissions.get("statuses") != "write":
        failures.append(
            ".github/workflows/trusted-sensitive-pr.yml must have statuses: write "
            "to bind the trusted result to the candidate SHA"
        )

    def command_step(steps: list[dict], fragment: str) -> tuple[int, dict] | None:
        for index, step in enumerate(steps):
            command = " ".join(str(step.get("run", "")).replace("\\\n", " ").split())
            if fragment in command:
                return index, step
        return None

    required_steps = (
        (ci_steps, "--protected-history", "ci history"),
        (ci_steps, "--require-env-patterns", "ci push enforcement"),
        (release_steps, "--require-env-patterns", "release enforcement"),
        (trusted_steps, "--git-tree", "trusted PR enforcement"),
    )
    for steps, fragment, label in required_steps:
        found = command_step(steps, fragment)
        if found is None:
            continue
        _index, step = found
        condition = str(step.get("if", "")).strip()
        if label == "ci push enforcement":
            if condition != "github.event_name == 'push'":
                failures.append("CI private-pattern enforcement must run on every push")
        elif condition:
            failures.append(f"{label} must not be conditional")

    expected_env = {
        "FORWARD_SENSITIVE_PATTERNS": "${{ secrets.FORWARD_SENSITIVE_PATTERNS }}",
        "FORWARD_SENSITIVE_HISTORY_BASELINE": (
            "${{ vars.FORWARD_SENSITIVE_HISTORY_BASELINE }}"
        ),
    }
    for steps, fragment, label in (
        (ci_steps, "--require-env-patterns", "CI"),
        (release_steps, "--require-env-patterns", "release"),
        (trusted_steps, "--git-tree", "trusted PR"),
    ):
        found = command_step(steps, fragment)
        if found is None:
            continue
        _index, step = found
        environment = step.get("env", {})
        for name, expected_value in expected_env.items():
            if str(environment.get(name, "")).strip() != expected_value:
                failures.append(
                    f"{label} sensitive scan must source {name} from trusted settings"
                )

    trusted_checkout = next(
        (
            step
            for step in trusted_steps
            if str(step.get("uses", "")).startswith("actions/checkout@")
        ),
        {},
    )
    checkout_with = trusted_checkout.get("with", {})
    if (
        checkout_with.get("persist-credentials") is not False
        or str(checkout_with.get("ref", "")).strip()
        != "${{ github.event.pull_request.base.sha }}"
    ):
        failures.append(
            "trusted PR scan must check out only the credential-free base revision"
        )

    trusted_fetch = command_step(trusted_steps, "pull/${PR_NUMBER}/head")
    trusted_scan = command_step(trusted_steps, "--git-tree")
    if trusted_fetch and trusted_scan and trusted_fetch[0] >= trusted_scan[0]:
        failures.append("trusted PR scan must fetch candidate objects before scanning")

    trusted_status = command_step(trusted_steps, "Trusted sensitive-content scan")
    if trusted_status is None:
        failures.append("trusted PR scan must publish a candidate commit status")
    else:
        status_index, status_step = trusted_status
        if trusted_scan and status_index <= trusted_scan[0]:
            failures.append("trusted PR status must be published after candidate scan")
        if str(status_step.get("if", "")).strip() != "always()":
            failures.append("trusted PR status publication must run with if: always()")
        status_environment = status_step.get("env", {})
        if str(status_environment.get("GH_TOKEN", "")).strip() != (
            "${{ secrets.GITHUB_TOKEN }}"
        ):
            failures.append("trusted PR status must use the repository GITHUB_TOKEN")
        if str(status_environment.get("SCAN_OUTCOME", "")).strip() != (
            "${{ steps.scan.outcome }}"
        ):
            failures.append("trusted PR status must derive from the scanner outcome")

    tasks_path = REPO_ROOT / "tasks.py"
    if tasks_path.exists():
        tasks_text = tasks_path.read_text(encoding="utf-8")
        for fragment in (
            'scripts/check_sensitive_content.py")',
            "scripts/check_sensitive_content.py --protected-history",
        ):
            if fragment not in tasks_text:
                failures.append(f"tasks.py sensitive-check must execute {fragment}")


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
    ci_text = (REPO_ROOT / ".github/workflows/ci.yml").read_text(encoding="utf-8")
    if "pip install --upgrade" in release_text:
        failures.append("release workflow must not install mutable latest tooling")
    if release_text.count("--require-hashes") < 2:
        failures.append("release workflow must hash-lock validation and build tooling")
    if "--require-hashes" not in ci_text:
        failures.append("CI workflow must install the reviewed hash-locked toolchain")


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
        'PRIOR_RELEASE_TAG = "v2.5.11"',
        "BOOTSTRAP_REQUIRED_FILES",
        "BASE_REQUIRED_STATUS_CHECKS",
        "TRUSTED_STATUS_CONTEXT",
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
    _check_development_secret_boundary(failures)
    _check_trusted_private_fetch(failures)
    _check_harness_gardening_dependency(failures)
    _check_sensitive_guard_wiring(failures)
    _check_release_toolchain_lock(failures)
    _check_standard_release_tag_flow(failures)
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
