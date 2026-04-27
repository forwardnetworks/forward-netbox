#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]

REQUIRED_PATHS = [
    "AGENTS.md",
    "ARCHITECTURE.md",
    "docs/00_Project_Knowledge/README.md",
    "docs/00_Project_Knowledge/architecture.md",
    "docs/00_Project_Knowledge/agent-workflow.md",
    "docs/00_Project_Knowledge/code-boundary-map.md",
    "docs/00_Project_Knowledge/validation-matrix.md",
    "docs/00_Project_Knowledge/release-playbook.md",
    "docs/00_Project_Knowledge/local-docker-workflow.md",
    "docs/00_Project_Knowledge/quality-score.md",
    "docs/03_Plans/active/README.md",
    "docs/03_Plans/completed/README.md",
    "docs/03_Plans/plan-template.md",
    "docs/03_Plans/technical-debt.md",
    "scripts/tests/test_check_harness.py",
]

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
        "Overgrown But Stable Areas",
        "Non-Negotiable Constraints",
    ],
    "docs/00_Project_Knowledge/validation-matrix.md": [
        "invoke harness-check",
        "invoke harness-test",
        "invoke lint",
        "invoke check",
        "invoke scenario-test",
        "invoke test",
        "invoke docs",
        "scripts/check_sensitive_content.py --all-history",
    ],
    "docs/00_Project_Knowledge/agent-workflow.md": [
        "Choose The Lane",
        "Before Editing",
        "Before Commit",
        "invoke harness-test",
    ],
    "docs/00_Project_Knowledge/code-boundary-map.md": [
        "Forward API Boundary",
        "Branch Execution Boundary",
        "NetBox Adapter Boundary",
    ],
    "docs/00_Project_Knowledge/release-playbook.md": [
        "GitHub CI",
        "PyPI",
        "twine",
        "invoke harness-test",
    ],
    "docs/03_Plans/plan-template.md": [
        "Goal",
        "Validation",
        "Rollback",
        "Decision Log",
    ],
}


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


def main() -> int:
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

    if failures:
        print("Harness check failed:")
        for failure in failures:
            print(f"- {failure}")
        return 1

    print("Harness check passed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
