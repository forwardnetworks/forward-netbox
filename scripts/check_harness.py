#!/usr/bin/env python3
from __future__ import annotations

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

REQUIRED_TEXT = {
    "AGENTS.md": [
        "ARCHITECTURE.md",
        "Agent Workflow",
        "invoke harness-check",
        "sensitive",
    ],
    "ARCHITECTURE.md": [
        "Production Boundaries",
        "Overgrown But Stable Areas",
        "Non-Negotiable Constraints",
    ],
    "docs/00_Project_Knowledge/validation-matrix.md": [
        "invoke harness-check",
        "invoke lint",
        "invoke check",
        "invoke test",
        "invoke docs",
        "scripts/check_sensitive_content.py --all-history",
    ],
    "docs/00_Project_Knowledge/agent-workflow.md": [
        "Choose The Lane",
        "Before Editing",
        "Before Commit",
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
    ],
    "docs/03_Plans/plan-template.md": [
        "Goal",
        "Validation",
        "Rollback",
        "Decision Log",
    ],
}


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

    if failures:
        print("Harness check failed:")
        for failure in failures:
            print(f"- {failure}")
        return 1

    print("Harness check passed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
