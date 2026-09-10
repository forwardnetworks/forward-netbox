#!/usr/bin/env python3
# Operator-facing text may not tell someone to run a command.
#
# A customer read a count of 552 uncovered devices off a panel that offered no
# way to act on it, because the remediation shipped as a management-command
# flag. Everything an operator needs to uncover a problem AND fix it belongs in
# the GUI; a command is for scripting, scheduling and CI.
#
# A memory and a plan do not prevent that recurring. This does. It scans the
# strings an operator actually reads - health remediation, form help text,
# failure messages that reach a job log or an issue row, and every template -
# and fails when one names a shell.
#
# What it deliberately does NOT scan:
#   * `forward_netbox/management/` - a command's own help text names itself, and
#     commands remain supported for scripting.
#   * `forward_netbox/tests/` and `scripts/` - developer surfaces.
#   * comments and docstrings - developer-facing by definition. The rule is
#     about what reaches a browser or a job log, and a comment explaining WHY a
#     command exists is worth keeping.
from __future__ import annotations

import argparse
import ast
import re
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]

# Things that genuinely cannot be done from the NetBox GUI. Each entry needs a
# reason, because the temptation is to widen this list instead of building a
# surface - which is exactly how the rule stopped being true last time.
ALLOWED = (
    # Applying migrations requires shell access to the runtime by definition;
    # NetBox itself has no in-app migration runner.
    "manage.py migrate",
    # Installing or upgrading the package is a deployment action, not an
    # operator diagnostic.
    "pip install",
    # `manage.py check` is Django's own startup validation, run at deploy time.
    "manage.py check",
)

FORBIDDEN = (
    re.compile(r"\bmanage\.py\b"),
    re.compile(r"\binvoke\s+[a-z]"),
    re.compile(r"\bdocker\s+compose\b"),
    re.compile(r"\brqworker\b"),
)


def _command_names(repo_root: Path) -> tuple[str, ...]:
    """This plugin's own command names, read from disk rather than guessed.

    A pattern over `forward_*` matched database constraint names
    (`forward_netbox_sync`) as readily as commands. The directory listing is
    exact and stays correct when a command is added or removed.
    """
    commands = repo_root / "forward_netbox" / "management" / "commands"
    if not commands.is_dir():
        return ()
    return tuple(
        sorted(
            (path.stem for path in commands.glob("*.py") if not path.stem.startswith("_")),
            key=len,
            reverse=True,
        )
    )

SCAN_PY = ("forward_netbox",)
SKIP_PY_DIRS = ("management", "tests", "migrations")
SCAN_TEMPLATES = ("forward_netbox/templates",)


def _offending(text: str, commands: tuple[str, ...] = ()) -> str | None:
    """The forbidden fragment in `text`, or None once allowances are removed."""
    cleaned = text
    for allowed in ALLOWED:
        cleaned = cleaned.replace(allowed, "")
    for pattern in FORBIDDEN:
        match = pattern.search(cleaned)
        if match:
            return match.group(0)
    for name in commands:
        if name in cleaned:
            return name
    return None


def _python_string_findings(path: Path, commands: tuple[str, ...]) -> list[tuple[int, str, str]]:
    """Forbidden fragments in string literals, ignoring docstrings."""
    try:
        tree = ast.parse(path.read_text(encoding="utf-8"))
    except SyntaxError:
        return []

    # Docstrings are developer-facing; collect them so they can be skipped by
    # identity rather than by re-matching their text somewhere else.
    docstrings = set()
    for node in ast.walk(tree):
        if isinstance(node, (ast.Module, ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)):
            body = getattr(node, "body", None) or []
            if body and isinstance(body[0], ast.Expr) and isinstance(body[0].value, ast.Constant):
                if isinstance(body[0].value.value, str):
                    docstrings.add(id(body[0].value))

    findings = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Constant) or not isinstance(node.value, str):
            continue
        if id(node) in docstrings:
            continue
        fragment = _offending(node.value, commands)
        if fragment:
            findings.append((node.lineno, fragment, node.value.strip()))
    return findings


def _template_findings(path: Path, commands: tuple[str, ...]) -> list[tuple[int, str, str]]:
    findings = []
    for number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        fragment = _offending(line, commands)
        if fragment:
            findings.append((number, fragment, line.strip()))
    return findings


def scan(repo_root: Path) -> list[str]:
    problems = []
    commands = _command_names(repo_root)

    for root in SCAN_PY:
        for path in sorted((repo_root / root).rglob("*.py")):
            relative = path.relative_to(repo_root)
            if any(part in SKIP_PY_DIRS for part in relative.parts):
                continue
            for line, fragment, text in _python_string_findings(path, commands):
                problems.append(f"{relative}:{line}: names `{fragment}` in operator-facing text: {text[:160]}")

    for root in SCAN_TEMPLATES:
        for path in sorted((repo_root / root).rglob("*.html")):
            relative = path.relative_to(repo_root)
            for line, fragment, text in _template_findings(path, commands):
                problems.append(f"{relative}:{line}: names `{fragment}` in a template: {text[:160]}")

    return problems


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", default=str(REPO_ROOT))
    args = parser.parse_args(argv)

    problems = scan(Path(args.repo_root))
    if not problems:
        print("Operator-facing text names no management commands.")
        return 0

    print("Operator-facing text may not tell someone to run a command:")
    for problem in problems:
        print(f"  {problem}")
    print(
        "\nAn operator reads these. Give them a GUI control to click, or point "
        "them at the support bundle, rather than a shell. Widen ALLOWED only "
        "for something the GUI genuinely cannot do."
    )
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
