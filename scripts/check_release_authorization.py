#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
import shlex
import subprocess
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
PLAN_ROOT = REPO_ROOT / "docs/03_Plans"
REQUIRED_EVIDENCE_IDS = {
    "final-tree-full-gate",
    "exact-runtime-artifact",
    "scale-and-failure",
    "ui-validation",
    "ownership-audit",
    "customer-equivalent-acceptance",
    "independent-review",
}
ENTRY_RE = re.compile(
    r"^- \[(?P<checked>[ xX])\] `(?P<id>[a-z0-9-]+)` - (?P<evidence>.+)$"
)
PLACEHOLDER_RE = re.compile(
    r"\b(?:pending|tbd|todo|not run|not rerun|does not approve|historical only)\b",
    re.IGNORECASE,
)
COMMAND_RE = re.compile(r"`[^`\n]+`")
RETROSPECTIVE_OUTCOME_RE = re.compile(
    r"\b(?:passed|green|succeeded|successful|consistent|release_ready|"
    r"no findings|no blockers|0 failures|0 errors)\b",
    re.IGNORECASE,
)
PROSPECTIVE_RE = re.compile(
    r"\b(?:must|will|should|needs? to|required|to be (?:run|rerun|completed)|"
    r"to pass)\b",
    re.IGNORECASE,
)
DIGIT_RE = re.compile(r"\d")
CONTRADICTORY_OUTCOME_RE = re.compile(
    r"\b(?:failed|errored|unsuccessful)\b|"
    r"\b[1-9]\d*\s+(?:failures?(?!\s+scenarios?\b)|errors?|blockers?|"
    r"inconsistencies|open branches)\b",
    re.IGNORECASE,
)
SHELL_CONTROL_RE = re.compile(r"[;&|><`$()]")
EVIDENCE_BASE_RE = re.compile(
    r"^- Evidence base commit: `(?P<commit>[0-9a-f]{40})`$",
    re.MULTILINE,
)
EVIDENCE_REQUIRED_TEXT_PATTERNS = {
    "exact-runtime-artifact": (
        re.compile(r"\bNetBox\s+4\.6\.5\b", re.IGNORECASE),
        re.compile(r"\bBranching\s+1\.1\.1\b", re.IGNORECASE),
        re.compile(r"\bPython\s+3\.14\b", re.IGNORECASE),
        re.compile(r"\bSBOM\b", re.IGNORECASE),
    ),
    "ui-validation": (
        re.compile(r"\bdesktop\b", re.IGNORECASE),
        re.compile(r"\bmobile\b", re.IGNORECASE),
    ),
    "customer-equivalent-acceptance": (
        re.compile(r"\bsync\s+(?:id\s*[=:]?\s*)?#?\d+\b", re.IGNORECASE),
    ),
    "independent-review": (
        re.compile(r"\bindependent\s+(?:review|reviewer)\b", re.IGNORECASE),
        re.compile(r"\b0\s+(?:blockers?|findings?)\b", re.IGNORECASE),
    ),
}
RELEASE_RUNTIME_ENVIRONMENT = {
    "FORWARD_NETBOX_DOCKER_PROJECT": "forward-netbox-release-gate",
    "FORWARD_NETBOX_POSTGRES_DATA_PATH": "netbox-postgres-data",
    "FORWARD_NETBOX_WORKER_AUTORELOAD": "0",
    "NETBOX_VER": "v4.6.5",
}
ACCEPTANCE_RUNTIME_ENVIRONMENT = {
    "FORWARD_NETBOX_DOCKER_PROJECT": "forward-netbox-upgrade26",
    "FORWARD_NETBOX_POSTGRES_DATA_PATH": "netbox-postgres-data",
    "FORWARD_NETBOX_WORKER_AUTORELOAD": "0",
    "NETBOX_VER": "v4.6.5",
}


def _evidence_commands(evidence: str) -> list[list[str]]:
    commands = []
    for match in COMMAND_RE.finditer(evidence):
        command = match.group(0)[1:-1].strip()
        if not command or SHELL_CONTROL_RE.search(command):
            continue
        try:
            tokens = shlex.split(command)
        except ValueError:
            continue
        if tokens and tokens[0] == "rtk":
            commands.append(tokens)
    return commands


def _safe_environment_assignment(assignment: str) -> bool:
    name, separator, value = assignment.partition("=")
    if not separator:
        return False
    if name == "FORWARD_NETBOX_DOCKER_PROJECT":
        return value in {
            "forward-netbox-release-gate",
            "forward-netbox-upgrade26",
        }
    if name == "FORWARD_NETBOX_HOST_PORT":
        return value.isdigit() and 1 <= int(value) <= 65535
    if name == "NETBOX_URL":
        match = re.fullmatch(r"http://127\.0\.0\.1:(?P<port>[1-9]\d{0,4})", value)
        return bool(match and int(match.group("port")) <= 65535)
    return (name, value) in {
        ("NETBOX_VER", "v4.6.5"),
        ("FORWARD_NETBOX_WORKER_AUTORELOAD", "0"),
        ("FORWARD_NETBOX_POSTGRES_DATA_PATH", "netbox-postgres-data"),
    }


def _rtk_parts(tokens: list[str]) -> tuple[list[str], dict[str, str]] | None:
    tail = list(tokens[1:])
    if tail[:1] != ["env"]:
        return tail, {}
    tail = tail[1:]
    environment: dict[str, str] = {}
    while tail and re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*=.*", tail[0]):
        if not _safe_environment_assignment(tail[0]):
            return None
        name, _separator, value = tail[0].partition("=")
        if name in environment:
            return None
        environment[name] = value
        tail = tail[1:]
    if not environment:
        return None
    host_port = environment.get("FORWARD_NETBOX_HOST_PORT")
    netbox_url = environment.get("NETBOX_URL")
    if bool(host_port) != bool(netbox_url):
        return None
    if host_port and netbox_url.rsplit(":", 1)[-1] != host_port:
        return None
    return tail, environment


def _rtk_tail(tokens: list[str]) -> list[str] | None:
    parts = _rtk_parts(tokens)
    return parts[0] if parts is not None else None


def _environment_matches(
    command: list[str],
    expected: dict[str, str],
    *,
    require_url: bool = False,
) -> bool:
    parts = _rtk_parts(command)
    if parts is None:
        return False
    _tail, environment = parts
    if require_url:
        if set(environment) != {
            *expected,
            "FORWARD_NETBOX_HOST_PORT",
            "NETBOX_URL",
        }:
            return False
        return all(environment.get(key) == value for key, value in expected.items())
    return environment == expected


def _parse_command_options(
    arguments: list[str],
    *,
    value_options: set[str],
    flag_options: set[str] | None = None,
) -> tuple[dict[str, str], set[str]] | None:
    values: dict[str, str] = {}
    flags: set[str] = set()
    allowed_flags = flag_options or set()
    index = 0
    while index < len(arguments):
        option = arguments[index]
        if option in allowed_flags and option not in flags:
            flags.add(option)
            index += 1
            continue
        if option not in value_options or option in values:
            return None
        if index + 1 >= len(arguments) or arguments[index + 1].startswith("-"):
            return None
        values[option] = arguments[index + 1]
        index += 2
    return values, flags


def _invoke_arguments(command: list[str], task: str) -> list[str] | None:
    tail = _rtk_tail(command)
    if tail is None or tail[:2] != ["invoke", task]:
        return None
    return tail[2:]


def _has_exact_invoke_task(
    commands: list[list[str]],
    task: str,
    *,
    environment: dict[str, str],
    require_url: bool = False,
) -> bool:
    return any(
        _invoke_arguments(command, task) == []
        and _environment_matches(command, environment, require_url=require_url)
        for command in commands
    )


def _has_scale_soak_command(commands: list[list[str]]) -> bool:
    for command in commands:
        arguments = _invoke_arguments(command, "scale-soak")
        if arguments is None or not _environment_matches(
            command,
            ACCEPTANCE_RUNTIME_ENVIRONMENT,
        ):
            continue
        parsed = _parse_command_options(
            arguments,
            value_options={
                "--runs",
                "--max-changes-per-staging-item",
                "--pause-seconds",
            },
        )
        if parsed is None:
            continue
        values, _flags = parsed
        try:
            runs = int(values.get("--runs", "3"))
            max_changes = int(values.get("--max-changes-per-staging-item", "10000"))
            pause_seconds = int(values.get("--pause-seconds", "30"))
        except ValueError:
            continue
        if runs >= 3 and max_changes > 0 and pause_seconds >= 0:
            return True
    return False


def _has_sync_release_gate_command(commands: list[list[str]]) -> bool:
    for command in commands:
        arguments = _invoke_arguments(command, "sync-release-gate")
        if arguments is None or not _environment_matches(
            command,
            ACCEPTANCE_RUNTIME_ENVIRONMENT,
        ):
            continue
        parsed = _parse_command_options(
            arguments,
            value_options={
                "--sync-ids",
                "--max-polls",
                "--interval-seconds",
                "--output-prefix",
            },
            flag_options={"--include-all-ingestions"},
        )
        if parsed is None:
            continue
        values, _flags = parsed
        if not re.fullmatch(r"[1-9]\d*(?:,[1-9]\d*)*", values.get("--sync-ids", "")):
            continue
        try:
            max_polls = int(values.get("--max-polls", "6"))
            interval_seconds = int(values.get("--interval-seconds", "10"))
        except ValueError:
            continue
        if max_polls > 0 and interval_seconds >= 0:
            return True
    return False


def _is_ownership_audit_command(command: list[str]) -> bool:
    tail = _rtk_tail(command)
    if tail is None or tail[:2] != ["docker", "compose"]:
        return False
    arguments = tail[2:]
    index = 0
    compose_value_options = {
        "-p",
        "--project-name",
        "--project-directory",
        "-f",
        "--file",
    }
    while index < len(arguments) and arguments[index] in compose_value_options:
        if index + 1 >= len(arguments):
            return False
        index += 2
    if arguments[index : index + 1] != ["exec"]:
        return False
    index += 1
    if arguments[index : index + 1] == ["-T"]:
        index += 1
    if arguments[index : index + 1] != ["netbox"]:
        return False
    index += 1
    if arguments[index : index + 1] not in (["python"], ["python3"]):
        return False
    index += 1
    if arguments[index : index + 1] not in (
        ["manage.py"],
        ["/opt/netbox/netbox/manage.py"],
    ):
        return False
    index += 1
    if arguments[index : index + 1] != ["forward_ownership_audit"]:
        return False
    return set(arguments[index + 1 :]) == {
        "--fail-on-inconsistent",
        "--require-no-open-branches",
    } and len(arguments[index + 1 :]) == 2


def _commands_satisfy(evidence_id: str, commands: list[list[str]]) -> bool:
    if evidence_id == "final-tree-full-gate":
        return _has_exact_invoke_task(
            commands,
            "ci",
            environment=RELEASE_RUNTIME_ENVIRONMENT,
            require_url=True,
        )
    if evidence_id == "exact-runtime-artifact":
        return _has_exact_invoke_task(
            commands,
            "artifact-test",
            environment=RELEASE_RUNTIME_ENVIRONMENT,
        )
    if evidence_id == "scale-and-failure":
        return (
            _has_scale_soak_command(commands)
            and _has_exact_invoke_task(
                commands,
                "scenario-test",
                environment=RELEASE_RUNTIME_ENVIRONMENT,
            )
            and _has_exact_invoke_task(
                commands,
                "bulk-merge-retry-scale-test",
                environment=RELEASE_RUNTIME_ENVIRONMENT,
            )
        )
    if evidence_id == "ui-validation":
        return _has_exact_invoke_task(
            commands,
            "playwright-test",
            environment=RELEASE_RUNTIME_ENVIRONMENT,
            require_url=True,
        )
    if evidence_id == "customer-equivalent-acceptance":
        return _has_sync_release_gate_command(commands)
    if evidence_id == "independent-review":
        return any(
            _rtk_parts(command) == (["git", "diff", "--check"], {})
            for command in commands
        )
    if evidence_id == "ownership-audit":
        return any(
            _rtk_parts(command) is not None
            and _rtk_parts(command)[1] == {}
            and _is_ownership_audit_command(command)
            for command in commands
        )
    return False


def _evidence_is_concrete(evidence_id: str, evidence: str) -> bool:
    required_patterns = EVIDENCE_REQUIRED_TEXT_PATTERNS.get(evidence_id, ())
    commands = _evidence_commands(evidence)
    return bool(
        len(evidence) >= 12
        and not PLACEHOLDER_RE.search(evidence)
        and COMMAND_RE.search(evidence)
        and RETROSPECTIVE_OUTCOME_RE.search(evidence)
        and not PROSPECTIVE_RE.search(evidence)
        and not CONTRADICTORY_OUTCOME_RE.search(evidence)
        and DIGIT_RE.search(evidence)
        and _commands_satisfy(evidence_id, commands)
        and all(pattern.search(evidence) for pattern in required_patterns)
    )


def discover_release_plan(version: str) -> Path:
    candidates = sorted(
        path
        for state in ("active", "completed")
        for path in (PLAN_ROOT / state).glob(f"*release-{version}*.md")
    )
    if len(candidates) != 1:
        raise ValueError(
            f"Expected exactly one release plan for {version}; found {len(candidates)}."
        )
    return candidates[0]


def check_release_authorization(
    path: Path,
    *,
    expected_base_commit: str | None = None,
    evidence_commit: str | None = None,
) -> dict[str, object]:
    text = path.read_text(encoding="utf-8")
    marker = "## Release Authorization"
    if marker not in text:
        raise ValueError(f"{path}: missing {marker!r} section.")
    section = text.split(marker, 1)[1].split("\n## ", 1)[0]
    evidence_base_match = EVIDENCE_BASE_RE.search(section)
    evidence_base_commit = (
        evidence_base_match.group("commit") if evidence_base_match else None
    )
    if expected_base_commit and evidence_base_commit != expected_base_commit:
        raise ValueError(
            f"{path}: evidence base commit must be {expected_base_commit}; "
            f"found {evidence_base_commit or 'none'}."
        )
    entries: dict[str, tuple[bool, str]] = {}
    for line in section.splitlines():
        match = ENTRY_RE.match(line.strip())
        if not match:
            continue
        evidence_id = match.group("id")
        if evidence_id in entries:
            raise ValueError(f"{path}: duplicate evidence id {evidence_id!r}.")
        entries[evidence_id] = (
            match.group("checked").lower() == "x",
            match.group("evidence").strip(),
        )

    missing = sorted(REQUIRED_EVIDENCE_IDS - entries.keys())
    unchecked = sorted(
        evidence_id
        for evidence_id in REQUIRED_EVIDENCE_IDS
        if evidence_id in entries and not entries[evidence_id][0]
    )
    placeholders = sorted(
        evidence_id
        for evidence_id in REQUIRED_EVIDENCE_IDS
        if evidence_id in entries
        and not _evidence_is_concrete(evidence_id, entries[evidence_id][1])
    )
    if missing or unchecked or placeholders:
        raise ValueError(
            f"{path}: release authorization incomplete; missing={missing}, "
            f"unchecked={unchecked}, placeholder_evidence={placeholders}."
        )
    try:
        display_path = path.relative_to(REPO_ROOT)
    except ValueError:
        display_path = path
    result = {
        "plan": str(display_path),
        "authorized_evidence_ids": sorted(REQUIRED_EVIDENCE_IDS),
    }
    if evidence_base_commit:
        result["evidence_base_commit"] = evidence_base_commit
    if evidence_commit:
        result["evidence_commit"] = evidence_commit
    return result


def _git_capture(*args: str) -> str:
    return subprocess.run(
        ["git", *args],
        cwd=REPO_ROOT,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()


def release_evidence_commit_binding(path: Path) -> tuple[str, str]:
    """Require an evidence-only commit whose sole parent is the tested tree."""
    head = _git_capture("rev-parse", "HEAD")
    commit_line = _git_capture("rev-list", "--parents", "-n", "1", head).split()
    if len(commit_line) != 2:
        raise ValueError("Release evidence commit must have exactly one parent.")
    base = commit_line[1]
    changed_files = {
        line
        for line in _git_capture("diff", "--name-only", base, head).splitlines()
        if line
    }
    try:
        plan_path = str(path.resolve().relative_to(REPO_ROOT))
    except ValueError as exc:
        raise ValueError("Release plan must be inside the repository.") from exc
    if changed_files != {plan_path}:
        raise ValueError(
            "Release evidence commit may change only its release plan; "
            f"changed={sorted(changed_files)}."
        )
    if _git_capture("status", "--porcelain"):
        raise ValueError("Release authorization requires a clean working tree.")
    return base, head


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Fail closed unless final-tree release evidence is authorized."
    )
    parser.add_argument("--version", required=True)
    parser.add_argument("--plan", type=Path)
    args = parser.parse_args()
    plan = args.plan or discover_release_plan(args.version)
    base_commit, evidence_commit = release_evidence_commit_binding(plan)
    print(
        json.dumps(
            check_release_authorization(
                plan,
                expected_base_commit=base_commit,
                evidence_commit=evidence_commit,
            ),
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
