from __future__ import annotations

import re
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable
from typing import Sequence


LOCAL_PATTERN_FILE = ".sensitive-patterns.local.txt"


@dataclass(frozen=True)
class SensitivePattern:
    label: str
    regex: re.Pattern[str]
    source: str


@dataclass(frozen=True)
class SensitiveFinding:
    source: str
    line_number: int
    label: str
    matched_text: str
    line_text: str


def _builtin_pattern(label: str, expression: str) -> SensitivePattern:
    return SensitivePattern(
        label=label,
        regex=re.compile(expression, re.IGNORECASE),
        source="builtin",
    )


BUILTIN_PATTERNS = (
    _builtin_pattern(
        "Forward plus-alias email address",
        r"\b[A-Za-z0-9._%+-]+\+[A-Za-z0-9._%+-]+@forwardnetworks\.com\b",
    ),
    _builtin_pattern(
        "Forward network identifier",
        r"\bnetwork(?:[ _-]?id)?\b[\"']?\s*[:=]?\s*[\"']?\d{5,}\b",
    ),
    _builtin_pattern(
        "Forward snapshot identifier",
        r"\bsnapshot(?:[ _-]?id)?\b[\"']?\s*[:=]?\s*[\"']?\d{5,}\b",
    ),
)


def load_sensitive_patterns(repo_root: Path) -> list[SensitivePattern]:
    patterns = list(BUILTIN_PATTERNS)
    local_patterns_path = repo_root / LOCAL_PATTERN_FILE

    if not local_patterns_path.exists():
        return patterns

    for line_number, raw_line in enumerate(
        local_patterns_path.read_text(encoding="utf-8").splitlines(),
        start=1,
    ):
        value = raw_line.strip()
        if not value or value.startswith("#"):
            continue

        if value.startswith("re:"):
            pattern_text = value[3:].strip()
            label = f"local regex pattern line {line_number}"
        else:
            pattern_text = re.escape(value)
            label = f"local literal pattern line {line_number}"

        patterns.append(
            SensitivePattern(
                label=label,
                regex=re.compile(pattern_text, re.IGNORECASE),
                source=str(local_patterns_path.relative_to(repo_root)),
            )
        )

    return patterns


def scan_text(
    text: str,
    *,
    source: str,
    patterns: Sequence[SensitivePattern],
) -> list[SensitiveFinding]:
    findings: list[SensitiveFinding] = []
    for line_number, line_text in enumerate(text.splitlines(), start=1):
        for pattern in patterns:
            match = pattern.regex.search(line_text)
            if not match:
                continue
            findings.append(
                SensitiveFinding(
                    source=source,
                    line_number=line_number,
                    label=pattern.label,
                    matched_text=match.group(0),
                    line_text=line_text.strip(),
                )
            )
    return findings


def scan_file(path: Path, *, repo_root: Path, patterns: Sequence[SensitivePattern]):
    data = path.read_bytes()
    if b"\x00" in data:
        return []
    text = data.decode("utf-8", errors="replace")
    return scan_text(
        text,
        source=str(path.relative_to(repo_root)),
        patterns=patterns,
    )


def _iter_files(paths: Iterable[Path]) -> Iterable[Path]:
    for path in paths:
        if path.is_dir():
            yield from (
                nested_path
                for nested_path in sorted(path.rglob("*"))
                if nested_path.is_file()
            )
            continue
        if path.is_file():
            yield path


def tracked_files(repo_root: Path) -> list[Path]:
    result = subprocess.run(
        ["git", "ls-files", "-z"],
        check=True,
        cwd=repo_root,
        capture_output=True,
    )
    return [
        repo_root / Path(item.decode("utf-8"))
        for item in result.stdout.split(b"\x00")
        if item
    ]


def scan_paths(
    paths: Iterable[Path],
    *,
    repo_root: Path,
    patterns: Sequence[SensitivePattern],
) -> list[SensitiveFinding]:
    findings: list[SensitiveFinding] = []
    seen_paths: set[Path] = set()
    for path in _iter_files(paths):
        resolved_path = path.resolve()
        if resolved_path in seen_paths:
            continue
        seen_paths.add(resolved_path)
        findings.extend(
            scan_file(resolved_path, repo_root=repo_root, patterns=patterns)
        )
    return findings


def scan_commit_history(
    *,
    repo_root: Path,
    patterns: Sequence[SensitivePattern],
    rev_args: Sequence[str] | None = None,
) -> list[SensitiveFinding]:
    revisions = subprocess.run(
        ["git", "rev-list", *(rev_args or ["--all"])],
        check=True,
        cwd=repo_root,
        capture_output=True,
        text=True,
    ).stdout.splitlines()

    findings: list[SensitiveFinding] = []
    for revision in revisions:
        commit_message = subprocess.run(
            ["git", "show", "-s", "--format=%B", revision],
            check=True,
            cwd=repo_root,
            capture_output=True,
            text=True,
        ).stdout
        findings.extend(
            scan_text(
                commit_message,
                source=f"commit:{revision[:12]}",
                patterns=patterns,
            )
        )
    return findings


def format_finding(finding: SensitiveFinding) -> str:
    line_text = finding.line_text
    if len(line_text) > 160:
        line_text = f"{line_text[:157]}..."
    return f"{finding.source}:{finding.line_number}: " f"{finding.label}: {line_text}"
