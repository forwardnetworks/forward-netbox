from __future__ import annotations

import os
import re
import subprocess
from dataclasses import dataclass
from hashlib import sha256
from pathlib import Path
from typing import Iterable
from typing import Sequence


LOCAL_PATTERN_FILE = ".sensitive-patterns.local.txt"
HISTORY_BASELINE_FILE = ".sensitive-history-baseline"
BINARY_ALLOWLIST_FILE = ".sensitive-binary-allowlist"
# Newline-separated extra patterns injected at scan time (each line is a literal
# customer identifier, or `re:<regex>`). CI populates this from a repo secret so
# customer names can be blocked WITHOUT committing them to the public repo — the
# gitignored local file is invisible to CI, which is how a customer name once
# slipped through.
ENV_PATTERN_VAR = "FORWARD_SENSITIVE_PATTERNS"
HISTORY_BASELINE_ENV_VAR = "FORWARD_SENSITIVE_HISTORY_BASELINE"
HISTORY_BINARY_ENV_VAR = "FORWARD_SENSITIVE_BINARY_HISTORY_ALLOWLIST"
COMMIT_RE = re.compile(r"[0-9a-f]{40}")
SHA256_RE = re.compile(r"[0-9a-f]{64}")


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
        r"\bnetwork(?:[ _-]?id)?\b[\s`\"'()\[\]{}:=,-]*\d{5,}\b",
    ),
    _builtin_pattern(
        "Forward organization identifier",
        r"\b(?:org|organization)(?:[ _-]?id)?\b[\s`\"'()\[\]{}:=,-]*\d{5,}\b",
    ),
    _builtin_pattern(
        "Forward snapshot identifier",
        r"\bsnapshot(?:[ _-]?id)?\b[\s`\"'()\[\]{}:=,-]*\d{5,}\b",
    ),
    _builtin_pattern(
        "Forward query identifier",
        r"\bQ_[0-9a-f]{40}\b",
    ),
)


def load_sensitive_patterns(repo_root: Path) -> list[SensitivePattern]:
    patterns = list(BUILTIN_PATTERNS)
    local_patterns_path = repo_root / LOCAL_PATTERN_FILE

    if not local_patterns_path.exists():
        patterns.extend(_env_patterns())
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

    patterns.extend(_env_patterns())
    return patterns


def require_environment_patterns() -> None:
    if not _env_patterns():
        raise ValueError(
            f"{ENV_PATTERN_VAR} must contain at least one valid release pattern."
        )


def load_protected_history_baseline(repo_root: Path) -> str:
    path = repo_root / HISTORY_BASELINE_FILE
    if not path.is_file():
        raise ValueError(f"Missing protected-history baseline file: {path.name}")
    value = path.read_text(encoding="utf-8").strip()
    if not COMMIT_RE.fullmatch(value):
        raise ValueError(
            f"{path.name} must contain exactly one full lowercase commit hash."
        )
    return value


def protected_history_range(
    repo_root: Path,
    *,
    require_trusted_baseline: bool = False,
) -> str:
    baseline = load_protected_history_baseline(repo_root)
    trusted_baseline = os.environ.get(HISTORY_BASELINE_ENV_VAR, "").strip()
    if require_trusted_baseline:
        if not COMMIT_RE.fullmatch(trusted_baseline):
            raise ValueError(
                f"{HISTORY_BASELINE_ENV_VAR} must contain the approved full "
                "lowercase commit hash."
            )
        if trusted_baseline != baseline:
            raise ValueError(
                f"{HISTORY_BASELINE_FILE} does not match the externally approved "
                f"{HISTORY_BASELINE_ENV_VAR}."
            )
    result = subprocess.run(
        ["git", "merge-base", "--is-ancestor", baseline, "HEAD"],
        cwd=repo_root,
        capture_output=True,
    )
    if result.returncode != 0:
        raise ValueError(
            "Protected-history baseline is unavailable or is not an ancestor of HEAD. "
            "Use a full-history checkout and review the baseline before changing it."
        )
    return f"{baseline}..HEAD"


def _env_patterns() -> list[SensitivePattern]:
    """Parse extra patterns from the ENV_PATTERN_VAR env var (CI secret feed)."""
    raw = os.environ.get(ENV_PATTERN_VAR, "")
    parsed: list[SensitivePattern] = []
    for line_number, raw_line in enumerate(raw.splitlines(), start=1):
        value = raw_line.strip()
        if not value or value.startswith("#"):
            continue
        if value.startswith("re:"):
            pattern_text = value[3:].strip()
            label = f"env regex pattern line {line_number}"
        else:
            pattern_text = re.escape(value)
            label = f"env literal pattern line {line_number}"
        if not pattern_text:
            continue
        parsed.append(
            SensitivePattern(
                label=label,
                regex=re.compile(pattern_text, re.IGNORECASE),
                source=f"${ENV_PATTERN_VAR}",
            )
        )
    return parsed


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
                )
            )
    return findings


def load_binary_allowlist(repo_root: Path) -> dict[str, str]:
    path = repo_root / BINARY_ALLOWLIST_FILE
    if not path.is_file():
        raise ValueError(f"Missing binary allowlist file: {path.name}")

    allowlist: dict[str, str] = {}
    for line_number, raw_line in enumerate(
        path.read_text(encoding="utf-8").splitlines(),
        start=1,
    ):
        value = raw_line.strip()
        if not value or value.startswith("#"):
            continue
        try:
            digest, relative_path = value.split(maxsplit=1)
        except ValueError as exc:
            raise ValueError(
                f"{path.name}:{line_number} must contain '<sha256> <path>'."
            ) from exc
        candidate = Path(relative_path)
        if (
            not SHA256_RE.fullmatch(digest)
            or candidate.is_absolute()
            or ".." in candidate.parts
            or relative_path in allowlist
        ):
            raise ValueError(
                f"{path.name}:{line_number} has an invalid or duplicate entry."
            )
        allowlist[relative_path] = digest

    tracked = {
        str(path.relative_to(repo_root)) for path in tracked_files(repo_root)
    }
    for relative_path, expected_digest in allowlist.items():
        candidate = repo_root / relative_path
        if relative_path not in tracked or not candidate.is_file():
            raise ValueError(
                f"{path.name} contains an entry without a current tracked file."
            )
        data = candidate.read_bytes()
        if _decode_text(data) is not None or sha256(data).hexdigest() != expected_digest:
            raise ValueError(
                f"{path.name} contains a non-binary or digest-mismatched entry."
            )
    return allowlist


def load_history_binary_allowlist() -> set[tuple[str, str, str]]:
    approved: set[tuple[str, str, str]] = set()
    raw = os.environ.get(HISTORY_BINARY_ENV_VAR, "")
    for line_number, raw_line in enumerate(raw.splitlines(), start=1):
        value = raw_line.strip()
        if not value or value.startswith("#"):
            continue
        try:
            revision, digest, relative_path = value.split(maxsplit=2)
        except ValueError as exc:
            raise ValueError(
                f"{HISTORY_BINARY_ENV_VAR} line {line_number} must contain "
                "'<commit> <sha256> <path>'."
            ) from exc
        candidate = Path(relative_path)
        if (
            not COMMIT_RE.fullmatch(revision)
            or not SHA256_RE.fullmatch(digest)
            or candidate.is_absolute()
            or ".." in candidate.parts
        ):
            raise ValueError(
                f"{HISTORY_BINARY_ENV_VAR} line {line_number} is invalid."
            )
        approved.add((revision, relative_path, digest))
    return approved


def _decode_text(data: bytes) -> str | None:
    try:
        text = data.decode("utf-8")
    except UnicodeDecodeError:
        return None
    if "\x00" in text:
        return None
    return text


def _opaque_location(kind: str, value: str) -> str:
    digest = sha256(value.encode("utf-8", errors="surrogatepass")).hexdigest()[:16]
    return f"{kind}:sha256:{digest}"


def _safe_location(
    kind: str,
    value: str,
    *,
    patterns: Sequence[SensitivePattern],
) -> str:
    unsafe = any(pattern.regex.search(value) for pattern in patterns)
    unsafe = unsafe or any(
        ord(character) < 32 or ord(character) == 127 for character in value
    )
    if unsafe:
        return _opaque_location(kind, value)
    escaped = value.encode("unicode_escape").decode("ascii")
    if len(escaped) > 240:
        return _opaque_location(kind, value)
    return f"{kind}:{escaped}"


def scan_name(
    value: str,
    *,
    kind: str,
    patterns: Sequence[SensitivePattern],
) -> list[SensitiveFinding]:
    source = _opaque_location(kind, value)
    findings = scan_text(value, source=source, patterns=patterns)
    return [
        SensitiveFinding(source=finding.source, line_number=0, label=finding.label)
        for finding in findings
    ]


def _scan_bytes(
    data: bytes,
    *,
    source: str,
    allowlist_path: str,
    patterns: Sequence[SensitivePattern],
    binary_allowlist: dict[str, str],
    history_approval: tuple[str, str] | None = None,
    history_binary_allowlist: set[tuple[str, str, str]] | None = None,
) -> list[SensitiveFinding]:
    text = _decode_text(data)
    if text is None:
        expected_digest = binary_allowlist.get(allowlist_path)
        actual_digest = sha256(data).hexdigest()
        if expected_digest == actual_digest or (
            history_approval is not None
            and history_binary_allowlist is not None
            and (*history_approval, actual_digest) in history_binary_allowlist
        ):
            return []
        return [
            SensitiveFinding(
                source=source,
                line_number=0,
                label="unreviewed binary or non-UTF-8 content",
            )
        ]
    return scan_text(text, source=source, patterns=patterns)


def scan_file(
    path: Path,
    *,
    repo_root: Path,
    patterns: Sequence[SensitivePattern],
    binary_allowlist: dict[str, str] | None = None,
):
    data = path.read_bytes()
    relative_path = str(path.relative_to(repo_root))
    source = _safe_location("path", relative_path, patterns=patterns)
    findings = scan_name(relative_path, kind="path", patterns=patterns)
    findings.extend(
        _scan_bytes(
            data,
            source=source,
            allowlist_path=relative_path,
            patterns=patterns,
            binary_allowlist=(
                binary_allowlist if binary_allowlist is not None else {}
            ),
        )
    )
    return findings


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
    binary_allowlist: dict[str, str] | None = None,
) -> list[SensitiveFinding]:
    findings: list[SensitiveFinding] = []
    reviewed_binaries = (
        binary_allowlist
        if binary_allowlist is not None
        else load_binary_allowlist(repo_root)
    )
    seen_paths: set[Path] = set()
    for path in _iter_files(paths):
        resolved_path = path.resolve()
        if resolved_path in seen_paths:
            continue
        seen_paths.add(resolved_path)
        findings.extend(
            scan_file(
                resolved_path,
                repo_root=repo_root,
                patterns=patterns,
                binary_allowlist=reviewed_binaries,
            )
        )
    return findings


def _changed_paths(repo_root: Path, revision: str) -> list[str]:
    result = subprocess.run(
        [
            "git",
            "diff-tree",
            "--root",
            "--no-commit-id",
            "--name-only",
            "-r",
            "-m",
            "-z",
            revision,
        ],
        check=True,
        cwd=repo_root,
        capture_output=True,
    )
    return [
        item.decode("utf-8")
        for item in result.stdout.split(b"\x00")
        if item
    ]


def _blob_at_revision(repo_root: Path, revision: str, path: str) -> bytes | None:
    result = subprocess.run(
        ["git", "show", f"{revision}:{path}"],
        cwd=repo_root,
        capture_output=True,
    )
    if result.returncode != 0:
        return None
    return result.stdout


def _tree_blobs(repo_root: Path, revision: str) -> dict[str, bytes]:
    result = subprocess.run(
        ["git", "ls-tree", "-r", "-z", "--full-tree", revision],
        check=True,
        cwd=repo_root,
        capture_output=True,
    )
    blobs: dict[str, bytes] = {}
    for item in result.stdout.split(b"\x00"):
        if not item:
            continue
        metadata, separator, raw_path = item.partition(b"\t")
        fields = metadata.decode("ascii").split()
        if not separator or len(fields) != 3 or fields[1] != "blob":
            continue
        path = raw_path.decode("utf-8")
        blob = _blob_at_revision(repo_root, revision, path)
        if blob is not None:
            blobs[path] = blob
    return blobs


def scan_git_tree(
    *,
    repo_root: Path,
    revision: str,
    patterns: Sequence[SensitivePattern],
    require_trusted_controls: bool = False,
) -> list[SensitiveFinding]:
    blobs = _tree_blobs(repo_root, revision)
    binary_allowlist = load_binary_allowlist(repo_root)
    findings: list[SensitiveFinding] = []

    if require_trusted_controls:
        trusted_baseline = os.environ.get(HISTORY_BASELINE_ENV_VAR, "").strip()
        candidate_baseline = blobs.get(HISTORY_BASELINE_FILE, b"").decode(
            "utf-8", errors="replace"
        ).strip()
        if not COMMIT_RE.fullmatch(trusted_baseline) or candidate_baseline != trusted_baseline:
            raise ValueError(
                "Candidate history baseline does not match the external trust anchor."
            )
        candidate_binary_allowlist = blobs.get(BINARY_ALLOWLIST_FILE)
        trusted_binary_allowlist = (repo_root / BINARY_ALLOWLIST_FILE).read_bytes()
        if candidate_binary_allowlist != trusted_binary_allowlist:
            raise ValueError(
                "Candidate binary allowlist differs from the trusted base branch."
            )

    missing_approved_paths = sorted(set(binary_allowlist).difference(blobs))
    for relative_path in missing_approved_paths:
        findings.append(
            SensitiveFinding(
                source=_safe_location(
                    "path",
                    relative_path,
                    patterns=patterns,
                ),
                line_number=0,
                label="reviewed binary is missing from candidate tree",
            )
        )

    for relative_path, data in sorted(blobs.items()):
        findings.extend(scan_name(relative_path, kind="path", patterns=patterns))
        source = _safe_location("path", relative_path, patterns=patterns)
        findings.extend(
            _scan_bytes(
                data,
                source=source,
                allowlist_path=relative_path,
                patterns=patterns,
                binary_allowlist=binary_allowlist,
            )
        )
    return findings


def _tag_ref_objects(repo_root: Path) -> list[tuple[str, str]]:
    result = subprocess.run(
        [
            "git",
            "for-each-ref",
            "--format=%(refname)%00%(objectname)",
            "refs/tags",
        ],
        check=True,
        cwd=repo_root,
        capture_output=True,
        text=True,
    )
    refs = []
    for line in result.stdout.splitlines():
        fields = line.split("\x00")
        if len(fields) == 2:
            refs.append((fields[0], fields[1]))
    return refs


def _annotated_tag_objects(
    repo_root: Path,
) -> list[tuple[str, str, str]]:
    objects: list[tuple[str, str, str]] = []
    visited: set[str] = set()
    pending = list(_tag_ref_objects(repo_root))
    while pending:
        ref_name, object_name = pending.pop()
        if object_name in visited:
            continue
        visited.add(object_name)
        object_type = subprocess.run(
            ["git", "cat-file", "-t", object_name],
            check=True,
            cwd=repo_root,
            capture_output=True,
            text=True,
        ).stdout.strip()
        if object_type != "tag":
            continue
        tag_object = subprocess.run(
            ["git", "cat-file", "-p", object_name],
            check=True,
            cwd=repo_root,
            capture_output=True,
            text=True,
        ).stdout
        headers, _, message = tag_object.partition("\n\n")
        objects.append((ref_name, object_name, message))
        target_match = re.search(r"^object ([0-9a-f]{40})$", headers, re.MULTILINE)
        type_match = re.search(r"^type (\S+)$", headers, re.MULTILINE)
        if target_match and type_match and type_match.group(1) == "tag":
            pending.append((ref_name, target_match.group(1)))
    return objects


def scan_ref_names(
    *,
    repo_root: Path,
    patterns: Sequence[SensitivePattern],
) -> list[SensitiveFinding]:
    result = subprocess.run(
        ["git", "for-each-ref", "--format=%(refname)"],
        check=True,
        cwd=repo_root,
        capture_output=True,
        text=True,
    )
    findings: list[SensitiveFinding] = []
    for ref_name in result.stdout.splitlines():
        findings.extend(scan_name(ref_name, kind="ref", patterns=patterns))
    return findings


def scan_commit_history(
    *,
    repo_root: Path,
    patterns: Sequence[SensitivePattern],
    rev_args: Sequence[str] | None = None,
) -> list[SensitiveFinding]:
    revisions = subprocess.run(
        ["git", "rev-list", "--reverse", *(rev_args or ["--all"])],
        check=True,
        cwd=repo_root,
        capture_output=True,
        text=True,
    ).stdout.splitlines()

    findings: list[SensitiveFinding] = []
    history_binary_allowlist = load_history_binary_allowlist()
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
        for path in _changed_paths(repo_root, revision):
            findings.extend(scan_name(path, kind="path", patterns=patterns))
            blob = _blob_at_revision(repo_root, revision, path)
            if blob is None:
                continue
            source = _safe_location(
                f"commit:{revision[:12]}:path",
                path,
                patterns=patterns,
            )
            findings.extend(
                _scan_bytes(
                    blob,
                    source=source,
                    allowlist_path=path,
                    patterns=patterns,
                    binary_allowlist={},
                    history_approval=(revision, path),
                    history_binary_allowlist=history_binary_allowlist,
                )
            )
    findings.extend(scan_ref_names(repo_root=repo_root, patterns=patterns))
    for tag_name, object_name, tag_message in _annotated_tag_objects(repo_root):
        findings.extend(scan_name(tag_name, kind="tag-ref", patterns=patterns))
        findings.extend(
            scan_text(
                tag_message,
                source=f"tag-object:{object_name[:16]}",
                patterns=patterns,
            )
        )
    return findings


def format_finding(finding: SensitiveFinding) -> str:
    location = finding.source
    if finding.line_number:
        location = f"{location}:{finding.line_number}"
    return f"{location}: {finding.label}"
