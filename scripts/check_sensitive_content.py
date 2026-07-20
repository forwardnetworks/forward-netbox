#!/usr/bin/env python3
from __future__ import annotations

import argparse
import re
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


def _load_sensitive_utilities():
    from scripts.sensitive_content import format_finding
    from scripts.sensitive_content import load_binary_allowlist
    from scripts.sensitive_content import load_sensitive_patterns
    from scripts.sensitive_content import protected_history_range
    from scripts.sensitive_content import require_environment_patterns
    from scripts.sensitive_content import scan_commit_history
    from scripts.sensitive_content import scan_file
    from scripts.sensitive_content import scan_git_tree
    from scripts.sensitive_content import scan_name
    from scripts.sensitive_content import scan_paths
    from scripts.sensitive_content import tracked_files

    return {
        "format_finding": format_finding,
        "load_binary_allowlist": load_binary_allowlist,
        "load_sensitive_patterns": load_sensitive_patterns,
        "protected_history_range": protected_history_range,
        "require_environment_patterns": require_environment_patterns,
        "scan_commit_history": scan_commit_history,
        "scan_file": scan_file,
        "scan_git_tree": scan_git_tree,
        "scan_name": scan_name,
        "scan_paths": scan_paths,
        "tracked_files": tracked_files,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Block customer-derived identifiers from repo content and commit messages."
    )
    parser.add_argument(
        "paths",
        nargs="*",
        help="Files or directories to scan. Defaults to tracked files when no scan mode is selected.",
    )
    parser.add_argument(
        "--git-files",
        action="store_true",
        help="Scan all tracked files in the current git repository.",
    )
    parser.add_argument(
        "--all-history",
        action="store_true",
        help=(
            "Scan changed file content, commit messages, and annotated tag messages "
            "for every reachable commit."
        ),
    )
    parser.add_argument(
        "--protected-history",
        action="store_true",
        help=(
            "Scan changed file content, commit messages, and annotated tag messages "
            "after the reviewed baseline in "
            ".sensitive-history-baseline."
        ),
    )
    parser.add_argument(
        "--require-env-patterns",
        action="store_true",
        help="Fail unless the release-only environment pattern feed is configured.",
    )
    parser.add_argument(
        "--require-baseline-env",
        action="store_true",
        help="Fail unless the tracked baseline matches the external trust anchor.",
    )
    parser.add_argument(
        "--rev-list",
        action="append",
        default=[],
        help="Additional git rev-list argument or revision range to scan.",
    )
    parser.add_argument(
        "--git-tree",
        action="append",
        default=[],
        help="Scan every blob and path in a Git tree without checking it out.",
    )
    parser.add_argument(
        "--ref-name",
        action="append",
        default=[],
        help="Scan an externally supplied branch or tag name using opaque output.",
    )
    parser.add_argument(
        "--commit-msg-file",
        type=Path,
        help="Scan the provided commit message file.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    repo_root = REPO_ROOT
    utilities = _load_sensitive_utilities()
    try:
        utilities["load_binary_allowlist"](repo_root)
        if args.require_env_patterns:
            utilities["require_environment_patterns"]()
        patterns = utilities["load_sensitive_patterns"](repo_root)
    except (ValueError, re.error) as exc:
        print(f"Sensitive content guard failed: {exc}")
        return 1
    findings = []

    explicit_mode = any(
        [
            args.git_files,
            args.all_history,
            args.protected_history,
            args.rev_list,
            args.git_tree,
            args.ref_name,
            args.commit_msg_file is not None,
            bool(args.paths),
        ]
    )

    if args.commit_msg_file is not None:
        findings.extend(
            utilities["scan_file"](
                args.commit_msg_file.resolve(),
                repo_root=args.commit_msg_file.resolve().parent,
                patterns=patterns,
            )
        )

    if args.all_history:
        findings.extend(
            utilities["scan_commit_history"](repo_root=repo_root, patterns=patterns)
        )

    if args.protected_history:
        try:
            history_range = utilities["protected_history_range"](
                repo_root,
                require_trusted_baseline=args.require_baseline_env,
            )
        except ValueError as exc:
            print(f"Sensitive content guard failed: {exc}")
            return 1
        try:
            findings.extend(
                utilities["scan_commit_history"](
                    repo_root=repo_root,
                    patterns=patterns,
                    rev_args=[history_range],
                )
            )
        except ValueError as exc:
            print(f"Sensitive content guard failed: {exc}")
            return 1

    for rev_arg in args.rev_list:
        findings.extend(
            utilities["scan_commit_history"](
                repo_root=repo_root,
                patterns=patterns,
                rev_args=[rev_arg],
            )
        )

    for revision in args.git_tree:
        try:
            findings.extend(
                utilities["scan_git_tree"](
                    repo_root=repo_root,
                    revision=revision,
                    patterns=patterns,
                    require_trusted_controls=args.require_baseline_env,
                )
            )
        except (subprocess.CalledProcessError, ValueError) as exc:
            print(f"Sensitive content guard failed: {exc}")
            return 1

    for ref_name in args.ref_name:
        findings.extend(
            utilities["scan_name"](
                ref_name,
                kind="ref",
                patterns=patterns,
            )
        )

    if args.git_files:
        findings.extend(
            utilities["scan_paths"](
                utilities["tracked_files"](repo_root),
                repo_root=repo_root,
                patterns=patterns,
            )
        )

    if args.paths:
        findings.extend(
            utilities["scan_paths"](
                [Path(path).resolve() for path in args.paths],
                repo_root=repo_root,
                patterns=patterns,
            )
        )

    if not explicit_mode:
        findings.extend(
            utilities["scan_paths"](
                utilities["tracked_files"](repo_root),
                repo_root=repo_root,
                patterns=patterns,
            )
        )

    if not findings:
        return 0

    print("Sensitive content guard failed:")
    for finding in findings:
        print(utilities["format_finding"](finding))
    print(
        "Add local customer names to .sensitive-patterns.local.txt "
        "(literal lines or re:<regex>) so they are blocked before commit."
    )
    return 1


if __name__ == "__main__":
    sys.exit(main())
