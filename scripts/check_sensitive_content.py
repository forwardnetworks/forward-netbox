#!/usr/bin/env python3

from __future__ import annotations

import argparse
from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from forward_netbox.utilities.sensitive_content import format_finding
from forward_netbox.utilities.sensitive_content import load_sensitive_patterns
from forward_netbox.utilities.sensitive_content import scan_commit_history
from forward_netbox.utilities.sensitive_content import scan_file
from forward_netbox.utilities.sensitive_content import scan_paths
from forward_netbox.utilities.sensitive_content import tracked_files


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
        help="Scan every reachable commit message in the current git repository.",
    )
    parser.add_argument(
        "--rev-list",
        action="append",
        default=[],
        help="Additional git rev-list argument or revision range to scan.",
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
    patterns = load_sensitive_patterns(repo_root)
    findings = []

    explicit_mode = any(
        [
            args.git_files,
            args.all_history,
            args.rev_list,
            args.commit_msg_file is not None,
            bool(args.paths),
        ]
    )

    if args.commit_msg_file is not None:
        findings.extend(
            scan_file(
                args.commit_msg_file.resolve(),
                repo_root=args.commit_msg_file.resolve().parent,
                patterns=patterns,
            )
        )

    if args.all_history:
        findings.extend(scan_commit_history(repo_root=repo_root, patterns=patterns))

    for rev_arg in args.rev_list:
        findings.extend(
            scan_commit_history(
                repo_root=repo_root,
                patterns=patterns,
                rev_args=[rev_arg],
            )
        )

    if args.git_files:
        findings.extend(
            scan_paths(
                tracked_files(repo_root),
                repo_root=repo_root,
                patterns=patterns,
            )
        )

    if args.paths:
        findings.extend(
            scan_paths(
                [Path(path).resolve() for path in args.paths],
                repo_root=repo_root,
                patterns=patterns,
            )
        )

    if not explicit_mode:
        findings.extend(
            scan_paths(
                tracked_files(repo_root),
                repo_root=repo_root,
                patterns=patterns,
            )
        )

    if not findings:
        return 0

    print("Sensitive content guard failed:")
    for finding in findings:
        print(format_finding(finding))
    print(
        "Add local customer names to .sensitive-patterns.local.txt "
        "(literal lines or re:<regex>) so they are blocked before commit."
    )
    return 1


if __name__ == "__main__":
    sys.exit(main())
