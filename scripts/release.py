#!/usr/bin/env python3
# Release automation for forward-netbox.
#
# Encodes the full release flow that was previously run by hand, including the
# gotchas that cost CI round-trips:
#   - `git add -A` BEFORE the local pre-commit mirror, so the sensitive-content
#     guard (tracked-files only) sees new plan/doc files.
#   - run pre-commit twice (convergence) and grep test SUMMARIES, not tails.
#   - keep the high-risk diff and a plan file in the same push (harness gate).
#
# Stages:
#   prepare  - bump version + the 3 README tables, scaffold the plan, lint-fix
#   verify   - the full local CI mirror (pre-commit x2, harness, harness tests,
#              py_compile, mkdocs --strict, build)
#   publish  - branch, push, wait for GitHub CI, fast-forward main, tag, GitHub
#              release, PyPI upload, sync local main  (ONLY with --publish)
#
# Default run is prepare + verify. Rollout never happens without --publish, so
# this is safe to run for a dry build.
from __future__ import annotations

import argparse
import re
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
PYPROJECT = REPO_ROOT / "pyproject.toml"
INIT_PY = REPO_ROOT / "forward_netbox/__init__.py"
README_TABLES = (
    REPO_ROOT / "README.md",
    REPO_ROOT / "docs/README.md",
    REPO_ROOT / "docs/01_User_Guide/README.md",
)
INSTALL_DOC = REPO_ROOT / "docs/01_User_Guide/README.md"
PLAN_DIR = REPO_ROOT / "docs/03_Plans/active"

# The compatibility cell shared by every table row, so a new row reuses the
# previous row's NetBox-support text verbatim.
NETBOX_SUPPORT_RE = re.compile(
    r"^\| `v[0-9][^|]*` \| (?P<support>[^|]*) \| Current release;", re.MULTILINE
)

SEMVER_RE = re.compile(r"^\d+\.\d+\.\d+$")


class ReleaseError(RuntimeError):
    pass


def bump_version_text(text: str, old: str, new: str, *, key: str) -> str:
    """Replace a `version = "old"` assignment. Raises if not found exactly once."""
    pattern = re.compile(rf'({re.escape(key)}\s*=\s*")' + re.escape(old) + r'(")')
    new_text, n = pattern.subn(rf"\g<1>{new}\g<2>", text)
    if n != 1:
        raise ReleaseError(f'expected exactly one `{key} = "{old}"` to bump, found {n}')
    return new_text


def insert_release_row(table_text: str, version: str, summary: str) -> str:
    """Insert the new current-release row and demote the prior one.

    The prior `Current release;` row is rewritten to `Superseded by vX.Y.Z;` and
    the new row is inserted above it, reusing its NetBox-support cell.
    """
    match = NETBOX_SUPPORT_RE.search(table_text)
    if not match:
        raise ReleaseError("could not find the current-release row to supersede")
    support = match.group("support")
    new_row = f"| `v{version}` | {support} | Current release; {summary} |"
    # Replace the matched row-prefix line with new_row + the demoted old row.
    old_line_start = match.start()
    line_end = table_text.index("\n", old_line_start)
    old_line = table_text[old_line_start:line_end]
    demoted_line = old_line.replace(
        "| Current release;", f"| Superseded by `v{version}`;"
    )
    return (
        table_text[:old_line_start]
        + new_row
        + "\n"
        + demoted_line
        + table_text[line_end:]
    )


def read_current_version() -> str:
    text = PYPROJECT.read_text(encoding="utf-8")
    match = re.search(r'^version = "([^"]+)"', text, re.MULTILINE)
    if not match:
        raise ReleaseError("could not read current version from pyproject.toml")
    return match.group(1)


def run(cmd: list[str], *, cwd: Path = REPO_ROOT, check: bool = True) -> int:
    print(f"  $ {' '.join(cmd)}")
    result = subprocess.run(cmd, cwd=cwd)
    if check and result.returncode != 0:
        raise ReleaseError(f"command failed ({result.returncode}): {' '.join(cmd)}")
    return result.returncode


def stage_prepare(version: str, summary: str, *, write: bool) -> None:
    old = read_current_version()
    print(f"[prepare] bump {old} -> {version}")
    edits = {
        PYPROJECT: bump_version_text(
            PYPROJECT.read_text(encoding="utf-8"), old, version, key="version"
        ),
        INIT_PY: bump_version_text(
            INIT_PY.read_text(encoding="utf-8"), old, version, key="version"
        ),
    }
    for path in README_TABLES:
        edits[path] = insert_release_row(
            path.read_text(encoding="utf-8"), version, summary
        )
    # Install-doc wheel/sdist/pin references.
    install_text = edits.get(INSTALL_DOC, INSTALL_DOC.read_text(encoding="utf-8"))
    install_text = install_text.replace(
        f"forward_netbox-{old}", f"forward_netbox-{version}"
    ).replace(f"forward-netbox=={old}", f"forward-netbox=={version}")
    edits[INSTALL_DOC] = install_text

    if not write:
        print("[prepare] dry-run: not writing files")
        return
    for path, text in edits.items():
        path.write_text(text, encoding="utf-8")
        print(f"[prepare] wrote {path.relative_to(REPO_ROOT)}")
    print(
        "[prepare] NOTE: author the plan file in docs/03_Plans/active with all 7 "
        "headings, then `git add -A` before verify."
    )


def stage_verify() -> None:
    print("[verify] local CI mirror")
    run(["git", "add", "-A"])  # so the sensitive guard sees new tracked files
    run([sys.executable, "-m", "pre_commit", "clean"], check=False)
    run([sys.executable, "-m", "pre_commit", "run", "--all-files"], check=False)
    run(["git", "add", "-A"])
    run([sys.executable, "-m", "pre_commit", "run", "--all-files"])
    run([sys.executable, "scripts/check_harness.py"])
    run(
        [
            sys.executable,
            "-m",
            "unittest",
            "discover",
            "-s",
            "scripts/tests",
            "-p",
            "test_*.py",
        ]
    )
    run([sys.executable, "-m", "mkdocs", "build", "--strict"])
    run([sys.executable, "-m", "build"])
    print("[verify] OK — run the Django suite separately in the container.")


def stage_publish(version: str, notes_file: Path) -> None:
    print(f"[publish] rolling out v{version}")
    branch = f"release/{version}"
    run(["git", "checkout", "-b", branch])
    run(["git", "add", "-A"])
    run(["git", "commit", "-m", f"release: cut v{version}"])
    run(["git", "push", "--no-verify", "-u", "origin", branch])
    print("[publish] wait for GitHub CI to go green, then re-run with --finish")
    # Finishing (FF main, tag, gh release, PyPI, sync) is deliberately a separate
    # manual confirmation after CI — see --finish.


def stage_finish(version: str, notes_file: Path) -> None:
    print(f"[finish] tag + release v{version}")
    run(["git", "push", "--no-verify", "origin", f"release/{version}:main"])
    run(["git", "tag", "-a", f"v{version}", "-m", f"Release {version}"])
    run(["git", "push", "--no-verify", "origin", f"v{version}"])
    whl = f"dist/forward_netbox-{version}-py3-none-any.whl"
    sdist = f"dist/forward_netbox-{version}.tar.gz"
    run(
        [
            "gh",
            "release",
            "create",
            f"v{version}",
            whl,
            sdist,
            "--title",
            f"v{version}",
            "--notes-file",
            str(notes_file),
        ]
    )
    print("[finish] upload to PyPI with: twine upload " + whl + " " + sdist)
    run(["git", "checkout", "main"])
    run(["git", "merge", "--ff-only", "origin/main"])


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Release forward-netbox.")
    parser.add_argument("version", help="target version, e.g. 1.5.11")
    parser.add_argument(
        "--summary",
        help="one-line release summary for the compatibility tables",
        default="",
    )
    parser.add_argument(
        "--notes-file", type=Path, help="release body for the GitHub release"
    )
    parser.add_argument("--write", action="store_true", help="write prepare edits")
    parser.add_argument(
        "--publish",
        action="store_true",
        help="branch + push (rollout). Off by default — safe to omit.",
    )
    parser.add_argument(
        "--finish",
        action="store_true",
        help="after CI is green: FF main, tag, GitHub release (rollout)",
    )
    args = parser.parse_args(argv)

    if not SEMVER_RE.match(args.version):
        parser.error(f"version must be X.Y.Z, got {args.version!r}")

    try:
        if args.finish:
            stage_finish(args.version, args.notes_file)
            return 0
        stage_prepare(args.version, args.summary, write=args.write)
        if args.write:
            stage_verify()
        if args.publish:
            if not args.notes_file:
                parser.error("--publish requires --notes-file")
            stage_publish(args.version, args.notes_file)
    except ReleaseError as exc:
        print(f"release error: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
