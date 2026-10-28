#!/usr/bin/env python3
"""A digest of a release tree that leaves out its own release plan.

The cut gates the release tree. The plan then gains its Release Authorization,
and the tag is pushed from main. Whether the tagged tree is the gated tree is a
question about every file but that plan, so the digest covers every tracked
file's mode, blob and path except `docs/03_Plans/*/*release-<version>*.md`.

The cut records the digest of the tree it gated in the plan; the tag push
recomputes it from the tagged commit. Equal means the full gate already covered
this exact content, so the tag push can run the quick tier. Anything else
(another change landed on main in between, or the plan carries no digest) means
the full gate runs again.
"""
import argparse
import hashlib
import os
import re
import subprocess
import sys
import tempfile
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
DIGEST_LINE_RE = re.compile(
    r"^- Gated tree digest \(every file except this plan\): `(?P<digest>[0-9a-f]{64})`$",
    re.MULTILINE,
)


def plan_path_re(version: str) -> re.Pattern:
    return re.compile(
        rf"^docs/03_Plans/(?:active|completed)/[^/]*release-{re.escape(version)}[^/]*\.md$"
    )


def digest_entries(entries, version: str) -> str:
    """Digest `git ls-tree -r` entries, without the release plan for `version`."""
    plan = plan_path_re(version)
    kept = sorted(entry for entry in entries if not plan.match(_entry_path(entry)))
    return hashlib.sha256("\n".join(kept).encode("utf-8")).hexdigest()


def _entry_path(entry: str) -> str:
    return entry.split("\t", 1)[1]


def _git(*args, env=None) -> str:
    return subprocess.run(
        ["git", *args],
        cwd=REPO_ROOT,
        check=True,
        capture_output=True,
        text=True,
        env=env,
    ).stdout


def _list_tree(tree: str, env=None) -> list[str]:
    return [line for line in _git("ls-tree", "-r", tree, env=env).splitlines() if line]


def commit_digest(ref: str, version: str) -> str:
    return digest_entries(_list_tree(ref), version)


def working_tree_digest(version: str) -> str:
    """The digest of what is on disk, tracked or not, minus ignored files.

    Built through a throwaway index so the real one is never touched: this is
    the content `git add -A && git commit` would record, which is the content
    the gate ran against.

    The scratch index starts from HEAD: `git add -A` into an empty index skips
    a tracked file that .gitignore also matches, which a real commit keeps. A
    file untracked from the real index but still in HEAD is the one case that
    differs, and it only forces a full gate.
    """
    with tempfile.TemporaryDirectory() as scratch:
        env = {**os.environ, "GIT_INDEX_FILE": str(Path(scratch) / "index")}
        _git("read-tree", "HEAD", env=env)
        _git("add", "-A", env=env)
        tree = _git("write-tree", env=env).strip()
        return digest_entries(_list_tree(tree, env=env), version)


def recorded_digest(plan_text: str) -> str | None:
    match = DIGEST_LINE_RE.search(plan_text)
    return match.group("digest") if match else None


def check_ref(ref: str, version: str) -> tuple[bool, str]:
    """Whether `ref`'s tree is the tree its release plan says the cut gated."""
    plan = plan_path_re(version)
    plan_paths = [
        _entry_path(entry)
        for entry in _list_tree(ref)
        if plan.match(_entry_path(entry))
    ]
    if len(plan_paths) != 1:
        return (
            False,
            f"expected one release plan for {version}, found {len(plan_paths)}",
        )
    recorded = recorded_digest(_git("show", f"{ref}:{plan_paths[0]}"))
    if recorded is None:
        return False, "the release plan records no gated tree digest"
    actual = commit_digest(ref, version)
    if actual != recorded:
        return False, "the tree differs from the one the cut gated"
    return True, "the tree is the one the cut gated"


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--version", required=True)
    parser.add_argument("--ref", help="check this commit against its release plan")
    parser.add_argument(
        "--working-tree", action="store_true", help="print the working tree's digest"
    )
    args = parser.parse_args(argv)
    if args.working_tree:
        print(working_tree_digest(args.version))
        return 0
    if not args.ref:
        parser.error("give --ref or --working-tree")
    ok, reason = check_ref(args.ref, args.version)
    print(reason)
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
