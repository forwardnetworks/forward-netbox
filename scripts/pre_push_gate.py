#!/usr/bin/env python3
"""The tier of gate a push has to pass.

A push runs the quick tier (`invoke ci-quick`, about 35 minutes): static checks,
docs, the built package and the upgrade leg. The full gate (`invoke ci`, about
four hours) is run by the release cut, which is where it decides anything, and
by a push that sets FORWARD_NETBOX_FULL_GATE=1.

A tag push is the one place the full gate used to run a third time. It runs the
quick tier only when the tagged tree is proven to be the tree the cut gated
(`scripts/gated_tree.py`); otherwise it runs the full gate.

Git's pre-push hook passes one `<local ref> <local sha> <remote ref> <remote
sha>` line per ref on stdin.
"""
import os
import re
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "scripts"))

import gated_tree  # noqa: E402

FULL_GATE_ENV = "FORWARD_NETBOX_FULL_GATE"
RELEASE_TAG_RE = re.compile(r"^refs/tags/v(?P<version>\d+\.\d+\.\d+)$")


def parse_refs(stdin_text: str):
    refs = []
    for line in stdin_text.splitlines():
        parts = line.split()
        if len(parts) == 4:
            refs.append(
                {
                    "local_ref": parts[0],
                    "local_sha": parts[1],
                    "remote_ref": parts[2],
                    "remote_sha": parts[3],
                }
            )
    return refs


def choose_tier(refs, *, env, tag_tree_check=gated_tree.check_ref):
    """`("full" | "quick", reason)` for the refs being pushed."""
    if (env.get(FULL_GATE_ENV) or "").strip() in {"1", "true", "yes"}:
        return "full", f"{FULL_GATE_ENV} is set"
    for ref in refs:
        tag = RELEASE_TAG_RE.match(ref["remote_ref"])
        if tag is None:
            continue
        ok, reason = tag_tree_check(ref["local_sha"], tag.group("version"))
        if not ok:
            return "full", f"tag {ref['remote_ref']}: {reason}"
    return "quick", "a push runs the quick tier; the release cut runs the full gate"


def main() -> int:
    tier, reason = choose_tier(parse_refs(sys.stdin.read()), env=os.environ)
    task = "ci" if tier == "full" else "ci-quick"
    print(f"[forward-netbox] pre-push gate: invoke {task} ({reason})", flush=True)
    return subprocess.run(["invoke", task], cwd=REPO_ROOT).returncode


if __name__ == "__main__":
    sys.exit(main())
