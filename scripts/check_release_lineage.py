#!/usr/bin/env python3
"""Run the publish workflow's structural provenance rules BEFORE tagging.

`verify_release_provenance.py` is fail-closed and correct, but it only ever runs
after a tag exists - and a tag is immutable, so every rule it enforces is a rule
that costs a version number to discover. `v2.7.10` was refused for a lineage of
three commits where four are required, which is a pure-git fact that was knowable
minutes earlier.

Everything checked here is decidable from the local clone: the lineage walk, the
bridge's parent and documentation-only shape, the length floor, the
production/release pairing, and the presence of a release plan. The rules that
genuinely need GitHub - that each commit arrived through a merged pull request,
and that its workflows succeeded - are deliberately NOT reimplemented; a second
implementation of a security check is a second thing to get wrong. This narrows
the window to those, rather than pretending to close it.

Run against the merge commit you are about to tag:

    python3 scripts/check_release_lineage.py --version 2.7.11
"""
from __future__ import annotations

import argparse
import sys

import verify_release_provenance as provenance


class LineageError(Exception):
    """A structural rule the publish workflow would refuse."""


def check_release_lineage(release_commit: str, version: str) -> dict[str, object]:
    resolved = provenance._git_capture("rev-parse", release_commit).strip()
    production_commit = provenance._commit_parent(resolved)

    try:
        prior_release = provenance._require_annotated_tag(provenance.PRIOR_RELEASE_TAG)
    except Exception as exc:  # noqa: BLE001 - surfaced verbatim below
        raise LineageError(
            f"prior release tag {provenance.PRIOR_RELEASE_TAG} is not resolvable "
            f"locally ({exc}); fetch tags first"
        ) from exc

    lineage = provenance._first_parent_commits(prior_release, resolved)
    if not lineage or lineage[0] != provenance.PRIOR_POST_RELEASE_DOC_COMMIT:
        raise LineageError(
            "lineage must start at the recorded post-release bridge "
            f"{provenance.PRIOR_POST_RELEASE_DOC_COMMIT[:12]}; it starts at "
            f"{(lineage[0][:12] if lineage else '<empty>')}"
        )
    if (
        provenance._commit_parent(provenance.PRIOR_POST_RELEASE_DOC_COMMIT)
        != prior_release
    ):
        raise LineageError("the recorded bridge does not sit directly on the prior tag")

    # The rule that burned v2.7.10. Stated with the arithmetic visible, because
    # "four" is not obvious from a history that looks complete.
    if len(lineage) < 4:
        raise LineageError(
            f"lineage has {len(lineage)} commits and needs at least 4 "
            "(bridge, control, production, release). Land the production "
            "content as its own pull request ahead of the release commit "
            "instead of squashing it together."
        )

    reviewed = lineage[1:]
    if reviewed[-2:] != [production_commit, resolved]:
        raise LineageError(
            "the last two reviewed commits must be the production commit and "
            f"the release commit; got {[commit[:12] for commit in reviewed[-2:]]} "
            f"but expected {[production_commit[:12], resolved[:12]]}"
        )

    try:
        plan = provenance._require_release_plan(production_commit, resolved, version)
    except Exception as exc:  # noqa: BLE001 - surfaced verbatim below
        raise LineageError(f"release plan check failed: {exc}") from exc

    try:
        provenance._require_security_bootstrap(resolved)
    except Exception as exc:  # noqa: BLE001 - surfaced verbatim below
        raise LineageError(f"security bootstrap check failed: {exc}") from exc

    return {
        "release_commit": resolved,
        "production_commit": production_commit,
        "lineage_length": len(lineage),
        "reviewed_commits": reviewed,
        "release_plan": plan,
        "not_checked_here": [
            "each commit arrived through a merged pull request",
            "required workflows succeeded",
        ],
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--version", required=True, help="Release version, e.g. 2.7.11")
    parser.add_argument(
        "--release-commit",
        default="origin/main",
        help="Commit about to be tagged (default: origin/main).",
    )
    args = parser.parse_args(argv)
    try:
        result = check_release_lineage(args.release_commit, args.version)
    except LineageError as exc:
        print(f"release lineage would be REFUSED: {exc}", file=sys.stderr)
        return 1
    print("release lineage looks publishable:")
    for key, value in result.items():
        print(f"  {key}: {value}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
