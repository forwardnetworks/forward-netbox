#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import re
import subprocess
import tomllib
from pathlib import Path

from scripts.verify_release_provenance import GITHUB_REPOSITORY
from scripts.verify_release_provenance import TRUSTED_ANCHOR_TAG
from scripts.verify_release_provenance import verify_github_release_controls
from scripts.verify_release_provenance import verify_release_commit_provenance
from scripts.verify_release_provenance import verify_trusted_anchor_candidate


REPO_ROOT = Path(__file__).resolve().parents[1]
VERSION_TAG_RE = re.compile(r"v(?P<version>[0-9]+\.[0-9]+\.[0-9]+)")


class TrustedTagError(RuntimeError):
    pass


def _git_capture(*arguments: str) -> str:
    result = subprocess.run(
        ["git", *arguments],
        cwd=REPO_ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    return result.stdout.strip()


def _package_version() -> str:
    with (REPO_ROOT / "pyproject.toml").open("rb") as pyproject:
        return str(tomllib.load(pyproject)["tool"]["poetry"]["version"])


def authorize_trusted_tag(
    tag: str,
    expected_sha: str,
    reviewer: str,
    token: str,
) -> dict:
    if not re.fullmatch(r"[0-9a-f]{40}", expected_sha):
        raise TrustedTagError("expected SHA must be a full lowercase commit SHA")
    if os.environ.get("GITHUB_REPOSITORY") != GITHUB_REPOSITORY:
        raise TrustedTagError(f"tag creation must run in {GITHUB_REPOSITORY}")
    if os.environ.get("GITHUB_REF") != "refs/heads/main":
        raise TrustedTagError("tag creation must be dispatched from protected main")
    if os.environ.get("GITHUB_SHA") != expected_sha:
        raise TrustedTagError("workflow SHA does not match the requested commit")
    if _git_capture("rev-parse", "refs/remotes/origin/main") != expected_sha:
        raise TrustedTagError("requested commit is not the current origin/main")

    if tag == TRUSTED_ANCHOR_TAG:
        controls = verify_github_release_controls(
            reviewer,
            token,
            require_trusted_status=False,
        )
        evidence = verify_trusted_anchor_candidate(expected_sha, reviewer, token)
        return {
            "tag": tag,
            "target": expected_sha,
            "kind": "anchor",
            "controls": controls,
            **evidence,
        }

    match = VERSION_TAG_RE.fullmatch(tag)
    if match is None:
        raise TrustedTagError(f"unsupported trusted tag name: {tag!r}")
    version = match.group("version")
    if _package_version() != version:
        raise TrustedTagError("version tag does not match the package version")
    controls = verify_github_release_controls(
        reviewer,
        token,
        require_trusted_status=True,
    )
    evidence = verify_release_commit_provenance(
        expected_sha,
        version,
        reviewer,
        token,
    )
    return {
        "tag": tag,
        "target": expected_sha,
        "kind": "release",
        "controls": controls,
        **evidence,
    }


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Authorize a protected-main tag for trusted automation."
    )
    parser.add_argument("--tag", required=True)
    parser.add_argument("--expected-sha", required=True)
    parser.add_argument("--reviewer", required=True)
    args = parser.parse_args()
    token = os.environ.get("GH_TOKEN", "").strip()
    if not token:
        raise SystemExit("GH_TOKEN is required")
    authorize_trusted_tag(
        args.tag,
        args.expected_sha,
        args.reviewer,
        token,
    )
    print("Trusted tag authorization passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
