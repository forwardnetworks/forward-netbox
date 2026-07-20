#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import subprocess
import urllib.parse
import urllib.request
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
GITHUB_REPOSITORY = "forwardnetworks/forward-netbox"
GITHUB_API_URL = "https://api.github.com"
TRUSTED_STATUS_CONTEXT = "Trusted sensitive-content scan"
TRUSTED_STATUS_CREATOR = "github-actions[bot]"
REQUIRED_WORKFLOWS = (
    ".github/workflows/ci.yml",
    ".github/workflows/codeql.yml",
)


class ProvenanceError(RuntimeError):
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


def _github_json(path: str, token: str) -> object:
    request = urllib.request.Request(
        f"{GITHUB_API_URL}/repos/{GITHUB_REPOSITORY}/{path.lstrip('/')}",
        headers={
            "Accept": "application/vnd.github+json",
            "Authorization": f"Bearer {token}",
            "X-GitHub-Api-Version": "2022-11-28",
        },
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        return json.load(response)


def _require_verified_commit(commit: str, token: str) -> dict:
    payload = _github_json(f"commits/{commit}", token)
    if not isinstance(payload, dict):
        raise ProvenanceError(f"GitHub returned invalid commit data for {commit}")
    verification = (payload.get("commit") or {}).get("verification") or {}
    if verification.get("verified") is not True:
        raise ProvenanceError(f"commit {commit} is not GitHub-verified")
    parents = payload.get("parents") or []
    if len(parents) != 1:
        raise ProvenanceError(f"commit {commit} must have exactly one parent")
    return payload


def _latest_review_by_user(reviews: list[dict], login: str) -> dict | None:
    matching = [
        review
        for review in reviews
        if str((review.get("user") or {}).get("login") or "").lower()
        == login.lower()
    ]
    if not matching:
        return None
    return max(matching, key=lambda review: int(review.get("id") or 0))


def _require_trusted_candidate_status(candidate: str, token: str) -> None:
    payload = _github_json(f"commits/{candidate}/status?per_page=100", token)
    if not isinstance(payload, dict):
        raise ProvenanceError(f"GitHub returned invalid status data for {candidate}")
    statuses = payload.get("statuses") or []
    matching = [
        status
        for status in statuses
        if status.get("context") == TRUSTED_STATUS_CONTEXT
        and str((status.get("creator") or {}).get("login") or "").lower()
        == TRUSTED_STATUS_CREATOR
    ]
    if not matching:
        raise ProvenanceError(
            f"candidate {candidate} has no authenticated trusted scanner status"
        )
    latest = max(matching, key=lambda status: int(status.get("id") or 0))
    if latest.get("state") != "success":
        raise ProvenanceError(
            f"candidate {candidate} trusted scanner state is {latest.get('state')!r}"
        )


def _require_reviewed_main_pr(commit: str, reviewer: str, token: str) -> dict:
    payload = _github_json(f"commits/{commit}/pulls?per_page=100", token)
    pulls = payload if isinstance(payload, list) else []
    matches = [
        pull
        for pull in pulls
        if pull.get("merged_at")
        and (pull.get("base") or {}).get("ref") == "main"
        and pull.get("merge_commit_sha") == commit
    ]
    if len(matches) != 1:
        raise ProvenanceError(
            f"commit {commit} must map to exactly one merged main pull request"
        )
    pull = _github_json(f"pulls/{matches[0]['number']}", token)
    if not isinstance(pull, dict):
        raise ProvenanceError(f"GitHub returned invalid pull request for {commit}")
    candidate = str((pull.get("head") or {}).get("sha") or "")
    if not candidate:
        raise ProvenanceError(f"pull request for {commit} has no candidate SHA")
    reviews_payload = _github_json(
        f"pulls/{pull['number']}/reviews?per_page=100", token
    )
    reviews = reviews_payload if isinstance(reviews_payload, list) else []
    approval = _latest_review_by_user(reviews, reviewer)
    if approval is None or approval.get("state") != "APPROVED":
        raise ProvenanceError(
            f"pull request #{pull['number']} lacks current approval by {reviewer}"
        )
    if approval.get("commit_id") != candidate:
        raise ProvenanceError(
            f"pull request #{pull['number']} approval does not cover its final SHA"
        )
    if not approval.get("submitted_at") or approval["submitted_at"] > pull["merged_at"]:
        raise ProvenanceError(
            f"pull request #{pull['number']} approval timestamp is invalid"
        )
    _require_trusted_candidate_status(candidate, token)
    return pull


def _require_successful_workflow(commit: str, workflow_path: str, token: str) -> None:
    encoded_path = urllib.parse.quote(workflow_path, safe="")
    workflow = _github_json(f"actions/workflows/{encoded_path}", token)
    if not isinstance(workflow, dict):
        raise ProvenanceError(f"GitHub returned invalid workflow data for {workflow_path}")
    if workflow.get("path") != workflow_path or workflow.get("state") != "active":
        raise ProvenanceError(f"required workflow {workflow_path} is not active")
    workflow_id = workflow.get("id")
    query = urllib.parse.urlencode(
        {"head_sha": commit, "event": "push", "per_page": 100}
    )
    payload = _github_json(f"actions/workflows/{workflow_id}/runs?{query}", token)
    runs = payload.get("workflow_runs", []) if isinstance(payload, dict) else []
    exact = [
        run
        for run in runs
        if run.get("workflow_id") == workflow_id
        and run.get("path") == workflow_path
        and run.get("head_sha") == commit
        and run.get("head_branch") == "main"
        and run.get("event") == "push"
    ]
    if not exact:
        raise ProvenanceError(
            f"commit {commit} has no exact main push run for {workflow_path}"
        )
    latest = max(exact, key=lambda run: int(run.get("id") or 0))
    if latest.get("status") != "completed" or latest.get("conclusion") != "success":
        raise ProvenanceError(
            f"commit {commit} latest {workflow_path} run did not succeed"
        )


def _require_release_plan_only(parent: str, commit: str, version: str) -> str:
    changed = [
        line
        for line in _git_capture("diff", "--name-only", parent, commit).splitlines()
        if line
    ]
    if len(changed) != 1:
        raise ProvenanceError(
            f"release evidence commit must change one plan; changed={changed}"
        )
    path = changed[0]
    if (
        not path.startswith(("docs/03_Plans/active/", "docs/03_Plans/completed/"))
        or f"release-{version}" not in Path(path).name
        or not path.endswith(".md")
    ):
        raise ProvenanceError(f"release evidence commit changed unexpected path {path}")
    return path


def verify_release_provenance(tag: str, reviewer: str, token: str) -> dict:
    if not tag.startswith("v"):
        raise ProvenanceError(f"release tag must start with v: {tag!r}")
    version = tag[1:]
    if _git_capture("cat-file", "-t", f"refs/tags/{tag}") != "tag":
        raise ProvenanceError(f"{tag} must be an annotated tag")
    release_commit = _git_capture("rev-parse", f"refs/tags/{tag}^{{commit}}")
    if _git_capture("rev-parse", "refs/remotes/origin/main") != release_commit:
        raise ProvenanceError(f"{tag} must point to the current origin/main commit")
    local_parents = _git_capture(
        "rev-list", "--parents", "-n", "1", release_commit
    ).split()
    if len(local_parents) != 2:
        raise ProvenanceError("release evidence commit must have exactly one parent")
    production_commit = local_parents[1]
    plan = _require_release_plan_only(
        production_commit, release_commit, version
    )

    for commit in (production_commit, release_commit):
        _require_verified_commit(commit, token)
        _require_reviewed_main_pr(commit, reviewer, token)
        for workflow_path in REQUIRED_WORKFLOWS:
            _require_successful_workflow(commit, workflow_path, token)

    return {
        "tag": tag,
        "release_commit": release_commit,
        "production_commit": production_commit,
        "release_plan": plan,
        "reviewer": reviewer,
        "workflows": list(REQUIRED_WORKFLOWS),
    }


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Verify immutable reviewed release provenance."
    )
    parser.add_argument("--tag", required=True)
    parser.add_argument("--reviewer", required=True)
    args = parser.parse_args()
    token = os.environ.get("GH_TOKEN", "").strip()
    if not token:
        raise SystemExit("GH_TOKEN is required")
    if os.environ.get("GITHUB_REPOSITORY", GITHUB_REPOSITORY) != GITHUB_REPOSITORY:
        raise SystemExit(f"release must run in {GITHUB_REPOSITORY}")
    print(
        json.dumps(
            verify_release_provenance(args.tag, args.reviewer, token),
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
