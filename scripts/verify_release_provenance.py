#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import urllib.parse
import urllib.request
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
GITHUB_REPOSITORY = "forwardnetworks/forward-netbox"
GITHUB_API_URL = "https://api.github.com"
TRUSTED_STATUS_CONTEXT = "Trusted sensitive-content scan"
TRUSTED_STATUS_CREATOR = "github-actions[bot]"
TRUSTED_SCANNER_WORKFLOW = ".github/workflows/trusted-sensitive-pr.yml"
TRUSTED_TAG_WORKFLOW = ".github/workflows/trusted-tag.yml"
TRUSTED_ANCHOR_TAG = "security-bootstrap-2.6"
PRIOR_RELEASE_TAG = "v2.5.11"
PRIOR_POST_RELEASE_DOC_COMMIT = "df85f2e94b91f5afe3a419c3121aeb189f2b2737"
TRUSTED_RELEASE_FILES = (
    ".github/workflows/release.yml",
    TRUSTED_SCANNER_WORKFLOW,
    TRUSTED_TAG_WORKFLOW,
    "requirements-release.in",
    "requirements-release.txt",
    "scripts/authorize_trusted_tag.py",
    "scripts/build_reproducible_distribution.py",
    "scripts/release.py",
    "scripts/verify_release_provenance.py",
)
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


def _github_pages(path: str, token: str) -> list[dict]:
    items: list[dict] = []
    separator = "&" if "?" in path else "?"
    for page in range(1, 1001):
        payload = _github_json(
            f"{path}{separator}per_page=100&page={page}",
            token,
        )
        if not isinstance(payload, list):
            raise ProvenanceError(f"GitHub returned invalid paginated data for {path}")
        items.extend(payload)
        if len(payload) < 100:
            return items
    raise ProvenanceError(f"GitHub pagination exceeded the safety bound for {path}")


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
        if str((review.get("user") or {}).get("login") or "").lower() == login.lower()
    ]
    if not matching:
        return None
    return max(matching, key=lambda review: int(review.get("id") or 0))


def _trusted_scanner_workflow_id(token: str) -> int:
    encoded_path = urllib.parse.quote(TRUSTED_SCANNER_WORKFLOW, safe="")
    workflow = _github_json(f"actions/workflows/{encoded_path}", token)
    if not isinstance(workflow, dict):
        raise ProvenanceError("GitHub returned invalid trusted scanner workflow data")
    if (
        workflow.get("path") != TRUSTED_SCANNER_WORKFLOW
        or workflow.get("state") != "active"
    ):
        raise ProvenanceError("trusted scanner workflow is not active")
    try:
        return int(workflow["id"])
    except (KeyError, TypeError, ValueError) as exc:
        raise ProvenanceError("trusted scanner workflow has no stable ID") from exc


def _trusted_status_run_id(target_url: object) -> int:
    parsed = urllib.parse.urlparse(str(target_url or ""))
    expected_path = rf"/{re.escape(GITHUB_REPOSITORY)}/actions/runs/([0-9]+)"
    match = re.fullmatch(expected_path, parsed.path)
    if (
        parsed.scheme != "https"
        or parsed.netloc != "github.com"
        or parsed.params
        or parsed.query
        or parsed.fragment
        or match is None
    ):
        raise ProvenanceError("trusted scanner status has an invalid run URL")
    return int(match.group(1))


def _require_trusted_candidate_status(
    candidate: str,
    pull_number: int,
    token: str,
) -> None:
    statuses = _github_pages(f"commits/{candidate}/statuses", token)
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
    run_id = _trusted_status_run_id(latest.get("target_url"))
    run = _github_json(f"actions/runs/{run_id}", token)
    if not isinstance(run, dict):
        raise ProvenanceError("GitHub returned invalid trusted scanner run data")
    workflow_id = _trusted_scanner_workflow_id(token)
    if (
        run.get("id") != run_id
        or run.get("workflow_id") != workflow_id
        or run.get("path") != TRUSTED_SCANNER_WORKFLOW
        or run.get("event") != "pull_request_target"
        or run.get("status") != "completed"
        or run.get("conclusion") != "success"
    ):
        raise ProvenanceError("trusted scanner status is not backed by the trusted run")
    pull_matches = [
        pull
        for pull in run.get("pull_requests") or []
        if pull.get("number") == pull_number
        and (pull.get("head") or {}).get("sha") == candidate
        and (pull.get("base") or {}).get("ref") == "main"
    ]
    if len(pull_matches) != 1:
        raise ProvenanceError(
            "trusted scanner run does not cover the exact pull request candidate"
        )


def _require_reviewed_main_pr(
    commit: str,
    reviewer: str,
    token: str,
    *,
    require_trusted_status: bool = True,
) -> dict:
    pulls = _github_pages(f"commits/{commit}/pulls", token)
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
    reviews = _github_pages(f"pulls/{pull['number']}/reviews", token)
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
    if require_trusted_status:
        _require_trusted_candidate_status(candidate, int(pull["number"]), token)
    return pull


def _require_successful_workflow(commit: str, workflow_path: str, token: str) -> None:
    encoded_path = urllib.parse.quote(workflow_path, safe="")
    workflow = _github_json(f"actions/workflows/{encoded_path}", token)
    if not isinstance(workflow, dict):
        raise ProvenanceError(
            f"GitHub returned invalid workflow data for {workflow_path}"
        )
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


def _commit_parent(commit: str) -> str:
    parts = _git_capture("rev-list", "--parents", "-n", "1", commit).split()
    if len(parts) != 2:
        raise ProvenanceError(f"commit {commit} must have exactly one parent")
    return parts[1]


def _require_annotated_tag(tag: str) -> str:
    if _git_capture("cat-file", "-t", f"refs/tags/{tag}") != "tag":
        raise ProvenanceError(f"{tag} must be an annotated tag")
    return _git_capture("rev-parse", f"refs/tags/{tag}^{{commit}}")


def _first_parent_commits(start: str, end: str) -> list[str]:
    try:
        _git_capture("merge-base", "--is-ancestor", start, end)
    except subprocess.CalledProcessError as exc:
        raise ProvenanceError(f"{start} is not an ancestor of {end}") from exc
    return [
        line
        for line in _git_capture(
            "rev-list",
            "--first-parent",
            "--reverse",
            f"{start}..{end}",
        ).splitlines()
        if line
    ]


def _require_prior_release_bridge(anchor: str) -> None:
    prior_release = _require_annotated_tag(PRIOR_RELEASE_TAG)
    bridge = _first_parent_commits(prior_release, anchor)
    if bridge != [PRIOR_POST_RELEASE_DOC_COMMIT, anchor]:
        raise ProvenanceError(
            "trusted bootstrap must directly follow the reviewed post-release bridge"
        )
    if _commit_parent(PRIOR_POST_RELEASE_DOC_COMMIT) != prior_release:
        raise ProvenanceError("post-release documentation commit has the wrong parent")
    changed = [
        line
        for line in _git_capture(
            "diff",
            "--name-only",
            prior_release,
            PRIOR_POST_RELEASE_DOC_COMMIT,
        ).splitlines()
        if line
    ]
    if (
        len(changed) != 1
        or not changed[0].startswith("docs/03_Plans/completed/")
        or not changed[0].endswith(".md")
    ):
        raise ProvenanceError(
            f"post-release bridge must be documentation-only; changed={changed}"
        )
    if _commit_parent(anchor) != PRIOR_POST_RELEASE_DOC_COMMIT:
        raise ProvenanceError("trusted bootstrap has an unexpected parent")


def _require_trust_files_unchanged(anchor: str, release_commit: str) -> None:
    changed = [
        line
        for line in _git_capture(
            "diff",
            "--name-only",
            anchor,
            release_commit,
            "--",
            *TRUSTED_RELEASE_FILES,
        ).splitlines()
        if line
    ]
    if changed:
        raise ProvenanceError(
            f"trusted release controller changed after bootstrap: {changed}"
        )


def verify_trusted_anchor_candidate(
    anchor_commit: str,
    reviewer: str,
    token: str,
) -> dict:
    _require_prior_release_bridge(anchor_commit)
    _require_verified_commit(anchor_commit, token)
    pull = _require_reviewed_main_pr(
        anchor_commit,
        reviewer,
        token,
        require_trusted_status=False,
    )
    for workflow_path in REQUIRED_WORKFLOWS:
        _require_successful_workflow(anchor_commit, workflow_path, token)
    return {
        "trusted_anchor": anchor_commit,
        "pull_request": pull["number"],
        "reviewer": reviewer,
        "workflows": list(REQUIRED_WORKFLOWS),
    }


def verify_release_commit_provenance(
    release_commit: str,
    version: str,
    reviewer: str,
    token: str,
) -> dict:
    if _git_capture("rev-parse", "refs/remotes/origin/main") != release_commit:
        raise ProvenanceError(
            "release commit must equal the current origin/main commit"
        )
    production_commit = _commit_parent(release_commit)
    plan = _require_release_plan_only(production_commit, release_commit, version)

    anchor = _require_annotated_tag(TRUSTED_ANCHOR_TAG)
    _require_prior_release_bridge(anchor)
    _require_trust_files_unchanged(anchor, release_commit)
    reviewed_commits = [anchor, *_first_parent_commits(anchor, release_commit)]
    if reviewed_commits[-2:] != [production_commit, release_commit]:
        raise ProvenanceError(
            "release must end with the production and evidence pull requests"
        )

    for index, commit in enumerate(reviewed_commits):
        _require_verified_commit(commit, token)
        _require_reviewed_main_pr(
            commit,
            reviewer,
            token,
            require_trusted_status=index != 0,
        )
        for workflow_path in REQUIRED_WORKFLOWS:
            _require_successful_workflow(commit, workflow_path, token)

    return {
        "release_commit": release_commit,
        "production_commit": production_commit,
        "trusted_anchor": anchor,
        "reviewed_commits": reviewed_commits,
        "release_plan": plan,
        "reviewer": reviewer,
        "workflows": list(REQUIRED_WORKFLOWS),
    }


def verify_release_provenance(tag: str, reviewer: str, token: str) -> dict:
    if not tag.startswith("v"):
        raise ProvenanceError(f"release tag must start with v: {tag!r}")
    result = verify_release_commit_provenance(
        _require_annotated_tag(tag),
        tag[1:],
        reviewer,
        token,
    )
    return {"tag": tag, **result}


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
