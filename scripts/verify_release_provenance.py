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
PRIOR_RELEASE_TAG = "v2.5.11"
PRIOR_POST_RELEASE_DOC_COMMIT = "f9a8420a8bcc2d3afe338d0435a17df9e2bc01d0"
BOOTSTRAP_REQUIRED_FILES = (
    TRUSTED_SCANNER_WORKFLOW,
    "scripts/check_sensitive_content.py",
    "scripts/sensitive_content.py",
)
REQUIRED_WORKFLOWS = (
    ".github/workflows/ci.yml",
    ".github/workflows/codeql.yml",
)
GITHUB_ACTIONS_APP_ID = 15368
GITHUB_ADVANCED_SECURITY_APP_ID = 57789
MAIN_RULESET_NAME = "main-release-integrity"
RETIRED_VERSION_TAG_CREATION_RULESET = "version-tag-creation"
VERSION_TAG_INTEGRITY_RULESET = "version-tag-integrity"
PYPI_ENVIRONMENT = "pypi"
BASE_REQUIRED_STATUS_CHECKS = {
    ("Validate NetBox v4.6.5", GITHUB_ACTIONS_APP_ID),
    ("CodeQL python", GITHUB_ACTIONS_APP_ID),
    ("CodeQL javascript-typescript", GITHUB_ACTIONS_APP_ID),
    ("CodeQL", GITHUB_ADVANCED_SECURITY_APP_ID),
}


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
    endpoint = f"{GITHUB_API_URL}/repos/{GITHUB_REPOSITORY}"
    if path.strip("/"):
        endpoint = f"{endpoint}/{path.lstrip('/')}"
    request = urllib.request.Request(
        endpoint,
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


def _named_ruleset(name: str, token: str) -> dict:
    matches = [
        ruleset
        for ruleset in _github_pages("rulesets", token)
        if ruleset.get("name") == name
        and ruleset.get("source_type") == "Repository"
        and ruleset.get("source") == GITHUB_REPOSITORY
    ]
    if len(matches) != 1:
        raise ProvenanceError(f"required repository ruleset {name!r} is not unique")
    payload = _github_json(f"rulesets/{matches[0].get('id')}", token)
    if not isinstance(payload, dict):
        raise ProvenanceError(f"GitHub returned invalid ruleset data for {name!r}")
    return payload


def _require_ruleset_absent(name: str, token: str) -> None:
    matches = [
        ruleset
        for ruleset in _github_pages("rulesets", token)
        if ruleset.get("name") == name
        and ruleset.get("source_type") == "Repository"
        and ruleset.get("source") == GITHUB_REPOSITORY
    ]
    if matches:
        raise ProvenanceError(f"retired repository ruleset {name!r} remains active")


def _require_ruleset_identity(
    ruleset: dict,
    *,
    name: str,
    target: str,
    ref_pattern: str,
) -> None:
    if (
        ruleset.get("name") != name
        or ruleset.get("target") != target
        or ruleset.get("enforcement") != "active"
        or ruleset.get("source_type") != "Repository"
        or ruleset.get("source") != GITHUB_REPOSITORY
        or ruleset.get("bypass_actors") is None
    ):
        raise ProvenanceError(f"ruleset {name!r} identity or enforcement is invalid")
    ref_name = (ruleset.get("conditions") or {}).get("ref_name") or {}
    if ref_name.get("include") != [ref_pattern] or ref_name.get("exclude") != []:
        raise ProvenanceError(f"ruleset {name!r} has an invalid ref condition")


def _rules_by_type(ruleset: dict, expected: set[str]) -> dict[str, dict]:
    grouped: dict[str, list[dict]] = {}
    for rule in ruleset.get("rules") or []:
        grouped.setdefault(str(rule.get("type") or ""), []).append(rule)
    if set(grouped) != expected or any(len(rules) != 1 for rules in grouped.values()):
        raise ProvenanceError(f"ruleset {ruleset.get('name')!r} has invalid rules")
    return {rule_type: rules[0] for rule_type, rules in grouped.items()}


def _require_main_ruleset(token: str, *, require_trusted_status: bool) -> list[str]:
    ruleset = _named_ruleset(MAIN_RULESET_NAME, token)
    _require_ruleset_identity(
        ruleset,
        name=MAIN_RULESET_NAME,
        target="branch",
        ref_pattern="refs/heads/main",
    )
    if ruleset.get("bypass_actors") != []:
        raise ProvenanceError("protected main ruleset must not have bypass actors")
    rules = _rules_by_type(
        ruleset,
        {
            "deletion",
            "non_fast_forward",
            "required_linear_history",
            "pull_request",
            "required_status_checks",
        },
    )
    pull_parameters = rules["pull_request"].get("parameters") or {}
    required_pull_parameters = {
        "required_approving_review_count": 0,
        "dismiss_stale_reviews_on_push": False,
        "require_code_owner_review": False,
        "require_last_push_approval": False,
        "required_review_thread_resolution": True,
        "allowed_merge_methods": ["squash"],
    }
    if any(
        pull_parameters.get(key) != value
        for key, value in required_pull_parameters.items()
    ):
        raise ProvenanceError("protected main pull-request controls are incomplete")
    status_parameters = rules["required_status_checks"].get("parameters") or {}
    if (
        status_parameters.get("strict_required_status_checks_policy") is not True
        or status_parameters.get("do_not_enforce_on_create") is not False
    ):
        raise ProvenanceError("protected main status-check policy is not strict")
    actual_statuses = {
        (str(status.get("context") or ""), status.get("integration_id"))
        for status in status_parameters.get("required_status_checks") or []
    }
    expected_statuses = set(BASE_REQUIRED_STATUS_CHECKS)
    if require_trusted_status:
        expected_statuses.add((TRUSTED_STATUS_CONTEXT, GITHUB_ACTIONS_APP_ID))
    if not expected_statuses.issubset(actual_statuses):
        raise ProvenanceError(
            "protected main is missing a required authenticated status"
        )
    return sorted(context for context, _integration_id in expected_statuses)


def _require_tag_ruleset(
    token: str,
    *,
    name: str,
    ref_pattern: str,
) -> None:
    ruleset = _named_ruleset(name, token)
    _require_ruleset_identity(
        ruleset,
        name=name,
        target="tag",
        ref_pattern=ref_pattern,
    )
    _rules_by_type(ruleset, {"deletion", "non_fast_forward"})
    if ruleset.get("bypass_actors") != []:
        raise ProvenanceError(f"tag integrity ruleset {name!r} has a bypass")


def _require_environment(
    token: str,
    *,
    name: str,
    policy_name: str,
    policy_type: str,
) -> None:
    encoded_name = urllib.parse.quote(name, safe="")
    environment = _github_json(f"environments/{encoded_name}", token)
    if not isinstance(environment, dict) or environment.get("name") != name:
        raise ProvenanceError(f"GitHub returned invalid environment data for {name!r}")
    if environment.get("can_admins_bypass") is not False:
        raise ProvenanceError(f"environment {name!r} permits administrator bypass")
    if environment.get("deployment_branch_policy") != {
        "protected_branches": False,
        "custom_branch_policies": True,
    }:
        raise ProvenanceError(f"environment {name!r} has an invalid branch policy")
    reviewer_rules = [
        rule
        for rule in environment.get("protection_rules") or []
        if rule.get("type") == "required_reviewers"
    ]
    if reviewer_rules:
        raise ProvenanceError(f"environment {name!r} has an approval gate")
    policies = _github_json(
        f"environments/{encoded_name}/deployment-branch-policies",
        token,
    )
    if not isinstance(policies, dict):
        raise ProvenanceError(f"GitHub returned invalid policies for {name!r}")
    actual_policies = policies.get("branch_policies") or []
    if len(actual_policies) != 1 or {
        "name": actual_policies[0].get("name"),
        "type": actual_policies[0].get("type"),
    } != {"name": policy_name, "type": policy_type}:
        raise ProvenanceError(f"environment {name!r} deployment policy is invalid")


def verify_github_release_controls(token: str) -> dict:
    repository = _github_json("", token)
    if not isinstance(repository, dict):
        raise ProvenanceError("GitHub returned invalid repository settings")
    required_repository_settings = {
        "allow_auto_merge": True,
        "allow_merge_commit": False,
        "allow_squash_merge": True,
        "delete_branch_on_merge": True,
    }
    if any(
        repository.get(key) != value
        for key, value in required_repository_settings.items()
    ):
        raise ProvenanceError("repository merge controls are not release-safe")
    actions = _github_json("actions/permissions", token)
    if not isinstance(actions, dict) or actions.get("enabled") is not True:
        raise ProvenanceError("GitHub Actions is not enabled")
    if actions.get("sha_pinning_required") is not True:
        raise ProvenanceError("GitHub Actions SHA pinning is not required")

    required_statuses = _require_main_ruleset(token, require_trusted_status=True)
    _require_ruleset_absent(RETIRED_VERSION_TAG_CREATION_RULESET, token)
    _require_tag_ruleset(
        token,
        name=VERSION_TAG_INTEGRITY_RULESET,
        ref_pattern="refs/tags/v*",
    )
    _require_environment(
        token,
        name=PYPI_ENVIRONMENT,
        policy_name="v*",
        policy_type="tag",
    )
    return {
        "main_ruleset": MAIN_RULESET_NAME,
        "required_statuses": required_statuses,
        "pypi_environment": PYPI_ENVIRONMENT,
    }


def _require_release_commit_shape(commit: str, token: str) -> dict:
    """Validate the GitHub commit object; PR provenance supplies trust."""
    payload = _github_json(f"commits/{commit}", token)
    if not isinstance(payload, dict):
        raise ProvenanceError(f"GitHub returned invalid commit data for {commit}")
    parents = payload.get("parents") or []
    if len(parents) != 1:
        raise ProvenanceError(f"commit {commit} must have exactly one parent")
    return payload


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


def _require_merged_main_pr(
    commit: str,
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


def _require_prior_release_bridge(release_commit: str) -> list[str]:
    prior_release = _require_annotated_tag(PRIOR_RELEASE_TAG)
    lineage = _first_parent_commits(prior_release, release_commit)
    if not lineage or lineage[0] != PRIOR_POST_RELEASE_DOC_COMMIT:
        raise ProvenanceError(
            "release lineage must start with the known post-release documentation bridge"
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
    if len(lineage) < 4:
        raise ProvenanceError(
            "release lineage must include bootstrap, production, and evidence commits"
        )
    return lineage[1:]


def _require_security_bootstrap(parent: str, commit: str) -> None:
    changed = [
        line
        for line in _git_capture(
            "diff",
            "--name-only",
            parent,
            commit,
        ).splitlines()
        if line
    ]
    missing = sorted(set(BOOTSTRAP_REQUIRED_FILES) - set(changed))
    if missing:
        raise ProvenanceError(
            f"security bootstrap is missing required files: {missing}"
        )
    if any(path.startswith(("forward_netbox/", "development/")) for path in changed):
        raise ProvenanceError(
            "security bootstrap must not contain production runtime changes"
        )


def _require_release_on_main_lineage(release_commit: str) -> str:
    current_main = _git_capture("rev-parse", "refs/remotes/origin/main")
    try:
        _git_capture("merge-base", "--is-ancestor", release_commit, current_main)
    except subprocess.CalledProcessError as exc:
        raise ProvenanceError(
            "release commit must be an ancestor of the current origin/main commit"
        ) from exc
    return current_main


def verify_release_commit_provenance(
    release_commit: str,
    version: str,
    token: str,
) -> dict:
    _require_release_on_main_lineage(release_commit)
    production_commit = _commit_parent(release_commit)
    plan = _require_release_plan_only(production_commit, release_commit, version)

    reviewed_commits = _require_prior_release_bridge(release_commit)
    if reviewed_commits[-2:] != [production_commit, release_commit]:
        raise ProvenanceError(
            "release must end with the production and evidence pull requests"
        )
    _require_security_bootstrap(PRIOR_POST_RELEASE_DOC_COMMIT, reviewed_commits[0])

    for index, commit in enumerate(reviewed_commits):
        _require_release_commit_shape(commit, token)
        _require_merged_main_pr(
            commit,
            token,
            require_trusted_status=index != 0,
        )
        for workflow_path in REQUIRED_WORKFLOWS:
            _require_successful_workflow(commit, workflow_path, token)

    return {
        "release_commit": release_commit,
        "production_commit": production_commit,
        "security_bootstrap_commit": reviewed_commits[0],
        "reviewed_commits": reviewed_commits,
        "release_plan": plan,
        "workflows": list(REQUIRED_WORKFLOWS),
    }


def verify_release_provenance(tag: str, token: str) -> dict:
    if not tag.startswith("v"):
        raise ProvenanceError(f"release tag must start with v: {tag!r}")
    result = verify_release_commit_provenance(
        _require_annotated_tag(tag),
        tag[1:],
        token,
    )
    return {"tag": tag, **result}


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Verify immutable reviewed release provenance."
    )
    operation = parser.add_mutually_exclusive_group(required=True)
    operation.add_argument("--tag")
    operation.add_argument("--controls-only", action="store_true")
    args = parser.parse_args()
    token = os.environ.get("GH_TOKEN", "").strip()
    if not token:
        raise SystemExit("GH_TOKEN is required")
    if os.environ.get("GITHUB_REPOSITORY", GITHUB_REPOSITORY) != GITHUB_REPOSITORY:
        raise SystemExit(f"release must run in {GITHUB_REPOSITORY}")
    if args.controls_only:
        verify_github_release_controls(token)
        print("GitHub release controls verification passed.")
    else:
        verify_release_provenance(args.tag, token)
        print("Release provenance verification passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
