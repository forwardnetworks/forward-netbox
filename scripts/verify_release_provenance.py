#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import http.client
import json
import os
import subprocess
import urllib.parse
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
GITHUB_REPOSITORY = "forwardnetworks/forward-netbox"
GITHUB_API_URL = "https://api.github.com"
# The trusted-scanner workflow was removed with the rest of the CI gates. The
# sensitive-content scan still runs - `release.yml` invokes
# `check_sensitive_content.py` directly, and pre-commit runs it locally - but
# there is no longer a GitHub check run or commit status to verify.
# Tags that were cut and then refused at the publish gate. They exist and are
# annotated, so they are valid provenance anchors, but they published nothing
# and must never be promoted in the compatibility table. The harness consults
# this list so that an anchor ahead of the current release is accepted ONLY for
# a tag recorded here - a forgotten promotion is still a failure everywhere else.
#
# v2.7.7: the sensitive-content guard matched a customer name in a test fixture.
# v2.7.8: the history rewrite that removed that name stripped PR association
#         from two commits, so their provenance could not be verified.
UNPUBLISHED_RELEASE_TAGS = ("v2.7.3", "v2.7.7", "v2.7.8", "v2.7.10")

PRIOR_RELEASE_TAG = "v2.8.8"
PRIOR_POST_RELEASE_DOC_COMMIT = "82f04d6c4a932dc5eb484bfa8ef7010791a4d12a"

# Content a specific bridge commit is excused for carrying, keyed by commit hash.
#
# Empty, and that is the design working rather than an oversight. The v2.8.3
# bridge carried four version surfaces, because `stage_post_release` committed
# an unreleased `.dev0` bump onto its own branch and the next branch cut
# inherited it. The bridge is pinned to the first commit after the tag and the
# diff that disqualifies it is immutable, so it was excused here by hash.
#
# The anchor has since moved to v2.8.4, whose bridge is one documentation file,
# so that entry stopped being consulted and was deleted. Keyed by hash means an
# exception expires the moment the anchor passes it, and a dead excuse for a
# provenance rule does not sit in the file waiting to be copied.
#
# The stage that produced the bad bridge now generates a documentation-only one,
# so the next entry here should never be needed. If one is, it names a single
# commit and its exact paths - never a pattern, never a directory.
BRIDGE_CONTENT_EXCEPTIONS = {}
BOOTSTRAP_REQUIRED_FILES = (
    "scripts/check_sensitive_content.py",
    "scripts/sensitive_content.py",
)
# SHA-256 of each bootstrap file as reviewed. Update these in the same pull
# request that changes the file; a release whose tree disagrees fails closed.
BOOTSTRAP_FILE_DIGESTS = {
    "scripts/check_sensitive_content.py": (
        "69ccf428f255bc158217e0cb3e167d44fe3dbb68f0dc3bb47bc26bf054e747a8"
    ),
    "scripts/sensitive_content.py": (
        "769660482d01fb5c25484c4baeb21ee263e0c233a2a77db87f6f058a8f5fc6a0"
    ),
}
# Gates run locally now. `invoke ci` and `invoke artifact-test` are recorded in
# the release plan's authorization section, bound to the tagged commit's parent,
# so provenance no longer looks for a workflow run it cannot find.
REQUIRED_WORKFLOWS = ()
GITHUB_ACTIONS_APP_ID = 15368
MAIN_RULESET_NAME = "main-release-integrity"
RETIRED_VERSION_TAG_CREATION_RULESET = "version-tag-creation"
VERSION_TAG_INTEGRITY_RULESET = "version-tag-integrity"
PYPI_ENVIRONMENT = "pypi"


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


def _git_capture_bytes(*arguments: str) -> bytes:
    """Capture raw bytes. Digests must not depend on decoding or stripping."""
    result = subprocess.run(
        ["git", *arguments],
        cwd=REPO_ROOT,
        check=True,
        capture_output=True,
    )
    return result.stdout


GITHUB_HTTP_ATTEMPTS = 4
GITHUB_HTTP_TIMEOUT = 30


def _github_json(path: str, token: str) -> object:
    """Read one GitHub API endpoint, over a connection that is not closed.

    `urllib.request` forces `Connection: close` on every request - `do_open`
    sets the header after the caller's, so it cannot be overridden - and this
    release host stalls close-mode responses. Measured on it: urllib completed
    1 request in 10, `http.client` with keep-alive completed 6 in 6 and the
    same client sending `close` completed 1 in 6. `gh` and `curl`, which both
    keep the connection alive, were unaffected.

    The stall cost four release cycles and was worked around with a
    `sitecustomize` shim that stripped the header - which lived outside the
    repository, so the tagged tree stayed broken. This is the durable version:
    speak HTTP/1.1 directly and send no `Connection` header at all, leaving the
    HTTP/1.1 default in place.

    Retries are bounded and cover only transport faults. An HTTP status is an
    answer, so a 404 or a 403 is raised on the first response rather than
    attempted again - retrying a refusal would turn a clear provenance failure
    into a slow one.
    """
    endpoint = f"{GITHUB_API_URL}/repos/{GITHUB_REPOSITORY}"
    if path.strip("/"):
        endpoint = f"{endpoint}/{path.lstrip('/')}"
    parsed = urllib.parse.urlsplit(endpoint)
    selector = parsed.path + (f"?{parsed.query}" if parsed.query else "")
    headers = {
        "Accept": "application/vnd.github+json",
        "Authorization": f"Bearer {token}",
        "User-Agent": "forward-netbox-release-provenance",
        "X-GitHub-Api-Version": "2022-11-28",
    }
    last_error: Exception | None = None
    for _ in range(GITHUB_HTTP_ATTEMPTS):
        connection = http.client.HTTPSConnection(
            parsed.hostname,
            parsed.port or 443,
            timeout=GITHUB_HTTP_TIMEOUT,
        )
        try:
            connection.request("GET", selector, headers=headers)
            response = connection.getresponse()
            body = response.read()
            if response.status >= 400:
                raise ProvenanceError(
                    f"GitHub returned HTTP {response.status} for "
                    f"{path or '<repository>'}: "
                    f"{body.decode('utf-8', 'replace')[:200]}"
                )
            return json.loads(body)
        except (OSError, http.client.HTTPException) as error:
            # Transport only. `ProvenanceError` and a JSON decode failure are
            # answers about the release and must not be retried into silence.
            last_error = error
        finally:
            connection.close()
    raise ProvenanceError(
        f"GitHub request for {path or '<repository>'} failed after "
        f"{GITHUB_HTTP_ATTEMPTS} attempts: {last_error}"
    )


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


def _require_main_ruleset(token: str) -> list[str]:
    ruleset = _named_ruleset(MAIN_RULESET_NAME, token)
    _require_ruleset_identity(
        ruleset,
        name=MAIN_RULESET_NAME,
        target="branch",
        ref_pattern="refs/heads/main",
    )
    if ruleset.get("bypass_actors") != []:
        raise ProvenanceError("protected main ruleset must not have bypass actors")
    # `required_status_checks` is deliberately absent. This repository has a
    # single maintainer and runs its gates locally (`invoke ci`,
    # `invoke artifact-test`), whose results are recorded in the release plan's
    # authorization section and bound to the tagged commit's parent. The branch
    # controls that still matter - no deletion, no force-push, linear history,
    # squash-only through a pull request - are all still asserted below.
    rules = _rules_by_type(
        ruleset,
        {
            "deletion",
            "non_fast_forward",
            "required_linear_history",
            "pull_request",
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
    return []


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

    _require_main_ruleset(token)
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
        "required_statuses": [],
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


def _require_merged_main_pr(
    commit: str,
    token: str,
    *,
    allow_direct_control_commit: bool = False,
) -> bool:
    pulls = _github_pages(f"commits/{commit}/pulls", token)
    matches = [
        pull
        for pull in pulls
        if pull.get("merged_at")
        and (pull.get("base") or {}).get("ref") == "main"
        and pull.get("merge_commit_sha") == commit
    ]
    if len(matches) != 1:
        if allow_direct_control_commit:
            changed = {
                line
                for line in _git_capture(
                    "diff",
                    "--name-only",
                    f"{commit}^",
                    commit,
                ).splitlines()
                if line
            }
            runtime_paths = {
                path
                for path in changed
                if path.startswith("forward_netbox/")
                and not path.startswith("forward_netbox/tests/")
            }
            if runtime_paths:
                raise ProvenanceError(
                    f"direct release-control commit {commit} changes production code"
                )
            return True
        raise ProvenanceError(
            f"commit {commit} must map to exactly one merged main pull request"
        )
    pull = _github_json(f"pulls/{matches[0]['number']}", token)
    if not isinstance(pull, dict):
        raise ProvenanceError(f"GitHub returned invalid pull request for {commit}")
    candidate = str((pull.get("head") or {}).get("sha") or "")
    if not candidate:
        raise ProvenanceError(f"pull request for {commit} has no candidate SHA")
    return False


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


def _require_release_plan(parent: str, commit: str, version: str) -> str:
    """Require the tagged commit to carry this version's authorization record.

    The commit may also change code. Demanding an evidence-only commit forced a
    second, prose-only pull request per release whose sole content was a plan
    file; the branch ruleset requires zero approving reviews, so that commit
    never delivered the independent sign-off its shape implied. What actually
    binds authorization to the tagged tree is that the plan naming this exact
    version is part of the tagged commit, which is still enforced here.
    """
    changed = [
        line
        for line in _git_capture("diff", "--name-only", parent, commit).splitlines()
        if line
    ]
    plans = [
        path
        for path in changed
        if path.startswith(("docs/03_Plans/active/", "docs/03_Plans/completed/"))
        and f"release-{version}" in Path(path).name
        and path.endswith(".md")
    ]
    if len(plans) != 1:
        raise ProvenanceError(
            "release commit must change exactly one release-"
            f"{version} plan; changed={changed}"
        )
    return plans[0]


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


# Exactly which paths a post-release commit may touch. The rule is the intent
# the narrower predecessor was reaching for - the bridge carries no executable
# code - expressed as an allowlist rather than as a single hard-coded filename.
DOCUMENTATION_BRIDGE_FILES = frozenset({"CHANGELOG.md", "README.md"})


def _is_documentation_path(path: str) -> bool:
    """Return whether a post-release bridge may legitimately touch ``path``.

    Two commit shapes legitimately follow a release, and the previous rule
    admitted only the first: archiving the release plan under
    ``docs/03_Plans/completed/``, and promoting the release candidate, which
    rewrites the changelog and the three compatibility tables. `v2.7.0` was
    promoted without being archived, so promotion took the slot the check
    reserved for archival and no later commit could reclaim it - the bridge is
    fixed at the first commit after the tag. That made every subsequent release
    unverifiable, which is the same shape of unsatisfiable pairing that spent
    `v2.6.10` and `v2.6.11`.

    Widening to a path allowlist accepts both shapes and still refuses anything
    executable: a bridge touching plugin code, scripts, or workflows fails
    exactly as before.
    """
    if path in DOCUMENTATION_BRIDGE_FILES:
        return True
    return path.startswith("docs/") and path.endswith(".md")


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
    excused = BRIDGE_CONTENT_EXCEPTIONS.get(PRIOR_POST_RELEASE_DOC_COMMIT, ())
    if not changed or not all(
        _is_documentation_path(path) or path in excused for path in changed
    ):
        raise ProvenanceError(
            f"post-release bridge must be documentation-only; changed={changed}"
        )
    if len(lineage) < 4:
        raise ProvenanceError(
            "release lineage must include bootstrap, production, and evidence commits"
        )
    return lineage[1:]


def _require_security_bootstrap(release_commit: str) -> None:
    """Require the trusted scanner to be present, byte for byte, at the release.

    This used to diff `PRIOR_POST_RELEASE_DOC_COMMIT` against the first reviewed
    commit and require the three files to appear in that diff. That only ever
    held because the anchor was pinned immediately before `3b6fe4d`, the single
    commit that introduced them - so the anchor could never move forward, while
    the retention walk in `_require_merged_main_pr` needs it to. Those two
    requirements are not jointly satisfiable: with a fixed anchor the reviewed
    chain grows until GitHub expires a run, which is what burned `v2.6.10`, and
    with a moved anchor the diff no longer contains the bootstrap, which is what
    burned `v2.6.11`.

    Pinning content is strictly stronger than the diff test it replaces. The old
    check accepted any change that touched all three paths, including one that
    gutted them; this one accepts only the exact reviewed bytes. Changing the
    scanner means updating these digests in the same reviewed pull request,
    which the trusted scanner and CodeQL both gate.
    """
    mismatched = []
    for path, expected in sorted(BOOTSTRAP_FILE_DIGESTS.items()):
        try:
            blob = _git_capture_bytes("show", f"{release_commit}:{path}")
        except subprocess.CalledProcessError:
            mismatched.append(f"{path} (absent)")
            continue
        if hashlib.sha256(blob).hexdigest() != expected:
            mismatched.append(f"{path} (content)")
    if mismatched:
        raise ProvenanceError(
            f"security bootstrap does not match reviewed content: {mismatched}"
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
    plan = _require_release_plan(production_commit, release_commit, version)

    reviewed_commits = _require_prior_release_bridge(release_commit)
    if reviewed_commits[-2:] != [production_commit, release_commit]:
        raise ProvenanceError(
            "release must end with the production and evidence pull requests"
        )
    _require_security_bootstrap(release_commit)

    # The production and release commits must ALWAYS come through a merged pull
    # request; only the control commits ahead of them may be direct.
    #
    # This was `index < 3`, which reads as "the first three positions", but a
    # lineage is allowed to be exactly three reviewed commits and ours routinely
    # is - anchor, production, release. At that size the allowance covered every
    # commit including the release pair, which is the opposite of the intent.
    # Anchoring to the END makes the rule say what it means at any length.
    control_commit_limit = len(reviewed_commits) - 2
    for index, commit in enumerate(reviewed_commits):
        _require_release_commit_shape(commit, token)
        direct_control_commit = _require_merged_main_pr(
            commit,
            token,
            allow_direct_control_commit=index < control_commit_limit,
        )
        if direct_control_commit:
            continue
        for workflow_path in REQUIRED_WORKFLOWS:
            _require_successful_workflow(commit, workflow_path, token)

    return {
        "release_commit": release_commit,
        "production_commit": production_commit,
        "first_reviewed_commit": reviewed_commits[0],
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
