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
#   publish  - branch, push, and wait for the exact GitHub workflows
#   finish   - promote metadata, open the reviewed production/evidence PRs, or
#              tag the reviewed evidence-only main commit
#
# Default run is prepare + verify. Rollout never happens without --publish, so
# this is safe to run for a dry build.
from __future__ import annotations

import argparse
import json
import os
import re
import socket
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
# The compatibility cell shared by every table row, so a new row reuses the
# previous row's NetBox-support text verbatim.
CURRENT_RELEASE_RE = re.compile(
    r"^\| `v[0-9][^|]*` \| (?P<support>[^|]*) \| Current release;", re.MULTILINE
)

SEMVER_RE = re.compile(r"^\d+\.\d+\.\d+$")
GITHUB_REPOSITORY = "forwardnetworks/forward-netbox"
REQUIRED_RELEASE_WORKFLOWS = (
    ".github/workflows/ci.yml",
    ".github/workflows/codeql.yml",
)
TRUSTED_TAG_WORKFLOW = ".github/workflows/trusted-tag.yml"
RELEASE_REVIEWER = "brandonheller"


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
    """Insert a release candidate while retaining the published current row.

    Finalization promotes the candidate and demotes the prior release only after
    the release branch is green, so unreleased docs never claim publication.
    """
    if "| Release candidate;" in table_text:
        raise ReleaseError("a release candidate already exists")
    match = CURRENT_RELEASE_RE.search(table_text)
    if not match:
        raise ReleaseError("could not find the current-release row to supersede")
    support = match.group("support")
    new_row = f"| `v{version}` | {support} | Release candidate; {summary} |"
    old_line_start = match.start()
    return table_text[:old_line_start] + new_row + "\n" + table_text[old_line_start:]


def promote_release_candidate_text(table_text: str, version: str) -> str:
    """Promote exactly one candidate and demote exactly one current release."""
    candidate_prefix = f"| `v{version}` |"
    lines = table_text.splitlines(keepends=True)
    candidate_indexes = [
        index
        for index, line in enumerate(lines)
        if line.startswith(candidate_prefix) and "| Release candidate;" in line
    ]
    current_indexes = [
        index for index, line in enumerate(lines) if "| Current release;" in line
    ]
    target_is_current = any(
        line.startswith(candidate_prefix) and "| Current release;" in line
        for line in lines
    )
    if not candidate_indexes and target_is_current and len(current_indexes) == 1:
        return table_text
    if len(candidate_indexes) != 1 or len(current_indexes) != 1:
        raise ReleaseError(
            "expected exactly one matching release candidate and current release"
        )
    candidate_index = candidate_indexes[0]
    current_index = current_indexes[0]
    lines[candidate_index] = lines[candidate_index].replace(
        "| Release candidate;", "| Current release;", 1
    )
    lines[current_index] = lines[current_index].replace(
        "| Current release;", f"| Superseded by `v{version}`;", 1
    )
    return "".join(lines)


def read_current_version() -> str:
    text = PYPROJECT.read_text(encoding="utf-8")
    match = re.search(r'^version = "([^"]+)"', text, re.MULTILINE)
    if not match:
        raise ReleaseError("could not read current version from pyproject.toml")
    return match.group(1)


def run(
    cmd: list[str],
    *,
    cwd: Path = REPO_ROOT,
    check: bool = True,
    env: dict[str, str] | None = None,
) -> int:
    print("  $ [redacted release command]")
    result = subprocess.run(cmd, cwd=cwd, env=env)
    if check and result.returncode != 0:
        raise ReleaseError(f"release command failed with exit code {result.returncode}")
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
    # Keep CHANGELOG.md in lockstep with the README table (a pre-commit hook
    # enforces this).
    run([sys.executable, "scripts/gen_changelog.py"])
    print(
        "[prepare] NOTE: author the plan file in docs/03_Plans/active with all 7 "
        "headings, then `git add -A` before verify."
    )


def _available_loopback_port() -> str:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
        listener.bind(("127.0.0.1", 0))
        return str(listener.getsockname()[1])


def release_distribution_artifacts(version: str) -> list[Path]:
    """Return exactly the current wheel and sdist, ignoring stale releases."""
    dist_dir = REPO_ROOT / "dist"
    wheels = sorted(dist_dir.glob(f"forward_netbox-{version}-*.whl"))
    sdists = sorted(dist_dir.glob(f"forward_netbox-{version}.tar.gz"))
    if len(wheels) != 1 or len(sdists) != 1:
        raise ReleaseError(
            "expected exactly one current-version wheel and sdist in dist/; "
            f"found {len(wheels)} wheel(s) and {len(sdists)} sdist(s) for {version}"
        )
    return [wheels[0], sdists[0]]


def stage_verify() -> None:
    print("[verify] mandatory isolated local release gate")
    release_project = "forward-netbox-release-gate"
    release_env = {
        **os.environ,
        "FORWARD_NETBOX_DOCKER_PROJECT": release_project,
        "FORWARD_NETBOX_HOST_PORT": _available_loopback_port(),
        "FORWARD_NETBOX_POSTGRES_DATA_PATH": "netbox-postgres-data",
        "FORWARD_NETBOX_WORKER_AUTORELOAD": "0",
    }
    release_env["NETBOX_URL"] = (
        f"http://127.0.0.1:{release_env['FORWARD_NETBOX_HOST_PORT']}"
    )
    try:
        run([sys.executable, "-m", "invoke", "ci"], env=release_env)
        artifacts = release_distribution_artifacts(read_current_version())
        run(
            [sys.executable, "-m", "twine", "check", *(str(p) for p in artifacts)],
            env=release_env,
        )
        run(
            [sys.executable, "-m", "invoke", "artifact-test"],
            env=release_env,
        )
    finally:
        run(
            [
                "docker",
                "compose",
                "--project-name",
                release_project,
                "--project-directory",
                str(REPO_ROOT / "development"),
                "down",
                "--volumes",
                "--remove-orphans",
            ],
            check=False,
            env=release_env,
        )


def _capture(cmd: list[str]) -> str:
    result = subprocess.run(cmd, cwd=REPO_ROOT, capture_output=True, text=True)
    return result.stdout.strip()


def _verify_live_release_controls() -> None:
    token = _capture(["gh", "auth", "token"])
    if not token:
        raise ReleaseError("GitHub authentication is required for release controls")
    env = {**os.environ, "GH_TOKEN": token}
    run(
        [
            sys.executable,
            "scripts/verify_release_provenance.py",
            "--controls-only",
            "--reviewer",
            RELEASE_REVIEWER,
        ],
        env=env,
    )


def _assert_branch_head(branch: str, expected_commit: str) -> None:
    current_branch = _capture(["git", "branch", "--show-current"])
    current_commit = _capture(["git", "rev-parse", "HEAD"])
    remote_lines = [
        line.split()
        for line in _capture(
            ["git", "ls-remote", "--heads", "origin", branch]
        ).splitlines()
        if line.strip()
    ]
    remote_commits = [
        fields[0]
        for fields in remote_lines
        if len(fields) == 2 and fields[1] == f"refs/heads/{branch}"
    ]
    if current_branch != branch:
        raise ReleaseError(
            f"release operation requires branch {branch}, found {current_branch!r}"
        )
    if current_commit != expected_commit:
        raise ReleaseError(
            "release branch HEAD changed after CI: "
            f"expected {expected_commit}, found {current_commit}"
        )
    if remote_commits != [expected_commit]:
        raise ReleaseError(
            f"origin/{branch} must point only to CI-approved {expected_commit}; "
            f"found {remote_commits}"
        )


def _assert_release_head(version: str, expected_commit: str) -> None:
    _assert_branch_head(f"release/{version}", expected_commit)


def wait_for_required_workflows(
    expected_commit: str,
    *,
    expected_branch: str,
    expected_event: str = "push",
    poll_seconds: int = 30,
    max_polls: int = 80,
) -> bool:
    """Require successful runs from exact workflow identities on one commit."""
    import time

    for _ in range(max_polls):
        incomplete: list[str] = []
        for workflow_path in REQUIRED_RELEASE_WORKFLOWS:
            raw = _capture(
                [
                    "gh",
                    "api",
                    "--method",
                    "GET",
                    f"repos/{GITHUB_REPOSITORY}/actions/workflows/{workflow_path}/runs",
                    "-f",
                    f"head_sha={expected_commit}",
                    "-f",
                    f"event={expected_event}",
                    "-f",
                    "per_page=100",
                ]
            )
            try:
                payload = json.loads(raw) if raw else {}
            except json.JSONDecodeError:
                payload = {}
            exact = [
                run
                for run in payload.get("workflow_runs", [])
                if run.get("path") == workflow_path
                and run.get("head_sha") == expected_commit
                and run.get("head_branch") == expected_branch
                and run.get("event") == expected_event
            ]
            if not exact:
                incomplete.append(f"{workflow_path}:missing")
                continue
            latest = max(exact, key=lambda run: int(run.get("id") or 0))
            if latest.get("status") != "completed":
                incomplete.append(f"{workflow_path}:{latest.get('status')}")
                continue
            if latest.get("conclusion") != "success":
                print(
                    f"[checks] {workflow_path} failed on {expected_commit}: "
                    f"{latest.get('conclusion')!r}"
                )
                return False
        if not incomplete:
            print(f"[checks] exact required workflows passed for {expected_commit}")
            return True
        print(f"[checks] waiting for: {', '.join(incomplete)}")
        time.sleep(poll_seconds)
    print(f"[checks] timed out waiting for exact workflows on {expected_commit}")
    return False


def stage_publish(version: str, *, auto_finish: bool = False) -> None:
    print(f"[publish] rolling out v{version}")
    branch = f"release/{version}"
    current_branch = _capture(["git", "branch", "--show-current"])
    if current_branch != branch:
        local_branches = _capture(["git", "branch", "--format=%(refname:short)"])
        checkout_arguments = ["git", "checkout"]
        if branch not in local_branches.splitlines():
            checkout_arguments.append("-b")
        run([*checkout_arguments, branch])
    run(["git", "add", "-A"])
    run(["git", "commit", "-m", f"release: cut v{version}"])
    # Simulate the push-event harness gate (every commit's high-risk paths need a
    # plan file in the SAME commit) BEFORE pushing — avoids a failed-CI round-trip.
    run([sys.executable, "scripts/check_harness.py", "--base", "origin/main"])
    run(["git", "push", "--no-verify", "-u", "origin", branch])
    published_head = _capture(["git", "rev-parse", "HEAD"])
    if not wait_for_required_workflows(
        published_head,
        expected_branch=branch,
    ):
        raise ReleaseError("Exact required GitHub workflows did not all succeed")
    _assert_release_head(version, published_head)
    if auto_finish:
        stage_finish(version)
    else:
        print("[publish] workflows green; re-run with --finish for reviewed PRs.")


def _promote_release_candidate(version: str) -> bool:
    originals = {path: path.read_text(encoding="utf-8") for path in README_TABLES}
    edits = {
        path: promote_release_candidate_text(text, version)
        for path, text in originals.items()
    }
    if edits == originals:
        print(f"[finish] v{version} metadata is already promoted")
        return False
    for path, text in edits.items():
        path.write_text(text, encoding="utf-8")
    run([sys.executable, "scripts/gen_changelog.py"])
    run(
        [
            "git",
            "add",
            *(str(path.relative_to(REPO_ROOT)) for path in README_TABLES),
            "CHANGELOG.md",
        ]
    )
    run(["git", "commit", "-m", f"release: promote v{version}"])
    run([sys.executable, "scripts/check_harness.py", "--base", "origin/main"])
    run(["git", "push", "--no-verify", "origin", f"release/{version}"])
    promoted_head = _capture(["git", "rev-parse", "HEAD"])
    if not wait_for_required_workflows(
        promoted_head,
        expected_branch=f"release/{version}",
    ):
        raise ReleaseError("Promoted release exact workflows did not all succeed")
    _assert_release_head(version, promoted_head)
    return True


def _pull_request_for_branch(branch: str) -> dict | None:
    raw = _capture(
        [
            "gh",
            "pr",
            "list",
            "--repo",
            GITHUB_REPOSITORY,
            "--head",
            branch,
            "--base",
            "main",
            "--state",
            "all",
            "--limit",
            "1",
            "--json",
            "number,state,mergedAt,url,headRefName,baseRefName",
        ]
    )
    try:
        pulls = json.loads(raw) if raw else []
    except json.JSONDecodeError:
        pulls = []
    return pulls[0] if pulls else None


def _open_reviewed_pull_request(version: str, branch: str, *, evidence: bool) -> None:
    pull = _pull_request_for_branch(branch)
    if pull and pull.get("state") == "MERGED":
        print(f"[finish] reviewed PR already merged: {pull['url']}")
        return
    _verify_live_release_controls()
    if not pull:
        kind = "release evidence" if evidence else "production release"
        title = f"release: {'authorize' if evidence else 'ship'} v{version}"
        body = (
            f"Reviewed {kind} PR for v{version}. "
            "Required CI, CodeQL, trusted sensitive-content status, and "
            "CODEOWNERS approval must all pass before squash merge."
        )
        run(
            [
                "gh",
                "pr",
                "create",
                "--repo",
                GITHUB_REPOSITORY,
                "--base",
                "main",
                "--head",
                branch,
                "--title",
                title,
                "--body",
                body,
            ]
        )
        pull = _pull_request_for_branch(branch)
    if not pull:
        raise ReleaseError(f"failed to resolve pull request for {branch}")
    run(
        [
            "gh",
            "pr",
            "edit",
            str(pull["number"]),
            "--repo",
            GITHUB_REPOSITORY,
            "--add-reviewer",
            RELEASE_REVIEWER,
        ]
    )
    run(
        [
            "gh",
            "pr",
            "merge",
            str(pull["number"]),
            "--repo",
            GITHUB_REPOSITORY,
            "--auto",
            "--squash",
        ]
    )
    print(
        f"[finish] queued reviewed squash merge for {pull['url']}; "
        f"approval by {RELEASE_REVIEWER} is mandatory"
    )


def wait_for_release_workflow(
    version: str, *, poll_seconds: int = 30, max_polls: int = 80
) -> str:
    import json
    import time

    tag = f"v{version}"
    commit = _capture(["git", "rev-list", "-n", "1", tag])
    for _ in range(max_polls):
        raw = _capture(
            [
                "gh",
                "api",
                "--method",
                "GET",
                f"repos/{GITHUB_REPOSITORY}/actions/workflows/release.yml/runs",
                "-f",
                f"head_sha={commit}",
                "-f",
                "event=push",
                "-f",
                "per_page=100",
            ]
        )
        try:
            payload = json.loads(raw) if raw else {}
        except json.JSONDecodeError:
            payload = {}
        runs = [
            run
            for run in payload.get("workflow_runs", [])
            if run.get("path") == ".github/workflows/release.yml"
            and run.get("head_sha") == commit
            and run.get("event") == "push"
            and run.get("head_branch") == tag
        ]
        latest = max(runs, key=lambda run: int(run.get("id") or 0)) if runs else None
        if latest and latest.get("status") == "completed":
            conclusion = latest.get("conclusion") or ""
            print(f"[release] {tag} concluded: {conclusion}")
            return conclusion
        print(f"[release] waiting for tested artifact publication of {tag}")
        time.sleep(poll_seconds)
    return ""


def _trusted_tag_workflow_runs(expected_commit: str) -> list[dict]:
    raw = _capture(
        [
            "gh",
            "api",
            "--method",
            "GET",
            (f"repos/{GITHUB_REPOSITORY}/actions/workflows/" "trusted-tag.yml/runs"),
            "-f",
            f"head_sha={expected_commit}",
            "-f",
            "event=workflow_dispatch",
            "-f",
            "per_page=100",
        ]
    )
    try:
        payload = json.loads(raw) if raw else {}
    except json.JSONDecodeError:
        payload = {}
    return [
        workflow_run
        for workflow_run in payload.get("workflow_runs", [])
        if workflow_run.get("path") == TRUSTED_TAG_WORKFLOW
        and workflow_run.get("head_sha") == expected_commit
        and workflow_run.get("head_branch") == "main"
        and workflow_run.get("event") == "workflow_dispatch"
    ]


def wait_for_trusted_tag_workflow(
    expected_commit: str,
    prior_run_ids: set[int],
    *,
    poll_seconds: int = 15,
    max_polls: int = 80,
) -> str:
    import time

    for _ in range(max_polls):
        new_runs = [
            workflow_run
            for workflow_run in _trusted_tag_workflow_runs(expected_commit)
            if int(workflow_run.get("id") or 0) not in prior_run_ids
        ]
        latest = (
            max(new_runs, key=lambda workflow_run: int(workflow_run.get("id") or 0))
            if new_runs
            else None
        )
        if latest and latest.get("status") == "completed":
            return str(latest.get("conclusion") or "")
        print("[release] waiting for protected-main tag authorization")
        time.sleep(poll_seconds)
    return ""


def ensure_trusted_tag(tag: str, expected_commit: str) -> None:
    run(
        [
            "git",
            "fetch",
            "--force",
            "origin",
            f"refs/tags/{tag}:refs/tags/{tag}",
        ],
        check=False,
    )
    existing_tag_commit = _capture(["git", "rev-list", "-n", "1", tag])
    if existing_tag_commit:
        if existing_tag_commit != expected_commit:
            raise ReleaseError(
                f"existing {tag} points to {existing_tag_commit}, not {expected_commit}"
            )
        if _capture(["git", "cat-file", "-t", f"refs/tags/{tag}"]) != "tag":
            raise ReleaseError(f"existing {tag} is not an annotated tag")
        return

    prior_run_ids = {
        int(workflow_run.get("id") or 0)
        for workflow_run in _trusted_tag_workflow_runs(expected_commit)
    }
    run(
        [
            "gh",
            "workflow",
            "run",
            TRUSTED_TAG_WORKFLOW,
            "--repo",
            GITHUB_REPOSITORY,
            "--ref",
            "main",
            "-f",
            f"tag_name={tag}",
            "-f",
            f"expected_sha={expected_commit}",
        ]
    )
    conclusion = wait_for_trusted_tag_workflow(expected_commit, prior_run_ids)
    if conclusion != "success":
        raise ReleaseError(
            "protected-main tag workflow did not authorize the release "
            f"(conclusion={conclusion!r})"
        )
    run(
        [
            "git",
            "fetch",
            "--force",
            "origin",
            f"refs/tags/{tag}:refs/tags/{tag}",
        ]
    )
    if _capture(["git", "rev-list", "-n", "1", tag]) != expected_commit:
        raise ReleaseError(f"trusted workflow created {tag} at the wrong commit")
    if _capture(["git", "cat-file", "-t", f"refs/tags/{tag}"]) != "tag":
        raise ReleaseError(f"trusted workflow did not create annotated tag {tag}")


def stage_finish(version: str) -> None:
    print(f"[finish] reviewed two-PR release flow for v{version}")
    production_branch = f"release/{version}"
    evidence_branch = f"release/{version}-evidence"
    current_branch = _capture(["git", "branch", "--show-current"])

    if current_branch == production_branch:
        if _promote_release_candidate(version):
            print(
                "[finish] metadata promotion is green. Re-run --finish to open "
                "the reviewed production PR."
            )
            return
        head_commit = _capture(["git", "rev-parse", "HEAD"])
        run([sys.executable, "scripts/check_harness.py", "--base", "origin/main"])
        run(["git", "push", "--no-verify", "origin", production_branch])
        if not wait_for_required_workflows(
            head_commit,
            expected_branch=production_branch,
        ):
            raise ReleaseError("Production release exact workflows did not all succeed")
        _assert_branch_head(production_branch, head_commit)
        _open_reviewed_pull_request(
            version,
            production_branch,
            evidence=False,
        )
        return

    if current_branch == evidence_branch:
        head_commit = _capture(["git", "rev-parse", "HEAD"])
        run([sys.executable, "scripts/check_harness.py", "--base", "origin/main"])
        run(
            [
                sys.executable,
                "scripts/check_release_authorization.py",
                "--version",
                version,
            ]
        )
        run(["git", "push", "--no-verify", "-u", "origin", evidence_branch])
        if not wait_for_required_workflows(
            head_commit,
            expected_branch=evidence_branch,
        ):
            raise ReleaseError("Evidence release exact workflows did not all succeed")
        _assert_branch_head(evidence_branch, head_commit)
        _open_reviewed_pull_request(
            version,
            evidence_branch,
            evidence=True,
        )
        return

    if current_branch != "main":
        raise ReleaseError(
            f"finish requires {production_branch}, {evidence_branch}, or main; "
            f"found {current_branch!r}"
        )
    run(["git", "fetch", "origin", "main"])
    head_commit = _capture(["git", "rev-parse", "HEAD"])
    remote_main = _capture(["git", "rev-parse", "origin/main"])
    if head_commit != remote_main:
        raise ReleaseError("local main must exactly match origin/main before tagging")
    run(
        [
            sys.executable,
            "scripts/check_release_authorization.py",
            "--version",
            version,
        ]
    )
    if not wait_for_required_workflows(head_commit, expected_branch="main"):
        raise ReleaseError("Final main exact workflows did not all succeed")
    tag = f"v{version}"
    ensure_trusted_tag(tag, head_commit)
    conclusion = wait_for_release_workflow(version)
    if conclusion != "success":
        raise ReleaseError(
            "tagged release workflow did not publish identical PyPI and GitHub "
            f"artifacts (conclusion={conclusion!r})"
        )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Release forward-netbox.")
    parser.add_argument("version", help="target version, e.g. 1.5.11")
    parser.add_argument(
        "--summary",
        help="one-line release summary for the compatibility tables",
        default="",
    )
    parser.add_argument("--write", action="store_true", help="write prepare edits")
    parser.add_argument(
        "--publish",
        action="store_true",
        help="branch + push (rollout), then wait for GitHub CI. Off by default.",
    )
    parser.add_argument(
        "--auto-finish",
        action="store_true",
        help="with --publish: after CI is green, promote + tag + publish release",
    )
    parser.add_argument(
        "--finish",
        action="store_true",
        help="promote, open reviewed PRs, or tag reviewed main (rollout)",
    )
    args = parser.parse_args(argv)

    if not SEMVER_RE.match(args.version):
        parser.error(f"version must be X.Y.Z, got {args.version!r}")

    try:
        if args.finish:
            stage_finish(args.version)
            return 0
        stage_prepare(args.version, args.summary, write=args.write)
        if args.write:
            stage_verify()
        if args.publish:
            stage_publish(args.version, auto_finish=args.auto_finish)
    except ReleaseError as exc:
        print(f"release error: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
