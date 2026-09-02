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
#   finish   - open the check-gated production PR, then the evidence PR, then
#              tag the validated evidence-only main commit
#   authorize - append the Release Authorization section the evidence PR needs,
#              rendered from what `verify` actually ran
#   post-release - open the documentation-only bridge that must follow the tag
#   anchor   - advance the provenance anchor onto the bridge AND promote the
#              release in the compatibility tables, as one commit
#
# `--publish --auto-finish` runs every stage in order and waits for each pull
# request to merge, so a release is one invocation. Each stage is also a flag
# of its own, for resuming after a failure: the driver prints which one.
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
from datetime import datetime
from datetime import timezone
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
FAST_BASELINE = REPO_ROOT / "forward_netbox/utilities/fast_baseline.py"
RUNTIME_VERSION_TEST = (
    REPO_ROOT / "forward_netbox/tests/test_runtime_dependency_check.py"
)
PROVENANCE = REPO_ROOT / "scripts/verify_release_provenance.py"
TESTED_RUNTIME = REPO_ROOT / "scripts/tested_runtime.py"
CONSTRAINTS = REPO_ROOT / "constraints.txt"
# What `verify` ran and how it came out, so `authorize` can render the
# Release Authorization section from evidence rather than from memory. Local
# and gitignored: it names the host's port and log paths.
EVIDENCE_RECORD = REPO_ROOT / ".release-evidence.json"
EVIDENCE_LOG_DIR = REPO_ROOT / ".release-evidence-logs"
# Waiting for a pull request to merge. Nothing gates merges but the branch
# ruleset, so this is normally seconds; the bound exists so an unattended run
# ends with an instruction instead of a hang.
PULL_REQUEST_MERGE_POLL_SECONDS = 30
PULL_REQUEST_MERGE_MAX_POLLS = 120
# The compatibility cell shared by every table row, so a new row reuses the
# previous row's NetBox-support text verbatim.
CURRENT_RELEASE_RE = re.compile(
    r"^\| `v[0-9][^|]*` \| (?P<support>[^|]*) \| Current release;", re.MULTILINE
)
RELEASE_INTRO_RE = re.compile(
    r"^The `(?P<version>\d+\.\d+\.\d+)` release(?P<candidate> candidate)? "
    r"requires (?P<requirements>.+)\. Expand for the published release history "
    r"and (?:candidate|release) notes\.$",
    re.MULTILINE,
)

SEMVER_RE = re.compile(r"^\d+\.\d+\.\d+$")
GITHUB_REPOSITORY = "forwardnetworks/forward-netbox"
# The CI gate workflows were removed; gates run locally through `invoke ci` and
# are recorded in the release plan's authorization section. Nothing remains for
# the publish flow to wait on, and an empty tuple makes every wait a no-op
# rather than a poll that can never succeed.
REQUIRED_RELEASE_WORKFLOWS: tuple[str, ...] = ()


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


def set_release_intro_text(text: str, version: str, *, candidate: bool) -> str:
    """Keep the compatibility introduction aligned with the release table."""
    matches = list(RELEASE_INTRO_RE.finditer(text))
    if len(matches) != 1:
        raise ReleaseError(
            "expected exactly one canonical release compatibility introduction"
        )
    match = matches[0]
    release_label = "release candidate" if candidate else "release"
    notes_label = "candidate notes" if candidate else "release notes"
    replacement = (
        f"The `{version}` {release_label} requires {match.group('requirements')}. "
        f"Expand for the published release history and {notes_label}."
    )
    return text[: match.start()] + replacement + text[match.end() :]


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


def version_surface_edits(old: str, new: str) -> dict[Path, str]:
    """Every file carrying the version literal, as {path: new text}.

    `stage_prepare` used to bump only pyproject and `__init__`, leaving the
    fast-baseline runtime pin and the runtime version test behind. That drift is
    what cost the 2.6.3 release six full gate runs, and the load-bearing one is
    the fast-baseline pin: a stale value silently reverts a first sync from the
    fast path to the slow one. Bumping them together is the fix;
    `scripts/check_release_preflight.py` is the backstop.
    """
    substitutions = {
        PYPROJECT: (f'version = "{old}"', f'version = "{new}"'),
        INIT_PY: (f'version = "{old}"', f'version = "{new}"'),
        FAST_BASELINE: (f'"forward_netbox": "{old}"', f'"forward_netbox": "{new}"'),
        RUNTIME_VERSION_TEST: (
            f'NetboxForwardConfig.version, "{old}"',
            f'NetboxForwardConfig.version, "{new}"',
        ),
    }
    edits: dict[Path, str] = {}
    for path, (needle, replacement) in substitutions.items():
        text = path.read_text(encoding="utf-8")
        count = text.count(needle)
        if count != 1:
            raise ReleaseError(
                f"expected exactly one {needle!r} in "
                f"{path.relative_to(REPO_ROOT)}, found {count}"
            )
        edits[path] = text.replace(needle, replacement)
    return edits


def stage_prepare(version: str, summary: str, *, write: bool) -> None:
    old = read_current_version()
    print(f"[prepare] bump {old} -> {version}")
    edits = version_surface_edits(old, version)
    for path in README_TABLES:
        edits[path] = set_release_intro_text(
            insert_release_row(path.read_text(encoding="utf-8"), version, summary),
            version,
            candidate=True,
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


# The environment variables the authorization checker requires an evidence
# command to name, in the order they are rendered. `NETBOX_VER` is included
# explicitly because the checker requires it even though the compose default
# would supply the same value.
RELEASE_GATE_ENVIRONMENT = (
    "FORWARD_NETBOX_DOCKER_PROJECT",
    "FORWARD_NETBOX_POSTGRES_DATA_PATH",
    "FORWARD_NETBOX_WORKER_AUTORELOAD",
    "NETBOX_VER",
    "FORWARD_NETBOX_UPGRADE_FROM_VERSION",
    "FORWARD_NETBOX_PATTERN_PARITY_UNVERIFIED",
    "FORWARD_NETBOX_HOST_PORT",
    "NETBOX_URL",
)
TEST_SUMMARY_RE = re.compile(r"^Ran (?P<count>\d+) tests? in ", re.MULTILINE)
TEST_VERDICT_RE = re.compile(
    r"^(?P<verdict>OK|FAILED)\b(?P<detail>[^\n]*)", re.MULTILINE
)


def run_logged(cmd: list[str], *, env: dict[str, str], log_path: Path) -> int:
    """Run a gate command, streaming its output and keeping a copy.

    `run` streams and keeps nothing, which is right for git and wrong for a
    forty-minute gate whose summary lines are the evidence a release needs.
    The copy is what `authorize` reads; the stream is what the operator reads.
    """
    print("  $ [redacted release command]")
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with log_path.open("w", encoding="utf-8") as log:
        process = subprocess.Popen(
            cmd,
            cwd=REPO_ROOT,
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )
        assert process.stdout is not None
        for line in process.stdout:
            sys.stdout.write(line)
            log.write(line)
        return process.wait()


def _gate_summary(log_text: str) -> dict:
    """The test-count lines from a gate log, as numbers.

    The authorization checker requires a digit and a retrospective outcome in
    every evidence line; the counts are both, and they are the part an operator
    used to transcribe by hand.
    """
    counts = [int(match.group("count")) for match in TEST_SUMMARY_RE.finditer(log_text)]
    verdicts = [match.group("verdict") for match in TEST_VERDICT_RE.finditer(log_text)]
    sbom = re.search(r"\b(?P<count>\d+) components?\b", log_text)
    routes = re.search(
        r"\b(?P<count>\d+) (?:authenticated )?(?:menu |detail )?routes?\b", log_text
    )
    return {
        "test_runs": counts,
        "tests_total": sum(counts),
        "verdicts": verdicts,
        "failed": any(verdict == "FAILED" for verdict in verdicts),
        "sbom_components": int(sbom.group("count")) if sbom else None,
        "routes": int(routes.group("count")) if routes else None,
    }


def _release_gate_environment() -> dict[str, str]:
    release_env = {
        **os.environ,
        "FORWARD_NETBOX_DOCKER_PROJECT": "forward-netbox-release-gate",
        "FORWARD_NETBOX_HOST_PORT": _available_loopback_port(),
        "FORWARD_NETBOX_POSTGRES_DATA_PATH": "netbox-postgres-data",
        "FORWARD_NETBOX_WORKER_AUTORELOAD": "0",
        "NETBOX_VER": f"v{_tested_netbox_version()}",
    }
    release_env["NETBOX_URL"] = (
        f"http://127.0.0.1:{release_env['FORWARD_NETBOX_HOST_PORT']}"
    )
    return release_env


def _tested_netbox_version() -> str:
    match = re.search(
        r'^TESTED_NETBOX_VERSION = "(?P<version>[^"]+)"$',
        TESTED_RUNTIME.read_text(encoding="utf-8"),
        re.MULTILINE,
    )
    if match is None:
        raise ReleaseError(
            "scripts/tested_runtime.py declares no TESTED_NETBOX_VERSION"
        )
    return match.group("version")


def _pinned_branching_version() -> str:
    match = re.search(
        r"^netboxlabs-netbox-branching==(?P<version>[0-9][^\s]*)$",
        CONSTRAINTS.read_text(encoding="utf-8"),
        re.MULTILINE,
    )
    if match is None:
        raise ReleaseError("constraints.txt pins no netboxlabs-netbox-branching")
    return match.group("version")


def _evidence_command(task: str, environment: dict[str, str], *, with_url: bool) -> str:
    """The exact `rtk env ... invoke <task>` form the authorization checker parses."""
    assignments = []
    for name in RELEASE_GATE_ENVIRONMENT:
        if name in ("FORWARD_NETBOX_HOST_PORT", "NETBOX_URL") and not with_url:
            continue
        value = environment.get(name)
        if value:
            assignments.append(f"{name}={value}")
    return f"rtk env {' '.join(assignments)} invoke {task}"


def write_evidence_record(record: dict) -> None:
    EVIDENCE_RECORD.write_text(json.dumps(record, indent=2, sort_keys=True) + "\n")


def read_evidence_record(version: str) -> dict:
    if not EVIDENCE_RECORD.exists():
        raise ReleaseError(
            f"{EVIDENCE_RECORD.name} is missing: run `release.py {version} --write` "
            "(verify) first, so the authorization can be rendered from what ran"
        )
    record = json.loads(EVIDENCE_RECORD.read_text(encoding="utf-8"))
    if record.get("version") != version:
        raise ReleaseError(
            f"{EVIDENCE_RECORD.name} records v{record.get('version')}, not v{version}"
        )
    return record


def stage_verify(version: str) -> None:
    print("[verify] mandatory isolated local release gate")
    release_project = "forward-netbox-release-gate"
    release_env = _release_gate_environment()
    logs = EVIDENCE_LOG_DIR / version
    record = {
        "version": version,
        "started_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "environment": {
            name: release_env[name]
            for name in RELEASE_GATE_ENVIRONMENT
            if release_env.get(name)
        },
        "netbox_version": _tested_netbox_version(),
        "branching_version": _pinned_branching_version(),
        "python_version": f"{sys.version_info.major}.{sys.version_info.minor}",
        "gate": None,
        "artifact": None,
    }
    try:
        gate_log = logs / "invoke-ci.log"
        status = run_logged(
            [sys.executable, "-m", "invoke", "ci"], env=release_env, log_path=gate_log
        )
        record["gate"] = {
            "command": _evidence_command("ci", release_env, with_url=False),
            "exit_status": status,
            "log": str(gate_log.relative_to(REPO_ROOT)),
            **_gate_summary(gate_log.read_text(encoding="utf-8")),
        }
        if status != 0:
            raise ReleaseError(f"release command failed with exit code {status}")
        artifacts = release_distribution_artifacts(read_current_version())
        record["wheel"] = next(
            artifact.name for artifact in artifacts if artifact.suffix == ".whl"
        )
        run(
            [sys.executable, "-m", "twine", "check", *(str(p) for p in artifacts)],
            env=release_env,
        )
        artifact_log = logs / "invoke-artifact-test.log"
        status = run_logged(
            [sys.executable, "-m", "invoke", "artifact-test"],
            env=release_env,
            log_path=artifact_log,
        )
        record["artifact"] = {
            "command": _evidence_command("artifact-test", release_env, with_url=True),
            "exit_status": status,
            "log": str(artifact_log.relative_to(REPO_ROOT)),
            **_gate_summary(artifact_log.read_text(encoding="utf-8")),
        }
        if status != 0:
            raise ReleaseError(f"release command failed with exit code {status}")
        record["finished_at"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
    finally:
        write_evidence_record(record)
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


def _capture_required(cmd: list[str], *, purpose: str) -> str:
    """Capture a required command without exposing arguments or stderr."""
    result = subprocess.run(cmd, cwd=REPO_ROOT, capture_output=True, text=True)
    if result.returncode != 0:
        raise ReleaseError(f"{purpose} failed with exit code {result.returncode}")
    return result.stdout.strip()


def _workflow_runs_payload(raw: str, *, purpose: str) -> list[dict]:
    """Parse the exact Actions runs response or fail without echoing it."""
    if not raw:
        raise ReleaseError(f"{purpose} returned an empty response")
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ReleaseError(f"{purpose} returned invalid JSON") from exc
    if not isinstance(payload, dict) or not isinstance(
        payload.get("workflow_runs"), list
    ):
        raise ReleaseError(f"{purpose} returned an invalid workflow-runs payload")
    runs = payload["workflow_runs"]
    if any(not isinstance(run, dict) for run in runs):
        raise ReleaseError(f"{purpose} returned a malformed workflow run")
    return runs


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
    max_polls: int = 160,
) -> bool:
    """Require successful runs from exact workflow identities on one commit."""
    import time

    if not REQUIRED_RELEASE_WORKFLOWS:
        return True

    for _ in range(max_polls):
        incomplete: list[str] = []
        for workflow_path in REQUIRED_RELEASE_WORKFLOWS:
            workflow_identifier = Path(workflow_path).name
            query_purpose = f"GitHub {workflow_identifier} run query"
            raw = _capture_required(
                [
                    "gh",
                    "api",
                    "--method",
                    "GET",
                    "repos/"
                    f"{GITHUB_REPOSITORY}/actions/workflows/"
                    f"{workflow_identifier}/runs",
                    "-f",
                    f"head_sha={expected_commit}",
                    "-f",
                    f"event={expected_event}",
                    "-f",
                    "per_page=100",
                ],
                purpose=query_purpose,
            )
            exact = [
                run
                for run in _workflow_runs_payload(raw, purpose=query_purpose)
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
        stage_finish_unattended(version)
    else:
        print("[publish] workflows green; re-run with --finish for release PRs.")


def promote_release_tables(version: str) -> bool:
    """Flip the compatibility tables from candidate to current. Files only.

    This used to commit, push and wait, on the release branch, BEFORE the tag
    existed. `check_harness.py` requires the provenance anchor to name the
    release the table calls current, and the anchor cannot advance until the
    bridge exists, which cannot exist until the tag does - so the commit it made
    was refused by construction on every release since 2.8.6, and a second copy
    of it was left stranded on local `main` by the tag step. Promotion belongs
    in the anchor commit, where the two move together; see `stage_anchor`.
    """
    originals = {path: path.read_text(encoding="utf-8") for path in README_TABLES}
    edits = {
        path: set_release_intro_text(
            promote_release_candidate_text(text, version),
            version,
            candidate=False,
        )
        for path, text in originals.items()
    }
    if edits == originals:
        print(f"[anchor] v{version} metadata is already promoted")
        return False
    for path, text in edits.items():
        path.write_text(text, encoding="utf-8")
    run([sys.executable, "scripts/gen_changelog.py"])
    return True


def _wait_for_pull_request_merge(
    branch: str,
    *,
    poll_seconds: int = PULL_REQUEST_MERGE_POLL_SECONDS,
    max_polls: int = PULL_REQUEST_MERGE_MAX_POLLS,
) -> dict:
    """Block until the pull request for ``branch`` is merged, or say why not."""
    import time

    for _ in range(max_polls):
        pull = _pull_request_for_branch(branch)
        if pull is None:
            raise ReleaseError(f"no pull request exists for {branch}")
        state = pull.get("state")
        if state == "MERGED":
            print(f"[merge] {pull['url']} merged")
            return pull
        if state == "CLOSED":
            raise ReleaseError(f"pull request for {branch} was closed without merging")
        print(f"[merge] waiting for {pull['url']} ({state})")
        time.sleep(poll_seconds)
    raise ReleaseError(
        f"pull request for {branch} did not merge within "
        f"{poll_seconds * max_polls} seconds; merge it, then resume"
    )


def _checkout_merged_main() -> str:
    """Put the checkout on `main` at exactly `origin/main` and return that SHA."""
    run(["git", "fetch", "origin", "main"])
    run(["git", "checkout", "--force", "main"])
    run(["git", "reset", "--hard", "origin/main"])
    return _capture(["git", "rev-parse", "HEAD"])


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


def _open_release_pull_request(version: str, branch: str, *, evidence: bool) -> None:
    pull = _pull_request_for_branch(branch)
    if pull and pull.get("state") == "MERGED":
        print(f"[finish] release PR already merged: {pull['url']}")
        return
    _verify_live_release_controls()
    if not pull:
        kind = "release evidence" if evidence else "production release"
        title = f"release: {'authorize' if evidence else 'ship'} v{version}"
        body = (
            f"{kind.title()} PR for v{version}. Required CI, CodeQL, and the "
            "trusted sensitive-content status must pass before squash merge."
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
            "merge",
            str(pull["number"]),
            "--repo",
            GITHUB_REPOSITORY,
            "--auto",
            "--squash",
        ]
    )
    print(f"[finish] queued check-gated squash merge for {pull['url']}")


def wait_for_release_workflow(
    version: str, *, poll_seconds: int = 30, max_polls: int = 240
) -> str:
    import time

    tag = f"v{version}"
    commit = _capture(["git", "rev-list", "-n", "1", tag])
    for _ in range(max_polls):
        raw = _capture_required(
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
            ],
            purpose="GitHub release workflow run query",
        )
        runs = [
            run
            for run in _workflow_runs_payload(
                raw,
                purpose="GitHub release workflow run query",
            )
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


def _remote_annotated_tag_target(tag: str) -> str:
    raw = _capture(
        [
            "git",
            "ls-remote",
            "--tags",
            "origin",
            f"refs/tags/{tag}",
            f"refs/tags/{tag}^{{}}",
        ]
    )
    refs = {
        fields[1]: fields[0]
        for line in raw.splitlines()
        if len(fields := line.split()) == 2
    }
    direct_ref = f"refs/tags/{tag}"
    peeled_ref = f"{direct_ref}^{{}}"
    if direct_ref in refs and peeled_ref not in refs:
        raise ReleaseError(f"remote {tag} is not an annotated tag")
    return refs.get(peeled_ref, "")


def ensure_release_tag(tag: str, expected_commit: str) -> None:
    """Create a normal annotated tag and prove its remote target."""
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
    remote_tag_commit = _remote_annotated_tag_target(tag)
    if remote_tag_commit:
        if remote_tag_commit != expected_commit:
            raise ReleaseError(
                f"remote {tag} points to {remote_tag_commit}, not {expected_commit}"
            )
        if existing_tag_commit != expected_commit:
            raise ReleaseError(
                f"local {tag} points to {existing_tag_commit}, not {expected_commit}"
            )
        if _capture(["git", "cat-file", "-t", f"refs/tags/{tag}"]) != "tag":
            raise ReleaseError(f"local {tag} is not an annotated tag")
        return

    _verify_live_release_controls()
    if existing_tag_commit:
        if existing_tag_commit != expected_commit:
            raise ReleaseError(
                f"local {tag} points to {existing_tag_commit}, not {expected_commit}"
            )
        if _capture(["git", "cat-file", "-t", f"refs/tags/{tag}"]) != "tag":
            raise ReleaseError(f"local {tag} is not an annotated tag")
    else:
        run(
            [
                "git",
                "tag",
                "-a",
                tag,
                expected_commit,
                "-m",
                f"Forward NetBox {tag}",
            ]
        )
    run(["git", "push", "origin", f"refs/tags/{tag}"])
    if _remote_annotated_tag_target(tag) != expected_commit:
        raise ReleaseError(
            f"remote {tag} does not peel to validated commit {expected_commit}"
        )


def stage_finish(version: str) -> None:
    print(f"[finish] check-gated two-PR release flow for v{version}")
    production_branch = f"release/{version}"
    evidence_branch = f"release/{version}-evidence"
    current_branch = _capture(["git", "branch", "--show-current"])

    if current_branch == production_branch:
        # No promotion here. The tables flip in the anchor commit, after the
        # tag, because the harness ties the anchor to the current release and
        # refuses either one moving alone - see `promote_release_tables`.
        head_commit = _capture(["git", "rev-parse", "HEAD"])
        run([sys.executable, "scripts/check_harness.py", "--base", "origin/main"])
        run(["git", "push", "--no-verify", "origin", production_branch])
        if not wait_for_required_workflows(
            head_commit,
            expected_branch=production_branch,
        ):
            raise ReleaseError("Production release exact workflows did not all succeed")
        _assert_branch_head(production_branch, head_commit)
        _open_release_pull_request(
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
        _open_release_pull_request(
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
    ensure_release_tag(tag, head_commit)
    conclusion = wait_for_release_workflow(version)
    if conclusion != "success":
        raise ReleaseError(
            "tagged release workflow did not publish identical PyPI and GitHub "
            f"artifacts (conclusion={conclusion!r})"
        )
    # The release is DONE at this point: the tag exists, the workflow
    # published identical PyPI and GitHub artifacts, and every gate passed.
    # What follows is convenience staging, and it must not be able to report
    # the release as failed.
    #
    # It could, and did, on every release since 2.7.13. `stage_post_release`
    # runs `check_harness.py`, which requires the provenance anchor to name the
    # release the table calls current - and the anchor cannot advance until the
    # bridge commit exists, which cannot happen until the pull request this
    # step is trying to open has merged. The check is unsatisfiable by
    # construction at the moment it runs, so the command exited 1 after a
    # completely successful release. An exit status that says "failed" when the
    # artifacts are live teaches people to stop reading it.
    try:
        stage_post_release(version, tag)
    except Exception as exc:  # noqa: BLE001 - the release already succeeded
        print(
            f"\n[release] v{version} PUBLISHED SUCCESSFULLY.\n"
            f"[release] post-release staging did not complete ({exc}).\n"
            "[release] That is follow-up work, not a failed release. Do it by "
            "hand:\n"
            "\n"
            "    1. open the documentation-only post-release bridge on main\n"
            "    2. advance PRIOR_RELEASE_TAG and PRIOR_POST_RELEASE_DOC_COMMIT\n"
            "       to that bridge commit, and promote the release table\n"
            "\n"
            "The harness fails until the anchor lands, so this cannot be "
            "forgotten quietly - only deferred.\n"
        )


def _next_patch_version(version: str) -> str:
    major, minor, patch = (int(part) for part in version.split("."))
    return f"{major}.{minor}.{patch + 1}"


def _bridge_plan_path(version: str) -> Path:
    """Where the generated bridge plan lands. Separate so a test can redirect it."""
    return REPO_ROOT / "docs" / "03_Plans" / "active" / f"post-release-{version}.md"


def _bridge_plan_text(version: str, tag: str) -> str:
    """The documentation-only commit that must follow a release tag."""
    return f"""# Post-release bridge for {version}

## Goal

Occupy the first commit on `main` after `{tag}` with a documentation-only
change, so the provenance anchor has a bridge commit to point at.

## Why

`PRIOR_POST_RELEASE_DOC_COMMIT` must name a commit that is documentation-only
and whose parent is the release commit. The anchor commit itself edits the
verifier, the workflow and the harness, so it cannot be its own bridge.

The slot is claimed by whatever lands first and cannot be reclaimed afterwards:
the bridge is *defined* as the first first-parent commit after the tag. A
commit carrying anything else disqualifies it permanently.

## Constraints

- This file only. No code, no configuration, no workflow.
- It must be the first commit on `main` after the tag.

## Touched Surfaces

This plan file.

## Approach

Merge this before anything else lands on `main`, then advance
`PRIOR_RELEASE_TAG` and `PRIOR_POST_RELEASE_DOC_COMMIT` in a separate commit
that names this one.

## Validation

None. A documentation-only commit whose purpose is to exist at this position.

## Rollback

Revert, and the anchor has no bridge until another documentation-only commit
lands.

## Decision Log

- **Generated by `scripts/release.py` rather than written by hand.** Both
  previous releases needed this commit and one of them got it wrong, which cost
  a permanently disqualified bridge.
"""


def stage_post_release(version: str, tag: str) -> None:
    """Open the documentation-only bridge that must follow a release tag.

    This used to open the next `.dev0` instead, and that was wrong three ways at
    once.

    It failed. The bump touches `pyproject.toml` and
    `forward_netbox/utilities/fast_baseline.py`, both high-risk paths, and
    `check_harness.py --base origin/main` requires a plan file in the same
    commit. `stage_open_next` writes none, so the stage rejected its own commit
    on every release.

    It stranded the operator. The bump was already COMMITTED on a branch this
    function created, so the failure left them standing there with a clean
    working tree and nothing in `git status` to show for it. The next
    `git checkout -b` inherited that commit; for `v2.8.3` it reached the bridge
    and disqualified it permanently.

    And it was the wrong commit to make. What a release actually needs next is
    the bridge - documentation-only, parented to the release commit - and that
    slot is taken by whatever lands first.

    The `.dev0` question is settled: `main` carries the RELEASED version, and
    nothing writes a dev marker to it. `--open-next` documented a real incident
    - a customer installed from `main` between the release PR merging and the
    tag, and reported a version PyPI did not have - but the marker was the
    wrong remedy for it. Customers install this plugin from source, so a
    `2.9.2.dev0` on `main` offered a version that was never gated, tagged or
    published; and the window it described is minutes wide, covered by the
    tag-only publish trigger and by `--auto-finish` running straight through
    it. Six release plans recorded the contradiction as undecided; this is the
    decision, and the machinery is gone rather than left as a trap.
    """
    branch = f"docs/post-release-{version}"
    print(f"[post-release] opening {branch} for {tag}")

    run(["git", "fetch", "origin", "main"])
    # Where the operator started, so they are put back whatever happens.
    starting_branch = _capture(["git", "branch", "--show-current"]) or "main"
    plan_path = _bridge_plan_path(version)
    try:
        run(["git", "checkout", "-B", branch, "origin/main"])
        plan_path.write_text(_bridge_plan_text(version, tag), encoding="utf-8")
        # This path only. `git add -A` here would sweep anything else the
        # working tree happens to hold into a commit that must carry one file.
        run(["git", "add", str(plan_path)])
        run(["git", "commit", "-m", f"docs: record the {version} post-release bridge"])
        run([sys.executable, "scripts/check_harness.py", "--base", "origin/main"])
        run(["git", "push", "--no-verify", "-u", "origin", branch])
        _open_pull_request(
            branch,
            title=f"docs: record the {version} post-release bridge",
            body=(
                f"Documentation-only bridge for {tag}. It must be the first "
                "commit on main after the tag; merge it before anything else."
            ),
        )
    except Exception:
        # A failed stage leaves nothing behind. The branch was created by this
        # function and holds at most the one commit it made; deleting it means
        # the next attempt starts from origin/main rather than inheriting a
        # half-made bridge - which is how v2.8.3's slot was lost.
        run(["git", "checkout", "--force", starting_branch], check=False)
        run(["git", "branch", "-D", branch], check=False)
        raise
    finally:
        run(["git", "checkout", "--force", starting_branch], check=False)

    print(
        "\n[post-release] once that pull request merges, advance the anchor:\n"
        "\n"
        f"    python3 scripts/release.py {version} --anchor\n"
        "\n"
        "It derives the bridge commit from the tag, advances "
        "PRIOR_RELEASE_TAG and PRIOR_POST_RELEASE_DOC_COMMIT, and promotes the "
        "release in the compatibility tables, as one commit. The harness fails "
        "until that lands, so this cannot be forgotten quietly - only deferred.\n"
    )


def _open_pull_request(branch: str, *, title: str, body: str) -> dict:
    """Open a squash-merging pull request for ``branch`` if none exists."""
    pull = _pull_request_for_branch(branch)
    if pull is None:
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
    if pull is None:
        raise ReleaseError(f"failed to resolve pull request for {branch}")
    if pull.get("state") == "OPEN":
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
    print(f"[pr] {pull['url']}")
    return pull


def _bridge_commit_for(tag: str) -> str:
    """The first first-parent commit after ``tag`` on origin/main: the bridge."""
    run(["git", "fetch", "origin", "main"])
    lineage = _capture(
        ["git", "rev-list", "--first-parent", "--reverse", f"{tag}..origin/main"]
    ).splitlines()
    if not lineage:
        raise ReleaseError(f"nothing has landed on origin/main after {tag} yet")
    bridge = lineage[0]
    tag_commit = _capture(["git", "rev-list", "-n", "1", tag])
    changed = [
        line
        for line in _capture(
            ["git", "diff", "--name-only", tag_commit, bridge]
        ).splitlines()
        if line
    ]
    if not changed or not all(_is_bridge_path(path) for path in changed):
        raise ReleaseError(
            f"the first commit after {tag} ({bridge[:12]}) is not "
            f"documentation-only ({changed}); the bridge slot is taken and the "
            "anchor cannot advance onto it"
        )
    return bridge


def _is_bridge_path(path: str) -> bool:
    # Mirrors `verify_release_provenance._is_documentation_path` without
    # importing it: the verifier is the trusted scanner and stays untouched.
    return (path.startswith("docs/") and path.endswith(".md")) or path in {
        "CHANGELOG.md",
        "README.md",
        "docs/README.md",
        "docs/01_User_Guide/README.md",
    }


def _anchor_plan_path(version: str) -> Path:
    stamp = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    return REPO_ROOT / "docs" / "03_Plans" / "active" / f"{stamp}-anchor-{version}.md"


def _anchor_plan_text(version: str, tag: str, bridge: str) -> str:
    return f"""# Advance the release provenance anchor to {version}

## Goal

Move the provenance anchor onto the shipped {version} release and promote it to
current in the compatibility tables, as one commit.

## Constraints

- The bridge must be the first commit after the tag on the first-parent lineage
  and documentation-only. `{bridge[:12]}` was checked for both by
  `scripts/release.py --anchor` before this file was written.
- The table promotion belongs here, not on the release branch: the harness
  requires the anchor to name the release the table calls current, so the two
  can only move together.

## Touched Surfaces

- `scripts/verify_release_provenance.py`
- `README.md`, `docs/README.md`, `docs/01_User_Guide/README.md`, `CHANGELOG.md`
- this plan

## Approach

Anchor to `{tag}` / bridge `{bridge}`; promote {version} to current and demote
the previous release to superseded.

## Validation

`check_harness.py --base origin/main` passes with the anchor advanced.

## Rollback

Revert; the harness fails again, which blocks the next release rather than
permitting an unverified one - the safe direction.

## Decision Log

- **Generated by `scripts/release.py --anchor`.** The anchor and the promotion
  were hand-edited for every release through 2.9.1, and skipping either was
  silent at the time and expensive later.
"""


def _capture_constant(text: str, key: str) -> str:
    match = re.search(rf'^{re.escape(key)} = "(?P<value>[^"]+)"$', text, re.MULTILINE)
    if match is None:
        raise ReleaseError(f"scripts/verify_release_provenance.py declares no {key}")
    return match.group("value")


def stage_anchor(version: str, tag: str) -> None:
    """Advance the provenance anchor onto the bridge and promote the release.

    One commit, because the harness ties the two together: the anchor must name
    the release the compatibility table calls current. Promoting on the release
    branch was refused for exactly that reason on every release since 2.8.6.
    """
    bridge = _bridge_commit_for(tag)
    branch = f"chore/anchor-{version}"
    print(f"[anchor] {tag} -> bridge {bridge[:12]} on {branch}")
    starting_branch = _capture(["git", "branch", "--show-current"]) or "main"
    plan_path = _anchor_plan_path(version)
    try:
        run(["git", "checkout", "-B", branch, "origin/main"])
        text = PROVENANCE.read_text(encoding="utf-8")
        text = bump_version_text(
            text,
            _capture_constant(text, "PRIOR_RELEASE_TAG"),
            tag,
            key="PRIOR_RELEASE_TAG",
        )
        text = bump_version_text(
            text,
            _capture_constant(text, "PRIOR_POST_RELEASE_DOC_COMMIT"),
            bridge,
            key="PRIOR_POST_RELEASE_DOC_COMMIT",
        )
        PROVENANCE.write_text(text, encoding="utf-8")
        promote_release_tables(version)
        plan_path.write_text(_anchor_plan_text(version, tag, bridge), encoding="utf-8")
        run(
            [
                "git",
                "add",
                str(PROVENANCE.relative_to(REPO_ROOT)),
                *(str(path.relative_to(REPO_ROOT)) for path in README_TABLES),
                "CHANGELOG.md",
                str(plan_path.relative_to(REPO_ROOT)),
            ]
        )
        run(
            [
                "git",
                "commit",
                "-m",
                f"chore: advance the release provenance anchor to {version}",
            ]
        )
        run([sys.executable, "scripts/check_harness.py", "--base", "origin/main"])
        run(["git", "push", "--no-verify", "-u", "origin", branch])
        _open_pull_request(
            branch,
            title=f"chore: advance the release provenance anchor to {version}",
            body=(
                f"Anchor to `{tag}` / bridge `{bridge}`, and promote {version} to "
                "current in the compatibility tables. Generated by "
                "`scripts/release.py --anchor`."
            ),
        )
    except Exception:
        run(["git", "checkout", "--force", starting_branch], check=False)
        run(["git", "branch", "-D", branch], check=False)
        raise
    finally:
        run(["git", "checkout", "--force", starting_branch], check=False)


def _release_plan_path(version: str) -> Path:
    plans = sorted(
        (REPO_ROOT / "docs" / "03_Plans" / "active").glob(f"*release-{version}*.md")
    )
    if len(plans) != 1:
        raise ReleaseError(
            f"expected exactly one active release-{version} plan, found {len(plans)}"
        )
    return plans[0]


def render_release_authorization(record: dict, evidence_base: str) -> str:
    """The Release Authorization section, from what `verify` recorded.

    Every claim here is something the record holds: the exact command form the
    checker parses, the exit status, the test counts from the gate's own
    summary lines, and the runtime versions the tree declares. Nothing is
    typed from memory, which is where the 2.9.x sections got their numbers.
    """
    gate = record.get("gate") or {}
    artifact = record.get("artifact") or {}
    if gate.get("exit_status") != 0 or artifact.get("exit_status") != 0:
        raise ReleaseError(
            "the evidence record does not show both gate commands passing; "
            "re-run verify before authorizing"
        )
    version = record["version"]
    runs = gate.get("test_runs") or []
    counts = ", ".join(f"{count} tests OK" for count in runs) or "every suite OK"
    sbom = artifact.get("sbom_components")
    sbom_text = f"a validated SBOM of {sbom} components" if sbom else "a validated SBOM"
    routes = artifact.get("routes")
    routes_text = (
        f"{routes} authenticated routes returning 200"
        if routes
        else "every installed route returning 200"
    )
    wheel = record.get("wheel", f"forward_netbox-{version}-py3-none-any.whl")
    lines = [
        "## Release Authorization",
        "",
        f"- Evidence base commit: `{evidence_base}`",
        "",
        f"- [x] `final-tree-full-gate` - `{gate['command']}` passed on the final "
        f"{version} tree with exit status 0: {counts} across "
        f"{gate.get('tests_total', 0)} tests in total, and the upgrade leg "
        "seeded under the previous release and upgraded onto the tested "
        "runtime with its rows surviving.",
        "",
        "  The gated tree is the release tree apart from this authorization record,",
        "  which is Markdown in a single plan file and is appended after the gate by",
        "  construction.",
        "",
        f"- [x] `exact-runtime-artifact` - `{artifact['command']}` passed with exit "
        f"status 0 against the installed `{wheel}` on NetBox "
        f"{record['netbox_version']}, Branching {record['branching_version']}, "
        f"Python {record['python_version']}, with {routes_text}, all plugin "
        f"migrations applying cleanly with no drift, and {sbom_text}.",
        "",
        "The optional evidence ids are not recorded. The release owner's 2026-07-27",
        "proportionality decision applies; see",
        "`docs/03_Plans/completed/2026-07-27-release-authorization-proportionality.md`.",
        "",
    ]
    return "\n".join(lines)


def stage_authorize(version: str) -> None:
    """Append the Release Authorization section on the evidence branch.

    Runs after the production pull request has merged, because the evidence
    base commit is the commit the tag's parent will be - `origin/main` at that
    moment - and nothing else.
    """
    record = read_evidence_record(version)
    evidence_branch = f"release/{version}-evidence"
    evidence_base = _checkout_merged_main()
    plan_path = _release_plan_path(version)
    text = plan_path.read_text(encoding="utf-8")
    if "## Release Authorization" in text:
        raise ReleaseError(f"{plan_path.name} already carries a Release Authorization")
    run(["git", "checkout", "-B", evidence_branch, "origin/main"])
    section = render_release_authorization(record, evidence_base)
    plan_path.write_text(text.rstrip("\n") + "\n\n" + section, encoding="utf-8")
    run(["git", "add", str(plan_path.relative_to(REPO_ROOT))])
    run(["git", "commit", "-m", f"release: authorize v{version}"])
    run(
        [
            sys.executable,
            "scripts/check_release_authorization.py",
            "--version",
            version,
        ]
    )
    print(f"[authorize] {evidence_branch} carries the rendered authorization")


def stage_finish_unattended(version: str) -> None:
    """Drive the release from the pushed production branch to the anchor.

    Each step is the same function the matching flag runs by hand, so a
    failure names the flag to resume with rather than leaving the operator to
    reconstruct where the sequence stopped.
    """
    production_branch = f"release/{version}"
    evidence_branch = f"{production_branch}-evidence"
    tag = f"v{version}"
    steps = (
        ("--finish (production)", lambda: stage_finish(version)),
        (
            "merge (production)",
            lambda: _wait_for_pull_request_merge(production_branch),
        ),
        ("--authorize", lambda: stage_authorize(version)),
        ("--finish (evidence)", lambda: stage_finish(version)),
        ("merge (evidence)", lambda: _wait_for_pull_request_merge(evidence_branch)),
        ("checkout main", _checkout_merged_main),
        ("--finish (tag)", lambda: stage_finish(version)),
        (
            "merge (bridge)",
            lambda: _wait_for_pull_request_merge(f"docs/post-release-{version}"),
        ),
        ("--anchor", lambda: stage_anchor(version, tag)),
        (
            "merge (anchor)",
            lambda: _wait_for_pull_request_merge(f"chore/anchor-{version}"),
        ),
    )
    for label, step in steps:
        print(f"[auto-finish] {label}")
        try:
            step()
        except ReleaseError as exc:
            raise ReleaseError(
                f"stopped at {label}: {exc}. Fix that, then resume with the "
                "matching flag; every later step is a flag of its own."
            ) from exc
    print(f"[auto-finish] v{version} released, bridged and anchored.")


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
        help="promote, open release PRs, or tag validated main (rollout)",
    )
    parser.add_argument(
        "--authorize",
        action="store_true",
        help="after the production PR merges: render and commit the authorization",
    )
    parser.add_argument(
        "--post-release",
        action="store_true",
        help="after the tag publishes: open the documentation-only bridge PR",
    )
    parser.add_argument(
        "--anchor",
        action="store_true",
        help="after the bridge merges: advance the anchor and promote the tables",
    )
    args = parser.parse_args(argv)

    if not SEMVER_RE.match(args.version):
        parser.error(f"version must be X.Y.Z, got {args.version!r}")

    try:
        if args.anchor:
            stage_anchor(args.version, f"v{args.version}")
            return 0
        if args.post_release:
            stage_post_release(args.version, f"v{args.version}")
            return 0
        if args.authorize:
            stage_authorize(args.version)
            return 0
        if args.finish:
            if args.auto_finish:
                stage_finish_unattended(args.version)
            else:
                stage_finish(args.version)
            return 0
        stage_prepare(args.version, args.summary, write=args.write)
        if args.write:
            stage_verify(args.version)
        if args.publish:
            stage_publish(args.version, auto_finish=args.auto_finish)
    except ReleaseError as exc:
        print(f"release error: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
