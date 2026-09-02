"""Fast, static preflight for the local release gate.

Every check here is sub-second and fails closed. The point is ordering: the
gate's expensive stages (docker bring-up, the ~13-minute Django suite, the
Playwright run) sit behind these, so a one-line version mismatch or a missing
npm dependency is reported in seconds instead of after half an hour.

Releasing 2.6.3 cost six ~35-minute gate runs, and every failure was
detectable statically:

* a hardcoded plugin version left at the previous release
* the Playwright package never installed, surfaced ~30 minutes in

Both are covered below.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import subprocess
import sys
import tomllib
from pathlib import Path

import verify_release_provenance as provenance

REPO_ROOT = Path(__file__).resolve().parent.parent

PYPROJECT = REPO_ROOT / "pyproject.toml"
PACKAGE_INIT = REPO_ROOT / "forward_netbox" / "__init__.py"
FAST_BASELINE = REPO_ROOT / "forward_netbox" / "utilities" / "fast_baseline.py"
RUNTIME_VERSION_TEST = (
    REPO_ROOT / "forward_netbox" / "tests" / "test_runtime_dependency_check.py"
)
PACKAGE_JSON = REPO_ROOT / "package.json"
NODE_MODULES = REPO_ROOT / "node_modules"


PATTERN_FEED_VARIABLE = "FORWARD_SENSITIVE_PATTERNS"
PATTERN_PARITY_ACKNOWLEDGEMENT = "FORWARD_NETBOX_PATTERN_PARITY_UNVERIFIED"


class PreflightError(Exception):
    """A release preflight check failed."""


def _read(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8")
    except OSError as exc:  # pragma: no cover - unreadable tree is fatal anyway
        raise PreflightError(f"cannot read {path.relative_to(REPO_ROOT)}: {exc}")


def declared_version() -> str:
    with PYPROJECT.open("rb") as handle:
        return tomllib.load(handle)["tool"]["poetry"]["version"]


def _sole_match(path: Path, pattern: str, label: str) -> str:
    matches = re.findall(pattern, _read(path))
    if len(matches) != 1:
        raise PreflightError(
            f"{label}: expected exactly one version literal in "
            f"{path.relative_to(REPO_ROOT)}, found {len(matches)}"
        )
    return matches[0]


def version_surfaces() -> dict[str, str]:
    """Every place the release version is written out.

    Kept explicit rather than globbed: a surface that silently stops being
    checked is exactly the failure this guards against. `fast_baseline` is the
    load-bearing one - its pin gates the fast baseline engine, so a stale value
    silently reverts a first sync to the slow path.
    """
    return {
        # Anchored so the neighbouring min_version/max_version NetBox pins are
        # not mistaken for the plugin version.
        "forward_netbox/__init__.py": _sole_match(
            PACKAGE_INIT, r'(?m)^\s*version\s*=\s*"([^"]+)"', "package __init__"
        ),
        # The pin is a dict entry whose value routinely wraps to its own line.
        "forward_netbox/utilities/fast_baseline.py": _sole_match(
            FAST_BASELINE,
            r'"forward_netbox":\s*"([^"]+)"',
            "fast-baseline runtime pin",
        ),
        "forward_netbox/tests/test_runtime_dependency_check.py": _sole_match(
            RUNTIME_VERSION_TEST,
            r'NetboxForwardConfig\.version,\s*"([^"]+)"',
            "runtime version test",
        ),
    }


def check_version_surfaces() -> str:
    expected = declared_version()
    drifted = {
        path: found for path, found in version_surfaces().items() if found != expected
    }
    if drifted:
        detail = ", ".join(
            f"{path} has {found!r}" for path, found in sorted(drifted.items())
        )
        raise PreflightError(
            f"version surfaces disagree with pyproject {expected!r}: {detail}"
        )
    return expected


def check_ui_harness_dependencies() -> str:
    """The Playwright package must be installed before the gate starts.

    `invoke ci` runs the UI suite last, so a missing `npm install` was only
    reported after the full Django suite had already passed.
    """
    if not PACKAGE_JSON.exists():
        raise PreflightError("package.json is missing; the UI harness cannot run")
    manifest = json.loads(_read(PACKAGE_JSON))
    required = sorted(manifest.get("devDependencies", {}))
    if not required:
        raise PreflightError("package.json declares no UI harness dependencies")
    missing = [name for name in required if not (NODE_MODULES / name).is_dir()]
    if missing:
        raise PreflightError(
            f"UI harness dependencies are not installed: {', '.join(missing)}. "
            "Run `npm install` before the release gate."
        )
    return ", ".join(required)


CONSTRAINTS = REPO_ROOT / "constraints.txt"

# Advisories we accept rather than fix, because forward_netbox's own code never
# exercises the vulnerable path. Each entry needs a comment naming why, and the
# SAME ids must be passed to `pip-audit` in release.yml - a waiver only one of
# the two checks knows about reintroduces the exact gap this preflight exists
# to close (a release that passes here and fails in the actual workflow).
ACCEPTED_DEPENDENCY_ADVISORIES = {
    # PYSEC-2026-2858 / CVE-2026-44405: paramiko <=4.0.0 allows SHA-1 in
    # rsakey.py; fixed only in paramiko 5.0.0. paramiko reaches constraints.txt
    # transitively through netbox-validity's own dependencies (dulwich,
    # django-storages[sftp], scrapli, scrapli_netconf, scp, netmiko) - netmiko
    # pins paramiko <5.0, so 5.0.0 is unreachable without dropping Validity.
    # forward_netbox never imports paramiko, netmiko, or scrapli itself
    # (confirmed by grep); the vulnerable code path is Validity's own SSH
    # device-polling, which config backup exists specifically to avoid using.
    # Revisit when netmiko or Validity relaxes the paramiko ceiling.
    "PYSEC-2026-2858",
}


def check_dependency_advisories() -> str:
    """No pinned dependency may carry a known advisory.

    `pip-audit` ran only in GitHub CI, and an advisory can be published against
    a version that was clean when it was pinned - nothing in the repository has
    to change for it to start failing. That turned every open pull request red
    within a minute of `cryptography` CVE-2026-69247 landing.

    The dangerous case is the release, not the pull request. `release.yml`
    audits the same file, so an advisory published between the gate passing and
    the tag being pushed fails *after* the tag exists - and a tag cannot be
    moved or reused, so the version number is spent. `v2.6.10` and `v2.6.11`
    were both lost to failures discovered at that point.

    Ten seconds here, ahead of a forty-minute gate, is the cheapest place to
    find out.
    """
    if not CONSTRAINTS.exists():
        raise PreflightError(
            "constraints.txt is missing; dependencies cannot be audited"
        )
    executable = shutil.which("pip-audit")
    if executable is None:
        raise PreflightError(
            "pip-audit is not installed, so pinned dependencies cannot be audited "
            "before the gate. Install it (`pip install pip-audit`) or the release "
            "workflow will be the first thing to notice an advisory."
        )
    command = [executable, "--progress-spinner", "off", "-r", str(CONSTRAINTS)]
    for vuln_id in sorted(ACCEPTED_DEPENDENCY_ADVISORIES):
        command += ["--ignore-vuln", vuln_id]
    completed = subprocess.run(
        command,
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
    )
    if completed.returncode != 0:
        detail = (completed.stdout or completed.stderr or "").strip()
        raise PreflightError(
            "pinned dependencies carry a known advisory, which will fail CI and "
            f"the release workflow:\n{detail}"
        )
    if ACCEPTED_DEPENDENCY_ADVISORIES:
        return (
            "no unaccepted advisories in constraints.txt (ignoring: "
            f"{', '.join(sorted(ACCEPTED_DEPENDENCY_ADVISORIES))})"
        )
    return "no known advisories in constraints.txt"


# Dependabot alert ids (GHSA, not PYSEC - a different vocabulary from
# `ACCEPTED_DEPENDENCY_ADVISORIES`, and the two lists do not overlap: paramiko
# PYSEC-2026-2858 does not appear in this repo's Dependabot alerts at all, and
# nothing here has yet needed a waiver) that are accepted rather than fixed.
# Same rule as above: name why, here.
ACCEPTED_DEPENDABOT_ALERTS: set[str] = set()

_DEPENDABOT_BLOCKING_SEVERITIES = frozenset({"high", "critical"})


def _dependabot_token() -> str:
    token = os.environ.get("GH_TOKEN", "").strip()
    if token:
        return token
    completed = subprocess.run(
        ["gh", "auth", "token"],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
    )
    token = completed.stdout.strip()
    if completed.returncode != 0 or not token:
        raise PreflightError(
            "no GitHub token available (GH_TOKEN is unset and `gh auth token` "
            "failed), so open Dependabot alerts cannot be checked before the "
            "gate. Dependabot already scans the full dependency graph pip-audit "
            "cannot see through constraints.txt alone - see the sqlparse "
            "alerts caught after v2.9.0 shipped, which pip-audit never saw "
            "because constraints.txt does not pin it."
        )
    return token


def check_dependabot_alerts() -> str:
    """No open high-or-critical Dependabot alert may be unaccepted.

    `check_dependency_advisories` only audits `constraints.txt` - a small,
    hand-curated pin set for packages this project pins deliberately
    (cryptography, httpx, the optional NetBox plugins). It was never the full
    dependency closure, so a transitive dependency's advisory (sqlparse,
    pulled in by Django with only a floor pin) passed every local and CI
    `pip-audit` run while GitHub's own Dependabot scan - which reads the whole
    `poetry.lock` - already had it open. Nothing in the release process looked
    at Dependabot before this. This closes that gap the same way the pip-audit
    check closes its own: fail closed, before the forty-minute gate, not after
    the tag.
    """
    try:
        token = _dependabot_token()
        # `dependabot/alerts` paginates by cursor (`Link` header), not the
        # `page=N` query parameter every other endpoint `_github_pages` calls
        # uses - it rejects `page` outright. A single 100-alert page is a
        # deliberate, named limit rather than real pagination: this is a
        # single plugin repository, nowhere near 100 open alerts, and cursor
        # pagination would mean exposing response headers through
        # `_github_json`, which nothing else needs.
        alerts = provenance._github_json(
            "dependabot/alerts?state=open&per_page=100", token
        )
        if not isinstance(alerts, list):
            raise PreflightError("GitHub returned invalid Dependabot alert data")
        if len(alerts) >= 100:
            raise PreflightError(
                "100+ open Dependabot alerts - this check only reads the first "
                "page; extend it to follow the Link header before trusting it"
            )
    except provenance.ProvenanceError as exc:
        raise PreflightError(f"could not read Dependabot alerts: {exc}")
    unaccepted = [
        alert
        for alert in alerts
        if alert.get("security_advisory", {}).get("severity")
        in _DEPENDABOT_BLOCKING_SEVERITIES
        and alert.get("security_advisory", {}).get("ghsa_id")
        not in ACCEPTED_DEPENDABOT_ALERTS
    ]
    if unaccepted:
        detail = "; ".join(
            f"{alert.get('dependency', {}).get('package', {}).get('name')} "
            f"({alert.get('security_advisory', {}).get('severity')}, "
            f"{alert.get('security_advisory', {}).get('ghsa_id')})"
            for alert in unaccepted
        )
        raise PreflightError(f"open Dependabot alerts are not accepted: {detail}")
    if ACCEPTED_DEPENDABOT_ALERTS:
        return (
            "no unaccepted high/critical Dependabot alerts (ignoring: "
            f"{', '.join(sorted(ACCEPTED_DEPENDABOT_ALERTS))})"
        )
    return "no open high/critical Dependabot alerts"


def _git(*arguments: str) -> str:
    import subprocess

    return subprocess.run(
        ["git", *arguments],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
    ).stdout.strip()


def check_release_plan_evidence_base(version: str) -> str:
    """The plan's evidence base commit must be what `main` will hold at tag time.

    Authorization binds the tagged commit to its parent. A release branch's own
    parent is *not* that commit: the release squash-merges, so on `main` the
    release commit's parent is `origin/main`'s head at merge time. Recording the
    branch-side parent therefore always mismatches once merged, and the release
    cannot be tagged until a second PR corrects it — which costs a full CI cycle
    for a one-line change.

    Reported here, before the push, so the round trip is seconds instead of an
    hour. Skipped when `origin/main` is unknown (a fresh clone or offline run),
    since a missing remote ref is not evidence of a wrong value.

    `origin/main` is only the right comparison *before* the merge. Once the
    release has merged, `origin/main` is the release commit itself, so demanding
    it here would tell the operator to record the tagged commit in place of its
    parent — the exact value `release_evidence_commit_binding` then rejects,
    since it binds against `HEAD^`. In that window the two checks contradicted
    each other and the release could not be tagged at all. So when `HEAD` is
    already `origin/main`, compare against `HEAD^`: same rule, evaluated from
    the side of the merge the repository is actually on.
    """
    if _git("tag", "--list", f"v{version}"):
        # Already tagged: the recorded value was correct at tag time and is now
        # history. Re-checking it against a moved origin/main is meaningless.
        return f"skipped (v{version} is already tagged)"
    plans = sorted((REPO_ROOT / "docs" / "03_Plans").rglob(f"*release-{version}*.md"))
    if len(plans) != 1:
        return (
            f"skipped (expected exactly one release-{version} plan, found {len(plans)})"
        )
    text = _read(plans[0])
    match = re.search(
        r"^- Evidence base commit: `([0-9a-f]{40})`$", text, flags=re.MULTILINE
    )
    if match is None:
        return "skipped (plan records no evidence base commit yet)"
    remote_main = _git("rev-parse", "origin/main")
    if len(remote_main) != 40:
        return "skipped (origin/main is unknown in this checkout)"
    merged = _git("rev-parse", "HEAD") == remote_main
    reference = "HEAD^" if merged else "origin/main"
    expected = _git("rev-parse", reference) if merged else remote_main
    if len(expected) != 40:
        return f"skipped ({reference} is unknown in this checkout)"
    recorded = match.group(1)
    if recorded != expected:
        detail = (
            f"the release has merged, so the commit about to be tagged is HEAD "
            f"and its parent is {expected}"
            if merged
            else f"a squash merge will make the release commit's parent "
            f"{expected} (origin/main)"
        )
        raise PreflightError(
            f"{plans[0].relative_to(REPO_ROOT)} records evidence base commit "
            f"{recorded}, but {detail}. Authorization binds the tagged commit to "
            f"its parent, so record {expected}."
        )
    return f"{recorded[:12]} matches {reference}"


def check_sensitive_pattern_parity(environment: dict[str, str] | None = None) -> str:
    """Run the RELEASE-TIME sensitive scan before the tag exists.

    The guard matches against whatever pattern feed it is handed. Locally that
    is `.sensitive-patterns.local.txt`; the publish workflow additionally
    supplies `FORWARD_SENSITIVE_PATTERNS`, a repository SECRET that is a strict
    superset of it. Nothing local could see the difference, so a customer name
    used as a test fixture passed every local gate and was caught only by the
    publish workflow - after the tag was pushed and therefore immutable. That
    spent `v2.7.7`.

    The scan itself is unchanged; only its timing is. Running it here makes the
    same refusal cost nothing instead of a version number.

    The feed is a secret, so a checkout that does not have it cannot verify
    parity. That case is not silently tolerated: it fails unless the operator
    acknowledges it explicitly, and the acknowledgement is meant to be recorded
    in the release authorization the way the offline upgrade discovery already
    is - visible in the evidence rather than hidden in someone's shell.
    """
    environment = dict(os.environ if environment is None else environment)
    if not environment.get(PATTERN_FEED_VARIABLE, "").strip():
        if environment.get(PATTERN_PARITY_ACKNOWLEDGEMENT, "").strip():
            return (
                f"UNVERIFIED - {PATTERN_FEED_VARIABLE} is not available in this "
                f"checkout and {PATTERN_PARITY_ACKNOWLEDGEMENT} was set; the "
                "release gate will apply the superset feed and can still refuse "
                "the tag. Record this in the release authorization."
            )
        raise PreflightError(
            f"{PATTERN_FEED_VARIABLE} is not set, so the release-time sensitive "
            "scan cannot run and this checkout cannot tell whether the tree "
            "carries customer data the release gate will refuse. That refusal "
            "happens after the tag is pushed, and a tag is immutable. Export the "
            f"feed to verify parity, or set {PATTERN_PARITY_ACKNOWLEDGEMENT}=1 to "
            "proceed with the gap recorded."
        )

    completed = subprocess.run(
        [
            sys.executable,
            "scripts/check_sensitive_content.py",
            "--git-files",
            "--protected-history",
            "--require-env-patterns",
        ],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=False,
        env=environment,
    )
    if completed.returncode != 0:
        detail = (completed.stdout + completed.stderr).strip().splitlines()
        raise PreflightError(
            "the release-time sensitive scan refused this tree: "
            + (detail[-1] if detail else "no output")
        )
    return f"verified against {PATTERN_FEED_VARIABLE}"


def _outcome(detail: str) -> str:
    """Say `skipped` for a check that declined to run.

    Several checks return a `skipped (reason)` string when they cannot decide -
    no plan yet, no `origin/main`, already tagged. Every one of them was printed
    under a `passed:` prefix, so the release log read

        release preflight passed: evidence base commit skipped (v2.8.3 is
        already tagged)

    which reports a pass for the one check that did not run. That is the defect
    this repository keeps finding in its own products, and it was sitting in the
    tool that gates releases: an operator scanning for the word `passed` sees
    five, and five checks did not happen.

    A skipped check is not a failure and must not stop a release - the reasons
    are all legitimate. It simply must not claim to be a pass.
    """
    if "UNVERIFIED" in detail:
        # The parity gap is the one that has actually refused tags. Printing it
        # as a pass is how a known hole reads as a clean run.
        return "unverified"
    return "skipped" if "skipped" in detail else "passed"


def _report_lines(
    version, dependencies, advisories, dependabot_alerts, evidence_base, pattern_parity
):
    checks = (
        (f"version {version} consistent across surfaces", "passed"),
        (f"UI harness dependencies present ({dependencies})", "passed"),
        (advisories, _outcome(advisories)),
        (dependabot_alerts, _outcome(dependabot_alerts)),
        (f"evidence base commit {evidence_base}", _outcome(evidence_base)),
        (f"sensitive pattern parity {pattern_parity}", _outcome(pattern_parity)),
    )
    lines = []
    for detail, outcome in checks:
        # The detail already carries the word for its own state; the prefix
        # supplies it, so drop the duplicate rather than print it twice.
        if outcome == "skipped":
            detail = detail.replace("skipped (", "(", 1).replace(" skipped:", ":", 1)
        lines.append(f"release preflight {outcome}: {detail}")
    return lines


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--json", action="store_true", help="emit the results as JSON")
    arguments = parser.parse_args()

    try:
        version = check_version_surfaces()
        dependencies = check_ui_harness_dependencies()
        advisories = check_dependency_advisories()
        dependabot_alerts = check_dependabot_alerts()
        evidence_base = check_release_plan_evidence_base(version)
        pattern_parity = check_sensitive_pattern_parity()
    except PreflightError as exc:
        print(f"release preflight failed: {exc}", file=sys.stderr)
        return 1

    result = {
        "version": version,
        "ui_harness_dependencies": dependencies,
        "dependency_advisories": advisories,
        "dependabot_alerts": dependabot_alerts,
        "evidence_base_commit": evidence_base,
        "sensitive_pattern_parity": pattern_parity,
    }
    if arguments.json:
        print(json.dumps(result, sort_keys=True))
    else:
        for line in _report_lines(
            version,
            dependencies,
            advisories,
            dependabot_alerts,
            evidence_base,
            pattern_parity,
        ):
            print(line)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
