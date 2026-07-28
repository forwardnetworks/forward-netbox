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
import re
import sys
import tomllib
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent

PYPROJECT = REPO_ROOT / "pyproject.toml"
PACKAGE_INIT = REPO_ROOT / "forward_netbox" / "__init__.py"
FAST_BASELINE = REPO_ROOT / "forward_netbox" / "utilities" / "fast_baseline.py"
RUNTIME_VERSION_TEST = (
    REPO_ROOT / "forward_netbox" / "tests" / "test_runtime_dependency_check.py"
)
PACKAGE_JSON = REPO_ROOT / "package.json"
NODE_MODULES = REPO_ROOT / "node_modules"


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


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--json", action="store_true", help="emit the results as JSON")
    arguments = parser.parse_args()

    try:
        version = check_version_surfaces()
        dependencies = check_ui_harness_dependencies()
    except PreflightError as exc:
        print(f"release preflight failed: {exc}", file=sys.stderr)
        return 1

    result = {"version": version, "ui_harness_dependencies": dependencies}
    if arguments.json:
        print(json.dumps(result, sort_keys=True))
    else:
        print(f"release preflight passed: version {version} consistent across surfaces")
        print(
            f"release preflight passed: UI harness dependencies present ({dependencies})"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
