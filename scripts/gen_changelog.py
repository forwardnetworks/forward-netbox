#!/usr/bin/env python3
# Generate CHANGELOG.md from the README compatibility table (the maintained
# single source of per-release summaries) plus git tag dates.
#
# Usage: python scripts/gen_changelog.py [--check]
#   --check exits non-zero if CHANGELOG.md is out of date (for CI).
import argparse
import re
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
README = REPO_ROOT / "README.md"
CHANGELOG = REPO_ROOT / "CHANGELOG.md"

ROW_RE = re.compile(r"^\| `v([0-9][^`]*)` \| [^|]* \| (.+?) \|\s*$")


def _tag_date(version: str) -> str:
    try:
        out = subprocess.run(
            ["git", "log", "-1", "--format=%as", f"v{version}"],
            cwd=REPO_ROOT,
            capture_output=True,
            text=True,
        )
        return out.stdout.strip()
    except Exception:  # pragma: no cover - defensive
        return ""


def _summary(status_cell: str) -> str:
    # Drop the leading "Current release;" / "Superseded by `vX`;" prefix.
    text = re.sub(r"^(Current release;|Superseded by `v[^`]+`;)\s*", "", status_cell)
    return text.strip()


def build_changelog() -> str:
    lines = [
        "# Changelog",
        "",
        "Generated from the README compatibility table by "
        "`scripts/gen_changelog.py`. Do not edit by hand.",
        "",
    ]
    for raw in README.read_text(encoding="utf-8").splitlines():
        match = ROW_RE.match(raw)
        if not match:
            continue
        version, status_cell = match.group(1), match.group(2)
        date = _tag_date(version)
        header = f"## v{version}" + (f" — {date}" if date else "")
        lines += [header, "", _summary(status_cell), ""]
    return "\n".join(lines).rstrip() + "\n"


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Generate CHANGELOG.md.")
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args(argv)

    content = build_changelog()
    if args.check:
        current = CHANGELOG.read_text(encoding="utf-8") if CHANGELOG.exists() else ""
        if current != content:
            print(
                "CHANGELOG.md is out of date; run scripts/gen_changelog.py",
                file=sys.stderr,
            )
            return 1
        return 0
    CHANGELOG.write_text(content, encoding="utf-8")
    print(f"wrote {CHANGELOG.relative_to(REPO_ROOT)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
