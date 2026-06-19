#!/usr/bin/env python3
# Conventional-commit check (commit-msg hook). Enforces the style already in use:
#   <type>(optional scope)!: <subject>
# Allowed types include the project's `release:` prefix. Merge/fixup/revert auto-
# commits are exempt.
import re
import sys
from pathlib import Path

TYPES = (
    "feat",
    "fix",
    "docs",
    "style",
    "refactor",
    "perf",
    "test",
    "build",
    "ci",
    "chore",
    "revert",
    "release",
)
PATTERN = re.compile(rf"^({'|'.join(TYPES)})(\([^)]+\))?!?: .+")
EXEMPT_PREFIXES = ("Merge ", "Revert ", "fixup!", "squash!")


def main(argv=None) -> int:
    argv = argv if argv is not None else sys.argv[1:]
    if not argv:
        return 0
    first_line = Path(argv[0]).read_text(encoding="utf-8").splitlines()[0].strip()
    if not first_line or first_line.startswith(EXEMPT_PREFIXES):
        return 0
    if PATTERN.match(first_line):
        return 0
    print(
        "Commit subject must be a Conventional Commit: "
        f"<type>: <subject> (types: {', '.join(TYPES)}).\n"
        f"Got: {first_line!r}",
        file=sys.stderr,
    )
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
