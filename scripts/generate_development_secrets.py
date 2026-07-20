#!/usr/bin/env python3
from __future__ import annotations

from development_secrets import DEFAULT_SECRET_DIR
from development_secrets import ensure_development_secrets


def main() -> int:
    paths = ensure_development_secrets()
    print(
        f"Development secrets ready: {len(paths)} files in "
        f"{DEFAULT_SECRET_DIR.relative_to(DEFAULT_SECRET_DIR.parents[1])}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
