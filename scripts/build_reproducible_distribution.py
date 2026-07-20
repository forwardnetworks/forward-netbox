#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]


class ReproducibleBuildError(RuntimeError):
    pass


def _source_date_epoch() -> str:
    result = subprocess.run(
        ["git", "show", "-s", "--format=%ct", "HEAD"],
        cwd=REPO_ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    epoch = result.stdout.strip()
    if not epoch.isdecimal():
        raise ReproducibleBuildError("git returned an invalid source date epoch")
    return epoch


def _artifact_digests(directory: Path) -> dict[str, str]:
    artifacts = sorted(path for path in directory.iterdir() if path.is_file())
    if not artifacts:
        raise ReproducibleBuildError(f"build produced no artifacts in {directory}")
    return {
        artifact.name: hashlib.sha256(artifact.read_bytes()).hexdigest()
        for artifact in artifacts
    }


def _build_once(output: Path, *, source_date_epoch: str) -> dict[str, str]:
    environment = os.environ.copy()
    environment["SOURCE_DATE_EPOCH"] = source_date_epoch
    subprocess.run(
        [
            sys.executable,
            "-m",
            "build",
            "--no-isolation",
            "--outdir",
            str(output),
        ],
        cwd=REPO_ROOT,
        env=environment,
        check=True,
    )
    return _artifact_digests(output)


def build_reproducible_distribution(output: Path) -> dict[str, str]:
    source_date_epoch = _source_date_epoch()
    with tempfile.TemporaryDirectory(prefix="forward-netbox-build-a-") as first_dir:
        with tempfile.TemporaryDirectory(
            prefix="forward-netbox-build-b-"
        ) as second_dir:
            first = Path(first_dir)
            second = Path(second_dir)
            first_digests = _build_once(first, source_date_epoch=source_date_epoch)
            second_digests = _build_once(second, source_date_epoch=source_date_epoch)
            if first_digests != second_digests:
                raise ReproducibleBuildError(
                    "independent release builds produced different SHA-256 digests"
                )
            shutil.rmtree(output, ignore_errors=True)
            output.mkdir(parents=True)
            for artifact in sorted(first.iterdir()):
                if artifact.is_file():
                    shutil.copy2(artifact, output / artifact.name)
    return first_digests


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Build the release twice and require byte-identical artifacts."
    )
    parser.add_argument("--output", type=Path, default=REPO_ROOT / "dist")
    args = parser.parse_args()
    digests = build_reproducible_distribution(args.output.resolve())
    print(json.dumps({"artifacts": digests}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
