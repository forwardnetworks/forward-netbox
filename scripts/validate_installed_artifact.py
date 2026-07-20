#!/usr/bin/env python3
"""Fail unless the exact release wheel is imported in the exact runtime."""
import argparse
import importlib.metadata
import json
import sys
from pathlib import Path

sys.path.insert(0, "/opt/netbox/netbox")

import forward_netbox  # noqa: E402
from utilities.release import load_release_data  # noqa: E402


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--expected-version", required=True)
    args = parser.parse_args()

    module_path = Path(forward_netbox.__file__).resolve()
    package_version = importlib.metadata.version("forward-netbox")
    branching_version = importlib.metadata.version("netboxlabs-netbox-branching")
    netbox_version = load_release_data().full_version.split("-", 1)[0]

    if (
        module_path.is_relative_to("/source")
        or "site-packages" not in module_path.parts
    ):
        raise SystemExit(
            f"forward_netbox imported outside site-packages: {module_path}"
        )
    if package_version != args.expected_version:
        raise SystemExit(
            f"forward-netbox {package_version} != expected {args.expected_version}"
        )
    if netbox_version != "4.6.5":
        raise SystemExit(f"NetBox {netbox_version} != required 4.6.5")
    if branching_version != "1.1.1":
        raise SystemExit(f"netbox-branching {branching_version} != required 1.1.1")

    print(
        json.dumps(
            {
                "branching_version": branching_version,
                "module_path": str(module_path),
                "netbox_version": netbox_version,
                "package_version": package_version,
            },
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
