#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
from pathlib import Path


REQUIRED_COMPONENTS = {
    "forward-netbox": None,
    "httpx": "0.28.1",
    "netbox": "4.6.5",
    "netbox-cisco-aci": "0.4.0",
    "netbox-dlm": "0.4.1",
    "netbox-peering-manager": "0.3.0",
    "netbox-routing": "0.4.3",
    "netboxlabs-netbox-branching": "1.1.1",
    "pyzipper": "0.4.0",
}


def _canonical_name(value: str) -> str:
    return re.sub(r"[-_.]+", "-", value).lower()


def validate_sbom(path: Path, expected_version: str) -> dict[str, object]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if payload.get("bomFormat") != "CycloneDX":
        raise ValueError("SBOM is not a CycloneDX document.")

    components = list(payload.get("components") or [])
    metadata_component = (payload.get("metadata") or {}).get("component")
    if metadata_component:
        components.append(metadata_component)
    versions = {
        _canonical_name(str(component.get("name") or "")): str(
            component.get("version") or ""
        )
        for component in components
        if component.get("name")
    }
    required = dict(REQUIRED_COMPONENTS)
    required["forward-netbox"] = expected_version
    failures = {
        name: {"expected": version, "actual": versions.get(name)}
        for name, version in required.items()
        if versions.get(name) != version
    }
    if failures:
        raise ValueError(f"SBOM required-component mismatch: {failures}")
    if len(versions) < 20:
        raise ValueError(
            "SBOM does not represent the installed runtime environment: "
            f"only {len(versions)} components were found."
        )
    return {
        "component_count": len(versions),
        "forward_netbox_version": versions["forward-netbox"],
        "netbox_version": versions["netbox"],
        "branching_version": versions["netboxlabs-netbox-branching"],
    }


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Validate the installed-runtime CycloneDX release SBOM."
    )
    parser.add_argument("--sbom", type=Path, required=True)
    parser.add_argument("--expected-version", required=True)
    args = parser.parse_args()
    print(json.dumps(validate_sbom(args.sbom, args.expected_version), sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
