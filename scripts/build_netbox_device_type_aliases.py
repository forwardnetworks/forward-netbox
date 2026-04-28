#!/usr/bin/env python3
"""Build a Forward NQE JSON data file from netbox-community/devicetype-library."""
from __future__ import annotations

import argparse
import json
import re
import subprocess
from pathlib import Path
from typing import Any

try:
    import yaml
except ImportError:  # pragma: no cover - exercised by users without PyYAML.
    yaml = None

DATA_ROW_FIELDS = [
    "record_type",
    "source",
    "source_commit",
    "source_path",
    "manufacturer",
    "manufacturer_slug",
    "forward_manufacturer",
    "forward_manufacturer_slug",
    "forward_vendor",
    "forward_model",
    "forward_model_slug",
    "netbox_model",
    "netbox_slug",
    "part_number",
    "match_source",
]

MANUFACTURER_OVERRIDES = [
    ("Vendor.A10", "A10"),
    ("Vendor.AMAZON", "Amazon"),
    ("Vendor.ARISTA", "Arista"),
    ("Vendor.ARUBA", "Aruba"),
    ("Vendor.AVAYA", "Avaya"),
    ("Vendor.AVI_NETWORKS", "Avi Networks"),
    ("Vendor.AZURE", "Microsoft"),
    ("Vendor.BLUECAT", "BlueCat"),
    ("Vendor.BROCADE", "Brocade"),
    ("Vendor.CHECKPOINT", "Check Point"),
    ("Vendor.CISCO", "Cisco"),
    ("Vendor.CITRIX", "Citrix"),
    ("Vendor.CUMULUS", "Cumulus"),
    ("Vendor.DELL", "Dell"),
    ("Vendor.EDGE_CORE", "Edge Core"),
    ("Vendor.EXTREME", "Extreme Networks"),
    ("Vendor.F5", "F5"),
    ("Vendor.FORCEPOINT", "Forcepoint"),
    ("Vendor.FORTINET", "Fortinet"),
    ("Vendor.GENERAL_DYNAMICS", "General Dynamics"),
    ("Vendor.GOOGLE", "Google"),
    ("Vendor.HP", "HPE"),
    ("Vendor.HUAWEI", "Huawei"),
    ("Vendor.JUNIPER", "Juniper"),
    ("Vendor.LINUX_GENERIC", "Linux"),
    ("Vendor.NOKIA", "Nokia"),
    ("Vendor.PALO_ALTO_NETWORKS", "Palo Alto Networks"),
    ("Vendor.PENSANDO", "Pensando"),
    ("Vendor.PICA8", "Pica8"),
    ("Vendor.RIVERBED", "Riverbed"),
    ("Vendor.SILVER_PEAK", "Silver Peak"),
    ("Vendor.SYMANTEC", "Symantec"),
    ("Vendor.T128", "128T"),
    ("Vendor.UNKNOWN", "Unknown"),
    ("Vendor.VERSA", "Versa"),
    ("Vendor.VIASAT", "Viasat"),
    ("Vendor.VMWARE", "VMware"),
    ("Vendor.ALKIRA", "Alkira"),
]


def normalize_data_row(values: dict[str, Any]) -> dict[str, str]:
    return {field: str(values.get(field) or "") for field in DATA_ROW_FIELDS}


def slugify(value: str) -> str:
    value = str(value or "").strip().lower().replace("&", " and ")
    value = re.sub(r"[^a-z0-9]+", "-", value)
    return re.sub(r"-+", "-", value).strip("-")


def slugify_netbox_model(value: str) -> str:
    value = (
        str(value or "")
        .replace("+", " plus ")
        .replace("/", " slash ")
        .replace(".", " dot ")
    )
    return slugify(value)


def git_commit(repo_root: Path) -> str:
    try:
        return subprocess.check_output(
            ["git", "-C", str(repo_root), "rev-parse", "HEAD"],
            stderr=subprocess.DEVNULL,
            text=True,
        ).strip()
    except (OSError, subprocess.CalledProcessError):
        return ""


def load_device_type(path: Path) -> dict[str, Any]:
    if yaml is None:
        raise SystemExit(
            "PyYAML is required to parse device type library YAML files. "
            "Install it with `python -m pip install pyyaml`."
        )
    data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    if not isinstance(data, dict):
        return {}
    return data


def build_manufacturer_override_rows() -> list[dict[str, str]]:
    return [
        normalize_data_row(
            {
                "record_type": "manufacturer_override",
                "source": "forward-netbox",
                "manufacturer": manufacturer,
                "manufacturer_slug": slugify(manufacturer),
                "forward_manufacturer": manufacturer,
                "forward_manufacturer_slug": slugify(manufacturer),
                "forward_vendor": forward_vendor,
                "match_source": "manufacturer_override",
            }
        )
        for forward_vendor, manufacturer in MANUFACTURER_OVERRIDES
    ]


def build_alias_rows(
    library_root: Path, *, include_manufacturer_overrides: bool = True
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    device_type_root = library_root / "device-types"
    if not device_type_root.is_dir():
        raise SystemExit(
            f"{library_root} does not look like a device type library checkout; "
            "expected a device-types/ directory."
        )

    commit = git_commit(library_root)
    canonical_rows: list[dict[str, Any]] = []
    conflicts: list[dict[str, Any]] = []
    seen_canonical: set[tuple[str, str]] = set()

    for path in sorted(device_type_root.glob("*/*.y*ml")):
        try:
            data = load_device_type(path)
        except Exception as exc:  # noqa: BLE001 - report and keep building.
            conflicts.append(
                {
                    "type": "parse_error",
                    "source_path": str(path.relative_to(library_root)),
                    "error": str(exc),
                }
            )
            continue

        manufacturer = str(data.get("manufacturer") or path.parent.name).strip()
        model = str(data.get("model") or "").strip()
        netbox_slug = str(data.get("slug") or "").strip()
        part_number = str(data.get("part_number") or "").strip()
        if not manufacturer or not model or not netbox_slug:
            continue

        manufacturer_slug = slugify(manufacturer)
        row = {
            "source": "netbox-community/devicetype-library",
            "source_commit": commit,
            "source_path": str(path.relative_to(library_root)),
            "manufacturer": manufacturer,
            "manufacturer_slug": manufacturer_slug,
            "netbox_model": model,
            "netbox_slug": netbox_slug,
            "part_number": part_number,
        }
        key = (manufacturer_slug, netbox_slug)
        if key in seen_canonical:
            conflicts.append(
                {
                    "type": "duplicate_canonical_slug",
                    "manufacturer_slug": manufacturer_slug,
                    "netbox_slug": netbox_slug,
                    "source_path": row["source_path"],
                }
            )
        seen_canonical.add(key)
        canonical_rows.append(row)

    alias_rows: list[dict[str, Any]] = []
    seen_aliases: dict[tuple[str, str], dict[str, Any]] = {}

    def add_alias(row: dict[str, Any], alias: str, match_source: str) -> None:
        alias_row = normalize_data_row(
            {
                "record_type": "device_type_alias",
                "source": row["source"],
                "source_commit": row["source_commit"],
                "source_path": row["source_path"],
                "manufacturer": row["manufacturer"],
                "manufacturer_slug": row["manufacturer_slug"],
                "forward_manufacturer": row["manufacturer"],
                "forward_manufacturer_slug": row["manufacturer_slug"],
                "forward_model": alias,
                "forward_model_slug": slugify_netbox_model(alias),
                "netbox_model": row["netbox_model"],
                "netbox_slug": row["netbox_slug"],
                "part_number": row["part_number"],
                "match_source": match_source,
            }
        )
        key = (alias_row["forward_manufacturer_slug"], alias_row["forward_model_slug"])
        existing = seen_aliases.get(key)
        if existing is not None:
            if (existing["netbox_slug"], existing["netbox_model"]) != (
                alias_row["netbox_slug"],
                alias_row["netbox_model"],
            ):
                conflicts.append(
                    {
                        "type": "alias_conflict_skipped",
                        "manufacturer_slug": key[0],
                        "forward_model_slug": key[1],
                        "skipped_match_source": match_source,
                        "kept": {
                            "match_source": existing["match_source"],
                            "netbox_slug": existing["netbox_slug"],
                            "netbox_model": existing["netbox_model"],
                            "source_path": existing["source_path"],
                        },
                        "skipped": {
                            "netbox_slug": alias_row["netbox_slug"],
                            "netbox_model": alias_row["netbox_model"],
                            "source_path": alias_row["source_path"],
                        },
                    }
                )
            return
        seen_aliases[key] = alias_row
        alias_rows.append(alias_row)

    for row in canonical_rows:
        add_alias(row, row["netbox_model"], "model")
    for row in canonical_rows:
        part_number = row["part_number"]
        if part_number and slugify_netbox_model(part_number) != slugify_netbox_model(
            row["netbox_model"]
        ):
            add_alias(row, part_number, "part_number")

    if include_manufacturer_overrides:
        alias_rows = [*build_manufacturer_override_rows(), *alias_rows]

    return alias_rows, conflicts


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Build netbox_device_type_aliases.json for Forward NQE data files."
    )
    parser.add_argument(
        "library_root",
        type=Path,
        help="Path to a netbox-community/devicetype-library checkout.",
    )
    parser.add_argument(
        "-o",
        "--output",
        type=Path,
        default=Path("netbox_device_type_aliases.json"),
        help="Output JSON file path.",
    )
    parser.add_argument(
        "--conflicts-output",
        type=Path,
        help="Optional JSON report for skipped conflicting aliases.",
    )
    parser.add_argument(
        "--no-manufacturer-overrides",
        action="store_true",
        help="Do not include Forward vendor to NetBox manufacturer override rows.",
    )
    args = parser.parse_args()

    rows, conflicts = build_alias_rows(
        args.library_root.resolve(),
        include_manufacturer_overrides=not args.no_manufacturer_overrides,
    )
    args.output.write_text(
        json.dumps(rows, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    if args.conflicts_output:
        args.conflicts_output.write_text(
            json.dumps(conflicts, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
    print(
        json.dumps(
            {
                "output": str(args.output),
                "rows": len(rows),
                "conflicts": len(conflicts),
                "conflicts_output": (
                    str(args.conflicts_output) if args.conflicts_output else None
                ),
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
