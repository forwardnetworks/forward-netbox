from __future__ import annotations

import csv
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable


MODULE_BAY_IMPORT_FIELDS = (
    "device",
    "name",
    "label",
    "position",
    "description",
)


@dataclass(frozen=True)
class ModuleReadinessReport:
    candidate_rows: int
    existing_bay_rows: int
    missing_bay_rows: int
    missing_device_rows: int
    unique_missing_bays: int
    module_bay_import_rows: tuple[dict[str, str], ...]
    missing_device_names: tuple[str, ...]

    @property
    def ready(self) -> bool:
        return self.missing_bay_rows == 0 and self.missing_device_rows == 0

    def as_dict(self) -> dict[str, int | bool | tuple[str, ...]]:
        return {
            "candidate_rows": self.candidate_rows,
            "existing_bay_rows": self.existing_bay_rows,
            "missing_bay_rows": self.missing_bay_rows,
            "missing_device_rows": self.missing_device_rows,
            "unique_missing_bays": self.unique_missing_bays,
            "ready": self.ready,
            "missing_device_names": self.missing_device_names,
        }


def _clean(value, *, max_length: int | None = None) -> str:
    text = str(value or "").strip()
    if max_length is not None:
        return text[:max_length]
    return text


def derive_module_bay_position(module_bay_name: str) -> str:
    """Best-effort native import convenience; blank is valid in NetBox."""

    name = _clean(module_bay_name)
    match = re.search(r"(\d+)\s*$", name)
    if match:
        return match.group(1)[:30]
    return ""


def module_bay_import_row(row: dict) -> dict[str, str]:
    module_bay_name = _clean(row.get("module_bay"), max_length=64)
    return {
        "device": _clean(row.get("device")),
        "name": module_bay_name,
        "label": module_bay_name,
        "position": _clean(
            row.get("module_bay_position")
            or derive_module_bay_position(module_bay_name),
            max_length=30,
        ),
        "description": "Required for optional Forward module import.",
    }


def summarize_module_readiness(
    rows: Iterable[dict],
    *,
    existing_devices: set[str],
    existing_module_bays: set[tuple[str, str]],
) -> ModuleReadinessReport:
    candidate_rows = 0
    existing_bay_rows = 0
    missing_bay_rows = 0
    missing_device_rows = 0
    missing_devices: set[str] = set()
    missing_bays: dict[tuple[str, str], dict[str, str]] = {}

    for row in rows:
        candidate_rows += 1
        device = _clean(row.get("device"))
        module_bay = _clean(row.get("module_bay"))
        if not device:
            missing_device_rows += 1
            continue
        if device not in existing_devices:
            missing_device_rows += 1
            missing_devices.add(device)
            continue
        key = (device, module_bay)
        if key in existing_module_bays:
            existing_bay_rows += 1
            continue
        missing_bay_rows += 1
        if module_bay:
            missing_bays.setdefault(key, module_bay_import_row(row))

    import_rows = tuple(
        missing_bays[key] for key in sorted(missing_bays, key=lambda item: item)
    )
    return ModuleReadinessReport(
        candidate_rows=candidate_rows,
        existing_bay_rows=existing_bay_rows,
        missing_bay_rows=missing_bay_rows,
        missing_device_rows=missing_device_rows,
        unique_missing_bays=len(import_rows),
        module_bay_import_rows=import_rows,
        missing_device_names=tuple(sorted(missing_devices)),
    )


def write_module_bay_import_csv(path: Path, rows: Iterable[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as output:
        writer = csv.DictWriter(output, fieldnames=MODULE_BAY_IMPORT_FIELDS)
        writer.writeheader()
        for row in rows:
            writer.writerow(
                {field: row.get(field, "") for field in MODULE_BAY_IMPORT_FIELDS}
            )
