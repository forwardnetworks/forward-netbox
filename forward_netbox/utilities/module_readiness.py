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


def fetch_module_rows_for_sync(sync) -> list[dict]:
    """Run the sync's dcim.module query specs and return the raw Forward rows."""
    from .query_registry import get_query_specs
    from .query_registry import get_seeded_builtin_query_spec
    from .query_registry import resolve_query_specs_for_client

    client = sync.source.get_client()
    network_id = sync.get_network_id()
    snapshot_id = sync.resolve_snapshot_id(client)
    specs = get_query_specs("dcim.module", maps=sync.get_maps())
    if not specs:
        specs = [get_seeded_builtin_query_spec("dcim.module", "Forward Modules")]
    specs = resolve_query_specs_for_client(specs, client)

    rows: list[dict] = []
    for spec in specs:
        rows.extend(
            client.run_nqe_query(
                query=spec.query,
                query_id=spec.run_query_id,
                commit_id=spec.commit_id,
                network_id=network_id,
                snapshot_id=snapshot_id,
                parameters=spec.merged_parameters(sync.get_query_parameters()),
                fetch_all=True,
            )
        )
    return rows


def compute_module_readiness_for_sync(sync) -> ModuleReadinessReport:
    """Live module-readiness report for a sync (which module bays are missing)."""
    from dcim.models import Device
    from dcim.models.device_components import ModuleBay

    rows = fetch_module_rows_for_sync(sync)
    return summarize_module_readiness(
        rows,
        existing_devices=set(Device.objects.values_list("name", flat=True)),
        existing_module_bays=set(ModuleBay.objects.values_list("device__name", "name")),
    )


def create_missing_module_bays(report: ModuleReadinessReport) -> dict:
    """Create the missing module bays directly in NetBox (out-of-band ORM).

    This is the same effect as importing the readiness CSV — module bays are
    MPTT, but a plain ``.save()`` outside a Branching merge creates them fine.
    Idempotent: bays that already exist are skipped.
    """
    from dcim.models import Device
    from dcim.models.device_components import ModuleBay
    from django.db import transaction

    wanted_devices = {row["device"] for row in report.module_bay_import_rows}
    devices_by_name = {
        device.name: device for device in Device.objects.filter(name__in=wanted_devices)
    }

    created = 0
    skipped_missing_device = 0
    with transaction.atomic():
        for row in report.module_bay_import_rows:
            device = devices_by_name.get(row["device"])
            if device is None:
                skipped_missing_device += 1
                continue
            if ModuleBay.objects.filter(device=device, name=row["name"]).exists():
                continue
            module_bay = ModuleBay(
                device=device,
                name=row["name"],
                label=row.get("label", ""),
                position=row.get("position", "") or "",
                description=row.get("description", ""),
            )
            module_bay.full_clean()
            module_bay.save()
            created += 1
    return {
        "candidate_bays": len(report.module_bay_import_rows),
        "created": created,
        "skipped_missing_device": skipped_missing_device,
    }
