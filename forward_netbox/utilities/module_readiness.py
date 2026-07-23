from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Iterable


@dataclass(frozen=True)
class ModuleReadinessReport:
    candidate_rows: int
    existing_bay_rows: int
    missing_bay_rows: int
    missing_device_rows: int
    unique_missing_bays: int
    module_bay_plan_rows: tuple[dict[str, str], ...]
    missing_device_names: tuple[str, ...]

    def as_dict(self) -> dict[str, object]:
        return {
            "candidate_rows": self.candidate_rows,
            "existing_bay_rows": self.existing_bay_rows,
            "missing_bay_rows": self.missing_bay_rows,
            "missing_device_rows": self.missing_device_rows,
            "unique_missing_bays": self.unique_missing_bays,
            "planned_bays": self.module_bay_plan_rows,
            "missing_device_names": self.missing_device_names,
        }


def _clean(value, *, max_length: int | None = None) -> str:
    text = str(value or "").strip()
    if max_length is not None:
        return text[:max_length]
    return text


def derive_module_bay_position(module_bay_name: str) -> str:
    """Best-effort branch creation value; blank is valid in NetBox."""

    name = _clean(module_bay_name)
    match = re.search(r"(\d+)\s*$", name)
    if match:
        return match.group(1)[:30]
    return ""


def module_bay_plan_row(row: dict) -> dict[str, str]:
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
        "description": "Created by Forward sync for module import.",
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
            missing_bays.setdefault(key, module_bay_plan_row(row))

    plan_rows = tuple(
        missing_bays[key] for key in sorted(missing_bays, key=lambda item: item)
    )
    return ModuleReadinessReport(
        candidate_rows=candidate_rows,
        existing_bay_rows=existing_bay_rows,
        missing_bay_rows=missing_bay_rows,
        missing_device_rows=missing_device_rows,
        unique_missing_bays=len(plan_rows),
        module_bay_plan_rows=plan_rows,
        missing_device_names=tuple(sorted(missing_devices)),
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
    try:
        specs = resolve_query_specs_for_client(specs, client)
    except Exception:
        # A moved or stale repository path must not make the diagnostic page
        # unusable. Fall back to the shipped query, which is also the safe
        # source for a readiness report and preserves sync tag parameters.
        specs = [get_seeded_builtin_query_spec("dcim.module", "Forward Modules")]

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
