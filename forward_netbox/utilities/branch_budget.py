import hashlib
import math
from dataclasses import dataclass
from dataclasses import field

from ..exceptions import ForwardQueryError

DEFAULT_MAX_CHANGES_PER_BRANCH = 10000

DEVICE_SHARD_MODELS = {
    "dcim.interface",
    "dcim.macaddress",
    "dcim.inventoryitem",
    "ipam.ipaddress",
}


@dataclass(frozen=True)
class BranchWorkload:
    model_string: str
    label: str
    upsert_rows: list[dict] = field(default_factory=list)
    delete_rows: list[dict] = field(default_factory=list)
    sync_mode: str = "full"
    coalesce_fields: list[list[str]] = field(default_factory=list)

    @property
    def estimated_changes(self):
        return len(self.upsert_rows) + len(self.delete_rows)


@dataclass(frozen=True)
class BranchPlanItem:
    index: int
    model_string: str
    label: str
    estimated_changes: int
    upsert_rows: list[dict]
    delete_rows: list[dict]
    sync_mode: str
    coalesce_fields: list[list[str]] = field(default_factory=list)
    shard_keys: tuple[str, ...] = ()


def row_shard_key(model_string, row, coalesce_fields):
    if model_string in DEVICE_SHARD_MODELS and row.get("device") not in ("", None):
        return f"device:{row['device']}"

    for field_set in coalesce_fields:
        values = []
        for field_name in field_set:
            if row.get(field_name) in ("", None):
                values = []
                break
            values.append(f"{field_name}={row.get(field_name)}")
        if values:
            return "|".join(values)

    if row:
        return "|".join(f"{key}={row[key]}" for key in sorted(row))
    raise ForwardQueryError(f"Unable to derive a shard key for `{model_string}`.")


def shard_key_digest(shard_key):
    return hashlib.sha256(str(shard_key).encode("utf-8")).hexdigest()


def split_workload(workload, *, max_changes_per_branch):
    if max_changes_per_branch < 1:
        raise ValueError("`max_changes_per_branch` must be at least 1.")

    if workload.estimated_changes <= max_changes_per_branch:
        return [
            BranchPlanItem(
                index=1,
                model_string=workload.model_string,
                label=workload.label,
                estimated_changes=workload.estimated_changes,
                upsert_rows=workload.upsert_rows,
                delete_rows=workload.delete_rows,
                sync_mode=workload.sync_mode,
                coalesce_fields=workload.coalesce_fields,
            )
        ]

    buckets = {}
    for row in workload.upsert_rows:
        key = row_shard_key(
            workload.model_string,
            row,
            workload.coalesce_fields,
        )
        buckets.setdefault(key, {"upsert_rows": [], "delete_rows": []})[
            "upsert_rows"
        ].append(row)
    for row in workload.delete_rows:
        key = row_shard_key(
            workload.model_string,
            row,
            workload.coalesce_fields,
        )
        buckets.setdefault(key, {"upsert_rows": [], "delete_rows": []})[
            "delete_rows"
        ].append(row)

    oversized = [
        (key, len(rows["upsert_rows"]) + len(rows["delete_rows"]))
        for key, rows in buckets.items()
        if len(rows["upsert_rows"]) + len(rows["delete_rows"]) > max_changes_per_branch
    ]
    if oversized:
        key, count = sorted(oversized, key=lambda item: item[1], reverse=True)[0]
        raise ForwardQueryError(
            f"`{workload.model_string}` shard key `{key}` has {count} rows, "
            f"which exceeds the branch budget of {max_changes_per_branch}."
        )

    minimum_branch_count = math.ceil(
        workload.estimated_changes / max_changes_per_branch
    )
    branches = [
        {"upsert_rows": [], "delete_rows": [], "shard_keys": []}
        for _ in range(minimum_branch_count)
    ]
    ordered_buckets = sorted(
        buckets.items(),
        key=lambda item: (
            -(len(item[1]["upsert_rows"]) + len(item[1]["delete_rows"])),
            shard_key_digest(item[0]),
        ),
    )

    for key, rows in ordered_buckets:
        count = len(rows["upsert_rows"]) + len(rows["delete_rows"])
        candidate = None
        for branch in sorted(
            branches,
            key=lambda item: len(item["upsert_rows"]) + len(item["delete_rows"]),
        ):
            branch_count = len(branch["upsert_rows"]) + len(branch["delete_rows"])
            if branch_count + count <= max_changes_per_branch:
                candidate = branch
                break
        if candidate is None:
            candidate = {"upsert_rows": [], "delete_rows": [], "shard_keys": []}
            branches.append(candidate)

        candidate["upsert_rows"].extend(rows["upsert_rows"])
        candidate["delete_rows"].extend(rows["delete_rows"])
        candidate["shard_keys"].append(key)

    plan_items = []
    for index, branch in enumerate(branches, start=1):
        estimated_changes = len(branch["upsert_rows"]) + len(branch["delete_rows"])
        if not estimated_changes:
            continue
        plan_items.append(
            BranchPlanItem(
                index=index,
                model_string=workload.model_string,
                label=f"{workload.label} shard {index}",
                estimated_changes=estimated_changes,
                upsert_rows=branch["upsert_rows"],
                delete_rows=branch["delete_rows"],
                sync_mode=workload.sync_mode,
                coalesce_fields=workload.coalesce_fields,
                shard_keys=tuple(sorted(branch["shard_keys"])),
            )
        )
    return plan_items


def build_branch_plan(workloads, *, max_changes_per_branch):
    plan = []
    for workload in workloads:
        plan.extend(
            split_workload(
                workload,
                max_changes_per_branch=max_changes_per_branch,
            )
        )
    return [
        BranchPlanItem(
            index=index,
            model_string=item.model_string,
            label=item.label,
            estimated_changes=item.estimated_changes,
            upsert_rows=item.upsert_rows,
            delete_rows=item.delete_rows,
            sync_mode=item.sync_mode,
            coalesce_fields=item.coalesce_fields,
            shard_keys=item.shard_keys,
        )
        for index, item in enumerate(plan, start=1)
    ]
