import hashlib
import math
from dataclasses import dataclass
from dataclasses import field

from ..choices import FORWARD_SUPPORTED_MODELS
from ..choices import ForwardApplyEngineChoices
from ..exceptions import ForwardQueryError
from .sync_contracts import canonical_cable_endpoint_identity
from .sync_contracts import default_coalesce_fields_for_model

DEFAULT_MAX_CHANGES_PER_BRANCH = 10000
BRANCH_BUDGET_SOFT_OVERRUN_PERCENT = 0.05
BRANCH_RUN_STATE_PARAMETER = "_branch_run"
MODEL_CHANGE_DENSITY_PARAMETER = "_model_change_density"
DEFAULT_DENSITY_SAFETY_FACTOR = 0.7
DEFAULT_MODEL_CHANGE_DENSITY = {
    "dcim.cable": 3.0,
    "dcim.module": 2.0,
    "netbox_routing.bgppeer": 5.0,
    "netbox_routing.bgpaddressfamily": 2.0,
    "netbox_routing.bgppeeraddressfamily": 2.0,
    "netbox_routing.ospfinstance": 2.0,
    "netbox_routing.ospfarea": 1.0,
    "netbox_routing.ospfinterface": 3.0,
    "netbox_peering_manager.peeringsession": 2.0,
}
MODEL_DENSITY_SAFETY_FACTORS = {
    "dcim.cable": 0.5,
    "netbox_routing.bgppeer": 0.5,
    "netbox_routing.ospfinterface": 0.5,
}

DEVICE_SHARD_MODELS = {
    "dcim.cable",
    "dcim.interface",
    "dcim.macaddress",
    "dcim.inventoryitem",
    "dcim.module",
    "extras.taggeditem",
    "ipam.ipaddress",
    "netbox_routing.bgppeer",
    "netbox_routing.bgpaddressfamily",
    "netbox_routing.bgppeeraddressfamily",
    "netbox_routing.ospfinstance",
    "netbox_routing.ospfinterface",
    "netbox_peering_manager.peeringsession",
}

IPAM_SHARD_FILTER_FIELDS = {
    "ipam.prefix": ("prefix",),
    "ipam.vlan": ("vid",),
    "ipam.vrf": ("rd", "name"),
}
STRUCTURED_SHARD_FILTER_FIELDS = {
    "dcim.site": ("slug", "name"),
    "dcim.manufacturer": ("slug", "name"),
    "dcim.devicerole": ("slug", "name"),
    "dcim.platform": ("slug", "name"),
    "dcim.devicetype": ("slug", "model"),
    "dcim.device": ("name",),
    "dcim.virtualchassis": ("name",),
    "netbox_routing.ospfarea": ("area_id",),
    **IPAM_SHARD_FILTER_FIELDS,
}

SHARD_FETCH_PARAMETER_MODE = "shard_keys"
SHARD_FETCH_PARAMETER_KEYS = "forward_netbox_shard_keys"
SHARD_FETCH_PARAMETER_MODE_NAME = "forward_netbox_shard_mode"
SHARD_FETCH_PARAMETER_BUCKET = "forward_netbox_shard_bucket"
SHARD_FETCH_PARAMETER_BUCKET_COUNT = "forward_netbox_shard_bucket_count"


def _model_fetch_fallback_contract(model_string):
    bucket_key_family = _fallback_bucket_key_family(model_string)
    return {
        "model": model_string,
        "fetch_mode": "model",
        "key_family": "",
        "shard_safe": False,
        "local_safety_filter": True,
        "schema_contract": "full_model_shape",
        "reason_code": "model_fetch_fallback",
        "reason": (
            "No deterministic shard fetch contract is defined yet; the model is "
            "fetched and the persisted shard is applied locally."
        ),
        "bucket_strategy": {
            "supported": bool(bucket_key_family),
            "key_family": bucket_key_family,
            "reason_code": (
                "deterministic_bucket_candidate"
                if bucket_key_family
                else "bucket_strategy_unavailable"
            ),
            "reason": (
                f"Model can use deterministic `{bucket_key_family}` shard buckets "
                "when query pushdown/bucketing primitives are available."
                if bucket_key_family
                else "No stable shard key family is available for deterministic bucket pushdown."
            ),
        },
    }


def _build_shard_fetch_model_contracts():
    contracts = {}
    for model_string in FORWARD_SUPPORTED_MODELS:
        if model_string in DEVICE_SHARD_MODELS:
            contracts[model_string] = {
                "model": model_string,
                "fetch_mode": "nqe_column_filter",
                "key_family": "device",
                "shard_safe": True,
                "local_safety_filter": True,
                "schema_contract": "same_nqe_row_shape",
                "reason_code": "device_column_filter",
                "reason": (
                    "Device-scoped rows can be fetched with native Forward NQE "
                    "column filters and still receive local shard safety filtering."
                ),
                "bucket_strategy": {
                    "supported": True,
                    "key_family": "device",
                    "reason_code": "device_bucket_available",
                    "reason": "Device shard keys can be bucketed deterministically.",
                },
            }
        elif model_string in STRUCTURED_SHARD_FILTER_FIELDS:
            contracts[model_string] = {
                "model": model_string,
                "fetch_mode": "nqe_column_filter",
                "key_family": ",".join(STRUCTURED_SHARD_FILTER_FIELDS[model_string]),
                "shard_safe": True,
                "local_safety_filter": True,
                "schema_contract": "same_nqe_row_shape",
                "reason_code": (
                    "ipam_column_filter"
                    if model_string in IPAM_SHARD_FILTER_FIELDS
                    else "structured_column_filter"
                ),
                "reason": (
                    "Rows can be fetched with native Forward NQE column filters "
                    "when the shard key exposes one of the stable identity columns."
                ),
                "bucket_strategy": {
                    "supported": True,
                    "key_family": ",".join(
                        STRUCTURED_SHARD_FILTER_FIELDS[model_string]
                    ),
                    "reason_code": "column_filter_bucket_available",
                    "reason": (
                        "This model has deterministic identity fields suitable for "
                        "column-filter pushdown and deterministic bucketing."
                    ),
                },
            }
        else:
            contracts[model_string] = _model_fetch_fallback_contract(model_string)
    return contracts


SHARD_FETCH_MODEL_CONTRACTS = _build_shard_fetch_model_contracts()


@dataclass(frozen=True)
class BranchWorkload:
    model_string: str
    label: str
    upsert_rows: list[dict] = field(default_factory=list)
    delete_rows: list[dict] = field(default_factory=list)
    sync_mode: str = "full"
    coalesce_fields: list[list[str]] = field(default_factory=list)
    query_name: str = ""
    execution_mode: str = ""
    execution_value: str = ""
    query_runtime_ms: float | None = None
    baseline_snapshot_id: str = ""
    apply_engine: str = ForwardApplyEngineChoices.ADAPTER
    apply_engine_reason: str = ""
    apply_engine_decision: dict = field(default_factory=dict)

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
    query_name: str = ""
    execution_mode: str = ""
    execution_value: str = ""
    query_runtime_ms: float | None = None
    baseline_snapshot_id: str = ""
    apply_engine: str = ForwardApplyEngineChoices.ADAPTER
    apply_engine_reason: str = ""
    apply_engine_decision: dict = field(default_factory=dict)


def row_shard_key(model_string, row, coalesce_fields):
    if model_string == "dcim.cable":
        canonical_identity = canonical_cable_endpoint_identity(row)
        if canonical_identity is not None:
            return "cable:" + "|".join(
                f"{device}:{interface}" for device, interface in canonical_identity
            )

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


def shard_fetch_contract(model_string, shard_keys):
    shard_keys = tuple(sorted(str(key) for key in shard_keys or () if key))
    if not shard_keys:
        return {
            "fetch_mode": "model",
            "fetch_key_family": "",
            "fetch_parameters": {},
            "fetch_column_filters": [],
        }

    device_names = _device_names_from_shard_keys(shard_keys)
    if device_names and model_string in DEVICE_SHARD_MODELS:
        fetch_parameters = {
            SHARD_FETCH_PARAMETER_MODE_NAME: SHARD_FETCH_PARAMETER_MODE,
            SHARD_FETCH_PARAMETER_KEYS: list(shard_keys),
            SHARD_FETCH_PARAMETER_BUCKET: 0,
            SHARD_FETCH_PARAMETER_BUCKET_COUNT: 1,
        }
        return {
            "fetch_mode": "nqe_column_filter",
            "fetch_key_family": "device",
            "fetch_parameters": fetch_parameters,
            "query_parameters": {},
            "fetch_column_filters": (
                [
                    {
                        "operator": "DEFAULT",
                        "columnName": "device",
                        "value": device_names[0],
                    }
                ]
                if len(device_names) == 1
                else [
                    {
                        "operator": "EQUALS_ANY",
                        "columnName": "device",
                        "values": list(device_names),
                    }
                ]
            ),
        }

    structured_contract = _structured_column_filter_contract(model_string, shard_keys)
    if structured_contract:
        return structured_contract

    return {
        "fetch_mode": "model",
        "fetch_key_family": "",
        "fetch_parameters": {},
        "query_parameters": {},
        "fetch_column_filters": [],
    }


def shard_fetch_capability_for_model(model_string):
    model_string = str(model_string or "")
    return dict(
        SHARD_FETCH_MODEL_CONTRACTS.get(
            model_string,
            _model_fetch_fallback_contract(model_string),
        )
    )


def _device_names_from_shard_keys(shard_keys):
    device_names = []
    for key in shard_keys:
        if not str(key).startswith("device:"):
            return []
        device_name = str(key).removeprefix("device:")
        if not device_name:
            return []
        device_names.append(device_name)
    return sorted(device_names)


def _structured_column_filter_contract(model_string, shard_keys):
    candidate_fields = STRUCTURED_SHARD_FILTER_FIELDS.get(model_string)
    if not candidate_fields:
        return {}

    parsed_keys = [_parse_structured_shard_key(key) for key in shard_keys]
    if not parsed_keys or any(not parsed_key for parsed_key in parsed_keys):
        return {}

    for field_name in candidate_fields:
        values = [
            str(parsed_key[field_name])
            for parsed_key in parsed_keys
            if parsed_key.get(field_name) not in ("", None)
        ]
        if len(values) != len(parsed_keys):
            continue
        unique_values = sorted(set(values))
        fetch_mode = "nqe_column_filter"
        filters = [
            {
                "operator": "EQUALS_ANY",
                "columnName": field_name,
                "values": list(unique_values),
            }
        ]
        return {
            "fetch_key_family": field_name,
            "fetch_parameters": {
                SHARD_FETCH_PARAMETER_MODE_NAME: SHARD_FETCH_PARAMETER_MODE,
                SHARD_FETCH_PARAMETER_KEYS: list(shard_keys),
                SHARD_FETCH_PARAMETER_BUCKET: 0,
                SHARD_FETCH_PARAMETER_BUCKET_COUNT: 1,
            },
            "fetch_mode": fetch_mode,
            "fetch_column_filters": filters,
        }
    return {}


def _fallback_bucket_key_family(model_string):
    field_sets = default_coalesce_fields_for_model(model_string)
    if not field_sets:
        return ""
    first = field_sets[0]
    if not first:
        return ""
    return ",".join(str(field_name) for field_name in first if str(field_name))


def _parse_structured_shard_key(shard_key):
    parsed = {}
    for part in str(shard_key).split("|"):
        if "=" not in part:
            return {}
        field_name, value = part.split("=", 1)
        if not field_name:
            return {}
        parsed[field_name] = value
    return parsed


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
                query_name=workload.query_name,
                execution_mode=workload.execution_mode,
                execution_value=workload.execution_value,
                query_runtime_ms=workload.query_runtime_ms,
                baseline_snapshot_id=workload.baseline_snapshot_id,
                apply_engine=workload.apply_engine,
                apply_engine_reason=workload.apply_engine_reason,
                apply_engine_decision=workload.apply_engine_decision,
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

    soft_limit = soft_budget_limit(max_changes_per_branch)
    oversized = [
        (key, len(rows["upsert_rows"]) + len(rows["delete_rows"]))
        for key, rows in buckets.items()
        if len(rows["upsert_rows"]) + len(rows["delete_rows"]) > soft_limit
    ]
    if oversized:
        key, count = sorted(oversized, key=lambda item: item[1], reverse=True)[0]
        raise ForwardQueryError(
            f"`{workload.model_string}` shard key `{key}` has {count} rows, "
            f"which exceeds the soft branch budget limit of {soft_limit} "
            f"(guideline {max_changes_per_branch})."
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
                query_name=workload.query_name,
                execution_mode=workload.execution_mode,
                execution_value=workload.execution_value,
                query_runtime_ms=workload.query_runtime_ms,
                baseline_snapshot_id=workload.baseline_snapshot_id,
                apply_engine=workload.apply_engine,
                apply_engine_reason=workload.apply_engine_reason,
                apply_engine_decision=workload.apply_engine_decision,
            )
        )
    return plan_items


def soft_budget_limit(max_changes_per_branch):
    budget = max(1, int(max_changes_per_branch or 1))
    return int(budget * (1 + BRANCH_BUDGET_SOFT_OVERRUN_PERCENT))


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
            query_name=item.query_name,
            execution_mode=item.execution_mode,
            execution_value=item.execution_value,
            query_runtime_ms=item.query_runtime_ms,
            baseline_snapshot_id=item.baseline_snapshot_id,
            apply_engine=item.apply_engine,
            apply_engine_reason=item.apply_engine_reason,
            apply_engine_decision=item.apply_engine_decision,
        )
        for index, item in enumerate(plan, start=1)
    ]


def effective_row_budget_for_model(
    model_string,
    *,
    max_changes_per_branch,
    model_change_density=None,
    safety_factor=DEFAULT_DENSITY_SAFETY_FACTOR,
):
    if max_changes_per_branch < 1:
        raise ValueError("`max_changes_per_branch` must be at least 1.")

    density = (model_change_density or {}).get(model_string)
    if density is None:
        density = DEFAULT_MODEL_CHANGE_DENSITY.get(model_string)
    if density is None:
        return max_changes_per_branch
    try:
        density_value = float(density)
    except (TypeError, ValueError):
        return max_changes_per_branch
    if density_value <= 0:
        return max_changes_per_branch

    effective_safety_factor = MODEL_DENSITY_SAFETY_FACTORS.get(
        model_string,
        float(safety_factor),
    )
    scaled_budget = int(
        (max_changes_per_branch * float(effective_safety_factor)) / density_value
    )
    return max(1, min(max_changes_per_branch, scaled_budget))


def build_branch_plan_with_density(
    workloads,
    *,
    max_changes_per_branch,
    model_change_density=None,
    safety_factor=DEFAULT_DENSITY_SAFETY_FACTOR,
):
    plan = []
    for workload in workloads:
        model_budget = effective_row_budget_for_model(
            workload.model_string,
            max_changes_per_branch=max_changes_per_branch,
            model_change_density=model_change_density,
            safety_factor=safety_factor,
        )
        plan.extend(
            split_workload(
                workload,
                max_changes_per_branch=model_budget,
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
            query_name=item.query_name,
            execution_mode=item.execution_mode,
            execution_value=item.execution_value,
            query_runtime_ms=item.query_runtime_ms,
            baseline_snapshot_id=item.baseline_snapshot_id,
            apply_engine=item.apply_engine,
            apply_engine_reason=item.apply_engine_reason,
            apply_engine_decision=item.apply_engine_decision,
        )
        for index, item in enumerate(plan, start=1)
    ]


def build_branch_budget_hints(
    model_strings,
    *,
    max_changes_per_branch,
    model_change_density=None,
):
    return {
        model_string: effective_row_budget_for_model(
            model_string,
            max_changes_per_branch=max_changes_per_branch,
            model_change_density=model_change_density,
        )
        for model_string in model_strings
    }
