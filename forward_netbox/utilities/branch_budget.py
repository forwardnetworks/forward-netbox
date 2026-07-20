import hashlib
import logging
import math
from dataclasses import dataclass
from dataclasses import field

from ..choices import FORWARD_SUPPORTED_MODELS
from ..choices import ForwardApplyEngineChoices
from ..exceptions import ForwardQueryError
from .density_learning import density_budget_policy
from .density_learning import normalize_density_profile
from .sync_contracts import canonical_cable_endpoint_identity
from .sync_contracts import default_coalesce_fields_for_model
from .sync_contracts import row_coalesce_field_is_complete

logger = logging.getLogger(__name__)

DEFAULT_MAX_CHANGES_PER_STAGING_ITEM = 10000
MODEL_CHANGE_DENSITY_PARAMETER = "_model_change_density"
MODEL_CHANGE_DENSITY_PROFILE_PARAMETER = "_model_change_density_profile"
DEFAULT_DENSITY_SAFETY_FACTOR = 0.7
DENSITY_ROW_BUDGET_MAX_MULTIPLIER = 5
DENSITY_ROW_BUDGET_WIDEN_POLICIES = {"high_confidence_learned_density"}
DEFAULT_MODEL_CHANGE_DENSITY = {
    "dcim.cable": 3.0,
    "dcim.module": 2.0,
    "netbox_dlm.softwareversion": 1.0,
    "netbox_dlm.hardwarenotice": 1.0,
    "netbox_dlm.devicesoftware": 1.0,
    "netbox_dlm.cve": 1.0,
    # ~16 vulnerable CVEs per device (73,973 pairs / 4,728 devices observed).
    "netbox_dlm.vulnerability": 16.0,
    "netbox_routing.bgppeer": 5.0,
    "netbox_routing.bgpaddressfamily": 2.0,
    "netbox_routing.bgppeeraddressfamily": 2.0,
    "netbox_routing.ospfinstance": 2.0,
    "netbox_routing.ospfarea": 1.0,
    "netbox_routing.ospfinterface": 3.0,
    "netbox_peering_manager.peeringsession": 2.0,
    "ipam.fhrpgroup": 3.0,
}
DEFAULT_MODEL_DELETE_CHANGE_DENSITY = {
    "dcim.device": 20.0,
}
DELETE_LARGE_SHARD_WARNING_RATIO = 0.8
DELETE_WAVE_WARNING_ROW_COUNT = 1000
DELETE_WAVE_WARNING_SHARE = 0.5
MODEL_DENSITY_SAFETY_FACTORS = {
    "dcim.cable": 0.5,
    "netbox_routing.bgppeer": 0.5,
    "netbox_routing.ospfinterface": 0.5,
}
BRANCH_PLAN_OPERATION_APPLY = "apply"
BRANCH_PLAN_OPERATION_DELETE = "delete"
BRANCH_PLAN_OPERATION_MIXED = "mixed"
APPLY_DEPENDENCY_MODEL_ORDER = (
    "dcim.site",
    "dcim.manufacturer",
    "dcim.devicerole",
    "dcim.platform",
    "dcim.devicetype",
    "ipam.vlan",
    "ipam.vrf",
    "ipam.prefix",
    "dcim.device",
    "dcim.virtualchassis",
    "extras.taggeditem",
    "dcim.interface",
    "dcim.module",
    "dcim.inventoryitem",
    "ipam.ipaddress",
    "ipam.fhrpgroup",
    "dcim.macaddress",
    "dcim.cable",
    "netbox_routing.ospfarea",
    "netbox_routing.ospfinstance",
    "netbox_routing.ospfinterface",
    "netbox_routing.bgppeer",
    "netbox_routing.bgpaddressfamily",
    "netbox_routing.bgppeeraddressfamily",
    "netbox_peering_manager.peeringsession",
    "netbox_dlm.devicesoftware",
    "netbox_dlm.softwareversion",
    "netbox_dlm.hardwarenotice",
    "netbox_dlm.cve",
    "netbox_dlm.vulnerability",
    "netbox_cisco_aci.acifabric",
    "netbox_cisco_aci.acipod",
    "netbox_cisco_aci.acinode",
    "netbox_cisco_aci.acitenant",
    "netbox_cisco_aci.acivrf",
    "netbox_cisco_aci.acibridgedomain",
    "netbox_cisco_aci.acifilter",
    "netbox_cisco_aci.acil3out",
)
APPLY_DEPENDENCY_MODEL_RANK = {
    model_string: index
    for index, model_string in enumerate(APPLY_DEPENDENCY_MODEL_ORDER)
}
APPLY_PARENT_MODEL_DEPENDENCIES = {
    "dcim.device": (
        "dcim.site",
        "dcim.manufacturer",
        "dcim.devicerole",
        "dcim.platform",
        "dcim.devicetype",
    ),
    "dcim.virtualchassis": ("dcim.device",),
    "extras.taggeditem": ("dcim.device",),
    "dcim.interface": ("dcim.device",),
    "dcim.module": ("dcim.device",),
    "dcim.inventoryitem": ("dcim.device",),
    "ipam.ipaddress": ("dcim.device", "dcim.interface"),
    "ipam.fhrpgroup": ("dcim.device", "dcim.interface"),
    "dcim.macaddress": ("dcim.device", "dcim.interface"),
    "dcim.cable": ("dcim.device", "dcim.interface"),
    "netbox_routing.ospfinstance": ("dcim.device", "ipam.vrf"),
    "netbox_routing.ospfinterface": (
        "dcim.device",
        "dcim.interface",
        "netbox_routing.ospfinstance",
    ),
    "netbox_dlm.softwareversion": ("dcim.platform",),
    "netbox_dlm.hardwarenotice": ("dcim.devicetype",),
    # The adapter creates SoftwareVersion and DeviceSoftware atomically.
    "netbox_dlm.devicesoftware": ("dcim.device", "dcim.platform"),
    "netbox_dlm.vulnerability": (
        "dcim.device",
        "netbox_dlm.cve",
        "netbox_dlm.softwareversion",
    ),
    "netbox_routing.bgppeer": ("dcim.device", "ipam.vrf"),
    "netbox_routing.bgpaddressfamily": ("dcim.device", "ipam.vrf"),
    "netbox_routing.bgppeeraddressfamily": (
        "dcim.device",
        "ipam.vrf",
        "netbox_routing.bgppeer",
    ),
    "netbox_peering_manager.peeringsession": (
        "dcim.device",
        "netbox_routing.bgppeer",
    ),
    "netbox_cisco_aci.acipod": ("netbox_cisco_aci.acifabric",),
    "netbox_cisco_aci.acinode": ("netbox_cisco_aci.acipod",),
    "netbox_cisco_aci.acitenant": ("netbox_cisco_aci.acifabric",),
    "netbox_cisco_aci.acivrf": ("netbox_cisco_aci.acitenant",),
    "netbox_cisco_aci.acibridgedomain": ("netbox_cisco_aci.acivrf",),
    "netbox_cisco_aci.acifilter": ("netbox_cisco_aci.acitenant",),
    "netbox_cisco_aci.acil3out": ("netbox_cisco_aci.acitenant",),
}
DELETE_DEPENDENCY_MODEL_ORDER = (
    "netbox_dlm.vulnerability",
    "netbox_dlm.cve",
    "netbox_dlm.devicesoftware",
    "netbox_dlm.hardwarenotice",
    "netbox_dlm.softwareversion",
    "dcim.cable",
    "ipam.fhrpgroup",
    "ipam.ipaddress",
    "dcim.macaddress",
    "netbox_routing.bgppeeraddressfamily",
    "netbox_routing.bgpsetting",
    "netbox_peering_manager.peeringsession",
    "netbox_routing.bgpaddressfamily",
    "netbox_routing.bgppeer",
    "netbox_routing.bgpscope",
    "netbox_routing.bgprouter",
    "netbox_routing.ospfinterface",
    "netbox_routing.ospfinstance",
    "netbox_routing.ospfarea",
    "netbox_cisco_aci.acil3out",
    "netbox_cisco_aci.acifilter",
    "netbox_cisco_aci.acibridgedomain",
    "netbox_cisco_aci.acivrf",
    "netbox_cisco_aci.acitenant",
    "netbox_cisco_aci.acinode",
    "netbox_cisco_aci.acipod",
    "netbox_cisco_aci.acifabric",
    "dcim.interface",
    "dcim.inventoryitem",
    "dcim.module",
    "extras.taggeditem",
    "dcim.device",
    "dcim.virtualchassis",
    "ipam.prefix",
    "ipam.vlan",
    "ipam.vrf",
    "dcim.devicetype",
    "dcim.platform",
    "dcim.devicerole",
    "dcim.manufacturer",
    "dcim.site",
)
DELETE_DEPENDENCY_MODEL_RANK = {
    model_string: index
    for index, model_string in enumerate(DELETE_DEPENDENCY_MODEL_ORDER)
}


def apply_parent_dependency_contracts():
    return {
        model_string: tuple(parent_models)
        for model_string, parent_models in sorted(
            APPLY_PARENT_MODEL_DEPENDENCIES.items()
        )
    }


def apply_dependency_dry_run(enabled_models) -> dict:
    enabled_models = sorted(str(model) for model in (enabled_models or []) if model)
    enabled_model_set = set(enabled_models)
    supported_model_set = set(FORWARD_SUPPORTED_MODELS)
    model_entries = []
    missing_dependencies = []
    for model_string in enabled_models:
        parent_models = tuple(APPLY_PARENT_MODEL_DEPENDENCIES.get(model_string, ()))
        missing_parent_models = [
            parent_model
            for parent_model in parent_models
            if parent_model in supported_model_set
            and parent_model not in enabled_model_set
        ]
        entry = {
            "model": model_string,
            "apply_rank": APPLY_DEPENDENCY_MODEL_RANK.get(model_string),
            "parent_models": list(parent_models),
            "missing_parent_models": missing_parent_models,
            "status": "warn" if missing_parent_models else "pass",
        }
        model_entries.append(entry)
        for parent_model in missing_parent_models:
            missing_dependencies.append(
                {
                    "model": model_string,
                    "parent_model": parent_model,
                    "model_apply_rank": APPLY_DEPENDENCY_MODEL_RANK.get(model_string),
                    "parent_apply_rank": APPLY_DEPENDENCY_MODEL_RANK.get(parent_model),
                    "message": (
                        f"{model_string} is enabled without parent model "
                        f"{parent_model}; child rows may be skipped when the parent "
                        "does not already exist in NetBox."
                    ),
                }
            )
    return {
        "available": True,
        "status": "warn" if missing_dependencies else "pass",
        "message": (
            f"{len(missing_dependencies)} apply dependency gap(s) found."
            if missing_dependencies
            else "Enabled models satisfy declared apply parent dependencies."
        ),
        "enabled_models": enabled_models,
        "model_count": len(model_entries),
        "missing_dependency_count": len(missing_dependencies),
        "models": model_entries,
        "missing_dependencies": missing_dependencies,
    }


DEVICE_SHARD_MODELS = {
    "dcim.cable",
    "dcim.interface",
    "dcim.macaddress",
    "dcim.inventoryitem",
    "dcim.module",
    "extras.taggeditem",
    "ipam.fhrpgroup",
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
    "netbox_cisco_aci.acifabric": ("name",),
    "netbox_cisco_aci.acipod": ("fabric_name", "pod_id", "name"),
    "netbox_cisco_aci.acinode": ("fabric_name", "pod_id", "node_id", "name"),
    "netbox_cisco_aci.acitenant": ("fabric_name", "name"),
    "netbox_cisco_aci.acivrf": ("fabric_name", "tenant_name", "name"),
    "netbox_cisco_aci.acibridgedomain": ("fabric_name", "tenant_name", "name"),
    "netbox_cisco_aci.acifilter": ("fabric_name", "tenant_name", "name"),
    "netbox_cisco_aci.acil3out": ("fabric_name", "tenant_name", "name"),
    **IPAM_SHARD_FILTER_FIELDS,
}

SHARD_FETCH_PARAMETER_MODE = "shard_keys"
SHARD_FETCH_PARAMETER_KEYS = "forward_netbox_shard_keys"
SHARD_FETCH_PARAMETER_MODE_NAME = "forward_netbox_shard_mode"
SHARD_FETCH_PARAMETER_BUCKET = "forward_netbox_shard_bucket"
SHARD_FETCH_PARAMETER_BUCKET_COUNT = "forward_netbox_shard_bucket_count"


def _fallback_bucket_key_family(model_string):
    field_sets = default_coalesce_fields_for_model(model_string)
    if not field_sets:
        return ""
    first = field_sets[0]
    if not first:
        return ""
    return ",".join(str(field_name) for field_name in first if str(field_name))


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
        if model_string == "ipam.fhrpgroup":
            # FHRP groups span devices, so they are bucketed by group identity and
            # fetched as a full model diff (not per-device), letting the diff dedup
            # drop state-flap delete/create churn.
            contracts[model_string] = {
                "model": model_string,
                "fetch_mode": "model",
                "key_family": "fhrp_identity",
                "shard_safe": True,
                "local_safety_filter": True,
                "schema_contract": "same_nqe_row_shape",
                "reason_code": "fhrp_group_identity_bucket",
                "reason": (
                    "FHRP groups span multiple devices (active/standby), so they "
                    "are bucketed by group identity and fetched as a full model "
                    "diff; the diff dedup then drops state-flap churn."
                ),
                "bucket_strategy": {
                    "supported": True,
                    "key_family": "fhrp_identity",
                    "reason_code": "identity_bucket_available",
                    "reason": (
                        "Group identity co-locates both routers of a group in one "
                        "shard for deterministic bucketing."
                    ),
                },
            }
        elif model_string in DEVICE_SHARD_MODELS:
            contracts[model_string] = {
                "model": model_string,
                "fetch_mode": "nqe_parameters",
                "key_family": "device",
                "shard_safe": True,
                "local_safety_filter": True,
                "schema_contract": "same_nqe_row_shape",
                "reason_code": "device_query_parameter",
                "reason": (
                    "Device-scoped rows can be fetched with the built-in query-side "
                    "shard parameter and still receive local shard safety filtering."
                ),
                "bucket_strategy": {
                    "supported": True,
                    "key_family": "device",
                    "reason_code": "device_bucket_available",
                    "reason": "Device shard keys can be bucketed deterministically.",
                },
            }
        elif model_string == "ipam.prefix":
            contracts[model_string] = {
                "model": model_string,
                "fetch_mode": "nqe_parameters",
                "key_family": "prefix",
                "shard_safe": True,
                "local_safety_filter": True,
                "schema_contract": "same_nqe_row_shape",
                "reason_code": "ipam_prefix_query_parameter",
                "reason": (
                    "Prefix rows can be fetched with the built-in query-side "
                    "shard parameter and still receive local shard safety filtering."
                ),
                "bucket_strategy": {
                    "supported": True,
                    "key_family": "prefix",
                    "reason_code": "query_parameter_bucket_available",
                    "reason": (
                        "Prefix shard keys can be passed to the built-in NQE as "
                        "a deterministic shard-key list."
                    ),
                },
            }
        elif model_string in STRUCTURED_SHARD_FILTER_FIELDS:
            contracts[model_string] = {
                "model": model_string,
                "fetch_mode": "nqe_parameters",
                "key_family": ",".join(STRUCTURED_SHARD_FILTER_FIELDS[model_string]),
                "shard_safe": True,
                "local_safety_filter": True,
                "schema_contract": "same_nqe_row_shape",
                "reason_code": (
                    "ipam_query_parameter"
                    if model_string in IPAM_SHARD_FILTER_FIELDS
                    else "structured_query_parameter"
                ),
                "reason": (
                    "Rows can be fetched with the built-in query-side shard "
                    "parameter when the shard key exposes one of the stable "
                    "identity columns."
                ),
                "bucket_strategy": {
                    "supported": True,
                    "key_family": ",".join(
                        STRUCTURED_SHARD_FILTER_FIELDS[model_string]
                    ),
                    "reason_code": "query_parameter_bucket_available",
                    "reason": (
                        "This model has deterministic identity fields suitable for "
                        "query-parameter pushdown and deterministic bucketing."
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
    fetch_mode: str = "model"
    fetch_key_family: str = ""
    fetch_parameters: dict = field(default_factory=dict)
    query_parameters: dict = field(default_factory=dict)
    operation: str = BRANCH_PLAN_OPERATION_MIXED
    shard_keys: tuple[str, ...] = ()

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
    fetch_mode: str = "model"
    fetch_key_family: str = ""
    fetch_parameters: dict = field(default_factory=dict)
    query_parameters: dict = field(default_factory=dict)
    operation: str = BRANCH_PLAN_OPERATION_MIXED


def row_shard_key(model_string, row, coalesce_fields):
    if model_string == "dcim.cable":
        canonical_identity = canonical_cable_endpoint_identity(row)
        if canonical_identity is not None:
            return "cable:" + "|".join(
                f"{device}:{interface}" for device, interface in canonical_identity
            )

    if model_string == "ipam.fhrpgroup":
        # FHRP groups span multiple devices (active/standby routers). Bucket by
        # GROUP identity, not device, so both routers land in one shard — the
        # diff dedup can then pair the state-flap ADD/DELETE and avoid churn.
        return "fhrp:" + "|".join(
            str(row.get(field) or "")
            for field in ("protocol", "group_id", "address", "vrf")
        )

    if model_string in DEVICE_SHARD_MODELS and row.get("device") not in ("", None):
        return f"device:{row['device']}"

    for field_set in coalesce_fields:
        values = []
        for field_name in field_set:
            if not row_coalesce_field_is_complete(model_string, row, field_name):
                values = []
                break
            value = (
                "<global>"
                if model_string == "ipam.prefix"
                and field_name == "vrf"
                and row.get(field_name) in ("", None)
                else row.get(field_name)
            )
            values.append(f"{field_name}={value}")
        if values:
            return "|".join(values)

    if row:
        return "|".join(f"{key}={row[key]}" for key in sorted(row))
    raise ForwardQueryError(f"Unable to derive a shard key for `{model_string}`.")


def shard_key_digest(shard_key):
    return hashlib.sha256(str(shard_key).encode("utf-8")).hexdigest()


def split_workload(
    workload, *, max_row_budget, oversized_bucket_policy="warn"
) -> list[BranchWorkload]:
    if max_row_budget < 1:
        raise ValueError("`max_row_budget` must be at least 1.")
    if oversized_bucket_policy not in {"warn", "fail"}:
        raise ValueError("`oversized_bucket_policy` must be `warn` or `fail`.")
    if workload.estimated_changes <= max_row_budget:
        return [workload]

    buckets = []
    for row_kind, rows in (
        ("upsert_rows", workload.upsert_rows),
        ("delete_rows", workload.delete_rows),
    ):
        grouped = {}
        for index, row in enumerate(rows):
            try:
                key = row_shard_key(
                    workload.model_string, row, workload.coalesce_fields
                )
            except ForwardQueryError:
                key = f"row:{index}"
            grouped.setdefault(key, []).append(row)
        for key, bucket_rows in grouped.items():
            buckets.append((key, row_kind, bucket_rows))

    ordered_buckets = sorted(
        buckets,
        key=lambda item: (-len(item[2]), shard_key_digest(item[0]), item[1]),
    )
    branch_count = math.ceil(workload.estimated_changes / max_row_budget)
    packed = [
        {"upsert_rows": [], "delete_rows": [], "shard_keys": [], "oversized": False}
        for _ in range(branch_count)
    ]
    for key, row_kind, bucket_rows in ordered_buckets:
        bucket_size = len(bucket_rows)
        is_oversized = bucket_size > max_row_budget
        if is_oversized:
            message = (
                f"`{workload.model_string}` shard bucket `{key}` has {bucket_size} "
                f"rows, exceeding the staging-item budget of {max_row_budget}."
            )
            if oversized_bucket_policy == "fail":
                raise ForwardQueryError(message)
            logger.warning(message)

        candidate = None
        if not is_oversized:
            for chunk in sorted(
                packed,
                key=lambda item: len(item["upsert_rows"]) + len(item["delete_rows"]),
            ):
                chunk_size = len(chunk["upsert_rows"]) + len(chunk["delete_rows"])
                if (
                    not chunk["oversized"]
                    and chunk_size + bucket_size <= max_row_budget
                ):
                    candidate = chunk
                    break
        if candidate is None:
            candidate = {
                "upsert_rows": [],
                "delete_rows": [],
                "shard_keys": [],
                "oversized": is_oversized,
            }
            packed.append(candidate)
        candidate[row_kind].extend(bucket_rows)
        candidate["shard_keys"].append(key)
        candidate["oversized"] = candidate["oversized"] or is_oversized

    return [
        BranchWorkload(
            model_string=workload.model_string,
            label=workload.label,
            upsert_rows=chunk["upsert_rows"],
            delete_rows=chunk["delete_rows"],
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
            fetch_mode=workload.fetch_mode,
            fetch_key_family=workload.fetch_key_family,
            fetch_parameters=workload.fetch_parameters,
            query_parameters=workload.query_parameters,
            operation=workload.operation,
            shard_keys=tuple(sorted(chunk["shard_keys"])),
        )
        for chunk in packed
        if chunk["upsert_rows"] or chunk["delete_rows"]
    ]


def shard_fetch_contract(model_string, shard_keys):
    shard_keys = tuple(sorted(str(key) for key in shard_keys or () if key))
    if not shard_keys:
        return {
            "fetch_mode": "model",
            "fetch_key_family": "",
            "fetch_parameters": {},
        }

    cable_device_names = _cable_device_names_from_shard_keys(shard_keys)
    if cable_device_names and model_string == "dcim.cable":
        fetch_parameters = {
            SHARD_FETCH_PARAMETER_KEYS: list(cable_device_names),
        }
        return {
            "fetch_mode": "nqe_parameters",
            "fetch_key_family": "device",
            "fetch_parameters": fetch_parameters,
            "query_parameters": {},
        }

    device_names = _device_names_from_shard_keys(shard_keys)
    if (
        device_names
        and model_string in DEVICE_SHARD_MODELS
        and model_string != "ipam.fhrpgroup"
    ):
        fetch_parameters = {
            SHARD_FETCH_PARAMETER_KEYS: list(device_names),
        }
        return {
            "fetch_mode": "nqe_parameters",
            "fetch_key_family": "device",
            "fetch_parameters": fetch_parameters,
            "query_parameters": {},
        }

    structured_contract = _structured_parameter_contract(model_string, shard_keys)
    if structured_contract:
        return structured_contract

    return {
        "fetch_mode": "model",
        "fetch_key_family": "",
        "fetch_parameters": {},
        "query_parameters": {},
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


def _cable_device_names_from_shard_keys(shard_keys):
    device_names = set()
    for key in shard_keys:
        key = str(key)
        if not key.startswith("cable:"):
            return []
        endpoints = key.removeprefix("cable:").split("|")
        if not endpoints:
            return []
        first_endpoint = endpoints[0]
        if ":" not in first_endpoint:
            return []
        device_name, _interface_name = first_endpoint.split(":", 1)
        if not device_name:
            return []
        device_names.add(device_name)
    return sorted(device_names)


def _structured_parameter_contract(model_string, shard_keys):
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
        if model_string == "ipam.prefix" and field_name == "prefix":
            return {
                "fetch_key_family": field_name,
                "fetch_parameters": {
                    SHARD_FETCH_PARAMETER_KEYS: list(unique_values),
                },
                "query_parameters": {},
                "fetch_mode": "nqe_parameters",
            }
        return {
            "fetch_key_family": field_name,
            "fetch_parameters": {
                SHARD_FETCH_PARAMETER_KEYS: list(unique_values),
            },
            "fetch_mode": "nqe_parameters",
        }
    return {}


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


def dependency_phased_workloads(workloads):
    apply_workloads = []
    delete_workloads = []
    for position, workload in enumerate(workloads):
        if workload.upsert_rows:
            apply_workloads.append(
                (
                    position,
                    _workload_for_operation(
                        workload,
                        upsert_rows=workload.upsert_rows,
                        delete_rows=[],
                        operation=BRANCH_PLAN_OPERATION_APPLY,
                    ),
                )
            )
        if workload.delete_rows:
            delete_workloads.append(
                (
                    position,
                    _workload_for_operation(
                        workload,
                        upsert_rows=[],
                        delete_rows=workload.delete_rows,
                        operation=BRANCH_PLAN_OPERATION_DELETE,
                    ),
                )
            )

    ordered_applies = sorted(
        apply_workloads,
        key=lambda item: (
            APPLY_DEPENDENCY_MODEL_RANK.get(item[1].model_string, 10_000),
            item[0],
        ),
    )
    ordered_deletes = sorted(
        delete_workloads,
        key=lambda item: (
            DELETE_DEPENDENCY_MODEL_RANK.get(item[1].model_string, 10_000),
            item[0],
        ),
    )
    return [item[1] for item in ordered_applies] + [item[1] for item in ordered_deletes]


def _workload_for_operation(workload, *, upsert_rows, delete_rows, operation):
    return BranchWorkload(
        model_string=workload.model_string,
        label=(
            f"{workload.label} deletes"
            if operation == BRANCH_PLAN_OPERATION_DELETE
            else workload.label
        ),
        upsert_rows=list(upsert_rows),
        delete_rows=list(delete_rows),
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
        fetch_mode=workload.fetch_mode,
        fetch_key_family=workload.fetch_key_family,
        fetch_parameters=workload.fetch_parameters,
        query_parameters=workload.query_parameters,
        operation=operation,
    )


def build_branch_plan(
    workloads, *, max_changes_per_staging_item=None, oversized_bucket_policy="warn"
):
    """Build dependency-phased plan items, split when a row budget is supplied."""
    phased_workloads = dependency_phased_workloads(workloads)
    if max_changes_per_staging_item is None:
        plan_workloads = [(workload, workload.label) for workload in phased_workloads]
    else:
        plan_workloads = []
        for workload in phased_workloads:
            chunks = split_workload(
                workload,
                max_row_budget=max_changes_per_staging_item,
                oversized_bucket_policy=oversized_bucket_policy,
            )
            chunk_count = len(chunks)
            plan_workloads.extend(
                (
                    chunk,
                    (
                        f"{chunk.label} shard {chunk_index}/{chunk_count}"
                        if chunk_count > 1
                        else chunk.label
                    ),
                )
                for chunk_index, chunk in enumerate(chunks, start=1)
            )

    return [
        BranchPlanItem(
            index=index,
            model_string=workload.model_string,
            label=label,
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
            fetch_mode=workload.fetch_mode,
            fetch_key_family=workload.fetch_key_family,
            fetch_parameters=workload.fetch_parameters,
            query_parameters=workload.query_parameters,
            operation=workload.operation,
            shard_keys=workload.shard_keys,
        )
        for index, (workload, label) in enumerate(plan_workloads, start=1)
    ]


def effective_row_budget_for_model(
    model_string,
    *,
    max_changes_per_staging_item,
    model_change_density=None,
    model_change_density_profile=None,
    safety_factor=DEFAULT_DENSITY_SAFETY_FACTOR,
):
    if max_changes_per_staging_item < 1:
        raise ValueError("`max_changes_per_staging_item` must be at least 1.")

    density_policy = _effective_budget_density_policy(
        model_string,
        model_change_density=model_change_density,
        model_change_density_profile=model_change_density_profile,
    )
    density = density_policy.get("density")
    if density is None:
        return max_changes_per_staging_item
    try:
        density_value = float(density)
    except (TypeError, ValueError):
        return max_changes_per_staging_item
    if density_value <= 0:
        return max_changes_per_staging_item

    effective_safety_factor = MODEL_DENSITY_SAFETY_FACTORS.get(
        model_string,
        float(safety_factor),
    )
    row_budget_ceiling = _density_row_budget_ceiling(
        max_changes_per_staging_item,
        density_policy=density_policy,
        density_value=density_value,
        safety_factor=effective_safety_factor,
    )
    scaled_budget = int(
        (max_changes_per_staging_item * float(effective_safety_factor)) / density_value
    )
    return max(1, min(row_budget_ceiling, scaled_budget))


def build_branch_budget_hints(
    model_strings,
    *,
    max_changes_per_staging_item,
    model_change_density=None,
    model_change_density_profile=None,
):
    return {
        model_string: effective_row_budget_for_model(
            model_string,
            max_changes_per_staging_item=max_changes_per_staging_item,
            model_change_density=model_change_density,
            model_change_density_profile=model_change_density_profile,
        )
        for model_string in model_strings
    }


def delete_dependency_plan_summary(plan_items, *, max_changes_per_staging_item):
    total_changes = sum(max(0, int(item.estimated_changes or 0)) for item in plan_items)
    delete_items = [
        item
        for item in plan_items
        if item.operation == BRANCH_PLAN_OPERATION_DELETE or item.delete_rows
    ]
    total_delete_rows = sum(len(item.delete_rows or []) for item in delete_items)
    models = {}
    execution_order = []
    max_delete_shard_changes = 0
    for item in delete_items:
        delete_count = len(item.delete_rows or [])
        if not delete_count:
            continue
        rank = DELETE_DEPENDENCY_MODEL_RANK.get(item.model_string)
        dependent_model_count = _dependent_delete_model_count(item.model_string)
        entry = models.setdefault(
            item.model_string,
            {
                "delete_rows": 0,
                "delete_shards": 0,
                "max_delete_shard_changes": 0,
                "dependency_rank": rank,
                "dependent_model_count": dependent_model_count,
                "reference_blocker_risk": _reference_blocker_risk(
                    dependent_model_count
                ),
                "first_plan_index": item.index,
                "last_plan_index": item.index,
            },
        )
        entry["delete_rows"] += delete_count
        entry["delete_shards"] += 1
        entry["max_delete_shard_changes"] = max(
            entry["max_delete_shard_changes"],
            max(0, int(item.estimated_changes or 0)),
        )
        entry["first_plan_index"] = min(entry["first_plan_index"], item.index)
        entry["last_plan_index"] = max(entry["last_plan_index"], item.index)
        max_delete_shard_changes = max(
            max_delete_shard_changes,
            max(0, int(item.estimated_changes or 0)),
        )
        if item.model_string not in execution_order:
            execution_order.append(item.model_string)

    delete_share = float(total_delete_rows / total_changes) if total_changes else 0.0
    warnings = []
    if total_delete_rows >= DELETE_WAVE_WARNING_ROW_COUNT or (
        total_delete_rows and delete_share >= DELETE_WAVE_WARNING_SHARE
    ):
        warnings.append(
            {
                "code": "delete_wave",
                "severity": "warning",
                "message": (
                    "Delete work is a material share of this plan; review the "
                    "delete summary and dependency-risk models before merge."
                ),
            }
        )
    soft_large_shard = int(
        max(1, int(max_changes_per_staging_item or 1))
        * DELETE_LARGE_SHARD_WARNING_RATIO
    )
    if max_delete_shard_changes >= soft_large_shard:
        warnings.append(
            {
                "code": "large_delete_shard",
                "severity": "warning",
                "message": (
                    "At least one delete staging item is near its budget; "
                    "reference blockers may make the merge slower or noisier."
                ),
            }
        )
    high_risk_models = [
        model
        for model, entry in models.items()
        if entry["reference_blocker_risk"] == "high"
    ]
    if high_risk_models:
        warnings.append(
            {
                "code": "reference_blocker_risk",
                "severity": "warning",
                "models": high_risk_models,
                "message": (
                    "Some deleted models are dependency anchors; unresolved "
                    "references should surface as row issues or merge blockers."
                ),
            }
        )

    status = "none"
    if total_delete_rows:
        status = "high" if warnings else "low"
    return {
        "status": status,
        "delete_rows": total_delete_rows,
        "delete_shards": len(delete_items),
        "delete_model_count": len(models),
        "delete_share": round(delete_share, 4),
        "max_delete_shard_changes": max_delete_shard_changes,
        "execution_order": execution_order,
        "models": models,
        "warnings": warnings,
    }


def _dependent_delete_model_count(model_string):
    if model_string not in DELETE_DEPENDENCY_MODEL_RANK:
        return 0
    rank = DELETE_DEPENDENCY_MODEL_RANK[model_string]
    return sum(
        1
        for candidate, candidate_rank in DELETE_DEPENDENCY_MODEL_RANK.items()
        if candidate != model_string and candidate_rank < rank
    )


def _reference_blocker_risk(dependent_model_count):
    if dependent_model_count >= 5:
        return "high"
    if dependent_model_count > 0:
        return "medium"
    return "low"


def branch_budget_density_policy_summary(
    model_strings,
    *,
    model_change_density=None,
    model_change_density_profile=None,
):
    profile = normalize_density_profile(model_change_density_profile or {})
    return {
        model_string: density_budget_policy(
            model_string,
            learned_density=(model_change_density or {}).get(model_string),
            profile_entry=profile.get(model_string) or {},
            default_density=DEFAULT_MODEL_CHANGE_DENSITY.get(model_string),
        )
        for model_string in model_strings
    }


def _effective_budget_density(
    model_string,
    *,
    model_change_density=None,
    model_change_density_profile=None,
):
    return _effective_budget_density_policy(
        model_string,
        model_change_density=model_change_density,
        model_change_density_profile=model_change_density_profile,
    )["density"]


def _effective_budget_density_policy(
    model_string,
    *,
    model_change_density=None,
    model_change_density_profile=None,
):
    learned_density = (model_change_density or {}).get(model_string)
    default_density = DEFAULT_MODEL_CHANGE_DENSITY.get(model_string)
    profile = normalize_density_profile(model_change_density_profile or {})
    return density_budget_policy(
        model_string,
        learned_density=learned_density,
        profile_entry=profile.get(model_string) or {},
        default_density=default_density,
    )


def _density_row_budget_ceiling(
    max_changes_per_staging_item,
    *,
    density_policy,
    density_value,
    safety_factor,
):
    base = max(1, int(max_changes_per_staging_item))
    policy = str((density_policy or {}).get("policy") or "")
    if policy not in DENSITY_ROW_BUDGET_WIDEN_POLICIES:
        return base
    if float(density_value or 0.0) >= float(safety_factor):
        return base
    return base * DENSITY_ROW_BUDGET_MAX_MULTIPLIER
