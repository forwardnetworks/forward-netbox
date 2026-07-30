from dataclasses import dataclass
from importlib.metadata import PackageNotFoundError
from importlib.metadata import version as distribution_version

from django.conf import settings
from netbox_branching.choices import BranchStatusChoices
from netbox_branching.contextvars import active_branch

from ..choices import FORWARD_SUPPORTED_MODELS
from ..choices import ForwardApplyEngineChoices


BULK_ORM_ENABLED_MODELS = {
    "dcim.site",
    "dcim.manufacturer",
    "dcim.devicerole",
    "dcim.platform",
    "dcim.devicetype",
    "dcim.macaddress",
    "dcim.virtualchassis",
    "ipam.vlan",
    "ipam.vrf",
    # Promoted to the default safe set: both carry adapter-parity tests and
    # compare-before-write (no churn). These are the two highest-volume models,
    # so default bulk gives the largest ingest speedup.
    "dcim.interface",
    "ipam.ipaddress",
    # Promoted: Prefix create/update/delete batches rebuild NetBox's cached
    # hierarchy once per affected VRF and emit branch review evidence in the
    # same transaction. Null-VRF identity and canonical-CIDR lookup are handled
    # in the bulk lookup builders.
    "ipam.prefix",
    # Promoted: highest-volume model. Its parents (site/manufacturer/role/
    # device-type/platform) are bulk-staged immediately before it, so the bulk
    # path resolves them by lookup; rows needing adapter sequencing (missing
    # parent, virtual-chassis membership, opt-in scope tagging) delegate to the
    # adapter for exact parity. Biggest single staging speedup.
    "dcim.device",
}
BULK_ORM_SPEC_MODELS = {
    "dcim.site",
    "dcim.manufacturer",
    "dcim.devicerole",
    "dcim.platform",
    "dcim.devicetype",
    "dcim.macaddress",
    "dcim.virtualchassis",
    "ipam.vlan",
    "ipam.vrf",
    "ipam.prefix",
    "ipam.ipaddress",
    "dcim.interface",
    "dcim.device",
}

# COPY/SQL is deliberately narrower than the bulk-ORM safe set. Each entry is a
# versioned executable contract, not a claim that arbitrary scalar models are
# safe for raw DML or SQL-generated branch evidence.
COPY_SQL_MODEL_SPEC_VERSIONS = {
    "dcim.macaddress": 1,
}
COPY_SQL_ALLOWED_MODELS = frozenset(COPY_SQL_MODEL_SPEC_VERSIONS)
COPY_SQL_SUPPORTED_NETBOX_VERSION = "4.6.5"
COPY_SQL_SUPPORTED_BRANCHING_VERSION = "1.1.1"
COPY_SQL_SUPPORTED_OPTIONAL_DISTRIBUTIONS = {
    "netbox-cisco-aci": "0.4.0",
    "netbox-dlm": "0.4.1",
    "netbox-peering-manager": "0.3.0",
    "netbox-routing": "0.4.3",
}
COPY_SQL_SUPPORTED_PLUGIN_APPS = frozenset(
    {
        "forward_netbox",
        "netbox_branching",
        "netbox_cisco_aci",
        "netbox_dlm",
        "netbox_peering_manager",
        "netbox_routing",
    }
)
ADAPTER_REQUIRED_MODELS = set(FORWARD_SUPPORTED_MODELS) - BULK_ORM_ENABLED_MODELS

ADAPTER_MODEL_BLOCKERS = {
    "netbox_dlm.softwareversion": {
        "blocker_code": "plugin_model_dependencies",
        "blocker_reason": (
            "DLM software-version writes require plugin model and platform "
            "dependency resolution handled by the adapter."
        ),
    },
    "netbox_dlm.hardwarenotice": {
        "blocker_code": "plugin_model_dependencies",
        "blocker_reason": (
            "DLM hardware-notice writes require plugin model and device-type "
            "dependency resolution handled by the adapter."
        ),
    },
    "netbox_dlm.devicesoftware": {
        "blocker_code": "plugin_model_dependencies",
        "blocker_reason": (
            "DLM device-software writes require device and software-version "
            "dependency resolution handled by the adapter."
        ),
    },
    "netbox_dlm.cve": {
        "blocker_code": "plugin_model_dependencies",
        "blocker_reason": (
            "DLM CVE writes require plugin model resolution and unique cve_id "
            "upsert handled by the adapter."
        ),
    },
    "netbox_dlm.vulnerability": {
        "blocker_code": "plugin_model_dependencies",
        "blocker_reason": (
            "DLM vulnerability writes require cve, software-version, and device "
            "foreign-key resolution handled by the adapter."
        ),
    },
    "dcim.cable": {
        "blocker_code": "relationship_identity_directionality",
        "blocker_reason": (
            "Cable upserts require direction-insensitive identity handling and "
            "existing-link conflict checks that currently live in adapter logic."
        ),
    },
    "dcim.inventoryitem": {
        "blocker_code": "dependency_resolution",
        "blocker_reason": (
            "Inventory item writes require device/interface anchoring and optional "
            "module/component reconciliation handled by adapters."
        ),
    },
    "dcim.module": {
        "blocker_code": "dependency_resolution",
        "blocker_reason": (
            "Module writes depend on module-bay/type readiness and guarded "
            "dependency skips managed by adapter workflow."
        ),
    },
    "extras.taggeditem": {
        "blocker_code": "generic_foreign_key_relations",
        "blocker_reason": (
            "Tagged item writes use generic relation semantics and adapter-level "
            "dedupe/skip behavior."
        ),
    },
    "ipam.fhrpgroup": {
        "blocker_code": "generic_foreign_key_relations",
        "blocker_reason": (
            "FHRP group writes create group assignments and VIP generic relations "
            "that require adapter sequencing and row-level dependency handling."
        ),
    },
    "netbox_peering_manager.peeringsession": {
        "blocker_code": "plugin_model_dependencies",
        "blocker_reason": (
            "Peering session writes require plugin-object dependency checks and "
            "tolerant row-level error handling."
        ),
    },
    "netbox_routing.bgpaddressfamily": {
        "blocker_code": "plugin_model_dependencies",
        "blocker_reason": (
            "Routing address-family writes require plugin object dependency "
            "resolution and adapter sequencing."
        ),
    },
    "netbox_routing.bgppeer": {
        "blocker_code": "plugin_model_dependencies",
        "blocker_reason": (
            "BGP peer writes require device/ASN/plugin dependency resolution and "
            "adapter-side guarded creation semantics."
        ),
    },
    "netbox_routing.bgppeeraddressfamily": {
        "blocker_code": "plugin_model_dependencies",
        "blocker_reason": (
            "BGP peer address-family writes require prior peer resolution and "
            "plugin dependency sequencing."
        ),
    },
    "netbox_routing.ospfarea": {
        "blocker_code": "plugin_model_dependencies",
        "blocker_reason": (
            "OSPF area writes require plugin-model dependency checks and adapter "
            "guard behavior."
        ),
    },
    "netbox_routing.ospfinstance": {
        "blocker_code": "plugin_model_dependencies",
        "blocker_reason": (
            "OSPF instance writes require routing plugin dependency handling and "
            "row-level guarded apply semantics."
        ),
    },
    "netbox_routing.ospfinterface": {
        "blocker_code": "plugin_model_dependencies",
        "blocker_reason": (
            "OSPF interface writes depend on instance/area relationships and "
            "adapter sequencing guarantees."
        ),
    },
    "netbox_cisco_aci.acifabric": {
        "blocker_code": "plugin_model_dependencies",
        "blocker_reason": (
            "ACI fabric writes target an optional plugin and must preserve "
            "plugin validation, ContentType detection, and adapter-managed "
            "row issue behavior."
        ),
    },
    "netbox_cisco_aci.acipod": {
        "blocker_code": "plugin_model_dependencies",
        "blocker_reason": (
            "ACI pod writes depend on prior ACI fabric resolution and optional "
            "plugin model validation."
        ),
    },
    "netbox_cisco_aci.acinode": {
        "blocker_code": "plugin_model_dependencies",
        "blocker_reason": (
            "ACI node writes depend on ACI pod resolution and optional native "
            "dcim.Device linkage through a generic foreign key."
        ),
    },
    "netbox_cisco_aci.acitenant": {
        "blocker_code": "plugin_model_dependencies",
        "blocker_reason": "ACI tenant writes depend on prior ACI fabric resolution.",
    },
    "netbox_cisco_aci.acivrf": {
        "blocker_code": "plugin_model_dependencies",
        "blocker_reason": "ACI VRF writes depend on ACI tenant resolution.",
    },
    "netbox_cisco_aci.acibridgedomain": {
        "blocker_code": "plugin_model_dependencies",
        "blocker_reason": "ACI bridge-domain writes depend on tenant and VRF resolution.",
    },
    "netbox_cisco_aci.acifilter": {
        "blocker_code": "plugin_model_dependencies",
        "blocker_reason": "ACI filter writes depend on ACI tenant resolution.",
    },
    "netbox_cisco_aci.acil3out": {
        "blocker_code": "plugin_model_dependencies",
        "blocker_reason": "ACI L3Out writes depend on tenant and VRF resolution.",
    },
}

APPLY_ENGINE_MODEL_CLASSIFICATIONS = {
    **{model_string: "bulk_orm" for model_string in BULK_ORM_ENABLED_MODELS},
    **{model_string: "adapter_required" for model_string in ADAPTER_REQUIRED_MODELS},
}

UNCLASSIFIED_SUPPORTED_MODELS = tuple(
    model_string
    for model_string in FORWARD_SUPPORTED_MODELS
    if model_string not in APPLY_ENGINE_MODEL_CLASSIFICATIONS
)

ADAPTER_MODELS_WITHOUT_BLOCKER = tuple(
    model_string
    for model_string in ADAPTER_REQUIRED_MODELS
    if model_string not in ADAPTER_MODEL_BLOCKERS
)

BULK_ORM_ENABLED_MODELS_WITHOUT_SPECS = tuple(
    model_string
    for model_string in BULK_ORM_ENABLED_MODELS
    if model_string not in BULK_ORM_SPEC_MODELS
)


@dataclass(frozen=True)
class ForwardApplyEngineDecision:
    model_string: str
    selected_engine: str
    reason_code: str
    reason: str
    available_engines: tuple[str, ...]
    rejected_engines: tuple[dict, ...]
    bulk_orm_safe: bool = False
    copy_sql_safe: bool = False

    def as_dict(self):
        return {
            "model": self.model_string,
            "selected_engine": self.selected_engine,
            "reason_code": self.reason_code,
            "reason": self.reason,
            "available_engines": list(self.available_engines),
            "rejected_engines": list(self.rejected_engines),
            "bulk_orm_safe": self.bulk_orm_safe,
            "copy_sql_safe": self.copy_sql_safe,
        }


def _bulk_orm_enabled_state(sync_parameters):
    raw_value = sync_parameters.get("enable_bulk_orm")
    if raw_value in ("", None):
        auto_enabled = True
        return auto_enabled, auto_enabled
    if isinstance(raw_value, str):
        normalized = raw_value.strip().lower()
        if normalized in {"true", "1", "yes", "on"}:
            return True, False
        if normalized in {"false", "0", "no", "off"}:
            return False, False
    return bool(raw_value), False


def _copy_sql_enabled_state(sync_parameters):
    raw_value = sync_parameters.get("enable_copy_sql", False)
    if isinstance(raw_value, str):
        return raw_value.strip().lower() in {"true", "1", "yes", "on"}
    return bool(raw_value)


def _copy_sql_kill_switches(sync_parameters):
    raw_value = sync_parameters.get("copy_sql_kill_switches") or []
    if isinstance(raw_value, str):
        raw_value = [part.strip() for part in raw_value.split(",")]
    if not isinstance(raw_value, (list, tuple, set, frozenset)):
        return frozenset()
    return frozenset(str(value).strip() for value in raw_value if str(value).strip())


def _installed_distribution_version(distribution):
    try:
        return distribution_version(distribution)
    except PackageNotFoundError:
        return None


def copy_sql_runtime_version_tuple():
    """Return the exact runtime tuple consumed by COPY/SQL selection."""
    release = getattr(settings, "RELEASE", None)
    netbox_version = getattr(release, "version", None) or getattr(
        settings, "VERSION", ""
    )
    return (
        str(netbox_version or ""),
        _installed_distribution_version("netboxlabs-netbox-branching"),
        tuple(
            (distribution, _installed_distribution_version(distribution))
            for distribution in sorted(COPY_SQL_SUPPORTED_OPTIONAL_DISTRIBUTIONS)
        ),
    )


def _copy_sql_runtime_supported():
    netbox_version, branching_version, optional_versions = (
        copy_sql_runtime_version_tuple()
    )
    if netbox_version != COPY_SQL_SUPPORTED_NETBOX_VERSION:
        return (
            False,
            "unsupported_netbox_version",
            {
                "expected": COPY_SQL_SUPPORTED_NETBOX_VERSION,
                "actual": netbox_version,
            },
        )
    if branching_version != COPY_SQL_SUPPORTED_BRANCHING_VERSION:
        return (
            False,
            "unsupported_branching_version",
            {
                "expected": COPY_SQL_SUPPORTED_BRANCHING_VERSION,
                "actual": branching_version,
            },
        )
    for distribution, actual in optional_versions:
        expected = COPY_SQL_SUPPORTED_OPTIONAL_DISTRIBUTIONS[distribution]
        # Absence is also a different runtime tuple.  WP-A was proved with this
        # exact optional-plugin set; any other installation fails closed.
        if actual != expected:
            return (
                False,
                "unsupported_optional_plugin_version",
                {
                    "distribution": distribution,
                    "expected": expected,
                    "actual": actual,
                },
            )
    actual_plugin_apps = frozenset(getattr(settings, "PLUGINS", ()) or ())
    if actual_plugin_apps != COPY_SQL_SUPPORTED_PLUGIN_APPS:
        return (
            False,
            "unsupported_plugin_app_tuple",
            {
                "expected": sorted(COPY_SQL_SUPPORTED_PLUGIN_APPS),
                "actual": sorted(actual_plugin_apps),
            },
        )
    return (
        True,
        "supported_exact_runtime_tuple",
        {
            "netbox": netbox_version,
            "branching": branching_version,
            "optional_plugins": dict(optional_versions),
        },
    )


def _active_provisioned_copy_sql_branch():
    branch = active_branch.get()
    if branch is None:
        return None, "active_branch_required"
    if getattr(branch, "status", None) != BranchStatusChoices.READY:
        return None, "provisioned_ready_branch_required"
    if not getattr(branch, "schema_name", None) or not getattr(
        branch, "connection_name", None
    ):
        return None, "provisioned_branch_schema_required"
    # A registered field migrator changes the JSON comparison/replay contract.
    # MAC has none on the supported tuple; fail closed if a runtime adds one.
    if getattr(branch, "migrators", {}).get("dcim.macaddress"):
        return None, "objectchange_field_migrator_present"
    return branch, "active_provisioned_branch"


def _copy_sql_rejection(*, model_string, sync_parameters):
    if model_string not in COPY_SQL_ALLOWED_MODELS:
        return "model_not_allowlisted", {
            "allowlist": sorted(COPY_SQL_ALLOWED_MODELS),
        }
    if model_string in _copy_sql_kill_switches(sync_parameters):
        return "model_kill_switch_set", {"model": model_string}
    runtime_supported, reason_code, context = _copy_sql_runtime_supported()
    if not runtime_supported:
        return reason_code, context
    branch, branch_reason = _active_provisioned_copy_sql_branch()
    if branch is None:
        return branch_reason, {}
    return None, {
        **context,
        "branch_id": getattr(branch, "pk", None),
        "model_spec_version": COPY_SQL_MODEL_SPEC_VERSIONS[model_string],
    }


def apply_engine_decision_for(*, sync, model_string, consider_copy_sql=True):
    model_string = str(model_string or "")
    sync_parameters = getattr(sync, "parameters", None) or {}
    bulk_orm_enabled, bulk_orm_auto_enabled = _bulk_orm_enabled_state(sync_parameters)
    rejected_engines = ()
    if consider_copy_sql and _copy_sql_enabled_state(sync_parameters):
        copy_rejection, copy_context = _copy_sql_rejection(
            model_string=model_string,
            sync_parameters=sync_parameters,
        )
        if copy_rejection is None:
            available = [ForwardApplyEngineChoices.ADAPTER]
            if model_string in BULK_ORM_ENABLED_MODELS:
                available.append(ForwardApplyEngineChoices.BULK_ORM)
            available.append(ForwardApplyEngineChoices.COPY_SQL)
            return ForwardApplyEngineDecision(
                model_string=model_string,
                selected_engine=ForwardApplyEngineChoices.COPY_SQL,
                reason_code="copy_sql_enabled_exact_model_spec",
                reason=(
                    "Using branch-only COPY/SQL for an explicitly enabled, "
                    "version-pinned model specification."
                ),
                available_engines=tuple(available),
                rejected_engines=(),
                bulk_orm_safe=model_string in BULK_ORM_ENABLED_MODELS,
                copy_sql_safe=True,
            )
        rejected_engines = (
            {
                "engine": ForwardApplyEngineChoices.COPY_SQL,
                "reason_code": copy_rejection,
                "reason": (
                    "COPY/SQL failed a branch, version, allowlist, or model "
                    "kill-switch safety gate; using the existing engine."
                ),
                "context": copy_context,
            },
        )
    if model_string in BULK_ORM_ENABLED_MODELS:
        if model_string in BULK_ORM_ENABLED_MODELS_WITHOUT_SPECS:
            return ForwardApplyEngineDecision(
                model_string=model_string,
                selected_engine=ForwardApplyEngineChoices.ADAPTER,
                reason_code="bulk_orm_enabled_model_missing_spec",
                reason=(
                    "Using adapter engine because this model is marked bulk-ORM "
                    "enabled but has no implemented bulk specification."
                ),
                available_engines=(ForwardApplyEngineChoices.ADAPTER,),
                rejected_engines=(
                    {
                        "engine": ForwardApplyEngineChoices.BULK_ORM,
                        "reason_code": "missing_bulk_model_spec",
                        "reason": (
                            "Define and parity-test the bulk ORM model spec before "
                            "enabling this model."
                        ),
                    },
                    *rejected_engines,
                ),
                bulk_orm_safe=False,
            )
        if not bulk_orm_enabled:
            return ForwardApplyEngineDecision(
                model_string=model_string,
                selected_engine=ForwardApplyEngineChoices.ADAPTER,
                reason_code="bulk_orm_disabled_by_default",
                reason=(
                    "Using adapter engine by default. Enable `enable_bulk_orm` "
                    "on the sync to run bulk ORM for eligible models."
                ),
                available_engines=(ForwardApplyEngineChoices.ADAPTER,),
                rejected_engines=(
                    {
                        "engine": ForwardApplyEngineChoices.BULK_ORM,
                        "reason_code": "feature_flag_disabled",
                        "reason": (
                            "Bulk ORM is disabled unless explicitly enabled per sync."
                        ),
                    },
                    *rejected_engines,
                ),
                bulk_orm_safe=False,
            )
        return ForwardApplyEngineDecision(
            model_string=model_string,
            selected_engine=ForwardApplyEngineChoices.BULK_ORM,
            reason_code=(
                "bulk_orm_auto_enabled_safe_model_set"
                if bulk_orm_auto_enabled
                else "bulk_orm_enabled_safe_model_set"
            ),
            reason=(
                "Using bulk ORM automatically for the parity-tested safe "
                "model set because no explicit sync override was provided."
                if bulk_orm_auto_enabled
                else "Using bulk ORM for a narrow safe model set with scalar "
                "fields and deterministic coalesce identity."
            ),
            available_engines=(
                ForwardApplyEngineChoices.ADAPTER,
                ForwardApplyEngineChoices.BULK_ORM,
            ),
            rejected_engines=rejected_engines,
            bulk_orm_safe=True,
        )
    if model_string in ADAPTER_REQUIRED_MODELS:
        blocker = ADAPTER_MODEL_BLOCKERS.get(
            model_string,
            {
                "blocker_code": "adapter_contract_required",
                "blocker_reason": (
                    "Model-specific adapter behavior is required until a dedicated "
                    "bulk ORM parity proof exists."
                ),
            },
        )
        return ForwardApplyEngineDecision(
            model_string=model_string,
            selected_engine=ForwardApplyEngineChoices.ADAPTER,
            reason_code="adapter_required_model_contract",
            reason=(
                "Using the adapter engine because this model has model-specific "
                "validation, dependency handling, relationship side effects, or "
                "row-level skip behavior that must stay on the native adapter path."
            ),
            available_engines=(ForwardApplyEngineChoices.ADAPTER,),
            rejected_engines=(
                {
                    "engine": ForwardApplyEngineChoices.BULK_ORM,
                    "reason_code": "model_contract_requires_adapter",
                    "blocker_code": blocker["blocker_code"],
                    "reason": blocker["blocker_reason"],
                },
                *rejected_engines,
            ),
            bulk_orm_safe=False,
        )
    return ForwardApplyEngineDecision(
        model_string=model_string,
        selected_engine=ForwardApplyEngineChoices.ADAPTER,
        reason_code="adapter_supported_model_contract",
        reason=(
            "Using the adapter engine as the supported path for this model's "
            "NetBox validation and relationship contract."
        ),
        available_engines=(ForwardApplyEngineChoices.ADAPTER,),
        rejected_engines=(
            {
                "engine": ForwardApplyEngineChoices.BULK_ORM,
                "reason_code": "model_contract_requires_adapter",
                "reason": (
                    "This model's supported architecture uses the adapter path."
                ),
            },
            *rejected_engines,
        ),
        bulk_orm_safe=False,
    )


def apply_engine_decision_summary(*, sync, model_string):
    return apply_engine_decision_for(sync=sync, model_string=model_string).as_dict()
