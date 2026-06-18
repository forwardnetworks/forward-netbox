from dataclasses import dataclass

from ..choices import FORWARD_SUPPORTED_MODELS
from ..choices import ForwardApplyEngineChoices
from ..choices import ForwardExecutionBackendChoices


SIMPLE_BULK_CANDIDATE_MODELS = {
    "dcim.site",
    "dcim.manufacturer",
    "dcim.devicerole",
    "dcim.platform",
    "dcim.devicetype",
    "dcim.macaddress",
    "dcim.virtualchassis",
    "ipam.vlan",
    "ipam.vrf",
}

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
}
EXPERIMENTAL_BULK_ORM_MODELS = {"ipam.prefix", "ipam.ipaddress", "dcim.interface"}

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
}
BULK_ORM_PARITY_GATES = (
    {
        "code": "netbox_validation_parity",
        "description": (
            "Bulk writes must call the same NetBox validation contract and reject "
            "the same invalid rows as the adapter path."
        ),
    },
    {
        "code": "object_change_tracking_parity",
        "description": (
            "Bulk writes must preserve NetBox object-change visibility expected by "
            "operators and Branching review."
        ),
    },
    {
        "code": "branching_semantics_parity",
        "description": (
            "Bulk writes must produce equivalent Branching diffs, merge behavior, "
            "and rollback/discard behavior."
        ),
    },
    {
        "code": "row_issue_parity",
        "description": (
            "Bulk writes must preserve per-row skip/failure behavior and issue "
            "reporting."
        ),
    },
    {
        "code": "runtime_non_regression",
        "description": (
            "Bulk writes must show equal or better runtime on synthetic and large "
            "runtime evidence before default enablement."
        ),
    },
)

BULK_ORM_PARITY_CHECKLIST = (
    {
        "code": "create_parity",
        "description": "Adapter and bulk engines create the same NetBox objects.",
    },
    {
        "code": "update_parity",
        "description": "Adapter and bulk engines update the same fields.",
    },
    {
        "code": "delete_parity",
        "description": "Adapter and bulk engines remove or skip the same rows.",
    },
    {
        "code": "validation_failure_parity",
        "description": "Invalid rows fail or skip with equivalent issue records.",
    },
    {
        "code": "row_issue_parity",
        "description": "Per-row warnings, skips, and failures remain equivalent.",
    },
    {
        "code": "dependency_behavior_parity",
        "description": "Missing dependencies and guarded skips behave equivalently.",
    },
    {
        "code": "object_change_tracking_parity",
        "description": "NetBox object-change visibility is preserved.",
    },
    {
        "code": "branching_semantics_parity",
        "description": "Branch diffs, merge, discard, and rollback behavior match.",
    },
    {
        "code": "support_bundle_statistics_parity",
        "description": "Statistics and issue summaries remain operator-equivalent.",
    },
    {
        "code": "runtime_non_regression",
        "description": "Synthetic and large-run evidence is equal or faster.",
    },
)

BLOCKER_PROMOTION_LANES = {
    "optional_contract_guarding": {
        "lane": "optional_noop_contracts",
        "priority": 1,
        "risk": "medium",
        "message": (
            "Optional/no-op model contracts are the smallest next parity target, "
            "but must prove absent-data and guarded-skip behavior first."
        ),
        "required_gate": "optional_contract_parity",
    },
    "dependency_resolution": {
        "lane": "dependency_anchored_models",
        "priority": 2,
        "risk": "high",
        "message": (
            "Dependency-anchored models can improve large baseline speed, but "
            "bulk writes must preserve staged object resolution and guarded skips."
        ),
        "required_gate": "dependency_resolution_parity",
    },
    "plugin_model_dependencies": {
        "lane": "plugin_models",
        "priority": 3,
        "risk": "high",
        "message": (
            "Plugin models need plugin-presence, dependency, and tolerant row-error "
            "parity before bulk promotion."
        ),
        "required_gate": "plugin_dependency_parity",
    },
    "relationship_side_effects": {
        "lane": "relationship_side_effect_models",
        "priority": 4,
        "risk": "high",
        "message": (
            "Relationship-heavy models require side-effect parity before any bulk "
            "promotion."
        ),
        "required_gate": "relationship_side_effect_parity",
    },
    "relationship_identity_directionality": {
        "lane": "relationship_side_effect_models",
        "priority": 4,
        "risk": "high",
        "message": (
            "Direction-insensitive relationship identity requires explicit parity "
            "before bulk promotion."
        ),
        "required_gate": "relationship_identity_parity",
    },
    "generic_foreign_key_relations": {
        "lane": "relationship_side_effect_models",
        "priority": 4,
        "risk": "high",
        "message": (
            "Generic relation writes require content-type identity and dedupe parity "
            "before bulk promotion."
        ),
        "required_gate": "generic_relation_parity",
    },
    "ipam_parent_prefix_semantics": {
        "lane": "ipam_hierarchy_models",
        "priority": 5,
        "risk": "high",
        "message": (
            "IPAM writes must preserve hierarchy, parent-prefix, and guarded skip "
            "semantics before bulk promotion."
        ),
        "required_gate": "ipam_hierarchy_parity",
    },
    "ipam_hierarchy_semantics": {
        "lane": "ipam_hierarchy_models",
        "priority": 5,
        "risk": "high",
        "message": (
            "IPAM writes must preserve hierarchy, parent-prefix, and guarded skip "
            "semantics before bulk promotion."
        ),
        "required_gate": "ipam_hierarchy_parity",
    },
}

BULK_ORM_PERFORMANCE_IMPACT_PRIORITY = {
    "dcim.device": 1,
    "dcim.interface": 2,
    "ipam.ipaddress": 3,
    "ipam.prefix": 4,
    "dcim.cable": 5,
    "extras.taggeditem": 6,
    "dcim.inventoryitem": 7,
    "dcim.module": 8,
    "dcim.macaddress": 9,
}

ADAPTER_REQUIRED_MODELS = {
    "dcim.cable",
    "dcim.device",
    "dcim.inventoryitem",
    "dcim.module",
    "extras.taggeditem",
    "ipam.fhrpgroup",
    "ipam.prefix",
    "netbox_peering_manager.peeringsession",
    "netbox_routing.bgpaddressfamily",
    "netbox_routing.bgppeer",
    "netbox_routing.bgppeeraddressfamily",
    "netbox_routing.ospfarea",
    "netbox_routing.ospfinstance",
    "netbox_routing.ospfinterface",
    "netbox_cisco_aci.acifabric",
    "netbox_cisco_aci.acipod",
    "netbox_cisco_aci.acinode",
    "netbox_cisco_aci.acitenant",
    "netbox_cisco_aci.acivrf",
    "netbox_cisco_aci.acibridgedomain",
    "netbox_cisco_aci.aciappprofile",
    "netbox_cisco_aci.aciendpointgroup",
    "netbox_cisco_aci.acicontract",
    "netbox_cisco_aci.acifilter",
    "netbox_cisco_aci.acil3out",
    "netbox_cisco_aci.acistaticportbinding",
}

ADAPTER_MODEL_BLOCKERS = {
    "dcim.cable": {
        "blocker_code": "relationship_identity_directionality",
        "blocker_reason": (
            "Cable upserts require direction-insensitive identity handling and "
            "existing-link conflict checks that currently live in adapter logic."
        ),
    },
    "dcim.device": {
        "blocker_code": "dependency_resolution",
        "blocker_reason": (
            "Device writes depend on staged manufacturer/site/device-type/role "
            "resolution and model-level validation sequencing."
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
    "ipam.prefix": {
        "blocker_code": "ipam_hierarchy_semantics",
        "blocker_reason": (
            "Prefix writes include hierarchy/relationship semantics and skip paths "
            "that are currently enforced in adapters."
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
    "netbox_cisco_aci.aciappprofile": {
        "blocker_code": "plugin_model_dependencies",
        "blocker_reason": "ACI application-profile writes depend on ACI tenant resolution.",
    },
    "netbox_cisco_aci.aciendpointgroup": {
        "blocker_code": "plugin_model_dependencies",
        "blocker_reason": (
            "ACI endpoint-group writes depend on application profile and bridge-domain "
            "resolution."
        ),
    },
    "netbox_cisco_aci.acicontract": {
        "blocker_code": "plugin_model_dependencies",
        "blocker_reason": "ACI contract writes depend on ACI tenant resolution.",
    },
    "netbox_cisco_aci.acifilter": {
        "blocker_code": "plugin_model_dependencies",
        "blocker_reason": "ACI filter writes depend on ACI tenant resolution.",
    },
    "netbox_cisco_aci.acil3out": {
        "blocker_code": "plugin_model_dependencies",
        "blocker_reason": "ACI L3Out writes depend on tenant and VRF resolution.",
    },
    "netbox_cisco_aci.acistaticportbinding": {
        "blocker_code": "plugin_model_dependencies",
        "blocker_reason": (
            "ACI static-port binding writes depend on EPG and native NetBox "
            "interface resolution."
        ),
    },
}

APPLY_ENGINE_MODEL_CLASSIFICATIONS = {
    **{
        model_string: "bulk_orm_candidate"
        for model_string in SIMPLE_BULK_CANDIDATE_MODELS
    },
    **{
        model_string: "bulk_orm_experimental_candidate"
        for model_string in EXPERIMENTAL_BULK_ORM_MODELS
    },
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
    backend: str
    selected_engine: str
    reason_code: str
    reason: str
    available_engines: tuple[str, ...]
    rejected_engines: tuple[dict, ...]
    bulk_orm_safe: bool = False

    def as_dict(self):
        return {
            "model": self.model_string,
            "backend": self.backend,
            "selected_engine": self.selected_engine,
            "reason_code": self.reason_code,
            "reason": self.reason,
            "available_engines": list(self.available_engines),
            "rejected_engines": list(self.rejected_engines),
            "bulk_orm_safe": self.bulk_orm_safe,
        }


def sync_backend(sync):
    parameters = getattr(sync, "parameters", None) or {}
    return parameters.get(
        "execution_backend",
        ForwardExecutionBackendChoices.BRANCHING,
    )


def _bulk_orm_enabled_state(sync_parameters, *, backend):
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


def apply_engine_decision_for(*, sync, model_string, backend):
    backend = backend or sync_backend(sync)
    model_string = str(model_string or "")
    sync_parameters = getattr(sync, "parameters", None) or {}
    bulk_orm_enabled, bulk_orm_auto_enabled = _bulk_orm_enabled_state(
        sync_parameters,
        backend=backend,
    )
    configured_bulk_orm_models = {
        str(value)
        for value in (sync_parameters.get("bulk_orm_models") or [])
        if str(value)
    }
    if not configured_bulk_orm_models:
        configured_bulk_orm_models = set(BULK_ORM_ENABLED_MODELS)
    rejected_engines = (
        {
            "engine": ForwardApplyEngineChoices.TURBOBULK,
            "reason_code": "experimental_branch_only",
            "reason": (
                "TurboBulk is isolated to the experimental bulk branch until a "
                "supported NetBox runtime surface is available."
            ),
        },
        {
            "engine": ForwardApplyEngineChoices.PARQUET_BULK,
            "reason_code": "experimental_branch_only",
            "reason": (
                "Parquet bulk loading is isolated to the experimental bulk branch."
            ),
        },
    )
    if model_string in BULK_ORM_ENABLED_MODELS:
        if model_string in BULK_ORM_ENABLED_MODELS_WITHOUT_SPECS:
            return ForwardApplyEngineDecision(
                model_string=model_string,
                backend=backend,
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
                backend=backend,
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
            backend=backend,
            selected_engine=ForwardApplyEngineChoices.BULK_ORM,
            reason_code=(
                "bulk_orm_auto_enabled_fast_bootstrap"
                if bulk_orm_auto_enabled
                and backend == ForwardExecutionBackendChoices.FAST_BOOTSTRAP
                else (
                    "bulk_orm_auto_enabled_safe_model_set"
                    if bulk_orm_auto_enabled
                    else "bulk_orm_enabled_safe_model_set"
                )
            ),
            reason=(
                "Using bulk ORM automatically for safe models during Fast "
                "bootstrap to reduce trusted-baseline runtime."
                if bulk_orm_auto_enabled
                and backend == ForwardExecutionBackendChoices.FAST_BOOTSTRAP
                else (
                    "Using bulk ORM automatically for the parity-tested safe "
                    "model set because no explicit sync override was provided."
                    if bulk_orm_auto_enabled
                    else "Using bulk ORM for a narrow safe model set with scalar "
                    "fields and deterministic coalesce identity."
                )
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
            backend=backend,
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
    if model_string in EXPERIMENTAL_BULK_ORM_MODELS:
        if not bulk_orm_enabled:
            return ForwardApplyEngineDecision(
                model_string=model_string,
                backend=backend,
                selected_engine=ForwardApplyEngineChoices.ADAPTER,
                reason_code="bulk_orm_disabled_by_default",
                reason=(
                    "Using adapter engine by default. Enable `enable_bulk_orm` "
                    "on the sync to evaluate experimental bulk ORM candidates."
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
        if model_string not in configured_bulk_orm_models:
            return ForwardApplyEngineDecision(
                model_string=model_string,
                backend=backend,
                selected_engine=ForwardApplyEngineChoices.ADAPTER,
                reason_code="bulk_orm_model_not_allowlisted",
                reason=(
                    "Using adapter engine because this experimental model is not "
                    "present in `bulk_orm_models` for this sync."
                ),
                available_engines=(ForwardApplyEngineChoices.ADAPTER,),
                rejected_engines=(
                    {
                        "engine": ForwardApplyEngineChoices.BULK_ORM,
                        "reason_code": "model_not_allowlisted",
                        "reason": (
                            "Add this model to `bulk_orm_models` after parity "
                            "validation in your environment."
                        ),
                    },
                    *rejected_engines,
                ),
                bulk_orm_safe=False,
            )
        return ForwardApplyEngineDecision(
            model_string=model_string,
            backend=backend,
            selected_engine=ForwardApplyEngineChoices.BULK_ORM,
            reason_code="bulk_orm_experimental_allowlisted_model",
            reason=(
                "Using bulk ORM for an experimental model that is explicitly "
                "allowlisted on this sync."
            ),
            available_engines=(
                ForwardApplyEngineChoices.ADAPTER,
                ForwardApplyEngineChoices.BULK_ORM,
            ),
            rejected_engines=rejected_engines,
            bulk_orm_safe=False,
        )
    if model_string in SIMPLE_BULK_CANDIDATE_MODELS:
        return ForwardApplyEngineDecision(
            model_string=model_string,
            backend=backend,
            selected_engine=ForwardApplyEngineChoices.ADAPTER,
            reason_code="bulk_orm_deferred_native_parity",
            reason=(
                "Using the adapter engine because bulk ORM is not enabled until "
                "this model proves equivalent native validation, object change "
                "tracking, Branching diff visibility, and row-level issue capture."
            ),
            available_engines=(ForwardApplyEngineChoices.ADAPTER,),
            rejected_engines=(
                {
                    "engine": ForwardApplyEngineChoices.BULK_ORM,
                    "reason_code": "native_parity_unproven",
                    "reason": (
                        "This model is a possible future bulk candidate, but "
                        "bulk ORM remains disabled until parity tests are added."
                    ),
                },
                *rejected_engines,
            ),
            bulk_orm_safe=False,
        )
    return ForwardApplyEngineDecision(
        model_string=model_string,
        backend=backend,
        selected_engine=ForwardApplyEngineChoices.ADAPTER,
        reason_code="adapter_default_unclassified_model",
        reason=(
            "Using the adapter engine because no faster apply engine has been "
            "classified as safe for this model."
        ),
        available_engines=(ForwardApplyEngineChoices.ADAPTER,),
        rejected_engines=(
            {
                "engine": ForwardApplyEngineChoices.BULK_ORM,
                "reason_code": "unclassified_model",
                "reason": (
                    "Bulk ORM requires a per-model contract before it can be "
                    "enabled."
                ),
            },
            *rejected_engines,
        ),
        bulk_orm_safe=False,
    )


def apply_engine_decision_summary(*, sync, model_string, backend):
    return apply_engine_decision_for(
        sync=sync,
        model_string=model_string,
        backend=backend,
    ).as_dict()


def bulk_orm_expansion_summary(model_strings=None):
    selected_models = set(str(model) for model in (model_strings or []) if str(model))
    if not selected_models:
        selected_models = set(FORWARD_SUPPORTED_MODELS)

    safe_models = sorted(BULK_ORM_ENABLED_MODELS & selected_models)
    experimental_models = sorted(
        (EXPERIMENTAL_BULK_ORM_MODELS - ADAPTER_REQUIRED_MODELS) & selected_models
    )
    blocked_models = []
    for model_string in sorted(ADAPTER_REQUIRED_MODELS & selected_models):
        blocker = ADAPTER_MODEL_BLOCKERS.get(model_string) or {}
        blocker_code = blocker.get("blocker_code", "adapter_contract_required")
        lane = BLOCKER_PROMOTION_LANES.get(blocker_code) or {
            "lane": "adapter_contract_models",
            "priority": 99,
            "risk": "unknown",
            "message": "Model-specific adapter behavior requires parity proof first.",
            "required_gate": "adapter_contract_parity",
        }
        blocked_models.append(
            {
                "model": model_string,
                "blocker_code": blocker_code,
                "blocker_reason": blocker.get(
                    "blocker_reason",
                    "Model-specific adapter behavior requires parity proof first.",
                ),
                "promotion_lane": lane["lane"],
                "promotion_priority": lane["priority"],
                "promotion_risk": lane["risk"],
                "required_gate": lane["required_gate"],
            }
        )

    if experimental_models:
        status = "experimental_candidates"
        message = (
            "Experimental bulk ORM candidates exist, but require explicit allowlist "
            "and full parity evidence before broad use."
        )
    elif blocked_models:
        status = "blocked_pending_parity"
        message = (
            "No additional models should be promoted to bulk ORM until adapter "
            "blockers are cleared by parity evidence."
        )
    else:
        status = "safe_set_only"
        message = "All selected models are in the current bulk ORM safe set."

    parity_plan = _bulk_orm_parity_plan(blocked_models)
    return {
        "status": status,
        "message": message,
        "safe_models": safe_models,
        "experimental_models": experimental_models,
        "blocked_models": blocked_models,
        "blocked_model_count": len(blocked_models),
        "promotion_lanes": _bulk_orm_promotion_lanes(blocked_models),
        "recommended_next_models": _bulk_orm_recommended_next_models(blocked_models),
        "high_impact_blocked_models": _bulk_orm_high_impact_blocked_models(
            blocked_models
        ),
        "parity_gates": [dict(item) for item in BULK_ORM_PARITY_GATES],
        "parity_plan": parity_plan,
        "next_action": (
            "Choose the first recommended promotion lane, write adapter-vs-bulk "
            "parity tests for that model family, and enable it only after "
            "validation, object-change, Branching, row-issue, and runtime gates pass."
            if blocked_models
            else "Keep current safe set and monitor runtime evidence."
        ),
    }


def _bulk_orm_promotion_lanes(blocked_models):
    lanes = {}
    for item in blocked_models or []:
        lane_name = item["promotion_lane"]
        lane = lanes.setdefault(
            lane_name,
            {
                "lane": lane_name,
                "priority": int(item["promotion_priority"]),
                "risk": item["promotion_risk"],
                "models": [],
                "blocker_codes": set(),
                "required_gates": set(),
                "message": (
                    BLOCKER_PROMOTION_LANES.get(item["blocker_code"], {}).get("message")
                    or "Model-specific adapter behavior requires parity proof first."
                ),
            },
        )
        lane["models"].append(item["model"])
        lane["blocker_codes"].add(item["blocker_code"])
        lane["required_gates"].add(item["required_gate"])

    normalized = []
    for lane in lanes.values():
        normalized.append(
            {
                "lane": lane["lane"],
                "priority": lane["priority"],
                "risk": lane["risk"],
                "models": sorted(lane["models"]),
                "model_count": len(lane["models"]),
                "blocker_codes": sorted(lane["blocker_codes"]),
                "required_gates": sorted(lane["required_gates"]),
                "message": lane["message"],
            }
        )
    return sorted(
        normalized,
        key=lambda item: (
            int(item["priority"]),
            -int(item["model_count"]),
            str(item["lane"]),
        ),
    )


def _bulk_orm_recommended_next_models(blocked_models):
    return sorted(
        [
            {
                "model": item["model"],
                "promotion_lane": item["promotion_lane"],
                "priority": item["promotion_priority"],
                "risk": item["promotion_risk"],
                "blocker_code": item["blocker_code"],
                "required_gate": item["required_gate"],
            }
            for item in blocked_models or []
        ],
        key=lambda item: (
            int(item["priority"]),
            str(item["promotion_lane"]),
            str(item["model"]),
        ),
    )[:5]


def _bulk_orm_high_impact_blocked_models(blocked_models):
    return sorted(
        [
            {
                "model": item["model"],
                "promotion_lane": item["promotion_lane"],
                "impact_priority": BULK_ORM_PERFORMANCE_IMPACT_PRIORITY.get(
                    item["model"], 99
                ),
                "promotion_priority": item["promotion_priority"],
                "risk": item["promotion_risk"],
                "blocker_code": item["blocker_code"],
                "required_gate": item["required_gate"],
            }
            for item in blocked_models or []
            if item["model"] in BULK_ORM_PERFORMANCE_IMPACT_PRIORITY
        ],
        key=lambda item: (
            int(item["impact_priority"]),
            int(item["promotion_priority"]),
            str(item["model"]),
        ),
    )[:5]


def _bulk_orm_parity_plan(blocked_models):
    blocked_by_model = {item["model"]: item for item in blocked_models or []}
    selected_models = []
    candidate_sources = {}
    for item in _bulk_orm_recommended_next_models(blocked_models)[:3]:
        model = item["model"]
        if model not in selected_models:
            selected_models.append(model)
        candidate_sources.setdefault(model, set()).add("lowest_risk_lane")
    for item in _bulk_orm_high_impact_blocked_models(blocked_models)[:2]:
        model = item["model"]
        if model not in selected_models:
            selected_models.append(model)
        candidate_sources.setdefault(model, set()).add("highest_impact_model")

    candidates = []
    for model in selected_models:
        item = blocked_by_model.get(model)
        if not item:
            continue
        candidates.append(
            {
                "model": model,
                "candidate_sources": sorted(candidate_sources.get(model) or []),
                "promotion_lane": item["promotion_lane"],
                "priority": item["promotion_priority"],
                "risk": item["promotion_risk"],
                "blocker_code": item["blocker_code"],
                "blocker_reason": item["blocker_reason"],
                "lane_gate": item["required_gate"],
                "required_checklist": [
                    dict(gate) for gate in BULK_ORM_PARITY_CHECKLIST
                ],
                "required_test_ids": _bulk_orm_required_test_ids(model),
                "evidence_command_hint": (
                    "Run the candidate-specific adapter-vs-bulk parity tests, then "
                    "run architecture audit, harness check, and the large-run "
                    "regression gate before enabling the model."
                ),
            }
        )

    return {
        "status": "pending_candidate_parity" if candidates else "no_blocked_candidates",
        "candidate_count": len(candidates),
        "candidates": candidates,
        "checklist": [dict(gate) for gate in BULK_ORM_PARITY_CHECKLIST],
        "next_action": (
            "Implement the first candidate's required test IDs and keep the model "
            "on the adapter engine until every checklist item has direct evidence."
            if candidates
            else "No parity work is required for the selected model set."
        ),
    }


def _bulk_orm_required_test_ids(model_string):
    slug = str(model_string or "").replace(".", "_")
    return [
        f"ForwardApplyEngineParityTest.test_{slug}_create_parity",
        f"ForwardApplyEngineParityTest.test_{slug}_update_parity",
        f"ForwardApplyEngineParityTest.test_{slug}_delete_parity",
        f"ForwardApplyEngineParityTest.test_{slug}_validation_failure_parity",
        f"ForwardApplyEngineParityTest.test_{slug}_row_issue_parity",
        f"ForwardApplyEngineParityTest.test_{slug}_dependency_behavior_parity",
        f"ForwardApplyEngineParityTest.test_{slug}_object_change_tracking_parity",
        f"ForwardApplyEngineParityTest.test_{slug}_branching_semantics_parity",
        f"ForwardApplyEngineParityTest.test_{slug}_support_bundle_statistics_parity",
        f"ForwardApplyEngineParityTest.test_{slug}_runtime_non_regression",
    ]
