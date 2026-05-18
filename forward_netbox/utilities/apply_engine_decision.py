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
    "ipam.vlan",
    "ipam.vrf",
}

BULK_ORM_ENABLED_MODELS = {
    "dcim.site",
    "dcim.manufacturer",
    "dcim.devicerole",
    "dcim.platform",
    "dcim.devicetype",
    "ipam.vlan",
    "ipam.vrf",
}
EXPERIMENTAL_BULK_ORM_MODELS = set()

BULK_ORM_SPEC_MODELS = {
    "dcim.site",
    "dcim.manufacturer",
    "dcim.devicerole",
    "dcim.platform",
    "dcim.devicetype",
    "ipam.vlan",
    "ipam.vrf",
}

ADAPTER_REQUIRED_MODELS = {
    "dcim.cable",
    "dcim.device",
    "dcim.interface",
    "dcim.inventoryitem",
    "dcim.macaddress",
    "dcim.module",
    "dcim.virtualchassis",
    "extras.taggeditem",
    "ipam.ipaddress",
    "ipam.prefix",
    "netbox_peering_manager.peeringsession",
    "netbox_routing.bgpaddressfamily",
    "netbox_routing.bgppeer",
    "netbox_routing.bgppeeraddressfamily",
    "netbox_routing.ospfarea",
    "netbox_routing.ospfinstance",
    "netbox_routing.ospfinterface",
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
    "dcim.interface": {
        "blocker_code": "relationship_side_effects",
        "blocker_reason": (
            "Interface writes include LAG/member behavior, cable relationships, and "
            "row-level merge semantics that require adapter-specific logic."
        ),
    },
    "dcim.inventoryitem": {
        "blocker_code": "dependency_resolution",
        "blocker_reason": (
            "Inventory item writes require device/interface anchoring and optional "
            "module/component reconciliation handled by adapters."
        ),
    },
    "dcim.macaddress": {
        "blocker_code": "relationship_side_effects",
        "blocker_reason": (
            "MAC address writes depend on interface relationships and duplicate/skip "
            "handling that currently relies on adapter row logic."
        ),
    },
    "dcim.module": {
        "blocker_code": "dependency_resolution",
        "blocker_reason": (
            "Module writes depend on module-bay/type readiness and guarded "
            "dependency skips managed by adapter workflow."
        ),
    },
    "dcim.virtualchassis": {
        "blocker_code": "optional_contract_guarding",
        "blocker_reason": (
            "Virtual chassis rows rely on conditional/no-op behavior when stacking "
            "evidence is absent and adapter guards enforce that contract."
        ),
    },
    "extras.taggeditem": {
        "blocker_code": "generic_foreign_key_relations",
        "blocker_reason": (
            "Tagged item writes use generic relation semantics and adapter-level "
            "dedupe/skip behavior."
        ),
    },
    "ipam.ipaddress": {
        "blocker_code": "ipam_parent_prefix_semantics",
        "blocker_reason": (
            "IP address writes require parent-prefix/role checks and conditional "
            "skip logic tied to adapter diagnostics."
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
}

APPLY_ENGINE_MODEL_CLASSIFICATIONS = {
    **{model_string: "bulk_orm_candidate" for model_string in SIMPLE_BULK_CANDIDATE_MODELS},
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


def apply_engine_decision_for(*, sync, model_string, backend):
    backend = backend or sync_backend(sync)
    model_string = str(model_string or "")
    sync_parameters = getattr(sync, "parameters", None) or {}
    bulk_orm_enabled = bool(sync_parameters.get("enable_bulk_orm", False))
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
            reason_code="bulk_orm_enabled_safe_model_set",
            reason=(
                "Using bulk ORM for a narrow safe model set with scalar fields "
                "and deterministic coalesce identity."
            ),
            available_engines=(
                ForwardApplyEngineChoices.ADAPTER,
                ForwardApplyEngineChoices.BULK_ORM,
            ),
            rejected_engines=rejected_engines,
            bulk_orm_safe=True,
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
