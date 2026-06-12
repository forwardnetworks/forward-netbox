from dataclasses import dataclass

from ..choices import FORWARD_SUPPORTED_MODELS
from .apply_engine import ADAPTER_MODEL_BLOCKERS
from .apply_engine import APPLY_ENGINE_MODEL_CLASSIFICATIONS
from .apply_engine import BULK_ORM_ENABLED_MODELS
from .apply_engine import BULK_ORM_SPEC_MODELS
from .branch_budget import DELETE_DEPENDENCY_MODEL_RANK
from .branch_budget import shard_fetch_capability_for_model
from .sync_contracts import contract_for_model
from .sync_contracts import field_ownership_for_model


@dataclass(frozen=True)
class ForwardModelArchitectureContract:
    model: str
    required_fields: tuple[str, ...]
    allowed_coalesce_fields: tuple[str, ...]
    default_coalesce_fields: tuple[tuple[str, ...], ...]
    preserve_existing_on_blank_fields: tuple[str, ...]
    field_ownership: dict
    fetch_contract: dict
    delete_dependency_rank: int | None
    apply_engine_classification: str
    apply_engine_blocker_code: str
    apply_engine_blocker_reason: str
    bulk_orm_safe: bool
    support_diagnostic_fields: tuple[str, ...]

    def as_dict(self):
        return {
            "model": self.model,
            "required_fields": list(self.required_fields),
            "allowed_coalesce_fields": list(self.allowed_coalesce_fields),
            "default_coalesce_fields": [
                list(field_set) for field_set in self.default_coalesce_fields
            ],
            "preserve_existing_on_blank_fields": list(
                self.preserve_existing_on_blank_fields
            ),
            "field_ownership": dict(self.field_ownership),
            "fetch_contract": dict(self.fetch_contract),
            "delete_dependency_rank": self.delete_dependency_rank,
            "apply_engine_classification": self.apply_engine_classification,
            "apply_engine_blocker_code": self.apply_engine_blocker_code,
            "apply_engine_blocker_reason": self.apply_engine_blocker_reason,
            "bulk_orm_safe": self.bulk_orm_safe,
            "support_diagnostic_fields": list(self.support_diagnostic_fields),
        }


def architecture_contract_for_model(model_string: str):
    model_string = str(model_string or "")
    sync_contract = contract_for_model(model_string)
    fetch_contract = shard_fetch_capability_for_model(model_string)
    blocker = ADAPTER_MODEL_BLOCKERS.get(model_string) or {}
    support_fields = _support_diagnostic_fields(
        required_fields=sync_contract.required_fields,
        default_coalesce_fields=sync_contract.default_coalesce_fields,
        fetch_contract=fetch_contract,
    )
    return ForwardModelArchitectureContract(
        model=model_string,
        required_fields=tuple(sync_contract.required_fields),
        allowed_coalesce_fields=tuple(sync_contract.allowed_coalesce_fields),
        default_coalesce_fields=tuple(
            tuple(field_name for field_name in field_set)
            for field_set in sync_contract.default_coalesce_fields
        ),
        preserve_existing_on_blank_fields=tuple(
            sync_contract.preserve_existing_on_blank_fields
        ),
        field_ownership=field_ownership_for_model(model_string),
        fetch_contract=fetch_contract,
        delete_dependency_rank=DELETE_DEPENDENCY_MODEL_RANK.get(model_string),
        apply_engine_classification=APPLY_ENGINE_MODEL_CLASSIFICATIONS.get(
            model_string,
            "",
        ),
        apply_engine_blocker_code=str(blocker.get("blocker_code") or ""),
        apply_engine_blocker_reason=str(blocker.get("blocker_reason") or ""),
        bulk_orm_safe=model_string in BULK_ORM_ENABLED_MODELS,
        support_diagnostic_fields=support_fields,
    )


def architecture_contract_matrix(model_strings=None):
    model_strings = tuple(model_strings or FORWARD_SUPPORTED_MODELS)
    return {
        model_string: architecture_contract_for_model(model_string).as_dict()
        for model_string in sorted(model_strings)
    }


def architecture_fetch_contract_for_model(model_string: str):
    return dict(architecture_contract_for_model(model_string).fetch_contract)


def architecture_default_coalesce_fields_for_model(model_string: str):
    return [
        list(field_set)
        for field_set in architecture_contract_for_model(
            model_string
        ).default_coalesce_fields
    ]


def architecture_fetch_contracts(model_strings=None):
    model_strings = tuple(model_strings or FORWARD_SUPPORTED_MODELS)
    return {
        model_string: architecture_fetch_contract_for_model(model_string)
        for model_string in sorted(model_strings)
    }


def architecture_bulk_orm_safe_models(model_strings=None):
    contracts = architecture_contract_matrix(model_strings)
    return sorted(
        model_string
        for model_string, contract in contracts.items()
        if contract.get("bulk_orm_safe")
    )


def architecture_adapter_required_models(model_strings=None):
    contracts = architecture_contract_matrix(model_strings)
    return sorted(
        model_string
        for model_string, contract in contracts.items()
        if contract.get("apply_engine_classification") == "adapter_required"
    )


def architecture_adapter_blockers(model_strings=None):
    contracts = architecture_contract_matrix(model_strings)
    return {
        model_string: contract.get("apply_engine_blocker_code")
        for model_string, contract in sorted(contracts.items())
        if contract.get("apply_engine_blocker_code")
    }


def architecture_unclassified_supported_models(model_strings=None):
    contracts = architecture_contract_matrix(model_strings)
    return sorted(
        model_string
        for model_string, contract in contracts.items()
        if not contract.get("apply_engine_classification")
    )


def architecture_adapter_models_without_blocker(model_strings=None):
    contracts = architecture_contract_matrix(model_strings)
    return sorted(
        model_string
        for model_string, contract in contracts.items()
        if contract.get("apply_engine_classification") == "adapter_required"
        and not contract.get("apply_engine_blocker_code")
    )


def architecture_bulk_orm_safe_models_without_specs(model_strings=None):
    contracts = architecture_contract_matrix(model_strings)
    return sorted(
        model_string
        for model_string, contract in contracts.items()
        if contract.get("bulk_orm_safe") and model_string not in BULK_ORM_SPEC_MODELS
    )


def architecture_contract_gaps(model_strings=None):
    contracts = architecture_contract_matrix(model_strings)
    gaps = []
    for model_string, contract in contracts.items():
        fetch_contract = contract.get("fetch_contract") or {}
        if contract.get("model") != model_string:
            gaps.append(
                _gap(
                    model_string,
                    "model_identity_mismatch",
                    "Contract model identity does not match the supported model key.",
                )
            )
        if not contract.get("required_fields"):
            gaps.append(
                _gap(
                    model_string,
                    "missing_required_fields",
                    "Contract does not define required NQE row fields.",
                )
            )
        if not contract.get("default_coalesce_fields"):
            gaps.append(
                _gap(
                    model_string,
                    "missing_default_coalesce_fields",
                    "Contract does not define default coalesce identity.",
                )
            )
        if fetch_contract.get("model") != model_string:
            gaps.append(
                _gap(
                    model_string,
                    "missing_fetch_contract",
                    "Contract does not expose a matching shard fetch contract.",
                )
            )
        if not fetch_contract.get("reason_code"):
            gaps.append(
                _gap(
                    model_string,
                    "missing_fetch_reason_code",
                    "Fetch contract does not explain its fetch mode.",
                )
            )
        if not contract.get("apply_engine_classification"):
            gaps.append(
                _gap(
                    model_string,
                    "missing_apply_engine_classification",
                    "Contract does not classify apply-engine eligibility.",
                )
            )
        if contract.get("apply_engine_classification") == "adapter_required" and not (
            contract.get("apply_engine_blocker_code")
        ):
            gaps.append(
                _gap(
                    model_string,
                    "missing_apply_engine_blocker",
                    "Adapter-required model does not explain why bulk apply is blocked.",
                )
            )
        if contract.get("delete_dependency_rank") is None:
            gaps.append(
                _gap(
                    model_string,
                    "missing_delete_dependency_rank",
                    "Contract does not define delete dependency ordering.",
                )
            )
        if not contract.get("support_diagnostic_fields"):
            gaps.append(
                _gap(
                    model_string,
                    "missing_support_diagnostic_fields",
                    "Contract does not define support-safe diagnostic fields.",
                )
            )
        field_ownership = contract.get("field_ownership") or {}
        if field_ownership.get("model") != model_string:
            gaps.append(
                _gap(
                    model_string,
                    "missing_field_ownership",
                    "Contract does not expose field ownership metadata.",
                )
            )
        if not field_ownership.get("blank_update_policy"):
            gaps.append(
                _gap(
                    model_string,
                    "missing_blank_update_policy",
                    "Field ownership does not define blank-update behavior.",
                )
            )
    return gaps


def architecture_contract_summary(model_strings=None):
    contracts = architecture_contract_matrix(model_strings)
    gaps = architecture_contract_gaps(model_strings)
    return {
        "status": "pass" if not gaps else "fail",
        "contract_count": len(contracts),
        "models": sorted(contracts),
        "contracts": contracts,
        "gaps": gaps,
    }


def _support_diagnostic_fields(
    *,
    required_fields,
    default_coalesce_fields,
    fetch_contract,
):
    fields = set(required_fields or ())
    for field_set in default_coalesce_fields or ():
        fields.update(field_set)
    key_family = str((fetch_contract or {}).get("key_family") or "")
    for field_name in key_family.split(","):
        if field_name:
            fields.add(field_name)
    return tuple(sorted(fields))


def _gap(model_string, code, message):
    return {
        "model": model_string,
        "code": code,
        "message": message,
    }
