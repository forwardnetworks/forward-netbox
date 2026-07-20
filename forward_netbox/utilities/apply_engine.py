from dataclasses import dataclass

from . import apply_engine_decision as _decision_mod
from ..choices import ForwardApplyEngineChoices
from .apply_engine_bulk import (
    bulk_orm_apply_simple_models as _bulk_orm_apply_simple_models_impl,
)
from .apply_engine_bulk import (
    bulk_orm_apply_tree_models as _bulk_orm_apply_tree_models_impl,
)
from .apply_engine_bulk import bulk_orm_delete_prefixes
from .apply_engine_bulk import lookup_key_from_object as _lookup_key_from_object_impl
from .apply_engine_bulk import lookup_key_from_values as _lookup_key_from_values_impl
from .apply_engine_bulk import lookup_key_value as _lookup_key_value_impl

ADAPTER_MODEL_BLOCKERS = _decision_mod.ADAPTER_MODEL_BLOCKERS
ADAPTER_MODELS_WITHOUT_BLOCKER = _decision_mod.ADAPTER_MODELS_WITHOUT_BLOCKER
ADAPTER_REQUIRED_MODELS = _decision_mod.ADAPTER_REQUIRED_MODELS
APPLY_ENGINE_MODEL_CLASSIFICATIONS = _decision_mod.APPLY_ENGINE_MODEL_CLASSIFICATIONS
BULK_ORM_ENABLED_MODELS = _decision_mod.BULK_ORM_ENABLED_MODELS
BULK_ORM_ENABLED_MODELS_WITHOUT_SPECS = (
    _decision_mod.BULK_ORM_ENABLED_MODELS_WITHOUT_SPECS
)
BULK_ORM_SPEC_MODELS = _decision_mod.BULK_ORM_SPEC_MODELS
ForwardApplyEngineDecision = _decision_mod.ForwardApplyEngineDecision
UNCLASSIFIED_SUPPORTED_MODELS = _decision_mod.UNCLASSIFIED_SUPPORTED_MODELS


@dataclass(frozen=True)
class ForwardApplyEngine:
    """Behavior-preserving apply-engine boundary.

    The adapter engine delegates to the model-specific row adapters; bulk ORM is
    selected only for the parity-tested model set.
    """

    name: str = ForwardApplyEngineChoices.ADAPTER
    decision: ForwardApplyEngineDecision | None = None

    def apply_upserts(self, runner, model_string, rows):
        if self.name == ForwardApplyEngineChoices.BULK_ORM and model_string in (
            BULK_ORM_ENABLED_MODELS
        ):
            if model_string in BULK_ORM_ENABLED_MODELS_WITHOUT_SPECS:
                runner._record_issue(
                    model_string,
                    "Bulk ORM was selected but no model spec is defined; falling back to adapter.",
                    {},
                    context={"reason_code": "bulk_orm_enabled_model_missing_spec"},
                )
            if _bulk_orm_apply_simple_models(runner, model_string, rows):
                return
        return runner._apply_model_rows(model_string, rows)

    def apply_deletes(self, runner, model_string, rows):
        if (
            self.name == ForwardApplyEngineChoices.BULK_ORM
            and model_string == "ipam.prefix"
            and bulk_orm_delete_prefixes(runner, rows)
        ):
            return
        return runner._delete_model_rows(model_string, rows)


def select_apply_engine(*, sync, model_string):
    decision = apply_engine_decision_for(sync=sync, model_string=model_string)
    return ForwardApplyEngine(name=decision.selected_engine, decision=decision)


def apply_engine_name_for(*, sync, model_string):
    return select_apply_engine(sync=sync, model_string=model_string).name


def apply_engine_decision_for(*, sync, model_string):
    _decision_mod.BULK_ORM_ENABLED_MODELS = BULK_ORM_ENABLED_MODELS
    _decision_mod.ADAPTER_REQUIRED_MODELS = ADAPTER_REQUIRED_MODELS
    _decision_mod.ADAPTER_MODEL_BLOCKERS = ADAPTER_MODEL_BLOCKERS
    _decision_mod.BULK_ORM_ENABLED_MODELS_WITHOUT_SPECS = (
        BULK_ORM_ENABLED_MODELS_WITHOUT_SPECS
    )
    return _decision_mod.apply_engine_decision_for(
        sync=sync,
        model_string=model_string,
    )


def apply_engine_decision_summary(*, sync, model_string):
    return apply_engine_decision_for(sync=sync, model_string=model_string).as_dict()


def _bulk_orm_apply_simple_models(runner, model_string, rows):
    return _bulk_orm_apply_simple_models_impl(runner, model_string, rows)


def _lookup_key_from_object(obj, lookup_set):
    return _lookup_key_from_object_impl(obj, lookup_set)


def _lookup_key_from_values(values, lookup_set):
    return _lookup_key_from_values_impl(values, lookup_set)


def _lookup_key_value(value):
    return _lookup_key_value_impl(value)


def _bulk_orm_apply_tree_models(
    *,
    runner,
    model_string,
    model,
    fields,
    lookup_sets,
    normalized_rows,
):
    return _bulk_orm_apply_tree_models_impl(
        runner=runner,
        model_string=model_string,
        model=model,
        fields=fields,
        lookup_sets=lookup_sets,
        normalized_rows=normalized_rows,
    )
