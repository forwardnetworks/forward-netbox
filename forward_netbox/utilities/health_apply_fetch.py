from collections import Counter

from ..choices import FORWARD_OPTIONAL_MODELS
from ..choices import FORWARD_SUPPORTED_MODELS
from .apply_engine import apply_engine_decision_summary
from .model_contracts import architecture_contract_summary
from .model_contracts import architecture_fetch_contract_for_model


def model_summary(sync, maps):
    enabled_models = list(sync.enabled_models())
    optional_enabled = [
        model for model in enabled_models if model in FORWARD_OPTIONAL_MODELS
    ]
    mapped_models = {query_map.model_string for query_map in maps}
    enabled_model_set = set(enabled_models)
    return {
        "enabled_count": len(enabled_models),
        "optional_enabled_count": len(optional_enabled),
        "enabled_models": enabled_models,
        "optional_enabled_models": optional_enabled,
        "enabled_models_without_map": [
            model for model in enabled_models if model not in mapped_models
        ],
        # The inverse dark-map case: an OPTIONAL-model map (e.g. the netbox-dlm
        # CVE / Vulnerability maps) is enabled in the NQE Maps list but its
        # model is not selected in THIS sync's Model Selection, so the map
        # never runs and produces nothing — silently. Scoped to optional
        # models because base models are on by default; enabling an opt-in map
        # without selecting its model is almost always a mistake.
        "enabled_optional_maps_without_model": sorted(
            {
                query_map.name
                for query_map in maps
                if query_map.model_string in FORWARD_OPTIONAL_MODELS
                and query_map.model_string not in enabled_model_set
            }
        ),
    }


def apply_engine_summary(sync, model_strings):
    decisions = [
        apply_engine_decision_summary(
            sync=sync,
            model_string=model_string,
        )
        for model_string in model_strings
    ]
    global_matrix = [
        apply_engine_decision_summary(
            sync=sync,
            model_string=model_string,
        )
        for model_string in FORWARD_SUPPORTED_MODELS
    ]
    selected = Counter(item["selected_engine"] for item in decisions)
    fallback_reasons = Counter(item["reason_code"] for item in decisions)
    global_selected = Counter(item["selected_engine"] for item in global_matrix)
    global_fallback_reasons = Counter(item["reason_code"] for item in global_matrix)
    adapter_blockers = Counter()
    global_adapter_blockers = Counter()
    for item in decisions:
        for rejection in item.get("rejected_engines", []):
            if rejection.get("engine") != "bulk_orm":
                continue
            blocker = rejection.get("blocker_code")
            if blocker:
                adapter_blockers[blocker] += 1
    for item in global_matrix:
        for rejection in item.get("rejected_engines", []):
            if rejection.get("engine") != "bulk_orm":
                continue
            blocker = rejection.get("blocker_code")
            if blocker:
                global_adapter_blockers[blocker] += 1
    return {
        "selected": dict(selected),
        "fallback_reasons": dict(fallback_reasons),
        "blocker_codes": dict(adapter_blockers),
        "decisions": decisions,
        "global_selected": dict(global_selected),
        "global_fallback_reasons": dict(global_fallback_reasons),
        "global_blocker_codes": dict(global_adapter_blockers),
        "global_decisions": global_matrix,
    }


def fetch_contract_summary(model_strings):
    contracts = [
        architecture_fetch_contract_for_model(model_string)
        for model_string in model_strings
    ]
    registry = architecture_contract_summary(model_strings)
    modes = Counter(item["fetch_mode"] for item in contracts)
    reasons = Counter(item["reason_code"] for item in contracts)
    bucket_supported = sum(
        1 for item in contracts if (item.get("bucket_strategy") or {}).get("supported")
    )
    return {
        "modes": dict(modes),
        "fallback_reasons": dict(reasons),
        "shard_safe_count": sum(1 for item in contracts if item["shard_safe"]),
        "model_fallback_count": sum(1 for item in contracts if not item["shard_safe"]),
        "bucket_supported_count": bucket_supported,
        "contract_registry_status": registry["status"],
        "contract_registry_gap_count": len(registry["gaps"]),
        "contracts": contracts,
    }
