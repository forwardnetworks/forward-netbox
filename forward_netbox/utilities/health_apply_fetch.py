from collections import Counter

from ..choices import FORWARD_OPTIONAL_MODELS
from ..choices import FORWARD_SUPPORTED_MODELS
from .apply_engine import apply_engine_decision_summary
from .branch_budget import shard_fetch_capability_for_model


def model_summary(sync, maps):
    enabled_models = list(sync.enabled_models())
    optional_enabled = [
        model for model in enabled_models if model in FORWARD_OPTIONAL_MODELS
    ]
    mapped_models = {query_map.model_string for query_map in maps}
    return {
        "enabled_count": len(enabled_models),
        "optional_enabled_count": len(optional_enabled),
        "enabled_models": enabled_models,
        "optional_enabled_models": optional_enabled,
        "enabled_models_without_map": [
            model for model in enabled_models if model not in mapped_models
        ],
    }


def apply_engine_summary(sync, model_strings):
    decisions = [
        apply_engine_decision_summary(
            sync=sync,
            model_string=model_string,
            backend=None,
        )
        for model_string in model_strings
    ]
    global_matrix = [
        apply_engine_decision_summary(
            sync=sync,
            model_string=model_string,
            backend=None,
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
        shard_fetch_capability_for_model(model_string) for model_string in model_strings
    ]
    modes = Counter(item["fetch_mode"] for item in contracts)
    reasons = Counter(item["reason_code"] for item in contracts)
    bucket_supported = sum(
        1
        for item in contracts
        if (item.get("bucket_strategy") or {}).get("supported")
    )
    return {
        "modes": dict(modes),
        "fallback_reasons": dict(reasons),
        "shard_safe_count": sum(1 for item in contracts if item["shard_safe"]),
        "model_fallback_count": sum(
            1 for item in contracts if not item["shard_safe"]
        ),
        "bucket_supported_count": bucket_supported,
        "contracts": contracts,
    }
