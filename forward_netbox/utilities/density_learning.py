import math

from django.utils import timezone
from django.utils.dateparse import parse_datetime


MIN_MODEL_DENSITY = 0.01
MAX_MODEL_DENSITY = 1000.0
DENSITY_BASELINE_SAMPLE_TARGET = 8
DENSITY_POLICY_MEDIUM_CONFIDENCE_WEIGHT = 0.5
DENSITY_POLICY_FALLBACK_BASELINE = 1.0


def clamp_density(value):
    try:
        density = float(value)
    except (TypeError, ValueError):
        return None
    if density <= 0:
        return None
    return max(MIN_MODEL_DENSITY, min(MAX_MODEL_DENSITY, density))


def normalize_density_map(density_map):
    normalized = {}
    for model_string, density in (density_map or {}).items():
        density_value = clamp_density(density)
        if density_value is None:
            continue
        normalized[str(model_string)] = density_value
    return normalized


def normalize_density_profile(profile):
    normalized = {}
    for model_string, item in (profile or {}).items():
        if not isinstance(item, dict):
            continue
        density_value = clamp_density(item.get("density"))
        if density_value is None:
            continue
        sample_count = _safe_int(item.get("sample_count"))
        accepted_observations = _safe_int(item.get("accepted_observations"))
        rejected_observations = _safe_int(item.get("rejected_observations"))
        mean = _safe_float(item.get("mean"), default=density_value)
        m2 = max(0.0, _safe_float(item.get("m2"), default=0.0))
        variance = max(0.0, _safe_float(item.get("variance"), default=0.0))
        stddev = max(0.0, _safe_float(item.get("stddev"), default=0.0))
        if sample_count >= 2:
            derived_variance = m2 / float(sample_count - 1)
            variance = max(variance, derived_variance)
            stddev = math.sqrt(variance)
        normalized[str(model_string)] = {
            "density": density_value,
            "sample_count": sample_count,
            "accepted_observations": accepted_observations,
            "rejected_observations": rejected_observations,
            "mean": mean,
            "m2": m2,
            "variance": variance,
            "stddev": stddev,
            "last_observed_density": _safe_float(
                item.get("last_observed_density"),
                default=density_value,
            ),
            "last_observed_at": str(item.get("last_observed_at") or ""),
            "last_updated_at": str(item.get("last_updated_at") or ""),
        }
    return normalized


def density_profile_summary(*, density_map, density_profile, default_density_map):
    learned = normalize_density_map(density_map)
    profile = normalize_density_profile(density_profile)
    models = sorted(set(learned) | set(profile))
    model_rows = []
    for model_string in models:
        learned_density = learned.get(model_string)
        profile_entry = profile.get(model_string) or {}
        default_density = _safe_density(default_density_map.get(model_string))
        sample_count = _safe_int(profile_entry.get("sample_count"))
        confidence = density_confidence_score(
            sample_count=sample_count,
            variance=_safe_float(profile_entry.get("variance"), default=0.0),
            last_updated_at=profile_entry.get("last_updated_at"),
        )
        confidence_label = confidence_bucket(confidence)
        baseline_density = (
            default_density if default_density is not None else learned_density
        )
        delta = None
        if baseline_density and learned_density:
            delta = round(float(learned_density) / float(baseline_density), 4)
        budget_policy = density_budget_policy(
            model_string,
            learned_density=learned_density,
            profile_entry=profile_entry,
            default_density=default_density,
        )
        model_rows.append(
            {
                "model": model_string,
                "learned_density": learned_density,
                "default_density": default_density,
                "budget_density": budget_policy["density"],
                "budget_policy": budget_policy["policy"],
                "budget_policy_reason": budget_policy["reason"],
                "sample_count": sample_count,
                "accepted_observations": _safe_int(
                    profile_entry.get("accepted_observations")
                ),
                "rejected_observations": _safe_int(
                    profile_entry.get("rejected_observations")
                ),
                "variance": round(
                    _safe_float(profile_entry.get("variance"), default=0.0),
                    6,
                ),
                "stddev": round(
                    _safe_float(profile_entry.get("stddev"), default=0.0),
                    6,
                ),
                "confidence_score": confidence,
                "confidence": confidence_label,
                "delta_vs_baseline": delta,
                "last_updated_at": str(profile_entry.get("last_updated_at") or ""),
            }
        )

    model_rows = sorted(
        model_rows,
        key=lambda item: (
            item["confidence_score"],
            item["sample_count"],
            item["model"],
        ),
    )
    return {
        "model_count": len(model_rows),
        "high_confidence_count": len(
            [item for item in model_rows if item["confidence"] == "high"]
        ),
        "medium_confidence_count": len(
            [item for item in model_rows if item["confidence"] == "medium"]
        ),
        "low_confidence_count": len(
            [item for item in model_rows if item["confidence"] == "low"]
        ),
        "models": model_rows,
    }


def density_budget_policy(
    model_string,
    *,
    learned_density,
    profile_entry,
    default_density,
):
    learned = clamp_density(learned_density)
    default = clamp_density(default_density)
    if learned is None:
        return {
            "model": str(model_string or ""),
            "density": _safe_density(default),
            "policy": "default_density",
            "reason": (
                "No learned density is available; using the model default density."
                if default is not None
                else "No learned or default density is available; using row-count budget."
            ),
            "confidence": "",
            "confidence_score": None,
        }

    if not profile_entry:
        return {
            "model": str(model_string or ""),
            "density": _safe_density(
                default if default is not None else DENSITY_POLICY_FALLBACK_BASELINE
            ),
            "policy": "unprofiled_baseline_density",
            "reason": (
                "Learned density has no confidence profile; using the "
                "conservative baseline until observations establish confidence."
            ),
            "confidence": "",
            "confidence_score": None,
        }

    confidence_score = density_confidence_score(
        sample_count=_safe_int(profile_entry.get("sample_count")),
        variance=_safe_float(profile_entry.get("variance"), default=0.0),
        last_updated_at=profile_entry.get("last_updated_at"),
    )
    confidence = confidence_bucket(confidence_score)
    baseline = default if default is not None else DENSITY_POLICY_FALLBACK_BASELINE
    if confidence == "high":
        density = learned
        policy = "high_confidence_learned_density"
        reason = "High-confidence learned density is used for staging-item shaping."
    elif confidence == "medium":
        density = (baseline * (1.0 - DENSITY_POLICY_MEDIUM_CONFIDENCE_WEIGHT)) + (
            learned * DENSITY_POLICY_MEDIUM_CONFIDENCE_WEIGHT
        )
        policy = "medium_confidence_blended_density"
        reason = (
            "Medium-confidence learned density is blended with the conservative "
            "baseline before staging-item shaping."
        )
    else:
        density = baseline
        policy = "low_confidence_baseline_density"
        reason = (
            "Low-confidence learned density is not used for auto-tuning; the "
            "planner falls back to the conservative baseline."
        )

    return {
        "model": str(model_string or ""),
        "density": _safe_density(density),
        "policy": policy,
        "reason": reason,
        "confidence": confidence,
        "confidence_score": confidence_score,
    }


def density_confidence_score(*, sample_count, variance, last_updated_at):
    sample_component = min(
        1.0,
        float(max(0, int(sample_count))) / float(DENSITY_BASELINE_SAMPLE_TARGET),
    )
    variance_component = 1.0 / (1.0 + math.sqrt(max(0.0, float(variance))))
    recency_component = _recency_component(last_updated_at)
    confidence = sample_component * variance_component * recency_component
    return round(max(0.0, min(1.0, confidence)), 4)


def confidence_bucket(score):
    value = float(score or 0.0)
    if value >= 0.7:
        return "high"
    if value >= 0.35:
        return "medium"
    return "low"


def _recency_component(last_updated_at):
    if not last_updated_at:
        return 0.5
    parsed = parse_datetime(str(last_updated_at))
    if parsed is None:
        return 0.5
    if timezone.is_naive(parsed):
        parsed = timezone.make_aware(parsed, timezone.get_current_timezone())
    age_days = max(0.0, (timezone.now() - parsed).total_seconds() / 86400.0)
    if age_days <= 7:
        return 1.0
    if age_days <= 30:
        return 0.8
    if age_days <= 90:
        return 0.6
    return 0.4


def _safe_int(value, *, default=0):
    try:
        return int(value)
    except (TypeError, ValueError):
        return int(default)


def _safe_float(value, *, default=0.0):
    try:
        return float(value)
    except (TypeError, ValueError):
        return float(default)


def _safe_density(value):
    density = clamp_density(value)
    if density is None:
        return None
    return round(float(density), 6)
