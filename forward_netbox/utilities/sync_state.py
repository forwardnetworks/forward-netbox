from ..choices import ForwardDiffFallbackModeChoices
from ..choices import ForwardSyncStatusChoices
from .branch_budget import branch_budget_density_policy_summary
from .branch_budget import build_branch_budget_hints
from .branch_budget import DEFAULT_MODEL_CHANGE_DENSITY
from .branch_budget import MODEL_CHANGE_DENSITY_PARAMETER
from .branch_budget import MODEL_CHANGE_DENSITY_PROFILE_PARAMETER
from .density_learning import density_profile_summary
from .density_learning import normalize_density_map
from .density_learning import normalize_density_profile
from .execution_telemetry import build_sync_execution_summary

INITIAL_BASELINE_LARGE_SHARD_THRESHOLD = 10
INITIAL_BASELINE_LARGE_CHANGE_MULTIPLIER = 5
INITIAL_BASELINE_DAY_SHARD_THRESHOLD = 150
RUNTIME_CLASS_MINUTES_SECONDS = 60 * 60
RUNTIME_CLASS_DAYS_SECONDS = 24 * 60 * 60


def get_model_change_density(sync):
    density = (sync.parameters or {}).get(MODEL_CHANGE_DENSITY_PARAMETER) or {}
    if not isinstance(density, dict):
        return {}
    return normalize_density_map(density)


def get_model_change_density_profile(sync):
    profile = (sync.parameters or {}).get(MODEL_CHANGE_DENSITY_PROFILE_PARAMETER) or {}
    if not isinstance(profile, dict):
        return {}
    return normalize_density_profile(profile)


def set_model_change_density(sync, model_change_density):
    normalized = normalize_density_map(model_change_density)
    parameters = dict(sync.parameters or {})
    parameters[MODEL_CHANGE_DENSITY_PARAMETER] = normalized
    sync.parameters = parameters
    sync.__class__.objects.filter(pk=sync.pk).update(parameters=parameters)


def set_model_change_density_profile(sync, model_change_density_profile):
    normalized = normalize_density_profile(model_change_density_profile)
    parameters = dict(sync.parameters or {})
    parameters[MODEL_CHANGE_DENSITY_PROFILE_PARAMETER] = normalized
    sync.parameters = parameters
    sync.__class__.objects.filter(pk=sync.pk).update(parameters=parameters)


def ready_for_sync(sync):
    return sync.status not in (
        ForwardSyncStatusChoices.QUEUED,
        ForwardSyncStatusChoices.SYNCING,
        ForwardSyncStatusChoices.MERGING,
        ForwardSyncStatusChoices.READY_TO_MERGE,
    )


def get_max_changes_per_staging_item(sync, default_max_changes_per_staging_item):
    try:
        value = int(
            (sync.parameters or {}).get(
                "max_changes_per_staging_item",
                default_max_changes_per_staging_item,
            )
        )
    except (TypeError, ValueError):
        return default_max_changes_per_staging_item
    return max(1, value)


def get_display_parameters(
    sync,
    *,
    max_changes_per_staging_item_default,
):
    parameters = {}
    network_id = sync.get_network_id() or ""
    if network_id:
        parameters["network_id"] = network_id
    parameters["snapshot_id"] = sync.get_snapshot_id()
    parameters["auto_merge"] = bool(
        (sync.parameters or {}).get("auto_merge", sync.auto_merge)
    )
    parameters["diff_fallback_mode"] = (sync.parameters or {}).get(
        "diff_fallback_mode",
        ForwardDiffFallbackModeChoices.ALLOW_FALLBACK,
    )
    parameters["max_changes_per_staging_item"] = get_max_changes_per_staging_item(
        sync,
        max_changes_per_staging_item_default,
    )
    # Always expose canonical standing-schedule intent so API clients can
    # round-trip disabled schedules without changing their meaning.
    for intent_key in (
        "validation_schedule_interval",
        "preview_schedule_interval",
    ):
        parameters[intent_key] = int((sync.parameters or {}).get(intent_key) or 0)
    model_change_density = get_model_change_density(sync)
    density_profile = get_model_change_density_profile(sync)
    if model_change_density:
        parameters["model_change_density"] = model_change_density
    if density_profile:
        parameters["model_change_density_profile"] = density_profile_summary(
            density_map=model_change_density,
            density_profile=density_profile,
            default_density_map=DEFAULT_MODEL_CHANGE_DENSITY,
        )
    enabled_models = sync.get_model_strings()
    if enabled_models:
        parameters["branch_budget_hints"] = build_branch_budget_hints(
            enabled_models,
            max_changes_per_staging_item=parameters["max_changes_per_staging_item"],
            model_change_density=model_change_density,
            model_change_density_profile=density_profile,
        )
        parameters["branch_budget_density_policy"] = (
            branch_budget_density_policy_summary(
                enabled_models,
                model_change_density=model_change_density,
                model_change_density_profile=density_profile,
            )
        )
    parameters["models"] = enabled_models
    return parameters


def get_execution_summary(sync):
    enabled_models = sync.get_model_strings()
    max_changes_per_staging_item = get_max_changes_per_staging_item(
        sync,
        sync.get_max_changes_per_staging_item(),
    )
    model_change_density = get_model_change_density(sync)
    density_profile = get_model_change_density_profile(sync)
    last_ingestion = sync.last_ingestion
    summary = build_sync_execution_summary(
        enabled_models=enabled_models,
        max_changes_per_staging_item=max_changes_per_staging_item,
        model_change_density=model_change_density,
        model_change_density_profile=density_profile,
        latest_ingestion_summary=(
            last_ingestion.get_execution_summary() if last_ingestion else None
        ),
    )
    return summary


def get_analysis_summary(sync):
    last_ingestion = sync.last_ingestion
    latest_validation_run = sync.latest_validation_run
    summary = {
        "enabled_models": list(sync.get_model_strings()),
        "latest_validation_run": (
            latest_validation_run.pk if latest_validation_run else None
        ),
        "latest_validation_status": (
            latest_validation_run.status if latest_validation_run else ""
        ),
        "latest_ingestion": None,
        "latest_ingestion_analysis_summary": {},
        "query_path_resolution": {},
        "query_modes": {},
        "dependency_lookup_cache": {},
        "dependency_parent_coverage": {},
    }
    if last_ingestion is not None:
        latest_execution_summary = last_ingestion.get_execution_summary()
        latest_ingestion_analysis_summary = last_ingestion.get_analysis_summary()
        summary["latest_ingestion"] = latest_ingestion_analysis_summary
        summary["latest_ingestion_analysis_summary"] = latest_ingestion_analysis_summary
        summary["query_modes"] = latest_execution_summary.get("query_modes", {})
        summary["query_path_resolution"] = latest_execution_summary.get(
            "query_path_resolution", {}
        )
        summary["dependency_lookup_cache"] = latest_execution_summary.get(
            "dependency_lookup_cache", {}
        )
        summary["dependency_parent_coverage"] = latest_execution_summary.get(
            "dependency_parent_coverage", {}
        )
        summary["baseline_ready"] = bool(last_ingestion.baseline_ready)
        summary["sync_mode"] = last_ingestion.sync_mode or ""
    return summary


def get_workload_summary(sync):
    enabled_models = sync.get_model_strings()
    max_changes_per_staging_item = get_max_changes_per_staging_item(
        sync,
        sync.get_max_changes_per_staging_item(),
    )
    model_change_density = get_model_change_density(sync)
    density_profile = get_model_change_density_profile(sync)
    summary = {
        "enabled_models": list(enabled_models),
        "max_changes_per_staging_item": max_changes_per_staging_item,
        "model_change_density": dict(model_change_density or {}),
        "model_change_density_profile": density_profile_summary(
            density_map=model_change_density,
            density_profile=density_profile,
            default_density_map=DEFAULT_MODEL_CHANGE_DENSITY,
        ),
        "branch_budget_hints": build_branch_budget_hints(
            enabled_models,
            max_changes_per_staging_item=max_changes_per_staging_item,
            model_change_density=model_change_density,
            model_change_density_profile=density_profile,
        ),
        "branch_budget_density_policy": branch_budget_density_policy_summary(
            enabled_models,
            model_change_density=model_change_density,
            model_change_density_profile=density_profile,
        ),
        "baseline_ready": (
            bool(sync.last_ingestion.baseline_ready) if sync.last_ingestion else False
        ),
    }
    summary["initial_baseline_lane"] = get_initial_baseline_lane_advice(
        sync,
        workload_summary=summary,
    )
    summary["branching_guidance"] = get_branching_guidance(
        sync,
        lane_advice=summary["initial_baseline_lane"],
    )
    return summary


def get_branching_guidance(sync, *, lane_advice=None):
    advice = lane_advice or get_initial_baseline_lane_advice(sync)
    max_changes_per_staging_item = get_max_changes_per_staging_item(
        sync,
        sync.get_max_changes_per_staging_item(),
    )
    if advice.get("status") != "warn":
        return {}
    if advice.get("confidence") == "unknown":
        return {}
    return {
        "severity": "warning",
        "message": advice.get("message") or "",
        "max_changes_per_staging_item": max_changes_per_staging_item,
    }


def get_initial_baseline_lane_advice(sync, *, workload_summary=None):
    workload_summary = workload_summary or {}
    preview = workload_summary.get("pre_run_estimate") or {}
    max_changes_per_staging_item = get_max_changes_per_staging_item(
        sync,
        sync.get_max_changes_per_staging_item(),
    )
    planned_shards = _safe_int(preview.get("planned_shards"))
    estimated_changes = _safe_int(preview.get("estimated_changes"))
    model_estimates = _model_estimates_from_preview(preview)
    delete_heavy_models = _delete_heavy_models_from_preview(preview)
    baseline_ready = _has_baseline_ready_ingestion(sync)
    first_baseline = not baseline_ready
    large_baseline = _large_baseline(
        planned_shards=planned_shards,
        estimated_changes=estimated_changes,
        max_changes_per_staging_item=max_changes_per_staging_item,
    )
    runtime_projection = _runtime_projection(sync, planned_shards)
    retry_risk = str(preview.get("retry_risk") or "").strip() or "unknown"
    risk = _baseline_lane_risk(
        first_baseline=first_baseline,
        large_baseline=large_baseline,
        retry_risk=retry_risk,
        delete_heavy_models=delete_heavy_models,
    )

    recommendation = _baseline_lane_recommendation(
        first_baseline=first_baseline,
        large_baseline=large_baseline,
        retry_risk=retry_risk,
        delete_heavy_models=delete_heavy_models,
        planned_shards=planned_shards,
        estimated_changes=estimated_changes,
    )
    message = _baseline_lane_message(
        recommendation=recommendation,
        planned_shards=planned_shards,
        estimated_changes=estimated_changes,
        runtime_projection=runtime_projection,
    )
    return {
        "status": recommendation["status"],
        "severity": recommendation["severity"],
        "confidence": runtime_projection["confidence"],
        "recommendation": recommendation["code"],
        "recommendation_label": recommendation["label"],
        "message": message,
        "first_baseline": first_baseline,
        "baseline_ready": baseline_ready,
        "lane_risk": risk,
        "runtime_class": runtime_projection["runtime_class"],
        "projected_seconds": runtime_projection["projected_seconds"],
        "estimate": {
            "planned_shards": planned_shards,
            "estimated_changes": estimated_changes,
            "model_count": _safe_int(preview.get("model_count")),
            "retry_risk": retry_risk,
            "max_changes_per_staging_item": max_changes_per_staging_item,
            "models": model_estimates,
            "delete_heavy_models": delete_heavy_models,
        },
        "capacity_actions": (
            _single_branch_capacity_actions()
            if recommendation["status"] == "warn"
            else []
        ),
        "reason_codes": recommendation["reason_codes"],
    }


def _large_baseline(*, planned_shards, estimated_changes, max_changes_per_staging_item):
    if planned_shards >= INITIAL_BASELINE_LARGE_SHARD_THRESHOLD:
        return True
    return bool(
        estimated_changes
        and estimated_changes
        >= (max_changes_per_staging_item * INITIAL_BASELINE_LARGE_CHANGE_MULTIPLIER)
    )


def _baseline_lane_risk(
    *,
    first_baseline,
    large_baseline,
    retry_risk,
    delete_heavy_models,
):
    if first_baseline and large_baseline:
        return "high"
    if large_baseline or retry_risk in {"high", "medium"} or delete_heavy_models:
        return "medium"
    return "low"


def _baseline_lane_recommendation(
    *,
    first_baseline,
    large_baseline,
    retry_risk,
    delete_heavy_models,
    planned_shards,
    estimated_changes,
):
    if not planned_shards and not estimated_changes:
        return {
            "code": "collect_pre_run_estimate",
            "label": "Estimate unavailable",
            "status": "info",
            "severity": "info",
            "reason_codes": ["no_plan_preview"],
        }
    if first_baseline and large_baseline:
        return {
            "code": "single_branch_capacity_review",
            "label": "Review single-branch capacity",
            "status": "warn",
            "severity": "warning",
            "reason_codes": ["first_baseline", "large_branching_projection"],
        }
    if large_baseline or retry_risk == "high" or delete_heavy_models:
        return {
            "code": "single_branch_with_tuning",
            "label": "Single branch with tuning",
            "status": "info",
            "severity": "info",
            "reason_codes": ["review_lane", "capacity_or_delete_pressure"],
        }
    return {
        "code": "single_branch_bounded_review",
        "label": "Single branch",
        "status": "pass",
        "severity": "success",
        "reason_codes": ["bounded_branching_projection"],
    }


def _baseline_lane_message(
    *,
    recommendation,
    planned_shards,
    estimated_changes,
    runtime_projection,
):
    if recommendation["code"] == "collect_pre_run_estimate":
        return (
            "No workload estimate is available yet. Run validation/planning or inspect "
            "recent benchmark evidence before starting a very large first baseline."
        )
    estimate = (
        f"{planned_shards} workload unit(s), {estimated_changes} estimated change(s)"
    )
    runtime_class = runtime_projection["runtime_class"]
    if recommendation["code"] == "single_branch_capacity_review":
        return (
            f"The projected single-branch baseline is large ({estimate}; runtime "
            f"class {runtime_class}). Verify worker timeout, Postgres capacity, "
            "and branch ObjectChange volume before running it."
        )
    if recommendation["code"] == "single_branch_with_tuning":
        return (
            f"The single-branch run has scale or delete "
            f"pressure ({estimate}; runtime class {runtime_class}). Tune capacity "
            "or review delete/fallback pressure before restarting."
        )
    return (
        f"The single-branch projection is bounded ({estimate}; runtime class "
        f"{runtime_class})."
    )


def _runtime_projection(sync, planned_shards):
    average_seconds = _recent_average_workload_seconds(sync)
    if planned_shards and average_seconds:
        projected = round(float(planned_shards) * average_seconds, 3)
        return {
            "runtime_class": _runtime_class(projected),
            "projected_seconds": projected,
            "confidence": "medium",
            "basis": "recent_workload_units",
        }
    if planned_shards >= INITIAL_BASELINE_DAY_SHARD_THRESHOLD:
        return {
            "runtime_class": "days",
            "projected_seconds": None,
            "confidence": "low",
            "basis": "planned_workload_unit_count",
        }
    if planned_shards >= INITIAL_BASELINE_LARGE_SHARD_THRESHOLD:
        return {
            "runtime_class": "hours",
            "projected_seconds": None,
            "confidence": "low",
            "basis": "planned_workload_unit_count",
        }
    if planned_shards:
        return {
            "runtime_class": "minutes",
            "projected_seconds": None,
            "confidence": "low",
            "basis": "planned_workload_unit_count",
        }
    return {
        "runtime_class": "unknown",
        "projected_seconds": None,
        "confidence": "unknown",
        "basis": "insufficient_evidence",
    }


def _runtime_class(seconds):
    if seconds >= RUNTIME_CLASS_DAYS_SECONDS:
        return "days"
    if seconds >= RUNTIME_CLASS_MINUTES_SECONDS:
        return "hours"
    return "minutes"


def _recent_average_workload_seconds(sync):
    ingestion = getattr(sync, "last_ingestion", None)
    if ingestion is None:
        return None
    summary = ingestion.get_execution_summary()
    runtime_ms = float(summary.get("runtime_ms") or 0.0)
    workload_units = int(summary.get("shard_count") or 0)
    if runtime_ms <= 0 or workload_units <= 0:
        return None
    return runtime_ms / 1000.0 / workload_units


def _seconds_between(started, completed):
    if not started or not completed:
        return None
    try:
        return max(0.0, (completed - started).total_seconds())
    except (TypeError, ValueError):
        return None


def _model_estimates_from_preview(preview):
    models = preview.get("models") or {}
    if not isinstance(models, dict):
        return {}
    return {
        str(model): {
            "estimated_changes": _safe_int(values.get("estimated_changes")),
            "shard_count": _safe_int(values.get("shard_count")),
            "max_shard_changes": _safe_int(values.get("max_shard_changes")),
            "budget": _safe_int(values.get("budget")),
        }
        for model, values in sorted(models.items())
        if isinstance(values, dict)
    }


def _delete_heavy_models_from_preview(preview):
    delete_plan = preview.get("delete_dependency_plan") or {}
    models = delete_plan.get("models") or {}
    if not isinstance(models, dict):
        return []
    rows = []
    for model, values in sorted(models.items()):
        if not isinstance(values, dict):
            continue
        delete_rows = _safe_int(values.get("delete_rows"))
        if not delete_rows:
            continue
        risk = str(values.get("reference_blocker_risk") or "unknown")
        rows.append(
            {
                "model": str(model),
                "delete_rows": delete_rows,
                "delete_shards": _safe_int(values.get("delete_shards")),
                "reference_blocker_risk": risk,
            }
        )
    return sorted(rows, key=lambda item: item["delete_rows"], reverse=True)


def _single_branch_capacity_actions():
    return [
        "Set the worker timeout above the projected stage and merge runtime.",
        "Confirm Postgres capacity for the projected branch ObjectChange volume.",
        "Use repository query paths or IDs so later snapshots can use diffs.",
    ]


def _has_baseline_ready_ingestion(sync):
    try:
        return sync.latest_baseline_ingestion() is not None
    except Exception:
        return (
            bool(sync.last_ingestion.baseline_ready) if sync.last_ingestion else False
        )


def _safe_int(value):
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def get_advisory_summary(sync):
    summary = get_workload_summary(sync)
    last_ingestion = sync.last_ingestion
    if last_ingestion is not None:
        latest_execution_summary = last_ingestion.get_execution_summary()
        latest_ingestion_advisory_summary = last_ingestion.get_advisory_summary()
        latest_ingestion_analysis_summary = last_ingestion.get_analysis_summary()
        summary["latest_ingestion"] = latest_ingestion_advisory_summary
        summary["analysis_summary"] = latest_ingestion_analysis_summary
        summary["latest_ingestion_analysis_summary"] = latest_ingestion_analysis_summary
        summary["query_modes"] = latest_execution_summary.get("query_modes", {})
        summary["query_path_resolution"] = latest_execution_summary.get(
            "query_path_resolution", {}
        )
        summary["dependency_lookup_cache"] = latest_execution_summary.get(
            "dependency_lookup_cache", {}
        )
        summary["dependency_parent_coverage"] = latest_execution_summary.get(
            "dependency_parent_coverage", {}
        )
    latest_validation_run = sync.latest_validation_run
    if latest_validation_run is not None:
        summary["latest_validation_run"] = latest_validation_run.pk
        summary["latest_validation_status"] = latest_validation_run.status
        summary["latest_validation_allowed"] = latest_validation_run.allowed
        summary["latest_validation_drift_summary"] = dict(
            latest_validation_run.drift_summary or {}
        )
    return summary


def get_sync_activity(sync):
    if sync.status == ForwardSyncStatusChoices.SYNCING:
        return "Sync is running."
    if sync.status == ForwardSyncStatusChoices.READY_TO_MERGE:
        return "Waiting for branch merge."
    return ""


def get_job_logs(job):
    if not job:
        return {}
    return job.data or {}
