from types import SimpleNamespace

from django.core.cache import cache
from django.utils import timezone
from django.utils.dateparse import parse_datetime

from ..choices import ForwardDiffFallbackModeChoices
from ..choices import ForwardExecutionBackendChoices
from ..choices import ForwardExecutionRunStatusChoices
from ..choices import ForwardSyncStatusChoices
from .branch_budget import branch_budget_density_policy_summary
from .branch_budget import BRANCH_RUN_STATE_PARAMETER
from .branch_budget import build_branch_budget_hints
from .branch_budget import DEFAULT_MODEL_CHANGE_DENSITY
from .branch_budget import MODEL_CHANGE_DENSITY_PARAMETER
from .branch_budget import MODEL_CHANGE_DENSITY_PROFILE_PARAMETER
from .density_learning import density_profile_summary
from .density_learning import normalize_density_map
from .density_learning import normalize_density_profile
from .execution_ledger_serialization import (
    dependency_lookup_cache_support_summary as _dependency_lookup_cache_support_summary,
)
from .execution_telemetry import build_branch_run_summary
from .execution_telemetry import build_sync_execution_summary
from .job_liveness import job_has_live_execution

STALE_BRANCH_PROGRESS_SECONDS = 15 * 60
INITIAL_BASELINE_LARGE_SHARD_THRESHOLD = 10
INITIAL_BASELINE_LARGE_CHANGE_MULTIPLIER = 5
INITIAL_BASELINE_DAY_SHARD_THRESHOLD = 150
RUNTIME_CLASS_MINUTES_SECONDS = 60 * 60
RUNTIME_CLASS_DAYS_SECONDS = 24 * 60 * 60
PROGRESS_STATE_KEYS = (
    "last_progress_message",
    "last_progress_at",
    "current_model_string",
    "current_shard_index",
    "current_row_count",
    "current_row_total",
)


def get_branch_run_state(sync):
    state = (sync.parameters or {}).get(BRANCH_RUN_STATE_PARAMETER) or {}
    if not isinstance(state, dict):
        return {}
    if state and _has_execution_runs(sync):
        # Once ledger history exists, compatibility state is no longer an
        # authoritative runtime surface. Opportunistically prune if no run is
        # active, and always present compatibility state as empty to callers.
        prune_stale_branch_run_state(sync)
        return {}
    return state


def get_branch_run_display_state(sync):
    state = get_branch_run_state(sync)
    run = _active_execution_run(sync)
    if run is not None:
        from .execution_ledger import branch_run_state_from_execution_run

        return branch_run_state_from_execution_run(run)
    if _has_execution_runs(sync):
        # Once ledger runs exist, compatibility JSON is no longer a display
        # surface when no active run is present.
        return {}
    return state


def get_execution_display_state(sync):
    return get_branch_run_display_state(sync)


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


def set_branch_run_state(sync, state):
    from .execution_ledger import active_execution_run

    active_run = active_execution_run(sync)
    # Keep compatibility payload strictly read-only once a real execution run
    # exists. Ledger rows are the active orchestration source.
    if active_run is not None:
        return False
    if _has_execution_runs(sync):
        # Once any execution run history exists, compatibility JSON is only a
        # read-through upgrade bridge and must not be mutated by active runtime
        # paths.
        return False
    parameters = dict(sync.parameters or {})
    parameters[BRANCH_RUN_STATE_PARAMETER] = state or {}
    sync.parameters = parameters
    sync.__class__.objects.filter(pk=sync.pk).update(parameters=parameters)
    return True


def clear_branch_run_state(sync):
    parameters = dict(sync.parameters or {})
    if BRANCH_RUN_STATE_PARAMETER in parameters:
        parameters.pop(BRANCH_RUN_STATE_PARAMETER, None)
        sync.parameters = parameters
        sync.__class__.objects.filter(pk=sync.pk).update(parameters=parameters)


def prune_stale_branch_run_state(sync):
    """Drop legacy compatibility state once only ledger history remains."""
    if not getattr(sync, "pk", None):
        return False
    if not _has_execution_runs(sync):
        return False
    if _active_execution_run(sync) is not None:
        return False
    parameters = dict(sync.parameters or {})
    if BRANCH_RUN_STATE_PARAMETER not in parameters:
        return False
    parameters.pop(BRANCH_RUN_STATE_PARAMETER, None)
    sync.parameters = parameters
    sync.__class__.objects.filter(pk=sync.pk).update(parameters=parameters)
    return True


def clear_branch_run_progress_fields(state):
    for key in PROGRESS_STATE_KEYS:
        state.pop(key, None)
    return state


def mark_branch_run_failed(sync, message):
    from .execution_ledger import active_execution_run

    run = active_execution_run(sync)
    if run is not None:
        run.status = ForwardExecutionRunStatusChoices.FAILED
        run.phase = "failed"
        run.phase_message = str(message)
        run.last_error = str(message)
        run.latest_heartbeat = timezone.now()
        run.save(
            update_fields=[
                "status",
                "phase",
                "phase_message",
                "last_error",
                "latest_heartbeat",
            ]
        )
        from .fetch_artifacts import prune_fetch_artifacts_for_run

        prune_fetch_artifacts_for_run(run.pk)
        return True
    state = get_branch_run_display_state(sync)
    if not state:
        return False
    clear_branch_run_progress_fields(state)
    state["phase"] = "failed"
    state["phase_message"] = str(message)
    state["phase_started"] = timezone.now().isoformat()
    state["awaiting_merge"] = False
    set_branch_run_state(sync, state)
    return True


def touch_branch_run_progress(
    sync,
    *,
    phase_message=None,
    model_string=None,
    shard_index=None,
    total_plan_items=None,
    row_count=None,
    row_total=None,
):
    from .execution_ledger import active_execution_run
    from .execution_ledger import touch_execution_step_progress

    run = active_execution_run(sync)
    if run is not None:
        if phase_message is not None:
            run.phase_message = str(phase_message)
        updated_fields = ["phase_message"]
        if shard_index is not None:
            run.next_step_index = int(shard_index)
            updated_fields.append("next_step_index")
        if total_plan_items is not None:
            run.total_steps = int(total_plan_items)
            updated_fields.append("total_steps")
        if model_string is not None or row_count is not None or row_total is not None:
            touch_execution_step_progress(
                sync,
                model_string=model_string or "",
                shard_index=shard_index,
                row_count=row_count,
                row_total=row_total,
            )
        run.latest_heartbeat = timezone.now()
        updated_fields.append("latest_heartbeat")
        run.save(update_fields=updated_fields)
        return True
    state = get_branch_run_display_state(sync)
    if not state:
        return False
    if phase_message is not None:
        state["last_progress_message"] = str(phase_message)
    if model_string is not None:
        state["current_model_string"] = str(model_string)
    if shard_index is not None:
        state["current_shard_index"] = int(shard_index)
    if total_plan_items is not None:
        state["total_plan_items"] = int(total_plan_items)
    if row_count is not None:
        state["current_row_count"] = int(row_count)
    if row_total is not None:
        state["current_row_total"] = int(row_total)
    state["last_progress_at"] = timezone.now().isoformat()
    set_branch_run_state(sync, state)
    return True


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


def is_waiting_for_branch_merge(sync):
    run = _active_execution_run(sync)
    if run is None:
        if _has_execution_runs(sync):
            return False
        return bool(get_branch_run_state(sync).get("awaiting_merge"))
    from ..choices import ForwardExecutionStepStatusChoices

    return run.steps.filter(
        status=ForwardExecutionStepStatusChoices.STAGED,
    ).exists()


def has_pending_branch_run(sync):
    run = _active_execution_run(sync)
    if run is None:
        if _has_execution_runs(sync):
            return False
        return bool(get_branch_run_state(sync))
    from ..choices import ForwardExecutionRunStatusChoices
    from ..choices import ForwardExecutionStepKindChoices
    from ..choices import ForwardExecutionStepStatusChoices

    if run.status == ForwardExecutionRunStatusChoices.COMPLETED:
        return False
    return (
        run.steps.filter(
            kind=ForwardExecutionStepKindChoices.STAGE,
        )
        .exclude(
            status__in=[
                ForwardExecutionStepStatusChoices.MERGED,
                ForwardExecutionStepStatusChoices.SKIPPED,
                ForwardExecutionStepStatusChoices.CANCELLED,
            ]
        )
        .exists()
    )


def ready_for_sync(sync):
    return not is_waiting_for_branch_merge(sync) and sync.status not in (
        ForwardSyncStatusChoices.QUEUED,
        ForwardSyncStatusChoices.SYNCING,
        ForwardSyncStatusChoices.MERGING,
    )


def ready_to_continue_sync(sync):
    return has_pending_branch_run(sync) and ready_for_sync(sync)


def _active_execution_run(sync):
    if not getattr(sync, "pk", None):
        return None
    try:
        from .execution_ledger import active_execution_run

        return active_execution_run(sync)
    except Exception:
        return None


def _has_execution_runs(sync):
    if not getattr(sync, "pk", None):
        return False
    try:
        return sync.execution_runs.exists()
    except Exception:
        return False


def get_max_changes_per_branch(sync, default_max_changes_per_branch):
    try:
        value = int(
            (sync.parameters or {}).get(
                "max_changes_per_branch",
                default_max_changes_per_branch,
            )
        )
    except (TypeError, ValueError):
        return default_max_changes_per_branch
    return max(1, value)


def format_timestamp_elapsed(timestamp):
    if not timestamp:
        return ""
    started = parse_datetime(str(timestamp))
    if started is None:
        return ""
    if timezone.is_naive(started):
        started = timezone.make_aware(started, timezone.get_current_timezone())
    elapsed_seconds = max(0, int((timezone.now() - started).total_seconds()))
    minutes, seconds = divmod(elapsed_seconds, 60)
    hours, minutes = divmod(minutes, 60)
    if hours:
        return f"{hours}h {minutes}m"
    if minutes:
        return f"{minutes}m {seconds}s"
    return f"{seconds}s"


def format_phase_elapsed(phase_started):
    return format_timestamp_elapsed(phase_started)


def branch_progress_stale(sync, timestamp):
    if sync.status not in (
        ForwardSyncStatusChoices.SYNCING,
        ForwardSyncStatusChoices.MERGING,
    ):
        return False
    if not timestamp:
        return False
    started = parse_datetime(str(timestamp))
    if started is None:
        return False
    if timezone.is_naive(started):
        started = timezone.make_aware(started, timezone.get_current_timezone())
    elapsed_seconds = max(0, int((timezone.now() - started).total_seconds()))
    return elapsed_seconds >= STALE_BRANCH_PROGRESS_SECONDS


def get_display_parameters(
    sync,
    *,
    max_changes_per_branch_default,
):
    parameters = {}
    parameters["execution_backend"] = (sync.parameters or {}).get(
        "execution_backend",
        ForwardExecutionBackendChoices.BRANCHING,
    )
    network_id = sync.get_network_id() or ""
    if network_id:
        parameters["network_id"] = network_id
    parameters["snapshot_id"] = sync.get_snapshot_id()
    parameters["auto_merge"] = bool(
        (sync.parameters or {}).get("auto_merge", sync.auto_merge)
    )
    parameters["multi_branch"] = sync.uses_multi_branch()
    parameters["diff_fallback_mode"] = (sync.parameters or {}).get(
        "diff_fallback_mode",
        ForwardDiffFallbackModeChoices.ALLOW_FALLBACK,
    )
    parameters["max_changes_per_branch"] = get_max_changes_per_branch(
        sync,
        max_changes_per_branch_default,
    )
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
            max_changes_per_branch=parameters["max_changes_per_branch"],
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
    state = get_branch_run_display_state(sync)
    if state:
        branch_run = build_branch_run_summary(state)
        parameters["branch_run"] = _compact_display_branch_run(branch_run)
    parameters["models"] = enabled_models
    return parameters


def _compact_display_branch_run(branch_run):
    summary = dict(branch_run or {})
    plan_items = summary.pop("plan_items", None)
    if isinstance(plan_items, list):
        summary["plan_items_count"] = len(plan_items)
    preview = summary.get("plan_preview")
    if isinstance(preview, dict) and len(preview) > 25:
        summary["plan_preview"] = {
            "planned_shards": preview.get("planned_shards"),
            "planned_changes": preview.get("planned_changes"),
            "truncated": True,
        }
    return summary


def get_execution_summary(sync):
    enabled_models = sync.get_model_strings()
    max_changes_per_branch = get_max_changes_per_branch(
        sync,
        sync.get_max_changes_per_branch(),
    )
    model_change_density = get_model_change_density(sync)
    density_profile = get_model_change_density_profile(sync)
    state = get_branch_run_display_state(sync)
    last_ingestion = sync.last_ingestion
    return build_sync_execution_summary(
        enabled_models=enabled_models,
        max_changes_per_branch=max_changes_per_branch,
        model_change_density=model_change_density,
        model_change_density_profile=density_profile,
        branch_run_state=state,
        latest_ingestion_summary=(
            last_ingestion.get_execution_summary() if last_ingestion else None
        ),
    )


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
    }
    if last_ingestion is not None:
        summary["latest_ingestion"] = last_ingestion.get_analysis_summary()
        summary["latest_ingestion_analysis_summary"] = (
            last_ingestion.get_analysis_summary()
        )
        summary["query_modes"] = last_ingestion.get_execution_summary().get(
            "query_modes", {}
        )
        summary["query_path_resolution"] = last_ingestion.get_execution_summary().get(
            "query_path_resolution", {}
        )
        summary["dependency_lookup_cache"] = _dependency_lookup_cache_support_summary(
            SimpleNamespace(job=last_ingestion.job)
        )
        summary["baseline_ready"] = bool(last_ingestion.baseline_ready)
        summary["sync_mode"] = last_ingestion.sync_mode or ""
    return summary


def get_workload_summary(sync):
    enabled_models = sync.get_model_strings()
    max_changes_per_branch = get_max_changes_per_branch(
        sync,
        sync.get_max_changes_per_branch(),
    )
    model_change_density = get_model_change_density(sync)
    density_profile = get_model_change_density_profile(sync)
    state = get_branch_run_display_state(sync)
    summary = {
        "enabled_models": list(enabled_models),
        "max_changes_per_branch": max_changes_per_branch,
        "model_change_density": dict(model_change_density or {}),
        "model_change_density_profile": density_profile_summary(
            density_map=model_change_density,
            density_profile=density_profile,
            default_density_map=DEFAULT_MODEL_CHANGE_DENSITY,
        ),
        "branch_budget_hints": build_branch_budget_hints(
            enabled_models,
            max_changes_per_branch=max_changes_per_branch,
            model_change_density=model_change_density,
            model_change_density_profile=density_profile,
        ),
        "branch_budget_density_policy": branch_budget_density_policy_summary(
            enabled_models,
            model_change_density=model_change_density,
            model_change_density_profile=density_profile,
        ),
        "branch_run": build_branch_run_summary(state) if state else {},
        "pre_run_estimate": state.get("plan_preview") or {},
        "baseline_ready": (
            bool(sync.last_ingestion.baseline_ready) if sync.last_ingestion else False
        ),
        "execution_backend": (
            (sync.parameters or {}).get("execution_backend")
            or ForwardExecutionBackendChoices.BRANCHING
        ),
        "uses_multi_branch": sync.uses_multi_branch(),
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
    max_changes_per_branch = get_max_changes_per_branch(
        sync,
        sync.get_max_changes_per_branch(),
    )
    execution_backend = (sync.parameters or {}).get(
        "execution_backend"
    ) or ForwardExecutionBackendChoices.BRANCHING
    if execution_backend != ForwardExecutionBackendChoices.BRANCHING:
        return {}
    if (
        advice.get("recommended_backend")
        != ForwardExecutionBackendChoices.FAST_BOOTSTRAP
    ):
        return {}
    if advice.get("confidence") == "unknown":
        return {}
    return {
        "severity": "warning",
        "message": advice.get("message") or "",
        "max_changes_per_branch": max_changes_per_branch,
    }


def get_initial_baseline_lane_advice(sync, *, workload_summary=None):
    workload_summary = workload_summary or {}
    state = get_branch_run_display_state(sync)
    preview = (
        workload_summary.get("pre_run_estimate") or state.get("plan_preview") or {}
    )
    max_changes_per_branch = get_max_changes_per_branch(
        sync,
        sync.get_max_changes_per_branch(),
    )
    planned_shards = _safe_int(preview.get("planned_shards"))
    estimated_changes = _safe_int(preview.get("estimated_changes"))
    model_estimates = _model_estimates_from_preview(preview)
    delete_heavy_models = _delete_heavy_models_from_preview(preview)
    current_backend = (sync.parameters or {}).get(
        "execution_backend"
    ) or ForwardExecutionBackendChoices.BRANCHING
    baseline_ready = _has_baseline_ready_ingestion(sync)
    first_baseline = not baseline_ready
    large_baseline = _large_baseline(
        planned_shards=planned_shards,
        estimated_changes=estimated_changes,
        max_changes_per_branch=max_changes_per_branch,
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
        current_backend=current_backend,
        first_baseline=first_baseline,
        large_baseline=large_baseline,
        retry_risk=retry_risk,
        delete_heavy_models=delete_heavy_models,
        planned_shards=planned_shards,
        estimated_changes=estimated_changes,
    )
    message = _baseline_lane_message(
        recommendation=recommendation,
        current_backend=current_backend,
        planned_shards=planned_shards,
        estimated_changes=estimated_changes,
        runtime_projection=runtime_projection,
    )
    return {
        "status": recommendation["status"],
        "severity": recommendation["severity"],
        "confidence": runtime_projection["confidence"],
        "current_backend": current_backend,
        "recommended_backend": recommendation["recommended_backend"],
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
            "max_changes_per_branch": max_changes_per_branch,
            "models": model_estimates,
            "delete_heavy_models": delete_heavy_models,
        },
        "fast_bootstrap_confirmation": (
            _fast_bootstrap_confirmation()
            if recommendation["recommended_backend"]
            == ForwardExecutionBackendChoices.FAST_BOOTSTRAP
            or current_backend == ForwardExecutionBackendChoices.FAST_BOOTSTRAP
            else []
        ),
        "reason_codes": recommendation["reason_codes"],
    }


def _large_baseline(*, planned_shards, estimated_changes, max_changes_per_branch):
    if planned_shards >= INITIAL_BASELINE_LARGE_SHARD_THRESHOLD:
        return True
    return bool(
        estimated_changes
        and estimated_changes
        >= (max_changes_per_branch * INITIAL_BASELINE_LARGE_CHANGE_MULTIPLIER)
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
    current_backend,
    first_baseline,
    large_baseline,
    retry_risk,
    delete_heavy_models,
    planned_shards,
    estimated_changes,
):
    if current_backend == ForwardExecutionBackendChoices.FAST_BOOTSTRAP:
        return {
            "code": "fast_bootstrap_active",
            "label": "Fast bootstrap baseline",
            "recommended_backend": ForwardExecutionBackendChoices.FAST_BOOTSTRAP,
            "status": "info",
            "severity": "info",
            "reason_codes": ["fast_bootstrap_selected"],
        }
    if not planned_shards and not estimated_changes:
        return {
            "code": "collect_pre_run_estimate",
            "label": "Estimate unavailable",
            "recommended_backend": ForwardExecutionBackendChoices.BRANCHING,
            "status": "info",
            "severity": "info",
            "reason_codes": ["no_plan_preview"],
        }
    if first_baseline and large_baseline:
        return {
            "code": "use_fast_bootstrap_for_trusted_baseline",
            "label": "Use Fast bootstrap for trusted baseline",
            "recommended_backend": ForwardExecutionBackendChoices.FAST_BOOTSTRAP,
            "status": "warn",
            "severity": "warning",
            "reason_codes": ["first_baseline", "large_branching_projection"],
        }
    if large_baseline or retry_risk == "high" or delete_heavy_models:
        return {
            "code": "branching_with_tuning",
            "label": "Branching with tuning",
            "recommended_backend": ForwardExecutionBackendChoices.BRANCHING,
            "status": "info",
            "severity": "info",
            "reason_codes": ["review_lane", "capacity_or_delete_pressure"],
        }
    return {
        "code": "branching_bounded_review",
        "label": "Branching",
        "recommended_backend": ForwardExecutionBackendChoices.BRANCHING,
        "status": "pass",
        "severity": "success",
        "reason_codes": ["bounded_branching_projection"],
    }


def _baseline_lane_message(
    *,
    recommendation,
    current_backend,
    planned_shards,
    estimated_changes,
    runtime_projection,
):
    if recommendation["code"] == "collect_pre_run_estimate":
        return (
            "No shard estimate is available yet. Run validation/planning or inspect "
            "recent benchmark evidence before starting a very large first baseline."
        )
    estimate = (
        f"{planned_shards} planned shard(s), {estimated_changes} estimated change(s)"
    )
    runtime_class = runtime_projection["runtime_class"]
    if recommendation["code"] == "use_fast_bootstrap_for_trusted_baseline":
        return (
            f"Projected Branching baseline is large ({estimate}; runtime class "
            f"{runtime_class}). Use Fast bootstrap only if this is a trusted first "
            "baseline, then switch back to Branching for a later reviewable diff."
        )
    if recommendation["code"] == "branching_with_tuning":
        return (
            f"Branching remains the review lane, but this run has scale or delete "
            f"pressure ({estimate}; runtime class {runtime_class}). Tune capacity "
            "or review delete/fallback pressure before restarting."
        )
    if recommendation["code"] == "fast_bootstrap_active":
        return (
            "Fast bootstrap is selected. Confirm this is a trusted initial baseline; "
            "after it completes, switch back to Branching when later snapshots and "
            "query identity support reviewable diffs."
        )
    return (
        f"Branching is appropriate for this bounded projection ({estimate}; runtime "
        f"class {runtime_class})"
        if current_backend == ForwardExecutionBackendChoices.BRANCHING
        else f"Current backend is {current_backend}; projection is {estimate}."
    )


def _runtime_projection(sync, planned_shards):
    average_seconds = _recent_average_stage_seconds(sync)
    if planned_shards and average_seconds:
        projected = round(float(planned_shards) * average_seconds, 3)
        return {
            "runtime_class": _runtime_class(projected),
            "projected_seconds": projected,
            "confidence": "medium",
            "basis": "recent_execution_steps",
        }
    if planned_shards >= INITIAL_BASELINE_DAY_SHARD_THRESHOLD:
        return {
            "runtime_class": "days",
            "projected_seconds": None,
            "confidence": "low",
            "basis": "planned_shard_count",
        }
    if planned_shards >= INITIAL_BASELINE_LARGE_SHARD_THRESHOLD:
        return {
            "runtime_class": "hours",
            "projected_seconds": None,
            "confidence": "low",
            "basis": "planned_shard_count",
        }
    if planned_shards:
        return {
            "runtime_class": "minutes",
            "projected_seconds": None,
            "confidence": "low",
            "basis": "planned_shard_count",
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


def _recent_average_stage_seconds(sync):
    if not getattr(sync, "pk", None):
        return None
    try:
        from ..choices import ForwardExecutionStepKindChoices

        durations = []
        runs = sync.execution_runs.order_by("-pk")[:5]
        for run in runs:
            steps = run.steps.filter(
                kind=ForwardExecutionStepKindChoices.STAGE,
                started__isnull=False,
                completed__isnull=False,
            ).order_by("-completed")[:20]
            for step in steps:
                duration = _seconds_between(step.started, step.completed)
                if duration is not None:
                    durations.append(duration)
        if not durations:
            return None
        return sum(durations) / len(durations)
    except Exception:
        return None


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


def _fast_bootstrap_confirmation():
    return [
        "Use only for a trusted initial baseline.",
        "Fast bootstrap skips Branching review for the initial seed.",
        (
            "Switch back to Branching only after the baseline completes and a later "
            "snapshot/query identity can support reviewable diffs."
        ),
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
        summary["latest_ingestion"] = last_ingestion.get_advisory_summary()
        summary["analysis_summary"] = last_ingestion.get_analysis_summary()
        summary["latest_ingestion_analysis_summary"] = (
            last_ingestion.get_analysis_summary()
        )
        summary["query_modes"] = last_ingestion.get_execution_summary().get(
            "query_modes", {}
        )
        summary["query_path_resolution"] = last_ingestion.get_execution_summary().get(
            "query_path_resolution", {}
        )
        summary["dependency_lookup_cache"] = _dependency_lookup_cache_support_summary(
            SimpleNamespace(job=last_ingestion.job)
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
    run = _active_execution_run(sync)
    if run is not None and _should_reconcile_for_activity(run):
        try:
            from .execution_ledger import reconcile_execution_run

            reconcile_execution_run(run)
        except Exception:
            # Activity rendering should never hard-fail on reconciliation.
            pass
    state = get_branch_run_display_state(sync)
    progress_message = state.get("last_progress_message") or ""
    if not progress_message and state.get("current_model_string"):
        progress_message = f"Processing {state.get('current_model_string')}"
        shard_index = state.get("current_shard_index")
        total_plan_items = state.get("total_plan_items")
        if shard_index and total_plan_items:
            progress_message += f" shard {shard_index}/{total_plan_items}"
        row_count = state.get("current_row_count")
        row_total = state.get("current_row_total")
        if row_count and row_total:
            progress_message += f" ({row_count}/{row_total} rows)"
    progress_elapsed = format_timestamp_elapsed(state.get("last_progress_at"))
    phase_message = state.get("phase_message") or ""
    phase = state.get("phase") or ""
    elapsed = format_phase_elapsed(state.get("phase_started"))
    if progress_message:
        if branch_progress_stale(sync, state.get("last_progress_at")):
            return (
                f"No shard progress reported for {progress_elapsed}; "
                f"last update: {progress_message}"
            )
        return (
            f"{progress_message} ({progress_elapsed})"
            if progress_elapsed
            else progress_message
        )
    if phase_message:
        return f"{phase_message} ({elapsed})" if elapsed else phase_message
    if phase:
        phase_label = phase.replace("_", " ")
        return f"{phase_label} ({elapsed})" if elapsed else phase_label
    if sync.status == ForwardSyncStatusChoices.SYNCING:
        return "Sync is running."
    if is_waiting_for_branch_merge(sync):
        return "Waiting for branch merge."
    return ""


def get_job_logs(job):
    if not job:
        return {}
    if job.data:
        return job.data
    return cache.get(f"forward_sync_{job.pk}") or {}


def _should_reconcile_for_activity(run):
    try:
        from ..choices import ForwardExecutionStepKindChoices
        from ..choices import ForwardExecutionStepStatusChoices
        from .execution_ledger import TERMINAL_RUN_STATUSES

        if run.status in TERMINAL_RUN_STATUSES:
            return False
        step_qs = run.steps.filter(kind=ForwardExecutionStepKindChoices.STAGE)
        inflight = step_qs.filter(
            status__in=[
                ForwardExecutionStepStatusChoices.QUEUED,
                ForwardExecutionStepStatusChoices.RUNNING,
                ForwardExecutionStepStatusChoices.MERGE_QUEUED,
            ]
        )
        if inflight.count() > 1:
            return True
        running = inflight.filter(
            status=ForwardExecutionStepStatusChoices.RUNNING
        ).first()
        if (
            running is not None
            and running.job_id
            and getattr(running.job, "completed", None)
        ):
            return True
        queued = inflight.filter(
            status=ForwardExecutionStepStatusChoices.QUEUED
        ).first()
        if (
            queued is not None
            and queued.job_id
            and getattr(queued.job, "completed", None)
        ):
            return True
        merge_queued = inflight.filter(
            status=ForwardExecutionStepStatusChoices.MERGE_QUEUED
        ).first()
        for candidate in (running, queued, merge_queued):
            if candidate is None:
                continue
            candidate_job = (
                candidate.merge_job
                if candidate.status == ForwardExecutionStepStatusChoices.MERGE_QUEUED
                else candidate.job
            )
            if candidate_job is None:
                continue
            if not job_has_live_execution(candidate_job):
                return True
    except Exception:
        return False
    return False
