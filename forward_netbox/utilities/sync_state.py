from django.core.cache import cache
from django.utils import timezone
from django.utils.dateparse import parse_datetime

from ..choices import ForwardExecutionBackendChoices
from ..choices import ForwardSyncStatusChoices
from .branch_budget import BRANCH_RUN_STATE_PARAMETER
from .branch_budget import build_branch_budget_hints
from .branch_budget import MODEL_CHANGE_DENSITY_PARAMETER
from .execution_telemetry import build_branch_run_summary
from .execution_telemetry import build_sync_execution_summary

STALE_BRANCH_PROGRESS_SECONDS = 15 * 60


def get_branch_run_state(sync):
    state = (sync.parameters or {}).get(BRANCH_RUN_STATE_PARAMETER) or {}
    return state if isinstance(state, dict) else {}


def get_model_change_density(sync):
    density = (sync.parameters or {}).get(MODEL_CHANGE_DENSITY_PARAMETER) or {}
    return density if isinstance(density, dict) else {}


def set_branch_run_state(sync, state):
    parameters = dict(sync.parameters or {})
    parameters[BRANCH_RUN_STATE_PARAMETER] = dict(state)
    sync.parameters = parameters
    sync.__class__.objects.filter(pk=sync.pk).update(parameters=parameters)


def clear_branch_run_state(sync):
    parameters = dict(sync.parameters or {})
    if BRANCH_RUN_STATE_PARAMETER in parameters:
        parameters.pop(BRANCH_RUN_STATE_PARAMETER, None)
        sync.parameters = parameters
        sync.__class__.objects.filter(pk=sync.pk).update(parameters=parameters)


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
    state = get_branch_run_state(sync)
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
    normalized = {}
    for model_string, density in (model_change_density or {}).items():
        try:
            density_value = float(density)
        except (TypeError, ValueError):
            continue
        if density_value <= 0:
            continue
        normalized[str(model_string)] = density_value
    parameters = dict(sync.parameters or {})
    parameters[MODEL_CHANGE_DENSITY_PARAMETER] = normalized
    sync.parameters = parameters
    sync.__class__.objects.filter(pk=sync.pk).update(parameters=parameters)


def is_waiting_for_branch_merge(sync):
    return bool(get_branch_run_state(sync).get("awaiting_merge"))


def has_pending_branch_run(sync):
    state = get_branch_run_state(sync)
    return bool(
        state
        and int(state.get("next_plan_index") or 1)
        <= int(state.get("total_plan_items") or 0)
    )


def ready_for_sync(sync):
    return not is_waiting_for_branch_merge(sync) and sync.status not in (
        ForwardSyncStatusChoices.QUEUED,
        ForwardSyncStatusChoices.SYNCING,
        ForwardSyncStatusChoices.MERGING,
    )


def ready_to_continue_sync(sync):
    return has_pending_branch_run(sync) and ready_for_sync(sync)


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
    parameters["max_changes_per_branch"] = get_max_changes_per_branch(
        sync,
        max_changes_per_branch_default,
    )
    model_change_density = get_model_change_density(sync)
    if model_change_density:
        parameters["model_change_density"] = model_change_density
    enabled_models = sync.get_model_strings()
    if enabled_models:
        parameters["branch_budget_hints"] = build_branch_budget_hints(
            enabled_models,
            max_changes_per_branch=parameters["max_changes_per_branch"],
            model_change_density=model_change_density,
        )
    state = get_branch_run_state(sync)
    if state:
        parameters["branch_run"] = build_branch_run_summary(state)
    parameters["models"] = enabled_models
    return parameters


def get_execution_summary(sync):
    enabled_models = sync.get_model_strings()
    max_changes_per_branch = get_max_changes_per_branch(
        sync,
        sync.get_max_changes_per_branch(),
    )
    model_change_density = get_model_change_density(sync)
    state = get_branch_run_state(sync)
    last_ingestion = sync.last_ingestion
    return build_sync_execution_summary(
        enabled_models=enabled_models,
        max_changes_per_branch=max_changes_per_branch,
        model_change_density=model_change_density,
        branch_run_state=state,
        latest_ingestion_summary=(
            last_ingestion.get_execution_summary() if last_ingestion else None
        ),
    )


def get_sync_activity(sync):
    state = get_branch_run_state(sync)
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
