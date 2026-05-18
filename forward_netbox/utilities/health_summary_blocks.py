from django.conf import settings

from .. import NetboxForwardConfig
from ..choices import ForwardExecutionBackendChoices
from ..choices import ForwardExecutionStepStatusChoices
from .execution_ledger import execution_run_recovery_recommendation
from .runtime_guidance import configured_rq_default_timeout
from .runtime_guidance import source_query_fetch_concurrency
from .runtime_guidance import source_timeout_seconds


def source_summary(sync):
    source = sync.source
    return {
        "id": source.pk,
        "name": source.name,
        "url": source.url,
        "status": source.status,
        "type": source.type,
        "last_synced": source.last_synced.isoformat() if source.last_synced else None,
    }


def runtime_summary(sync):
    branch_plugin_available = True
    try:
        import netbox_branching  # noqa: F401
    except Exception:
        branch_plugin_available = False

    return {
        "plugin_version": NetboxForwardConfig.version,
        "netbox_version": getattr(settings, "VERSION", ""),
        "branching_available": branch_plugin_available,
        "execution_backend": (sync.parameters or {}).get(
            "execution_backend",
            ForwardExecutionBackendChoices.BRANCHING,
        ),
        "auto_merge": bool(sync.auto_merge),
        "max_changes_per_branch": sync.get_max_changes_per_branch(),
        "source_timeout_seconds": source_timeout_seconds(sync),
        "query_fetch_concurrency": source_query_fetch_concurrency(sync),
        "rq_default_timeout_seconds": configured_rq_default_timeout(),
        "snapshot_selector": sync.get_snapshot_id(),
    }


def query_map_summary(query_map):
    return {
        "id": query_map.pk,
        "name": query_map.name,
        "model": query_map.model_string,
        "mode": query_map.execution_mode,
        "query_repository": query_map.query_repository or "",
        "query_path": query_map.query_path or "",
        "has_query_id": bool(query_map.query_id),
        "has_commit_id": bool(query_map.commit_id),
        "built_in": bool(query_map.built_in),
    }


def validation_summary(validation_run):
    if validation_run is None:
        return None
    return {
        "id": validation_run.pk,
        "status": validation_run.status,
        "allowed": bool(validation_run.allowed),
        "snapshot_selector": validation_run.snapshot_selector,
        "snapshot_id": validation_run.snapshot_id,
        "blocking_reason_count": len(validation_run.blocking_reasons or []),
        "created": validation_run.created.isoformat() if validation_run.created else None,
        "completed": (
            validation_run.completed.isoformat() if validation_run.completed else None
        ),
    }


def ingestion_summary(ingestion):
    if ingestion is None:
        return None
    return {
        "id": ingestion.pk,
        "name": ingestion.name,
        "sync_mode": ingestion.sync_mode or "",
        "baseline_ready": bool(ingestion.baseline_ready),
        "snapshot_selector": ingestion.snapshot_selector or "",
        "snapshot_id": ingestion.snapshot_id or "",
        "branch": ingestion.branch.name if ingestion.branch else "",
        "issue_count": ingestion.issues.count(),
        "applied_change_count": ingestion.applied_change_count,
        "failed_change_count": ingestion.failed_change_count,
        "created": ingestion.created.isoformat() if ingestion.created else None,
    }


def execution_run_summary(run):
    if run is None:
        return None
    return {
        "id": run.pk,
        "backend": run.backend,
        "status": run.status,
        "phase": run.phase,
        "phase_message": run.phase_message,
        "total_steps": run.total_steps,
        "next_step_index": run.next_step_index,
        "baseline_ready": bool(run.baseline_ready),
        "recovery_recommendation": execution_run_recovery_recommendation(run),
        "latest_heartbeat": (
            run.latest_heartbeat.isoformat() if run.latest_heartbeat else None
        ),
        "last_error": run.last_error,
    }


def capacity_summary(run):
    if run is None:
        return {
            "available": False,
            "message": "No execution run is available for capacity projection.",
        }
    steps = list(run.steps.all())
    completed_steps = [
        step
        for step in steps
        if step.status
        in {
            ForwardExecutionStepStatusChoices.STAGED,
            ForwardExecutionStepStatusChoices.MERGED,
            ForwardExecutionStepStatusChoices.SKIPPED,
            ForwardExecutionStepStatusChoices.CANCELLED,
        }
    ]
    durations = [
        step_duration_seconds(step)
        for step in completed_steps
        if step_duration_seconds(step) is not None
    ]
    remaining_steps = max(0, int(run.total_steps or len(steps)) - len(completed_steps))
    average_seconds = round(sum(durations) / len(durations), 3) if durations else None
    max_seconds = round(max(durations), 3) if durations else None
    projected_remaining_seconds = (
        round(average_seconds * remaining_steps, 3)
        if average_seconds is not None
        else None
    )
    return {
        "available": bool(durations),
        "total_steps": int(run.total_steps or len(steps)),
        "completed_steps": len(completed_steps),
        "remaining_steps": remaining_steps,
        "average_completed_step_seconds": average_seconds,
        "max_completed_step_seconds": max_seconds,
        "projected_remaining_seconds": projected_remaining_seconds,
        "message": capacity_message(
            run,
            average_seconds=average_seconds,
            max_seconds=max_seconds,
            remaining_steps=remaining_steps,
        ),
    }


def step_duration_seconds(step):
    if not step.started or not step.completed:
        return None
    try:
        return max(0.0, (step.completed - step.started).total_seconds())
    except (TypeError, ValueError):
        return None


def capacity_message(run, *, average_seconds, max_seconds, remaining_steps):
    if average_seconds is None:
        return "Capacity estimate is unavailable until at least one stage step completes."
    if remaining_steps <= 0:
        return "All planned steps are complete."
    return (
        f"Average completed stage step is {average_seconds:.1f}s "
        f"(max {max_seconds:.1f}s); {remaining_steps} step(s) remain."
    )
