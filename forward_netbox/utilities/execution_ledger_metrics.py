from .apply_engine import apply_engine_decision_summary


def job_summary(job):
    if job is None:
        return None
    return {
        "pk": job.pk,
        "status": getattr(job, "status", ""),
        "created": getattr(job, "created", None),
        "started": getattr(job, "started", None),
        "completed": getattr(job, "completed", None),
        "duration": getattr(job, "duration", None),
        "data": getattr(job, "data", {}) or {},
        "log_entries": list(getattr(job, "log_entries", []) or []),
    }


def execution_run_metrics(run, steps):
    query_runtime_ms = sum_optional_float(step.query_runtime_ms for step in steps)
    step_metrics = [
        {
            "index": step.index,
            "kind": step.kind,
            "model": step.model_string,
            "status": step.status,
            "estimated_changes": int(step.estimated_changes or 0),
            "actual_changes": int(step.actual_changes or 0),
            "fetched_row_count": int(step.fetched_row_count or 0),
            "query_runtime_ms": step.query_runtime_ms,
            "attempted_row_count": int(step.attempted_row_count or 0),
            "applied_row_count": int(step.applied_row_count or 0),
            "skipped_row_count": int(step.skipped_row_count or 0),
            "failed_row_count": int(step.failed_row_count or 0),
            "retry_count": int(step.retry_count or 0),
            "fetch_mode": step.fetch_mode,
            "fetch_explanation": fetch_explanation(step),
            "apply_engine": step.apply_engine,
            "apply_engine_decision": apply_engine_decision(step),
            "stage_duration_seconds": duration_seconds(
                step.started,
                step.completed,
            ),
            "merge_duration_seconds": duration_seconds(
                getattr(step.merge_job, "started", None),
                getattr(step.merge_job, "completed", None),
            ),
        }
        for step in steps
    ]
    return {
        "total_runtime_seconds": duration_seconds(run.created, run.completed),
        "step_count": len(steps),
        "estimated_changes": sum(int(step.estimated_changes or 0) for step in steps),
        "actual_changes": sum(int(step.actual_changes or 0) for step in steps),
        "fetched_row_count": sum(int(step.fetched_row_count or 0) for step in steps),
        "query_runtime_ms": query_runtime_ms,
        "attempted_row_count": sum(
            int(step.attempted_row_count or 0) for step in steps
        ),
        "applied_row_count": sum(int(step.applied_row_count or 0) for step in steps),
        "skipped_row_count": sum(int(step.skipped_row_count or 0) for step in steps),
        "failed_row_count": sum(int(step.failed_row_count or 0) for step in steps),
        "retry_count": sum(int(step.retry_count or 0) for step in steps),
        "fetch_modes": sorted({step.fetch_mode for step in steps if step.fetch_mode}),
        "apply_engines": sorted(
            {step.apply_engine for step in steps if step.apply_engine}
        ),
        "bottleneck": runtime_bottleneck(step_metrics, query_runtime_ms),
        "steps": step_metrics,
    }


def runtime_bottleneck(step_metrics, query_runtime_ms):
    query_seconds = (
        round(float(query_runtime_ms) / 1000.0, 3)
        if query_runtime_ms is not None
        else None
    )
    stage_seconds = sum_optional_float(
        step["stage_duration_seconds"] for step in step_metrics
    )
    merge_seconds = sum_optional_float(
        step["merge_duration_seconds"] for step in step_metrics
    )
    apply_or_stage_seconds = None
    if stage_seconds is not None:
        apply_or_stage_seconds = max(
            0.0,
            round(stage_seconds - float(query_seconds or 0), 3),
        )
    candidates = [
        ("forward_query", query_seconds),
        ("row_apply_or_stage_overhead", apply_or_stage_seconds),
        ("branching_merge", merge_seconds),
    ]
    measured = [(phase, seconds) for phase, seconds in candidates if seconds]
    if not measured:
        return {
            "phase": "unknown",
            "seconds": None,
            "message": "No completed query, stage, or merge timing data is available.",
        }
    phase, seconds = max(measured, key=lambda item: item[1])
    messages = {
        "forward_query": "Forward NQE query runtime is the largest measured phase.",
        "row_apply_or_stage_overhead": (
            "Row apply or stage overhead is the largest measured phase."
        ),
        "branching_merge": "NetBox Branching merge runtime is the largest measured phase.",
    }
    return {
        "phase": phase,
        "seconds": seconds,
        "message": messages[phase],
    }


def duration_seconds(started, completed):
    if not started or not completed:
        return None
    try:
        return max(0.0, round((completed - started).total_seconds(), 3))
    except (TypeError, ValueError):
        return None


def sum_optional_float(values):
    numeric_values = [float(value) for value in values if value is not None]
    if not numeric_values:
        return None
    return round(sum(numeric_values), 3)


def fetch_explanation(step):
    mode = step.fetch_mode or "model"
    if mode == "nqe_column_filter":
        filter_count = len(step.fetch_column_filters or [])
        key_family = step.fetch_key_family or "shard"
        return (
            "Fetched the shard with native Forward NQE column filters "
            f"for {key_family} keys ({filter_count} filter(s))."
        )
    if mode == "shard":
        key_family = step.fetch_key_family or "shard"
        return f"Fetched the shard with persisted {key_family} shard parameters."
    if mode == "diff_fallback":
        return (
            "Used a diff fallback because the query can run Forward diffs but "
            "the current shard could not be safely pushed down."
        )
    if mode == "full_fallback":
        return (
            "Used a full-query fallback because this model has no safe persisted "
            "shard fetch contract."
        )
    if mode == "model":
        return (
            "Fetched the model result and applied the persisted shard locally; "
            "this model does not have a safe narrower fetch contract yet."
        )
    return f"Fetch mode `{mode}` was recorded for this step."


def apply_engine_decision(step):
    if not step.model_string:
        return {}
    return apply_engine_decision_summary(
        sync=step.run.sync,
        model_string=step.model_string,
        backend=step.run.backend,
    )
