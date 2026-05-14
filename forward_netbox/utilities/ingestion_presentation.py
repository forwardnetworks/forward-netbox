from collections import Counter

from .execution_telemetry import build_ingestion_execution_summary


def get_snapshot_summary(ingestion):
    info = dict(ingestion.snapshot_info or {})
    return {
        "snapshot_selector": ingestion.snapshot_selector or "",
        "snapshot_id": ingestion.snapshot_id or "",
        "state": info.get("state") or "",
        "created_at": info.get("createdAt") or "",
        "processed_at": info.get("processedAt") or "",
    }


def get_snapshot_metrics_summary(ingestion):
    metrics = dict(ingestion.snapshot_metrics or {})
    keys = (
        "snapshotState",
        "numSuccessfulDevices",
        "numCollectionFailureDevices",
        "numProcessingFailureDevices",
        "numSuccessfulEndpoints",
        "numCollectionFailureEndpoints",
        "numProcessingFailureEndpoints",
        "collectionDuration",
        "processingDuration",
    )
    return {key: metrics[key] for key in keys if key in metrics}


def get_model_results_summary(ingestion):
    return list(ingestion.model_results or [])


def get_execution_summary(ingestion):
    return build_ingestion_execution_summary(
        model_results=get_model_results_summary(ingestion),
        job_logs=ingestion.get_job_logs(ingestion.job).get("logs", []),
        applied_change_count=ingestion.applied_change_count,
        failed_change_count=ingestion.failed_change_count,
        created_change_count=ingestion.created_change_count,
        updated_change_count=ingestion.updated_change_count,
        deleted_change_count=ingestion.deleted_change_count,
    )


def get_analysis_summary(ingestion):
    validation_run = ingestion.validation_run
    issues = ingestion.issues.all()
    issue_models = sorted(
        {
            model_string
            for model_string in issues.values_list("model", flat=True)
            if model_string
        }
    )
    issue_phases = Counter(
        phase for phase in issues.values_list("phase", flat=True) if phase
    )
    model_results = list(ingestion.model_results or [])
    diagnostic_count = sum(
        len(result.get("diagnostics") or []) for result in model_results
    )
    summary = {
        "baseline_ready": bool(ingestion.baseline_ready),
        "sync_mode": ingestion.sync_mode or "",
        "issue_count": issues.count(),
        "issue_models": issue_models,
        "issue_phases": dict(sorted(issue_phases.items())),
        "model_result_count": len(model_results),
        "diagnostic_count": diagnostic_count,
        "validation_run": validation_run.pk if validation_run else None,
        "validation_status": validation_run.status if validation_run else "",
        "validation_allowed": validation_run.allowed if validation_run else None,
        "validation_blocking_reason_count": (
            len(validation_run.blocking_reasons or []) if validation_run else 0
        ),
    }
    if validation_run is not None:
        summary["validation_drift_summary"] = dict(validation_run.drift_summary or {})
    return summary


def get_workload_summary(ingestion):
    execution = get_execution_summary(ingestion)
    model_results = list(ingestion.model_results or [])
    return {
        "sync_mode": ingestion.sync_mode or "",
        "baseline_ready": bool(ingestion.baseline_ready),
        "model_count": execution["model_count"],
        "shard_count": execution["shard_count"],
        "retry_count": execution["retry_count"],
        "estimated_changes": execution["estimated_changes"],
        "row_count": execution["row_count"],
        "delete_count": execution["delete_count"],
        "runtime_ms": execution["runtime_ms"],
        "slowest_model": execution["slowest_model"],
        "diagnostic_count": sum(
            len(result.get("diagnostics") or []) for result in model_results
        ),
        "applied_change_count": execution["applied_change_count"],
        "failed_change_count": execution["failed_change_count"],
        "created_change_count": execution["created_change_count"],
        "updated_change_count": execution["updated_change_count"],
        "deleted_change_count": execution["deleted_change_count"],
    }


def get_advisory_summary(ingestion):
    analysis = get_analysis_summary(ingestion)
    workload = get_workload_summary(ingestion)
    model_results = list(ingestion.model_results or [])
    sorted_results = sorted(
        model_results,
        key=lambda result: (
            -(int(result.get("estimated_changes") or 0)),
            str(result.get("model") or ""),
        ),
    )
    result_overview = []
    for result in sorted_results[:5]:
        result_overview.append(
            {
                "model": result.get("model") or "",
                "query_name": result.get("query_name") or "",
                "estimated_changes": int(result.get("estimated_changes") or 0),
                "row_count": int(result.get("row_count") or 0),
                "delete_count": int(result.get("delete_count") or 0),
                "diagnostic_count": len(result.get("diagnostics") or []),
                "sync_mode": result.get("sync_mode") or "",
                "execution_mode": result.get("execution_mode") or "",
            }
        )
    return {
        "baseline_ready": workload["baseline_ready"],
        "sync_mode": workload["sync_mode"],
        "blast_radius": {
            "estimated_changes": workload["estimated_changes"],
            "shard_count": workload["shard_count"],
            "retry_count": workload["retry_count"],
            "model_count": workload["model_count"],
            "slowest_model": workload["slowest_model"],
        },
        "intent_signals": {
            "validation_status": analysis.get("validation_status") or "",
            "validation_allowed": analysis.get("validation_allowed"),
            "validation_blocking_reason_count": analysis.get(
                "validation_blocking_reason_count", 0
            ),
            "validation_drift_summary": analysis.get("validation_drift_summary", {}),
            "issue_count": analysis.get("issue_count", 0),
            "issue_models": analysis.get("issue_models", []),
            "issue_phases": analysis.get("issue_phases", {}),
        },
        "path_signals": {
            "model_result_count": analysis.get("model_result_count", 0),
            "diagnostic_count": analysis.get("diagnostic_count", 0),
            "top_model_results": result_overview,
        },
        "workload_preview": workload,
    }


def get_statistics(ingestion, stage="sync"):
    job = ingestion.merge_job if stage == "merge" else ingestion.job
    job_results = ingestion.get_job_logs(job)
    raw_stats = job_results.get("statistics", {})
    statistics = {}
    for model_string, stats in raw_stats.items():
        total = stats.get("total", 0)
        if total:
            statistics[model_string] = stats.get("current", 0) / total * 100
    if not getattr(ingestion, "num_created", 0):
        ingestion.num_created = ingestion.created_change_count
    if not getattr(ingestion, "num_updated", 0):
        ingestion.num_updated = ingestion.updated_change_count
    if not getattr(ingestion, "num_deleted", 0):
        ingestion.num_deleted = ingestion.deleted_change_count
    if not getattr(ingestion, "staged_changes", 0):
        ingestion.staged_changes = ingestion.applied_change_count
    return {"job_results": job_results, "statistics": statistics}
