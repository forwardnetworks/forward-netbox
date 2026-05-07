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
