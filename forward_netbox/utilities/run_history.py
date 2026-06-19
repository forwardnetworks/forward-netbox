# Per-sync run-history summary for the observability panel.
#
# Reads stored ForwardIngestion rows (and their linked jobs) only — no live
# Forward call — so the panel renders fast even on large fabrics.

RUN_HISTORY_LIMIT = 20


def _change_weight(model_result):
    return int(
        model_result.get("estimated_changes") or model_result.get("row_count") or 0
    ) + int(model_result.get("delete_count") or 0)


def _model_result_summary(model_result):
    return {
        "model": model_result.get("model"),
        "row_count": model_result.get("row_count"),
        "delete_count": model_result.get("delete_count"),
        "estimated_changes": model_result.get("estimated_changes"),
        "runtime_ms": model_result.get("runtime_ms"),
    }


def _run_summary(ingestion):
    job = ingestion.job
    started = getattr(job, "started", None) if job is not None else None
    completed = getattr(job, "completed", None) if job is not None else None
    duration_seconds = None
    if started and completed:
        duration_seconds = round((completed - started).total_seconds(), 1)

    model_results = (
        ingestion.model_results if isinstance(ingestion.model_results, list) else []
    )
    top_models = sorted(
        (m for m in model_results if isinstance(m, dict)),
        key=_change_weight,
        reverse=True,
    )[:5]

    total_changes = (
        int(ingestion.created_change_count)
        + int(ingestion.updated_change_count)
        + int(ingestion.deleted_change_count)
    )
    try:
        url = ingestion.get_absolute_url()
    except Exception:  # pragma: no cover - defensive
        url = None
    return {
        "id": ingestion.pk,
        "url": url,
        "created": ingestion.created.isoformat() if ingestion.created else None,
        "snapshot_selector": ingestion.snapshot_selector,
        "snapshot_id": ingestion.snapshot_id,
        "sync_mode": ingestion.sync_mode,
        "applied": int(ingestion.applied_change_count),
        "created_count": int(ingestion.created_change_count),
        "updated_count": int(ingestion.updated_change_count),
        "deleted_count": int(ingestion.deleted_change_count),
        "failed": int(ingestion.failed_change_count),
        "total_changes": total_changes,
        "duration_seconds": duration_seconds,
        "model_count": len(model_results),
        "top_models": [_model_result_summary(m) for m in top_models],
    }


def sync_run_history(sync, *, limit=RUN_HISTORY_LIMIT):
    """Summarize the sync's most recent ingestion runs (newest first) plus a
    change-volume trend (oldest -> newest) and simple aggregates."""
    from forward_netbox.models import ForwardIngestion

    ingestions = list(
        ForwardIngestion.objects.filter(sync=sync)
        .select_related("job")
        .order_by("-pk")[:limit]
    )
    runs = [_run_summary(ingestion) for ingestion in ingestions]
    # Trend oldest -> newest for a left-to-right mini chart.
    trend = [run["total_changes"] for run in reversed(runs)]
    failed_runs = sum(1 for run in runs if run["failed"] > 0)
    return {
        "available": bool(runs),
        "run_count": len(runs),
        "runs": runs,
        "trend": trend,
        "max_changes": max(trend) if trend else 0,
        "failed_runs": failed_runs,
    }
