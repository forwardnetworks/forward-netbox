# Cached dependency previews can contain either exact comparisons or workload
# upper bounds. Keep those meanings explicit so fetched rows are never reported
# as object-level drift.


EXACT_COMPARISON = "exact_comparison"
WORKLOAD_UPPER_BOUND = "workload_upper_bound"


def _count(value):
    try:
        return max(0, int(value or 0))
    except (TypeError, ValueError):
        return 0


def _estimate_kind(result, *, row_count, estimated_changes, delete_count):
    kind = str(result.get("change_estimate_kind") or "").strip()
    if kind in {EXACT_COMPARISON, WORKLOAD_UPPER_BOUND}:
        return kind
    if (row_count or delete_count) and estimated_changes == row_count + delete_count:
        return WORKLOAD_UPPER_BOUND
    return EXACT_COMPARISON


def build_latest_sync_evidence(ingestion, preview_payload=None):
    """Summarize persisted sync counters without treating preview rows as drift."""
    if ingestion is None:
        return None

    payload = preview_payload if isinstance(preview_payload, dict) else {}
    context = payload.get("context") if isinstance(payload.get("context"), dict) else {}
    preview_snapshot_id = str(
        context.get("snapshot_id") or payload.get("snapshot_id") or ""
    )
    sync_snapshot_id = str(getattr(ingestion, "snapshot_id", "") or "")
    same_snapshot = (
        sync_snapshot_id == preview_snapshot_id
        if sync_snapshot_id and preview_snapshot_id
        else None
    )
    counters = {
        "applied": _count(getattr(ingestion, "applied_change_count", 0)),
        "failed": _count(getattr(ingestion, "failed_change_count", 0)),
        # Rows NetBox refused on its own validation rules. They are exceptions
        # and stay visible, but no rerun can satisfy them, so they must not
        # decide convergence status the way a retryable failure does.
        "skipped": _count(getattr(ingestion, "skipped_change_count", 0)),
        "created": _count(getattr(ingestion, "created_change_count", 0)),
        "updated": _count(getattr(ingestion, "updated_change_count", 0)),
        "deleted": _count(getattr(ingestion, "deleted_change_count", 0)),
    }
    has_changes = any(
        counters[key] > 0 for key in ("applied", "created", "updated", "deleted")
    )
    baseline_ready = bool(getattr(ingestion, "baseline_ready", False))
    merge_job = getattr(ingestion, "merge_job", None)
    execution_job = merge_job or getattr(ingestion, "job", None)
    job_status = str(getattr(execution_job, "status", "") or "").lower()
    completed_at = getattr(execution_job, "completed", None)
    execution_completed = job_status == "completed" and completed_at is not None
    execution_failed = job_status in {"errored", "failed"}
    ownership = {"complete": True, "required_domains": [], "pending_domains": []}
    sync = getattr(ingestion, "sync", None)
    if sync is not None:
        from .ownership import ownership_finalization_summary

        ownership = ownership_finalization_summary(
            sync,
            generation=getattr(ingestion, "pk", None),
        )
    if counters["failed"] or execution_failed:
        status = "failed"
    elif not execution_completed or not baseline_ready:
        status = "incomplete"
    elif not ownership["complete"]:
        status = "ownership_incomplete"
    elif has_changes:
        status = "confirmation_required"
    elif counters["skipped"]:
        # Otherwise converged, but rows the destination will refuse on every
        # run remain. The baseline promotes and drift is measured; convergence
        # is still not confirmed, and saying so is the point.
        status = "unsatisfiable_rows"
    elif same_snapshot is False:
        status = "snapshot_mismatch"
    elif same_snapshot is None:
        status = "snapshot_unknown"
    else:
        status = "converged"

    return {
        "ingestion_id": getattr(ingestion, "pk", None),
        "ingestion_created_at": getattr(ingestion, "created", None),
        "completed_at": completed_at,
        "job_status": job_status,
        "execution_completed": execution_completed,
        "snapshot_id": sync_snapshot_id,
        "preview_snapshot_id": preview_snapshot_id,
        "same_snapshot": same_snapshot,
        "snapshot_comparison_available": same_snapshot is not None,
        "baseline_ready": baseline_ready,
        "status": status,
        "convergence_confirmed": status == "converged",
        "ownership": ownership,
        **counters,
    }


def compute_drift_report(payload):
    """Build a per-model drift summary from a dependency dry-run payload.

    Exact payloads report object-level drift. Dependency workload payloads only
    report upper-bound apply work and therefore cannot establish in-sync state.
    """
    model_results = payload.get("model_results") if isinstance(payload, dict) else None
    rows = []
    total_drift = 0
    total_apply_work = 0
    total_upsert_candidates = 0
    total_removes = 0
    models_with_rows = 0
    full_create_like = 0
    for result in model_results or []:
        if not isinstance(result, dict):
            continue
        changes = _count(result.get("estimated_changes"))
        removes = _count(result.get("delete_count"))
        forward_rows = _count(result.get("row_count"))
        estimate_kind = _estimate_kind(
            result,
            row_count=forward_rows,
            estimated_changes=changes,
            delete_count=removes,
        )
        comparison_available = estimate_kind == EXACT_COMPARISON
        if comparison_available:
            upsert_candidates = changes
            apply_work = changes + removes
            drift = apply_work
            in_sync = drift == 0
            total_drift += drift
        else:
            upsert_candidates = max(0, changes - removes)
            apply_work = changes
            drift = None
            in_sync = None
        total_apply_work += apply_work
        total_upsert_candidates += upsert_candidates
        total_removes += removes
        # A model looks like a "full create" (empty/unmerged baseline) when every
        # Forward row is pending with nothing to remove.
        if forward_rows > 0:
            models_with_rows += 1
            if comparison_available and changes >= forward_rows and removes == 0:
                full_create_like += 1
        rows.append(
            {
                "model": result.get("model"),
                "forward_rows": result.get("row_count"),
                "pending_changes": upsert_candidates,
                "pending_removes": removes,
                "estimated_apply_work": apply_work,
                "change_estimate_kind": estimate_kind,
                "comparison_available": comparison_available,
                "drift": drift,
                "in_sync": in_sync,
            }
        )
    rows.sort(
        key=lambda row: (
            row["drift"] if row["drift"] is not None else row["estimated_apply_work"]
        ),
        reverse=True,
    )
    measured_rows = [row for row in rows if row["comparison_available"]]
    unmeasured_rows = [row for row in rows if not row["comparison_available"]]
    # Drift is reported over the models that were actually compared, rather than
    # withheld until every model can be. Requiring all of them meant one
    # uncovered model - and there is always at least one, because the
    # adapter-only models have no comparison - reported "Not measured" for the
    # whole estate on every run, permanently. "Drift 412 across 13 of 27 models"
    # is useful and true; "Not measured" was neither.
    comparison_available = bool(measured_rows)
    fully_measured = bool(rows) and not unmeasured_rows
    # Fingerprint of a preview taken against an empty/unmerged NetBox: several
    # models, every one of them fully pending, zero removals. That is "here is
    # everything Forward has," not real per-row drift — flag it so the operator
    # doesn't read a pre-ingest/pre-merge preview as genuine divergence.
    looks_like_full_create = (
        comparison_available
        and models_with_rows >= 3
        and full_create_like == models_with_rows
        and total_removes == 0
    )
    return {
        "models": rows,
        "model_count": len(rows),
        "comparison_available": comparison_available,
        # How much of the estate the drift figures actually cover. Reported
        # alongside them rather than implied, so a partial measurement is never
        # read as a whole-estate one.
        "measured_model_count": len(measured_rows),
        "unmeasured_model_count": len(unmeasured_rows),
        "unmeasured_models": sorted(
            row["model"] for row in unmeasured_rows if row.get("model")
        ),
        "fully_measured": fully_measured,
        "drifted_model_count": (
            sum(1 for row in measured_rows if not row["in_sync"])
            if comparison_available
            else None
        ),
        "total_drift": total_drift if comparison_available else None,
        "total_apply_work": total_apply_work,
        "total_upsert_candidates": total_upsert_candidates,
        "total_removes": total_removes,
        # "In sync" is a claim about the whole estate, so it stays unanswered
        # while any model is uncompared. Zero drift across the measured models
        # is reported as `total_drift`, which says what it covers; answering
        # "Yes" off a partial measurement would tell an operator they are in
        # sync when nothing checked the rest.
        "in_sync": (total_drift == 0) if (fully_measured and rows) else None,
        "looks_like_full_create": looks_like_full_create,
        "full_create_model_count": full_create_like,
        "generated_at": (
            payload.get("generated_at") if isinstance(payload, dict) else None
        ),
    }
