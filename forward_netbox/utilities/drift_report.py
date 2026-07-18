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
        "created": _count(getattr(ingestion, "created_change_count", 0)),
        "updated": _count(getattr(ingestion, "updated_change_count", 0)),
        "deleted": _count(getattr(ingestion, "deleted_change_count", 0)),
    }
    has_changes = any(
        counters[key] > 0 for key in ("applied", "created", "updated", "deleted")
    )
    baseline_ready = bool(getattr(ingestion, "baseline_ready", False))
    if counters["failed"]:
        status = "failed"
    elif not baseline_ready:
        status = "incomplete"
    elif has_changes:
        status = "confirmation_required"
    elif same_snapshot is False:
        status = "snapshot_mismatch"
    elif same_snapshot is None:
        status = "snapshot_unknown"
    else:
        status = "converged"

    return {
        "ingestion_id": getattr(ingestion, "pk", None),
        "ingestion_created_at": getattr(ingestion, "created", None),
        "completed_at": (
            getattr(getattr(ingestion, "merge_job", None), "completed", None)
            or getattr(getattr(ingestion, "job", None), "completed", None)
        ),
        "snapshot_id": sync_snapshot_id,
        "preview_snapshot_id": preview_snapshot_id,
        "same_snapshot": same_snapshot,
        "snapshot_comparison_available": same_snapshot is not None,
        "baseline_ready": baseline_ready,
        "status": status,
        "convergence_confirmed": status == "converged",
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
    comparison_available = all(row["comparison_available"] for row in rows)
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
        "drifted_model_count": (
            sum(1 for row in rows if not row["in_sync"])
            if comparison_available
            else None
        ),
        "total_drift": total_drift if comparison_available else None,
        "total_apply_work": total_apply_work,
        "total_upsert_candidates": total_upsert_candidates,
        "total_removes": total_removes,
        "in_sync": total_drift == 0 if comparison_available else None,
        "looks_like_full_create": looks_like_full_create,
        "full_create_model_count": full_create_like,
        "generated_at": (
            payload.get("generated_at") if isinstance(payload, dict) else None
        ),
    }
