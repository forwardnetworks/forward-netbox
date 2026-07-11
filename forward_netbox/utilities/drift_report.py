# Bidirectional drift report: distills the cached dependency dry-run into a
# per-model NetBox-vs-Forward divergence table (what a sync would change), so
# operators can see drift without applying. Reuses the dependency-preview job's
# cached payload — no extra heavy dry-run.


def compute_drift_report(payload):
    """Build a per-model drift summary from a dependency dry-run payload.

    Each model result carries the planner's view of the gap between Forward and
    NetBox: ``estimated_changes`` (rows that would be created/updated) and
    ``delete_count`` (NetBox rows Forward no longer has). A model is in sync when
    both are zero.
    """
    model_results = payload.get("model_results") if isinstance(payload, dict) else None
    rows = []
    total_drift = 0
    total_removes = 0
    models_with_rows = 0
    full_create_like = 0
    for result in model_results or []:
        if not isinstance(result, dict):
            continue
        changes = int(result.get("estimated_changes") or 0)
        removes = int(result.get("delete_count") or 0)
        forward_rows = int(result.get("row_count") or 0)
        drift = changes + removes
        total_drift += drift
        total_removes += removes
        # A model looks like a "full create" (empty/unmerged baseline) when every
        # Forward row is pending with nothing to remove.
        if forward_rows > 0:
            models_with_rows += 1
            if changes >= forward_rows and removes == 0:
                full_create_like += 1
        rows.append(
            {
                "model": result.get("model"),
                "forward_rows": result.get("row_count"),
                "pending_changes": changes,
                "pending_removes": removes,
                "drift": drift,
                "in_sync": drift == 0,
            }
        )
    rows.sort(key=lambda row: row["drift"], reverse=True)
    # Fingerprint of a preview taken against an empty/unmerged NetBox: several
    # models, every one of them fully pending, zero removals. That is "here is
    # everything Forward has," not real per-row drift — flag it so the operator
    # doesn't read a pre-ingest/pre-merge preview as genuine divergence.
    looks_like_full_create = (
        models_with_rows >= 3
        and full_create_like == models_with_rows
        and total_removes == 0
    )
    return {
        "models": rows,
        "model_count": len(rows),
        "drifted_model_count": sum(1 for row in rows if not row["in_sync"]),
        "total_drift": total_drift,
        "in_sync": total_drift == 0,
        "looks_like_full_create": looks_like_full_create,
        "full_create_model_count": full_create_like,
        "generated_at": (
            payload.get("generated_at") if isinstance(payload, dict) else None
        ),
    }
