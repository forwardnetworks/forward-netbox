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
    for result in model_results or []:
        if not isinstance(result, dict):
            continue
        changes = int(result.get("estimated_changes") or 0)
        removes = int(result.get("delete_count") or 0)
        drift = changes + removes
        total_drift += drift
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
    return {
        "models": rows,
        "model_count": len(rows),
        "drifted_model_count": sum(1 for row in rows if not row["in_sync"]),
        "total_drift": total_drift,
        "in_sync": total_drift == 0,
        "generated_at": (
            payload.get("generated_at") if isinstance(payload, dict) else None
        ),
    }
