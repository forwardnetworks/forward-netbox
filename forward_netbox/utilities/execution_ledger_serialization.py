from .execution_ledger_metrics import apply_engine_decision
from .execution_ledger_metrics import execution_run_metrics
from .execution_ledger_metrics import fetch_explanation
from .execution_ledger_metrics import job_summary


def execution_run_support_bundle(run, *, recommendation_fn):
    if run is None:
        return {}
    steps = run.steps.order_by("index", "kind")
    step_list = list(steps)
    return {
        "run": run.as_support_summary(),
        "run_job": job_summary(run.job),
        "recovery_recommendation": recommendation_fn(run),
        "metrics": execution_run_metrics(run, step_list),
        "steps": [
            {
                **step.as_support_summary(),
                "fetch_explanation": fetch_explanation(step),
                "apply_engine_decision": apply_engine_decision(step),
                "job_detail": job_summary(step.job),
                "merge_job_detail": job_summary(step.merge_job),
                "ingestion_detail": ingestion_support_summary(step.ingestion),
            }
            for step in step_list
        ],
    }


def ingestion_support_summary(ingestion):
    if ingestion is None:
        return None
    issues = list(ingestion.issues.order_by("timestamp")[:25])
    return {
        "id": ingestion.pk,
        "name": ingestion.name,
        "sync_mode": ingestion.sync_mode or "",
        "baseline_ready": bool(ingestion.baseline_ready),
        "snapshot_selector": ingestion.snapshot_selector or "",
        "snapshot_id": ingestion.snapshot_id or "",
        "branch": ingestion.branch_id,
        "branch_name": ingestion.branch.name if ingestion.branch else "",
        "applied_change_count": int(ingestion.applied_change_count or 0),
        "failed_change_count": int(ingestion.failed_change_count or 0),
        "created_change_count": int(ingestion.created_change_count or 0),
        "updated_change_count": int(ingestion.updated_change_count or 0),
        "deleted_change_count": int(ingestion.deleted_change_count or 0),
        "issue_count": ingestion.issues.count(),
        "issues": [
            {
                "id": issue.pk,
                "timestamp": issue.timestamp.isoformat() if issue.timestamp else None,
                "phase": issue.phase,
                "model": issue.model or "",
                "message": issue.message,
                "exception": issue.exception or "",
            }
            for issue in issues
        ],
    }

