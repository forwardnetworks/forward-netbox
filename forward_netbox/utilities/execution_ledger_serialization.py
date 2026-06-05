from ..choices import ForwardExecutionRunStatusChoices
from .api_usage import evaluate_forward_api_usage
from .branch_budget import BRANCH_RUN_STATE_PARAMETER
from .change_explainability import change_explainability_summary
from .execution_ledger_metrics import apply_engine_decision
from .execution_ledger_metrics import execution_run_metrics
from .execution_ledger_metrics import fetch_explanation
from .execution_ledger_metrics import job_summary


def execution_run_support_bundle(run, *, recommendation_fn):
    if run is None:
        return {}
    steps = run.steps.order_by("index", "kind")
    step_list = list(steps)
    latest_ingestion = getattr(getattr(run, "sync", None), "last_ingestion", None)
    return {
        "run": run.as_support_summary(),
        "run_job": job_summary(run.job),
        "latest_ingestion": ingestion_support_summary(latest_ingestion),
        "compatibility_cache": _compatibility_cache_evidence(run),
        "api_usage": api_usage_support_summary(run),
        "recovery_recommendation": recommendation_fn(run),
        "recovery_policy_summary": _recovery_policy_summary(run),
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


API_USAGE_COUNTER_KEYS = (
    "api_requests_per_minute",
    "http_attempts",
    "http_successes",
    "http_failures",
    "http_timeout_failures",
    "http_transport_failures",
    "http_status_failures",
    "http_429_failures",
    "http_retries",
    "http_status_classes",
    "throttle_sleep_seconds",
    "usage_window_seconds",
    "observed_http_attempts_per_minute",
    "nqe_query_calls",
    "nqe_diff_calls",
    "nqe_pages",
    "nqe_query_pages",
    "nqe_diff_pages",
)


def api_usage_support_summary(run):
    job = getattr(run, "job", None)
    job_data = getattr(job, "data", None) if job is not None else None
    if not isinstance(job_data, dict):
        return {
            "available": False,
            "reason": "run_job_data_missing",
            "source": "run_job_data.forward_api_usage",
        }
    raw_summary = job_data.get("forward_api_usage")
    if not isinstance(raw_summary, dict):
        return {
            "available": False,
            "reason": "forward_api_usage_missing",
            "source": "run_job_data.forward_api_usage",
        }

    counters = {
        key: raw_summary[key] for key in API_USAGE_COUNTER_KEYS if key in raw_summary
    }
    budget = raw_summary.get("budget")
    if not isinstance(budget, dict):
        budget = evaluate_forward_api_usage(
            counters,
            source_type=getattr(getattr(run, "source", None), "type", None),
        )
    return {
        "available": True,
        "source": "run_job_data.forward_api_usage",
        "counters": counters,
        "budget": budget,
    }


def _recovery_policy_summary(run):
    events = (
        run.reconciliation_events if isinstance(run.reconciliation_events, list) else []
    )
    auto_policy_reasons = {
        "failed_stage_with_live_job_auto_restore",
        "queued_stage_without_job_auto_reset",
        "stale_queued_without_branch_auto_reset",
        "stale_stage_without_branch_auto_requeue",
    }
    escalation_reasons = {
        "stale_stage_with_branch",
        "stale_merge_job",
    }
    watchdog_reason = "stale_run_no_progress_watchdog"
    escalation_threshold = 3
    watchdog_threshold = 2
    policy_events = [
        event
        for event in events
        if isinstance(event, dict) and event.get("reason") in auto_policy_reasons
    ]
    escalation_events = [
        event
        for event in events
        if isinstance(event, dict) and event.get("reason") in escalation_reasons
    ]
    watchdog_events = [
        event
        for event in events
        if isinstance(event, dict) and event.get("reason") == watchdog_reason
    ]
    by_reason = {}
    for event in policy_events:
        reason = str(event.get("reason") or "unknown")
        by_reason[reason] = int(by_reason.get(reason, 0)) + 1
    escalation_by_reason = {}
    for event in escalation_events:
        reason = str(event.get("reason") or "unknown")
        escalation_by_reason[reason] = int(escalation_by_reason.get(reason, 0)) + 1
    latest_event = policy_events[-1] if policy_events else None
    escalation_required = any(
        count >= escalation_threshold for count in escalation_by_reason.values()
    )
    watchdog_required = len(watchdog_events) >= watchdog_threshold
    return {
        "auto_policy_event_count": len(policy_events),
        "auto_policy_reasons": by_reason,
        "last_auto_policy_event": latest_event,
        "escalation_event_count": len(escalation_events),
        "escalation_reasons": escalation_by_reason,
        "escalation_threshold": escalation_threshold,
        "escalation_required": escalation_required,
        "watchdog_event_count": len(watchdog_events),
        "watchdog_reason": watchdog_reason,
        "watchdog_threshold": watchdog_threshold,
        "watchdog_required": watchdog_required,
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
        "change_explainability": change_explainability_summary(ingestion),
        "execution_summary": ingestion.get_execution_summary(),
        "analysis_summary": ingestion.get_analysis_summary(),
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


def _compatibility_cache_evidence(run):
    sync = run.sync
    parameters = sync.parameters or {}
    compatibility_state = parameters.get(BRANCH_RUN_STATE_PARAMETER)
    compatibility_present = BRANCH_RUN_STATE_PARAMETER in parameters
    compatibility_is_dict = isinstance(compatibility_state, dict)
    compatibility_size = len(compatibility_state) if compatibility_is_dict else 0
    compatibility_keys = (
        sorted(str(key) for key in compatibility_state.keys())[:10]
        if compatibility_is_dict
        else []
    )
    compatibility_execution_run_id = None
    if compatibility_is_dict:
        compatibility_execution_run_id = compatibility_state.get("execution_run_id")

    terminal_statuses = {
        ForwardExecutionRunStatusChoices.COMPLETED,
        ForwardExecutionRunStatusChoices.FAILED,
        ForwardExecutionRunStatusChoices.TIMEOUT,
        ForwardExecutionRunStatusChoices.CANCELLED,
    }
    active_run = (
        sync.execution_runs.exclude(status__in=terminal_statuses)
        .order_by("-pk")
        .first()
    )
    latest_run = sync.execution_runs.order_by("-pk").first()
    stale_payload_present = bool(
        latest_run is not None and active_run is None and compatibility_present
    )

    return {
        "ledger_history": bool(latest_run is not None),
        "active_execution_run": bool(active_run is not None),
        "active_execution_run_id": active_run.pk if active_run else None,
        "compatibility_state_present": bool(compatibility_present),
        "compatibility_state_size": compatibility_size,
        "compatibility_state_keys": compatibility_keys,
        "compatibility_execution_run_id": compatibility_execution_run_id,
        "stale_payload_present": stale_payload_present,
        "prune_recommended": bool(stale_payload_present),
    }
