# Execution-ledger no-op shim (2.0).
#
# The per-shard execution-ledger (run/step records, reconciliation, metrics,
# serialization — ~5,580 lines across five modules) only ever produced data on
# the deleted per-shard branching path. Single-branch ingest never creates
# ForwardExecutionRun/Step rows, so every ledger call already returned
# None/empty in practice. This shim preserves the import surface that the merge
# job, ingestion bookkeeping, health, and run-history views still reference, but
# returns inert results — letting the dead logic be deleted without rewiring
# every caller. The ForwardExecutionRun/Step models + run-history UI remain as
# inert shells (no rows are created); they can be dropped in a follow-up.
from ..choices import ForwardExecutionRunStatusChoices

TERMINAL_RUN_STATUSES = {
    ForwardExecutionRunStatusChoices.COMPLETED,
    ForwardExecutionRunStatusChoices.FAILED,
    ForwardExecutionRunStatusChoices.TIMEOUT,
    ForwardExecutionRunStatusChoices.CANCELLED,
}


# --- run / step accessors (single-branch never creates these) ---------------
def active_execution_run(sync):
    return None


def latest_execution_run(sync):
    return None


def execution_step_for_ingestion(ingestion):
    return None


def ensure_branch_execution_run(*args, **kwargs):
    return None


def current_retryable_step(run):
    return None


def current_discardable_step(run):
    return None


def current_mergeable_step(run):
    return None


def prepare_stage_step_retry(step):
    return None


def discard_stage_branch_for_retry(step):
    return None


# --- mutators / lifecycle (no-op without a run) -----------------------------
def mark_run_completed(sync, *, baseline_ready=False):
    return None


def mark_ingestion_step_merged(ingestion, *, baseline_ready=False, merge_job=None):
    return None


def claim_ingestion_merge_step(ingestion, job):
    # No ledger step to claim; allow the merge to proceed.
    return True


def claim_stage_step(sync, index, job):
    return None


def touch_execution_step_progress(
    sync, *, model_string, shard_index=None, row_count=None, row_total=None
):
    return False


def reconcile_execution_run(run):
    return None


def update_run_from_branch_state(sync):
    return None


def branch_run_state_from_execution_run(run):
    return {}


# --- summaries / observability (empty without run history) ------------------
def execution_run_bundle_for_sync(sync):
    return {}


def execution_run_failure_summary(run, step_list=None):
    return {}


def execution_run_insights_summary(run):
    return {}


def execution_run_metrics(run, steps):
    return {}


def execution_run_recovery_recommendation(run):
    return {}


def execution_run_support_bundle(run):
    return {}


def api_usage_support_summary(run):
    return {}


def apply_engine_decision(step):
    return {}


def dependency_lookup_cache_support_summary(run):
    return {}


def diff_baseline_transition_summary(run, steps, *, diff_utilization=None):
    return {}


def diff_utilization_summary(steps, *, alert_thresholds=None):
    return {}


def fallback_reason_summary(steps):
    return {}


def fetch_explanation(step):
    return {}


def job_summary(job):
    return {}


def live_support_diagnostics(sync, *, sync_health=None):
    return {}


def partition_retry_summary(steps):
    return {}


def pushdown_efficiency_summary(*, fetch_mode_counts_by_model, alert_thresholds=None):
    return {}


def pushdown_runtime_summary(steps, *, alert_thresholds=None):
    return {}


def pushdown_trend_history_for_sync(sync, *, limit=180):
    return []


def pushdown_tuning_guidance(
    *,
    efficiency,
    runtime_share,
    diff_utilization,
    alert_thresholds,
    partition_retries=None,
    throughput=None,
    query_fetch_concurrency=None,
):
    return {}


def recent_pushdown_trend_snapshots(run, *, limit=8):
    return []


def scheduler_overlap_capacity_evidence(sync):
    return {}


def throughput_smoothing_summary(step_metrics, *, capacity_evidence=None):
    return {}


# --- additional inert surface referenced by callers -------------------------
DEAD_STAGE_JOB_REQUEUE_GRACE_SECONDS = 300


def dependency_parent_coverage_support_summary(run):
    return {}


def ingestion_has_mergeable_execution_step(ingestion):
    return False


def ingestion_has_requeueable_merge_timeout_step(ingestion):
    return False
