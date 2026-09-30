"""Stop Auto merge for review when a run is far larger than normal.

Auto merge exists so a healthy sync needs nobody. It also means a run that is
wrong - or merely enormous - reaches NetBox with nobody having looked, which is
how a 147k-change run merged before its operator could ask whether it was
right. This decides, between staging and merging, whether to leave the run
staged for a person instead.

Deliberately narrow, because a guard that cries wolf gets switched off:

- only when the sync has Auto merge on (otherwise the run is already staged for
  review and there is nothing to add);
- only when the sync has finished a run before, so the first load a sync ever
  does is never held - that run has no norm to be far from;
- only for a model the rule in ``run_size_anomaly`` flags: at least
  ``MIN_CHANGES`` staged and several times the table's established size or the
  typical earlier run. A small model, an empty table and a model without
  history are all left alone.

It fails open. A guard that raises would stop every sync over a bug in its own
arithmetic, so an error here is logged and the run proceeds as it did before
this existed; the Health check still shows the run's size.
"""

from .health_evidence import applied_change_history
from .health_evidence import existing_row_count
from .run_size_anomaly import assess_run_size
from .run_size_anomaly import describe_finding
from .run_size_anomaly import HOLD_JOB_DATA_KEY
from .run_size_anomaly import MIN_CHANGES


def _has_finished_a_run_before(sync, ingestion):
    return (
        sync.forwardingestion_set.exclude(pk=ingestion.pk)
        .filter(job__completed__isnull=False)
        .exists()
    )


def decide_run_size_hold(sync, ingestion, model_change_counts):
    """Findings that justify holding this run, or an empty list.

    ``model_change_counts`` maps ``app.model`` to the changes staged for it in
    this run's branch.
    """
    if not sync.auto_merge:
        return []
    if not _has_finished_a_run_before(sync, ingestion):
        return []
    candidates = {
        model: changes
        for model, changes in model_change_counts.items()
        if changes >= MIN_CHANGES
    }
    if not candidates:
        return []
    models = {}
    for model, changes in candidates.items():
        rows, _note = existing_row_count(model)
        models[model] = {"changes": changes, "existing_rows": rows}
    return assess_run_size(models, history=applied_change_history(sync))


def hold_record(findings):
    """What is stored on the job so Health and the bundle can explain the hold."""
    return {
        "findings": [dict(finding) for finding in findings],
        "summary": " ".join(describe_finding(finding) for finding in findings),
    }


def record_hold(logger, findings):
    """Persist the hold on the job's data through the run's own logger."""
    logger.log_data[HOLD_JOB_DATA_KEY] = hold_record(findings)
    logger.flush()
