from ..choices import FORWARD_OPTIONAL_MODELS
from ..choices import ForwardExecutionStepStatusChoices
from ..exceptions import ForwardDependencySkipError
from .execution_ledger import execution_step_for_ingestion


TRANSIENT_RECOVERABLE_OPERATIONAL_ERROR_HINTS = (
    "the connection is closed",
    "terminating connection due to administrator command",
)


def _is_recoverable_operational_issue(ingestion, issue):
    if str(getattr(issue, "exception", "") or "") != "OperationalError":
        return False
    message = str(getattr(issue, "message", "") or "").lower()
    if not any(
        hint in message for hint in TRANSIENT_RECOVERABLE_OPERATIONAL_ERROR_HINTS
    ):
        return False
    step = execution_step_for_ingestion(ingestion)
    if step is None or step.ingestion_id != ingestion.pk:
        return False
    if step.status in {
        ForwardExecutionStepStatusChoices.MERGED,
        ForwardExecutionStepStatusChoices.SKIPPED,
        ForwardExecutionStepStatusChoices.CANCELLED,
    }:
        return True
    run = getattr(step, "run", None)
    if run is not None and int(run.next_step_index or 0) > int(step.index or 0):
        return True
    branch = step.branch or getattr(ingestion, "branch", None)
    if branch is None or getattr(branch, "status", "") == "merged":
        return False
    return step.status in {
        ForwardExecutionStepStatusChoices.STAGED,
        ForwardExecutionStepStatusChoices.MERGE_QUEUED,
        ForwardExecutionStepStatusChoices.MERGE_TIMEOUT,
    }


def blocking_issues_queryset(ingestion):
    """Return ingestion issues that should block baseline readiness."""
    queryset = ingestion.issues.exclude(model__in=FORWARD_OPTIONAL_MODELS).exclude(
        exception=ForwardDependencySkipError.__name__
    )
    try:
        issue_iterable = list(queryset)
    except TypeError:
        return queryset
    excluded_issue_ids = [
        issue.pk
        for issue in issue_iterable
        if _is_recoverable_operational_issue(ingestion, issue)
    ]
    if excluded_issue_ids:
        queryset = queryset.exclude(pk__in=excluded_issue_ids)
    return queryset


def has_blocking_issues(ingestion):
    try:
        exists_result = blocking_issues_queryset(ingestion).exists()
    except Exception:
        return False
    return exists_result if isinstance(exists_result, bool) else False
