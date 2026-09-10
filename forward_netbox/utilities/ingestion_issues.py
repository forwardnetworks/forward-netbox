from django.db.models import Q
from rq.timeouts import JobTimeoutException

from ..choices import FORWARD_OPTIONAL_MODELS
from ..exceptions import ForwardDependencySkipError


def blocking_issue_q():
    """The one definition of "blocking", as a ``Q`` over the issue rows.

    Baseline readiness, the Issues table's Blocking column and the Blocking
    filter all read this. Restating the two exclusions anywhere else is how the
    list comes to disagree with the readiness verdict it is meant to explain -
    an operator triaging 400 rows against a "not ready" banner needs the same
    answer in both places.
    """
    return ~Q(model__in=FORWARD_OPTIONAL_MODELS) & ~Q(
        exception=ForwardDependencySkipError.__name__
    )


def is_blocking_issue(issue):
    """Whether one already-loaded issue row blocks readiness.

    The Python twin of ``blocking_issue_q``, for the table column: rendering a
    page of rows must not cost a query per row, and the row already carries
    both fields the predicate reads.
    """
    return (
        issue.model not in FORWARD_OPTIONAL_MODELS
        and issue.exception != ForwardDependencySkipError.__name__
    )


def issue_blocking_disposition(issue):
    """What this row actually did: "blocking", "promoted_over", or "none".

    The class predicate above answers "would an issue of this kind hold the
    baseline back". It is not the same question as "did it", and conflating the
    two mislabels the row a customer sees most.

    A NetBox validation rejection is recorded and skipped: re-running cannot
    change it, so the merge records the row and promotes the baseline over it.
    `health_checks.py` gets this right by testing `skipped_change_count` BEFORE
    `has_blocking_issues`, so the ingestion reports "promoted over them". The
    row-level column had no such ordering and would have labelled a customer's
    recurring `ipam.ipaddress` primary-IP rejection "Blocking" on a run whose
    baseline had promoted - the exact disagreement between a list and the
    banner it explains that one shared predicate was meant to prevent.

    `baseline_ready` is the fact that settles it: if the baseline promoted,
    nothing was blocked, whatever class the row belongs to.
    """
    if not is_blocking_issue(issue):
        return "none"
    ingestion = getattr(issue, "ingestion", None)
    if ingestion is not None and getattr(ingestion, "baseline_ready", False):
        return "promoted_over"
    return "blocking"


def blocking_issues_queryset(ingestion):
    """Return ingestion issues that should block baseline readiness."""
    return ingestion.issues.filter(blocking_issue_q())


def has_blocking_issues(ingestion):
    try:
        exists_result = blocking_issues_queryset(ingestion).exists()
    except JobTimeoutException:
        raise
    except Exception:
        return False
    return exists_result if isinstance(exists_result, bool) else False
