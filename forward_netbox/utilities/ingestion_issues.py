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
