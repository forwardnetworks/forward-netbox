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


def row_disposition(issue):
    """What the merge decided about THIS row: "skipped", "failed", or None.

    Written by `_MergeIssueRecorder.record` and by any sync-phase recorder that
    passes `disposition=`. ``None`` means the row predates the key, or was
    recorded by a path that does not classify - both fall back to the
    ingestion-wide reading below.
    """
    raw = getattr(issue, "raw_data", None)
    value = raw.get("disposition") if isinstance(raw, dict) else None
    return value if value in ("skipped", "failed") else None


def issue_blocking_disposition(issue):
    """What this row did: "none", "blocking", "skipped" or "promoted_over".

    The class predicate above answers "would an issue of this kind hold the
    baseline back". That is not the same question as "did it", and conflating
    them mislabels the row a customer sees most.

    Four states, because there are four. A NetBox validation rejection is
    recorded and skipped: no retry can change it, so it never holds the
    baseline back. Whether the baseline actually promoted is a separate,
    ingestion-wide fact - one skipped row beside one retryable failure leaves
    the run unpromoted, and the skipped row is still not what blocked it.
    Collapsing "skipped, run not yet promoted" into either "blocking" or
    "promoted over" is exactly the bug a customer reported: a red Blocking
    badge on a row whose own message said it did not hold the baseline back.

    The ROW says whether a retry could help; `baseline_ready` says only whether
    this run promoted. Rows with no recorded disposition keep the previous
    behaviour, so no migration is needed for issues written before 2.9.5.
    """
    if not is_blocking_issue(issue):
        return "none"
    ingestion = getattr(issue, "ingestion", None)
    promoted = bool(
        ingestion is not None and getattr(ingestion, "baseline_ready", False)
    )
    if row_disposition(issue) == "skipped":
        return "promoted_over" if promoted else "skipped"
    return "promoted_over" if promoted else "blocking"


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
