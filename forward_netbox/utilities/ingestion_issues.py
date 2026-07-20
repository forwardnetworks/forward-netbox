from ..choices import FORWARD_OPTIONAL_MODELS
from ..exceptions import ForwardDependencySkipError


def blocking_issues_queryset(ingestion):
    """Return ingestion issues that should block baseline readiness."""
    queryset = ingestion.issues.exclude(model__in=FORWARD_OPTIONAL_MODELS).exclude(
        exception=ForwardDependencySkipError.__name__
    )
    return queryset


def has_blocking_issues(ingestion):
    try:
        exists_result = blocking_issues_queryset(ingestion).exists()
    except Exception:
        return False
    return exists_result if isinstance(exists_result, bool) else False
