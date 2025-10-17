from typing import TYPE_CHECKING

from core.exceptions import SyncError

if TYPE_CHECKING:
    from .models import ForwardIngestionIssue
    from .models import ForwardIngestion


class IngestionIssue(Exception):
    """
    This exception is used to indicate an issue during the ingestion process.
    """

    # Store created issue object ID if it exists for this exception
    issue_id = None
    model: str = ""
    defaults: dict[str, str] = {}
    coalesce_fields: dict[str, str] = {}

    def __init__(self, model: str, data: dict, context: dict = None, issue_id=None):
        super().__init__()
        self.model = model
        self.data = data
        context = context or {}
        self.defaults = context.pop("defaults", {})
        self.coalesce_fields = context
        self.issue_id = issue_id


class SearchError(IngestionIssue, LookupError):
    def __init__(self, message: str, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.message = message

    def __str__(self):
        return self.message


class SyncDataError(IngestionIssue, SyncError):
    def __str__(self):
        return f"Sync failed for {self.model}: coalesce_fields={self.coalesce_fields} defaults={self.defaults}."


class IPAddressDuplicateError(IngestionIssue, SyncError):
    def __str__(self):
        return f"IP address {self.data.get('address')} already exists in {self.model} with coalesce_fields={self.coalesce_fields}."


class ForwardAPIError(SyncError):
    """Raised when Forward REST API returns an error response."""

    def __init__(self, message: str, *, status_code: int | None = None):
        super().__init__(message)
        self.status_code = status_code


def create_or_get_sync_issue(
    exception: Exception,
    ingestion: "ForwardIngestion",
    message: str = None,
    model: str = None,
    context: dict = None,
    data: dict = None,
) -> (bool, "ForwardIngestionIssue"):
    """
    Helper function to handle sync errors and create ForwardIngestionIssue if needed.
    """
    context = context or {}

    from .models import ForwardIngestionIssue  # Imported lazily to avoid circular import.

    if not hasattr(exception, "issue_id") or not exception.issue_id:
        issue = ForwardIngestionIssue.objects.create(
            ingestion=ingestion,
            exception=exception.__class__.__name__,
            message=message or getattr(exception, "message", str(exception)),
            model=model,
            coalesce_fields={k: v for k, v in context.items() if k not in ["defaults"]},
            defaults=context.get("defaults", dict()),
            raw_data=data or dict(),
        )
        if hasattr(exception, "issue_id"):
            exception.issue_id = issue.id
        return True, issue
    else:
        issue = ForwardIngestionIssue.objects.get(id=exception.issue_id)
        return False, issue
