from typing import TYPE_CHECKING

from core.exceptions import SyncError

if TYPE_CHECKING:
    from .models import IPFabricIngestionIssue
    from .models import IPFabricIngestion


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


def create_or_get_sync_issue(
    exception: Exception,
    ingestion: "IPFabricIngestion",
    message: str = None,
    model: str = None,
    context: dict = None,
    data: dict = None,
) -> (bool, "IPFabricIngestionIssue"):
    """
    Helper function to handle sync errors and create IPFabricIngestionIssue if needed.
    """
    context = context or {}

    # TODO: This is to prevent circular import issues, clean it up later.
    from .models import IPFabricIngestionIssue

    if not hasattr(exception, "issue_id") or not exception.issue_id:
        issue = IPFabricIngestionIssue.objects.create(
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
        issue = IPFabricIngestionIssue.objects.get(id=exception.issue_id)
        return False, issue
