try:  # pragma: no cover - core is always present inside NetBox
    from core.exceptions import SyncError as _CoreSyncError
except ModuleNotFoundError:  # pragma: no cover - tooling imports outside NetBox
    _CoreSyncError = Exception


class ForwardSyncError(Exception):
    """Base exception for Forward sync failures."""


class ForwardShardResolutionError(_CoreSyncError):
    """Raised when a resumed shard's claimed index cannot be resolved against the
    rebuilt plan.

    Subclasses the core ``SyncError`` so existing sync error handling still
    catches it, while letting the stage-job runner distinguish a (bounded,
    retryable) resume-time plan desync from a genuine, terminal sync failure.
    """


class ForwardClientError(ForwardSyncError):
    """Raised when a Forward API request fails."""


class ForwardConnectivityError(ForwardClientError):
    """Raised when a Forward API call cannot be reached."""


class ForwardQueryError(ForwardSyncError):
    """Raised when a built-in Forward NQE query fails."""


class ForwardDataError(ForwardSyncError):
    """Base exception for sync data handling failures."""

    def __init__(
        self,
        message: str,
        *,
        model_string: str | None = None,
        context: dict | None = None,
        defaults: dict | None = None,
        data: dict | None = None,
        issue_id: int | None = None,
    ):
        super().__init__(message)
        self.model_string = model_string
        self.context = context or {}
        self.defaults = defaults or {}
        self.data = data or {}
        self.issue_id = issue_id


class ForwardSearchError(ForwardDataError):
    """Raised when a sync lookup returns none or multiple matches."""


class ForwardDependencySkipError(ForwardDataError):
    """Raised when a row is skipped because an upstream dependency failed."""


class ForwardSyncDataError(ForwardDataError):
    """Raised when row processing fails for non-search data reasons."""
