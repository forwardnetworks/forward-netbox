from core.exceptions import SyncError as _CoreSyncError


class ForwardSyncError(Exception):
    """Base exception for Forward sync failures."""


class ForwardShardResolutionError(_CoreSyncError):
    """Raised when a resumed shard's claimed index cannot be resolved against the
    rebuilt plan.

    Subclasses the core ``SyncError`` so existing sync error handling still
    catches it, while letting the stage-job runner distinguish a (bounded,
    retryable) resume-time plan desync from a genuine, terminal sync failure.
    """


class ForwardPartialMergeError(_CoreSyncError):
    """Raised when a branch merge applies only part of its change set.

    The branch remains ready so the failed rows can be inspected and retried;
    callers must not mark the ingestion complete or run post-sync overlays.
    """

    def __init__(self, message, *, applied, failed):
        super().__init__(message)
        self.applied = max(0, int(applied))
        self.failed = max(0, int(failed))


class ForwardOwnershipDispatchError(_CoreSyncError):
    """Durable ownership work was recorded but could not be enqueued."""


class ForwardClientError(ForwardSyncError):
    """Raised when a Forward API request fails."""


class ForwardFetchBudgetExceededError(ForwardClientError):
    """Per-workload wall-clock fetch budget exceeded."""


class ForwardConnectivityError(ForwardClientError):
    """Raised when a Forward API call cannot be reached."""


class ForwardLicenseTierError(ForwardClientError):
    """Raised when Forward refuses an NQE query for the org's license tier.

    A capability limit, not a fault: retrying, re-authenticating or re-running
    the sync cannot clear it. Distinguished from the generic client error so the
    operator is told which license facet is missing instead of reading a raw
    HTTP body. See `forward_netbox.utilities.license_tier`.
    """


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
        dependency: str | None = None,
        dependency_is_protecting: bool = False,
    ):
        super().__init__(message)
        self.model_string = model_string
        self.context = context or {}
        self.defaults = defaults or {}
        self.data = data or {}
        self.issue_id = issue_id
        # The model whose absence (or whose surviving children) caused this.
        # A schema identifier the plugin defines, so unlike `context` — which
        # carries the device or platform NAME and is reduced to key names before
        # anything persists it — this one can be recorded. Without it a skipped
        # row records only the exception class, and a customer reading six
        # identical rows has no way to learn which parent was missing.
        self.dependency = dependency or ""
        # Which DIRECTION the dependency points. Almost every skip is waiting on
        # something absent, but the delete path is the inverse: the row is still
        # REFERENCED and the database refuses to prune it. Both used to persist
        # the same "row processing skipped (...; app.model)" shape, so the two
        # opposite conditions read identically - a customer's DLM skips named
        # `netbox_dlm.inventoryitemsoftware` as the dependency of
        # `netbox_dlm.softwareversion`, which is backwards for a missing parent
        # and exactly right for a surviving child.
        self.dependency_is_protecting = bool(dependency_is_protecting)


class ForwardSearchError(ForwardDataError):
    """Raised when a sync lookup returns none or multiple matches."""


class ForwardDependencySkipError(ForwardDataError):
    """Raised when a row is skipped because an upstream dependency failed."""


class ForwardSyncDataError(ForwardDataError):
    """Raised when row processing fails for non-search data reasons."""
