class ForwardSyncError(Exception):
    """Base exception for Forward sync failures."""


class ForwardClientError(ForwardSyncError):
    """Raised when a Forward API request fails."""


class ForwardConnectivityError(ForwardClientError):
    """Raised when a Forward API call cannot be reached."""


class ForwardQueryError(ForwardSyncError):
    """Raised when a built-in Forward NQE query fails."""
