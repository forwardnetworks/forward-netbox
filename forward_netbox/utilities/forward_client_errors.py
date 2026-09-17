"""Translate `forward-sdk` exceptions into this plugin's own exception
hierarchy (forward-sdk migration plan, step 4 of 7 - see
docs/03_Plans/active/2026-09-07-forward-sdk-migration.md).

Not wired into `ForwardClient` yet - that is step 5's transport swap. This
module exists on its own, with its own test coverage, so step 5 becomes
"catch `forward_sdk.errors.ForwardError` and call `translate_client_exception`
instead of the current httpx exception handling in `_request()`" rather than
a change to what gets classified.

**The constraint this module exists to satisfy**: `diagnostics.py`'s
`failure_reason()` does not dispatch on exception type. It matches
value-free NEEDLE WORDS against `str(exc).casefold()`
(`_FAILURE_REASON_RULES`), and recovers an HTTP status via a regex over the
same string as a fallback (`_http_status_slug`). Both stay untouched by this
step. Every message this module builds is therefore engineered to reproduce
the exact wording `ForwardClient._request()` builds today for the
equivalent httpx failure, so `failure_reason()` and `exception_type()`
produce byte-identical output whether the raised exception came from httpx
or, after step 5, from `forward-sdk` - not because the SDK's own richer
typing (`ForwardAuthError`, `ForwardPermissionError`, ...) is discarded, but
because reproducing today's wording is what "no behavior change" means for
a step whose whole point is that the classifier does not need to change
underneath it.

A license-tier denial is checked first regardless of which `ForwardAPIError`
subclass carried it, exactly as `_request()` does today via
`is_license_tier_denial(body)` in its generic non-transient-status branch -
Forward's own denial wording, not the HTTP status, is what identifies it.
"""

import httpx
from forward_sdk.errors import ForwardAPIError
from forward_sdk.errors import ForwardError
from forward_sdk.errors import ForwardResponseError
from forward_sdk.errors import ForwardTimeoutError as SDKForwardTimeoutError
from forward_sdk.errors import ForwardTransportError

from ..exceptions import ForwardClientError
from ..exceptions import ForwardConnectivityError
from ..exceptions import ForwardFetchBudgetExceededError
from ..exceptions import ForwardLicenseTierError
from ..exceptions import ForwardSyncError
from .license_tier import is_license_tier_denial
from .license_tier import license_tier_denial_message

# Moved here from `forward_api_impl.py` (which re-imports it, since its own
# `_request()` still needs it until step 5's transport swap) rather than
# duplicated, because this module and that status set are the same contract:
# which HTTP failures are worth retrying, read by both the retry loop today
# and this translator's transient-vs-terminal branch once step 5 wires it in.
TRANSIENT_FORWARD_HTTP_STATUS_CODES = {408, 429, 502, 503, 504}


def _translate_api_error(exc: ForwardAPIError) -> ForwardSyncError:
    body = exc.text or str(getattr(exc.error_info, "message", "") or "")
    if is_license_tier_denial(body):
        return ForwardLicenseTierError(license_tier_denial_message(body))
    if exc.status in TRANSIENT_FORWARD_HTTP_STATUS_CODES:
        return ForwardConnectivityError(
            "Forward API request returned transient HTTP "
            f"{exc.status}; retry attempts were exhausted."
        )
    return ForwardClientError(
        f"Forward API request failed with HTTP {exc.status}: {body}"
    )


def _translate_transport_error(exc: ForwardTransportError) -> ForwardSyncError:
    cause = exc.__cause__
    if isinstance(cause, httpx.TimeoutException):
        return ForwardConnectivityError(
            "Forward API request timed out while connecting to Forward."
        )
    return ForwardConnectivityError(
        f"Could not connect to Forward API endpoint: {cause if cause is not None else exc}"
    )


def translate_client_exception(exc: ForwardError) -> ForwardSyncError:
    """Return the plugin exception `exc` should be re-raised as.

    Every branch is a status- or cause-based dispatch, never a message-text
    guess, because the SDK's typed hierarchy makes that unnecessary here -
    the guessing this replaces already happened once, inside the SDK itself,
    to decide which of its own exception classes to raise.
    """
    if isinstance(exc, ForwardTransportError):
        return _translate_transport_error(exc)
    if isinstance(exc, SDKForwardTimeoutError):
        return ForwardFetchBudgetExceededError(
            "Forward NQE fetch exceeded its wall-clock budget"
        )
    if isinstance(exc, ForwardAPIError):
        return _translate_api_error(exc)
    if isinstance(exc, ForwardResponseError):
        return ForwardClientError(
            f"Forward API returned an unexpected response shape: {exc}"
        )
    # ForwardPaginationError, ForwardExecutionError, ForwardConfigurationError,
    # and anything else this SDK might add later: no equivalent httpx failure
    # exists to reproduce the wording of, so this is the same generic
    # `ForwardClientError` the catch-all `httpx.HTTPError` branch in
    # `_request()` raises today.
    return ForwardClientError(f"Forward API request failed: {exc}")
