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
from forward_sdk.errors import ForwardExecutionError
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
    error = ForwardClientError(
        f"Forward API request failed with HTTP {exc.status}: {body}"
    )
    # Callers that must tell "refused" from "failed" read this rather than the
    # message text.
    error.status = exc.status
    return error


def _translate_transport_error(exc: ForwardTransportError) -> ForwardSyncError:
    cause = exc.__cause__
    if isinstance(cause, httpx.TimeoutException):
        return ForwardConnectivityError(
            "Forward API request timed out while connecting to Forward."
        )
    return ForwardConnectivityError(
        f"Could not connect to Forward API endpoint: {cause if cause is not None else exc}"
    )


# Forward's closed set of NQE execution outcomes that mean "did not succeed".
# The words are the plugin's own (never Forward's error text, which can quote a
# query or a value), so they can be named in a message, an issue and a bundle.
NQE_OUTCOME_WORDS = {
    "SYSTEM_ERROR": "system error",
    # Not "timed out": the fetch loop treats that phrase as a transient failure
    # and resubmits the whole workload, and a query that spent its whole budget
    # is not worth another one.
    "TIMED_OUT": "time limit reached",
    "USER_ERROR": "user error",
}
# The one outcome worth resubmitting: Forward failed internally. A timed-out
# query has already spent its whole budget and a user error fails the same way
# every time, so neither is retried.
NQE_RETRYABLE_OUTCOMES = frozenset({"SYSTEM_ERROR"})


def _translate_execution_error(exc: ForwardExecutionError) -> ForwardClientError:
    """An NQE execution that ran and finished badly, outcome kept.

    The SDK's message is ``finished with outcome <OUTCOME>: <detail>``, and the
    diagnostics summary stops at the first token that is not a plain word, so
    ``SYSTEM_ERROR`` cut every such failure to "finished with outcome" and the
    four failed queries in one customer's run read as identical and
    unexplainable. The outcome is one of three fixed values, so it is restated
    in the plugin's own words and the SDK's detail stays out.
    """
    outcome = str(getattr(exc, "outcome", "") or "")
    # A snapshot or collection job also raises this error, with its own state in
    # `outcome`; only an NQE execution is restated.
    words = (
        NQE_OUTCOME_WORDS.get(outcome, "")
        if str(exc).startswith("NQE execution")
        else ""
    )
    if not words:
        # An outcome Forward has not documented, or an execution error with
        # none (a snapshot or collection job): keep the generic wording rather
        # than invent a classification.
        error = ForwardClientError(f"Forward API request failed: {exc}")
        error.outcome = ""
        return error
    error = ForwardClientError(f"Forward NQE execution finished with outcome {words}.")
    error.outcome = outcome
    return error


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
    if isinstance(exc, ForwardExecutionError):
        return _translate_execution_error(exc)
    # ForwardPaginationError, ForwardExecutionError, ForwardConfigurationError,
    # and anything else this SDK might add later: no equivalent httpx failure
    # exists to reproduce the wording of, so this is the same generic
    # `ForwardClientError` the catch-all `httpx.HTTPError` branch in
    # `_request()` raises today.
    return ForwardClientError(f"Forward API request failed: {exc}")
