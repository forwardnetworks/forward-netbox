"""Pin `translate_client_exception`'s mapping from every `forward-sdk`
exception type to this plugin's own hierarchy, and confirm `diagnostics.py`'s
existing needle-based classifier reads the result exactly as it reads the
current httpx-based failures - the whole point of the forward-sdk migration
plan's step 4 (docs/03_Plans/active/2026-09-07-forward-sdk-migration.md).

Per the plan's own Validation section: "One case per SDK exception type
added to the persisted-failure-reason tests before the swap, not after."
This is not wired into `ForwardClient` yet (that is step 5), so every case
here constructs an SDK exception directly rather than exercising a live
call.
"""

from unittest import TestCase

import httpx
from forward_sdk.errors import ForwardAuthError
from forward_sdk.errors import ForwardBadRequestError
from forward_sdk.errors import ForwardConfigurationError
from forward_sdk.errors import ForwardConflictError
from forward_sdk.errors import ForwardExecutionError
from forward_sdk.errors import ForwardNotFoundError
from forward_sdk.errors import ForwardNqeQueryError
from forward_sdk.errors import ForwardPaginationError
from forward_sdk.errors import ForwardPermissionError
from forward_sdk.errors import ForwardRateLimitError
from forward_sdk.errors import ForwardResponseError
from forward_sdk.errors import ForwardServerError
from forward_sdk.errors import ForwardTimeoutError as SDKForwardTimeoutError
from forward_sdk.errors import ForwardTransportError

from forward_netbox.exceptions import ForwardClientError
from forward_netbox.exceptions import ForwardConnectivityError
from forward_netbox.exceptions import ForwardFetchBudgetExceededError
from forward_netbox.exceptions import ForwardLicenseTierError
from forward_netbox.utilities import diagnostics
from forward_netbox.utilities.forward_client_errors import translate_client_exception

LICENSE_DENIAL_BODY = (
    "Query forward_device_vulnerabilities is not permitted for this "
    "organization's license tier"
)


def _api_error(cls, *, status, text="generic failure body", **kwargs):
    return cls(
        f"request failed with HTTP {status}: {text}", status=status, text=text, **kwargs
    )


class TranslateClientExceptionTest(TestCase):
    def test_a_transport_error_with_a_timeout_cause_becomes_connectivity_timeout(self):
        cause = httpx.ConnectTimeout("timed out")
        sdk_exc = ForwardTransportError("GET /foo failed: timed out", attempts=3)
        sdk_exc.__cause__ = cause

        translated = translate_client_exception(sdk_exc)

        self.assertIsInstance(translated, ForwardConnectivityError)
        self.assertEqual(
            diagnostics.exception_type(translated), "ForwardConnectivityError"
        )
        self.assertEqual(diagnostics.failure_reason(translated), "timeout")

    def test_a_transport_error_with_a_connection_cause_becomes_connectivity_connection_refused(
        self,
    ):
        cause = httpx.ConnectError("[Errno 111] Connection refused")
        sdk_exc = ForwardTransportError(
            "GET /foo failed: connection refused", attempts=3
        )
        sdk_exc.__cause__ = cause

        translated = translate_client_exception(sdk_exc)

        self.assertIsInstance(translated, ForwardConnectivityError)
        # Both needles legitimately match: "could not connect" is the
        # translator's own fixed wording (matching `_request()`'s "Could not
        # connect to Forward API endpoint: {exc}" today) and "connection
        # refused" comes from the underlying httpx exception's own text -
        # exactly as today's httpx-based path produces both slugs together
        # for the same underlying failure.
        self.assertEqual(
            diagnostics.failure_reason(translated),
            "connection-refused+connection-failed",
        )

    def test_an_sdk_timeout_error_becomes_a_fetch_budget_exceeded_error(self):
        sdk_exc = SDKForwardTimeoutError("execution.wait() deadline expired")

        translated = translate_client_exception(sdk_exc)

        self.assertIsInstance(translated, ForwardFetchBudgetExceededError)
        self.assertEqual(
            diagnostics.failure_reason(translated), "fetch-budget-exceeded"
        )

    def test_a_bad_request_error_becomes_a_generic_client_error_with_the_status(self):
        sdk_exc = _api_error(ForwardBadRequestError, status=400)

        translated = translate_client_exception(sdk_exc)

        self.assertIsInstance(translated, ForwardClientError)
        self.assertNotIsInstance(translated, ForwardConnectivityError)
        self.assertEqual(diagnostics.failure_reason(translated), "http-400")

    def test_an_nqe_query_error_is_a_bad_request_and_translates_the_same_way(self):
        sdk_exc = _api_error(
            ForwardNqeQueryError, status=400, text="query does not compile"
        )

        translated = translate_client_exception(sdk_exc)

        self.assertIsInstance(translated, ForwardClientError)
        self.assertEqual(diagnostics.failure_reason(translated), "http-400")

    def test_an_auth_error_becomes_a_generic_client_error(self):
        sdk_exc = _api_error(ForwardAuthError, status=401)

        translated = translate_client_exception(sdk_exc)

        self.assertIsInstance(translated, ForwardClientError)
        self.assertEqual(diagnostics.failure_reason(translated), "http-401")

    def test_a_permission_error_with_a_license_denial_body_becomes_license_tier_error(
        self,
    ):
        sdk_exc = _api_error(
            ForwardPermissionError, status=403, text=LICENSE_DENIAL_BODY
        )

        translated = translate_client_exception(sdk_exc)

        self.assertIsInstance(translated, ForwardLicenseTierError)
        self.assertEqual(
            diagnostics.exception_type(translated), "ForwardLicenseTierError"
        )

    def test_a_permission_error_without_a_license_denial_body_is_generic(self):
        sdk_exc = _api_error(ForwardPermissionError, status=403)

        translated = translate_client_exception(sdk_exc)

        self.assertIsInstance(translated, ForwardClientError)
        self.assertNotIsInstance(translated, ForwardLicenseTierError)
        self.assertEqual(diagnostics.failure_reason(translated), "http-403")

    def test_a_not_found_error_becomes_a_generic_client_error(self):
        sdk_exc = _api_error(ForwardNotFoundError, status=404)

        translated = translate_client_exception(sdk_exc)

        self.assertIsInstance(translated, ForwardClientError)
        self.assertEqual(diagnostics.failure_reason(translated), "http-404")

    def test_a_conflict_error_becomes_a_generic_client_error(self):
        sdk_exc = _api_error(ForwardConflictError, status=409)

        translated = translate_client_exception(sdk_exc)

        self.assertIsInstance(translated, ForwardClientError)
        self.assertEqual(diagnostics.failure_reason(translated), "http-409")

    def test_a_rate_limit_error_is_transient_and_becomes_connectivity(self):
        sdk_exc = _api_error(ForwardRateLimitError, status=429, retry_after=30.0)

        translated = translate_client_exception(sdk_exc)

        self.assertIsInstance(translated, ForwardConnectivityError)
        self.assertEqual(diagnostics.failure_reason(translated), "http-429")

    def test_a_server_error_at_a_transient_status_becomes_connectivity(self):
        sdk_exc = _api_error(ForwardServerError, status=503)

        translated = translate_client_exception(sdk_exc)

        self.assertIsInstance(translated, ForwardConnectivityError)
        self.assertEqual(diagnostics.failure_reason(translated), "http-503")

    def test_a_server_error_at_a_non_transient_status_is_generic(self):
        sdk_exc = _api_error(ForwardServerError, status=500)

        translated = translate_client_exception(sdk_exc)

        self.assertIsInstance(translated, ForwardClientError)
        self.assertNotIsInstance(translated, ForwardConnectivityError)
        self.assertEqual(diagnostics.failure_reason(translated), "http-500")

    def test_a_response_shape_error_becomes_a_generic_client_error(self):
        sdk_exc = ForwardResponseError(
            "field 'id' is required",
            payload={"name": "x"},
            model_name="Snapshot",
        )

        translated = translate_client_exception(sdk_exc)

        self.assertIsInstance(translated, ForwardClientError)
        self.assertEqual(diagnostics.failure_reason(translated), "shape-error")

    def test_uncatalogued_sdk_exceptions_fall_back_to_a_generic_client_error(self):
        for sdk_exc in (
            ForwardPaginationError("page did not advance", rows=10, pages=2),
            ForwardExecutionError("execution failed", outcome="failed"),
            ForwardConfigurationError("mutually exclusive query references"),
        ):
            with self.subTest(sdk_exc=type(sdk_exc).__name__):
                translated = translate_client_exception(sdk_exc)
                self.assertIsInstance(translated, ForwardClientError)
                self.assertNotIsInstance(translated, ForwardConnectivityError)
                self.assertNotIsInstance(translated, ForwardLicenseTierError)
                self.assertNotIsInstance(translated, ForwardFetchBudgetExceededError)
