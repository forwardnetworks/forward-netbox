# A Forward license-tier denial must be recognised, not reported as HTTP noise.
#
# Forward gates NQE by license tier and refuses a query with one sentence. Without
# this classification the operator sees a raw HTTP body among timeouts and auth
# failures, with nothing saying the license is the problem or which capability is
# missing. The tier is not exposed over Forward's API, so the denial is the only
# signal available.
from types import SimpleNamespace
from unittest.mock import Mock
from unittest.mock import patch

import httpx
from django.test import TestCase

from forward_netbox.exceptions import ForwardClientError
from forward_netbox.exceptions import ForwardLicenseTierError
from forward_netbox.utilities.crypto import encrypt_secret
from forward_netbox.utilities.forward_api import ForwardClient
from forward_netbox.utilities.license_tier import denied_query_name
from forward_netbox.utilities.license_tier import is_license_tier_denial
from forward_netbox.utilities.license_tier import license_tier_denial_message

DENIAL = (
    "Query /Forward/NetBox/forward_devices is not permitted for this "
    "organization's license tier"
)


class LicenseTierDenialTest(TestCase):
    def test_recognises_the_denial(self):
        self.assertTrue(is_license_tier_denial(DENIAL))

    def test_recognises_it_with_a_typographic_apostrophe(self):
        # The apostrophe varies with response encoding; matching it literally
        # would silently fall back to the generic error path.
        self.assertTrue(
            is_license_tier_denial(DENIAL.replace("organization's", "organization’s"))
        )

    def test_does_not_match_unrelated_forward_errors(self):
        for message in (
            "Forward API request failed with HTTP 500: internal error",
            "Query /Forward/NetBox/forward_devices timed out",
            "unauthorized",
            "",
            None,
        ):
            with self.subTest(message=message):
                self.assertFalse(is_license_tier_denial(message))

    def test_extracts_the_denied_query_name(self):
        self.assertEqual(denied_query_name(DENIAL), "/Forward/NetBox/forward_devices")

    def test_survives_a_denial_without_a_query_name(self):
        message = "is not permitted for this organization's license tier"
        self.assertTrue(is_license_tier_denial(message))
        self.assertEqual(denied_query_name(message), "")
        # Still produces usable guidance rather than a half-built sentence.
        self.assertIn("NETWORK facet", license_tier_denial_message(message))

    def test_message_names_the_query_and_both_facets(self):
        rendered = license_tier_denial_message(DENIAL)

        self.assertIn("/Forward/NetBox/forward_devices", rendered)
        self.assertIn("NETWORK facet", rendered)
        self.assertIn("SECURITY facet", rendered)

    def test_message_states_the_tier_cannot_be_checked_in_advance(self):
        # Guards against a future change that adds a pre-flight tier check on
        # the assumption the API exposes it. It does not.
        self.assertIn("does not expose", license_tier_denial_message(DENIAL))


class LicenseTierClientTest(TestCase):
    """Drives the real `ForwardClient._request`, not a copy of its branch.

    Re-implementing the classification here would pass even if the client were
    never wired to it, which is the whole thing this test exists to prove.
    """

    def setUp(self):
        self.client = ForwardClient(
            SimpleNamespace(
                url="https://fwd.app",
                parameters={
                    "username": "user@example.com",
                    "password": encrypt_secret("secret"),
                    "verify": True,
                    "timeout": 1200,
                },
            )
        )

    def _request_returning(self, status_code, body):
        """Run `_request` against a transport that answers with `status_code`."""
        request = httpx.Request("POST", "https://fwd.app/api/nqe")
        response = httpx.Response(status_code, text=body, request=request)
        transport = Mock()
        transport.request.return_value = response
        transport.__enter__ = Mock(return_value=transport)
        transport.__exit__ = Mock(return_value=None)

        with (
            patch(
                "forward_netbox.utilities.forward_api_impl.httpx.Client",
                return_value=transport,
            ),
            patch.object(self.client, "_throttle_request"),
            patch("forward_netbox.utilities.forward_api_impl.time.sleep"),
        ):
            return self.client._request("POST", "/nqe")

    def test_license_denial_raises_the_specific_error(self):
        with self.assertRaises(ForwardLicenseTierError) as caught:
            self._request_returning(403, DENIAL)

        rendered = str(caught.exception)
        self.assertIn("/Forward/NetBox/forward_devices", rendered)
        self.assertIn("NETWORK facet", rendered)

    def test_license_error_is_still_a_client_error(self):
        # Existing sync handlers catch ForwardClientError; the new class must
        # not escape them and turn a capability limit into a crash.
        self.assertTrue(issubclass(ForwardLicenseTierError, ForwardClientError))

    def test_other_403s_keep_the_generic_error(self):
        with self.assertRaises(ForwardClientError) as caught:
            self._request_returning(403, "forbidden: insufficient permissions")

        self.assertNotIsInstance(caught.exception, ForwardLicenseTierError)
        self.assertIn("HTTP 403", str(caught.exception))
