from unittest import TestCase
from unittest.mock import Mock
from unittest.mock import patch

import httpx

from forward_netbox.exceptions import ForwardConnectivityError
from forward_netbox.utilities.turbobulk import TurboBulkClient
from forward_netbox.utilities.turbobulk import TurboBulkError


class TurboBulkClientTest(TestCase):
    def _client(self):
        return TurboBulkClient(
            base_url="https://netbox.example.com/",
            token="nbt_key.secret",
            verify=True,
            timeout=30,
        )

    def _response(self, status_code, data=None):
        response = Mock()
        response.status_code = status_code
        response.json.return_value = [] if data is None else data
        return response

    def test_requires_netbox_url_and_token(self):
        with self.assertRaisesRegex(TurboBulkError, "NetBox URL is required"):
            TurboBulkClient(base_url="", token="token")

        with self.assertRaisesRegex(TurboBulkError, "NetBox API token is required"):
            TurboBulkClient(base_url="https://netbox.example.com", token="")

    def test_check_capability_returns_available_model_count(self):
        response = self._response(
            200,
            [
                {"full_name": "dcim.site"},
                {"full_name": "dcim.device"},
            ],
        )
        http_client = Mock()
        http_client.request.return_value = response
        context = Mock()
        context.__enter__ = Mock(return_value=http_client)
        context.__exit__ = Mock(return_value=None)

        with patch(
            "forward_netbox.utilities.turbobulk.httpx.Client",
            return_value=context,
        ) as client_factory:
            capability = self._client().check_capability()

        self.assertTrue(capability.usable)
        self.assertEqual(capability.model_count, 2)
        client_factory.assert_called_once_with(
            timeout=30,
            verify=True,
            mounts=None,
        )
        http_client.request.assert_called_once_with(
            "GET",
            "https://netbox.example.com/api/plugins/turbobulk/models/",
            headers={
                "Accept": "application/json",
                "Authorization": "Bearer nbt_key.secret",
                "User-Agent": "forward-netbox-turbobulk-probe/0.3.1",
            },
        )

    def test_check_capability_maps_unavailable_statuses(self):
        for status_code, expected_reason in (
            (404, "not found"),
            (401, "cannot access"),
            (403, "cannot access"),
            (500, "HTTP 500"),
        ):
            with self.subTest(status_code=status_code):
                client = self._client()
                client._request = Mock(return_value=self._response(status_code))

                capability = client.check_capability()

                self.assertFalse(capability.usable)
                self.assertIn(expected_reason, capability.reason)
                self.assertEqual(capability.status_code, status_code)

    def test_legacy_token_uses_token_auth_scheme(self):
        client = TurboBulkClient(
            base_url="https://netbox.example.com",
            token="legacytoken",
        )

        self.assertEqual(client._headers()["Authorization"], "Token legacytoken")

    def test_request_uses_netbox_proxy_routers(self):
        response = self._response(404)
        http_client = Mock()
        http_client.request.return_value = response
        context = Mock()
        context.__enter__ = Mock(return_value=http_client)
        context.__exit__ = Mock(return_value=None)

        with (
            patch(
                "forward_netbox.utilities.turbobulk.resolve_proxies",
                return_value={"https": "http://proxy.example.com:3128"},
            ) as resolve_proxies,
            patch(
                "forward_netbox.utilities.turbobulk.httpx.HTTPTransport",
                side_effect=lambda proxy: f"transport:{proxy}",
            ),
            patch(
                "forward_netbox.utilities.turbobulk.httpx.Client",
                return_value=context,
            ) as client_factory,
        ):
            capability = self._client().check_capability()

        self.assertFalse(capability.usable)
        resolve_proxies.assert_called_once()
        client_factory.assert_called_once_with(
            timeout=30,
            verify=True,
            mounts={"https://": "transport:http://proxy.example.com:3128"},
        )

    def test_httpx_timeout_raises_connectivity_error(self):
        client = self._client()
        with patch(
            "forward_netbox.utilities.turbobulk.httpx.Client",
            side_effect=httpx.TimeoutException("timeout"),
        ):
            with self.assertRaises(ForwardConnectivityError):
                client.check_capability()
