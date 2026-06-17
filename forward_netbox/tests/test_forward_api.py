from types import SimpleNamespace
from unittest import TestCase
from unittest.mock import Mock
from unittest.mock import patch

import httpx

from forward_netbox.exceptions import ForwardClientError
from forward_netbox.exceptions import ForwardConnectivityError
from forward_netbox.utilities import forward_api_impl
from forward_netbox.utilities.forward_api import ForwardClient


class FakeSharedCache:
    def __init__(self):
        self.store = {}

    def get(self, key, default=None):
        return self.store.get(key, default)

    def set(self, key, value, timeout=None):
        self.store[key] = value

    def add(self, key, value, timeout=None):
        if key in self.store:
            return False
        self.store[key] = value
        return True

    def delete(self, key):
        self.store.pop(key, None)

    def incr(self, key):
        value = int(self.store.get(key, 0) or 0) + 1
        self.store[key] = value
        return value


class ForwardClientTest(TestCase):
    def setUp(self):
        shared_cache = forward_api_impl._shared_read_cache()
        if hasattr(shared_cache, "clear"):
            shared_cache.clear()
        self.client = ForwardClient(
            SimpleNamespace(
                url="https://fwd.app",
                parameters={
                    "username": "user@example.com",
                    "password": "secret",
                    "verify": True,
                    "timeout": 1200,
                },
            )
        )

    def _response(self, data):
        response = Mock()
        response.json.return_value = data
        return response

    def test_api_request_rate_limit_defaults_for_forward_saas(self):
        self.assertEqual(self.client.api_requests_per_minute, 1800)
        self.assertAlmostEqual(self.client._api_request_min_interval, 1 / 30)

    def test_api_request_rate_limit_defaults_disabled_for_custom_sources(self):
        client = ForwardClient(
            SimpleNamespace(
                type="custom",
                url="https://forward.example.com",
                parameters={
                    "username": "user@example.com",
                    "password": "secret",
                },
            )
        )

        self.assertEqual(client.api_requests_per_minute, 0)
        self.assertEqual(client._api_request_min_interval, 0.0)

    def test_api_request_rate_limit_explicit_zero_disables(self):
        client = ForwardClient(
            SimpleNamespace(
                type="saas",
                url="https://fwd.app",
                parameters={
                    "username": "user@example.com",
                    "password": "secret",
                    "api_requests_per_minute": 0,
                },
            )
        )

        self.assertEqual(client.api_requests_per_minute, 0)
        self.assertEqual(client._api_request_min_interval, 0.0)

    def test_api_request_rate_limit_spaces_requests_in_process(self):
        forward_api_impl._RATE_LIMIT_LAST_REQUEST_AT.clear()
        client = ForwardClient(
            SimpleNamespace(
                url="https://fwd.app",
                parameters={
                    "username": "rate-limit@example.com",
                    "password": "secret",
                    "api_requests_per_minute": 60,
                },
            )
        )

        with (
            patch(
                "forward_netbox.utilities.forward_api_impl._shared_rate_limit_cache",
                return_value=None,
            ),
            patch(
                "forward_netbox.utilities.forward_api_impl.time.time",
                side_effect=[100.0, 100.2, 101.0],
            ),
            patch("forward_netbox.utilities.forward_api_impl.time.sleep") as sleep,
        ):
            client._throttle_request()
            client._throttle_request()

        sleep.assert_called_once()
        self.assertAlmostEqual(sleep.call_args.args[0], 0.8)
        self.assertAlmostEqual(
            client.api_usage_summary()["throttle_sleep_seconds"],
            0.8,
        )

    def test_api_request_rate_limit_key_does_not_expose_username(self):
        client = ForwardClient(
            SimpleNamespace(
                url="https://fwd.app",
                parameters={
                    "username": "rate-limit@example.com",
                    "password": "secret",
                    "api_requests_per_minute": 120,
                },
            )
        )

        key = client._rate_limit_key()

        self.assertNotIn("rate-limit@example.com", key)
        self.assertIn("forward-api-rate-limit", key)

    def test_network_and_head_commit_reads_are_cached_per_client(self):
        self.client._request = Mock(
            side_effect=[
                self._response(
                    [
                        {
                            "id": "network-1",
                            "name": "Network 1",
                        }
                    ]
                ),
                self._response({"id": "commit-1"}),
            ]
        )

        networks_first = self.client.get_networks()
        networks_second = self.client.get_networks()
        head_first = self.client.get_org_nqe_head_commit_id()
        head_second = self.client.get_org_nqe_head_commit_id()

        self.assertEqual(self.client._request.call_count, 2)
        self.assertEqual(networks_first, networks_second)
        self.assertEqual(head_first, head_second)
        self.assertEqual(networks_first[0]["label"], "Network 1 (network-1)")
        self.assertEqual(head_first, "commit-1")

    def test_shared_read_cache_reuses_network_reads_across_clients(self):
        shared_cache = FakeSharedCache()
        client_one = ForwardClient(
            SimpleNamespace(
                url="https://fwd.app",
                parameters={
                    "username": "user@example.com",
                    "password": "secret",
                    "verify": True,
                    "timeout": 1200,
                },
            )
        )
        client_two = ForwardClient(
            SimpleNamespace(
                url="https://fwd.app",
                parameters={
                    "username": "user@example.com",
                    "password": "secret",
                    "verify": True,
                    "timeout": 1200,
                },
            )
        )
        client_one._request = Mock(
            return_value=self._response([{"id": "network-1", "name": "Network 1"}])
        )
        client_two._request = Mock(
            side_effect=AssertionError("shared cache should avoid second request")
        )

        with patch(
            "forward_netbox.utilities.forward_api_impl._shared_read_cache",
            return_value=shared_cache,
        ):
            first = client_one.get_networks()
            second = client_two.get_networks()

        self.assertEqual(first, second)
        self.assertEqual(client_one._request.call_count, 1)
        self.assertEqual(client_two._request.call_count, 0)
        self.assertEqual(first[0]["label"], "Network 1 (network-1)")

    def test_shared_query_cache_generation_invalidates_after_mutation(self):
        shared_cache = FakeSharedCache()
        client_one = ForwardClient(
            SimpleNamespace(
                url="https://fwd.app",
                parameters={
                    "username": "user@example.com",
                    "password": "secret",
                    "verify": True,
                    "timeout": 1200,
                },
            )
        )
        client_two = ForwardClient(
            SimpleNamespace(
                url="https://fwd.app",
                parameters={
                    "username": "user@example.com",
                    "password": "secret",
                    "verify": True,
                    "timeout": 1200,
                },
            )
        )
        client_three = ForwardClient(
            SimpleNamespace(
                url="https://fwd.app",
                parameters={
                    "username": "user@example.com",
                    "password": "secret",
                    "verify": True,
                    "timeout": 1200,
                },
            )
        )
        client_one._request = Mock(return_value=self._response({"id": "commit-1"}))
        client_two._request = Mock(
            side_effect=[
                self._response({}),
                self._response({"id": "commit-2"}),
            ]
        )
        client_three._request = Mock(
            side_effect=AssertionError("shared cache should avoid third request")
        )

        with patch(
            "forward_netbox.utilities.forward_api_impl._shared_read_cache",
            return_value=shared_cache,
        ):
            first = client_one.get_org_nqe_head_commit_id()
            second = client_two.commit_org_nqe_queries(
                query_paths=["netbox/forward_devices"],
                message="Publish test queries",
            )
            third = client_three.get_org_nqe_head_commit_id()

        self.assertEqual(first, "commit-1")
        self.assertEqual(second, "commit-2")
        self.assertEqual(third, "commit-2")
        self.assertEqual(client_one._request.call_count, 1)
        self.assertEqual(client_two._request.call_count, 2)
        self.assertEqual(client_three._request.call_count, 0)

    def test_request_throttles_each_forward_http_attempt(self):
        response = Mock()
        client = Mock()
        client.request.return_value = response
        client_context = Mock()
        client_context.__enter__ = Mock(return_value=client)
        client_context.__exit__ = Mock(return_value=None)

        with (
            patch(
                "forward_netbox.utilities.forward_api_impl.httpx.Client",
                return_value=client_context,
            ),
            patch.object(self.client, "_throttle_request") as throttle,
        ):
            result = self.client._request("GET", "/networks")

        self.assertEqual(result, response)
        throttle.assert_called_once_with()
        client.request.assert_called_once()

    def test_request_throttles_retried_forward_http_attempts(self):
        self.client.retries = 1
        response = Mock()
        first_client = Mock()
        first_client.request.side_effect = httpx.RemoteProtocolError(
            "Server disconnected without sending a response."
        )
        second_client = Mock()
        second_client.request.return_value = response
        first_context = Mock()
        first_context.__enter__ = Mock(return_value=first_client)
        first_context.__exit__ = Mock(return_value=None)
        second_context = Mock()
        second_context.__enter__ = Mock(return_value=second_client)
        second_context.__exit__ = Mock(return_value=None)

        with (
            patch(
                "forward_netbox.utilities.forward_api_impl.httpx.Client",
                side_effect=[first_context, second_context],
            ),
            patch.object(self.client, "_throttle_request") as throttle,
            patch("forward_netbox.utilities.forward_api_impl.time.sleep"),
        ):
            result = self.client._request("GET", "/networks")

        self.assertEqual(result, response)
        self.assertEqual(throttle.call_count, 2)

    def test_request_uses_netbox_proxy_routers(self):
        response = Mock()
        client = Mock()
        client.request.return_value = response
        client_context = Mock()
        client_context.__enter__ = Mock(return_value=client)
        client_context.__exit__ = Mock(return_value=None)

        with (
            patch(
                "forward_netbox.utilities.forward_api_impl.resolve_proxies",
                return_value={
                    "http": None,
                    "https": "http://proxy.example.com:3128",
                },
            ) as resolve_proxies,
            patch(
                "forward_netbox.utilities.forward_api_impl.httpx.HTTPTransport",
                side_effect=lambda proxy: f"transport:{proxy}",
            ),
            patch(
                "forward_netbox.utilities.forward_api_impl.httpx.Client",
                return_value=client_context,
            ) as http_client,
        ):
            result = self.client._request("GET", "/networks")

        self.assertEqual(result, response)
        resolve_proxies.assert_called_once_with(
            url="https://fwd.app/api/networks",
            context={
                "client": self.client,
                "source": self.client.source,
            },
        )
        http_client.assert_called_once_with(
            timeout=1200,
            verify=True,
            mounts={
                "https://": "transport:http://proxy.example.com:3128",
            },
        )
        client.request.assert_called_once_with(
            "GET",
            "https://fwd.app/api/networks",
            params=None,
            json=None,
            headers=self.client._headers(),
            auth=("user@example.com", "secret"),
        )
        response.raise_for_status.assert_called_once()

    def test_request_retries_transient_transport_errors(self):
        response = Mock()
        first_client = Mock()
        first_client.request.side_effect = httpx.RemoteProtocolError(
            "Server disconnected without sending a response."
        )
        second_client = Mock()
        second_client.request.return_value = response
        first_context = Mock()
        first_context.__enter__ = Mock(return_value=first_client)
        first_context.__exit__ = Mock(return_value=None)
        second_context = Mock()
        second_context.__enter__ = Mock(return_value=second_client)
        second_context.__exit__ = Mock(return_value=None)

        with (
            patch(
                "forward_netbox.utilities.forward_api_impl.httpx.Client",
                side_effect=[first_context, second_context],
            ),
            patch("forward_netbox.utilities.forward_api_impl.time.sleep") as sleep,
        ):
            result = self.client._request("POST", "/nqe", json_body={"query": "q"})

        self.assertEqual(result, response)
        sleep.assert_any_call(2)
        response.raise_for_status.assert_called_once()

    def test_request_retries_transient_http_status_errors(self):
        request = httpx.Request("POST", "https://fwd.app/api/nqe")
        response_504 = httpx.Response(
            504,
            request=request,
            text="gateway timeout",
        )
        response_ok = Mock()
        first_client = Mock()
        first_client.request.return_value = response_504
        second_client = Mock()
        second_client.request.return_value = response_ok
        first_context = Mock()
        first_context.__enter__ = Mock(return_value=first_client)
        first_context.__exit__ = Mock(return_value=None)
        second_context = Mock()
        second_context.__enter__ = Mock(return_value=second_client)
        second_context.__exit__ = Mock(return_value=None)

        with (
            patch(
                "forward_netbox.utilities.forward_api_impl.httpx.Client",
                side_effect=[first_context, second_context],
            ),
            patch("forward_netbox.utilities.forward_api_impl.time.sleep") as sleep,
        ):
            result = self.client._request("POST", "/nqe", json_body={"query": "q"})

        self.assertEqual(result, response_ok)
        sleep.assert_any_call(2)
        response_ok.raise_for_status.assert_called_once()

    def test_api_usage_summary_counts_http_attempts_retries_and_429s(self):
        self.client.retries = 1
        request = httpx.Request("POST", "https://fwd.app/api/nqe")
        response_429 = httpx.Response(
            429,
            request=request,
            text="too many requests",
        )
        response_ok = httpx.Response(
            200,
            request=request,
            json={"ok": True},
        )
        first_client = Mock()
        first_client.request.return_value = response_429
        second_client = Mock()
        second_client.request.return_value = response_ok
        first_context = Mock()
        first_context.__enter__ = Mock(return_value=first_client)
        first_context.__exit__ = Mock(return_value=None)
        second_context = Mock()
        second_context.__enter__ = Mock(return_value=second_client)
        second_context.__exit__ = Mock(return_value=None)

        with (
            patch(
                "forward_netbox.utilities.forward_api_impl.httpx.Client",
                side_effect=[first_context, second_context],
            ),
            patch.object(self.client, "_throttle_request"),
            patch(
                "forward_netbox.utilities.forward_api_impl.time.monotonic",
                side_effect=[100.0, 100.5],
            ),
            patch("forward_netbox.utilities.forward_api_impl.time.sleep"),
        ):
            result = self.client._request("POST", "/nqe", json_body={"query": "q"})

        self.assertEqual(result, response_ok)
        self.assertEqual(
            self.client.api_usage_summary(),
            {
                "api_requests_per_minute": 1800,
                "http_attempts": 2,
                "http_successes": 1,
                "http_failures": 1,
                "http_timeout_failures": 0,
                "http_transport_failures": 0,
                "http_status_failures": 1,
                "http_transient_status_failures": 1,
                "http_nontransient_status_failures": 0,
                "http_429_failures": 1,
                "http_retries": 1,
                "http_status_classes": {"2xx": 1, "4xx": 1},
                "throttle_sleep_seconds": 0.0,
                "usage_window_seconds": 0.5,
                "observed_http_attempts_per_minute": 120.0,
                "nqe_query_calls": 0,
                "nqe_diff_calls": 0,
                "nqe_pages": 0,
                "nqe_query_pages": 0,
                "nqe_diff_pages": 0,
                "nqe_async_query_calls": 0,
                "nqe_async_trigger_calls": 0,
                "nqe_async_status_calls": 0,
                "nqe_async_result_calls": 0,
                "read_cache_hits": 0,
                "read_cache_misses": 0,
                "read_cache_hit_rate": None,
            },
        )

    def test_reset_api_usage_summary_preserves_rate_limit_configuration(self):
        self.client._record_api_usage("http_attempts")
        self.client._record_api_usage("nqe_pages")

        self.client.reset_api_usage_summary()

        self.assertEqual(
            self.client.api_usage_summary(),
            {
                "api_requests_per_minute": 1800,
                "http_attempts": 0,
                "http_successes": 0,
                "http_failures": 0,
                "http_timeout_failures": 0,
                "http_transport_failures": 0,
                "http_status_failures": 0,
                "http_transient_status_failures": 0,
                "http_nontransient_status_failures": 0,
                "http_429_failures": 0,
                "http_retries": 0,
                "http_status_classes": {},
                "throttle_sleep_seconds": 0.0,
                "usage_window_seconds": 0.0,
                "observed_http_attempts_per_minute": None,
                "nqe_query_calls": 0,
                "nqe_diff_calls": 0,
                "nqe_pages": 0,
                "nqe_query_pages": 0,
                "nqe_diff_pages": 0,
                "nqe_async_query_calls": 0,
                "nqe_async_trigger_calls": 0,
                "nqe_async_status_calls": 0,
                "nqe_async_result_calls": 0,
                "read_cache_hits": 0,
                "read_cache_misses": 0,
                "read_cache_hit_rate": None,
            },
        )

    def test_api_usage_summary_reports_observed_http_attempt_rate(self):
        with patch(
            "forward_netbox.utilities.forward_api_impl.time.monotonic",
            side_effect=[100.0, 102.0, 104.0],
        ):
            self.client._record_http_attempt_usage()
            self.client._record_http_attempt_usage()
            self.client._record_http_attempt_usage()

        summary = self.client.api_usage_summary()

        self.assertEqual(summary["http_attempts"], 3)
        self.assertEqual(summary["usage_window_seconds"], 4.0)
        self.assertEqual(summary["observed_http_attempts_per_minute"], 30.0)

    def test_request_raises_connectivity_error_after_transient_http_status_retries(
        self,
    ):
        self.client.retries = 1
        request = httpx.Request("POST", "https://fwd.app/api/nqe")
        response_504 = httpx.Response(
            504,
            request=request,
            text="gateway timeout",
        )
        client = Mock()
        client.request.return_value = response_504
        client_context = Mock()
        client_context.__enter__ = Mock(return_value=client)
        client_context.__exit__ = Mock(return_value=None)

        with (
            patch(
                "forward_netbox.utilities.forward_api_impl.httpx.Client",
                side_effect=[client_context, client_context],
            ),
            patch("forward_netbox.utilities.forward_api_impl.time.sleep"),
            self.assertRaisesRegex(
                ForwardConnectivityError,
                "transient HTTP 504",
            ),
        ):
            self.client._request("POST", "/nqe", json_body={"query": "q"})

        self.assertEqual(client.request.call_count, 2)

    def test_request_raises_after_retry_exhaustion(self):
        self.client.retries = 1
        api_error = httpx.RemoteProtocolError(
            "Server disconnected without sending a response."
        )
        client = Mock()
        client.request.side_effect = api_error
        client_context = Mock()
        client_context.__enter__ = Mock(return_value=client)
        client_context.__exit__ = Mock(return_value=None)

        with (
            patch(
                "forward_netbox.utilities.forward_api_impl.httpx.Client",
                side_effect=[client_context, client_context],
            ),
            patch("forward_netbox.utilities.forward_api_impl.time.sleep"),
            self.assertRaisesRegex(
                ForwardConnectivityError,
                "Could not connect to Forward API endpoint",
            ),
        ):
            self.client._request("POST", "/nqe", json_body={"query": "q"})

        self.assertEqual(client.request.call_count, 2)

    def test_run_nqe_query_returns_single_page_by_default(self):
        self.client._request = Mock(
            side_effect=[
                self._response(
                    {"executionKey": "X_123", "status": "COMPLETED", "outcome": "OK"}
                ),
                self._response(
                    {
                        "items": [
                            {"fields": {"n": 1}},
                            {"fields": {"n": 2}},
                        ],
                        "totalNumItems": 5,
                    }
                ),
            ]
        )

        rows = self.client.run_nqe_query(
            query="select {n: 1}",
            network_id="network-1",
            snapshot_id="snapshot-1",
        )

        self.assertEqual(rows, [{"n": 1}, {"n": 2}])
        self.assertEqual(self.client._request.call_count, 2)
        self.assertEqual(
            [call.args for call in self.client._request.call_args_list],
            [
                ("POST", "/networks/network-1/nqe-executions"),
                ("GET", "/networks/network-1/nqe-executions/X_123/result"),
            ],
        )
        self.assertEqual(
            self.client._request.call_args_list[0].kwargs["params"],
            {"snapshotId": "snapshot-1"},
        )
        self.assertEqual(
            self.client._request.call_args_list[0].kwargs["json_body"],
            {"query": "select {n: 1}", "parameters": {}},
        )

    def test_run_nqe_query_omits_abbreviated_hex_commit_id(self):
        self.client._request = Mock(
            side_effect=[
                self._response(
                    {"executionKey": "X_123", "status": "COMPLETED", "outcome": "OK"}
                ),
                self._response(
                    {
                        "items": [{"fields": {"n": 1}}],
                        "totalNumItems": 1,
                    }
                ),
            ]
        )

        rows = self.client.run_nqe_query(
            query_id="Q_devices",
            commit_id="1a2b",
            network_id="network-1",
            snapshot_id="snapshot-1",
        )

        self.assertEqual(rows, [{"n": 1}])
        json_body = self.client._request.call_args_list[0].kwargs["json_body"]
        self.assertEqual(json_body["queryId"], "Q_devices")
        self.assertNotIn("commitId", json_body)

    def test_run_nqe_query_omits_head_commit_id(self):
        self.client._request = Mock(
            side_effect=[
                self._response(
                    {"executionKey": "X_123", "status": "COMPLETED", "outcome": "OK"}
                ),
                self._response(
                    {
                        "items": [{"fields": {"n": 1}}],
                        "totalNumItems": 1,
                    }
                ),
            ]
        )

        rows = self.client.run_nqe_query(
            query_id="Q_devices",
            commit_id="head",
            network_id="network-1",
            snapshot_id="snapshot-1",
        )

        self.assertEqual(rows, [{"n": 1}])
        json_body = self.client._request.call_args_list[0].kwargs["json_body"]
        self.assertEqual(json_body["queryId"], "Q_devices")
        self.assertNotIn("commitId", json_body)

    def test_run_nqe_query_async_polls_and_fetches_result(self):
        client = ForwardClient(
            SimpleNamespace(
                url="https://fwd.app",
                parameters={
                    "username": "user@example.com",
                    "password": "secret",
                    "nqe_async_poll_interval_seconds": 0,
                },
            )
        )
        client._request = Mock(
            side_effect=[
                self._response({"executionKey": "X_123", "status": "SUBMITTED"}),
                self._response({"status": "EXECUTING", "rowsProduced": 0}),
                self._response({"status": "COMPLETED", "outcome": "OK"}),
                self._response(
                    {
                        "items": [
                            {"fields": {"n": 1}},
                            {"fields": {"n": 2}},
                        ],
                        "totalNumItems": 2,
                    }
                ),
            ]
        )

        rows = client.run_nqe_query(
            query_id="Q_devices",
            commit_id="commit-1",
            network_id="network-1",
            snapshot_id="snapshot-1",
            parameters={"forward_netbox_shard_keys": ["device-1"]},
            column_filters=[
                {
                    "columnName": "name",
                    "operator": "EQUALS_ANY",
                    "values": ["device-1"],
                }
            ],
        )

        self.assertEqual(rows, [{"n": 1}, {"n": 2}])
        self.assertEqual(
            [call.args for call in client._request.call_args_list],
            [
                ("POST", "/networks/network-1/nqe-executions"),
                ("GET", "/networks/network-1/nqe-executions/X_123"),
                ("GET", "/networks/network-1/nqe-executions/X_123"),
                ("GET", "/networks/network-1/nqe-executions/X_123/result"),
            ],
        )
        trigger = client._request.call_args_list[0]
        self.assertEqual(trigger.kwargs["params"], {"snapshotId": "snapshot-1"})
        self.assertEqual(
            trigger.kwargs["json_body"],
            {
                "queryId": "Q_devices",
                "commitId": "commit-1",
                "parameters": {"forward_netbox_shard_keys": ["device-1"]},
                "columnFilters": [
                    {
                        "columnName": "name",
                        "operator": "EQUALS_ANY",
                        "values": ["device-1"],
                    }
                ],
            },
        )
        result = client._request.call_args_list[-1]
        self.assertEqual(result.kwargs["params"], {"offset": 0, "limit": 10000})
        summary = client.api_usage_summary()
        self.assertEqual(summary["nqe_query_calls"], 1)
        self.assertEqual(summary["nqe_async_query_calls"], 1)
        self.assertEqual(summary["nqe_async_trigger_calls"], 1)
        self.assertEqual(summary["nqe_async_status_calls"], 2)
        self.assertEqual(summary["nqe_async_result_calls"], 1)
        self.assertEqual(summary["nqe_pages"], 1)

    def test_run_nqe_query_async_prefers_ndjson_for_results(self):
        client = ForwardClient(
            SimpleNamespace(
                url="https://fwd.app",
                parameters={
                    "username": "user@example.com",
                    "password": "secret",
                    "nqe_async_poll_interval_seconds": 0,
                },
            )
        )
        result_response = self._response(None)
        result_response.headers = {"content-type": "application/x-ndjson"}
        result_response.text = '{"n": 1}\n{"n": 2}\n'
        client._request = Mock(
            side_effect=[
                self._response(
                    {"executionKey": "X_123", "status": "COMPLETED", "outcome": "OK"}
                ),
                result_response,
            ]
        )

        rows = client.run_nqe_query(
            query_id="Q_devices",
            network_id="network-1",
            snapshot_id="snapshot-1",
        )

        self.assertEqual(rows, [{"n": 1}, {"n": 2}])
        self.assertEqual(
            client._request.call_args_list[-1].kwargs["headers"]["Accept"],
            "application/x-ndjson, application/jsonl;q=0.9, application/json;q=0.1",
        )

    def test_parse_nqe_async_result_falls_back_to_json(self):
        client = ForwardClient(
            SimpleNamespace(
                url="https://fwd.app",
                parameters={
                    "username": "user@example.com",
                    "password": "secret",
                },
            )
        )
        response = self._response(
            {
                "items": [
                    {"fields": {"n": 1}},
                    {"fields": {"n": 2}},
                ],
                "totalNumItems": 2,
            }
        )
        response.headers = {"content-type": "application/json"}

        rows, total = client._parse_nqe_async_result(response)

        self.assertEqual(rows, [{"n": 1}, {"n": 2}])
        self.assertEqual(total, 2)

    def test_run_nqe_query_async_fetch_all_pages_single_execution(self):
        client = ForwardClient(
            SimpleNamespace(
                url="https://fwd.app",
                parameters={
                    "username": "user@example.com",
                    "password": "secret",
                    "nqe_async_poll_interval_seconds": 0,
                },
            )
        )
        client._request = Mock(
            side_effect=[
                self._response(
                    {"executionKey": "X_123", "status": "COMPLETED", "outcome": "OK"}
                ),
                self._response(
                    {
                        "items": [
                            {"fields": {"n": 1}},
                            {"fields": {"n": 2}},
                        ],
                        "totalNumItems": 3,
                    }
                ),
                self._response(
                    {
                        "items": [{"fields": {"n": 3}}],
                        "totalNumItems": 3,
                    }
                ),
            ]
        )

        rows = client.run_nqe_query(
            query="foreach d in network.devices select { n: 1 }",
            network_id="network-1",
            snapshot_id="snapshot-1",
            limit=2,
            fetch_all=True,
        )

        self.assertEqual(rows, [{"n": 1}, {"n": 2}, {"n": 3}])
        self.assertEqual(client._request.call_count, 3)
        self.assertEqual(
            [
                call.kwargs.get("params")
                for call in client._request.call_args_list
                if call.args[1].endswith("/result")
            ],
            [{"offset": 0, "limit": 2}, {"offset": 2, "limit": 2}],
        )
        self.assertEqual(client.api_usage_summary()["nqe_async_trigger_calls"], 1)
        self.assertEqual(client.api_usage_summary()["nqe_async_result_calls"], 2)

    def test_run_nqe_query_async_raises_on_non_ok_outcome(self):
        client = ForwardClient(
            SimpleNamespace(
                url="https://fwd.app",
                parameters={
                    "username": "user@example.com",
                    "password": "secret",
                    "nqe_async_poll_interval_seconds": 0,
                },
            )
        )
        client._request = Mock(
            return_value=self._response(
                {
                    "executionKey": "X_123",
                    "status": "COMPLETED",
                    "outcome": "USER_ERROR",
                    "error": {"message": "bad query"},
                }
            )
        )

        with self.assertRaisesRegex(
            ForwardClientError,
            "completed with outcome `USER_ERROR`: bad query",
        ):
            client.run_nqe_query(
                query_id="Q_devices",
                network_id="network-1",
                snapshot_id="snapshot-1",
            )
        self.assertEqual(client._request.call_count, 1)

    def test_run_nqe_query_async_raises_when_poll_limit_exceeded(self):
        client = ForwardClient(
            SimpleNamespace(
                url="https://fwd.app",
                parameters={
                    "username": "user@example.com",
                    "password": "secret",
                    "nqe_async_poll_interval_seconds": 0,
                    "nqe_async_max_polls": 2,
                },
            )
        )
        client._request = Mock(
            side_effect=[
                self._response({"executionKey": "X_123", "status": "SUBMITTED"}),
                self._response({"status": "EXECUTING"}),
                self._response({"status": "EXECUTING"}),
            ]
        )

        with self.assertRaisesRegex(
            ForwardClientError,
            "did not complete after 2 status poll",
        ):
            client.run_nqe_query(
                query_id="Q_devices",
                network_id="network-1",
                snapshot_id="snapshot-1",
            )
        self.assertEqual(client._request.call_count, 3)

    def test_run_nqe_query_async_poll_uses_exponential_backoff(self):
        client = ForwardClient(
            SimpleNamespace(
                url="https://fwd.app",
                parameters={
                    "username": "user@example.com",
                    "password": "secret",
                    "nqe_async_poll_interval_seconds": 1.0,
                    "nqe_async_max_polls": 10,
                },
            )
        )
        client._request = Mock(
            side_effect=[
                self._response({"executionKey": "X_1", "status": "SUBMITTED"}),
                self._response({"status": "EXECUTING"}),
                self._response({"status": "EXECUTING"}),
                self._response({"status": "EXECUTING"}),
                self._response({"status": "COMPLETED", "outcome": "OK"}),
                self._response({"items": [], "totalNumItems": 0}),
            ]
        )

        with patch("forward_netbox.utilities.forward_api_impl.time.sleep") as mock_sleep:
            client.run_nqe_query(
                query_id="Q_devices",
                network_id="network-1",
                snapshot_id="snapshot-1",
            )

        sleep_args = [call.args[0] for call in mock_sleep.call_args_list]
        # Backoff: poll_index 0→0.1, 1→0.2, 2→0.4, 3→0.8 (all < 1.0 ceiling)
        self.assertEqual(len(sleep_args), 4)
        self.assertAlmostEqual(sleep_args[0], 0.1)
        self.assertAlmostEqual(sleep_args[1], 0.2)
        self.assertAlmostEqual(sleep_args[2], 0.4)
        self.assertAlmostEqual(sleep_args[3], 0.8)

    def test_run_nqe_query_async_poll_backoff_caps_at_ceiling(self):
        client = ForwardClient(
            SimpleNamespace(
                url="https://fwd.app",
                parameters={
                    "username": "user@example.com",
                    "password": "secret",
                    "nqe_async_poll_interval_seconds": 0.5,
                    "nqe_async_max_polls": 10,
                },
            )
        )
        client._request = Mock(
            side_effect=[
                self._response({"executionKey": "X_1", "status": "SUBMITTED"}),
                self._response({"status": "EXECUTING"}),
                self._response({"status": "EXECUTING"}),
                self._response({"status": "EXECUTING"}),
                self._response({"status": "EXECUTING"}),
                self._response({"status": "COMPLETED", "outcome": "OK"}),
                self._response({"items": [], "totalNumItems": 0}),
            ]
        )

        with patch("forward_netbox.utilities.forward_api_impl.time.sleep") as mock_sleep:
            client.run_nqe_query(
                query_id="Q_devices",
                network_id="network-1",
                snapshot_id="snapshot-1",
            )

        sleep_args = [call.args[0] for call in mock_sleep.call_args_list]
        # poll 0→0.1, 1→0.2, 2→0.4, 3→capped at 0.5, 4→capped at 0.5
        self.assertEqual(len(sleep_args), 5)
        self.assertAlmostEqual(sleep_args[0], 0.1)
        self.assertAlmostEqual(sleep_args[1], 0.2)
        self.assertAlmostEqual(sleep_args[2], 0.4)
        self.assertAlmostEqual(sleep_args[3], 0.5)
        self.assertAlmostEqual(sleep_args[4], 0.5)

    def test_run_nqe_query_async_requires_snapshot_id(self):
        client = ForwardClient(
            SimpleNamespace(
                url="https://fwd.app",
                parameters={
                    "username": "user@example.com",
                    "password": "secret",
                    "nqe_async_poll_interval_seconds": 0,
                },
            )
        )
        with self.assertRaisesRegex(
            ForwardClientError,
            "Async NQE requires both `network_id` and `snapshot_id`.",
        ):
            client.run_nqe_query(query_id="Q_devices", network_id="network-1")

    def test_run_nqe_query_async_requires_json_item_format(self):
        client = ForwardClient(
            SimpleNamespace(
                url="https://fwd.app",
                parameters={
                    "username": "user@example.com",
                    "password": "secret",
                    "nqe_async_poll_interval_seconds": 0,
                },
            )
        )

        with self.assertRaisesRegex(
            ForwardClientError,
            "Async NQE only supports JSON item format.",
        ):
            client.run_nqe_query(
                query_id="Q_devices",
                network_id="network-1",
                snapshot_id="snapshot-1",
                item_format="CSV",
            )

    def test_run_nqe_query_fetch_all_pages_until_total_num_items(self):
        self.client._request = Mock(
            side_effect=[
                self._response(
                    {"executionKey": "X_123", "status": "COMPLETED", "outcome": "OK"}
                ),
                self._response(
                    {
                        "items": [
                            {"fields": {"n": 1}},
                            {"fields": {"n": 2}},
                        ],
                        "totalNumItems": 5,
                    }
                ),
                self._response(
                    {
                        "items": [
                            {"fields": {"n": 3}},
                            {"fields": {"n": 4}},
                        ],
                        "totalNumItems": 5,
                    }
                ),
                self._response(
                    {
                        "items": [
                            {"fields": {"n": 5}},
                        ],
                        "totalNumItems": 5,
                    }
                ),
            ]
        )

        rows = self.client.run_nqe_query(
            query="select {n: 1}",
            network_id="network-1",
            snapshot_id="snapshot-1",
            limit=2,
            fetch_all=True,
        )

        self.assertEqual(
            rows,
            [
                {"n": 1},
                {"n": 2},
                {"n": 3},
                {"n": 4},
                {"n": 5},
            ],
        )
        self.assertEqual(self.client._request.call_count, 4)
        self.assertEqual(
            [
                call.kwargs["params"]["offset"]
                for call in self.client._request.call_args_list
                if call.args[1].endswith("/result")
            ],
            [0, 2, 4],
        )
        self.assertEqual(self.client.api_usage_summary()["nqe_query_calls"], 1)
        self.assertEqual(self.client.api_usage_summary()["nqe_pages"], 3)
        self.assertEqual(self.client.api_usage_summary()["nqe_query_pages"], 3)
        self.assertEqual(self.client.api_usage_summary()["nqe_async_query_calls"], 1)
        self.assertEqual(self.client.api_usage_summary()["nqe_async_trigger_calls"], 1)
        self.assertEqual(self.client.api_usage_summary()["nqe_async_result_calls"], 3)

    def test_run_nqe_query_fetch_all_without_total_num_items_stops_on_short_page(self):
        self.client._request = Mock(
            side_effect=[
                self._response(
                    {"executionKey": "X_123", "status": "COMPLETED", "outcome": "OK"}
                ),
                self._response(
                    {
                        "items": [
                            {"fields": {"n": 1}},
                            {"fields": {"n": 2}},
                        ],
                    }
                ),
                self._response(
                    {
                        "items": [
                            {"fields": {"n": 3}},
                        ],
                    }
                ),
            ]
        )

        rows = self.client.run_nqe_query(
            query="select {n: 1}",
            network_id="network-1",
            snapshot_id="snapshot-1",
            limit=2,
            fetch_all=True,
        )

        self.assertEqual(rows, [{"n": 1}, {"n": 2}, {"n": 3}])
        self.assertEqual(self.client._request.call_count, 3)

    def test_run_nqe_query_fetch_all_raises_if_api_ends_early(self):
        self.client._request = Mock(
            side_effect=[
                self._response(
                    {"executionKey": "X_123", "status": "COMPLETED", "outcome": "OK"}
                ),
                self._response(
                    {
                        "items": [
                            {"fields": {"n": 1}},
                            {"fields": {"n": 2}},
                        ],
                        "totalNumItems": 5,
                    }
                ),
                self._response(
                    {
                        "items": [],
                        "totalNumItems": 5,
                    }
                ),
            ]
        )

        with self.assertRaisesRegex(
            ForwardClientError,
            "Forward async NQE result pagination ended early: fetched 2 rows but API reported 5.",
        ):
            self.client.run_nqe_query(
                query="select {n: 1}",
                network_id="network-1",
                snapshot_id="snapshot-1",
                limit=2,
                fetch_all=True,
            )

    def test_run_nqe_query_fetch_all_preserves_column_filters(self):
        self.client._request = Mock(
            side_effect=[
                self._response(
                    {"executionKey": "X_123", "status": "COMPLETED", "outcome": "OK"}
                ),
                self._response(
                    {
                        "items": [
                            {"fields": {"n": 1}},
                        ],
                        "totalNumItems": 2,
                    }
                ),
                self._response(
                    {
                        "items": [
                            {"fields": {"n": 2}},
                        ],
                        "totalNumItems": 2,
                    }
                ),
            ]
        )

        rows = self.client.run_nqe_query(
            query_id="Q_devices",
            column_filters=[{"column": "name", "operator": "contains", "value": "sw"}],
            network_id="network-1",
            snapshot_id="snapshot-1",
            limit=1,
            fetch_all=True,
        )

        self.assertEqual(rows, [{"n": 1}, {"n": 2}])
        self.assertEqual(self.client._request.call_count, 3)
        self.assertEqual(
            self.client._request.call_args_list[0].kwargs["json_body"]["queryId"],
            "Q_devices",
        )
        self.assertEqual(
            self.client._request.call_args_list[0].kwargs["json_body"]["columnFilters"],
            [{"column": "name", "operator": "contains", "value": "sw"}],
        )
        self.assertEqual(
            [call.kwargs["params"] for call in self.client._request.call_args_list[1:]],
            [{"offset": 0, "limit": 1}, {"offset": 1, "limit": 1}],
        )

    def test_run_nqe_query_fetch_all_raises_when_page_limit_exceeded(self):
        self.client.nqe_fetch_all_max_pages = 2
        self.client._request = Mock(
            side_effect=[
                self._response(
                    {"executionKey": "X_123", "status": "COMPLETED", "outcome": "OK"}
                ),
                self._response(
                    {
                        "items": [
                            {"fields": {"n": 1}},
                        ],
                    }
                ),
                self._response(
                    {
                        "items": [
                            {"fields": {"n": 2}},
                        ],
                    }
                ),
            ]
        )

        with self.assertRaisesRegex(
            ForwardClientError,
            "Forward async NQE result pagination exceeded 2 page\\(s\\)",
        ):
            self.client.run_nqe_query(
                query_id="Q_devices",
                network_id="network-1",
                snapshot_id="snapshot-1",
                limit=1,
                fetch_all=True,
            )
        self.assertEqual(self.client._request.call_count, 3)

    def test_run_nqe_query_fetch_all_raises_on_identical_full_pages(self):
        self.client.nqe_fetch_all_max_pages = 10
        self.client.nqe_identical_full_page_streak_limit = 2
        self.client._request = Mock(
            side_effect=[
                self._response(
                    {"executionKey": "X_123", "status": "COMPLETED", "outcome": "OK"}
                ),
                self._response(
                    {
                        "items": [
                            {"fields": {"n": 1}},
                            {"fields": {"n": 2}},
                        ],
                    }
                ),
                self._response(
                    {
                        "items": [
                            {"fields": {"n": 1}},
                            {"fields": {"n": 2}},
                        ],
                    }
                ),
                self._response(
                    {
                        "items": [
                            {"fields": {"n": 1}},
                            {"fields": {"n": 2}},
                        ],
                    }
                ),
            ]
        )

        with self.assertRaisesRegex(
            ForwardClientError,
            "Forward async NQE result pagination did not advance",
        ):
            self.client.run_nqe_query(
                query_id="Q_devices",
                network_id="network-1",
                snapshot_id="snapshot-1",
                limit=2,
                fetch_all=True,
            )
        self.assertEqual(self.client._request.call_count, 4)

    def test_snapshot_reads_are_cached_per_client(self):
        self.client._request = Mock(
            side_effect=[
                self._response(
                    {
                        "snapshots": [
                            {
                                "id": "snapshot-1",
                                "state": "processed",
                                "createdAt": "2026-06-01T00:00:00Z",
                                "processedAt": "2026-06-01T01:00:00Z",
                            }
                        ]
                    }
                ),
                self._response({"totalCount": 5}),
                self._response(
                    {
                        "id": "snapshot-1",
                        "state": "processed",
                        "createdAt": "2026-06-01T00:00:00Z",
                        "processedAt": "2026-06-01T01:00:00Z",
                    }
                ),
            ]
        )

        snapshots_first = self.client.get_snapshots("network-1")
        snapshots_second = self.client.get_snapshots("network-1")
        metrics_first = self.client.get_snapshot_metrics("snapshot-1")
        metrics_second = self.client.get_snapshot_metrics("snapshot-1")
        latest_first = self.client.get_latest_processed_snapshot("network-1")
        latest_second = self.client.get_latest_processed_snapshot("network-1")

        self.assertEqual(self.client._request.call_count, 3)
        self.assertEqual(snapshots_first, snapshots_second)
        self.assertEqual(metrics_first, metrics_second)
        self.assertEqual(latest_first, latest_second)
        self.assertEqual(snapshots_first[0]["id"], "snapshot-1")
        self.assertEqual(metrics_first, {"totalCount": 5})
        self.assertEqual(latest_first["id"], "snapshot-1")

    def test_get_org_nqe_queries_normalizes_directory(self):
        shared_cache = FakeSharedCache()
        self.client._request = Mock(
            return_value=self._response(
                [
                    {
                        "queryId": "Q_devices",
                        "path": "/forward_netbox_validation/forward_devices",
                        "intent": "Forward Devices",
                    }
                ]
            )
        )

        with patch(
            "forward_netbox.utilities.forward_api_impl._shared_read_cache",
            return_value=shared_cache,
        ):
            rows = self.client.get_org_nqe_queries(
                directory="/forward_netbox_validation"
            )

        self.assertEqual(rows[0]["queryId"], "Q_devices")
        self.client._request.assert_called_once_with(
            "GET",
            "/nqe/queries",
            params={"dir": "/forward_netbox_validation/"},
        )

    def test_nqe_query_lists_are_cached_per_client(self):
        shared_cache = FakeSharedCache()
        self.client._request = Mock(
            side_effect=[
                self._response(
                    [
                        {
                            "queryId": "Q_devices",
                            "path": "/forward_netbox_validation/forward_devices",
                            "intent": "Forward Devices",
                        }
                    ]
                ),
                self._response(
                    {
                        "queries": [
                            {
                                "queryId": "FQ_devices",
                                "path": "/netbox/forward_devices",
                                "lastCommitId": "commit-1",
                            }
                        ]
                    }
                ),
            ]
        )

        with patch(
            "forward_netbox.utilities.forward_api_impl._shared_read_cache",
            return_value=shared_cache,
        ):
            org_first = self.client.get_org_nqe_queries(
                directory="/forward_netbox_validation"
            )
            org_second = self.client.get_org_nqe_queries(
                directory="/forward_netbox_validation"
            )
            repo_first = self.client.get_nqe_repository_queries(
                repository="fwd",
                directory="/netbox",
            )
            repo_second = self.client.get_nqe_repository_queries(
                repository="fwd",
                directory="/netbox",
            )

        self.assertEqual(org_first, org_second)
        self.assertEqual(repo_first, repo_second)
        self.assertEqual(org_first[0]["queryId"], "Q_devices")
        self.assertEqual(repo_first[0]["queryId"], "FQ_devices")

    def test_nqe_repository_query_index_is_cached_per_client(self):
        shared_cache = FakeSharedCache()
        self.client._request = Mock(
            return_value=self._response(
                {
                    "queries": [
                        {
                            "queryId": "Q_devices",
                            "path": "/netbox/forward_devices",
                            "lastCommitId": "commit-1",
                        }
                    ]
                }
            )
        )

        with patch(
            "forward_netbox.utilities.forward_api_impl._shared_read_cache",
            return_value=shared_cache,
        ):
            first = self.client.get_nqe_repository_query_index(
                repository="fwd",
                directory="/netbox",
            )
            second = self.client.get_nqe_repository_query_index(
                repository="fwd",
                directory="/netbox",
            )

        self.assertEqual(self.client._request.call_count, 1)
        self.assertEqual(
            first["by_query_id"]["Q_devices"][0]["path"],
            second["by_query_id"]["Q_devices"][0]["path"],
        )
        self.assertEqual(
            first["by_path"]["/netbox/forward_devices"]["queryId"], "Q_devices"
        )

    def test_get_nqe_repository_queries_reads_forward_library(self):
        self.client._request = Mock(
            return_value=self._response(
                {
                    "queries": [
                        {
                            "queryId": "FQ_devices",
                            "path": "/netbox/forward_devices",
                            "lastCommitId": "commit-1",
                        },
                        {
                            "queryId": "FQ_other",
                            "path": "/other/query",
                            "lastCommitId": "commit-2",
                        },
                    ]
                }
            )
        )

        rows = self.client.get_nqe_repository_queries(
            repository="fwd",
            directory="/netbox",
        )

        self.assertEqual(
            rows,
            [
                {
                    "queryId": "FQ_devices",
                    "path": "/netbox/forward_devices",
                    "intent": "",
                    "repository": "fwd",
                    "lastCommitId": "commit-1",
                }
            ],
        )
        self.client._request.assert_called_once_with(
            "GET",
            "/nqe/repos/fwd/commits/head/queries",
        )

    def test_get_nqe_repository_queries_uses_org_query_list_without_fallback(self):
        self.client._request = Mock(
            return_value=self._response(
                [
                    {
                        "queryId": "Q_devices",
                        "path": "/forward_netbox_validation/forward_devices",
                        "intent": "Forward Devices",
                    }
                ]
            )
        )

        rows = self.client.get_nqe_repository_queries(
            repository="org",
            directory="/forward_netbox_validation",
        )

        self.assertEqual(
            rows,
            [
                {
                    "queryId": "Q_devices",
                    "path": "/forward_netbox_validation/forward_devices",
                    "intent": "Forward Devices",
                    "repository": "org",
                    "lastCommitId": "",
                }
            ],
        )
        self.client._request.assert_called_once_with(
            "GET",
            "/nqe/queries",
            params={"dir": "/forward_netbox_validation/"},
        )

    def test_get_nqe_query_history(self):
        self.client._request = Mock(
            return_value=self._response(
                {
                    "commits": [
                        {
                            "id": "commit-1",
                            "path": "/netbox/forward_devices",
                        }
                    ]
                }
            )
        )

        rows = self.client.get_nqe_query_history("FQ/devices")

        self.assertEqual(rows[0]["id"], "commit-1")
        self.client._request.assert_called_once_with(
            "GET",
            "/nqe/queries/FQ%2Fdevices/history",
        )

    def test_nqe_query_history_is_cached_per_client(self):
        shared_cache = FakeSharedCache()
        self.client._request = Mock(
            return_value=self._response(
                {
                    "commits": [
                        {
                            "id": "commit-1",
                            "path": "/netbox/forward_devices",
                        }
                    ]
                }
            )
        )

        with patch(
            "forward_netbox.utilities.forward_api_impl._shared_read_cache",
            return_value=shared_cache,
        ):
            first = self.client.get_nqe_query_history("FQ/devices")
            second = self.client.get_nqe_query_history("FQ/devices")

        self.assertEqual(first, second)
        self.assertEqual(first[0]["id"], "commit-1")

    def test_empty_nqe_list_reads_are_cached_per_client(self):
        shared_cache = FakeSharedCache()
        self.client._request = Mock(
            side_effect=[
                self._response([]),
                self._response({"commits": []}),
            ]
        )

        with patch(
            "forward_netbox.utilities.forward_api_impl._shared_read_cache",
            return_value=shared_cache,
        ):
            org_first = self.client.get_org_nqe_queries(directory="/empty")
            org_second = self.client.get_org_nqe_queries(directory="/empty")
            history_first = self.client.get_nqe_query_history("FQ/empty")
            history_second = self.client.get_nqe_query_history("FQ/empty")

        self.assertEqual(org_first, org_second)
        self.assertEqual(history_first, history_second)
        self.assertEqual(org_first, [])
        self.assertEqual(history_first, [])

    def test_get_committed_nqe_query_resolves_repository_path(self):
        self.client._request = Mock(
            return_value=self._response(
                {
                    "queryId": "Q_devices",
                    "path": "/netbox/forward_devices",
                    "lastCommit": {"id": "commit-1"},
                }
            )
        )

        query = self.client.get_committed_nqe_query(
            repository="org",
            query_path="netbox/forward_devices",
            commit_id="commit-1",
        )

        self.assertEqual(query["queryId"], "Q_devices")
        self.client._request.assert_called_once_with(
            "GET",
            "/nqe/repos/org/commits/commit-1/queries",
            params={"path": "/netbox/forward_devices", "with": "sourceCode"},
        )

    def test_get_committed_nqe_query_uses_repository_index_for_fwd_head(self):
        shared_cache = FakeSharedCache()
        self.client._request = Mock(
            return_value=self._response(
                {
                    "queries": [
                        {
                            "queryId": "FQ_devices",
                            "path": "/netbox/forward_devices",
                            "lastCommitId": "commit-1",
                            "intent": "Forward Devices",
                        }
                    ]
                }
            )
        )

        with patch(
            "forward_netbox.utilities.forward_api_impl._shared_read_cache",
            return_value=shared_cache,
        ):
            query = self.client.get_committed_nqe_query(
                repository="fwd",
                query_path="netbox/forward_devices",
                commit_id="head",
            )

        self.assertEqual(query["queryId"], "FQ_devices")
        self.assertEqual(query["lastCommitId"], "commit-1")
        self.assertEqual(query["intent"], "Forward Devices")
        self.assertEqual(self.client._request.call_count, 1)

    def test_get_committed_nqe_query_requests_source_for_head_when_requested(self):
        shared_cache = FakeSharedCache()
        self.client._request = Mock(
            side_effect=[
                self._response(
                    {
                        "queries": [
                            {
                                "queryId": "FQ_devices",
                                "path": "/netbox/forward_devices",
                                "lastCommitId": "commit-1",
                                "intent": "Forward Devices",
                                "sourceCode": "select {}",
                            }
                        ]
                    }
                )
            ]
        )

        with patch(
            "forward_netbox.utilities.forward_api_impl._shared_read_cache",
            return_value=shared_cache,
        ):
            query = self.client.get_committed_nqe_query(
                repository="fwd",
                query_path="netbox/forward_devices",
                commit_id="head",
                require_source_code=True,
                query_index={
                    "by_path": {
                        "/netbox/forward_devices": {
                            "queryId": "FQ_devices",
                            "path": "/netbox/forward_devices",
                            "lastCommitId": "commit-1",
                            "intent": "Forward Devices",
                        }
                    },
                    "by_query_id": {},
                    "rows": [],
                },
            )

        self.assertEqual(query["queryId"], "FQ_devices")
        self.assertEqual(query["sourceCode"], "select {}")
        self.assertEqual(self.client._request.call_count, 1)
        self.client._request.assert_called_once_with(
            "GET",
            "/nqe/repos/fwd/commits/commit-1/queries",
            params={"path": "/netbox/forward_devices", "with": "sourceCode"},
        )

    def test_get_committed_nqe_query_reuses_provided_query_index_on_miss(self):
        self.client._request = Mock(
            return_value=self._response(
                {
                    "queryId": "Q_devices",
                    "path": "/netbox/forward_devices",
                    "lastCommit": {"id": "commit-1"},
                }
            )
        )

        query = self.client.get_committed_nqe_query(
            repository="org",
            query_path="netbox/forward_devices",
            commit_id="head",
            query_index={"by_path": {}},
        )

        self.assertEqual(query["queryId"], "Q_devices")
        self.assertEqual(query["lastCommitId"], "commit-1")
        self.client._request.assert_called_once_with(
            "GET",
            "/nqe/repos/org/commits/head/queries",
            params={"path": "/netbox/forward_devices", "with": "sourceCode"},
        )

    def test_get_committed_nqe_query_uses_org_query_list_for_head(self):
        shared_cache = FakeSharedCache()
        self.client._request = Mock(
            return_value=self._response(
                [
                    {
                        "queryId": "Q_devices",
                        "path": "/netbox/forward_devices",
                        "lastCommitId": "commit-2",
                        "intent": "Forward Devices",
                    }
                ]
            )
        )

        with patch(
            "forward_netbox.utilities.forward_api_impl._shared_read_cache",
            return_value=shared_cache,
        ):
            query = self.client.get_committed_nqe_query(
                repository="org",
                query_path="netbox/forward_devices",
                commit_id="head",
            )

        self.assertEqual(query["queryId"], "Q_devices")
        self.assertEqual(query["lastCommitId"], "commit-2")
        self.assertEqual(query["intent"], "Forward Devices")
        self.assertEqual(self.client._request.call_count, 1)

    def test_get_committed_nqe_query_selects_matching_query_from_list_response(self):
        shared_cache = FakeSharedCache()
        self.client._request = Mock(
            return_value=self._response(
                {
                    "queries": [
                        {
                            "queryId": "Q_sites",
                            "path": "/netbox/forward_sites",
                            "lastCommitId": "commit-1",
                        },
                        {
                            "queryId": "Q_devices",
                            "path": "/netbox/forward_devices",
                            "lastCommitId": "commit-2",
                        },
                    ]
                }
            )
        )

        with patch(
            "forward_netbox.utilities.forward_api_impl._shared_read_cache",
            return_value=shared_cache,
        ):
            query = self.client.get_committed_nqe_query(
                repository="org",
                query_path="netbox/forward_devices",
                commit_id="head",
            )

        self.assertEqual(query["queryId"], "Q_devices")
        self.assertEqual(query["lastCommitId"], "commit-2")

    def test_resolve_nqe_query_reference_returns_query_id_and_commit(self):
        shared_cache = FakeSharedCache()
        self.client._request = Mock(
            return_value=self._response(
                [
                    {
                        "queryId": "Q_devices",
                        "path": "/netbox/forward_devices",
                        "lastCommitId": "commit-1",
                        "intent": "Forward Devices",
                    }
                ]
            )
        )

        with patch(
            "forward_netbox.utilities.forward_api_impl._shared_read_cache",
            return_value=shared_cache,
        ):
            resolved = self.client.resolve_nqe_query_reference(
                repository="org",
                query_path="/netbox/forward_devices",
            )

        self.assertEqual(
            resolved,
            {
                "queryId": "Q_devices",
                "commitId": "commit-1",
                "repository": "org",
                "path": "/netbox/forward_devices",
                "intent": "Forward Devices",
            },
        )

    def test_resolve_nqe_query_reference_falls_back_to_committed_lookup_when_index_missing(
        self,
    ):
        shared_cache = FakeSharedCache()
        self.client._request = Mock(
            side_effect=[
                self._response({"queries": []}),
                self._response(
                    {
                        "queries": [
                            {
                                "queryId": "Q_devices",
                                "path": "/netbox/forward_devices",
                                "lastCommitId": "commit-1",
                            }
                        ]
                    }
                ),
                self._response(
                    {
                        "queryId": "Q_devices",
                        "path": "/netbox/forward_devices",
                        "lastCommit": {"id": "commit-1"},
                    }
                ),
            ]
        )

        with patch(
            "forward_netbox.utilities.forward_api_impl._shared_read_cache",
            return_value=shared_cache,
        ):
            resolved = self.client.resolve_nqe_query_reference(
                repository="org",
                query_path="/netbox/forward_devices",
            )

        self.assertEqual(
            resolved,
            {
                "queryId": "Q_devices",
                "commitId": "commit-1",
                "repository": "org",
                "path": "/netbox/forward_devices",
                "intent": "",
            },
        )

    def test_committed_nqe_query_reads_are_cached_per_client(self):
        shared_cache = FakeSharedCache()
        self.client._request = Mock(
            return_value=self._response(
                {
                    "queries": [
                        {
                            "queryId": "Q_devices",
                            "path": "/netbox/forward_devices",
                            "lastCommitId": "commit-1",
                        }
                    ]
                }
            )
        )

        with patch(
            "forward_netbox.utilities.forward_api_impl._shared_read_cache",
            return_value=shared_cache,
        ):
            first = self.client.get_committed_nqe_query(
                repository="org",
                query_path="netbox/forward_devices",
                commit_id="commit-1",
            )
            second = self.client.get_committed_nqe_query(
                repository="org",
                query_path="netbox/forward_devices",
                commit_id="commit-1",
            )

        self.assertEqual(first["queryId"], "Q_devices")
        self.assertEqual(second["queryId"], "Q_devices")
        self.assertEqual(first["lastCommitId"], "commit-1")
        self.assertEqual(second["lastCommitId"], "commit-1")

    def test_resolve_nqe_query_reference_uses_cached_repository_index(self):
        shared_cache = FakeSharedCache()
        self.client._request = Mock(
            return_value=self._response(
                {
                    "queries": [
                        {
                            "queryId": "Q_devices",
                            "path": "/netbox/forward_devices",
                            "lastCommitId": "commit-2",
                        }
                    ]
                }
            )
        )

        with patch(
            "forward_netbox.utilities.forward_api_impl._shared_read_cache",
            return_value=shared_cache,
        ):
            first = self.client.resolve_nqe_query_reference(
                repository="org",
                query_path="/netbox/forward_devices",
            )
            second = self.client.resolve_nqe_query_reference(
                repository="org",
                query_path="/netbox/forward_devices",
            )

        self.assertEqual(first["queryId"], "Q_devices")
        self.assertEqual(second["queryId"], "Q_devices")
        self.assertEqual(second["commitId"], "commit-2")

    def test_add_org_nqe_query_creates_user_workspace_change(self):
        self.client._request = Mock(return_value=self._response({}))

        self.client.add_org_nqe_query(
            query_path="netbox/forward_devices",
            source_code="select {}",
        )

        self.client._request.assert_called_once_with(
            "POST",
            "/users/current/nqe/changes",
            params={"action": "addQuery", "path": "/netbox/forward_devices"},
            json_body={"sourceCode": "select {}"},
        )

    def test_edit_org_nqe_query_uses_existing_query_basis(self):
        self.client._request = Mock(return_value=self._response({}))

        self.client.edit_org_nqe_query(
            query_path="/netbox/forward_devices",
            source_code="select {}",
            query_id="OQ_devices",
            commit_id="commit-1",
        )

        self.client._request.assert_called_once_with(
            "POST",
            "/users/current/nqe/changes",
            params={"action": "editQuery", "path": "/netbox/forward_devices"},
            json_body={
                "sourceCode": "select {}",
                "basis": {
                    "queryId": "OQ_devices",
                    "commitId": "commit-1",
                },
            },
        )

    def test_commit_org_nqe_queries_commits_paths_and_returns_head(self):
        self.client._request = Mock(
            side_effect=[
                self._response({}),
                self._response("commit-2"),
            ]
        )

        commit_id = self.client.commit_org_nqe_queries(
            query_paths=["netbox/forward_devices"],
            message="Publish test queries",
        )

        self.assertEqual(commit_id, "commit-2")
        self.client._request.assert_any_call(
            "POST",
            "/nqe/repos/org/commits",
            json_body={
                "paths": ["/netbox/forward_devices"],
                "accessSettings": [],
                "message": {
                    "title": "Publish test queries",
                    "body": "",
                },
            },
        )
        self.client._request.assert_any_call("GET", "/nqe/repos/org/commits/head")

    def test_nqe_mutations_invalidate_cached_head_commit(self):
        shared_cache = FakeSharedCache()
        self.client._request = Mock(
            side_effect=[
                self._response({"id": "commit-1"}),
                self._response({}),
                self._response({"id": "commit-2"}),
            ]
        )

        with patch(
            "forward_netbox.utilities.forward_api_impl._shared_read_cache",
            return_value=shared_cache,
        ):
            first = self.client.get_org_nqe_head_commit_id()
            self.client.commit_org_nqe_queries(
                query_paths=["netbox/forward_devices"],
                message="Publish test queries",
            )
            second = self.client.get_org_nqe_head_commit_id()

        self.assertEqual(first, "commit-1")
        self.assertEqual(second, "commit-2")
        self.assertEqual(self.client._request.call_count, 3)

    def test_trigger_snapshot_reachability_posts_correct_url(self):
        self.client._request = Mock(
            side_effect=[
                self._response({"status": "COMPLETED"}),
            ]
        )
        result = self.client.trigger_snapshot_reachability(
            network_id="net-1", snapshot_id="snap-1"
        )
        self.assertEqual(result, {"status": "COMPLETED"})
        self.client._request.assert_called_once_with(
            "POST",
            "/networks/net-1/snapshots/snap-1/reachability",
        )

    def test_trigger_snapshot_reachability_polls_until_complete(self):
        self.client._request = Mock(
            side_effect=[
                self._response({"jobKey": "job-abc", "status": "SUBMITTED"}),
                self._response({"status": "RUNNING"}),
                self._response({"status": "COMPLETED"}),
            ]
        )
        with patch("forward_netbox.utilities.forward_api_impl.time.sleep"):
            result = self.client.trigger_snapshot_reachability(
                network_id="net-1", snapshot_id="snap-1"
            )
        self.assertEqual(result, {"status": "COMPLETED"})
        self.assertEqual(self.client._request.call_count, 3)

    def test_trigger_snapshot_reachability_raises_on_failure(self):
        self.client._request = Mock(
            side_effect=[
                self._response({"jobKey": "job-abc", "status": "SUBMITTED"}),
                self._response({"status": "FAILED", "error": "timeout"}),
            ]
        )
        with patch("forward_netbox.utilities.forward_api_impl.time.sleep"):
            with self.assertRaisesRegex(ForwardClientError, "reachability computation failed"):
                self.client.trigger_snapshot_reachability(
                    network_id="net-1", snapshot_id="snap-1"
                )

    def test_trigger_snapshot_reachability_requires_network_and_snapshot(self):
        with self.assertRaisesRegex(ForwardClientError, "requires both"):
            self.client.trigger_snapshot_reachability(network_id="", snapshot_id="s1")
        with self.assertRaisesRegex(ForwardClientError, "requires both"):
            self.client.trigger_snapshot_reachability(network_id="n1", snapshot_id="")

    def test_run_nqe_diff_returns_single_page_by_default(self):
        self.client._request = Mock(
            return_value=self._response(
                {
                    "rows": [
                        {"type": "ADDED", "before": None, "after": {"n": 1}},
                        {"type": "DELETED", "before": {"n": 2}, "after": None},
                    ],
                    "totalNumRows": 2,
                }
            )
        )

        rows = self.client.run_nqe_diff(
            query_id="Q_sites",
            before_snapshot_id="snapshot-before",
            after_snapshot_id="snapshot-after",
        )

        self.assertEqual(
            rows,
            [
                {"type": "ADDED", "before": None, "after": {"n": 1}},
                {"type": "DELETED", "before": {"n": 2}, "after": None},
            ],
        )
        self.client._request.assert_called_once()
        self.assertEqual(
            self.client._request.call_args.kwargs["json_body"]["options"]["limit"],
            10000,
        )
        self.assertNotIn(
            "parameters",
            self.client._request.call_args.kwargs["json_body"],
        )

    def test_run_nqe_diff_fetch_all_pages_until_total_num_rows(self):
        self.client._request = Mock(
            side_effect=[
                self._response(
                    {
                        "rows": [
                            {"type": "ADDED", "before": None, "after": {"n": 1}},
                            {"type": "ADDED", "before": None, "after": {"n": 2}},
                        ],
                        "totalNumRows": 3,
                    }
                ),
                self._response(
                    {
                        "rows": [
                            {"type": "DELETED", "before": {"n": 3}, "after": None},
                        ],
                        "totalNumRows": 3,
                    }
                ),
            ]
        )

        rows = self.client.run_nqe_diff(
            query_id="Q_sites",
            before_snapshot_id="snapshot-before",
            after_snapshot_id="snapshot-after",
            limit=2,
            fetch_all=True,
        )

        self.assertEqual(len(rows), 3)
        self.assertEqual(self.client._request.call_count, 2)
        self.assertEqual(
            [
                call.kwargs["json_body"]["options"]["offset"]
                for call in self.client._request.call_args_list
            ],
            [0, 2],
        )
        self.assertEqual(self.client.api_usage_summary()["nqe_diff_calls"], 1)
        self.assertEqual(self.client.api_usage_summary()["nqe_pages"], 2)
        self.assertEqual(self.client.api_usage_summary()["nqe_diff_pages"], 2)

    def test_run_nqe_diff_fetch_all_raises_if_api_ends_early(self):
        self.client._request = Mock(
            side_effect=[
                self._response(
                    {
                        "rows": [
                            {"type": "ADDED", "before": None, "after": {"n": 1}},
                            {"type": "ADDED", "before": None, "after": {"n": 2}},
                        ],
                        "totalNumRows": 5,
                    }
                ),
                self._response(
                    {
                        "rows": [],
                        "totalNumRows": 5,
                    }
                ),
            ]
        )

        with self.assertRaisesRegex(
            ForwardClientError,
            "Forward NQE diff pagination ended early: fetched 2 rows but API reported 5.",
        ):
            self.client.run_nqe_diff(
                query_id="Q_sites",
                before_snapshot_id="snapshot-before",
                after_snapshot_id="snapshot-after",
                limit=2,
                fetch_all=True,
            )

    def test_run_nqe_diff_fetch_all_preserves_column_filters(self):
        self.client._request = Mock(
            side_effect=[
                self._response(
                    {
                        "rows": [
                            {"type": "ADDED", "before": None, "after": {"n": 1}},
                        ],
                        "totalNumRows": 2,
                    }
                ),
                self._response(
                    {
                        "rows": [
                            {"type": "DELETED", "before": {"n": 2}, "after": None},
                        ],
                        "totalNumRows": 2,
                    }
                ),
            ]
        )

        rows = self.client.run_nqe_diff(
            query_id="Q_sites",
            before_snapshot_id="snapshot-before",
            after_snapshot_id="snapshot-after",
            column_filters=[{"column": "site", "operator": "eq", "value": "core"}],
            limit=1,
            fetch_all=True,
        )

        self.assertEqual(
            rows,
            [
                {"type": "ADDED", "before": None, "after": {"n": 1}},
                {"type": "DELETED", "before": {"n": 2}, "after": None},
            ],
        )
        self.assertEqual(self.client._request.call_count, 2)
        for call in self.client._request.call_args_list:
            self.assertEqual(call.kwargs["json_body"]["queryId"], "Q_sites")
            self.assertEqual(
                call.kwargs["json_body"]["options"]["columnFilters"],
                [{"column": "site", "operator": "eq", "value": "core"}],
            )

    def test_run_nqe_diff_fetch_all_raises_when_page_limit_exceeded(self):
        self.client.nqe_fetch_all_max_pages = 2
        self.client._request = Mock(
            side_effect=[
                self._response(
                    {
                        "rows": [
                            {"type": "ADDED", "before": None, "after": {"n": 1}},
                        ],
                    }
                ),
                self._response(
                    {
                        "rows": [
                            {"type": "ADDED", "before": None, "after": {"n": 2}},
                        ],
                    }
                ),
            ]
        )

        with self.assertRaisesRegex(
            ForwardClientError,
            "Forward NQE diff pagination exceeded 2 page\\(s\\)",
        ):
            self.client.run_nqe_diff(
                query_id="Q_sites",
                before_snapshot_id="snapshot-before",
                after_snapshot_id="snapshot-after",
                limit=1,
                fetch_all=True,
            )
        self.assertEqual(self.client._request.call_count, 2)

    def test_run_nqe_diff_fetch_all_raises_on_identical_full_pages(self):
        self.client.nqe_fetch_all_max_pages = 10
        self.client.nqe_identical_full_page_streak_limit = 2
        repeated_page = [
            {"type": "ADDED", "before": None, "after": {"n": 1}},
            {"type": "DELETED", "before": {"n": 2}, "after": None},
        ]
        self.client._request = Mock(
            side_effect=[
                self._response({"rows": repeated_page}),
                self._response({"rows": repeated_page}),
                self._response({"rows": repeated_page}),
            ]
        )

        with self.assertRaisesRegex(
            ForwardClientError,
            "Forward NQE diff pagination did not advance",
        ):
            self.client.run_nqe_diff(
                query_id="Q_sites",
                before_snapshot_id="snapshot-before",
                after_snapshot_id="snapshot-after",
                limit=2,
                fetch_all=True,
            )
        self.assertEqual(self.client._request.call_count, 3)

    def test_run_nqe_diff_includes_parameters_when_supplied(self):
        self.client._request = Mock(
            return_value=self._response(
                {
                    "rows": [
                        {"type": "ADDED", "before": None, "after": {"n": 1}},
                    ],
                    "totalNumRows": 1,
                }
            )
        )

        self.client.run_nqe_diff(
            query_id="Q_sites",
            before_snapshot_id="snapshot-before",
            after_snapshot_id="snapshot-after",
            parameters={"forward_netbox_shard_keys": ["device-1"]},
        )

        self.assertEqual(
            self.client._request.call_args.kwargs["json_body"]["parameters"],
            {"forward_netbox_shard_keys": ["device-1"]},
        )
