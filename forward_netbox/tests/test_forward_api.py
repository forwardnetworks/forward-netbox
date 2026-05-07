from types import SimpleNamespace
from unittest import TestCase
from unittest.mock import Mock
from unittest.mock import patch

from forward_netbox.exceptions import ForwardClientError
from forward_netbox.utilities.forward_api import ForwardClient


class ForwardClientTest(TestCase):
    def setUp(self):
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

    def test_request_uses_netbox_proxy_routers(self):
        response = Mock()
        client = Mock()
        client.request.return_value = response
        client_context = Mock()
        client_context.__enter__ = Mock(return_value=client)
        client_context.__exit__ = Mock(return_value=None)

        with (
            patch(
                "forward_netbox.utilities.forward_api.resolve_proxies",
                return_value={
                    "http": None,
                    "https": "http://proxy.example.com:3128",
                },
            ) as resolve_proxies,
            patch(
                "forward_netbox.utilities.forward_api.httpx.HTTPTransport",
                side_effect=lambda proxy: f"transport:{proxy}",
            ),
            patch(
                "forward_netbox.utilities.forward_api.httpx.Client",
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

    def test_run_nqe_query_returns_single_page_by_default(self):
        self.client._request = Mock(
            return_value=self._response(
                {
                    "items": [
                        {"fields": {"n": 1}},
                        {"fields": {"n": 2}},
                    ],
                    "totalNumItems": 5,
                }
            )
        )

        rows = self.client.run_nqe_query(query="select {n: 1}")

        self.assertEqual(rows, [{"n": 1}, {"n": 2}])
        self.client._request.assert_called_once()
        self.assertEqual(
            self.client._request.call_args.kwargs["json_body"]["queryOptions"]["limit"],
            10000,
        )

    def test_run_nqe_query_fetch_all_pages_until_total_num_items(self):
        self.client._request = Mock(
            side_effect=[
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
        self.assertEqual(self.client._request.call_count, 3)
        self.assertEqual(
            [
                call.kwargs["json_body"]["queryOptions"]["offset"]
                for call in self.client._request.call_args_list
            ],
            [0, 2, 4],
        )

    def test_run_nqe_query_fetch_all_without_total_num_items_stops_on_short_page(self):
        self.client._request = Mock(
            side_effect=[
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
            limit=2,
            fetch_all=True,
        )

        self.assertEqual(rows, [{"n": 1}, {"n": 2}, {"n": 3}])
        self.assertEqual(self.client._request.call_count, 2)

    def test_run_nqe_query_fetch_all_raises_if_api_ends_early(self):
        self.client._request = Mock(
            side_effect=[
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
            "Forward NQE pagination ended early: fetched 2 rows but API reported 5.",
        ):
            self.client.run_nqe_query(
                query="select {n: 1}",
                limit=2,
                fetch_all=True,
            )

    def test_run_nqe_query_fetch_all_preserves_column_filters(self):
        self.client._request = Mock(
            side_effect=[
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
            limit=1,
            fetch_all=True,
        )

        self.assertEqual(rows, [{"n": 1}, {"n": 2}])
        self.assertEqual(self.client._request.call_count, 2)
        for call in self.client._request.call_args_list:
            self.assertEqual(call.kwargs["json_body"]["queryId"], "Q_devices")
            self.assertEqual(
                call.kwargs["json_body"]["queryOptions"]["columnFilters"],
                [{"column": "name", "operator": "contains", "value": "sw"}],
            )

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
