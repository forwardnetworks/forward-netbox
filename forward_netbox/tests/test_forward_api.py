from types import SimpleNamespace
from unittest import TestCase
from unittest.mock import Mock

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
                    "timeout": 60,
                },
            )
        )

    def _response(self, data):
        response = Mock()
        response.json.return_value = data
        return response

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

        rows = self.client.run_nqe_query(query="select {n: 1}", limit=2)

        self.assertEqual(rows, [{"n": 1}, {"n": 2}])
        self.client._request.assert_called_once()

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
