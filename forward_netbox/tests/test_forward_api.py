from types import SimpleNamespace
from unittest import TestCase
from unittest.mock import Mock
from unittest.mock import patch

import httpx

from forward_netbox.exceptions import ForwardClientError
from forward_netbox.exceptions import ForwardConnectivityError
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
                "forward_netbox.utilities.forward_api.httpx.Client",
                side_effect=[first_context, second_context],
            ),
            patch("forward_netbox.utilities.forward_api.time.sleep") as sleep,
        ):
            result = self.client._request("POST", "/nqe", json_body={"query": "q"})

        self.assertEqual(result, response)
        sleep.assert_called_once_with(2)
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
                "forward_netbox.utilities.forward_api.httpx.Client",
                side_effect=[first_context, second_context],
            ),
            patch("forward_netbox.utilities.forward_api.time.sleep") as sleep,
        ):
            result = self.client._request("POST", "/nqe", json_body={"query": "q"})

        self.assertEqual(result, response_ok)
        sleep.assert_called_once_with(2)
        response_ok.raise_for_status.assert_called_once()

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
                "forward_netbox.utilities.forward_api.httpx.Client",
                side_effect=[client_context, client_context],
            ),
            patch("forward_netbox.utilities.forward_api.time.sleep"),
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
                "forward_netbox.utilities.forward_api.httpx.Client",
                side_effect=[client_context, client_context],
            ),
            patch("forward_netbox.utilities.forward_api.time.sleep"),
            self.assertRaisesRegex(
                ForwardConnectivityError,
                "Could not connect to Forward API endpoint",
            ),
        ):
            self.client._request("POST", "/nqe", json_body={"query": "q"})

        self.assertEqual(client.request.call_count, 2)

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

    def test_get_org_nqe_queries_normalizes_directory(self):
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

        rows = self.client.get_org_nqe_queries(directory="/forward_netbox_validation")

        self.assertEqual(rows[0]["queryId"], "Q_devices")
        self.client._request.assert_called_once_with(
            "GET",
            "/nqe/queries",
            params={"dir": "/forward_netbox_validation/"},
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
            params={"path": "/netbox/forward_devices"},
        )

    def test_get_committed_nqe_query_selects_matching_query_from_list_response(self):
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

        query = self.client.get_committed_nqe_query(
            repository="org",
            query_path="netbox/forward_devices",
            commit_id="head",
        )

        self.assertEqual(query["queryId"], "Q_devices")
        self.assertEqual(query["lastCommitId"], "commit-2")

    def test_resolve_nqe_query_reference_returns_query_id_and_commit(self):
        self.client._request = Mock(
            return_value=self._response(
                {
                    "queryId": "Q_devices",
                    "path": "/netbox/forward_devices",
                    "lastCommit": {"id": "commit-1"},
                }
            )
        )

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
