import time
from types import SimpleNamespace
from unittest import TestCase
from unittest.mock import Mock
from unittest.mock import patch

import httpx

from forward_netbox.exceptions import ForwardClientError
from forward_netbox.exceptions import ForwardConnectivityError
from forward_netbox.exceptions import ForwardFetchBudgetExceededError
from forward_netbox.utilities import forward_api_impl
from forward_netbox.utilities.crypto import encrypt_secret
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
                    "password": encrypt_secret("secret"),
                    "verify": True,
                    "timeout": 1200,
                },
            )
        )

    def _response(self, data):
        response = Mock()
        response.json.return_value = data
        return response

    def test_snapshot_data_file_hashes_are_snapshot_correct_and_cached(self):
        self.client._sdk_client.data_files.get_data_files = Mock(
            return_value=[
                {
                    "dataFileName": "netbox_feature_tag_rules.json",
                    "contentMd5Hex": "A1B2C3",
                },
                {
                    "dataFileName": "netbox_device_type_aliases.json",
                    "contentMd5Hex": "D4E5F6",
                },
            ]
        )

        first = forward_api_impl.get_snapshot_data_file_hashes(
            self.client,
            "network-1",
            "snapshot-1",
        )
        second = forward_api_impl.get_snapshot_data_file_hashes(
            self.client,
            "network-1",
            "snapshot-1",
        )

        self.assertEqual(
            first,
            {
                "netbox_feature_tag_rules": "md5:a1b2c3",
                "netbox_feature_tag_rules.json": "md5:a1b2c3",
                "netbox_device_type_aliases": "md5:d4e5f6",
                "netbox_device_type_aliases.json": "md5:d4e5f6",
            },
        )
        self.assertEqual(second, first)
        self.client._sdk_client.data_files.get_data_files.assert_called_once_with(
            network_id="network-1",
            view="snapshot",
            snapshot_id="snapshot-1",
        )

    def test_call_sdk_translates_an_sdk_exception(self):
        from forward_sdk.errors import ForwardAuthError

        sdk_call = Mock(
            side_effect=ForwardAuthError(
                "failed with HTTP 401: x", status=401, text="x"
            )
        )

        with self.assertRaises(ForwardClientError):
            self.client._call_sdk(sdk_call)

    def test_call_sdk_records_a_transport_failure_the_hooks_cannot_see(self):
        from forward_sdk.errors import ForwardTransportError

        transport_error = ForwardTransportError("GET /x failed: boom", attempts=1)
        transport_error.__cause__ = httpx.ConnectError("boom")
        sdk_call = Mock(side_effect=transport_error)

        with self.assertRaises(ForwardConnectivityError):
            self.client._call_sdk(sdk_call)

        summary = self.client.api_usage_summary()
        self.assertEqual(summary["http_failures"], 1)
        self.assertEqual(summary["http_transport_failures"], 1)

    def test_get_networks_filters_incomplete_rows_and_builds_a_label(self):
        self.client._sdk_client.networks.list = Mock(
            return_value=[
                SimpleNamespace(id="net-1", name="Lab"),
                SimpleNamespace(id="", name="No id"),
                SimpleNamespace(id="net-2", name=""),
            ]
        )

        networks = forward_api_impl.get_networks(self.client)

        self.assertEqual(
            networks,
            [{"id": "net-1", "name": "Lab", "label": "Lab (net-1)"}],
        )
        # Cached on the second call: the SDK is not called again.
        forward_api_impl.get_networks(self.client)
        self.client._sdk_client.networks.list.assert_called_once_with()

    def test_get_snapshots_normalizes_sdk_snapshots_into_the_plugins_own_shape(self):
        self.client._sdk_client.snapshots.list = Mock(
            return_value=[
                SimpleNamespace(
                    id="snap-1",
                    state="PROCESSED",
                    created_at="2026-06-01T00:00:00Z",
                    processed_at="2026-06-01T01:00:00Z",
                ),
            ]
        )

        snapshots = forward_api_impl.get_snapshots(
            self.client, "net-1", include_archived=True, limit=5
        )

        self.assertEqual(
            snapshots,
            [
                {
                    "id": "snap-1",
                    "state": "PROCESSED",
                    "created_at": "2026-06-01T00:00:00Z",
                    "processed_at": "2026-06-01T01:00:00Z",
                    "label": "snap-1 | PROCESSED | 2026-06-01T01:00:00Z",
                }
            ],
        )
        self.client._sdk_client.snapshots.list.assert_called_once_with(
            "net-1", include_archived=True, limit=5
        )

    def test_get_latest_processed_snapshot_uses_the_plugins_own_camelcase_shape(self):
        self.client._sdk_client.snapshots.latest_processed = Mock(
            return_value=SimpleNamespace(
                id="snap-1",
                state="PROCESSED",
                created_at="2026-06-01T00:00:00Z",
                processed_at="2026-06-01T01:00:00Z",
            )
        )

        snapshot = forward_api_impl.get_latest_processed_snapshot(self.client, "net-1")

        self.assertEqual(
            snapshot,
            {
                "id": "snap-1",
                "state": "PROCESSED",
                "createdAt": "2026-06-01T00:00:00Z",
                "processedAt": "2026-06-01T01:00:00Z",
            },
        )
        self.client._sdk_client.snapshots.latest_processed.assert_called_once_with(
            "net-1"
        )

    def test_get_latest_processed_snapshot_returns_empty_dict_when_none_exists(self):
        self.client._sdk_client.snapshots.latest_processed = Mock(return_value=None)

        snapshot = forward_api_impl.get_latest_processed_snapshot(self.client, "net-1")

        self.assertEqual(snapshot, {})
        with self.assertRaises(ForwardClientError) as cm:
            forward_api_impl.get_latest_processed_snapshot_id(self.client, "net-1")
        self.assertEqual(
            str(cm.exception),
            "Forward latestProcessed snapshot response did not include an ID.",
        )

    def test_get_snapshot_metrics_passes_through_the_sdks_dict_directly(self):
        self.client._sdk_client.snapshots.metrics = Mock(
            return_value={"numSuccessfulDevices": 12}
        )

        metrics = forward_api_impl.get_snapshot_metrics(self.client, "snap-1")

        self.assertEqual(metrics, {"numSuccessfulDevices": 12})
        self.client._sdk_client.snapshots.metrics.assert_called_once_with("snap-1")

    def test_get_latest_collected_snapshot_id_skips_backfilled_newest_first(self):
        snapshots_mock = Mock(
            return_value=[
                {"id": "snap-old", "state": "PROCESSED", "processed_at": "2026-06-15"},
                {"id": "snap-new", "state": "PROCESSED", "processed_at": "2026-06-17"},
                {"id": "snap-mid", "state": "PROCESSED", "processed_at": "2026-06-16"},
                {"id": "snap-unprocessed", "state": "PROCESSING", "processed_at": ""},
            ]
        )
        # snap-new (newest) is all-backfilled -> probe returns nothing;
        # snap-mid has a collected in-scope device.
        probe_results = {
            "snap-new": [],
            "snap-mid": [{"name": "device-1"}],
        }
        self.client.run_nqe_query = Mock(
            side_effect=lambda **kwargs: probe_results.get(kwargs["snapshot_id"], [])
        )

        with patch.object(forward_api_impl, "get_snapshots", snapshots_mock):
            snapshot_id = forward_api_impl.get_latest_collected_snapshot_id(
                self.client, "net-1", include_tags=["Prod_Core"], include_match="any"
            )

        self.assertEqual(snapshot_id, "snap-mid")
        probed = [
            call.kwargs["snapshot_id"] for call in self.client.run_nqe_query.mock_calls
        ]
        # Newest first, unprocessed skipped, stops once collected snapshot found.
        self.assertEqual(probed, ["snap-new", "snap-mid"])
        # Probe carries the completed filter and the tag scope.
        probe_query = self.client.run_nqe_query.mock_calls[0].kwargs["query"]
        self.assertIn("DeviceSnapshotResult.completed", probe_query)
        self.assertIn('"Prod_Core" in device.tagNames', probe_query)

    def test_get_latest_collected_snapshot_id_raises_when_all_backfilled(self):
        snapshots_mock = Mock(
            return_value=[
                {"id": "snap-a", "state": "PROCESSED", "processed_at": "2026-06-17"},
                {"id": "snap-b", "state": "PROCESSED", "processed_at": "2026-06-16"},
            ]
        )
        self.client.run_nqe_query = Mock(return_value=[])

        with self.assertRaises(ForwardClientError) as ctx:
            with patch.object(forward_api_impl, "get_snapshots", snapshots_mock):
                forward_api_impl.get_latest_collected_snapshot_id(
                    self.client, "net-1", include_tags=["Prod_Core"]
                )

        self.assertIn("backfilled", str(ctx.exception).lower())
        self.assertEqual(self.client.run_nqe_query.call_count, 2)

    def test_get_latest_collected_snapshot_id_respects_scan_limit(self):
        snapshots_mock = Mock(
            return_value=[
                {
                    "id": f"snap-{i}",
                    "state": "PROCESSED",
                    "processed_at": f"2026-06-{20 - i:02d}",
                }
                for i in range(5)
            ]
        )
        self.client.run_nqe_query = Mock(return_value=[])

        with self.assertRaises(ForwardClientError):
            with patch.object(forward_api_impl, "get_snapshots", snapshots_mock):
                forward_api_impl.get_latest_collected_snapshot_id(
                    self.client, "net-1", scan_limit=2
                )

        self.assertEqual(self.client.run_nqe_query.call_count, 2)

    def test_get_latest_collected_snapshot_id_raises_without_processed_snapshots(self):
        snapshots_mock = Mock(
            return_value=[{"id": "snap-x", "state": "PROCESSING", "processed_at": ""}]
        )
        self.client.run_nqe_query = Mock(return_value=[])

        with self.assertRaises(ForwardClientError) as ctx:
            with patch.object(forward_api_impl, "get_snapshots", snapshots_mock):
                forward_api_impl.get_latest_collected_snapshot_id(self.client, "net-1")

        self.assertIn("processed snapshot", str(ctx.exception).lower())
        self.client.run_nqe_query.assert_not_called()

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
                    "password": encrypt_secret("secret"),
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
                    "password": encrypt_secret("secret"),
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
                    "password": encrypt_secret("secret"),
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
                    "password": encrypt_secret("secret"),
                    "api_requests_per_minute": 120,
                },
            )
        )

        key = client._rate_limit_key()

        self.assertNotIn("rate-limit@example.com", key)
        self.assertIn("forward-api-rate-limit", key)

    def test_network_and_head_commit_reads_are_cached_per_client(self):
        self.client._sdk_client.networks.list = Mock(
            return_value=[SimpleNamespace(id="network-1", name="Network 1")]
        )
        self.client._sdk_client.nqe.repo.head_commit_id = Mock(return_value="commit-1")

        networks_first = forward_api_impl.get_networks(self.client)
        networks_second = forward_api_impl.get_networks(self.client)
        head_first = self.client.get_org_nqe_head_commit_id()
        head_second = self.client.get_org_nqe_head_commit_id()

        self.client._sdk_client.networks.list.assert_called_once_with()
        self.client._sdk_client.nqe.repo.head_commit_id.assert_called_once_with()
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
                    "password": encrypt_secret("secret"),
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
                    "password": encrypt_secret("secret"),
                    "verify": True,
                    "timeout": 1200,
                },
            )
        )
        client_one._sdk_client.networks.list = Mock(
            return_value=[SimpleNamespace(id="network-1", name="Network 1")]
        )
        client_two._sdk_client.networks.list = Mock(
            side_effect=AssertionError("shared cache should avoid second request")
        )

        with patch(
            "forward_netbox.utilities.forward_api_impl._shared_read_cache",
            return_value=shared_cache,
        ):
            first = forward_api_impl.get_networks(client_one)
            second = forward_api_impl.get_networks(client_two)

        self.assertEqual(first, second)
        self.assertEqual(client_one._sdk_client.networks.list.call_count, 1)
        self.assertEqual(client_two._sdk_client.networks.list.call_count, 0)
        self.assertEqual(first[0]["label"], "Network 1 (network-1)")

    def test_shared_query_cache_generation_invalidates_after_mutation(self):
        shared_cache = FakeSharedCache()
        client_one = ForwardClient(
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
        client_two = ForwardClient(
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
        client_three = ForwardClient(
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
        from forward_sdk.nqe.repository import CommitReport

        client_one._sdk_client.nqe.repo.head_commit_id = Mock(return_value="commit-1")
        client_two._sdk_client.nqe.repo.commit = Mock(
            return_value=CommitReport(
                committed_paths=("netbox/forward_devices",), commit_id="commit-2"
            )
        )
        client_three._sdk_client.nqe.repo.head_commit_id = Mock(
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
        self.assertEqual(client_one._sdk_client.nqe.repo.head_commit_id.call_count, 1)
        self.assertEqual(client_two._sdk_client.nqe.repo.commit.call_count, 1)
        self.assertEqual(client_three._sdk_client.nqe.repo.head_commit_id.call_count, 0)

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
                "nqe_execution_signature_count": 0,
                "nqe_repeated_execution_count": 0,
                "nqe_max_execution_signature_count": 0,
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

    def _fake_nqe_execution(self, *, pages=(), wait_error=None):
        """A stand-in for the SDK's `NqeExecution`: `.wait()`/`.result_page()`."""
        execution = Mock()
        if wait_error is not None:
            execution.wait.side_effect = wait_error
        execution.result_page.side_effect = [
            SimpleNamespace(items=items, total_num_items=total)
            for items, total in pages
        ]
        return execution

    def test_run_nqe_query_returns_single_page_by_default(self):
        execution = self._fake_nqe_execution(
            pages=[([{"n": 1}, {"n": 2}], 5)],
        )
        self.client._sdk_client.nqe.execute = Mock(return_value=execution)

        rows = self.client.run_nqe_query(
            query="select {n: 1}",
            network_id="network-1",
            snapshot_id="snapshot-1",
        )

        self.assertEqual(rows, [{"n": 1}, {"n": 2}])
        self.client._sdk_client.nqe.execute.assert_called_once_with(
            "select {n: 1}",
            network_id="network-1",
            snapshot_id="snapshot-1",
            parameters=None,
        )
        execution.wait.assert_called_once()
        execution.result_page.assert_called_once_with(
            offset=0, limit=self.client.nqe_page_size
        )

    def test_run_nqe_query_omits_abbreviated_hex_commit_id(self):
        execution = self._fake_nqe_execution(pages=[([{"n": 1}], 1)])
        self.client._sdk_client.nqe.execute = Mock(return_value=execution)

        rows = self.client.run_nqe_query(
            query_id="Q_devices",
            commit_id="1a2b",
            network_id="network-1",
            snapshot_id="snapshot-1",
        )

        self.assertEqual(rows, [{"n": 1}])
        ref = self.client._sdk_client.nqe.execute.call_args[0][0]
        self.assertEqual(ref.query_id, "Q_devices")
        self.assertIsNone(ref.commit_id)

    def test_run_nqe_query_omits_head_commit_id(self):
        execution = self._fake_nqe_execution(pages=[([{"n": 1}], 1)])
        self.client._sdk_client.nqe.execute = Mock(return_value=execution)

        rows = self.client.run_nqe_query(
            query_id="Q_devices",
            commit_id="head",
            network_id="network-1",
            snapshot_id="snapshot-1",
        )

        self.assertEqual(rows, [{"n": 1}])
        ref = self.client._sdk_client.nqe.execute.call_args[0][0]
        self.assertEqual(ref.query_id, "Q_devices")
        self.assertIsNone(ref.commit_id)

    def test_run_nqe_query_passes_commit_and_parameters_through(self):
        execution = self._fake_nqe_execution(pages=[([{"n": 1}, {"n": 2}], 2)])
        self.client._sdk_client.nqe.execute = Mock(return_value=execution)

        rows = self.client.run_nqe_query(
            query_id="Q_devices",
            commit_id="commit-1",
            network_id="network-1",
            snapshot_id="snapshot-1",
            parameters={"forward_netbox_shard_keys": ["device-1"]},
        )

        self.assertEqual(rows, [{"n": 1}, {"n": 2}])
        ref = self.client._sdk_client.nqe.execute.call_args[0][0]
        self.assertEqual(ref.query_id, "Q_devices")
        self.assertEqual(ref.commit_id, "commit-1")
        self.assertEqual(
            self.client._sdk_client.nqe.execute.call_args.kwargs["parameters"],
            {"forward_netbox_shard_keys": ["device-1"]},
        )
        summary = self.client.api_usage_summary()
        self.assertEqual(summary["nqe_query_calls"], 1)
        self.assertEqual(summary["nqe_async_query_calls"], 1)
        self.assertEqual(summary["nqe_async_trigger_calls"], 1)
        self.assertEqual(summary["nqe_async_status_calls"], 1)
        self.assertEqual(summary["nqe_async_result_calls"], 1)
        self.assertEqual(summary["nqe_pages"], 1)
        self.assertEqual(summary["nqe_execution_signature_count"], 1)
        self.assertEqual(summary["nqe_repeated_execution_count"], 0)

    def test_api_usage_detects_repeated_logical_nqe_execution(self):
        self.client._sdk_client.nqe.execute = Mock(
            side_effect=[
                self._fake_nqe_execution(pages=[([], 0)]),
                self._fake_nqe_execution(pages=[([], 0)]),
            ]
        )
        call = {
            "query_id": "Q_devices",
            "network_id": "network-1",
            "snapshot_id": "snapshot-1",
            "parameters": {"forward_netbox_shard_keys": ["device-1"]},
        }

        self.client.run_nqe_query(**call)
        self.client.run_nqe_query(**call)

        summary = self.client.api_usage_summary()
        self.assertEqual(summary["nqe_query_calls"], 2)
        self.assertEqual(summary["nqe_execution_signature_count"], 1)
        self.assertEqual(summary["nqe_repeated_execution_count"], 1)
        self.assertEqual(summary["nqe_max_execution_signature_count"], 2)

        self.client.reset_api_usage_summary()

        reset_summary = self.client.api_usage_summary()
        self.assertEqual(reset_summary["nqe_execution_signature_count"], 0)
        self.assertEqual(reset_summary["nqe_repeated_execution_count"], 0)

    def test_run_nqe_query_async_fetch_all_pages_single_execution(self):
        execution = self._fake_nqe_execution(
            pages=[
                ([{"n": 1}, {"n": 2}], 3),
                ([{"n": 3}], 3),
            ],
        )
        self.client._sdk_client.nqe.execute = Mock(return_value=execution)

        rows = self.client.run_nqe_query(
            query="foreach d in network.devices select { n: 1 }",
            network_id="network-1",
            snapshot_id="snapshot-1",
            limit=2,
            fetch_all=True,
        )

        self.assertEqual(rows, [{"n": 1}, {"n": 2}, {"n": 3}])
        self.client._sdk_client.nqe.execute.assert_called_once()
        execution.wait.assert_called_once()
        self.assertEqual(
            [call.kwargs for call in execution.result_page.call_args_list],
            [{"offset": 0, "limit": 2}, {"offset": 2, "limit": 2}],
        )
        self.assertEqual(self.client.api_usage_summary()["nqe_async_trigger_calls"], 1)
        self.assertEqual(self.client.api_usage_summary()["nqe_async_result_calls"], 2)

    def test_run_nqe_query_async_raises_on_non_ok_outcome(self):
        from forward_sdk.errors import ForwardExecutionError

        execution = self._fake_nqe_execution(
            wait_error=ForwardExecutionError(
                "NQE execution finished with outcome USER_ERROR: bad query"
            ),
        )
        self.client._sdk_client.nqe.execute = Mock(return_value=execution)

        with self.assertRaisesRegex(
            ForwardClientError,
            "finished with outcome USER_ERROR: bad query",
        ):
            self.client.run_nqe_query(
                query_id="Q_devices",
                network_id="network-1",
                snapshot_id="snapshot-1",
            )
        execution.result_page.assert_not_called()

    def test_run_nqe_query_wait_timeout_becomes_a_fetch_budget_error(self):
        # NqeExecution.wait() is purely time-based; a timeout there is what
        # today's poll-count ceiling (nqe_async_max_polls) is replaced by.
        from forward_sdk.errors import ForwardTimeoutError

        execution = self._fake_nqe_execution(
            wait_error=ForwardTimeoutError("NQE execution did not finish in time"),
        )
        self.client._sdk_client.nqe.execute = Mock(return_value=execution)

        with self.assertRaises(ForwardFetchBudgetExceededError):
            self.client.run_nqe_query(
                query_id="Q_devices",
                network_id="network-1",
                snapshot_id="snapshot-1",
            )

    def test_run_nqe_query_derives_a_wait_ceiling_from_poll_config_without_a_deadline(
        self,
    ):
        self.client.nqe_async_max_polls = 10
        self.client.nqe_async_poll_interval_seconds = 2.0
        execution = self._fake_nqe_execution(pages=[([], 0)])
        self.client._sdk_client.nqe.execute = Mock(return_value=execution)

        self.client.run_nqe_query(
            query_id="Q_devices",
            network_id="network-1",
            snapshot_id="snapshot-1",
        )

        execution.wait.assert_called_once_with(poll_interval=2.0, timeout=20.0)

    def test_run_nqe_query_async_requires_snapshot_id(self):
        client = ForwardClient(
            SimpleNamespace(
                url="https://fwd.app",
                parameters={
                    "username": "user@example.com",
                    "password": encrypt_secret("secret"),
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
                    "password": encrypt_secret("secret"),
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
        execution = self._fake_nqe_execution(
            pages=[
                ([{"n": 1}, {"n": 2}], 5),
                ([{"n": 3}, {"n": 4}], 5),
                ([{"n": 5}], 5),
            ],
        )
        self.client._sdk_client.nqe.execute = Mock(return_value=execution)

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
        self.assertEqual(
            [call.kwargs["offset"] for call in execution.result_page.call_args_list],
            [0, 2, 4],
        )
        self.assertEqual(self.client.api_usage_summary()["nqe_query_calls"], 1)
        self.assertEqual(self.client.api_usage_summary()["nqe_pages"], 3)
        self.assertEqual(self.client.api_usage_summary()["nqe_query_pages"], 3)
        self.assertEqual(self.client.api_usage_summary()["nqe_async_query_calls"], 1)
        self.assertEqual(self.client.api_usage_summary()["nqe_async_trigger_calls"], 1)
        self.assertEqual(self.client.api_usage_summary()["nqe_async_result_calls"], 3)

    def test_run_nqe_query_fetch_all_without_total_num_items_stops_on_short_page(self):
        execution = self._fake_nqe_execution(
            pages=[
                ([{"n": 1}, {"n": 2}], None),
                ([{"n": 3}], None),
            ],
        )
        self.client._sdk_client.nqe.execute = Mock(return_value=execution)

        rows = self.client.run_nqe_query(
            query="select {n: 1}",
            network_id="network-1",
            snapshot_id="snapshot-1",
            limit=2,
            fetch_all=True,
        )

        self.assertEqual(rows, [{"n": 1}, {"n": 2}, {"n": 3}])
        self.assertEqual(execution.result_page.call_count, 2)

    def test_run_nqe_query_fetch_all_raises_if_api_ends_early(self):
        execution = self._fake_nqe_execution(
            pages=[
                ([{"n": 1}, {"n": 2}], 5),
                ([], 5),
            ],
        )
        self.client._sdk_client.nqe.execute = Mock(return_value=execution)

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

    def test_run_nqe_query_fetch_all_raises_when_page_limit_exceeded(self):
        self.client.nqe_fetch_all_max_pages = 2
        execution = self._fake_nqe_execution(
            pages=[
                ([{"n": 1}], None),
                ([{"n": 2}], None),
            ],
        )
        self.client._sdk_client.nqe.execute = Mock(return_value=execution)

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
        self.assertEqual(execution.result_page.call_count, 2)

    def test_run_nqe_query_fetch_all_raises_when_row_ceiling_exceeded(self):
        # A giant unsharded result must abort before exhausting worker memory.
        self.client.nqe_fetch_all_max_rows = 1
        self.client.nqe_fetch_all_max_pages = 1000
        execution = self._fake_nqe_execution(
            pages=[
                ([{"n": 1}], None),
                ([{"n": 2}], None),
            ],
        )
        self.client._sdk_client.nqe.execute = Mock(return_value=execution)

        with self.assertRaisesRegex(
            ForwardClientError,
            "exceeded the in-memory row ceiling",
        ):
            self.client.run_nqe_query(
                query_id="Q_devices",
                network_id="network-1",
                snapshot_id="snapshot-1",
                limit=1,
                fetch_all=True,
            )

    def test_run_nqe_query_fetch_all_raises_on_identical_full_pages(self):
        self.client.nqe_fetch_all_max_pages = 10
        self.client.nqe_identical_full_page_streak_limit = 2
        repeated_page = [{"n": 1}, {"n": 2}]
        execution = self._fake_nqe_execution(
            pages=[
                (repeated_page, None),
                (repeated_page, None),
                (repeated_page, None),
            ],
        )
        self.client._sdk_client.nqe.execute = Mock(return_value=execution)

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
        self.assertEqual(execution.result_page.call_count, 3)

    def test_snapshot_reads_are_cached_per_client(self):
        self.client._sdk_client.snapshots.list = Mock(
            return_value=[
                SimpleNamespace(
                    id="snapshot-1",
                    state="processed",
                    created_at="2026-06-01T00:00:00Z",
                    processed_at="2026-06-01T01:00:00Z",
                )
            ]
        )
        self.client._sdk_client.snapshots.metrics = Mock(return_value={"totalCount": 5})
        self.client._sdk_client.snapshots.latest_processed = Mock(
            return_value=SimpleNamespace(
                id="snapshot-1",
                state="processed",
                created_at="2026-06-01T00:00:00Z",
                processed_at="2026-06-01T01:00:00Z",
            )
        )

        snapshots_first = forward_api_impl.get_snapshots(self.client, "network-1")
        snapshots_second = forward_api_impl.get_snapshots(self.client, "network-1")
        metrics_first = forward_api_impl.get_snapshot_metrics(self.client, "snapshot-1")
        metrics_second = forward_api_impl.get_snapshot_metrics(
            self.client, "snapshot-1"
        )
        latest_first = forward_api_impl.get_latest_processed_snapshot(
            self.client, "network-1"
        )
        latest_second = forward_api_impl.get_latest_processed_snapshot(
            self.client, "network-1"
        )

        self.assertEqual(self.client._sdk_client.snapshots.list.call_count, 1)
        self.assertEqual(self.client._sdk_client.snapshots.metrics.call_count, 1)
        self.assertEqual(
            self.client._sdk_client.snapshots.latest_processed.call_count, 1
        )
        self.assertEqual(snapshots_first, snapshots_second)
        self.assertEqual(metrics_first, metrics_second)
        self.assertEqual(latest_first, latest_second)
        self.assertEqual(snapshots_first[0]["id"], "snapshot-1")
        self.assertEqual(metrics_first, {"totalCount": 5})
        self.assertEqual(latest_first["id"], "snapshot-1")

    def test_get_org_nqe_queries_normalizes_directory(self):
        shared_cache = FakeSharedCache()
        self.client._sdk_client.nqe.queries = Mock(
            return_value=[
                SimpleNamespace(
                    query_id="Q_devices",
                    path="/forward_netbox_validation/forward_devices",
                    intent="Forward Devices",
                )
            ]
        )

        with patch(
            "forward_netbox.utilities.forward_api_impl._shared_read_cache",
            return_value=shared_cache,
        ):
            rows = self.client._get_org_nqe_queries(
                directory="/forward_netbox_validation"
            )

        self.assertEqual(rows[0]["queryId"], "Q_devices")
        self.client._sdk_client.nqe.queries.assert_called_once_with(
            directory="/forward_netbox_validation/"
        )

    def test_nqe_query_lists_are_cached_per_client(self):
        shared_cache = FakeSharedCache()
        self.client._sdk_client.nqe.queries = Mock(
            return_value=[
                SimpleNamespace(
                    query_id="Q_devices",
                    path="/forward_netbox_validation/forward_devices",
                    intent="Forward Devices",
                )
            ]
        )
        self.client._sdk_client.nqe.repo.queries = Mock(
            return_value=[
                SimpleNamespace(
                    query_id="FQ_devices",
                    path="/netbox/forward_devices",
                    intent="",
                    last_commit_id="commit-1",
                )
            ]
        )

        with patch(
            "forward_netbox.utilities.forward_api_impl._shared_read_cache",
            return_value=shared_cache,
        ):
            org_first = self.client._get_org_nqe_queries(
                directory="/forward_netbox_validation"
            )
            org_second = self.client._get_org_nqe_queries(
                directory="/forward_netbox_validation"
            )
            repo_first = self.client._get_nqe_repository_queries(
                repository="fwd",
                directory="/netbox",
            )
            repo_second = self.client._get_nqe_repository_queries(
                repository="fwd",
                directory="/netbox",
            )

        self.assertEqual(org_first, org_second)
        self.assertEqual(repo_first, repo_second)
        self.assertEqual(org_first[0]["queryId"], "Q_devices")
        self.assertEqual(repo_first[0]["queryId"], "FQ_devices")

    def test_nqe_repository_query_index_is_cached_per_client(self):
        shared_cache = FakeSharedCache()
        self.client._sdk_client.nqe.repo.queries = Mock(
            return_value=[
                SimpleNamespace(
                    query_id="Q_devices",
                    path="/netbox/forward_devices",
                    intent="",
                    last_commit_id="commit-1",
                )
            ]
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

        self.assertEqual(self.client._sdk_client.nqe.repo.queries.call_count, 1)
        self.assertEqual(
            first["by_query_id"]["Q_devices"][0]["path"],
            second["by_query_id"]["Q_devices"][0]["path"],
        )
        self.assertEqual(
            first["by_path"]["/netbox/forward_devices"]["queryId"], "Q_devices"
        )

    def test_get_nqe_repository_queries_reads_forward_library(self):
        self.client._sdk_client.nqe.repo.queries = Mock(
            return_value=[
                SimpleNamespace(
                    query_id="FQ_devices",
                    path="/netbox/forward_devices",
                    intent="",
                    last_commit_id="commit-1",
                ),
                SimpleNamespace(
                    query_id="FQ_other",
                    path="/other/query",
                    intent="",
                    last_commit_id="commit-2",
                ),
            ]
        )

        rows = self.client._get_nqe_repository_queries(
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
        self.client._sdk_client.nqe.repo.queries.assert_called_once_with(
            repository="fwd"
        )

    def test_get_nqe_repository_queries_uses_org_query_list_without_fallback(self):
        self.client._sdk_client.nqe.queries = Mock(
            return_value=[
                SimpleNamespace(
                    query_id="Q_devices",
                    path="/forward_netbox_validation/forward_devices",
                    intent="Forward Devices",
                )
            ]
        )

        rows = self.client._get_nqe_repository_queries(
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
        self.client._sdk_client.nqe.queries.assert_called_once_with(
            directory="/forward_netbox_validation/"
        )

    def test_get_nqe_query_history(self):
        self.client._sdk_client.nqe.repo.history = Mock(
            return_value=[{"id": "commit-1", "path": "/netbox/forward_devices"}]
        )

        rows = self.client.get_nqe_query_history("FQ/devices")

        self.assertEqual(rows[0]["id"], "commit-1")
        self.client._sdk_client.nqe.repo.history.assert_called_once_with("FQ/devices")

    def test_nqe_query_history_is_cached_per_client(self):
        shared_cache = FakeSharedCache()
        self.client._sdk_client.nqe.repo.history = Mock(
            return_value=[{"id": "commit-1", "path": "/netbox/forward_devices"}]
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
        self.client._sdk_client.nqe.queries = Mock(return_value=[])
        self.client._sdk_client.nqe.repo.history = Mock(return_value=[])

        with patch(
            "forward_netbox.utilities.forward_api_impl._shared_read_cache",
            return_value=shared_cache,
        ):
            org_first = self.client._get_org_nqe_queries(directory="/empty")
            org_second = self.client._get_org_nqe_queries(directory="/empty")
            history_first = self.client.get_nqe_query_history("FQ/empty")
            history_second = self.client.get_nqe_query_history("FQ/empty")

        self.assertEqual(org_first, org_second)
        self.assertEqual(history_first, history_second)
        self.assertEqual(org_first, [])
        self.assertEqual(history_first, [])

    def test_get_committed_nqe_query_resolves_repository_path(self):
        self.client._sdk_client.nqe.repo.queries = Mock(
            return_value=[
                SimpleNamespace(
                    query_id="Q_devices",
                    path="/netbox/forward_devices",
                    intent="",
                    last_commit_id=None,
                    last_commit=SimpleNamespace(id="commit-1"),
                    source_code=None,
                )
            ]
        )

        query = self.client.get_committed_nqe_query(
            repository="org",
            query_path="netbox/forward_devices",
            commit_id="commit-1",
        )

        self.assertEqual(query["queryId"], "Q_devices")
        self.client._sdk_client.nqe.repo.queries.assert_called_once_with(
            repository="org",
            commit_id="commit-1",
            path="/netbox/forward_devices",
            with_source=True,
        )

    def test_get_committed_nqe_query_uses_repository_index_for_fwd_head(self):
        shared_cache = FakeSharedCache()
        self.client._sdk_client.nqe.repo.queries = Mock(
            return_value=[
                SimpleNamespace(
                    query_id="FQ_devices",
                    path="/netbox/forward_devices",
                    intent="Forward Devices",
                    last_commit_id="commit-1",
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
            )

        self.assertEqual(query["queryId"], "FQ_devices")
        self.assertEqual(query["lastCommitId"], "commit-1")
        self.assertEqual(query["intent"], "Forward Devices")
        # The repository index already carried a usable commit, so no
        # separate commits-endpoint fetch was needed.
        self.assertEqual(self.client._sdk_client.nqe.repo.queries.call_count, 1)

    def test_get_committed_nqe_query_requests_source_for_head_when_requested(self):
        shared_cache = FakeSharedCache()
        self.client._sdk_client.nqe.repo.queries = Mock(
            return_value=[
                SimpleNamespace(
                    query_id="FQ_devices",
                    path="/netbox/forward_devices",
                    intent="Forward Devices",
                    last_commit_id="commit-1",
                    last_commit=None,
                    source_code="select {}",
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
        self.client._sdk_client.nqe.repo.queries.assert_called_once_with(
            repository="fwd",
            commit_id="commit-1",
            path="/netbox/forward_devices",
            with_source=True,
        )

    def test_get_committed_nqe_query_reuses_provided_query_index_on_miss(self):
        self.client._sdk_client.nqe.repo.queries = Mock(
            return_value=[
                SimpleNamespace(
                    query_id="Q_devices",
                    path="/netbox/forward_devices",
                    intent="",
                    last_commit_id=None,
                    last_commit=SimpleNamespace(id="commit-1"),
                    source_code=None,
                )
            ]
        )

        query = self.client.get_committed_nqe_query(
            repository="org",
            query_path="netbox/forward_devices",
            commit_id="head",
            query_index={"by_path": {}},
        )

        self.assertEqual(query["queryId"], "Q_devices")
        self.assertEqual(query["lastCommitId"], "commit-1")
        self.client._sdk_client.nqe.repo.queries.assert_called_once_with(
            repository="org",
            commit_id="head",
            path="/netbox/forward_devices",
            with_source=True,
        )

    def test_get_committed_nqe_query_uses_org_query_list_for_head(self):
        shared_cache = FakeSharedCache()
        # The org repository's directory listing carries no commit per query
        # (real Forward behavior, matching `NqeQuery` having no
        # `last_commit_id` field at all) - the index always misses on commit
        # for org, and this always falls through to the commits endpoint.
        self.client._sdk_client.nqe.queries = Mock(
            return_value=[
                SimpleNamespace(
                    query_id="Q_devices",
                    path="/netbox/forward_devices",
                    intent="Forward Devices",
                )
            ]
        )
        self.client._sdk_client.nqe.repo.queries = Mock(
            return_value=[
                SimpleNamespace(
                    query_id="Q_devices",
                    path="/netbox/forward_devices",
                    intent="Forward Devices",
                    last_commit_id="commit-2",
                    last_commit=None,
                    source_code=None,
                )
            ]
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
        self.assertEqual(self.client._sdk_client.nqe.queries.call_count, 1)
        self.assertEqual(self.client._sdk_client.nqe.repo.queries.call_count, 1)

    def test_get_committed_nqe_query_selects_matching_query_from_list_response(self):
        shared_cache = FakeSharedCache()
        self.client._sdk_client.nqe.queries = Mock(return_value=[])
        self.client._sdk_client.nqe.repo.queries = Mock(
            return_value=[
                SimpleNamespace(
                    query_id="Q_sites",
                    path="/netbox/forward_sites",
                    intent="",
                    last_commit_id="commit-1",
                    last_commit=None,
                    source_code=None,
                ),
                SimpleNamespace(
                    query_id="Q_devices",
                    path="/netbox/forward_devices",
                    intent="",
                    last_commit_id="commit-2",
                    last_commit=None,
                    source_code=None,
                ),
            ]
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
        self.client._sdk_client.nqe.queries = Mock(return_value=[])
        self.client._sdk_client.nqe.repo.queries = Mock(
            return_value=[
                SimpleNamespace(
                    query_id="Q_devices",
                    path="/netbox/forward_devices",
                    intent="Forward Devices",
                    last_commit_id="commit-1",
                    last_commit=None,
                    source_code=None,
                )
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
                "intent": "Forward Devices",
            },
        )

    def test_resolve_nqe_query_reference_falls_back_to_committed_lookup_when_index_missing(
        self,
    ):
        shared_cache = FakeSharedCache()
        self.client._sdk_client.nqe.queries = Mock(return_value=[])
        self.client._sdk_client.nqe.repo.queries = Mock(
            return_value=[
                SimpleNamespace(
                    query_id="Q_devices",
                    path="/netbox/forward_devices",
                    intent="",
                    last_commit_id="commit-1",
                    last_commit=None,
                    source_code=None,
                )
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
        self.client._sdk_client.nqe.repo.queries = Mock(
            return_value=[
                SimpleNamespace(
                    query_id="Q_devices",
                    path="/netbox/forward_devices",
                    intent="",
                    last_commit_id="commit-1",
                    last_commit=None,
                    source_code=None,
                )
            ]
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
        self.assertEqual(self.client._sdk_client.nqe.repo.queries.call_count, 1)

    def test_resolve_nqe_query_reference_uses_cached_repository_index(self):
        shared_cache = FakeSharedCache()
        self.client._sdk_client.nqe.queries = Mock(
            return_value=[
                SimpleNamespace(
                    query_id="Q_devices",
                    path="/netbox/forward_devices",
                    intent="",
                )
            ]
        )
        self.client._sdk_client.nqe.repo.queries = Mock(
            return_value=[
                SimpleNamespace(
                    query_id="Q_devices",
                    path="/netbox/forward_devices",
                    intent="",
                    last_commit_id="commit-2",
                    last_commit=None,
                    source_code=None,
                )
            ]
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
        # Both the org index and the resolved commit are cached, so the
        # second resolve makes no further SDK calls.
        self.assertEqual(self.client._sdk_client.nqe.queries.call_count, 1)
        self.assertEqual(self.client._sdk_client.nqe.repo.queries.call_count, 1)

    def test_add_org_nqe_query_creates_user_workspace_change(self):
        self.client._sdk_client.nqe.repo.stage_add = Mock()

        self.client.add_org_nqe_query(
            query_path="netbox/forward_devices",
            source_code="select {}",
        )

        self.client._sdk_client.nqe.repo.stage_add.assert_called_once_with(
            "/netbox/forward_devices", "select {}"
        )

    def test_nqe_library_write_permission_accepts_org_admin(self):
        self.client._sdk_client.user_accounts.get_current_user = Mock(
            return_value=SimpleNamespace(
                roles=SimpleNamespace(org=["ADMIN"], network={})
            )
        )

        self.assertTrue(self.client.has_nqe_library_write_permission())
        self.client._sdk_client.user_accounts.get_current_user.assert_called_once_with()

    def test_nqe_library_write_permission_accepts_selected_network_operator(self):
        self.client.source.parameters["network_id"] = "network-1"
        self.client._sdk_client.user_accounts.get_current_user = Mock(
            return_value=SimpleNamespace(
                roles=SimpleNamespace(org=[], network={"network-1": "OPERATOR"})
            )
        )

        self.assertTrue(self.client.has_nqe_library_write_permission())

    def test_nqe_library_write_permission_rejects_read_only_role(self):
        self.client.source.parameters["network_id"] = "network-1"
        self.client._sdk_client.user_accounts.get_current_user = Mock(
            return_value=SimpleNamespace(
                roles=SimpleNamespace(org=[], network={"network-1": "OBSERVER"})
            )
        )

        self.assertFalse(self.client.has_nqe_library_write_permission())

    def test_edit_org_nqe_query_uses_existing_query_basis(self):
        self.client._sdk_client.nqe.repo.stage_edit = Mock()

        self.client.edit_org_nqe_query(
            query_path="/netbox/forward_devices",
            source_code="select {}",
            query_id="OQ_devices",
            commit_id="commit-1",
        )

        self.client._sdk_client.nqe.repo.stage_edit.assert_called_once_with(
            "/netbox/forward_devices",
            "select {}",
            query_id="OQ_devices",
            commit_id="commit-1",
        )

    def test_commit_org_nqe_queries_commits_paths_and_returns_head(self):
        from forward_sdk.nqe.repository import CommitReport

        self.client._sdk_client.nqe.repo.commit = Mock(
            return_value=CommitReport(
                committed_paths=("/netbox/forward_devices",), commit_id="commit-2"
            )
        )

        commit_id = self.client.commit_org_nqe_queries(
            query_paths=["netbox/forward_devices"],
            message="Publish test queries",
        )

        self.assertEqual(commit_id, "commit-2")
        self.client._sdk_client.nqe.repo.commit.assert_called_once_with(
            ["/netbox/forward_devices"], title="Publish test queries", body=""
        )

    def test_commit_org_nqe_queries_reflects_paths_the_sdk_skipped_as_unchanged(self):
        # NqeRepository.commit does its own no-staged-changes retry internally
        # now (dropping paths Forward's 409 INVALID_CHANGE_PATH names as
        # unchanged) - this method only has to trust the CommitReport it
        # gets back, not re-parse the error itself.
        from forward_sdk.nqe.repository import CommitReport

        self.client._sdk_client.nqe.repo.commit = Mock(
            return_value=CommitReport(
                committed_paths=("/netbox/forward_interfaces",),
                skipped_paths=("/netbox/forward_devices",),
                commit_id="commit-2",
            )
        )

        commit_id = self.client.commit_org_nqe_queries(
            query_paths=["/netbox/forward_devices", "/netbox/forward_interfaces"],
            message="Publish test queries",
        )

        self.assertEqual(commit_id, "commit-2")
        self.client._sdk_client.nqe.repo.commit.assert_called_once_with(
            ["/netbox/forward_devices", "/netbox/forward_interfaces"],
            title="Publish test queries",
            body="",
        )

    def test_commit_org_nqe_queries_falls_back_to_head_commit_when_everything_skipped(
        self,
    ):
        from forward_sdk.nqe.repository import CommitReport

        self.client._sdk_client.nqe.repo.commit = Mock(
            return_value=CommitReport(skipped_paths=("/netbox/forward_devices",))
        )
        self.client._sdk_client.nqe.repo.head_commit_id = Mock(return_value="commit-1")

        commit_id = self.client.commit_org_nqe_queries(
            query_paths=["/netbox/forward_devices"],
            message="Publish test queries",
        )

        self.assertEqual(commit_id, "commit-1")
        self.client._sdk_client.nqe.repo.head_commit_id.assert_called_once_with()

    def test_commit_org_nqe_queries_translates_an_sdk_exception(self):
        from forward_sdk.errors import ForwardServerError

        self.client._sdk_client.nqe.repo.commit = Mock(
            side_effect=ForwardServerError(
                "failed with HTTP 500: server error", status=500, text="server error"
            )
        )

        with self.assertRaises(ForwardClientError) as ctx:
            self.client.commit_org_nqe_queries(
                query_paths=["/netbox/forward_devices"],
                message="Publish test queries",
            )

        self.assertIn("HTTP 500", str(ctx.exception))

    def test_nqe_mutations_invalidate_and_repopulate_cached_head_commit(self):
        shared_cache = FakeSharedCache()
        from forward_sdk.nqe.repository import CommitReport

        self.client._sdk_client.nqe.repo.head_commit_id = Mock(return_value="commit-1")
        self.client._sdk_client.nqe.repo.commit = Mock(
            return_value=CommitReport(
                committed_paths=("/netbox/forward_devices",), commit_id="commit-2"
            )
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
        self.client._sdk_client.nqe.repo.head_commit_id.assert_called_once_with()

    def _fake_diff_page(self, rows, total=None):
        return SimpleNamespace(
            rows=[SimpleNamespace(type=t, before=b, after=a) for t, b, a in rows],
            total_num_rows=total,
        )

    def test_run_nqe_diff_returns_single_page_by_default(self):
        self.client._sdk_client.nqe.diff_page = Mock(
            return_value=self._fake_diff_page(
                [("ADDED", None, {"n": 1}), ("DELETED", {"n": 2}, None)], total=2
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
        self.client._sdk_client.nqe.diff_page.assert_called_once()
        call = self.client._sdk_client.nqe.diff_page.call_args
        self.assertEqual(call.kwargs["before"], "snapshot-before")
        self.assertEqual(call.kwargs["after"], "snapshot-after")
        self.assertEqual(call.kwargs["limit"], 10000)
        ref = call.args[0]
        self.assertEqual(ref.query_id, "Q_sites")
        self.assertIsNone(ref.commit_id)

    def test_run_nqe_diff_fetch_all_pages_until_total_num_rows(self):
        self.client._sdk_client.nqe.diff_page = Mock(
            side_effect=[
                self._fake_diff_page(
                    [("ADDED", None, {"n": 1}), ("ADDED", None, {"n": 2})], total=3
                ),
                self._fake_diff_page([("DELETED", {"n": 3}, None)], total=3),
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
        self.assertEqual(
            [
                call.kwargs["offset"]
                for call in self.client._sdk_client.nqe.diff_page.call_args_list
            ],
            [0, 2],
        )
        self.assertEqual(self.client.api_usage_summary()["nqe_diff_calls"], 1)
        self.assertEqual(self.client.api_usage_summary()["nqe_pages"], 2)
        self.assertEqual(self.client.api_usage_summary()["nqe_diff_pages"], 2)

    def test_run_nqe_diff_fetch_all_raises_if_api_ends_early(self):
        self.client._sdk_client.nqe.diff_page = Mock(
            side_effect=[
                self._fake_diff_page(
                    [("ADDED", None, {"n": 1}), ("ADDED", None, {"n": 2})], total=5
                ),
                self._fake_diff_page([], total=5),
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

    def test_run_nqe_diff_fetch_all_raises_when_page_limit_exceeded(self):
        self.client.nqe_fetch_all_max_pages = 2
        self.client._sdk_client.nqe.diff_page = Mock(
            side_effect=[
                self._fake_diff_page([("ADDED", None, {"n": 1})]),
                self._fake_diff_page([("ADDED", None, {"n": 2})]),
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
        self.assertEqual(self.client._sdk_client.nqe.diff_page.call_count, 2)

    def test_run_nqe_diff_fetch_all_raises_on_identical_full_pages(self):
        self.client.nqe_fetch_all_max_pages = 10
        self.client.nqe_identical_full_page_streak_limit = 2
        repeated_page = [("ADDED", None, {"n": 1}), ("DELETED", {"n": 2}, None)]
        self.client._sdk_client.nqe.diff_page = Mock(
            side_effect=[
                self._fake_diff_page(repeated_page),
                self._fake_diff_page(repeated_page),
                self._fake_diff_page(repeated_page),
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
        self.assertEqual(self.client._sdk_client.nqe.diff_page.call_count, 3)

    def test_run_nqe_diff_passes_the_commit_id_through(self):
        self.client._sdk_client.nqe.diff_page = Mock(
            return_value=self._fake_diff_page([("ADDED", None, {"n": 1})], total=1)
        )

        self.client.run_nqe_diff(
            query_id="Q_sites",
            commit_id="commit-1",
            before_snapshot_id="snapshot-before",
            after_snapshot_id="snapshot-after",
        )

        ref = self.client._sdk_client.nqe.diff_page.call_args.args[0]
        self.assertEqual(ref.query_id, "Q_sites")
        self.assertEqual(ref.commit_id, "commit-1")

    def test_run_nqe_diff_rejects_a_non_json_item_format(self):
        with self.assertRaisesRegex(
            ForwardClientError,
            "NQE diff only supports JSON item format.",
        ):
            self.client.run_nqe_diff(
                query_id="Q_sites",
                before_snapshot_id="snapshot-before",
                after_snapshot_id="snapshot-after",
                item_format="CSV",
            )

    def test_run_nqe_diff_honors_an_already_exceeded_deadline_before_the_first_page(
        self,
    ):
        self.client._sdk_client.nqe.diff_page = Mock()

        with self.assertRaises(ForwardFetchBudgetExceededError):
            self.client.run_nqe_diff(
                query_id="Q_sites",
                before_snapshot_id="snapshot-before",
                after_snapshot_id="snapshot-after",
                deadline=time.monotonic() - 1.0,
            )
        self.client._sdk_client.nqe.diff_page.assert_not_called()


class WorkloadFetchBudgetTest(TestCase):
    """A slow workload cannot silently hang a multi-hour sync."""

    def test_budget_error_is_not_transient(self):
        from forward_netbox.exceptions import (
            ForwardConnectivityError,
            ForwardFetchBudgetExceededError,
        )
        from forward_netbox.utilities.query_fetch_execution import (
            _is_transient_fetch_error,
        )

        # A budget breach must fall straight through to failure, never retry.
        self.assertFalse(
            _is_transient_fetch_error(ForwardFetchBudgetExceededError("slow"))
        )
        # Sanity: a real transient error still retries.
        self.assertTrue(_is_transient_fetch_error(ForwardConnectivityError("boom")))

    def test_timeout_seconds_reads_source_param_and_defaults_off(self):
        from types import MethodType

        from forward_netbox.utilities.query_fetch_execution import (
            DEFAULT_WORKLOAD_FETCH_TIMEOUT_SECONDS,
            ForwardQueryFetcher,
        )

        probe = SimpleNamespace()
        probe._workload_fetch_timeout_seconds = MethodType(
            ForwardQueryFetcher._workload_fetch_timeout_seconds, probe
        )
        probe.sync = SimpleNamespace(
            source=SimpleNamespace(parameters={"workload_fetch_timeout_seconds": 900})
        )
        self.assertEqual(probe._workload_fetch_timeout_seconds(), 900)
        probe.sync = SimpleNamespace(source=SimpleNamespace(parameters={}))
        self.assertEqual(
            probe._workload_fetch_timeout_seconds(),
            DEFAULT_WORKLOAD_FETCH_TIMEOUT_SECONDS,
        )

    def test_async_wait_clamps_an_already_passed_deadline_to_a_zero_timeout(self):
        import time

        client = ForwardClient(
            SimpleNamespace(
                url="https://fwd.app",
                parameters={
                    "username": "u@example.com",
                    "password": encrypt_secret("x"),
                    "verify": True,
                },
            )
        )
        execution = Mock()
        past = time.monotonic() - 1

        client._wait_for_nqe_execution(execution, deadline=past)

        # `NqeExecution.wait()` treats `timeout=0` as "already out of time" and
        # raises `ForwardTimeoutError` itself - clamped rather than negative
        # is what this client is responsible for getting right.
        execution.wait.assert_called_once()
        self.assertEqual(execution.wait.call_args.kwargs["timeout"], 0.0)
