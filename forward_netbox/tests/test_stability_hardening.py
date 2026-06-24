# Regression tests for the workload-fetch stability hardening.
#
# Covers the transient workload-fetch retry (so a transient NQE failure does not
# fail staging) and the transient-error classifier. Pure / mock-based, no DB.
from types import SimpleNamespace
from unittest import TestCase
from unittest.mock import Mock
from unittest.mock import patch

from forward_netbox.exceptions import ForwardClientError
from forward_netbox.exceptions import ForwardConnectivityError
from forward_netbox.exceptions import ForwardQueryError
from forward_netbox.utilities.query_fetch_execution import _is_transient_fetch_error
from forward_netbox.utilities.query_fetch_execution import ForwardQueryFetcher


class IsTransientFetchErrorTest(TestCase):
    def test_connectivity_error_is_transient(self):
        self.assertTrue(_is_transient_fetch_error(ForwardConnectivityError("reset")))

    def test_query_error_is_never_transient(self):
        # Even with a transient-looking message: a bad/unpublished query won't
        # fix itself by retrying.
        self.assertFalse(_is_transient_fetch_error(ForwardQueryError("timeout 503")))

    def test_client_error_transient_token(self):
        self.assertTrue(
            _is_transient_fetch_error(ForwardClientError("503 Service Unavailable"))
        )
        self.assertTrue(_is_transient_fetch_error(ForwardClientError("read timed out")))

    def test_client_error_permanent(self):
        self.assertFalse(
            _is_transient_fetch_error(ForwardClientError("401 Unauthorized"))
        )


class ShardResolutionErrorTest(TestCase):
    def test_subclasses_core_sync_error(self):
        # Must subclass core SyncError so existing sync-error handling still
        # catches it, while remaining a distinct type for bounded stage retry.
        from core.exceptions import SyncError

        from forward_netbox.exceptions import ForwardShardResolutionError

        exc = ForwardShardResolutionError("claimed index 7")
        self.assertIsInstance(exc, SyncError)


class WorkloadFetchRetryTest(TestCase):
    def _fetcher(self, *, attempts=2):
        sync = SimpleNamespace(
            source=SimpleNamespace(
                parameters={
                    "workload_fetch_retry_attempts": attempts,
                    "workload_fetch_retry_backoff_seconds": 0,
                }
            ),
            parameters={},
        )
        return ForwardQueryFetcher(sync=sync, client=Mock(), logger_=Mock())

    def _payload(self):
        spec = SimpleNamespace(
            execution_value="Q_vlan",
            query_name="Forward VLANs",
            execution_mode="query_id",
        )
        baseline = SimpleNamespace(snapshot_id="snap-1")
        job = ("ipam.vlan", spec, baseline, [["protocol"]], None)
        context = SimpleNamespace(snapshot_id="snap-1")
        return (context, False, job)

    def test_transient_error_retries_then_fails_after_attempts(self):
        fetcher = self._fetcher(attempts=2)
        fetcher._fetch_spec_rows = Mock(
            side_effect=ForwardConnectivityError("connection reset")
        )
        sentinel = object()
        fetcher._failure_result = Mock(return_value=sentinel)
        result, workload = fetcher._run_workload_job(self._payload())
        # 1 initial + 2 retries = 3 attempts, then failure.
        self.assertEqual(fetcher._fetch_spec_rows.call_count, 3)
        self.assertIs(result, sentinel)
        self.assertIsNone(workload)

    def test_permanent_error_does_not_retry(self):
        fetcher = self._fetcher(attempts=2)
        fetcher._fetch_spec_rows = Mock(side_effect=ForwardQueryError("bad query"))
        fetcher._failure_result = Mock(return_value=object())
        fetcher._run_workload_job(self._payload())
        # ForwardQueryError is permanent: one attempt only.
        self.assertEqual(fetcher._fetch_spec_rows.call_count, 1)

    def test_transient_error_then_success(self):
        fetcher = self._fetcher(attempts=2)
        fetcher._fetch_spec_rows = Mock(
            side_effect=[
                ForwardConnectivityError("timeout"),
                ([], [], "full", {}),
            ]
        )
        sentinel_failure = object()
        fetcher._failure_result = Mock(return_value=sentinel_failure)
        decision = SimpleNamespace(
            selected_engine="bulk",
            reason="",
            as_dict=lambda: {},
        )
        with patch(
            "forward_netbox.utilities.query_fetch_execution."
            "apply_engine_decision_for",
            return_value=decision,
        ):
            result, workload = fetcher._run_workload_job(self._payload())
        self.assertEqual(fetcher._fetch_spec_rows.call_count, 2)
        self.assertIsNot(result, sentinel_failure)
        self.assertIsNone(workload)  # no rows -> no workload, but not a failure
