# A customer's cable query ran past Forward's own NQE time limit as one
# execution over ~3,500 devices, so the model failed and cables were not
# refreshed. A time-limited outcome is not retried as is (it would spend the
# whole budget again); a device-scoped full query is fetched again in halves of
# its device list and the parts merged.
#
# Two groups of tests: the first replaces `_fetch_spec_rows` and pins the
# splitter's own bookkeeping; the second runs the real `_fetch_spec_rows` and
# `_execute_workload_job` with only Forward replaced.
import time
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import Mock
from unittest.mock import patch

from django.test import SimpleTestCase

from forward_netbox.choices import ForwardDiffFallbackModeChoices
from forward_netbox.exceptions import ForwardClientError
from forward_netbox.exceptions import ForwardConnectivityError
from forward_netbox.exceptions import ForwardQueryError
from forward_netbox.utilities import query_fetch_execution
from forward_netbox.utilities.query_execution_contract import query_source_sha256
from forward_netbox.utilities.query_execution_contract import resolve_execution_contract
from forward_netbox.utilities.query_fetch_execution import _FULL_PATH_META_ATTR
from forward_netbox.utilities.query_fetch_execution import _PART_RETRIES_EXHAUSTED_ATTR
from forward_netbox.utilities.query_fetch_execution import ForwardQueryFetcher
from forward_netbox.utilities.query_registry import QuerySpec
from forward_netbox.utilities.tier3_reducers import TIER3_REDUCER_IDS

SOURCE = """
@query
f(forward_netbox_shard_keys: List<String>) =
foreach device in network.devices
where isEmpty(forward_netbox_shard_keys) || device.name in forward_netbox_shard_keys
select {device: device.name, name: "eth0"}
"""


def _contract(keys, model_string="dcim.interface"):
    spec = QuerySpec(
        model_string=model_string,
        query_name="Forward Interfaces",
        query_id="Q_interfaces",
        query_repository="org",
        resolved_query_path="/Root/forward_interfaces",
        commit_id=None,
        map_id=3,
        built_in=True,
        contract_key="forward_interfaces",
        full_query_source=SOURCE,
        full_source_sha256=query_source_sha256(SOURCE),
        parameters={"forward_netbox_shard_keys": []},
        coalesce_fields=(("device", "name"),),
    )
    return resolve_execution_contract(
        spec, effective_parameters={"forward_netbox_shard_keys": list(keys)}
    )


def _timed_out(*, full_path=True):
    """Forward's time-limit error; ``full_path`` marks it as raised by the full
    execution (what `_fetch_spec_rows` does), not by a diff or contributor fetch."""
    error = ForwardClientError(
        "Forward NQE execution finished with outcome time limit reached."
    )
    error.outcome = "TIMED_OUT"
    if full_path:
        setattr(error, _FULL_PATH_META_ATTR, {"fetch_mode": "model"})
    return error


class SplitOnTimeLimitTest(SimpleTestCase):
    def _fetcher(self, *, limit):
        """A fetcher whose Forward answers only for at most ``limit`` devices."""
        fetcher = ForwardQueryFetcher(
            sync=SimpleNamespace(source=SimpleNamespace(parameters={}), parameters={}),
            client=Mock(),
            logger_=Mock(),
        )
        fetcher.calls = []

        def fetch(model, contract, baseline, context, coalesce, **kwargs):
            keys = list(contract.full_effective_parameters["forward_netbox_shard_keys"])
            fetcher.calls.append((keys, kwargs["skip_full_run_removals"]))
            if len(keys) > limit:
                raise _timed_out()
            rows = [{"device": key, "name": "eth0"} for key in keys]
            # A row both halves return (a cable between them) is kept once.
            rows.append({"device": "shared", "name": "eth0"})
            return rows, [], "full", {"matched_type_rows_skipped": 1}

        fetcher._fetch_spec_rows = fetch
        fetcher._full_run_removals = Mock(return_value=[{"device": "gone"}])
        return fetcher

    def _run(self, fetcher, keys, **kwargs):
        return fetcher._fetch_spec_rows_splitting_on_time_limit(
            "dcim.interface",
            _contract(keys),
            None,
            Mock(),
            [["device", "name"]],
            **kwargs,
        )

    def test_a_timed_out_query_is_fetched_in_halves_and_merged(self):
        fetcher = self._fetcher(limit=2)
        keys = ["a", "b", "c", "d"]

        rows, deletes, mode, meta = self._run(fetcher, keys)

        self.assertEqual(
            sorted(row["device"] for row in rows), ["a", "b", "c", "d", "shared"]
        )
        self.assertEqual(mode, "full")
        # The workload is recorded against the whole device list.
        self.assertEqual(meta["query_parameters"]["forward_netbox_shard_keys"], keys)
        self.assertEqual(meta["matched_type_rows_skipped"], 2)
        # Removals are found once, on the whole result, never per part.
        self.assertEqual([skip for _keys, skip in fetcher.calls[1:]], [True, True])
        fetcher._full_run_removals.assert_called_once()
        self.assertEqual(
            len(fetcher._full_run_removals.call_args.kwargs["current_rows"]), 5
        )
        self.assertEqual(deletes, [{"device": "gone"}])

    def test_it_keeps_halving_until_each_part_fits(self):
        fetcher = self._fetcher(limit=1)

        rows, _deletes, _mode, _meta = self._run(fetcher, ["a", "b", "c", "d"])

        self.assertEqual(len(rows), 5)
        self.assertEqual(
            [keys for keys, _skip in fetcher.calls if len(keys) == 1],
            [["a"], ["b"], ["c"], ["d"]],
        )

    def test_a_part_that_still_times_out_at_the_depth_limit_fails_the_model(self):
        fetcher = self._fetcher(limit=0)

        with self.assertRaises(ForwardClientError):
            self._run(fetcher, [str(n) for n in range(64)])

    def test_other_failures_are_not_split(self):
        fetcher = self._fetcher(limit=10)
        fetcher._fetch_spec_rows = Mock(side_effect=ForwardClientError("HTTP 500"))

        with self.assertRaises(ForwardClientError):
            self._run(fetcher, ["a", "b"])
        self.assertEqual(fetcher._fetch_spec_rows.call_count, 1)

    def test_only_an_error_from_the_full_execution_is_split(self):
        # A diff-eligible contract is split when its FULL execution timed out
        # (the diff was blocked or fell back); an error from the diff or a
        # contributor fetch carries no mark and is never split.
        diff_eligible = replace(_contract(["a", "b"]), diff_eligible=True)
        self.assertEqual(
            ForwardQueryFetcher._time_limit_split_keys(
                _timed_out(), diff_eligible, None
            ),
            ["a", "b"],
        )
        self.assertIsNone(
            ForwardQueryFetcher._time_limit_split_keys(
                _timed_out(full_path=False), diff_eligible, None
            )
        )

    def test_addresses_are_never_split(self):
        # One winner per address across devices: halves would each pick one.
        for model in ("ipam.ipaddress", "dcim.macaddress"):
            self.assertIsNone(
                ForwardQueryFetcher._time_limit_split_keys(
                    _timed_out(), _contract(["a", "b"], model), None
                )
            )

    def test_a_fetch_that_already_carries_a_shard_is_not_split(self):
        self.assertIsNone(
            ForwardQueryFetcher._time_limit_split_keys(
                _timed_out(), _contract(["a", "b"]), {"fetch_mode": "model"}
            )
        )

    def test_a_contributor_contract_is_not_split(self):
        tier3 = replace(
            _contract(["a", "b"]),
            diff_eligible=True,
            reducer_id=sorted(TIER3_REDUCER_IDS)[0],
        )
        self.assertIsNone(
            ForwardQueryFetcher._time_limit_split_keys(_timed_out(), tier3, None)
        )


def _context():
    return SimpleNamespace(
        snapshot_id="snapshot",
        network_id="network",
        ingestion_id=None,
        scoped_device_names=set(),
        scoped_site_names=set(),
        device_tag_include_tags=(),
        device_tag_exclude_tags=(),
        device_tag_prune_out_of_scope=False,
        query_parameters={},
    )


def _transient():
    return ForwardConnectivityError("connection reset by peer")


class ForwardStubMixin:
    """The real `_fetch_spec_rows` and `_execute_workload_job`, with only Forward
    replaced: `run_nqe_query` answers for at most ``limit`` devices (a larger list
    hits the NQE time limit), `_run_nqe_diff` is scripted, and
    `_full_run_removals` is a spy."""

    COALESCE = [["device", "name"]]

    def setUp(self):
        super().setUp()
        sync = Mock()
        sync.source = SimpleNamespace(
            parameters={"workload_fetch_retry_backoff_seconds": 0}
        )
        sync.parameters = {}
        sync.latest_baseline_ingestion.return_value = None
        self.fetcher = ForwardQueryFetcher(sync=sync, client=Mock(), logger_=Mock())
        self.fetcher._full_run_removals = Mock(return_value=[])
        self.limit = 2
        self.calls = []
        # keys (tuple) -> errors to raise, one per call, before answering.
        self.failures = {}
        # keys (tuple) -> callable run when Forward is asked for those keys.
        self.hooks = {}
        self.diff = Mock(side_effect=ForwardClientError("diff failed"))
        self.fetcher._run_nqe_diff = self.diff
        patcher = patch.object(
            query_fetch_execution, "run_nqe_query", side_effect=self._forward
        )
        patcher.start()
        self.addCleanup(patcher.stop)

    def _forward(self, _client, **kwargs):
        keys = list(kwargs["parameters"]["forward_netbox_shard_keys"])
        self.calls.append(keys)
        hook = self.hooks.get(tuple(keys))
        if hook:
            hook()
        queued = self.failures.get(tuple(keys))
        if queued:
            raise queued.pop(0)
        if len(keys) > self.limit:
            raise _timed_out(full_path=False)
        return [{"device": key, "name": "eth0"} for key in keys]

    def _calls_for(self, *keys):
        return [call for call in self.calls if call == list(keys)]

    def _split(self, keys, *, contract=None, baseline=None, **kwargs):
        return self.fetcher._fetch_spec_rows_splitting_on_time_limit(
            "dcim.interface",
            contract or _contract(keys),
            baseline,
            _context(),
            self.COALESCE,
            **kwargs,
        )


class RealFetchSplitTest(ForwardStubMixin, SimpleTestCase):
    def test_real_fetch_leaves_the_removal_comparison_to_the_caller(self):
        # A part holds part of the model; comparing it alone would read the rest
        # as removed. `skip_full_run_removals` is what the splitter passes parts.
        contract = _contract(["a", "b"])
        self.fetcher._fetch_spec_rows(
            "dcim.interface",
            contract,
            None,
            _context(),
            self.COALESCE,
            skip_full_run_removals=True,
        )
        self.fetcher._full_run_removals.assert_not_called()

        self.fetcher._fetch_spec_rows(
            "dcim.interface", contract, None, _context(), self.COALESCE
        )
        self.fetcher._full_run_removals.assert_called_once()

    def test_a_split_runs_the_removal_comparison_once_on_the_merged_rows(self):
        rows, _deletes, mode, meta = self._split(["a", "b", "c", "d"])

        self.assertEqual(sorted(row["device"] for row in rows), ["a", "b", "c", "d"])
        self.assertEqual(mode, "full")
        self.assertEqual(
            meta["query_parameters"]["forward_netbox_shard_keys"],
            ["a", "b", "c", "d"],
        )
        self.fetcher._full_run_removals.assert_called_once()
        self.assertEqual(
            len(self.fetcher._full_run_removals.call_args.kwargs["current_rows"]), 4
        )

    def test_a_transient_error_in_one_part_retries_that_part_only(self):
        self.failures[("c", "d")] = [_transient()]

        rows, _deletes, _mode, _meta = self._split(["a", "b", "c", "d"])

        self.assertEqual(sorted(row["device"] for row in rows), ["a", "b", "c", "d"])
        # The whole list timed out once and is never fetched again; the part
        # that failed is fetched twice; the other part, already in hand, once.
        self.assertEqual(len(self._calls_for("a", "b", "c", "d")), 1)
        self.assertEqual(len(self._calls_for("c", "d")), 2)
        self.assertEqual(len(self._calls_for("a", "b")), 1)
        self.fetcher._full_run_removals.assert_called_once()

    def test_a_transient_error_deep_in_the_split_retries_only_that_part(self):
        self.limit = 1
        self.failures[("b",)] = [_transient()]

        rows, _deletes, _mode, _meta = self._split(["a", "b", "c", "d"])

        self.assertEqual(sorted(row["device"] for row in rows), ["a", "b", "c", "d"])
        self.assertEqual(len(self._calls_for("b")), 2)
        for other in ("a", "c", "d"):
            self.assertEqual(len(self._calls_for(other)), 1)
        self.assertEqual(len(self._calls_for("a", "b")), 1)

    def test_a_part_retries_no_more_than_the_workload_retry_config_allows(self):
        # Defaults: 2 retries, so 3 attempts; the error is then marked so the
        # workload loop does not restart the whole workload.
        self.failures[("c", "d")] = [_transient(), _transient(), _transient()]

        with self.assertRaises(ForwardConnectivityError) as caught:
            self._split(["a", "b", "c", "d"])

        self.assertEqual(len(self._calls_for("c", "d")), 3)
        self.assertTrue(getattr(caught.exception, _PART_RETRIES_EXHAUSTED_ATTR))

    def test_a_permanent_error_in_a_part_fails_the_model_without_a_retry(self):
        self.failures[("c", "d")] = [ForwardClientError("HTTP 400 bad request")]

        with self.assertRaises(ForwardClientError):
            self._split(["a", "b", "c", "d"])

        self.assertEqual(len(self._calls_for("c", "d")), 1)
        self.fetcher._full_run_removals.assert_not_called()

    def test_a_part_that_times_out_is_split_further_never_retried_as_is(self):
        self.limit = 1

        rows, _deletes, _mode, _meta = self._split(["a", "b", "c", "d"])

        self.assertEqual(len(rows), 4)
        for keys in (("a", "b", "c", "d"), ("a", "b"), ("c", "d")):
            self.assertEqual(len(self._calls_for(*keys)), 1)

    def test_a_retry_waits_the_workload_backoff_and_honours_the_deadline(self):
        self.fetcher.sync.source.parameters["workload_fetch_retry_backoff_seconds"] = 5
        self.failures[("c", "d")] = [_transient()]
        with patch.object(query_fetch_execution.time, "sleep") as sleep:
            self._split(["a", "b", "c", "d"], deadline=time.monotonic() + 3600)
        sleep.assert_called_once_with(5)

        # A part is not retried once the workload deadline has passed.
        self.calls.clear()
        clock = [0.0]
        self.hooks[("c", "d")] = lambda: clock.__setitem__(0, 500.0)
        self.failures[("c", "d")] = [_transient()]
        with patch.object(
            query_fetch_execution.time, "monotonic", side_effect=lambda: clock[0]
        ):
            with self.assertRaises(ForwardConnectivityError):
                self._split(["a", "b", "c", "d"], deadline=100.0)
        self.assertEqual(len(self._calls_for("c", "d")), 1)

    def test_a_diff_eligible_contract_with_a_baseline_is_split_when_its_full_fallback_times_out(
        self,
    ):
        contract = replace(_contract(["a", "b", "c", "d"]), diff_eligible=True)
        baseline = SimpleNamespace(snapshot_id="earlier")

        rows, _deletes, mode, meta = self._split(
            ["a", "b", "c", "d"], contract=contract, baseline=baseline
        )

        self.assertEqual(sorted(row["device"] for row in rows), ["a", "b", "c", "d"])
        self.assertEqual(mode, "full")
        # The diff ran once, for the whole list; no part was fetched as a diff.
        self.diff.assert_called_once()
        # A diff that fell back to full still reads as one.
        self.assertEqual(meta["fetch_mode"], "diff_fallback")
        self.assertIn("fallback_reason", meta["fetch_parameters"])
        self.fetcher._full_run_removals.assert_called_once()

    def test_a_diff_that_was_blocked_leaves_the_full_path_to_split(self):
        contract = replace(_contract(["a", "b", "c", "d"]), diff_eligible=True)
        baseline = SimpleNamespace(snapshot_id="earlier")
        self.fetcher._diff_circuit_is_open = Mock(return_value=True)

        rows, _deletes, _mode, meta = self._split(
            ["a", "b", "c", "d"], contract=contract, baseline=baseline
        )

        self.assertEqual(len(rows), 4)
        self.diff.assert_not_called()
        self.assertEqual(
            meta["fetch_parameters"]["fallback_reason"],
            query_fetch_execution.DIFF_CIRCUIT_OPEN_FALLBACK_REASON,
        )

    def test_a_diff_that_timed_out_falls_back_and_only_the_full_attempt_splits(self):
        contract = replace(_contract(["a", "b", "c", "d"]), diff_eligible=True)
        baseline = SimpleNamespace(snapshot_id="earlier")
        self.diff.side_effect = _timed_out(full_path=False)

        rows, _deletes, mode, _meta = self._split(
            ["a", "b", "c", "d"], contract=contract, baseline=baseline
        )

        self.assertEqual(len(rows), 4)
        self.assertEqual(mode, "full")
        self.diff.assert_called_once()

    def test_a_diff_timeout_whose_full_fallback_fits_is_not_split(self):
        self.limit = 10
        contract = replace(_contract(["a", "b", "c", "d"]), diff_eligible=True)
        self.diff.side_effect = _timed_out(full_path=False)

        rows, _deletes, mode, _meta = self._split(
            ["a", "b", "c", "d"],
            contract=contract,
            baseline=SimpleNamespace(snapshot_id="earlier"),
        )

        self.assertEqual(mode, "full")
        self.assertEqual(len(rows), 4)
        self.assertEqual(self.calls, [["a", "b", "c", "d"]])

    def test_a_diff_timeout_is_never_split_when_diff_is_required(self):
        self.fetcher.sync.parameters = {
            "diff_fallback_mode": ForwardDiffFallbackModeChoices.REQUIRE_DIFF
        }
        self.diff.side_effect = _timed_out(full_path=False)
        contract = replace(_contract(["a", "b", "c", "d"]), diff_eligible=True)

        with self.assertRaises(ForwardQueryError):
            self._split(
                ["a", "b", "c", "d"],
                contract=contract,
                baseline=SimpleNamespace(snapshot_id="earlier"),
            )

        self.assertEqual(self.calls, [])

    def test_a_contributor_fetch_that_timed_out_is_not_split(self):
        contract = replace(
            _contract(["a", "b", "c", "d"]),
            diff_eligible=True,
            diff_ownership_mode="contributor_relation",
            reducer_id=sorted(TIER3_REDUCER_IDS)[0],
        )
        self.fetcher._fetch_tier3_contributor_rows = Mock(
            side_effect=_timed_out(full_path=False)
        )

        with self.assertRaises(ForwardClientError):
            self._split(
                ["a", "b", "c", "d"],
                contract=contract,
                baseline=SimpleNamespace(snapshot_id="earlier"),
            )

        self.fetcher._fetch_tier3_contributor_rows.assert_called_once()
        self.assertEqual(self.calls, [])
        self.assertTrue(self.fetcher._contributor_staging_blocked)

    def test_the_timed_out_execution_cap_stops_the_descent(self):
        self.limit = 1
        keys = [str(n) for n in range(8)]

        with patch.object(
            query_fetch_execution, "TIME_LIMIT_SPLIT_MAX_TIMED_OUT_EXECUTIONS", 2
        ):
            with self.assertRaises(ForwardClientError) as caught:
                self._split(keys)

        # The original time-limit error stands: 8 devices, then 4, then 2 (the
        # third timeout is over the cap and is not split).
        self.assertEqual(caught.exception.outcome, "TIMED_OUT")
        self.assertEqual([len(call) for call in self.calls], [8, 4, 2])
        spent = self.fetcher.logger.log_warning.call_args.args[0]
        self.assertIn("3 time-limited execution(s) spent", spent)

    def test_a_nearly_spent_workload_deadline_stops_the_descent(self):
        with self.assertRaises(ForwardClientError) as caught:
            self._split(
                ["a", "b", "c", "d"],
                deadline=time.monotonic()
                + query_fetch_execution.TIME_LIMIT_SPLIT_MIN_REMAINING_SECONDS / 2,
            )

        self.assertEqual(caught.exception.outcome, "TIMED_OUT")
        self.assertEqual(self.calls, [["a", "b", "c", "d"]])

    def test_a_deadline_with_time_left_still_splits(self):
        rows, _deletes, _mode, _meta = self._split(
            ["a", "b", "c", "d"], deadline=time.monotonic() + 3600
        )

        self.assertEqual(len(rows), 4)

    def test_the_split_reports_what_it_spent(self):
        self.limit = 1

        _rows, _deletes, _mode, meta = self._split(["a", "b", "c", "d"])

        self.assertEqual(meta["time_limit_timed_out_executions"], 3)
        logged = [call.args[0] for call in self.fetcher.logger.log_info.call_args_list]
        self.assertTrue(any("3 time-limited execution(s)" in line for line in logged))


class WorkloadJobSplitTest(ForwardStubMixin, SimpleTestCase):
    """Through `_execute_workload_job`, the retry loop around the split."""

    def _job(self, keys):
        payload = (
            _context(),
            False,
            ("dcim.interface", _contract(keys), None, self.COALESCE, None),
        )
        decision = SimpleNamespace(
            selected_engine="adapter", reason="test", as_dict=lambda: {}
        )
        with patch.object(
            query_fetch_execution, "apply_engine_decision_for", return_value=decision
        ):
            return self.fetcher._execute_workload_job(payload)

    def test_a_split_workload_is_recorded_against_the_whole_device_list(self):
        result, workload = self._job(["a", "b", "c", "d"])

        self.assertEqual(result.failure_count, 0)
        self.assertEqual(result.row_count, 4)
        self.assertEqual(
            workload.query_parameters["forward_netbox_shard_keys"],
            ["a", "b", "c", "d"],
        )

    def test_a_part_that_exhausted_its_retries_does_not_restart_the_workload(self):
        self.failures[("c", "d")] = [_transient() for _ in range(10)]

        result, workload = self._job(["a", "b", "c", "d"])

        self.assertEqual(result.failure_count, 1)
        self.assertIsNone(workload)
        # The time limit is paid once: the whole list is not submitted again.
        self.assertEqual(len(self._calls_for("a", "b", "c", "d")), 1)
        self.assertEqual(len(self._calls_for("c", "d")), 3)

    def test_a_transient_error_in_one_part_is_absorbed_by_the_part(self):
        self.failures[("c", "d")] = [_transient()]

        result, workload = self._job(["a", "b", "c", "d"])

        self.assertEqual(result.failure_count, 0)
        self.assertEqual(len(workload.upsert_rows), 4)
        self.assertEqual(len(self._calls_for("a", "b", "c", "d")), 1)

    def test_a_transient_error_before_any_split_still_restarts_the_workload(self):
        # Unchanged: outside a split the workload loop retries the whole fetch.
        self.limit = 10
        self.failures[("a", "b", "c", "d")] = [_transient()]

        result, _workload = self._job(["a", "b", "c", "d"])

        self.assertEqual(result.failure_count, 0)
        self.assertEqual(len(self._calls_for("a", "b", "c", "d")), 2)
