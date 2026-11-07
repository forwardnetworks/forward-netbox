# A customer's cable query ran past Forward's own NQE time limit as one
# execution over ~3,500 devices, so the model failed and cables were not
# refreshed. A time-limited outcome is not retried as is (it would spend the
# whole budget again); a device-scoped full query is fetched again in halves of
# its device list and the parts merged.
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import Mock

from django.test import SimpleTestCase

from forward_netbox.exceptions import ForwardClientError
from forward_netbox.utilities.query_execution_contract import query_source_sha256
from forward_netbox.utilities.query_execution_contract import resolve_execution_contract
from forward_netbox.utilities.query_fetch_execution import ForwardQueryFetcher
from forward_netbox.utilities.query_registry import QuerySpec

SOURCE = """
@query
f(forward_netbox_shard_keys: List<String>) =
foreach device in network.devices
where isEmpty(forward_netbox_shard_keys) || device.name in forward_netbox_shard_keys
select {device: device.name, name: "eth0"}
"""


def _contract(keys):
    spec = QuerySpec(
        model_string="dcim.interface",
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


def _timed_out():
    error = ForwardClientError(
        "Forward NQE execution finished with outcome time limit reached."
    )
    error.outcome = "TIMED_OUT"
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

    def test_a_diff_fetch_with_a_baseline_is_not_split(self):
        contract = _contract(["a", "b"])
        diff = replace(contract, diff_eligible=True)
        self.assertIsNone(
            ForwardQueryFetcher._time_limit_split_keys(
                _timed_out(), diff, SimpleNamespace(snapshot_id="1"), None
            )
        )
        self.assertEqual(
            ForwardQueryFetcher._time_limit_split_keys(
                _timed_out(), contract, None, None
            ),
            ["a", "b"],
        )
