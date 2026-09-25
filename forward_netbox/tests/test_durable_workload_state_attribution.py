"""The durable-workload-state diagnostic is model-wide, attributed once.

`apply_durable_workload_deltas` consolidates every full-mode workload for a
model into one delta before this ever runs, so the numbers are genuinely
model-wide - but attaching the identical dict to every map sharing that model
reads as though each map carries its own state. Several built-in maps
commonly share a model (every `dcim.inventoryitem` map, for one), so this is
the same class of duplication `_dependency_model_result_summary`'s
`attribute_model_comparison` fixes for the comparison counts.
"""

from unittest.mock import Mock

from django.test import SimpleTestCase

from forward_netbox.utilities.query_fetch_execution import ForwardModelResult
from forward_netbox.utilities.query_fetch_execution import ForwardQueryFetcher


class DurableWorkloadStateAttributionTest(SimpleTestCase):
    def _fetcher(self, model_strings):
        fetcher = ForwardQueryFetcher(sync=Mock(), client=Mock(), logger_=Mock())
        fetcher.model_results = [
            ForwardModelResult(
                model_string=model_string,
                query_name=f"query-{index}",
                execution_mode="query_path",
                execution_value="",
                sync_mode="branching",
                row_count=0,
            )
            for index, model_string in enumerate(model_strings)
        ]
        return fetcher

    def _summary(self, model_string, **kw):
        base = dict(
            model=model_string,
            mode="delta",
            target_rows=306,
            upsert_rows=0,
            delete_rows=0,
            bootstrap_delete_rows=0,
            protected_delete_rows=0,
            tombstone_rows=0,
            unrepresented_peer=False,
            compressed_bytes=0,
        )
        base.update(kw)
        return base

    def test_the_first_map_for_a_model_gets_the_full_diagnostic(self):
        fetcher = self._fetcher(["dcim.inventoryitem", "dcim.inventoryitem"])
        fetcher._record_durable_workload_state_summaries(
            [self._summary("dcim.inventoryitem", target_rows=306)]
        )

        first = fetcher.model_results[0].diagnostics[0]
        self.assertEqual(first["type"], "durable_workload_state")
        self.assertEqual(first["target_row_count"], 306)
        self.assertNotIn("shared_with_sibling_map", first)

    def test_a_sibling_map_gets_a_marker_not_a_second_copy(self):
        fetcher = self._fetcher(["dcim.inventoryitem", "dcim.inventoryitem"])
        fetcher._record_durable_workload_state_summaries(
            [self._summary("dcim.inventoryitem", target_rows=306)]
        )

        second = fetcher.model_results[1].diagnostics[0]
        self.assertEqual(
            second, {"type": "durable_workload_state", "shared_with_sibling_map": True}
        )

    def test_three_maps_sharing_a_model_only_the_first_carries_it(self):
        fetcher = self._fetcher(
            ["dcim.inventoryitem", "dcim.inventoryitem", "dcim.inventoryitem"]
        )
        fetcher._record_durable_workload_state_summaries(
            [self._summary("dcim.inventoryitem")]
        )

        carriers = [
            result
            for result in fetcher.model_results
            if "target_row_count" in result.diagnostics[0]
        ]
        self.assertEqual(len(carriers), 1)

    def test_unrelated_models_are_unaffected(self):
        fetcher = self._fetcher(["dcim.inventoryitem", "ipam.prefix"])
        fetcher._record_durable_workload_state_summaries(
            [
                self._summary("dcim.inventoryitem"),
                self._summary("ipam.prefix", target_rows=12),
            ]
        )

        self.assertEqual(
            fetcher.model_results[0].diagnostics[0]["target_row_count"], 306
        )
        self.assertEqual(
            fetcher.model_results[1].diagnostics[0]["target_row_count"], 12
        )

    def test_a_model_with_no_summary_is_untouched(self):
        fetcher = self._fetcher(["dcim.device"])
        fetcher._record_durable_workload_state_summaries([self._summary("ipam.prefix")])

        self.assertEqual(fetcher.model_results[0].diagnostics, [])
