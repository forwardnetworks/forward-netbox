from django.test import SimpleTestCase

from forward_netbox.utilities.query_fetch_execution import ForwardModelResult
from forward_netbox.views import _dependency_model_result_summary


class DependencyModelResultSummaryTest(SimpleTestCase):
    """Regression: fetcher.model_results are ForwardModelResult dataclasses, but
    _dependency_model_result_summary called result.get(...) — AttributeError that
    errored the whole dependency preview (hidden as null-data until 2.2.4 surfaced
    job errors). The summary must accept a ForwardModelResult.
    """

    def _result(self, **kw):
        base = dict(
            model_string="ipam.prefix",
            query_name="Forward Prefixes",
            execution_mode="query_path",
            execution_value="",
            sync_mode="branching",
            row_count=12,
            delete_count=3,
        )
        base.update(kw)
        return ForwardModelResult(**base)

    def test_summary_accepts_dataclass(self):
        summary = _dependency_model_result_summary(self._result())
        self.assertEqual(summary["model"], "ipam.prefix")
        self.assertEqual(summary["row_count"], 12)
        self.assertEqual(summary["delete_count"], 3)
        # estimated_changes is derived (as_dict has no such field).
        self.assertEqual(summary["estimated_changes"], 15)
        self.assertEqual(summary["change_estimate_kind"], "workload_upper_bound")

    def test_summary_handles_none_runtime(self):
        # runtime_ms defaults to None on the dataclass; must not blow up.
        summary = _dependency_model_result_summary(self._result(runtime_ms=None))
        self.assertEqual(summary["runtime_ms"], 0.0)

    def test_summary_preserves_only_aggregate_durable_state_diagnostic(self):
        durable_state = {
            "type": "durable_workload_state",
            "mode": "delta",
            "target_row_count": 306,
            "staged_upsert_count": 0,
            "staged_delete_count": 0,
        }
        summary = _dependency_model_result_summary(
            self._result(
                diagnostics=[
                    {"type": "row_sample", "device": "private-device"},
                    durable_state,
                ]
            )
        )
        self.assertEqual(summary["durable_workload_state"], durable_state)
        self.assertNotIn("diagnostics", summary)

    def test_delete_count_folds_in_comparison_deletes(self):
        # `dcim.inventoryitem`'s module-native rows are Forward upsert rows
        # the apply deletes instead - not in `data["delete_count"]` at all
        # until the comparison classifies them (2.9.6).
        summary = _dependency_model_result_summary(
            self._result(),
            comparison={
                "creates": 2,
                "updates": 0,
                "unchanged": 0,
                "rejected": 0,
                "deletes": 5,
            },
        )
        self.assertEqual(summary["delete_count"], 8)
        self.assertEqual(summary["estimated_changes"], 2)
        self.assertEqual(summary["change_estimate_kind"], "exact_comparison")

    def test_summary_rejects_noncanonical_plain_dict(self):
        with self.assertRaisesRegex(TypeError, "must be ForwardModelResult"):
            _dependency_model_result_summary(
                {"model": "dcim.device", "row_count": 5, "delete_count": 1}
            )

    # -- a comparison shared by several maps of one model -----------------
    # A customer's dependency preview showed 1,394 pending inventory-item
    # removals on a map that had fetched zero rows, because
    # `comparison_by_model` is computed once per MODEL and three built-in
    # maps target `dcim.inventoryitem`. `attribute_model_comparison` is how
    # the caller (`_dependency_dry_run_payload`) says "this is the one map
    # that carries the shared comparison" - every sibling map must show its
    # own numbers only, never a second copy of the shared ones.

    def test_the_attributed_map_gets_the_full_comparison(self):
        comparison = {
            "creates": 2,
            "updates": 0,
            "unchanged": 0,
            "rejected": 1,
            "deletes": 1394,
        }
        summary = _dependency_model_result_summary(
            self._result(row_count=0, delete_count=0),
            comparison=comparison,
            attribute_model_comparison=True,
            comparison_shared_with_sibling_maps=2,
        )
        self.assertEqual(summary["delete_count"], 1394)
        self.assertEqual(summary["estimated_changes"], 2)
        self.assertEqual(summary["change_estimate_kind"], "exact_comparison")
        self.assertEqual(summary["comparison_rejected_rows"], 1)
        self.assertEqual(summary["comparison_shared_with_sibling_maps"], 2)

    def test_a_sibling_map_with_zero_rows_shows_zero_not_the_shared_deletes(self):
        comparison = {
            "creates": 2,
            "updates": 0,
            "unchanged": 0,
            "rejected": 1,
            "deletes": 1394,
        }
        summary = _dependency_model_result_summary(
            self._result(row_count=0, delete_count=0),
            comparison=comparison,
            attribute_model_comparison=False,
            comparison_shared_with_sibling_maps=2,
        )
        self.assertEqual(summary["delete_count"], 0)
        self.assertEqual(summary["estimated_changes"], 0)
        self.assertEqual(summary["change_estimate_kind"], "shared_with_sibling_maps")
        self.assertEqual(summary["comparison_rejected_rows"], 0)
        self.assertEqual(summary["unchanged_rows"], 0)
        self.assertEqual(summary["comparison_shared_with_sibling_maps"], 2)

    def test_a_sibling_map_still_shows_its_own_forward_declared_deletes(self):
        # Not attributed the SHARED comparison deletes, but its own
        # `data["delete_count"]` (Forward's own declaration for that map)
        # survives untouched - it is a different number from the same
        # duplication bug.
        comparison = {"creates": 0, "updates": 0, "unchanged": 5, "deletes": 1394}
        summary = _dependency_model_result_summary(
            self._result(row_count=5, delete_count=3),
            comparison=comparison,
            attribute_model_comparison=False,
        )
        self.assertEqual(summary["delete_count"], 3)
        self.assertEqual(summary["estimated_changes"], 8)

    def test_no_comparison_at_all_is_unaffected_by_the_new_parameter(self):
        summary = _dependency_model_result_summary(
            self._result(), attribute_model_comparison=False
        )
        self.assertEqual(summary["change_estimate_kind"], "workload_upper_bound")
        self.assertEqual(summary["comparison_shared_with_sibling_maps"], 0)
