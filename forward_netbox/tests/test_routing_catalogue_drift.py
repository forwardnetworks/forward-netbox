"""Pending removals are what the next sync removes, not what Forward declares.

A customer's preview read ~6,300 pending routing-policy removals on every run.
They were rows the device-tag scope drops from the catalogue maps and the
durable workload state had tombstoned long before: declared again every run,
staged never, nothing left to remove. The state's staged count is now the
pending number; the rest is reported as already removed, and neither is drift.
"""

from django.test import SimpleTestCase

from forward_netbox.utilities.drift_report import compute_drift_report
from forward_netbox.utilities.query_fetch_execution import ForwardModelResult
from forward_netbox.views import _dependency_model_result_summary


def _result(delete_count, *, staged=None, shared=False):
    diagnostics = []
    if shared:
        diagnostics.append(
            {"type": "durable_workload_state", "shared_with_sibling_map": True}
        )
    elif staged is not None:
        diagnostics.append(
            {
                "type": "durable_workload_state",
                "mode": "local_delta",
                "staged_delete_count": staged,
                "tombstone_count": delete_count,
            }
        )
    return ForwardModelResult(
        model_string="netbox_routing.prefixlistentry",
        query_name="Forward Routing Prefix Lists",
        execution_mode="query_id",
        execution_value="",
        sync_mode="full",
        row_count=8759,
        delete_count=delete_count,
        diagnostics=diagnostics,
    )


class PendingRemovalsTest(SimpleTestCase):
    def test_tombstoned_removals_are_not_pending(self):
        summary = _dependency_model_result_summary(_result(3159, staged=0))
        self.assertEqual(summary["delete_count"], 0)
        self.assertEqual(summary["forward_removal_count"], 3159)
        self.assertEqual(summary["already_removed_count"], 3159)

    def test_newly_staged_removals_are_pending(self):
        summary = _dependency_model_result_summary(_result(3159, staged=75))
        self.assertEqual(summary["delete_count"], 75)
        self.assertEqual(summary["already_removed_count"], 3084)

    def test_without_durable_state_the_declared_count_stands(self):
        summary = _dependency_model_result_summary(_result(40))
        self.assertEqual(summary["delete_count"], 40)
        self.assertEqual(summary["already_removed_count"], 0)

    def test_a_sibling_map_sharing_the_state_keeps_its_own_count(self):
        summary = _dependency_model_result_summary(_result(40, shared=True))
        self.assertEqual(summary["delete_count"], 40)

    def test_a_staged_count_larger_than_declared_is_not_trusted(self):
        # Staged also counts rows missing from the target, which the declared
        # removals do not; never report more removals than were declared here.
        summary = _dependency_model_result_summary(_result(5, staged=9))
        self.assertEqual(summary["delete_count"], 5)
        self.assertEqual(summary["already_removed_count"], 0)


class DriftReportTest(SimpleTestCase):
    def _payload(self, **row):
        base = {
            "model": "netbox_routing.prefixlistentry",
            "row_count": 8759,
            "estimated_changes": 0,
            "delete_count": 0,
            "change_estimate_kind": "exact_comparison",
            "already_removed_count": 3159,
            "outside_scope_in_netbox_count": 12,
        }
        base.update(row)
        return {"model_results": [base], "comparison_coverage": {"measured": 1}}

    def test_already_removed_and_outside_scope_are_not_drift(self):
        report = compute_drift_report(self._payload())
        row = report["models"][0]
        self.assertEqual(row["drift"], 0)
        self.assertTrue(row["in_sync"])
        self.assertEqual(row["already_removed"], 3159)
        self.assertEqual(row["outside_scope_in_netbox"], 12)
        self.assertEqual(report["total_already_removed"], 3159)
        self.assertEqual(report["total_outside_scope_in_netbox"], 12)

    def test_an_older_payload_reads_as_zero(self):
        payload = self._payload()
        del payload["model_results"][0]["already_removed_count"]
        del payload["model_results"][0]["outside_scope_in_netbox_count"]
        report = compute_drift_report(payload)
        self.assertEqual(report["total_already_removed"], 0)
        self.assertIsNone(report["models"][0]["outside_scope_in_netbox"])
