"""A model with several workloads must not render as one name repeated.

A deployment's drift report listed `dcim.inventoryitem` three times in the
per-model table and three times in "Not compared", with nothing to tell the
rows apart - one showed 0 Forward rows and 56 pending removals, another 48 and
0. The rows were correct: there is one row per WORKLOAD and that estate has
three inventory item maps. Only the label was wrong, and a page that prints the
same identifier three times with different numbers beside it reads as a
rendering fault, which is corrosive to a report whose whole job is to be
believed.

These tests pin that a label identifies its row, that the common case is left
alone, and that nothing here leaks shard data onto the page to achieve it.
"""

from django.test import SimpleTestCase

from forward_netbox.utilities.drift_report import compute_drift_report


def _result(model, **overrides):
    payload = {
        "model": model,
        "query_name": "",
        "row_count": 0,
        "delete_count": 0,
        "estimated_changes": 0,
        "change_estimate_kind": "exact_comparison",
    }
    payload.update(overrides)
    return payload


class OneWorkloadPerModelIsLeftAloneTest(SimpleTestCase):
    """The overwhelmingly common case must not grow a suffix."""

    def test_a_sole_workload_is_labelled_with_its_model_name(self):
        report = compute_drift_report(
            {
                "model_results": [
                    _result("dcim.device", query_name="devices", row_count=10),
                    _result("dcim.interface", query_name="interfaces", row_count=20),
                ],
                "comparison_coverage": {"runtime_ms": 1.0},
            }
        )

        labels = {row["label"] for row in report["models"]}
        self.assertEqual(labels, {"dcim.device", "dcim.interface"})


class RepeatedModelsAreDistinguishedTest(SimpleTestCase):
    """The case that produced three identical rows."""

    def _report(self, results):
        return compute_drift_report(
            {
                "model_results": results,
                "comparison_coverage": {"runtime_ms": 1.0},
            }
        )

    def test_each_workload_gets_a_label_of_its_own(self):
        report = self._report(
            [
                _result(
                    "dcim.inventoryitem", query_name="inventory_a", delete_count=56
                ),
                _result("dcim.inventoryitem", query_name="inventory_b", row_count=48),
                _result("dcim.inventoryitem", query_name="inventory_c", row_count=5),
            ]
        )

        labels = [row["label"] for row in report["models"]]
        self.assertEqual(len(set(labels)), 3, f"labels collided: {labels}")
        for label in labels:
            self.assertTrue(label.startswith("dcim.inventoryitem ("))

    def test_workloads_sharing_a_query_name_still_separate(self):
        # One map run over several shard keys carries the same query name. The
        # execution value that distinguishes them is shard data, so the label
        # must fall back to position rather than reach for it.
        report = self._report(
            [
                _result("dcim.interface", query_name="interfaces", row_count=1),
                _result("dcim.interface", query_name="interfaces", row_count=2),
            ]
        )

        labels = [row["label"] for row in report["models"]]
        self.assertEqual(len(set(labels)), 2, f"labels collided: {labels}")

    def test_workloads_with_no_query_name_still_separate(self):
        report = self._report(
            [
                _result("dcim.interface", row_count=1),
                _result("dcim.interface", row_count=2),
            ]
        )

        labels = [row["label"] for row in report["models"]]
        self.assertEqual(len(set(labels)), 2, f"labels collided: {labels}")

    def test_the_not_compared_list_names_each_workload_once(self):
        report = self._report(
            [
                _result(
                    "dcim.inventoryitem",
                    query_name="inventory_a",
                    row_count=48,
                    estimated_changes=48,
                    change_estimate_kind="workload_upper_bound",
                ),
                _result(
                    "dcim.inventoryitem",
                    query_name="inventory_b",
                    row_count=5,
                    estimated_changes=5,
                    change_estimate_kind="workload_upper_bound",
                ),
                _result(
                    "dcim.device",
                    query_name="devices",
                    row_count=3,
                    change_estimate_kind="exact_comparison",
                ),
            ]
        )

        unmeasured = report["unmeasured_models"]
        self.assertEqual(len(unmeasured), 2)
        self.assertEqual(
            len(set(unmeasured)),
            2,
            f"the list repeated a name with nothing to tell them apart: {unmeasured}",
        )

    def test_no_label_carries_the_execution_value(self):
        report = self._report(
            [
                _result(
                    "dcim.interface",
                    query_name="interfaces",
                    execution_value="shard-key-that-is-device-data",
                    row_count=1,
                ),
                _result(
                    "dcim.interface",
                    query_name="interfaces",
                    execution_value="another-shard-key",
                    row_count=2,
                ),
            ]
        )

        for row in report["models"]:
            self.assertNotIn("shard-key-that-is-device-data", row["label"])
            self.assertNotIn("another-shard-key", row["label"])


class TheSlowestModelIsNamedByItsLabelTest(SimpleTestCase):
    """Naming the outlier is useless if the name is ambiguous."""

    def test_the_slowest_row_reports_its_disambiguated_label(self):
        report = compute_drift_report(
            {
                "model_results": [
                    _result(
                        "dcim.inventoryitem",
                        query_name="inventory_a",
                        row_count=1,
                        comparison_runtime_ms=10.0,
                    ),
                    _result(
                        "dcim.inventoryitem",
                        query_name="inventory_b",
                        row_count=1,
                        comparison_runtime_ms=900.0,
                    ),
                ],
                "comparison_coverage": {"runtime_ms": 910.0},
            }
        )

        self.assertEqual(
            report["slowest_compared_model"]["model"],
            "dcim.inventoryitem (inventory_b)",
        )
