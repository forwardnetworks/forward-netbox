from django.test import SimpleTestCase

from forward_netbox.utilities.bundle_ui_figures import drift_table


class DriftTableTest(SimpleTestCase):
    """A figure read off the Drift report page is in the bundle under the page's
    column heading and badge words, so a quoted number can be found by them."""

    def test_rows_carry_the_page_headings_and_badges(self):
        report = {
            "models": [
                {
                    "model": "dcim.device",
                    "label": "dcim.device",
                    "forward_rows": 3829,
                    "pending_changes": 3,
                    "pending_removes": 29,
                    "already_removed": 19,
                    "estimated_apply_work": 32,
                    "comparison_available": True,
                    "drift": 32,
                    "in_sync": False,
                    "meaning": "3 rows are counted as new or different.",
                    "action": "Whether a removal happens follows the rules.",
                },
                {
                    "model": "dcim.inventoryitem",
                    "label": "dcim.inventoryitem (Forward ACI APIC CIMC Inventory)",
                    "forward_rows": 0,
                    "pending_changes": 0,
                    "pending_removes": 53,
                    "estimated_apply_work": 53,
                    "comparison_available": False,
                    "comparison_unavailable_reason": "full_only_by_design",
                    "drift": None,
                },
                {
                    "model": "dcim.inventoryitem",
                    "label": "dcim.inventoryitem (Forward CIMC Endpoint Inventory)",
                    "pending_removes": 0,
                    "comparison_available": False,
                    "comparison_unavailable_reason": "no_exact_comparison",
                },
                {
                    "model": "dcim.site",
                    "pending_removes": 0,
                    "comparison_available": True,
                    "drift": 0,
                    "in_sync": True,
                },
            ]
        }

        device, by_design, estimate, site = drift_table(report)

        self.assertEqual(device["Model"], "dcim.device")
        self.assertEqual(device["Forward rows"], 3829)
        self.assertEqual(device["Upsert/change candidates"], 3)
        self.assertEqual(
            device["Pending removals"], {"value": 29, "declared, not staged": 19}
        )
        self.assertEqual(device["Estimated apply work"], 32)
        self.assertEqual(device["Drift"], 32)
        self.assertEqual(device["In sync"], "No")
        self.assertEqual(
            device["What it means"],
            "3 rows are counted as new or different. "
            "Whether a removal happens follows the rules.",
        )
        self.assertEqual(by_design["Drift"], "Not measured")
        self.assertEqual(by_design["In sync"], "By design")
        self.assertEqual(estimate["In sync"], "Not measured")
        self.assertEqual(site["Model"], "dcim.site")
        self.assertEqual(site["Drift"], 0)
        self.assertEqual(site["In sync"], "Yes")

    def test_no_report_is_no_rows(self):
        self.assertEqual(drift_table(None), [])
