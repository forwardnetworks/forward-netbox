from django.test import TestCase

from forward_netbox.utilities.sync import ForwardSyncRunner


class ForwardSyncRunnerContractTest(TestCase):
    def setUp(self):
        self.runner = object.__new__(ForwardSyncRunner)
        self.runner._model_coalesce_fields = {
            "dcim.site": [("slug",), ("name",)],
            "dcim.cable": [
                ("device", "interface", "remote_device", "remote_interface")
            ],
        }

    def test_conflict_policy_defaults_to_strict_and_uses_cable_override(self):
        self.assertEqual(self.runner._conflict_policy("dcim.device"), "strict")
        self.assertEqual(
            self.runner._conflict_policy("dcim.cable"),
            "skip_warn_aggregate",
        )

    def test_module_native_inventory_row_detects_component_and_part_type(self):
        self.assertTrue(
            self.runner._is_module_native_inventory_row({"module_component": True})
        )
        self.assertTrue(
            self.runner._is_module_native_inventory_row({"part_type": "ROUTING ENGINE"})
        )
        self.assertFalse(self.runner._is_module_native_inventory_row({}))

    def test_split_diff_rows_keeps_modified_rows_without_identity_change(self):
        diff_rows = [
            {
                "type": "MODIFIED",
                "before": {"slug": "site-a", "name": "Site A"},
                "after": {"slug": "site-a", "name": "Site A 2"},
            }
        ]

        upsert_rows, delete_rows = self.runner._split_diff_rows("dcim.site", diff_rows)

        self.assertEqual(upsert_rows, [{"slug": "site-a", "name": "Site A 2"}])
        self.assertEqual(delete_rows, [])

    def test_split_diff_rows_deletes_modified_rows_when_identity_changes(self):
        diff_rows = [
            {
                "type": "MODIFIED",
                "before": {"slug": "site-a", "name": "Site A"},
                "after": {"slug": "site-b", "name": "Site B"},
            }
        ]

        upsert_rows, delete_rows = self.runner._split_diff_rows("dcim.site", diff_rows)

        self.assertEqual(upsert_rows, [{"slug": "site-b", "name": "Site B"}])
        self.assertEqual(delete_rows, [{"slug": "site-a", "name": "Site A"}])
