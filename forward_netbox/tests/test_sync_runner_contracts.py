from django.test import TestCase

from forward_netbox.exceptions import ForwardQueryError
from forward_netbox.utilities.sync import ForwardSyncRunner
from forward_netbox.utilities.sync_contracts import default_coalesce_fields_for_model
from forward_netbox.utilities.sync_contracts import row_coalesce_field_is_complete
from forward_netbox.utilities.sync_contracts import validate_row_shape_for_model


class ForwardSyncRunnerContractTest(TestCase):
    def setUp(self):
        self.runner = object.__new__(ForwardSyncRunner)
        self.runner._model_coalesce_fields = {
            "dcim.site": [("slug",), ("name",)],
            "dcim.cable": [
                ("device", "interface", "remote_device", "remote_interface")
            ],
            "ipam.fhrpgroup": [("protocol", "group_id", "address", "vrf")],
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

    def test_split_diff_rows_drops_delete_when_identity_also_upserted(self):
        # FHRP state flap: many (device, interface, state) rows collapse onto one
        # (protocol, group_id, address, vrf) group, so the snapshot diff emits an
        # ADDED variant and a DELETED variant for the SAME group. The delete must
        # be dropped so the group is not churned (created+deleted) every sync.
        identity = {
            "protocol": "hsrp",
            "group_id": 1100,
            "address": "11.240.24.1",
            "vrf": "default",
        }
        diff_rows = [
            {"type": "ADDED", "after": {**identity, "state": "active", "device": "r1"}},
            {
                "type": "DELETED",
                "before": {**identity, "state": "standby", "device": "r2"},
            },
        ]

        upsert_rows, delete_rows = self.runner._split_diff_rows(
            "ipam.fhrpgroup", diff_rows
        )

        self.assertEqual(len(upsert_rows), 1)
        self.assertEqual(delete_rows, [])

    def test_split_diff_rows_keeps_delete_for_distinct_identity(self):
        # A delete for a group that is NOT being upserted is still applied.
        diff_rows = [
            {
                "type": "ADDED",
                "after": {
                    "protocol": "hsrp",
                    "group_id": 1100,
                    "address": "11.240.24.1",
                    "vrf": "default",
                },
            },
            {
                "type": "DELETED",
                "before": {
                    "protocol": "hsrp",
                    "group_id": 999,
                    "address": "11.240.24.9",
                    "vrf": "default",
                },
            },
        ]

        upsert_rows, delete_rows = self.runner._split_diff_rows(
            "ipam.fhrpgroup", diff_rows
        )

        self.assertEqual(len(upsert_rows), 1)
        self.assertEqual(len(delete_rows), 1)
        self.assertEqual(delete_rows[0]["group_id"], 999)

    def test_nullable_vrf_identity_is_model_specific(self):
        self.assertTrue(
            row_coalesce_field_is_complete("ipam.prefix", {"vrf": None}, "vrf")
        )
        self.assertTrue(
            row_coalesce_field_is_complete("ipam.prefix", {"vrf": ""}, "vrf")
        )
        self.assertFalse(
            row_coalesce_field_is_complete("ipam.ipaddress", {"vrf": None}, "vrf")
        )
        self.assertFalse(
            row_coalesce_field_is_complete("netbox_routing.bgppeer", {"vrf": ""}, "vrf")
        )

    def test_default_vrf_fallback_contracts_are_explicit(self):
        self.assertEqual(
            default_coalesce_fields_for_model("ipam.prefix"),
            [["prefix", "vrf"]],
        )
        self.assertEqual(
            default_coalesce_fields_for_model("ipam.ipaddress"),
            [["address", "vrf"], ["address"]],
        )
        self.assertEqual(
            default_coalesce_fields_for_model("netbox_routing.bgppeer"),
            [["device", "vrf", "neighbor_address"], ["device", "neighbor_address"]],
        )

    def test_prefix_requires_explicit_vrf_column_even_for_global_rows(self):
        validate_row_shape_for_model(
            "ipam.prefix",
            {"prefix": "10.0.0.0/24", "vrf": None, "status": "active"},
            default_coalesce_fields_for_model("ipam.prefix"),
        )
        with self.assertRaises(ForwardQueryError):
            validate_row_shape_for_model(
                "ipam.prefix",
                {"prefix": "10.0.0.0/24", "status": "active"},
                default_coalesce_fields_for_model("ipam.prefix"),
            )
