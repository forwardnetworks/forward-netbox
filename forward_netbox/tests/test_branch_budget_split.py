from django.test import SimpleTestCase

from forward_netbox.exceptions import ForwardQueryError
from forward_netbox.utilities.branch_budget import BranchWorkload
from forward_netbox.utilities.branch_budget import build_branch_plan
from forward_netbox.utilities.branch_budget import split_workload


class BranchBudgetSplitTest(SimpleTestCase):
    def test_vlan_singleton_keys_pack_to_budget(self):
        workload = BranchWorkload(
            model_string="ipam.vlan",
            label="VLANs",
            upsert_rows=[{"vid": vid, "site": "site-1"} for vid in range(1, 8)],
            coalesce_fields=[["vid", "site"]],
        )

        chunks = split_workload(workload, max_row_budget=3)

        self.assertEqual(len(chunks), 3)
        self.assertTrue(all(chunk.estimated_changes <= 3 for chunk in chunks))
        self.assertTrue(all(chunk.shard_keys for chunk in chunks))
        self.assertTrue(
            all(chunk.shard_keys == tuple(sorted(chunk.shard_keys)) for chunk in chunks)
        )

    def test_interface_device_bucket_is_never_split(self):
        lag_rows = [
            {"device": "leaf-1", "name": "Port-Channel1"},
            {"device": "leaf-1", "name": "Ethernet1"},
        ]
        workload = BranchWorkload(
            model_string="dcim.interface",
            label="Interfaces",
            upsert_rows=lag_rows
            + [
                {"device": "leaf-2", "name": "Ethernet1"},
                {"device": "leaf-3", "name": "Ethernet1"},
            ],
            coalesce_fields=[["device", "name"]],
        )

        chunks = split_workload(workload, max_row_budget=2)

        leaf_1_chunks = [
            chunk
            for chunk in chunks
            if any(row["device"] == "leaf-1" for row in chunk.upsert_rows)
        ]
        self.assertEqual(len(leaf_1_chunks), 1)
        self.assertEqual(leaf_1_chunks[0].upsert_rows, lag_rows)

    def test_oversized_bucket_warns_and_remains_one_chunk(self):
        workload = BranchWorkload(
            model_string="dcim.interface",
            label="Interfaces",
            upsert_rows=[
                {"device": "leaf-1", "name": f"Ethernet{index}"} for index in range(3)
            ],
            coalesce_fields=[["device", "name"]],
        )

        with self.assertLogs("forward_netbox.utilities.branch_budget", level="WARNING"):
            chunks = split_workload(workload, max_row_budget=2)

        self.assertEqual(len(chunks), 1)
        self.assertEqual(chunks[0].estimated_changes, 3)
        with self.assertRaises(ForwardQueryError):
            split_workload(
                workload,
                max_row_budget=2,
                oversized_bucket_policy="fail",
            )

    def test_split_is_deterministic(self):
        workload = BranchWorkload(
            model_string="ipam.vlan",
            label="VLANs",
            upsert_rows=[{"vid": vid, "site": "site-1"} for vid in range(1, 10)],
            coalesce_fields=[["vid", "site"]],
        )

        first = split_workload(workload, max_row_budget=4)
        second = split_workload(workload, max_row_budget=4)

        self.assertEqual(
            [(chunk.shard_keys, chunk.upsert_rows) for chunk in first],
            [(chunk.shard_keys, chunk.upsert_rows) for chunk in second],
        )

    def test_none_budget_preserves_default_plan(self):
        workloads = [
            BranchWorkload(
                model_string="ipam.vlan",
                label="VLANs",
                upsert_rows=[{"vid": vid, "site": "site-1"} for vid in range(3)],
                coalesce_fields=[["vid", "site"]],
            )
        ]

        self.assertEqual(
            build_branch_plan(workloads),
            build_branch_plan(workloads, max_changes_per_staging_item=None),
        )
        self.assertEqual(len(build_branch_plan(workloads)), 1)
