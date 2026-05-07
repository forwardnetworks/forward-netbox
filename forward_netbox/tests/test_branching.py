from types import SimpleNamespace
from unittest import TestCase

from forward_netbox.utilities.branching import build_branch_name


class ForwardBranchingHelpersTest(TestCase):
    def test_build_branch_name_uses_stable_ids_instead_of_sync_name(self):
        sync = SimpleNamespace(pk=17, name="customer-visible-name")
        ingestion = SimpleNamespace(pk=23)
        item = SimpleNamespace(index=4, model_string="dcim.device")

        branch_name = build_branch_name(sync=sync, ingestion=ingestion, item=item)

        self.assertEqual(
            branch_name,
            "Forward Sync 17 - ingestion 23 - part 4 dcim.device",
        )
        self.assertNotIn("customer-visible-name", branch_name)
