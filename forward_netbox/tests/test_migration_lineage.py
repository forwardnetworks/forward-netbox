"""A database from either release lane upgrades onto this one.

`main` and `maint/2.9.x` share migrations only through `0052`; each numbered
its own after that. A 2.9.x database records the 2.9.x names as applied, and
3.x did not know them, so upgrading a 2.9.8+ database to 3.0 left its
`last_live_drift` columns behind a model with no such field - every NQE map
insert then failed on a NOT NULL column. These pin that every 2.9.x migration
name exists here with its original dependency, and that both chains end in
one leaf.
"""

from django.db.migrations.loader import MigrationLoader
from django.test import SimpleTestCase

# Every forward_netbox migration `maint/2.9.x` added after the fork, with the
# dependency it declares there. A new one on that lane belongs here too.
MAINT_2_9_X_HISTORY = {
    "0053_routing_policy_nqe_map_choices": "0052_device_absence_quarantine",
    "0054_aci_tenant_policy_nqe_map_choices": "0053_routing_policy_nqe_map_choices",
    "0055_operator_delete_releases_ownership": "0054_aci_tenant_policy_nqe_map_choices",
    "0056_aci_attachment_nqe_map_choices": "0055_operator_delete_releases_ownership",
    "0057_nqe_map_last_live_drift": "0056_aci_attachment_nqe_map_choices",
}


class MigrationLineageTest(SimpleTestCase):
    def setUp(self):
        # The graph on disk only: no connection, so no applied-state query.
        self.loader = MigrationLoader(None, ignore_no_migrations=True)

    def test_every_2_9_x_migration_is_known_by_its_own_name(self):
        for name, parent in MAINT_2_9_X_HISTORY.items():
            key = ("forward_netbox", name)
            self.assertIn(key, self.loader.disk_migrations, name)
            dependencies = self.loader.disk_migrations[key].dependencies
            self.assertIn(("forward_netbox", parent), dependencies, name)

    def test_both_chains_end_in_one_leaf(self):
        leaves = self.loader.graph.leaf_nodes("forward_netbox")
        self.assertEqual(len(leaves), 1, leaves)
        ancestors = self.loader.graph.forwards_plan(leaves[0])
        self.assertIn(("forward_netbox", "0057_nqe_map_last_live_drift"), ancestors)
        self.assertIn(
            ("forward_netbox", "0059_aci_attachment_nqe_map_choices"), ancestors
        )

    def test_only_the_last_live_drift_migration_changes_anything(self):
        # The other four are made on this lane by its own migrations; a real
        # operation in them would apply the same change twice on 3.0.0 data.
        for name in MAINT_2_9_X_HISTORY:
            migration = self.loader.disk_migrations[("forward_netbox", name)]
            if name == "0057_nqe_map_last_live_drift":
                self.assertTrue(migration.operations)
            else:
                self.assertEqual(migration.operations, [], name)
