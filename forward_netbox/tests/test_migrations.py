from django.db import connection
from django.db.migrations.executor import MigrationExecutor
from django.test import TransactionTestCase

APP = "forward_netbox"
# The last migration before the destructive ones (0025 device-analysis rebuild and
# 0028 execution-run removal). Data created here must survive the upgrade to head.
PRE_DESTRUCTIVE = "0024_forwarddeviceanalysis"
PROBE_NAME = "upgrade-probe-source"


def _head_migration():
    executor = MigrationExecutor(connection)
    leaves = [name for app, name in executor.loader.graph.leaf_nodes() if app == APP]
    return leaves[0]


def _migrate_to(target):
    executor = MigrationExecutor(connection)
    executor.migrate([(APP, target)])
    return executor


class ForwardUpgradeMigrationTest(TransactionTestCase):
    """In-place upgrade on a POPULATED database must preserve the core
    ForwardSource config across the destructive migrations — the path an operator
    exercises on `pip install -U`, previously untested (CI only migrates a fresh
    empty DB forward).
    """

    def tearDown(self):
        # Restore the shared test database to head for subsequent tests, then drop
        # the probe row.
        _migrate_to(_head_migration())
        from forward_netbox.models import ForwardSource

        ForwardSource.objects.filter(name=PROBE_NAME).delete()

    def test_source_survives_upgrade_across_destructive_migrations(self):
        executor = _migrate_to(PRE_DESTRUCTIVE)
        state = executor.loader.project_state((APP, PRE_DESTRUCTIVE))
        historical_source = state.apps.get_model(APP, "ForwardSource")
        historical_source.objects.create(
            name=PROBE_NAME,
            url="https://fwd.app",
            parameters={"username": "u", "password": "p", "network_id": "n"},
        )

        _migrate_to(_head_migration())

        from forward_netbox.models import ForwardSource

        survived = ForwardSource.objects.filter(name=PROBE_NAME).first()
        self.assertIsNotNone(survived)
        self.assertEqual(survived.parameters.get("network_id"), "n")
