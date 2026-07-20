from django.db import connection
from django.db.migrations.executor import MigrationExecutor
from django.test import TransactionTestCase
from netbox.context import current_request

APP = "forward_netbox"
# The last migration before the destructive ones (0025 device-analysis rebuild and
# 0028 execution-run removal). Data created here must survive the upgrade to head.
PRE_DESTRUCTIVE = "0024_forwarddeviceanalysis"
PRE_26 = "0033_alter_forwardnqemap_netbox_model"
PROBE_NAME = "upgrade-probe-source"
PROBE_SYNC_NAME = "upgrade-probe-sync"
PROBE_26_SOURCE_NAME = "upgrade-26-probe-source"
PROBE_26_SYNC_NAME = "upgrade-26-probe-sync"


def _head_migration():
    executor = MigrationExecutor(connection)
    leaves = [name for app, name in executor.loader.graph.leaf_nodes() if app == APP]
    return leaves[0]


def _migrate_to(target):
    current_request.set(None)
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
        from forward_netbox.models import ForwardSync
        from forward_netbox.models import ForwardSource

        ForwardSync.objects.filter(name=PROBE_SYNC_NAME).delete()
        ForwardSource.objects.filter(name=PROBE_NAME).delete()

    def test_source_survives_upgrade_across_destructive_migrations(self):
        executor = _migrate_to(PRE_DESTRUCTIVE)
        state = executor.loader.project_state((APP, PRE_DESTRUCTIVE))
        historical_source = state.apps.get_model(APP, "ForwardSource")
        source = historical_source.objects.create(
            name=PROBE_NAME,
            url="https://fwd.app",
            parameters={"username": "u", "password": "p", "network_id": "n"},
        )
        historical_sync = state.apps.get_model(APP, "ForwardSync")
        historical_sync.objects.create(
            name=PROBE_SYNC_NAME,
            source=source,
            parameters={
                "snapshot_id": "latestProcessed",
                "auto_prune_orphans": True,
            },
        )

        _migrate_to(_head_migration())

        from forward_netbox.models import ForwardSource
        from forward_netbox.models import ForwardSync

        survived = ForwardSource.objects.filter(name=PROBE_NAME).first()
        self.assertIsNotNone(survived)
        self.assertEqual(survived.parameters.get("network_id"), "n")
        survived_sync = ForwardSync.objects.get(name=PROBE_SYNC_NAME)
        self.assertNotIn("auto_prune_orphans", survived_sync.parameters)


class Forward26UpgradeMigrationTest(TransactionTestCase):
    """Prove the populated 2.5.11-era schema-to-2.6 transition end to end."""

    def tearDown(self):
        _migrate_to(_head_migration())

    def test_26_upgrade_normalizes_cross_domain_managed_tag_rows(self):
        executor = _migrate_to("0037_merge_identity_and_assignment_provenance")
        state = executor.loader.project_state(
            (APP, "0037_merge_identity_and_assignment_provenance")
        )
        HistoricalManagedTag = state.apps.get_model(
            APP,
            "ForwardManagedDeviceTag",
        )
        HistoricalTag = state.apps.get_model("extras", "Tag")
        reserved = HistoricalTag.objects.create(
            name="Forward Backfilled",
            slug="forward-backfilled",
        )
        scope = HistoricalTag.objects.create(
            name="Upgrade Scope",
            slug="upgrade-scope",
        )
        HistoricalManagedTag.objects.bulk_create(
            [
                HistoricalManagedTag(tag_id=reserved.pk, claim_type="backfilled"),
                HistoricalManagedTag(tag_id=reserved.pk, claim_type="scope"),
                HistoricalManagedTag(tag_id=scope.pk, claim_type="scope"),
                HistoricalManagedTag(tag_id=scope.pk, claim_type="out_of_scope"),
            ]
        )

        _migrate_to(_head_migration())

        from forward_netbox.models import ForwardManagedDeviceTag

        self.assertEqual(
            set(
                ForwardManagedDeviceTag.objects.values_list(
                    "tag__slug",
                    "claim_type",
                )
            ),
            {
                ("forward-backfilled", "backfilled"),
                ("upgrade-scope", "scope"),
            },
        )

    def test_26_upgrade_normalizes_configuration_and_initializes_provenance(self):
        import uuid

        from core.choices import JobStatusChoices
        from core.models import Job
        from dcim.models import Device
        from dcim.models import DeviceRole
        from dcim.models import DeviceType
        from dcim.models import Manufacturer
        from dcim.models import Site
        from django.contrib.auth import get_user_model
        from django.contrib.contenttypes.models import ContentType
        from extras.models import Tag

        executor = _migrate_to(PRE_26)
        state = executor.loader.project_state((APP, PRE_26))
        historical_source = state.apps.get_model(APP, "ForwardSource")
        historical_sync = state.apps.get_model(APP, "ForwardSync")
        historical_ingestion = state.apps.get_model(APP, "ForwardIngestion")
        historical_map = state.apps.get_model(APP, "ForwardNQEMap")

        source = historical_source.objects.create(
            name=PROBE_26_SOURCE_NAME,
            url="https://fwd.app",
            parameters={
                "username": "upgrade@example.com",
                "password": "upgrade-password",
                "network_id": "network-upgrade-26",
                "apply_device_scope_tags": True,
                "device_tag_include": "Upgrade 26 Scope",
                "device_tag_exclude": "Upgrade 26 Excluded",
                "scope_endpoints_by_include_tags": False,
                "scope_endpoints_by_include_tags_configured": False,
            },
        )
        sync = historical_sync.objects.create(
            name=PROBE_26_SYNC_NAME,
            source=source,
            parameters={
                "snapshot_id": "latestProcessed",
                "_branch_run": {"legacy": True},
                "_execution_progress": {"awaiting_merge": True},
                "execution_backend": "branching",
                "multi_branch": True,
                "scheduler_overlap": "skip",
                "bulk_orm_models": ["dcim.device"],
                "enable_branch_budget_split": True,
                "branch_budget_enforcement": "strict",
                "auto_tag_backfilled": True,
                "auto_prune_orphans": True,
                "max_changes_per_branch": 4321,
            },
        )
        partial_ingestion = historical_ingestion.objects.create(
            sync=sync,
            snapshot_id="historical-partial-snapshot",
            baseline_ready=True,
            applied_change_count=4,
            failed_change_count=1,
        )
        complete_ingestion = historical_ingestion.objects.create(
            sync=sync,
            snapshot_id="historical-complete-snapshot",
            baseline_ready=True,
            applied_change_count=5,
            failed_change_count=0,
        )

        owner = get_user_model().objects.create_user(username="upgrade-26-owner")
        sync_content_type = ContentType.objects.get(
            app_label=APP,
            model="forwardsync",
        )
        for name, interval in (("validation", 1440), ("dependency preview", 720)):
            Job.objects.create(
                object_type=sync_content_type,
                object_id=sync.pk,
                name=name,
                status=JobStatusChoices.STATUS_SCHEDULED,
                interval=interval,
                user=owner,
                job_id=uuid.uuid4(),
            )

        virtual_chassis_type = ContentType.objects.get(
            app_label="dcim",
            model="virtualchassis",
        )
        historical_map.objects.create(
            name="Forward Virtual Chassis",
            netbox_model_id=virtual_chassis_type.pk,
            built_in=True,
            enabled=True,
            query='select {device: ""}',
        )

        scope_tag = Tag.objects.create(
            name="Upgrade 26 Scope",
            slug="upgrade-26-scope",
        )
        status_tag = Tag.objects.create(
            name="Forward Backfilled",
            slug="forward-backfilled",
        )
        manufacturer = Manufacturer.objects.create(
            name="Upgrade 26 Manufacturer",
            slug="upgrade-26-manufacturer",
        )
        device_type = DeviceType.objects.create(
            manufacturer=manufacturer,
            model="Upgrade 26 Model",
            slug="upgrade-26-model",
        )
        role = DeviceRole.objects.create(
            name="Upgrade 26 Role",
            slug="upgrade-26-role",
        )
        site = Site.objects.create(
            name="Upgrade 26 Site",
            slug="upgrade-26-site",
        )
        device = Device.objects.create(
            name="upgrade-26-device",
            device_type=device_type,
            role=role,
            site=site,
        )
        device.tags.add(scope_tag, status_tag)

        _migrate_to(_head_migration())

        from forward_netbox.models import ForwardDeviceIdentity
        from forward_netbox.models import ForwardDeviceTagClaim
        from forward_netbox.models import ForwardManagedDeviceTag
        from forward_netbox.models import ForwardNQEMap
        from forward_netbox.models import ForwardPreservedDeviceTagAssignment
        from forward_netbox.models import ForwardSource
        from forward_netbox.models import ForwardSync
        from forward_netbox.models import ForwardVirtualParentClaim
        from forward_netbox.utilities.crypto import is_encrypted

        upgraded_sync = ForwardSync.objects.get(name=PROBE_26_SYNC_NAME)
        upgraded_source = ForwardSource.objects.get(name=PROBE_26_SOURCE_NAME)
        parameters = upgraded_sync.parameters
        self.assertEqual(parameters["max_changes_per_staging_item"], 4321)
        self.assertEqual(parameters["validation_schedule_interval"], 1440)
        self.assertEqual(parameters["preview_schedule_interval"], 720)
        self.assertEqual(upgraded_sync.user_id, owner.pk)
        self.assertTrue(
            {
                "_branch_run",
                "_execution_progress",
                "execution_backend",
                "multi_branch",
                "scheduler_overlap",
                "bulk_orm_models",
                "enable_branch_budget_split",
                "branch_budget_enforcement",
                "auto_tag_backfilled",
                "auto_prune_orphans",
                "max_changes_per_branch",
            }.isdisjoint(parameters)
        )
        self.assertTrue(upgraded_source.parameters["scope_endpoints_by_include_tags"])
        self.assertTrue(is_encrypted(upgraded_source.parameters["password"]))
        self.assertNotIn("upgrade-password", upgraded_source.parameters["password"])
        self.assertEqual(
            upgraded_source.parameters["device_tag_include_tags"],
            ["Upgrade 26 Scope"],
        )
        self.assertEqual(
            upgraded_source.parameters["device_tag_exclude_tags"],
            ["Upgrade 26 Excluded"],
        )
        self.assertNotIn("device_tag_include", upgraded_source.parameters)
        self.assertNotIn("device_tag_exclude", upgraded_source.parameters)
        self.assertNotIn(
            "scope_endpoints_by_include_tags_configured",
            upgraded_source.parameters,
        )
        self.assertFalse(
            ForwardNQEMap.objects.filter(
                name="Forward Virtual Chassis",
                enabled=True,
            ).exists()
        )
        self.assertEqual(
            set(
                ForwardManagedDeviceTag.objects.values_list(
                    "tag__slug",
                    "claim_type",
                )
            ),
            {
                ("upgrade-26-scope", "scope"),
                ("forward-backfilled", "backfilled"),
            },
        )
        self.assertFalse(
            ForwardPreservedDeviceTagAssignment.objects.filter(device=device).exists()
        )
        self.assertFalse(ForwardDeviceIdentity.objects.exists())
        self.assertFalse(ForwardDeviceTagClaim.objects.exists())
        self.assertFalse(ForwardVirtualParentClaim.objects.exists())
        from forward_netbox.models import ForwardIngestion

        upgraded_partial = ForwardIngestion.objects.get(pk=partial_ingestion.pk)
        self.assertFalse(upgraded_partial.baseline_ready)
        self.assertIsNone(upgraded_partial.merge_applied_at)
        self.assertIsNone(upgraded_partial.merge_finalized_at)
        upgraded_complete = ForwardIngestion.objects.get(pk=complete_ingestion.pk)
        self.assertTrue(upgraded_complete.baseline_ready)
        self.assertIsNotNone(upgraded_complete.merge_applied_at)
        self.assertIsNotNone(upgraded_complete.merge_finalized_at)
