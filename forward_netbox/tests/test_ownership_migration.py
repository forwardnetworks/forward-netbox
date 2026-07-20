import importlib

from core.choices import JobStatusChoices
from core.models import Job
from dcim.models import Device
from dcim.models import DeviceRole
from dcim.models import DeviceType
from dcim.models import Manufacturer
from dcim.models import Site
from dcim.models import VirtualDeviceContext
from django.apps import apps
from django.contrib.auth import get_user_model
from django.contrib.contenttypes.models import ContentType
from django.test import TestCase
from extras.models import Tag

from forward_netbox.models import ForwardDeviceTagClaim
from forward_netbox.models import ForwardManagedDeviceTag
from forward_netbox.models import ForwardManagedVirtualContext
from forward_netbox.models import ForwardNQEMap
from forward_netbox.models import ForwardPreservedDeviceTagAssignment
from forward_netbox.models import ForwardSource
from forward_netbox.models import ForwardSync
from forward_netbox.models import ForwardVirtualParentClaim


class OwnershipMigrationTest(TestCase):
    def test_initialization_rejects_reserved_scope_tag_slug(self):
        ForwardSource.objects.create(
            name="migration-reserved-scope-source",
            type="saas",
            url="https://fwd.app",
            parameters={
                "network_id": "migration-reserved-scope-network",
                "apply_device_scope_tags": True,
                "device_tag_include_tags": ["Forward Backfilled"],
            },
        )
        Tag.objects.create(
            name="Forward Backfilled",
            slug="forward-backfilled",
        )
        migration = importlib.import_module(
            "forward_netbox.migrations.0035_initialize_ownership_control_plane"
        )

        with self.assertRaises(RuntimeError):
            migration.initialize_ownership_control_plane(apps, None)

        self.assertFalse(ForwardManagedDeviceTag.objects.exists())

    def test_initialization_registers_control_tags_without_inventing_claims(self):
        source = ForwardSource.objects.create(
            name="migration-source",
            type="saas",
            url="https://fwd.app",
            parameters={
                "network_id": "network-1",
                "apply_device_scope_tags": True,
                "device_tag_include_tags": ["Migration Scope"],
            },
        )
        sync = ForwardSync.objects.create(
            name="migration-sync",
            source=source,
            parameters={
                "snapshot_id": "latestProcessed",
                "auto_prune_orphans": True,
            },
        )
        manufacturer = Manufacturer.objects.create(
            name="Migration Manufacturer",
            slug="migration-manufacturer",
        )
        device_type = DeviceType.objects.create(
            manufacturer=manufacturer,
            model="Migration Model",
            slug="migration-model",
        )
        role = DeviceRole.objects.create(
            name="Migration Role",
            slug="migration-role",
        )
        site = Site.objects.create(name="Migration Site", slug="migration-site")
        parent = Device.objects.create(
            name="migration-parent",
            device_type=device_type,
            role=role,
            site=site,
        )
        child = Device.objects.create(
            name="migration-child",
            device_type=device_type,
            role=role,
            site=site,
            custom_field_data={"forward_parent_device": parent.pk},
        )
        VirtualDeviceContext.objects.create(
            device=parent,
            name=child.name,
            status="active",
        )
        scope_tag = Tag.objects.create(
            name="Migration Scope",
            slug="migration-scope",
        )
        status_tag = Tag.objects.create(
            name="Forward Backfilled",
            slug="forward-backfilled",
        )
        child.tags.add(scope_tag, status_tag)

        migration = importlib.import_module(
            "forward_netbox.migrations.0035_initialize_ownership_control_plane"
        )
        migration.initialize_ownership_control_plane(apps, None)
        migration.initialize_ownership_control_plane(apps, None)

        sync.refresh_from_db()
        self.assertNotIn("auto_prune_orphans", sync.parameters)
        self.assertEqual(
            set(ForwardManagedDeviceTag.objects.values_list("tag__slug", "claim_type")),
            {
                ("migration-scope", "scope"),
                ("forward-backfilled", "backfilled"),
            },
        )
        self.assertFalse(ForwardDeviceTagClaim.objects.exists())
        self.assertFalse(ForwardVirtualParentClaim.objects.exists())
        self.assertFalse(ForwardManagedVirtualContext.objects.exists())

        migration_26 = importlib.import_module(
            "forward_netbox.migrations.0037_merge_identity_and_assignment_provenance"
        )
        migration_26.initialize_provenance_and_remove_obsolete_state(apps, None)
        self.assertFalse(
            ForwardPreservedDeviceTagAssignment.objects.filter(
                device=child,
                tag__in=(scope_tag, status_tag),
            ).exists()
        )

    def test_2_6_normalization_preserves_budget_and_removes_obsolete_controls(self):
        source = ForwardSource.objects.create(
            name="migration-2-6-source",
            type="saas",
            url="https://fwd.app",
            parameters={"network_id": "network-2"},
        )
        sync = ForwardSync.objects.create(
            name="migration-2-6-sync",
            source=source,
            parameters={"snapshot_id": "latestProcessed"},
        )
        ForwardSource.objects.filter(pk=source.pk).update(
            parameters={
                "network_id": "network-2",
                "device_tag_include": "Prod",
                "device_tag_exclude": "Excluded",
                "scope_endpoints_by_include_tags": False,
            }
        )
        ForwardSync.objects.filter(pk=sync.pk).update(
            parameters={
                "snapshot_id": "latestProcessed",
                "max_changes_per_branch": 4321,
                "enable_branch_budget_split": True,
                "branch_budget_enforcement": "strict",
                "auto_tag_backfilled": True,
                "auto_prune_orphans": True,
                "bulk_orm_models": ["dcim.device"],
            }
        )
        virtual_chassis_type = ContentType.objects.get(
            app_label="dcim", model="virtualchassis"
        )
        query_map, _ = ForwardNQEMap.objects.update_or_create(
            name="Forward Virtual Chassis",
            netbox_model=virtual_chassis_type,
            built_in=True,
            defaults={
                "enabled": True,
                "query": 'select {device: ""}',
            },
        )
        owner = get_user_model().objects.create_user(username="migration-2-6-owner")
        Job.objects.create(
            object_type=ContentType.objects.get_for_model(ForwardSync),
            object_id=sync.pk,
            name="validation",
            status=JobStatusChoices.STATUS_SCHEDULED,
            interval=1440,
            user=owner,
            job_id="123e4567-e89b-12d3-a456-426614175410",
        )

        migration = importlib.import_module(
            "forward_netbox.migrations.0037_merge_identity_and_assignment_provenance"
        )
        migration.initialize_provenance_and_remove_obsolete_state(apps, None)

        sync.refresh_from_db()
        source.refresh_from_db()
        query_map.refresh_from_db()
        self.assertEqual(sync.parameters["max_changes_per_staging_item"], 4321)
        self.assertEqual(sync.parameters["validation_schedule_interval"], 1440)
        self.assertEqual(sync.parameters["preview_schedule_interval"], 0)
        self.assertEqual(sync.user_id, owner.pk)
        self.assertTrue(
            {
                "max_changes_per_branch",
                "enable_branch_budget_split",
                "branch_budget_enforcement",
                "auto_tag_backfilled",
                "auto_prune_orphans",
                "bulk_orm_models",
            }.isdisjoint(sync.parameters)
        )
        self.assertTrue(source.parameters["scope_endpoints_by_include_tags"])
        self.assertEqual(source.parameters["device_tag_include_tags"], ["Prod"])
        self.assertEqual(source.parameters["device_tag_exclude_tags"], ["Excluded"])
        self.assertNotIn("device_tag_include", source.parameters)
        self.assertNotIn("device_tag_exclude", source.parameters)
        self.assertNotIn(
            "scope_endpoints_by_include_tags_configured", source.parameters
        )
        self.assertFalse(query_map.enabled)
