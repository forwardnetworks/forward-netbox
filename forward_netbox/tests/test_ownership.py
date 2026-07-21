import importlib
import threading

from dcim.models import Device
from dcim.models import DeviceRole
from dcim.models import DeviceType
from dcim.models import Manufacturer
from dcim.models import Site
from dcim.models import VirtualDeviceContext
from django.core.exceptions import ValidationError
from django.db import close_old_connections
from django.db.models.deletion import ProtectedError
from django.test import TestCase
from django.test import TransactionTestCase
from extras.models import Tag
from netbox_branching.utilities import supports_branching

from forward_netbox.choices import ForwardSyncStatusChoices
from forward_netbox.models import ForwardDeviceIdentity
from forward_netbox.models import ForwardDeviceTagClaim
from forward_netbox.models import ForwardIngestion
from forward_netbox.models import ForwardManagedDeviceTag
from forward_netbox.models import ForwardManagedVirtualContext
from forward_netbox.models import ForwardOwnershipReconciliation
from forward_netbox.models import ForwardPreservedDeviceTagAssignment
from forward_netbox.models import ForwardSource
from forward_netbox.models import ForwardSync
from forward_netbox.models import ForwardVirtualParentClaim
from forward_netbox.utilities.ownership import finalize_device_tag_domain
from forward_netbox.utilities.ownership import latest_baseline_generation
from forward_netbox.utilities.ownership import mark_ownership_pending
from forward_netbox.utilities.ownership import ownership_finalization_summary
from forward_netbox.utilities.ownership import ownership_integrity_summary
from forward_netbox.utilities.ownership import OwnershipConflictError
from forward_netbox.utilities.ownership import reconcile_source_device_tag_claims
from forward_netbox.utilities.ownership import reconcile_sync_scope_tag_claims
from forward_netbox.utilities.ownership import reconcile_virtual_parent_claims
from forward_netbox.utilities.ownership import (
    release_authoritative_device_delete_ownership,
)


class OwnershipControlPlaneTest(TestCase):
    def setUp(self):
        self.source = ForwardSource.objects.create(
            name="claim-source",
            type="saas",
            url="https://fwd.app",
            parameters={
                "network_id": "network-1",
                "apply_device_scope_tags": True,
                "device_tag_include_tags": ["Claim Tag"],
            },
        )
        self.sync = ForwardSync.objects.create(
            name="claim-sync",
            source=self.source,
            status=ForwardSyncStatusChoices.COMPLETED,
            parameters={"snapshot_id": "latestProcessed"},
        )
        self.ingestion = ForwardIngestion.objects.create(
            sync=self.sync,
            snapshot_id="snapshot-1",
            baseline_ready=True,
        )
        manufacturer = Manufacturer.objects.create(
            name="Claim Manufacturer",
            slug="claim-manufacturer",
        )
        device_type = DeviceType.objects.create(
            manufacturer=manufacturer,
            model="Claim Model",
            slug="claim-model",
        )
        role = DeviceRole.objects.create(name="Claim Role", slug="claim-role")
        site = Site.objects.create(name="Claim Site", slug="claim-site")
        self.device = Device.objects.create(
            name="claim-device",
            device_type=device_type,
            role=role,
            site=site,
        )

    def _complete_status_reconciliation(self, sync, ingestion):
        domain = ForwardOwnershipReconciliation.Domain.STATUS_TAGS
        mark_ownership_pending(
            sync,
            ingestion.pk,
            ingestion.snapshot_id,
            domains=[domain],
        )
        finalize_device_tag_domain(
            sync,
            domain,
            ingestion.pk,
            ingestion.snapshot_id,
        )

    def test_claims_are_main_schema_control_plane_models(self):
        for model in (
            ForwardDeviceTagClaim,
            ForwardManagedDeviceTag,
            ForwardManagedVirtualContext,
            ForwardOwnershipReconciliation,
            ForwardVirtualParentClaim,
        ):
            with self.subTest(model=model._meta.label_lower):
                self.assertFalse(supports_branching(model))
        self.assertTrue(
            importlib.import_module(
                "forward_netbox.migrations.0034_source_ownership_provenance"
            ).fake_on_branch
        )
        self.assertTrue(
            importlib.import_module(
                "forward_netbox.migrations.0035_initialize_ownership_control_plane"
            ).fake_on_branch
        )
        self.assertTrue(
            importlib.import_module(
                "forward_netbox.migrations.0036_protect_ownership_provenance"
            ).fake_on_branch
        )

    def test_generation_claim_materializes_and_sync_delete_releases(self):
        result = reconcile_sync_scope_tag_claims(
            self.sync,
            {self.device.name: ["Claim Tag"]},
            generation=self.ingestion.pk,
            snapshot_id=self.ingestion.snapshot_id,
        )

        self.assertEqual(result["claims_added"], 1)
        claim = ForwardDeviceTagClaim.objects.get(sync=self.sync, device=self.device)
        self.assertEqual(claim.generation, self.ingestion.pk)
        self.assertEqual(claim.snapshot_id, "snapshot-1")
        self.assertTrue(self.device.tags.filter(name="Claim Tag").exists())
        summary = ownership_integrity_summary()
        self.assertEqual(summary["missing_tag_assignments"], 0)
        self.assertEqual(summary["pending_managed_tag_domains"], 0)

        self.sync.delete()

        self.assertFalse(ForwardDeviceTagClaim.objects.exists())
        self.assertFalse(self.device.tags.filter(name="Claim Tag").exists())

    def test_authoritative_delete_releases_exclusive_unclaimed_identity(self):
        identity = ForwardDeviceIdentity.objects.create(
            sync=self.sync,
            ingestion=self.ingestion,
            source_device_key=self.device.name,
            device=self.device,
        )

        result = release_authoritative_device_delete_ownership(
            self.sync,
            [self.device.pk],
        )

        self.assertEqual(result["released_device_ids"], {self.device.pk})
        self.assertEqual(result["blocked_device_ids"], set())
        self.assertFalse(ForwardDeviceIdentity.objects.filter(pk=identity.pk).exists())

    def test_authoritative_delete_preserves_claimed_identity(self):
        identity = ForwardDeviceIdentity.objects.create(
            sync=self.sync,
            ingestion=self.ingestion,
            source_device_key=self.device.name,
            device=self.device,
        )
        tag = Tag.objects.create(name="Delete Protection", slug="delete-protection")
        claim = ForwardDeviceTagClaim.objects.create(
            sync=self.sync,
            ingestion=self.ingestion,
            device=self.device,
            tag=tag,
            claim_type=ForwardDeviceTagClaim.ClaimType.SCOPE,
        )

        result = release_authoritative_device_delete_ownership(
            self.sync,
            [self.device.pk],
        )

        self.assertEqual(result["released_device_ids"], set())
        self.assertEqual(result["blocked_device_ids"], {self.device.pk})
        self.assertTrue(ForwardDeviceIdentity.objects.filter(pk=identity.pk).exists())
        self.assertTrue(ForwardDeviceTagClaim.objects.filter(pk=claim.pk).exists())

    def test_scope_names_with_same_slug_union_their_device_claims(self):
        self.source.parameters = {
            **self.source.parameters,
            "device_tag_include_tags": ["Prod Core", "Prod-Core"],
        }
        self.source.save(update_fields=["parameters"])
        second = Device.objects.create(
            name="claim-device-two",
            device_type=self.device.device_type,
            role=self.device.role,
            site=self.device.site,
        )

        result = reconcile_sync_scope_tag_claims(
            self.sync,
            {
                self.device.name: ["Prod Core"],
                second.name: ["Prod-Core"],
            },
            generation=self.ingestion.pk,
            snapshot_id=self.ingestion.snapshot_id,
        )

        tag = Tag.objects.get(slug="prod-core")
        self.assertEqual(result["claims_added"], 2)
        self.assertEqual(
            set(
                ForwardDeviceTagClaim.objects.filter(
                    sync=self.sync,
                    tag=tag,
                    claim_type="scope",
                ).values_list("device__name", flat=True)
            ),
            {self.device.name, second.name},
        )
        self.assertTrue(self.device.tags.filter(pk=tag.pk).exists())
        self.assertTrue(second.tags.filter(pk=tag.pk).exists())

    def test_scope_reconciliation_reuses_exact_name_with_legacy_slug(self):
        legacy_tag = Tag.objects.create(
            name="Claim Tag",
            slug="operator-legacy-claim-tag",
            color="00ff00",
        )
        operator_device = Device.objects.create(
            name="operator-tagged-device",
            device_type=self.device.device_type,
            role=self.device.role,
            site=self.device.site,
        )
        operator_device.tags.add(legacy_tag)

        result = reconcile_sync_scope_tag_claims(
            self.sync,
            {self.device.name: ["Claim Tag"]},
            generation=self.ingestion.pk,
            snapshot_id=self.ingestion.snapshot_id,
        )

        self.assertEqual(result["claims_added"], 1)
        self.assertEqual(Tag.objects.filter(name="Claim Tag").count(), 1)
        self.assertFalse(Tag.objects.filter(slug="claim-tag").exists())
        self.assertTrue(self.device.tags.filter(pk=legacy_tag.pk).exists())
        self.assertTrue(operator_device.tags.filter(pk=legacy_tag.pk).exists())
        self.assertTrue(
            ForwardPreservedDeviceTagAssignment.objects.filter(
                device=operator_device,
                tag=legacy_tag,
            ).exists()
        )

    def test_scope_reconciliation_rejects_split_name_and_slug_identity(self):
        name_tag = Tag.objects.create(
            name="Claim Tag",
            slug="operator-legacy-claim-tag",
        )
        slug_tag = Tag.objects.create(
            name="Different Tag",
            slug="claim-tag",
        )

        with self.assertRaisesMessage(
            OwnershipConflictError,
            "Scope tag name `Claim Tag` and normalized slug `claim-tag` identify "
            "different NetBox tags.",
        ):
            reconcile_sync_scope_tag_claims(
                self.sync,
                {self.device.name: ["Claim Tag"]},
                generation=self.ingestion.pk,
                snapshot_id=self.ingestion.snapshot_id,
            )

        self.assertFalse(ForwardDeviceTagClaim.objects.exists())
        self.assertFalse(ForwardManagedDeviceTag.objects.exists())
        self.assertTrue(Tag.objects.filter(pk=name_tag.pk).exists())
        self.assertTrue(Tag.objects.filter(pk=slug_tag.pk).exists())

    def test_blank_newer_baseline_does_not_supersede_current_generation(self):
        self._complete_status_reconciliation(self.sync, self.ingestion)
        reconcile_sync_scope_tag_claims(
            self.sync,
            {self.device.name: ["Claim Tag"]},
            generation=self.ingestion.pk,
            snapshot_id=self.ingestion.snapshot_id,
        )
        reconcile_virtual_parent_claims(
            self.sync,
            {},
            generation=self.ingestion.pk,
            snapshot_id=self.ingestion.snapshot_id,
        )
        ForwardIngestion.objects.create(
            sync=self.sync,
            snapshot_id="",
            baseline_ready=True,
        )

        self.assertEqual(
            latest_baseline_generation(self.sync),
            {
                "generation": self.ingestion.pk,
                "snapshot_id": self.ingestion.snapshot_id,
            },
        )
        self.assertTrue(ownership_finalization_summary(self.sync)["complete"])

    def test_blank_only_baseline_does_not_block_scope_materialization(self):
        incomplete_source = ForwardSource.objects.create(
            name="blank-baseline-source",
            type="saas",
            url="https://fwd.app",
            parameters={
                "network_id": "blank-baseline-network",
                "apply_device_scope_tags": True,
                "device_tag_include_tags": ["Claim Tag"],
            },
        )
        incomplete_sync = ForwardSync.objects.create(
            name="blank-baseline-sync",
            source=incomplete_source,
            parameters={"snapshot_id": "latestProcessed"},
        )
        ForwardIngestion.objects.create(
            sync=incomplete_sync,
            snapshot_id="",
            baseline_ready=True,
        )

        reconcile_sync_scope_tag_claims(
            self.sync,
            {self.device.name: ["Claim Tag"]},
            generation=self.ingestion.pk,
            snapshot_id=self.ingestion.snapshot_id,
        )

        self.assertTrue(self.device.tags.filter(slug="claim-tag").exists())

    def test_scope_reconciliation_rejects_reserved_status_tag_slug(self):
        reconcile_source_device_tag_claims(
            self.sync,
            {self.device.name},
            slug="forward-backfilled",
            name="Forward Backfilled",
            color="9e9e9e",
            description="",
            claim_type="backfilled",
            generation=self.ingestion.pk,
            snapshot_id=self.ingestion.snapshot_id,
        )
        self.source.parameters = {
            **self.source.parameters,
            "device_tag_include_tags": ["Forward Backfilled"],
        }
        self.source.save(update_fields=["parameters"])

        with self.assertRaises(ValidationError):
            reconcile_sync_scope_tag_claims(
                self.sync,
                {self.device.name: ["Forward Backfilled"]},
                generation=self.ingestion.pk,
                snapshot_id=self.ingestion.snapshot_id,
            )

        self.assertEqual(
            list(
                ForwardManagedDeviceTag.objects.filter(
                    tag__slug="forward-backfilled"
                ).values_list("claim_type", flat=True)
            ),
            ["backfilled"],
        )

    def test_ingestion_with_provenance_cannot_be_deleted(self):
        reconcile_sync_scope_tag_claims(
            self.sync,
            {self.device.name: ["Claim Tag"]},
            generation=self.ingestion.pk,
            snapshot_id=self.ingestion.snapshot_id,
        )

        with self.assertRaises(ProtectedError):
            self.ingestion.delete()

        self.assertTrue(ForwardIngestion.objects.filter(pk=self.ingestion.pk).exists())

    def test_integrity_summary_detects_cross_sync_provenance(self):
        other_source = ForwardSource.objects.create(
            name="cross-sync-source",
            type="saas",
            url="https://fwd.app",
            parameters={"network_id": "cross-sync-network"},
        )
        other_sync = ForwardSync.objects.create(
            name="cross-sync",
            source=other_source,
            parameters={"snapshot_id": "latestProcessed"},
        )
        mismatched = ForwardOwnershipReconciliation(
            sync=other_sync,
            ingestion_id=self.ingestion.pk,
            domain=ForwardOwnershipReconciliation.Domain.STATUS_TAGS,
            snapshot_id=self.ingestion.snapshot_id,
            status=ForwardOwnershipReconciliation.Status.COMPLETED,
        )
        ForwardOwnershipReconciliation.objects.bulk_create([mismatched])

        summary = ownership_integrity_summary()

        self.assertEqual(summary["provenance_sync_mismatches"], 1)

    def test_never_run_sync_does_not_block_status_materialization(self):
        never_run_source = ForwardSource.objects.create(
            name="never-run-source",
            type="saas",
            url="https://fwd.app",
            parameters={"network_id": "network-never-run"},
        )
        ForwardSync.objects.create(
            name="never-run-sync",
            source=never_run_source,
            parameters={"snapshot_id": "latestProcessed"},
        )

        reconcile_source_device_tag_claims(
            self.sync,
            {self.device.name},
            slug="forward-backfilled",
            name="Forward Backfilled",
            color="9e9e9e",
            description="",
            claim_type="backfilled",
            generation=self.ingestion.pk,
            snapshot_id=self.ingestion.snapshot_id,
        )

        self.assertTrue(self.device.tags.filter(slug="forward-backfilled").exists())

    def test_queryset_delete_routes_through_claim_release(self):
        reconcile_sync_scope_tag_claims(
            self.sync,
            {self.device.name: ["Claim Tag"]},
            generation=self.ingestion.pk,
            snapshot_id=self.ingestion.snapshot_id,
        )

        deleted, _ = ForwardSync.objects.filter(pk=self.sync.pk).delete()

        self.assertGreaterEqual(deleted, 1)
        self.assertFalse(ForwardSync.objects.filter(pk=self.sync.pk).exists())
        self.assertFalse(ForwardDeviceTagClaim.objects.exists())
        self.assertFalse(self.device.tags.filter(name="Claim Tag").exists())

    def test_source_delete_releases_claims_and_materialized_tags(self):
        reconcile_sync_scope_tag_claims(
            self.sync,
            {self.device.name: ["Claim Tag"]},
            generation=self.ingestion.pk,
            snapshot_id=self.ingestion.snapshot_id,
        )

        self.source.delete()

        self.assertFalse(ForwardSource.objects.filter(pk=self.source.pk).exists())
        self.assertFalse(ForwardSync.objects.filter(pk=self.sync.pk).exists())
        self.assertFalse(ForwardDeviceTagClaim.objects.exists())
        self.assertFalse(self.device.tags.filter(name="Claim Tag").exists())

    def test_sync_delete_preserves_unrelated_virtual_parent_conflict(self):
        child = Device.objects.create(
            name="unrelated-conflict-child",
            device_type=self.device.device_type,
            role=self.device.role,
            site=self.device.site,
        )
        parents = [
            Device.objects.create(
                name=f"unrelated-conflict-parent-{index}",
                device_type=self.device.device_type,
                role=self.device.role,
                site=self.device.site,
            )
            for index in (1, 2)
        ]
        conflict_syncs = []
        for index, parent in enumerate(parents, start=1):
            source = ForwardSource.objects.create(
                name=f"unrelated-conflict-source-{index}",
                type="saas",
                url="https://fwd.app",
                parameters={"network_id": f"unrelated-conflict-{index}"},
            )
            sync = ForwardSync.objects.create(
                name=f"unrelated-conflict-sync-{index}",
                source=source,
                parameters={"snapshot_id": "latestProcessed"},
            )
            ingestion = ForwardIngestion.objects.create(
                sync=sync,
                snapshot_id=f"unrelated-snapshot-{index}",
                baseline_ready=True,
            )
            reconcile_virtual_parent_claims(
                sync,
                {child.pk: parent.pk},
                generation=ingestion.pk,
                snapshot_id=ingestion.snapshot_id,
            )
            conflict_syncs.append(sync)

        self.sync.delete()

        self.assertFalse(ForwardSync.objects.filter(pk=self.sync.pk).exists())
        self.assertEqual(
            ForwardVirtualParentClaim.objects.filter(
                sync__in=conflict_syncs,
                device=child,
            ).count(),
            2,
        )

    def test_claimed_objects_cannot_be_deleted_behind_provenance(self):
        reconcile_sync_scope_tag_claims(
            self.sync,
            {self.device.name: ["Claim Tag"]},
            generation=self.ingestion.pk,
            snapshot_id=self.ingestion.snapshot_id,
        )
        parent = Device.objects.create(
            name="claim-parent",
            device_type=self.device.device_type,
            role=self.device.role,
            site=self.device.site,
        )
        reconcile_virtual_parent_claims(
            self.sync,
            {self.device.pk: parent.pk},
            generation=self.ingestion.pk,
            snapshot_id=self.ingestion.snapshot_id,
        )
        tag = Tag.objects.get(name="Claim Tag")
        virtual_context = VirtualDeviceContext.objects.get(
            device=parent,
            name=self.device.name,
        )

        for protected_object in (self.device, parent, tag, virtual_context):
            with self.subTest(model=protected_object._meta.label_lower):
                with self.assertRaises(ProtectedError):
                    protected_object.delete()

        self.assertEqual(ForwardDeviceTagClaim.objects.count(), 1)
        self.assertEqual(ForwardVirtualParentClaim.objects.count(), 1)

    def test_positive_scope_claim_suppresses_cross_source_out_of_scope_tag(self):
        self._complete_status_reconciliation(self.sync, self.ingestion)
        reconcile_sync_scope_tag_claims(
            self.sync,
            {self.device.name: ["Claim Tag"]},
            generation=self.ingestion.pk,
            snapshot_id=self.ingestion.snapshot_id,
        )
        other_source = ForwardSource.objects.create(
            name="status-source",
            type="saas",
            url="https://fwd.app",
            parameters={"network_id": "network-2"},
        )
        other_sync = ForwardSync.objects.create(
            name="status-sync",
            source=other_source,
            status=ForwardSyncStatusChoices.COMPLETED,
            parameters={"snapshot_id": "latestProcessed"},
        )
        other_ingestion = ForwardIngestion.objects.create(
            sync=other_sync,
            snapshot_id="snapshot-2",
            baseline_ready=True,
        )
        reconcile_source_device_tag_claims(
            other_sync,
            {self.device.name},
            slug="forward-out-of-scope",
            name="Forward Out of Scope",
            color="9e9e9e",
            description="",
            claim_type="out_of_scope",
            generation=other_ingestion.pk,
            snapshot_id=other_ingestion.snapshot_id,
        )

        self.assertTrue(
            ForwardDeviceTagClaim.objects.filter(
                sync=other_sync,
                device=self.device,
                claim_type="out_of_scope",
            ).exists()
        )
        self.assertFalse(self.device.tags.filter(slug="forward-out-of-scope").exists())

        reconcile_sync_scope_tag_claims(
            self.sync,
            {},
            generation=self.ingestion.pk,
            snapshot_id=self.ingestion.snapshot_id,
        )

        self.assertTrue(self.device.tags.filter(slug="forward-out-of-scope").exists())

    def test_status_reconciliation_rejects_unowned_reserved_tag(self):
        stale_status = Tag.objects.create(
            name="Forward Out Of Scope",
            slug="forward-out-of-scope",
            color="f44336",
        )
        customer_tag = Tag.objects.create(
            name="Customer Managed",
            slug="customer-managed",
            color="9e9e9e",
        )
        self.device.tags.add(stale_status, customer_tag)

        with self.assertRaises(OwnershipConflictError):
            reconcile_source_device_tag_claims(
                self.sync,
                set(),
                slug=stale_status.slug,
                name=stale_status.name,
                color=stale_status.color,
                description="",
                claim_type="out_of_scope",
                generation=self.ingestion.pk,
                snapshot_id=self.ingestion.snapshot_id,
            )

        self.assertTrue(self.device.tags.filter(pk=stale_status.pk).exists())
        self.assertTrue(self.device.tags.filter(pk=customer_tag.pk).exists())

    def test_customer_removal_clears_preserved_assignment_without_resurrection(self):
        tag = Tag.objects.create(
            name="Claim Tag",
            slug="claim-tag",
            color="9e9e9e",
        )
        self.device.tags.add(tag)
        reconcile_sync_scope_tag_claims(
            self.sync,
            {},
            generation=self.ingestion.pk,
            snapshot_id=self.ingestion.snapshot_id,
        )
        self.assertTrue(
            ForwardPreservedDeviceTagAssignment.objects.filter(
                device=self.device,
                tag=tag,
            ).exists()
        )

        self.device.tags.remove(tag)
        next_ingestion = ForwardIngestion.objects.create(
            sync=self.sync,
            snapshot_id="snapshot-2",
            baseline_ready=True,
        )
        reconcile_sync_scope_tag_claims(
            self.sync,
            {},
            generation=next_ingestion.pk,
            snapshot_id=next_ingestion.snapshot_id,
        )

        self.assertFalse(self.device.tags.filter(pk=tag.pk).exists())
        self.assertFalse(
            ForwardPreservedDeviceTagAssignment.objects.filter(
                device=self.device,
                tag=tag,
            ).exists()
        )

    def test_preserved_assignment_does_not_block_explicit_device_deletion(self):
        tag = Tag.objects.create(
            name="Customer Tag",
            slug="customer-tag",
            color="9e9e9e",
        )
        self.device.tags.add(tag)
        ForwardPreservedDeviceTagAssignment.objects.create(
            device=self.device,
            tag=tag,
        )

        self.device.delete()

        self.assertFalse(ForwardPreservedDeviceTagAssignment.objects.exists())

    def test_same_generation_dispatch_does_not_reset_completed_work(self):
        reconcile_virtual_parent_claims(
            self.sync,
            {},
            generation=self.ingestion.pk,
            snapshot_id=self.ingestion.snapshot_id,
        )

        mark_ownership_pending(
            self.sync,
            self.ingestion.pk,
            self.ingestion.snapshot_id,
            domains=[ForwardOwnershipReconciliation.Domain.VIRTUAL_PARENTS],
        )

        reconciliation = ForwardOwnershipReconciliation.objects.get(
            sync=self.sync,
            domain=ForwardOwnershipReconciliation.Domain.VIRTUAL_PARENTS,
        )
        self.assertEqual(
            reconciliation.status,
            ForwardOwnershipReconciliation.Status.COMPLETED,
        )

    def test_finalization_detects_actual_parent_mismatch(self):
        self._complete_status_reconciliation(self.sync, self.ingestion)
        reconcile_sync_scope_tag_claims(
            self.sync,
            {self.device.name: ["Claim Tag"]},
            generation=self.ingestion.pk,
            snapshot_id=self.ingestion.snapshot_id,
        )
        parent = Device.objects.create(
            name="finalization-parent",
            device_type=self.device.device_type,
            role=self.device.role,
            site=self.device.site,
        )
        reconcile_virtual_parent_claims(
            self.sync,
            {self.device.pk: parent.pk},
            generation=self.ingestion.pk,
            snapshot_id=self.ingestion.snapshot_id,
        )
        self.assertTrue(ownership_finalization_summary(self.sync)["complete"])
        self.device.custom_field_data["forward_parent_device"] = None
        self.device.save()

        summary = ownership_finalization_summary(self.sync)

        self.assertEqual(summary["parent_mismatches"], 1)
        self.assertFalse(summary["complete"])


class OwnershipConcurrencyTest(TransactionTestCase):
    def test_concurrent_last_claim_release_converges(self):
        manufacturer = Manufacturer.objects.create(
            name="Concurrent Manufacturer",
            slug="concurrent-manufacturer",
        )
        device_type = DeviceType.objects.create(
            manufacturer=manufacturer,
            model="Concurrent Model",
            slug="concurrent-model",
        )
        role = DeviceRole.objects.create(
            name="Concurrent Role",
            slug="concurrent-role",
        )
        site = Site.objects.create(name="Concurrent Site", slug="concurrent-site")
        device = Device.objects.create(
            name="concurrent-device",
            device_type=device_type,
            role=role,
            site=site,
        )
        syncs = []
        ingestions = []
        for index in range(2):
            source = ForwardSource.objects.create(
                name=f"concurrent-source-{index}",
                type="saas",
                url="https://fwd.app",
                parameters={
                    "network_id": f"network-{index}",
                    "apply_device_scope_tags": True,
                    "device_tag_include_tags": ["Shared Scope"],
                },
            )
            sync = ForwardSync.objects.create(
                name=f"concurrent-sync-{index}",
                source=source,
                status=ForwardSyncStatusChoices.COMPLETED,
                parameters={"snapshot_id": "latestProcessed"},
            )
            ingestion = ForwardIngestion.objects.create(
                sync=sync,
                snapshot_id="same-snapshot",
                baseline_ready=True,
            )
            reconcile_sync_scope_tag_claims(
                sync,
                {device.name: ["Shared Scope"]},
                generation=ingestion.pk,
                snapshot_id=ingestion.snapshot_id,
            )
            syncs.append(sync)
            ingestions.append(ingestion)

        barrier = threading.Barrier(2)
        failures = []

        def release(index):
            close_old_connections()
            try:
                sync = ForwardSync.objects.get(pk=syncs[index].pk)
                barrier.wait(timeout=10)
                reconcile_sync_scope_tag_claims(
                    sync,
                    {},
                    generation=ingestions[index].pk,
                    snapshot_id=ingestions[index].snapshot_id,
                )
            except Exception as exc:  # pragma: no cover - assertion reports detail
                failures.append(exc)
            finally:
                close_old_connections()

        threads = [
            threading.Thread(target=release, args=(index,)) for index in range(2)
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=20)

        self.assertFalse(any(thread.is_alive() for thread in threads))
        self.assertEqual(failures, [])
        self.assertFalse(ForwardDeviceTagClaim.objects.exists())
        self.assertFalse(device.tags.filter(name="Shared Scope").exists())
