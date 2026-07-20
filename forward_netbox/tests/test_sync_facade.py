from unittest.mock import Mock
from unittest.mock import patch
from uuid import uuid4

from core.choices import JobStatusChoices
from core.exceptions import SyncError
from core.models import Job
from django.contrib.auth import get_user_model
from django.contrib.contenttypes.models import ContentType
from django.core.exceptions import ValidationError
from django.test import TestCase

from forward_netbox.choices import ForwardDiffFallbackModeChoices
from forward_netbox.choices import ForwardSyncStatusChoices
from forward_netbox.models import ForwardSource
from forward_netbox.models import ForwardSync
from forward_netbox.utilities.forward_api import LATEST_COLLECTED_SNAPSHOT
from forward_netbox.utilities.forward_api import LATEST_PROCESSED_SNAPSHOT
from forward_netbox.utilities.sync_facade import enqueue_sync_job
from forward_netbox.utilities.sync_facade import normalize_forward_sync
from forward_netbox.utilities.sync_facade import resolve_snapshot_id


class ForwardSyncFacadeHelperTest(TestCase):
    def setUp(self):
        self.source = ForwardSource.objects.create(
            name="source-sync-facade",
            type="saas",
            url="https://fwd.app",
            parameters={
                "username": "user@example.com",
                "password": "secret",
                "verify": True,
                "timeout": 1200,
                "network_id": "test-network",
            },
        )

    def test_normalize_forward_sync_applies_canonical_defaults(self):
        sync = ForwardSync(
            name="sync-normalize",
            source=self.source,
            auto_merge=False,
            parameters={
                "snapshot_id": LATEST_PROCESSED_SNAPSHOT,
                "auto_merge": True,
                "max_changes_per_staging_item": 0,
            },
        )

        normalize_forward_sync(sync)

        self.assertEqual(sync.parameters["max_changes_per_staging_item"], 1)
        self.assertTrue(sync.auto_merge)
        self.assertTrue(sync.parameters["enable_bulk_orm"])
        self.assertEqual(sync.parameters["validation_schedule_interval"], 0)
        self.assertEqual(sync.parameters["preview_schedule_interval"], 0)
        self.assertEqual(
            sync.parameters["diff_fallback_mode"],
            ForwardDiffFallbackModeChoices.ALLOW_FALLBACK,
        )

    def test_runtime_validation_rejects_retired_parameter_keys(self):
        sync = ForwardSync(
            name="sync-retired-key",
            source=self.source,
            parameters={
                "snapshot_id": LATEST_PROCESSED_SNAPSHOT,
                "multi_branch": False,
            },
        )

        with self.assertRaises(ValidationError):
            sync.full_clean()

    def test_normalize_forward_sync_sets_missing_bulk_orm_default(self):
        sync = ForwardSync.objects.create(
            name="sync-normalize-existing",
            source=self.source,
            parameters={
                "snapshot_id": LATEST_PROCESSED_SNAPSHOT,
                "dcim.device": True,
            },
        )
        parameters = dict(sync.parameters)
        parameters.pop("enable_bulk_orm", None)
        sync.parameters = parameters
        sync.save()

        normalize_forward_sync(sync)

        self.assertTrue(sync.parameters["enable_bulk_orm"])

    def test_enqueue_requires_durable_user_attribution(self):
        sync = ForwardSync.objects.create(
            name="sync-no-owner",
            source=self.source,
            parameters={"snapshot_id": LATEST_PROCESSED_SNAPSHOT},
        )

        with self.assertRaisesMessage(SyncError, "has no owner"):
            enqueue_sync_job(sync, adhoc=True)

    def test_manual_enqueue_persists_invoker_as_owner(self):
        sync = ForwardSync.objects.create(
            name="sync-owner-adoption",
            source=self.source,
            parameters={"snapshot_id": LATEST_PROCESSED_SNAPSHOT},
        )
        owner = get_user_model().objects.create_user(username="adopted-sync-owner")

        with patch(
            "forward_netbox.utilities.sync_facade.enqueue_forward_job",
            return_value=Mock(pk=1),
        ) as enqueue:
            enqueue_sync_job(sync, adhoc=True, user=owner)

        sync.refresh_from_db()
        self.assertEqual(sync.user_id, owner.pk)
        self.assertEqual(enqueue.call_args.kwargs["user"], owner)

    def test_first_owner_adoption_uses_database_winner_for_job_attribution(self):
        sync = ForwardSync.objects.create(
            name="sync-owner-adoption-race",
            source=self.source,
            parameters={"snapshot_id": LATEST_PROCESSED_SNAPSHOT},
        )
        stale_sync = ForwardSync.objects.get(pk=sync.pk)
        first_owner = get_user_model().objects.create_user(username="first-owner")
        second_owner = get_user_model().objects.create_user(username="second-owner")

        with patch(
            "forward_netbox.utilities.sync_facade.enqueue_forward_job",
            side_effect=(Mock(pk=1), Mock(pk=2)),
        ) as enqueue:
            enqueue_sync_job(sync, adhoc=True, user=first_owner)
            enqueue_sync_job(stale_sync, adhoc=True, user=second_owner)

        sync.refresh_from_db()
        self.assertEqual(sync.user_id, first_owner.pk)
        self.assertEqual(enqueue.call_count, 2)
        self.assertEqual(enqueue.call_args_list[0].kwargs["user"], first_owner)
        self.assertEqual(enqueue.call_args_list[1].kwargs["user"], first_owner)

    def test_manual_enqueue_reuses_pending_successor(self):
        owner = get_user_model().objects.create_user(username="queued-sync-owner")
        sync = ForwardSync.objects.create(
            name="sync-pending-successor",
            source=self.source,
            user=owner,
            parameters={"snapshot_id": LATEST_PROCESSED_SNAPSHOT},
        )
        pending = Job.objects.create(
            object_type=ContentType.objects.get_for_model(ForwardSync),
            object_id=sync.pk,
            name=f"{sync.name} - adhoc",
            status=JobStatusChoices.STATUS_PENDING,
            job_id=uuid4(),
        )

        with patch(
            "forward_netbox.utilities.sync_facade.enqueue_forward_job"
        ) as enqueue:
            result = enqueue_sync_job(sync, adhoc=True, user=owner)

        self.assertEqual(result.pk, pending.pk)
        enqueue.assert_not_called()

    def test_manual_enqueue_reuses_running_producer_without_resetting_sync(self):
        owner = get_user_model().objects.create_user(username="running-sync-owner")
        sync = ForwardSync.objects.create(
            name="sync-running-producer",
            source=self.source,
            user=owner,
            parameters={"snapshot_id": LATEST_PROCESSED_SNAPSHOT},
        )
        ForwardSync.objects.filter(pk=sync.pk).update(
            status=ForwardSyncStatusChoices.SYNCING
        )
        sync.refresh_from_db()
        running = Job.objects.create(
            object_type=ContentType.objects.get_for_model(ForwardSync),
            object_id=sync.pk,
            name=f"{sync.name} - adhoc",
            status=JobStatusChoices.STATUS_RUNNING,
            job_id=uuid4(),
        )

        with patch(
            "forward_netbox.utilities.sync_facade.enqueue_forward_job"
        ) as enqueue:
            result = enqueue_sync_job(sync, adhoc=True, user=owner)

        sync.refresh_from_db()
        self.assertEqual(result.pk, running.pk)
        self.assertEqual(sync.status, ForwardSyncStatusChoices.SYNCING)
        enqueue.assert_not_called()

    def test_active_sync_without_visible_producer_fails_closed(self):
        owner = get_user_model().objects.create_user(username="active-sync-owner")
        sync = ForwardSync.objects.create(
            name="sync-active-no-producer",
            source=self.source,
            user=owner,
            parameters={"snapshot_id": LATEST_PROCESSED_SNAPSHOT},
        )
        ForwardSync.objects.filter(pk=sync.pk).update(
            status=ForwardSyncStatusChoices.MERGING
        )
        sync.refresh_from_db()

        with self.assertRaisesMessage(SyncError, "already in progress"):
            enqueue_sync_job(sync, adhoc=True, user=owner)

        sync.refresh_from_db()
        self.assertEqual(sync.status, ForwardSyncStatusChoices.MERGING)

    def test_catchup_enqueue_ignores_only_the_current_running_producer(self):
        owner = get_user_model().objects.create_user(username="catchup-sync-owner")
        sync = ForwardSync.objects.create(
            name="sync-catchup-producer",
            source=self.source,
            user=owner,
            parameters={"snapshot_id": LATEST_PROCESSED_SNAPSHOT},
        )
        ForwardSync.objects.filter(pk=sync.pk).update(
            status=ForwardSyncStatusChoices.COMPLETED
        )
        sync.refresh_from_db()
        current = Job.objects.create(
            object_type=ContentType.objects.get_for_model(ForwardSync),
            object_id=sync.pk,
            name=f"{sync.name} - adhoc",
            status=JobStatusChoices.STATUS_RUNNING,
            job_id=uuid4(),
        )
        successor = Mock(pk=999)

        with patch(
            "forward_netbox.utilities.sync_facade.enqueue_forward_job",
            return_value=successor,
        ) as enqueue:
            result = enqueue_sync_job(
                sync,
                adhoc=True,
                user=owner,
                current_job=current,
            )

        sync.refresh_from_db()
        self.assertEqual(result.pk, successor.pk)
        self.assertEqual(sync.status, ForwardSyncStatusChoices.QUEUED)
        enqueue.assert_called_once()

    def test_resolve_snapshot_id_uses_latest_processed_snapshot_lookup(self):
        sync = ForwardSync.objects.create(
            name="sync-resolve-snapshot",
            source=self.source,
            parameters={
                "snapshot_id": LATEST_PROCESSED_SNAPSHOT,
                "dcim.device": True,
            },
        )
        client = Mock()
        client.get_latest_processed_snapshot_id.return_value = "snapshot-123"

        snapshot_id = resolve_snapshot_id(sync, client=client)

        self.assertEqual(snapshot_id, "snapshot-123")
        client.get_latest_processed_snapshot_id.assert_called_once_with("test-network")

    def test_resolve_snapshot_id_uses_latest_collected_with_tag_scope(self):
        self.source.parameters = {
            **self.source.parameters,
            "device_tag_include_tags": ["Prod_Core"],
            "device_tag_exclude_tags": ["Decommissioned"],
            "device_tag_include_match": "any",
        }
        self.source.save()
        sync = ForwardSync.objects.create(
            name="sync-resolve-latest-collected",
            source=self.source,
            parameters={
                "snapshot_id": LATEST_COLLECTED_SNAPSHOT,
                "dcim.device": True,
            },
        )
        client = Mock()
        client.get_latest_collected_snapshot_id.return_value = "snapshot-collected"

        snapshot_id = resolve_snapshot_id(sync, client=client)

        self.assertEqual(snapshot_id, "snapshot-collected")
        client.get_latest_collected_snapshot_id.assert_called_once_with(
            "test-network",
            include_tags=["Prod_Core"],
            exclude_tags=["Decommissioned"],
            include_match="any",
        )
        client.get_latest_processed_snapshot_id.assert_not_called()

    def test_resolve_snapshot_id_returns_fixed_snapshot_without_lookup(self):
        sync = ForwardSync.objects.create(
            name="sync-resolve-fixed",
            source=self.source,
            parameters={"snapshot_id": "snapshot-fixed", "dcim.device": True},
        )
        client = Mock()

        snapshot_id = resolve_snapshot_id(sync, client=client)

        self.assertEqual(snapshot_id, "snapshot-fixed")
        client.get_latest_processed_snapshot_id.assert_not_called()
        client.get_latest_collected_snapshot_id.assert_not_called()
