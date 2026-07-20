from django.test import TestCase

from forward_netbox.choices import ForwardSyncStatusChoices
from forward_netbox.models import ForwardIngestion
from forward_netbox.models import ForwardSource
from forward_netbox.models import ForwardSync
from forward_netbox.utilities.post_sync import current_post_sync_snapshot
from forward_netbox.utilities.post_sync import StalePostSyncSnapshotError


class PostSyncSnapshotTest(TestCase):
    def setUp(self):
        source = ForwardSource.objects.create(
            name="post-sync-source",
            type="saas",
            url="https://fwd.app",
            parameters={"network_id": "network-1"},
        )
        self.sync = ForwardSync.objects.create(
            name="post-sync",
            source=source,
            status=ForwardSyncStatusChoices.COMPLETED,
            parameters={"snapshot_id": "latestProcessed"},
        )
        self.first = ForwardIngestion.objects.create(
            sync=self.sync,
            snapshot_id="snapshot-1",
            baseline_ready=True,
        )

    def test_latest_completed_snapshot_is_accepted(self):
        with current_post_sync_snapshot(self.sync, "snapshot-1"):
            pass

    def test_obsolete_snapshot_is_rejected(self):
        ForwardIngestion.objects.create(
            sync=self.sync,
            snapshot_id="snapshot-2",
            baseline_ready=True,
        )

        with self.assertRaises(StalePostSyncSnapshotError):
            with current_post_sync_snapshot(self.sync, "snapshot-1"):
                pass

    def test_older_generation_is_rejected_when_snapshot_id_repeats(self):
        newer = ForwardIngestion.objects.create(
            sync=self.sync,
            snapshot_id="snapshot-1",
            baseline_ready=True,
        )

        with self.assertRaises(StalePostSyncSnapshotError):
            with current_post_sync_snapshot(
                self.sync,
                "snapshot-1",
                ingestion_id=self.first.pk,
            ):
                pass

        with current_post_sync_snapshot(
            self.sync,
            "snapshot-1",
            ingestion_id=newer.pk,
        ):
            pass

    def test_snapshot_is_rejected_while_sync_is_not_completed(self):
        ForwardSync.objects.filter(pk=self.sync.pk).update(
            status=ForwardSyncStatusChoices.SYNCING
        )

        with self.assertRaises(StalePostSyncSnapshotError):
            with current_post_sync_snapshot(self.sync, "snapshot-1"):
                pass
