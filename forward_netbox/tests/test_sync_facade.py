from unittest.mock import Mock

from django.test import TestCase

from forward_netbox.models import ForwardSource
from forward_netbox.models import ForwardSync
from forward_netbox.utilities.forward_api import LATEST_PROCESSED_SNAPSHOT
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

    def test_normalize_forward_sync_forces_native_branching(self):
        sync = ForwardSync(
            name="sync-normalize",
            source=self.source,
            auto_merge=False,
            parameters={
                "snapshot_id": LATEST_PROCESSED_SNAPSHOT,
                "multi_branch": False,
                "auto_merge": True,
                "max_changes_per_branch": 0,
            },
        )

        normalize_forward_sync(sync)

        self.assertTrue(sync.parameters["multi_branch"])
        self.assertEqual(sync.parameters["max_changes_per_branch"], 1)
        self.assertTrue(sync.auto_merge)

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
