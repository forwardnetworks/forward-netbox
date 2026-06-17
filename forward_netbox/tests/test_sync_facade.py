from unittest.mock import Mock

from django.test import TestCase

from forward_netbox.choices import ForwardDiffFallbackModeChoices
from forward_netbox.models import ForwardSource
from forward_netbox.models import ForwardSync
from forward_netbox.utilities.forward_api import LATEST_COLLECTED_SNAPSHOT
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
        self.assertTrue(sync.parameters["enable_bulk_orm"])
        self.assertEqual(
            sync.parameters["diff_fallback_mode"],
            ForwardDiffFallbackModeChoices.ALLOW_FALLBACK,
        )

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
            "device_tag_include_tags": ["N.Patel"],
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
            include_tags=["N.Patel"],
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
