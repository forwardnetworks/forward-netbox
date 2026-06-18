from unittest.mock import Mock

from dcim.models import Device
from django.test import TestCase
from extras.models import Tag

from forward_netbox.models import ForwardSource
from forward_netbox.models import ForwardSync
from forward_netbox.utilities.sync import ForwardSyncRunner
from forward_netbox.utilities.sync_device import apply_dcim_device


class DeviceScopeTaggingTest(TestCase):
    """When apply_device_scope_tags is enabled, synced devices are tagged in
    NetBox with their Forward device-scope include tag(s)."""

    def _source(self, *, apply_scope_tags):
        return ForwardSource.objects.create(
            name=f"scope-src-{apply_scope_tags}",
            type="saas",
            url="https://fwd.app",
            status="ready",
            parameters={
                "username": "u@example.com",
                "password": "p",
                "verify": True,
                "network_id": "net-1",
                "device_tag_include_tags": ["N.Patel"],
                "device_tag_include_match": "any",
                "apply_device_scope_tags": apply_scope_tags,
            },
        )

    def _runner(self, source):
        sync = ForwardSync.objects.create(
            name=f"scope-sync-{source.pk}",
            source=source,
            parameters={"snapshot_id": "latestProcessed"},
        )
        return ForwardSyncRunner(sync=sync, ingestion=None, client=None, logger_=Mock())

    def _row(self, name="dev-1"):
        return {
            "name": name,
            "site": "site-a",
            "site_slug": "site-a",
            "role": "role-a",
            "role_slug": "role-a",
            "role_color": "9e9e9e",
            "manufacturer": "Cisco",
            "manufacturer_slug": "cisco",
            "device_type": "model-a",
            "device_type_slug": "model-a",
            "platform": "",
            "platform_slug": "",
            "status": "active",
        }

    def test_scope_tag_applied_when_enabled(self):
        runner = self._runner(self._source(apply_scope_tags=True))
        apply_dcim_device(runner, self._row())

        device = Device.objects.get(name="dev-1")
        self.assertEqual(
            sorted(device.tags.values_list("name", flat=True)), ["N.Patel"]
        )
        self.assertEqual(Tag.objects.filter(name="N.Patel").count(), 1)

    def test_scope_tag_not_applied_when_disabled(self):
        runner = self._runner(self._source(apply_scope_tags=False))
        apply_dcim_device(runner, self._row())

        device = Device.objects.get(name="dev-1")
        self.assertEqual(list(device.tags.all()), [])
        self.assertFalse(Tag.objects.filter(name="N.Patel").exists())

    def test_reapply_does_not_duplicate_tag(self):
        runner = self._runner(self._source(apply_scope_tags=True))
        apply_dcim_device(runner, self._row())
        # Fresh runner (cleared per-device tag cache) re-applies the same device.
        runner2 = ForwardSyncRunner(
            sync=runner.sync, ingestion=None, client=None, logger_=Mock()
        )
        apply_dcim_device(runner2, self._row())

        device = Device.objects.get(name="dev-1")
        self.assertEqual(device.tags.filter(name="N.Patel").count(), 1)
