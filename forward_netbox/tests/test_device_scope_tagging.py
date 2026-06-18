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

    def _source(
        self, *, apply_scope_tags, include_tags=("Prod_Core",), include_match="any"
    ):
        return ForwardSource.objects.create(
            name=f"scope-src-{ForwardSource.objects.count()}",
            type="saas",
            url="https://fwd.app",
            status="ready",
            parameters={
                "username": "u@example.com",
                "password": "p",
                "verify": True,
                "network_id": "net-1",
                "device_tag_include_tags": list(include_tags),
                "device_tag_include_match": include_match,
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
            sorted(device.tags.values_list("name", flat=True)), ["Prod_Core"]
        )
        self.assertEqual(Tag.objects.filter(name="Prod_Core").count(), 1)

    def test_scope_tag_not_applied_when_disabled(self):
        runner = self._runner(self._source(apply_scope_tags=False))
        apply_dcim_device(runner, self._row())

        device = Device.objects.get(name="dev-1")
        self.assertEqual(list(device.tags.all()), [])
        self.assertFalse(Tag.objects.filter(name="Prod_Core").exists())

    def test_reapply_does_not_duplicate_tag(self):
        runner = self._runner(self._source(apply_scope_tags=True))
        apply_dcim_device(runner, self._row())
        # Fresh runner (cleared per-device tag cache) re-applies the same device.
        runner2 = ForwardSyncRunner(
            sync=runner.sync, ingestion=None, client=None, logger_=Mock()
        )
        apply_dcim_device(runner2, self._row())

        device = Device.objects.get(name="dev-1")
        self.assertEqual(device.tags.filter(name="Prod_Core").count(), 1)

    def test_multi_tag_any_mode_skips_tagging(self):
        # In "any" mode with multiple include tags, a device may match only one;
        # the row does not carry its Forward tags, so tagging is skipped to avoid
        # applying a tag the device does not have.
        source = self._source(
            apply_scope_tags=True,
            include_tags=["TagA", "TagB"],
            include_match="any",
        )
        runner = self._runner(source)
        apply_dcim_device(runner, self._row())

        device = Device.objects.get(name="dev-1")
        self.assertEqual(list(device.tags.all()), [])
        self.assertFalse(Tag.objects.filter(name__in=["TagA", "TagB"]).exists())

    def test_multi_tag_all_mode_applies_all(self):
        # In "all" mode every in-scope device carries every include tag, so all
        # are applied.
        source = self._source(
            apply_scope_tags=True,
            include_tags=["TagA", "TagB"],
            include_match="all",
        )
        runner = self._runner(source)
        apply_dcim_device(runner, self._row())

        device = Device.objects.get(name="dev-1")
        self.assertEqual(
            sorted(device.tags.values_list("name", flat=True)), ["TagA", "TagB"]
        )
