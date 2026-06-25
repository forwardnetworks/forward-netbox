from unittest.mock import Mock

from dcim.models import Device
from django.test import TestCase
from extras.models import Tag

from forward_netbox.models import ForwardSource
from forward_netbox.models import ForwardSync
from forward_netbox.utilities.sync import ForwardSyncRunner
from forward_netbox.utilities.sync_device import apply_dcim_device


class DeviceScopeTaggingTest(TestCase):
    """apply_device_scope_tags tags each device with exactly the include tags it
    carries. The per-device matched-tag map is resolved at fetch time and set on
    the runner (runner._scope_matched_tags) by the executor."""

    def _source(
        self, *, apply_scope_tags, include_tags=("N.Patel",), include_match="any"
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

    def _sync(self, source):
        return ForwardSync.objects.create(
            name=f"scope-sync-{source.pk}",
            source=source,
            parameters={"snapshot_id": "latestProcessed"},
        )

    def _runner(self, sync, matched=None):
        runner = ForwardSyncRunner(
            sync=sync, ingestion=None, client=None, logger_=Mock()
        )
        runner._scope_matched_tags = dict(matched or {})
        return runner

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
        sync = self._sync(self._source(apply_scope_tags=True))
        runner = self._runner(sync, matched={"dev-1": ["N.Patel"]})
        apply_dcim_device(runner, self._row())

        device = Device.objects.get(name="dev-1")
        self.assertEqual(
            sorted(device.tags.values_list("name", flat=True)), ["N.Patel"]
        )
        self.assertEqual(Tag.objects.filter(name="N.Patel").count(), 1)

    def test_scope_tag_not_applied_when_disabled(self):
        sync = self._sync(self._source(apply_scope_tags=False))
        runner = self._runner(sync, matched={"dev-1": ["N.Patel"]})
        apply_dcim_device(runner, self._row())

        device = Device.objects.get(name="dev-1")
        self.assertEqual(list(device.tags.all()), [])
        self.assertFalse(Tag.objects.filter(name="N.Patel").exists())

    def test_reapply_does_not_duplicate_tag(self):
        sync = self._sync(self._source(apply_scope_tags=True))
        apply_dcim_device(self._runner(sync, {"dev-1": ["N.Patel"]}), self._row())
        # Fresh runner (cleared caches) re-applies the same device -> idempotent.
        apply_dcim_device(self._runner(sync, {"dev-1": ["N.Patel"]}), self._row())

        device = Device.objects.get(name="dev-1")
        self.assertEqual(device.tags.filter(name="N.Patel").count(), 1)

    def test_multi_tag_any_applies_only_carried_tags(self):
        # The previously-skipped case: multiple include tags, "any" mode. The
        # device carries only TagA, so it gets only TagA — never TagB.
        sync = self._sync(
            self._source(
                apply_scope_tags=True,
                include_tags=["TagA", "TagB"],
                include_match="any",
            )
        )
        runner = self._runner(sync, matched={"dev-1": ["TagA"]})
        apply_dcim_device(runner, self._row())

        device = Device.objects.get(name="dev-1")
        self.assertEqual(sorted(device.tags.values_list("name", flat=True)), ["TagA"])
        self.assertFalse(device.tags.filter(name="TagB").exists())

    def test_multi_tag_all_mode_applies_all(self):
        # "all" mode: every in-scope device carries every include tag (the
        # resolver yields the full set), so all are applied — identical to before.
        sync = self._sync(
            self._source(
                apply_scope_tags=True,
                include_tags=["TagA", "TagB"],
                include_match="all",
            )
        )
        runner = self._runner(sync, matched={"dev-1": ["TagA", "TagB"]})
        apply_dcim_device(runner, self._row())

        device = Device.objects.get(name="dev-1")
        self.assertEqual(
            sorted(device.tags.values_list("name", flat=True)), ["TagA", "TagB"]
        )

    def test_stale_scope_tag_removed_user_tag_preserved(self):
        # A device that drops a Forward include tag between syncs loses the
        # corresponding scope tag, but unrelated (user/feature) tags are kept.
        sync = self._sync(
            self._source(
                apply_scope_tags=True,
                include_tags=["TagA", "TagB"],
                include_match="any",
            )
        )
        apply_dcim_device(self._runner(sync, {"dev-1": ["TagA", "TagB"]}), self._row())
        device = Device.objects.get(name="dev-1")
        user_tag, _ = Tag.objects.get_or_create(name="owner-x", slug="owner-x")
        device.tags.add(user_tag)
        self.assertEqual(
            sorted(device.tags.values_list("name", flat=True)),
            ["TagA", "TagB", "owner-x"],
        )

        # Re-sync: device now carries only TagA.
        apply_dcim_device(self._runner(sync, {"dev-1": ["TagA"]}), self._row())
        device.refresh_from_db()
        self.assertEqual(
            sorted(device.tags.values_list("name", flat=True)),
            ["TagA", "owner-x"],
        )

    def test_unmatched_device_gets_no_scope_tags(self):
        # A device absent from the matched map (e.g. exclude-only scope) is left
        # untagged and does not crash.
        sync = self._sync(self._source(apply_scope_tags=True))
        runner = self._runner(sync, matched={})
        apply_dcim_device(runner, self._row())

        device = Device.objects.get(name="dev-1")
        self.assertEqual(list(device.tags.all()), [])
