from unittest.mock import Mock
from unittest.mock import patch

from dcim.models import Device
from dcim.models import DeviceRole
from dcim.models import DeviceType
from dcim.models import Manufacturer
from dcim.models import Site
from django.test import TestCase
from extras.models import Tag

from forward_netbox.choices import ForwardSyncStatusChoices
from forward_netbox.models import ForwardIngestion
from forward_netbox.models import ForwardSource
from forward_netbox.models import ForwardSync
from forward_netbox.utilities.apply_engine_bulk import bulk_orm_apply_device
from forward_netbox.utilities.ownership import reconcile_sync_scope_tag_claims
from forward_netbox.utilities.sync import ForwardSyncRunner
from forward_netbox.utilities.sync_device import apply_dcim_device


class DeviceScopeTaggingTest(TestCase):
    """apply_device_scope_tags tags each device with exactly the include tags it
    carries. The per-device matched-tag map is resolved at fetch time and set on
    the runner (runner._scope_matched_tags) by the executor."""

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

    def _baseline(self, sync, snapshot_id="snapshot-1"):
        ForwardSync.objects.filter(pk=sync.pk).update(
            status=ForwardSyncStatusChoices.COMPLETED
        )
        sync.status = ForwardSyncStatusChoices.COMPLETED
        return ForwardIngestion.objects.create(
            sync=sync,
            snapshot_id=snapshot_id,
            baseline_ready=True,
        )

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
        runner = self._runner(sync, matched={"dev-1": ["Prod_Core"]})
        apply_dcim_device(runner, self._row())

        device = Device.objects.get(name="dev-1")
        self.assertEqual(
            sorted(device.tags.values_list("name", flat=True)), ["Prod_Core"]
        )
        self.assertEqual(Tag.objects.filter(name="Prod_Core").count(), 1)

    def test_bulk_device_applies_scope_tag_without_adapter(self):
        sync = self._sync(self._source(apply_scope_tags=True))
        runner = self._runner(sync, matched={"dev-1": ["Prod_Core"]})
        manufacturer = Manufacturer.objects.create(name="Cisco", slug="cisco")
        DeviceType.objects.create(
            manufacturer=manufacturer, model="model-a", slug="model-a"
        )
        DeviceRole.objects.create(name="role-a", slug="role-a", color="9e9e9e")
        Site.objects.create(name="site-a", slug="site-a")

        with patch(
            "forward_netbox.utilities.sync_device.apply_dcim_device",
            side_effect=AssertionError("scope tagging used per-row adapter"),
        ) as adapter:
            self.assertTrue(bulk_orm_apply_device(runner, [self._row()]))

        adapter.assert_not_called()
        device = Device.objects.get(name="dev-1")
        self.assertEqual(
            list(device.tags.values_list("name", flat=True)), ["Prod_Core"]
        )

    def test_bulk_device_scope_tag_reapply_is_write_free(self):
        sync = self._sync(self._source(apply_scope_tags=True))
        manufacturer = Manufacturer.objects.create(name="Cisco", slug="cisco")
        DeviceType.objects.create(
            manufacturer=manufacturer, model="model-a", slug="model-a"
        )
        DeviceRole.objects.create(name="role-a", slug="role-a", color="9e9e9e")
        Site.objects.create(name="site-a", slug="site-a")
        bulk_orm_apply_device(
            self._runner(sync, matched={"dev-1": ["Prod_Core"]}),
            [self._row()],
        )

        runner = self._runner(sync, matched={"dev-1": ["Prod_Core"]})
        with (
            patch.object(Device.objects, "bulk_create") as create_devices,
            patch.object(Device.objects, "bulk_update") as update_devices,
            patch("extras.models.TaggedItem.objects.bulk_create") as create_tags,
            patch.object(Device, "snapshot") as snapshot,
        ):
            self.assertTrue(bulk_orm_apply_device(runner, [self._row()]))
        create_devices.assert_not_called()
        update_devices.assert_not_called()
        create_tags.assert_not_called()
        snapshot.assert_not_called()

    def test_bulk_device_preserves_blank_serial_and_scopes_type_by_manufacturer(self):
        sync = self._sync(self._source(apply_scope_tags=False))
        juniper = Manufacturer.objects.create(name="Juniper", slug="juniper")
        juniper_type = DeviceType.objects.create(
            manufacturer=juniper,
            model="shared-model",
            slug="shared-model",
        )
        cisco = Manufacturer.objects.create(name="Cisco", slug="cisco")
        cisco_type = DeviceType.objects.create(
            manufacturer=cisco,
            model="shared-model",
            slug="shared-model",
        )
        role = DeviceRole.objects.create(name="role-a", slug="role-a", color="9e9e9e")
        site = Site.objects.create(name="site-a", slug="site-a")
        device = Device.objects.create(
            name="dev-1",
            site=site,
            role=role,
            device_type=cisco_type,
            serial="operator-serial",
            status="active",
        )
        row = {
            **self._row(),
            "manufacturer": "Juniper",
            "manufacturer_slug": "juniper",
            "device_type": "shared-model",
            "device_type_slug": "shared-model",
        }

        with patch(
            "forward_netbox.utilities.sync_device.apply_dcim_device",
            side_effect=AssertionError("manufacturer lookup used adapter"),
        ) as adapter:
            self.assertTrue(bulk_orm_apply_device(self._runner(sync), [row]))

        adapter.assert_not_called()
        device.refresh_from_db()
        self.assertEqual(device.device_type_id, juniper_type.pk)
        self.assertEqual(device.serial, "operator-serial")

    def test_scope_tag_not_applied_when_disabled(self):
        sync = self._sync(self._source(apply_scope_tags=False))
        runner = self._runner(sync, matched={"dev-1": ["Prod_Core"]})
        apply_dcim_device(runner, self._row())

        device = Device.objects.get(name="dev-1")
        self.assertEqual(list(device.tags.all()), [])
        self.assertFalse(Tag.objects.filter(name="Prod_Core").exists())

    def test_reapply_does_not_duplicate_tag(self):
        sync = self._sync(self._source(apply_scope_tags=True))
        apply_dcim_device(self._runner(sync, {"dev-1": ["Prod_Core"]}), self._row())
        # Fresh runner (cleared caches) re-applies the same device -> idempotent.
        apply_dcim_device(self._runner(sync, {"dev-1": ["Prod_Core"]}), self._row())

        device = Device.objects.get(name="dev-1")
        self.assertEqual(device.tags.filter(name="Prod_Core").count(), 1)

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
        initial_ingestion = self._baseline(sync, snapshot_id="snapshot-initial")
        reconcile_sync_scope_tag_claims(
            sync,
            {"dev-1": ["TagA", "TagB"]},
            generation=initial_ingestion.pk,
            snapshot_id=initial_ingestion.snapshot_id,
        )

        # Branch staging is additive. Post-merge ownership finalization performs
        # the globally safe removal from the exact generation.
        apply_dcim_device(self._runner(sync, {"dev-1": ["TagA"]}), self._row())
        ingestion = self._baseline(sync)
        reconcile_sync_scope_tag_claims(
            sync,
            {"dev-1": ["TagA"]},
            generation=ingestion.pk,
            snapshot_id=ingestion.snapshot_id,
        )
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

    def test_shared_managed_tag_uses_last_claim_removal(self):
        sync_a = self._sync(self._source(apply_scope_tags=True))
        sync_b = self._sync(self._source(apply_scope_tags=True))
        apply_dcim_device(self._runner(sync_a, {"dev-1": ["Prod_Core"]}), self._row())
        apply_dcim_device(self._runner(sync_b, {"dev-1": ["Prod_Core"]}), self._row())
        device = Device.objects.get(name="dev-1")
        operator = Tag.objects.create(name="Operator", slug="operator")
        device.tags.add(operator)

        ingestion_a = self._baseline(sync_a)
        ingestion_b = self._baseline(sync_b)
        reconcile_sync_scope_tag_claims(
            sync_a,
            {"dev-1": ["Prod_Core"]},
            generation=ingestion_a.pk,
            snapshot_id=ingestion_a.snapshot_id,
        )
        reconcile_sync_scope_tag_claims(
            sync_b,
            {"dev-1": ["Prod_Core"]},
            generation=ingestion_b.pk,
            snapshot_id=ingestion_b.snapshot_id,
        )

        apply_dcim_device(self._runner(sync_a, {"dev-1": []}), self._row())
        reconcile_sync_scope_tag_claims(
            sync_a,
            {},
            generation=ingestion_a.pk,
            snapshot_id=ingestion_a.snapshot_id,
        )
        self.assertEqual(
            set(device.tags.values_list("slug", flat=True)),
            {"prod_core", "operator"},
        )

        apply_dcim_device(self._runner(sync_b, {"dev-1": []}), self._row())
        reconcile_sync_scope_tag_claims(
            sync_b,
            {},
            generation=ingestion_b.pk,
            snapshot_id=ingestion_b.snapshot_id,
        )
        self.assertEqual(set(device.tags.values_list("slug", flat=True)), {"operator"})
