from unittest.mock import call
from unittest.mock import Mock
from unittest.mock import patch

from dcim.models import Device
from dcim.models import DeviceRole
from dcim.models import DeviceType
from dcim.models import Interface
from dcim.models import MACAddress
from dcim.models import Manufacturer
from dcim.models import Platform
from dcim.models import Site
from django.contrib.contenttypes.models import ContentType
from django.test import TestCase
from ipam.models import VLAN
from ipam.models import VRF

from forward_netbox.choices import forward_configured_models
from forward_netbox.management.commands.forward_smoke_sync import (
    Command as SmokeSyncCommand,
)
from forward_netbox.models import ForwardSource
from forward_netbox.models import ForwardSync
from forward_netbox.utilities import apply_engine as apply_engine_module
from forward_netbox.utilities.apply_engine import ADAPTER_MODELS_WITHOUT_BLOCKER
from forward_netbox.utilities.apply_engine import ADAPTER_REQUIRED_MODELS
from forward_netbox.utilities.apply_engine import BULK_ORM_ENABLED_MODELS
from forward_netbox.utilities.apply_engine import BULK_ORM_ENABLED_MODELS_WITHOUT_SPECS
from forward_netbox.utilities.apply_engine import select_apply_engine


class ForwardBulkOrmApplyEngineTest(TestCase):
    def setUp(self):
        self.source = ForwardSource.objects.create(
            name="apply-engine-source",
            type="saas",
            url="https://fwd.app",
            status="ready",
            parameters={
                "username": "user@example.com",
                "password": "secret",
                "verify": True,
                "network_id": "test-network",
            },
        )
        self.sync = ForwardSync.objects.create(
            name="apply-engine-sync",
            source=self.source,
            parameters={"snapshot_id": "latestProcessed"},
        )

    def _runner(self):
        runner = Mock()
        runner.logger = Mock()
        runner.events_clearer = Mock()
        runner._record_issue = Mock()
        runner._apply_model_rows = Mock()
        runner._content_type_for = lambda model: ContentType.objects.get_for_model(
            model
        )
        runner._dependency_failed = Mock(return_value=False)
        runner._mark_dependency_failed = Mock()
        return runner

    def _device(self, name="device-1"):
        manufacturer = Manufacturer.objects.create(name="Cisco", slug="cisco")
        device_type = DeviceType.objects.create(
            manufacturer=manufacturer,
            model=f"type-{name}",
            slug=f"type-{name}",
        )
        role = DeviceRole.objects.create(
            name=f"role-{name}",
            slug=f"role-{name}",
            color="9e9e9e",
        )
        site = Site.objects.create(name=f"site-{name}", slug=f"site-{name}")
        return Device.objects.create(
            name=name,
            device_type=device_type,
            role=role,
            site=site,
            status="active",
        )

    def test_bulk_orm_selected_for_safe_model(self):
        self.sync.parameters["enable_bulk_orm"] = True
        self.sync.save(update_fields=["parameters"])
        decision = select_apply_engine(
            sync=self.sync,
            model_string="dcim.site",
            backend="branching",
        ).decision
        self.assertEqual(decision.selected_engine, "bulk_orm")

    def test_bulk_orm_selected_for_platform(self):
        self.sync.parameters["enable_bulk_orm"] = True
        self.sync.save(update_fields=["parameters"])
        decision = select_apply_engine(
            sync=self.sync,
            model_string="dcim.platform",
            backend="branching",
        ).decision

        self.assertEqual(decision.selected_engine, "bulk_orm")
        self.assertEqual(decision.reason_code, "bulk_orm_enabled_safe_model_set")

    def test_fast_bootstrap_auto_enables_bulk_orm_for_safe_models(self):
        ForwardSync.objects.filter(pk=self.sync.pk).update(
            parameters={"snapshot_id": "latestProcessed"}
        )
        self.sync.refresh_from_db()
        decision = select_apply_engine(
            sync=self.sync,
            model_string="dcim.site",
            backend="fast_bootstrap",
        ).decision

        self.assertEqual(decision.selected_engine, "bulk_orm")
        self.assertEqual(
            decision.reason_code,
            "bulk_orm_auto_enabled_fast_bootstrap",
        )

    def test_branching_auto_enables_bulk_orm_for_safe_models(self):
        ForwardSync.objects.filter(pk=self.sync.pk).update(
            parameters={"snapshot_id": "latestProcessed"}
        )
        self.sync.refresh_from_db()
        decision = select_apply_engine(
            sync=self.sync,
            model_string="dcim.site",
            backend="branching",
        ).decision

        self.assertEqual(decision.selected_engine, "bulk_orm")
        self.assertEqual(
            decision.reason_code,
            "bulk_orm_auto_enabled_safe_model_set",
        )

    def test_fast_bootstrap_honors_explicit_bulk_orm_opt_out(self):
        self.sync.parameters["enable_bulk_orm"] = False
        self.sync.save(update_fields=["parameters"])
        decision = select_apply_engine(
            sync=self.sync,
            model_string="dcim.site",
            backend="fast_bootstrap",
        ).decision

        self.assertEqual(decision.selected_engine, "adapter")
        self.assertEqual(decision.reason_code, "bulk_orm_disabled_by_default")

    def test_bulk_orm_creates_and_updates_sites(self):
        self.sync.parameters["enable_bulk_orm"] = True
        self.sync.save(update_fields=["parameters"])
        Site.objects.create(name="site-a", slug="site-a")
        runner = self._runner()
        engine = select_apply_engine(
            sync=self.sync,
            model_string="dcim.site",
            backend="branching",
        )

        engine.apply_upserts(
            runner,
            "dcim.site",
            [
                {"name": "Site A", "slug": "site-a"},
                {"name": "Site B", "slug": "site-b"},
            ],
        )

        self.assertFalse(runner._apply_model_rows.called)
        self.assertEqual(Site.objects.filter(slug="site-b").count(), 1)
        self.assertEqual(Site.objects.get(slug="site-a").name, "Site A")

    def test_bulk_orm_creates_and_updates_manufacturers(self):
        self.sync.parameters["enable_bulk_orm"] = True
        self.sync.save(update_fields=["parameters"])
        Manufacturer.objects.create(name="Cisco", slug="cisco")
        runner = self._runner()
        engine = select_apply_engine(
            sync=self.sync,
            model_string="dcim.manufacturer",
            backend="branching",
        )

        engine.apply_upserts(
            runner,
            "dcim.manufacturer",
            [
                {"name": "Cisco Systems", "slug": "cisco"},
                {"name": "Juniper", "slug": "juniper"},
            ],
        )

        self.assertFalse(runner._apply_model_rows.called)
        self.assertEqual(Manufacturer.objects.get(slug="cisco").name, "Cisco Systems")
        self.assertEqual(Manufacturer.objects.filter(slug="juniper").count(), 1)

    def test_bulk_orm_creates_and_updates_device_types(self):
        self.sync.parameters["enable_bulk_orm"] = True
        self.sync.save(update_fields=["parameters"])
        manufacturer = Manufacturer.objects.create(name="Cisco", slug="cisco")
        DeviceType.objects.create(
            manufacturer=manufacturer,
            model="N9K",
            slug="n9k",
        )
        runner = self._runner()
        engine = select_apply_engine(
            sync=self.sync,
            model_string="dcim.devicetype",
            backend="branching",
        )

        engine.apply_upserts(
            runner,
            "dcim.devicetype",
            [
                {
                    "manufacturer": "Cisco",
                    "manufacturer_slug": "cisco",
                    "model": "N9K-X",
                    "slug": "n9k",
                },
                {
                    "manufacturer": "Juniper",
                    "manufacturer_slug": "juniper",
                    "model": "QFX",
                    "slug": "qfx",
                },
            ],
        )

        self.assertFalse(runner._apply_model_rows.called)
        self.assertEqual(DeviceType.objects.get(slug="n9k").model, "N9K-X")
        self.assertEqual(DeviceType.objects.filter(slug="qfx").count(), 1)

    def test_bulk_orm_creates_and_updates_mac_addresses(self):
        self.sync.parameters["enable_bulk_orm"] = True
        self.sync.save(update_fields=["parameters"])
        device = self._device()
        old_interface = Interface.objects.create(
            device=device,
            name="Ethernet1",
            type="1000base-t",
        )
        new_interface = Interface.objects.create(
            device=device,
            name="Ethernet2",
            type="1000base-t",
        )
        MACAddress.objects.create(
            mac_address="00:11:22:33:44:55",
            assigned_object_type=ContentType.objects.get_for_model(Interface),
            assigned_object_id=old_interface.pk,
        )
        runner = self._runner()
        engine = select_apply_engine(
            sync=self.sync,
            model_string="dcim.macaddress",
            backend="branching",
        )

        engine.apply_upserts(
            runner,
            "dcim.macaddress",
            [
                {
                    "device": device.name,
                    "interface": new_interface.name,
                    "mac": "00:11:22:33:44:55",
                },
                {
                    "device": device.name,
                    "interface": old_interface.name,
                    "mac": "00:11:22:33:44:66",
                },
            ],
        )

        self.assertFalse(runner._apply_model_rows.called)
        updated = MACAddress.objects.get(mac_address="00:11:22:33:44:55")
        created = MACAddress.objects.get(mac_address="00:11:22:33:44:66")
        self.assertEqual(updated.assigned_object_id, new_interface.pk)
        self.assertEqual(created.assigned_object_id, old_interface.pk)
        self.assertEqual(MACAddress.objects.count(), 2)
        runner.logger.increment_statistics.assert_any_call(
            "dcim.macaddress",
            outcome="applied",
        )

    def test_bulk_orm_mac_address_preserves_dependency_skip_behavior(self):
        self.sync.parameters["enable_bulk_orm"] = True
        self.sync.save(update_fields=["parameters"])
        device = self._device()
        runner = self._runner()
        runner._dependency_failed = Mock(return_value=True)
        engine = select_apply_engine(
            sync=self.sync,
            model_string="dcim.macaddress",
            backend="branching",
        )

        engine.apply_upserts(
            runner,
            "dcim.macaddress",
            [
                {
                    "device": device.name,
                    "interface": "Ethernet404",
                    "mac": "00:11:22:33:44:55",
                }
            ],
        )

        self.assertFalse(runner._apply_model_rows.called)
        self.assertEqual(MACAddress.objects.count(), 0)
        runner.logger.increment_statistics.assert_called_with(
            "dcim.macaddress",
            outcome="skipped",
        )
        runner._record_issue.assert_called_once()

    def test_bulk_orm_mac_address_records_missing_interface_failure(self):
        self.sync.parameters["enable_bulk_orm"] = True
        self.sync.save(update_fields=["parameters"])
        device = self._device()
        runner = self._runner()
        engine = select_apply_engine(
            sync=self.sync,
            model_string="dcim.macaddress",
            backend="branching",
        )

        engine.apply_upserts(
            runner,
            "dcim.macaddress",
            [
                {
                    "device": device.name,
                    "interface": "Ethernet404",
                    "mac": "00:11:22:33:44:55",
                }
            ],
        )

        self.assertFalse(runner._apply_model_rows.called)
        self.assertEqual(MACAddress.objects.count(), 0)
        runner.logger.increment_statistics.assert_called_with(
            "dcim.macaddress",
            outcome="failed",
        )
        runner._mark_dependency_failed.assert_called_once()
        runner._record_issue.assert_called_once()

    def test_devicerole_uses_bulk_orm_when_feature_enabled(self):
        self.sync.parameters["enable_bulk_orm"] = True
        self.sync.save(update_fields=["parameters"])
        decision = select_apply_engine(
            sync=self.sync,
            model_string="dcim.devicerole",
            backend="branching",
        ).decision

        self.assertEqual(decision.selected_engine, "bulk_orm")
        self.assertEqual(decision.reason_code, "bulk_orm_enabled_safe_model_set")

    def test_bulk_orm_enabled_models_use_bulk_orm_when_feature_enabled(self):
        self.sync.parameters["enable_bulk_orm"] = True
        self.sync.save(update_fields=["parameters"])
        self.assertEqual(BULK_ORM_ENABLED_MODELS_WITHOUT_SPECS, ())

        for model_string in sorted(BULK_ORM_ENABLED_MODELS):
            with self.subTest(model_string=model_string):
                decision = select_apply_engine(
                    sync=self.sync,
                    model_string=model_string,
                    backend="branching",
                ).decision
                self.assertEqual(decision.selected_engine, "bulk_orm")
                self.assertEqual(
                    decision.reason_code,
                    "bulk_orm_enabled_safe_model_set",
                )

    def test_bulk_orm_enabled_model_missing_spec_falls_back_to_adapter(self):
        self.sync.parameters["enable_bulk_orm"] = True
        self.sync.save(update_fields=["parameters"])
        runner = self._runner()

        with patch.object(
            apply_engine_module,
            "BULK_ORM_ENABLED_MODELS_WITHOUT_SPECS",
            ("dcim.site",),
        ):
            engine = select_apply_engine(
                sync=self.sync,
                model_string="dcim.site",
                backend="branching",
            )
            self.assertEqual(engine.decision.selected_engine, "adapter")
            self.assertEqual(
                engine.decision.reason_code,
                "bulk_orm_enabled_model_missing_spec",
            )
            engine.apply_upserts(
                runner,
                "dcim.site",
                [{"name": "Site A", "slug": "site-a"}],
            )

        runner._apply_model_rows.assert_called_once()
        runner._record_issue.assert_not_called()

    def test_adapter_required_models_stay_on_adapter_when_bulk_orm_enabled(self):
        self.sync.parameters["enable_bulk_orm"] = True
        self.sync.save(update_fields=["parameters"])
        self.assertEqual(ADAPTER_MODELS_WITHOUT_BLOCKER, ())

        for model_string in sorted(ADAPTER_REQUIRED_MODELS):
            with self.subTest(model_string=model_string):
                decision = select_apply_engine(
                    sync=self.sync,
                    model_string=model_string,
                    backend="branching",
                ).decision
                self.assertEqual(decision.selected_engine, "adapter")
                self.assertEqual(
                    decision.reason_code, "adapter_required_model_contract"
                )
                bulk_rejection = next(
                    item
                    for item in decision.rejected_engines
                    if item["engine"] == "bulk_orm"
                )
                self.assertEqual(
                    bulk_rejection["reason_code"],
                    "model_contract_requires_adapter",
                )
                self.assertTrue(bulk_rejection.get("blocker_code"))

    def test_bulk_orm_creates_and_updates_vrfs(self):
        self.sync.parameters["enable_bulk_orm"] = True
        self.sync.save(update_fields=["parameters"])
        VRF.objects.create(name="blue", rd="65000:1", description="old")
        runner = self._runner()
        engine = select_apply_engine(
            sync=self.sync,
            model_string="ipam.vrf",
            backend="branching",
        )

        engine.apply_upserts(
            runner,
            "ipam.vrf",
            [
                {
                    "name": "blue-renamed",
                    "rd": "65000:1",
                    "description": "updated",
                    "enforce_unique": False,
                },
                {
                    "name": "green",
                    "rd": None,
                    "description": "",
                    "enforce_unique": False,
                },
            ],
        )

        self.assertFalse(runner._apply_model_rows.called)
        self.assertEqual(VRF.objects.get(rd="65000:1").name, "blue-renamed")
        self.assertEqual(VRF.objects.filter(name="green").count(), 1)

    def test_bulk_orm_creates_and_updates_vlans_with_site_identity(self):
        self.sync.parameters["enable_bulk_orm"] = True
        self.sync.save(update_fields=["parameters"])
        site = Site.objects.create(name="Site A", slug="site-a")
        other_site = Site.objects.create(name="Site B", slug="site-b")
        VLAN.objects.create(site=site, vid=100, name="old", status="active")
        VLAN.objects.create(site=other_site, vid=100, name="other", status="active")
        runner = self._runner()
        engine = select_apply_engine(
            sync=self.sync,
            model_string="ipam.vlan",
            backend="branching",
        )

        engine.apply_upserts(
            runner,
            "ipam.vlan",
            [
                {
                    "site": "Site A",
                    "site_slug": "site-a",
                    "vid": 100,
                    "name": "updated",
                    "status": "active",
                },
                {
                    "site": "Site C",
                    "site_slug": "site-c",
                    "vid": "200",
                    "name": "created",
                    "status": "active",
                },
            ],
        )

        self.assertFalse(runner._apply_model_rows.called)
        self.assertEqual(VLAN.objects.get(site=site, vid=100).name, "updated")
        self.assertEqual(VLAN.objects.get(site=other_site, vid=100).name, "other")
        self.assertEqual(Site.objects.filter(slug="site-c").count(), 1)
        self.assertEqual(
            VLAN.objects.get(site__slug="site-c", vid=200).name,
            "created",
        )

    def test_bulk_orm_creates_and_updates_deviceroles(self):
        self.sync.parameters["enable_bulk_orm"] = True
        self.sync.save(update_fields=["parameters"])
        from dcim.models import DeviceRole

        DeviceRole.objects.create(name="access", slug="access", color="00aa00")
        runner = self._runner()
        engine = select_apply_engine(
            sync=self.sync,
            model_string="dcim.devicerole",
            backend="branching",
        )

        engine.apply_upserts(
            runner,
            "dcim.devicerole",
            [
                {"name": "Access", "slug": "access", "color": "ff0000"},
                {"name": "Core", "slug": "core"},
            ],
        )

        self.assertFalse(runner._apply_model_rows.called)
        self.assertEqual(DeviceRole.objects.get(slug="access").name, "Access")
        self.assertEqual(DeviceRole.objects.get(slug="access").color, "ff0000")
        self.assertEqual(DeviceRole.objects.filter(slug="core").count(), 1)
        self.assertEqual(DeviceRole.objects.get(slug="core").color, "9e9e9e")

    def test_bulk_orm_creates_and_updates_platforms(self):
        self.sync.parameters["enable_bulk_orm"] = True
        self.sync.save(update_fields=["parameters"])
        manufacturer = Manufacturer.objects.create(name="Cisco", slug="cisco")
        Platform.objects.create(
            name="NX-OS",
            slug="nxos",
            manufacturer=manufacturer,
        )
        runner = self._runner()
        engine = select_apply_engine(
            sync=self.sync,
            model_string="dcim.platform",
            backend="branching",
        )

        engine.apply_upserts(
            runner,
            "dcim.platform",
            [
                {
                    "name": "NX-OS",
                    "slug": "nxos",
                    "manufacturer": "Juniper",
                    "manufacturer_slug": "juniper",
                },
                {
                    "name": "IOS-XR",
                    "slug": "iosxr",
                    "manufacturer": "Cisco",
                    "manufacturer_slug": "cisco",
                },
            ],
        )

        self.assertFalse(runner._apply_model_rows.called)
        self.assertEqual(Platform.objects.get(slug="nxos").manufacturer.slug, "juniper")
        self.assertEqual(Platform.objects.get(slug="iosxr").manufacturer.slug, "cisco")

    def test_bulk_orm_counts_unchanged_platform_rows_as_unchanged(self):
        self.sync.parameters["enable_bulk_orm"] = True
        self.sync.save(update_fields=["parameters"])
        manufacturer = Manufacturer.objects.create(name="Cisco", slug="cisco")
        Platform.objects.create(
            name="ACI",
            slug="aci",
            manufacturer=manufacturer,
        )
        runner = self._runner()
        engine = select_apply_engine(
            sync=self.sync,
            model_string="dcim.platform",
            backend="branching",
        )

        engine.apply_upserts(
            runner,
            "dcim.platform",
            [
                {
                    "name": "ACI",
                    "slug": "aci",
                    "manufacturer": "Cisco",
                    "manufacturer_slug": "cisco",
                }
            ],
        )

        self.assertEqual(Platform.objects.filter(slug="aci").count(), 1)
        self.assertIn(
            call("dcim.platform", outcome="unchanged"),
            runner.logger.increment_statistics.mock_calls,
        )
        self.assertNotIn(
            call("dcim.platform", outcome="applied"),
            runner.logger.increment_statistics.mock_calls,
        )

    def test_bulk_orm_records_issue_for_invalid_row(self):
        self.sync.parameters["enable_bulk_orm"] = True
        self.sync.save(update_fields=["parameters"])
        runner = self._runner()
        engine = select_apply_engine(
            sync=self.sync,
            model_string="dcim.site",
            backend="branching",
        )

        engine.apply_upserts(
            runner,
            "dcim.site",
            [
                {"name": "Site A", "slug": "site-a"},
                {"name": "Missing Slug"},
            ],
        )

        self.assertEqual(Site.objects.filter(slug="site-a").count(), 1)
        runner._record_issue.assert_called()

    def test_smoke_sync_can_enable_bulk_orm_for_live_proof_runs(self):
        sync = SmokeSyncCommand()._build_sync(
            sync_name="bulk-orm-smoke-sync",
            source=self.source,
            user=None,
            snapshot_id="latestProcessed",
            selected_models=set(forward_configured_models()),
            auto_merge=True,
            execution_backend="branching",
            enable_bulk_orm=True,
            scheduler_overlap=True,
        )

        self.assertTrue(sync.parameters["enable_bulk_orm"])
        self.assertTrue(sync.parameters["scheduler_overlap"])

    @patch.dict("os.environ", {}, clear=True)
    def test_smoke_sync_defaults_safe_bulk_orm_on(self):
        self.assertTrue(SmokeSyncCommand()._enable_bulk_orm({}))

    @patch.dict("os.environ", {}, clear=True)
    def test_smoke_sync_can_disable_safe_bulk_orm_for_comparison(self):
        self.assertFalse(
            SmokeSyncCommand()._enable_bulk_orm({"disable_bulk_orm": True})
        )

    def test_smoke_sync_build_source_preserves_runtime_tuning_parameters(self):
        self.source.parameters.update(
            {
                "query_fetch_concurrency": 16,
                "nqe_page_size": 10000,
                "query_preflight_enabled": False,
                "query_preflight_row_limit": 2,
            }
        )
        self.source.save(update_fields=["parameters"])

        source = SmokeSyncCommand()._build_source(
            source_name=self.source.name,
            source_type="saas",
            url="https://fwd.app",
            username="user@example.com",
            password="secret",
            network_id="test-network",
        )

        self.assertEqual(source.parameters["query_fetch_concurrency"], 16)
        self.assertEqual(source.parameters["nqe_page_size"], 10000)
        self.assertFalse(source.parameters["query_preflight_enabled"])
        self.assertEqual(source.parameters["query_preflight_row_limit"], 2)

    def test_smoke_sync_build_sync_preserves_existing_parameters(self):
        sync = ForwardSync.objects.create(
            name="bulk-orm-smoke-sync",
            source=self.source,
            parameters={
                "bulk_orm_models": ["ipam.prefix"],
                "max_changes_per_branch": 12000,
                "dcim.site": False,
            },
        )

        updated = SmokeSyncCommand()._build_sync(
            sync_name=sync.name,
            source=self.source,
            user=None,
            snapshot_id="latestProcessed",
            selected_models={"dcim.site"},
            auto_merge=True,
            execution_backend="fast_bootstrap",
            enable_bulk_orm=True,
            scheduler_overlap=True,
        )

        self.assertEqual(updated.parameters["bulk_orm_models"], ["ipam.prefix"])
        self.assertEqual(updated.parameters["max_changes_per_branch"], 12000)
        self.assertTrue(updated.parameters["dcim.site"])
        self.assertEqual(updated.parameters["execution_backend"], "fast_bootstrap")

    def test_smoke_sync_build_sync_persists_max_changes_per_branch(self):
        sync = SmokeSyncCommand()._build_sync(
            sync_name="max-shard-smoke-sync",
            source=self.source,
            user=None,
            snapshot_id="latestProcessed",
            selected_models={"dcim.site"},
            auto_merge=True,
            execution_backend="branching",
            max_changes_per_branch=42,
            enable_bulk_orm=True,
            scheduler_overlap=False,
        )

        self.assertEqual(sync.parameters["max_changes_per_branch"], 42)
        self.assertEqual(sync.get_max_changes_per_branch(), 42)
