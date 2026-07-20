from unittest.mock import call
from unittest.mock import Mock
from unittest.mock import patch

from dcim.models import Cable
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
from ipam.models import IPAddress
from ipam.models import Prefix
from ipam.models import VLAN
from ipam.models import VRF

from forward_netbox.models import ForwardSource
from forward_netbox.models import ForwardSync
from forward_netbox.utilities import apply_engine as apply_engine_module
from forward_netbox.utilities.apply_engine import ADAPTER_MODELS_WITHOUT_BLOCKER
from forward_netbox.utilities.apply_engine import ADAPTER_REQUIRED_MODELS
from forward_netbox.utilities.apply_engine import BULK_ORM_ENABLED_MODELS
from forward_netbox.utilities.apply_engine import BULK_ORM_ENABLED_MODELS_WITHOUT_SPECS
from forward_netbox.utilities.apply_engine import select_apply_engine
from forward_netbox.utilities.apply_engine_bulk import _serializer_prefetch_fields
from forward_netbox.utilities.apply_engine_bulk import bulk_orm_apply_simple_models


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
        # Faithful to the real runner: the interface lookup caches are concrete
        # containers (bulk_orm_apply_interface refreshes them after a create).
        runner._interface_by_device_name_cache = {}
        runner._missing_interface_by_device_name_cache = set()
        runner._interface_canonical_cache = {}
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
        ).decision
        self.assertEqual(decision.selected_engine, "bulk_orm")

    def test_object_change_serializer_prefetches_all_interface_relationships(self):
        self.assertEqual(
            set(_serializer_prefetch_fields(Interface)),
            {"tags", "tagged_vlans", "vdcs", "wireless_lans"},
        )

    def test_bulk_orm_selected_for_platform(self):
        self.sync.parameters["enable_bulk_orm"] = True
        self.sync.save(update_fields=["parameters"])
        decision = select_apply_engine(
            sync=self.sync,
            model_string="dcim.platform",
        ).decision

        self.assertEqual(decision.selected_engine, "bulk_orm")
        self.assertEqual(decision.reason_code, "bulk_orm_enabled_safe_model_set")

    def test_branching_auto_enables_bulk_orm_for_safe_models(self):
        ForwardSync.objects.filter(pk=self.sync.pk).update(
            parameters={"snapshot_id": "latestProcessed"}
        )
        self.sync.refresh_from_db()
        decision = select_apply_engine(
            sync=self.sync,
            model_string="dcim.site",
        ).decision

        self.assertEqual(decision.selected_engine, "bulk_orm")
        self.assertEqual(
            decision.reason_code,
            "bulk_orm_auto_enabled_safe_model_set",
        )

    def test_bulk_orm_creates_and_updates_sites(self):
        self.sync.parameters["enable_bulk_orm"] = True
        self.sync.save(update_fields=["parameters"])
        Site.objects.create(name="site-a", slug="site-a")
        runner = self._runner()
        engine = select_apply_engine(
            sync=self.sync,
            model_string="dcim.site",
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

    def test_isolate_bulk_objects_applies_good_and_records_bad(self):
        # The per-row isolation fallback (used when a bulk write hits a DB
        # constraint and rolls back) must apply the good objects and record the
        # offending one as an issue, instead of failing the whole shard.
        from forward_netbox.utilities.apply_engine_bulk import _isolate_bulk_objects

        Site.objects.create(name="Taken", slug="taken")
        runner = self._runner()
        good = Site(name="Good", slug="good")
        bad = Site(name="Bad", slug="taken")  # duplicate slug -> IntegrityError

        _isolate_bulk_objects(Site, [good, bad], "create", runner, "dcim.site")

        self.assertEqual(Site.objects.filter(slug="good").count(), 1)
        self.assertEqual(Site.objects.filter(name="Bad").count(), 0)
        runner._record_issue.assert_called_once()

    def test_bulk_orm_creates_and_updates_manufacturers(self):
        self.sync.parameters["enable_bulk_orm"] = True
        self.sync.save(update_fields=["parameters"])
        Manufacturer.objects.create(name="Cisco", slug="cisco")
        runner = self._runner()
        engine = select_apply_engine(
            sync=self.sync,
            model_string="dcim.manufacturer",
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

    @patch(
        "forward_netbox.utilities.apply_engine_bulk._branch_is_active",
        return_value=True,
    )
    def test_bulk_orm_macaddress_duplicate_in_branch_no_crash(self, _branch):
        # Regression (full-network scale): a duplicate canonical MAC reassigned
        # across interfaces made the second row call snapshot() on the first
        # row's not-yet-saved in-memory MACAddress, which raises (tags need a pk).
        self.sync.parameters["enable_bulk_orm"] = True
        self.sync.save(update_fields=["parameters"])
        device = self._device()
        Interface.objects.create(device=device, name="Ethernet1", type="1000base-t")
        Interface.objects.create(device=device, name="Ethernet2", type="1000base-t")
        runner = self._runner()
        engine = select_apply_engine(sync=self.sync, model_string="dcim.macaddress")
        # Must not raise on the duplicate-MAC second row.
        engine.apply_upserts(
            runner,
            "dcim.macaddress",
            [
                {
                    "device": device.name,
                    "interface": "Ethernet1",
                    "mac": "00:aa:bb:cc:dd:ee",
                },
                {
                    "device": device.name,
                    "interface": "Ethernet2",
                    "mac": "00:aa:bb:cc:dd:ee",
                },
            ],
        )
        self.assertEqual(MACAddress.objects.count(), 1)

    def test_bulk_orm_mac_address_preserves_dependency_skip_behavior(self):
        self.sync.parameters["enable_bulk_orm"] = True
        self.sync.save(update_fields=["parameters"])
        device = self._device()
        runner = self._runner()
        runner._dependency_failed = Mock(return_value=True)
        runner._lookup_interface = lambda device, name: None
        engine = select_apply_engine(
            sync=self.sync,
            model_string="dcim.macaddress",
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

    def test_bulk_orm_mac_address_skips_missing_interface(self):
        # A MAC whose target interface was not imported is a BENIGN skip — the
        # same condition the IP path treats as an aggregated skip — not a hard
        # "failed" ForwardSearchError issue that paints the model red.
        self.sync.parameters["enable_bulk_orm"] = True
        self.sync.save(update_fields=["parameters"])
        device = self._device()
        runner = self._runner()
        runner._lookup_interface = lambda device, name: None
        runner._record_aggregated_skip_warning = Mock()
        engine = select_apply_engine(
            sync=self.sync,
            model_string="dcim.macaddress",
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

        self.assertEqual(MACAddress.objects.count(), 0)
        runner._record_aggregated_skip_warning.assert_called_once()
        self.assertEqual(
            runner._record_aggregated_skip_warning.call_args.kwargs["reason"],
            "missing-interface",
        )
        # NOT a hard failure: no issue row, no dependency mark.
        runner._record_issue.assert_not_called()
        runner._mark_dependency_failed.assert_not_called()

    def test_devicerole_uses_bulk_orm_when_feature_enabled(self):
        self.sync.parameters["enable_bulk_orm"] = True
        self.sync.save(update_fields=["parameters"])
        decision = select_apply_engine(
            sync=self.sync,
            model_string="dcim.devicerole",
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

    def _ipaddress_runner(self):
        runner = self._runner()
        runner._ipaddress_assignment_skip_reason = lambda address: None
        runner._record_aggregated_skip_warning = Mock()
        runner._lookup_interface = lambda device, name: None
        runner._ensure_vrf = Mock()
        return runner

    def _device_with_interface(self):
        mfr = Manufacturer.objects.create(name="MfrIP", slug="mfr-ip")
        dt = DeviceType.objects.create(manufacturer=mfr, model="dt-ip", slug="dt-ip")
        role = DeviceRole.objects.create(name="RoleIP", slug="role-ip")
        site = Site.objects.create(name="SiteIP", slug="site-ip")
        device = Device.objects.create(
            name="ip-dev", device_type=dt, role=role, site=site
        )
        interface = Interface.objects.create(
            device=device, name="Ethernet1", type="1000base-t"
        )
        return device, interface

    def test_bulk_orm_ipaddress_creates_and_updates(self):
        device, interface = self._device_with_interface()
        interface_ct = ContentType.objects.get_for_model(Interface)
        # Pre-existing IP to be updated (status + assignment).
        existing = IPAddress.objects.create(address="10.0.0.5/24", status="deprecated")

        self.sync.parameters["enable_bulk_orm"] = True
        self.sync.save(update_fields=["parameters"])

        engine = select_apply_engine(sync=self.sync, model_string="ipam.ipaddress")
        self.assertEqual(engine.decision.selected_engine, "bulk_orm")

        runner = self._ipaddress_runner()
        engine.apply_upserts(
            runner,
            "ipam.ipaddress",
            [
                {
                    "device": "ip-dev",
                    "interface": "Ethernet1",
                    "address": "10.0.0.10/24",
                    "status": "active",
                    "vrf": None,
                },
                {
                    "device": "ip-dev",
                    "interface": "Ethernet1",
                    "address": "10.0.0.5/24",
                    "status": "active",
                    "vrf": None,
                },
            ],
        )

        self.assertFalse(runner._apply_model_rows.called)
        created = IPAddress.objects.get(address="10.0.0.10/24")
        self.assertEqual(created.assigned_object_type_id, interface_ct.pk)
        self.assertEqual(created.assigned_object_id, interface.pk)
        existing.refresh_from_db()
        self.assertEqual(existing.status, "active")
        self.assertEqual(existing.assigned_object_id, interface.pk)

    @patch(
        "forward_netbox.utilities.apply_engine_bulk._branch_is_active",
        return_value=True,
    )
    def test_bulk_orm_ipaddress_duplicate_host_ip_in_branch_no_crash(self, _branch):
        # Regression (full-network scale): two rows sharing a (host_ip, vrf) key
        # (same host on different prefixes/interfaces) made the second row call
        # snapshot() on the first row's not-yet-saved in-memory IPAddress, which
        # raises "objects need a primary key before you can access their tags".
        device, _interface = self._device_with_interface()
        Interface.objects.create(device=device, name="Ethernet2", type="1000base-t")
        self.sync.parameters["enable_bulk_orm"] = True
        self.sync.save(update_fields=["parameters"])
        engine = select_apply_engine(sync=self.sync, model_string="ipam.ipaddress")
        runner = self._ipaddress_runner()
        # Must not raise on the duplicate (host_ip, vrf) second row.
        engine.apply_upserts(
            runner,
            "ipam.ipaddress",
            [
                {
                    "device": "ip-dev",
                    "interface": "Ethernet1",
                    "address": "10.9.9.9/24",
                    "status": "active",
                    "vrf": None,
                },
                {
                    "device": "ip-dev",
                    "interface": "Ethernet2",
                    "address": "10.9.9.9/32",
                    "status": "active",
                    "vrf": None,
                },
            ],
        )
        self.assertEqual(
            IPAddress.objects.filter(address__startswith="10.9.9.9").count(), 1
        )

    def test_bulk_orm_ipaddress_skips_missing_interface(self):
        self._device_with_interface()
        self.sync.parameters["enable_bulk_orm"] = True
        self.sync.save(update_fields=["parameters"])

        engine = select_apply_engine(sync=self.sync, model_string="ipam.ipaddress")
        runner = self._ipaddress_runner()
        engine.apply_upserts(
            runner,
            "ipam.ipaddress",
            [
                {
                    "device": "ip-dev",
                    "interface": "Ethernet99",
                    "address": "10.0.0.20/24",
                    "status": "active",
                    "vrf": None,
                }
            ],
        )

        self.assertFalse(IPAddress.objects.filter(address="10.0.0.20/24").exists())
        runner._record_aggregated_skip_warning.assert_called_once()

    def _interface_runner(self):
        runner = self._runner()
        runner._lookup_interface = lambda device, name: Interface.objects.filter(
            device=device, name=name
        ).first()
        return runner

    def test_bulk_orm_interface_creates_and_updates_plain(self):
        device, _ = self._device_with_interface()
        Interface.objects.create(
            device=device, name="Ethernet2", type="1000base-t", description="old"
        )
        self.sync.parameters["enable_bulk_orm"] = True
        self.sync.save(update_fields=["parameters"])

        engine = select_apply_engine(sync=self.sync, model_string="dcim.interface")
        self.assertEqual(engine.decision.selected_engine, "bulk_orm")

        runner = self._interface_runner()
        engine.apply_upserts(
            runner,
            "dcim.interface",
            [
                {
                    "device": "ip-dev",
                    "name": "Ethernet3",
                    "type": "1000base-t",
                    "enabled": True,
                    "mtu": 1500,
                },
                {
                    "device": "ip-dev",
                    "name": "Ethernet2",
                    "type": "1000base-t",
                    "enabled": True,
                    "description": "new",
                },
            ],
        )

        created = Interface.objects.get(device=device, name="Ethernet3")
        self.assertEqual(created.mtu, 1500)
        self.assertTrue(created.enabled)
        self.assertEqual(
            Interface.objects.get(device=device, name="Ethernet2").description, "new"
        )

    def test_bulk_orm_interface_batches_lag_membership(self):
        device, _ = self._device_with_interface()
        self.sync.parameters["enable_bulk_orm"] = True
        self.sync.save(update_fields=["parameters"])

        engine = select_apply_engine(sync=self.sync, model_string="dcim.interface")
        runner = self._interface_runner()
        lag_row = {
            "device": "ip-dev",
            "name": "Ethernet4",
            "type": "1000base-t",
            "enabled": True,
            "lag": "Port-Channel1",
        }
        plain_row = {
            "device": "ip-dev",
            "name": "Ethernet5",
            "type": "1000base-t",
            "enabled": True,
        }

        with patch(
            "forward_netbox.utilities.sync_interface.apply_dcim_interface",
            side_effect=AssertionError("LAG membership used adapter"),
        ) as mock_adapter:
            engine.apply_upserts(runner, "dcim.interface", [lag_row, plain_row])

        mock_adapter.assert_not_called()
        self.assertEqual(
            Interface.objects.get(device=device, name="Ethernet4").lag.name,
            "Port-Channel1",
        )
        self.assertTrue(
            Interface.objects.filter(device=device, name="Ethernet5").exists()
        )

    def test_bulk_orm_interface_converts_indirect_cabled_lag_parent_atomically(self):
        from forward_netbox.utilities.apply_engine_bulk import (
            bulk_orm_apply_interface,
        )
        from forward_netbox.utilities.sync import ForwardSyncRunner

        device, _ = self._device_with_interface()
        remote_device = Device.objects.create(
            name="ip-remote",
            device_type=device.device_type,
            role=device.role,
            site=device.site,
        )
        parent = Interface.objects.create(
            device=device,
            name="bond0",
            type="1000base-t",
        )
        remote = Interface.objects.create(
            device=remote_device,
            name="Ethernet1",
            type="1000base-t",
        )
        Cable.objects.create(a_terminations=[parent], b_terminations=[remote])
        runner = ForwardSyncRunner(
            sync=self.sync,
            ingestion=None,
            client=None,
            logger_=Mock(),
        )

        self.assertTrue(
            bulk_orm_apply_interface(
                runner,
                [
                    {
                        "device": device.name,
                        "name": "Ethernet-member",
                        "type": "1000base-t",
                        "enabled": True,
                        "lag": parent.name,
                    }
                ],
            )
        )

        parent.refresh_from_db()
        member = Interface.objects.get(device=device, name="Ethernet-member")
        self.assertEqual(parent.type, "lag")
        self.assertIsNone(parent.cable)
        self.assertEqual(member.lag_id, parent.pk)
        self.assertFalse(Cable.objects.exists())

    def test_bulk_orm_creates_and_updates_vrfs(self):
        self.sync.parameters["enable_bulk_orm"] = True
        self.sync.save(update_fields=["parameters"])
        VRF.objects.create(name="blue", rd="65000:1", description="old")
        runner = self._runner()
        engine = select_apply_engine(
            sync=self.sync,
            model_string="ipam.vrf",
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
        )

        engine.apply_upserts(
            runner,
            "dcim.platform",
            [
                {
                    "name": "NX-OS",
                    "slug": "nxos",
                    "manufacturer": None,
                    "manufacturer_slug": None,
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
        # An ambiguous Platform clears a legacy owner; an unambiguous Platform
        # resolves the same canonical Manufacturer used by DeviceType.
        self.assertIsNone(Platform.objects.get(slug="nxos").manufacturer)
        self.assertEqual(
            Platform.objects.get(slug="iosxr").manufacturer_id,
            manufacturer.pk,
        )

    def test_bulk_orm_counts_unchanged_platform_rows_as_unchanged(self):
        self.sync.parameters["enable_bulk_orm"] = True
        self.sync.save(update_fields=["parameters"])
        manufacturer = Manufacturer.objects.create(name="Cisco", slug="cisco")
        Platform.objects.create(name="ACI", slug="aci", manufacturer=manufacturer)
        runner = self._runner()
        engine = select_apply_engine(
            sync=self.sync,
            model_string="dcim.platform",
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

    def test_bulk_orm_update_uses_targeted_validation_not_full_clean(self):
        """B6: bulk engine UPDATE path calls clean_fields() + clean() instead
        of full_clean(). For existing objects, validate_unique() and
        validate_constraints() (the extra steps in full_clean()) hit the DB
        unnecessarily. Targeted validation skips them while preserving field-
        and model-level validation.

        Uses dcim.site (bulk_orm_apply_simple_models path). Lookup by slug
        finds the existing site; renaming it triggers the UPDATE code path."""
        self.sync.parameters["enable_bulk_orm"] = True
        self.sync.save(update_fields=["parameters"])
        Site.objects.create(name="Paris", slug="paris")
        runner = self._runner()
        engine = select_apply_engine(
            sync=self.sync,
            model_string="dcim.site",
        )

        full_clean_calls = []
        clean_fields_calls = []
        clean_calls = []
        original_full_clean = Site.full_clean
        original_clean_fields = Site.clean_fields
        original_clean = Site.clean

        def tracking_full_clean(self_obj, *args, **kwargs):
            full_clean_calls.append(self_obj.slug)
            return original_full_clean(self_obj, *args, **kwargs)

        def tracking_clean_fields(self_obj, *args, **kwargs):
            clean_fields_calls.append(self_obj.slug)
            return original_clean_fields(self_obj, *args, **kwargs)

        def tracking_clean(self_obj, *args, **kwargs):
            clean_calls.append(self_obj.slug)
            return original_clean(self_obj, *args, **kwargs)

        with (
            patch.object(Site, "full_clean", tracking_full_clean),
            patch.object(Site, "clean_fields", tracking_clean_fields),
            patch.object(Site, "clean", tracking_clean),
        ):
            # Lookup is by slug "paris" → finds existing; name change triggers UPDATE path.
            engine.apply_upserts(
                runner,
                "dcim.site",
                [{"name": "Paris-Renamed", "slug": "paris"}],
            )

        self.assertNotIn(
            "paris",
            full_clean_calls,
            "Bulk UPDATE must NOT call full_clean() on existing objects — "
            "full_clean() runs validate_unique() which issues extra DB queries.",
        )
        self.assertIn(
            "paris",
            clean_fields_calls,
            "Bulk UPDATE must call clean_fields() for field-level validation.",
        )
        self.assertIn(
            "paris",
            clean_calls,
            "Bulk UPDATE must call clean() for model-level validation.",
        )

    def test_bulk_prefix_vrf_ensure_does_not_clobber_existing_vrf(self):
        """Ensuring VRFs exist for prefix FK resolution must CREATE missing VRFs
        only — never upsert. An existing VRF's rd/description/enforce_unique
        (set by the ipam.vrf map) must survive a prefix bulk apply that
        references it, and a missing VRF is created on demand."""
        VRF.objects.create(
            name="blue", rd="65000:1", description="prod", enforce_unique=True
        )
        runner = self._runner()
        bulk_orm_apply_simple_models(
            runner,
            "ipam.prefix",
            [
                {"prefix": "10.0.0.0/24", "vrf": "blue", "status": "active"},
                {"prefix": "10.1.0.0/24", "vrf": "green", "status": "active"},
            ],
        )

        blue = VRF.objects.get(name="blue")
        self.assertEqual(blue.rd, "65000:1")
        self.assertEqual(blue.description, "prod")
        self.assertTrue(blue.enforce_unique)
        # Missing VRF created on demand and bound.
        green = VRF.objects.get(name="green")
        self.assertEqual(Prefix.objects.get(prefix="10.0.0.0/24").vrf_id, blue.pk)
        self.assertEqual(Prefix.objects.get(prefix="10.1.0.0/24").vrf_id, green.pk)

    def test_bulk_prefix_create_uses_batch_write_and_rebuilds_hierarchy(self):
        vrf = VRF.objects.create(name="hierarchy-vrf")
        rows = [
            {"prefix": "10.0.0.0/16", "vrf": None, "status": "active"},
            {"prefix": "10.0.1.0/24", "vrf": None, "status": "active"},
            {"prefix": "192.0.2.0/24", "vrf": vrf.name, "status": "active"},
            {"prefix": "192.0.2.0/25", "vrf": vrf.name, "status": "active"},
        ]

        with (
            patch.object(
                Prefix.objects,
                "bulk_create",
                wraps=Prefix.objects.bulk_create,
            ) as bulk_create,
            patch(
                "ipam.utils.rebuild_prefixes",
                wraps=__import__(
                    "ipam.utils", fromlist=["rebuild_prefixes"]
                ).rebuild_prefixes,
            ) as rebuild,
            patch.object(
                Prefix,
                "save",
                side_effect=AssertionError("per-prefix save used"),
            ),
            patch.object(
                Prefix,
                "refresh_from_db",
                side_effect=AssertionError("per-prefix refresh used"),
            ),
        ):
            bulk_orm_apply_simple_models(self._runner(), "ipam.prefix", rows)

        self.assertTrue(bulk_create.called)
        self.assertEqual(
            {call.args[0] for call in rebuild.call_args_list},
            {None, vrf.pk},
        )
        global_parent = Prefix.objects.get(prefix="10.0.0.0/16")
        global_child = Prefix.objects.get(prefix="10.0.1.0/24")
        vrf_parent = Prefix.objects.get(prefix="192.0.2.0/24", vrf=vrf)
        vrf_child = Prefix.objects.get(prefix="192.0.2.0/25", vrf=vrf)
        self.assertEqual((global_parent._depth, global_parent._children), (0, 1))
        self.assertEqual((global_child._depth, global_child._children), (1, 0))
        self.assertEqual((vrf_parent._depth, vrf_parent._children), (0, 1))
        self.assertEqual((vrf_child._depth, vrf_child._children), (1, 0))

    def test_bulk_prefix_rebuilds_only_vrf_with_actual_change(self):
        unchanged_vrf = VRF.objects.create(name="unchanged-vrf")
        changed_vrf = VRF.objects.create(name="changed-vrf")
        Prefix.objects.create(prefix="10.10.0.0/24", vrf=unchanged_vrf, status="active")
        Prefix.objects.create(prefix="10.20.0.0/24", vrf=changed_vrf, status="active")
        rows = [
            {
                "prefix": "10.10.0.0/24",
                "vrf": unchanged_vrf.name,
                "status": "active",
            },
            {
                "prefix": "10.20.0.0/24",
                "vrf": changed_vrf.name,
                "status": "reserved",
            },
        ]

        with patch(
            "forward_netbox.utilities.apply_engine_bulk._rebuild_prefix_hierarchies",
            wraps=__import__(
                "forward_netbox.utilities.apply_engine_bulk",
                fromlist=["_rebuild_prefix_hierarchies"],
            )._rebuild_prefix_hierarchies,
        ) as rebuild:
            bulk_orm_apply_simple_models(self._runner(), "ipam.prefix", rows)

        rebuild.assert_called_once()
        self.assertEqual(rebuild.call_args.args[0], {changed_vrf.pk})

    def test_bulk_simple_models_reapply_fk_no_churn(self):
        """The bulk simple-models update comparison compares relations by id (via
        the shared value matcher), so re-applying an unchanged row with an FK
        does not re-write it or lazily refetch the related object."""
        Site.objects.create(name="VlanSite", slug="vlan-site")
        rows = [
            {
                "name": "V10",
                "vid": 10,
                "status": "active",
                "site": "VlanSite",
                "site_slug": "vlan-site",
            }
        ]
        bulk_orm_apply_simple_models(self._runner(), "ipam.vlan", rows)  # create

        runner2 = self._runner()
        with patch.object(VLAN.objects, "bulk_update") as mock_update:
            bulk_orm_apply_simple_models(runner2, "ipam.vlan", rows)
            mock_update.assert_not_called()
        runner2.logger.increment_statistics.assert_any_call(
            "ipam.vlan", outcome="unchanged"
        )

    def test_bulk_orm_create_uses_full_clean(self):
        """Bulk CREATE path must keep full_clean() — new objects need
        validate_unique() to catch uniqueness violations before insertion."""
        self.sync.parameters["enable_bulk_orm"] = True
        self.sync.save(update_fields=["parameters"])
        runner = self._runner()
        engine = select_apply_engine(
            sync=self.sync,
            model_string="dcim.site",
        )

        full_clean_calls = []
        original_full_clean = Site.full_clean

        def tracking_full_clean(self_obj, *args, **kwargs):
            full_clean_calls.append(self_obj.name)
            return original_full_clean(self_obj, *args, **kwargs)

        with patch.object(Site, "full_clean", tracking_full_clean):
            engine.apply_upserts(
                runner,
                "dcim.site",
                [{"name": "Tokyo", "slug": "tokyo"}],
            )

        self.assertIn(
            "Tokyo",
            full_clean_calls,
            "Bulk CREATE must call full_clean() to catch uniqueness violations.",
        )

    def test_suppress_ingest_signals_disconnects_notify_object_changed(self):
        """suppress_ingest_side_effect_signals() must disconnect
        notify_object_changed from post_save so notification DB queries don't
        fire per-object during ingest. Verified by comparing the post_save
        receiver count before, inside, and after the context manager — inside
        the context, the count must be lower (notify_object_changed removed),
        and it must be restored on exit."""
        from django.db.models import signals as django_signals

        from forward_netbox.utilities.ingestion_merge import (
            suppress_ingest_side_effect_signals,
        )

        try:
            from extras.signals import notify_object_changed  # noqa: F401
        except ImportError:
            self.skipTest("notify_object_changed not available in this NetBox version")

        def live_receiver_count():
            django_signals.post_save._clear_dead_receivers()
            return len(django_signals.post_save.receivers)

        receivers_before = live_receiver_count()

        with suppress_ingest_side_effect_signals():
            receivers_inside = live_receiver_count()

        receivers_after = live_receiver_count()

        self.assertLess(
            receivers_inside,
            receivers_before,
            "Inside suppress_ingest_side_effect_signals(), post_save must have "
            "fewer receivers (notify_object_changed + others disconnected).",
        )
        self.assertEqual(
            receivers_after,
            receivers_before,
            "After suppress_ingest_side_effect_signals() exits, post_save "
            "receivers must be fully restored.",
        )
