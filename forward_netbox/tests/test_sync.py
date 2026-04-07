from unittest.mock import Mock
from unittest.mock import patch

from dcim.models import Device
from dcim.models import DeviceRole
from dcim.models import DeviceType
from dcim.models import Interface
from dcim.models import Manufacturer
from dcim.models import Site
from dcim.models import VirtualChassis
from django.test import TestCase

from forward_netbox.choices import FORWARD_SUPPORTED_MODELS
from forward_netbox.exceptions import ForwardQueryError
from forward_netbox.models import ForwardIngestion
from forward_netbox.models import ForwardSource
from forward_netbox.models import ForwardSync
from forward_netbox.utilities.forward_api import LATEST_PROCESSED_SNAPSHOT
from forward_netbox.utilities.query_registry import QuerySpec
from forward_netbox.utilities.sync import ForwardSyncRunner


class ForwardSyncRunnerTest(TestCase):
    def setUp(self):
        self.source = ForwardSource.objects.create(
            name="source-1",
            type="saas",
            url="https://fwd.app",
            parameters={
                "username": "user@example.com",
                "password": "secret",
                "verify": True,
                "timeout": 60,
                "network_id": "235937",
            },
        )
        self.sync = ForwardSync.objects.create(
            name="sync-1",
            source=self.source,
            auto_merge=False,
            parameters={
                "snapshot_id": LATEST_PROCESSED_SNAPSHOT,
                "dcim.device": True,
            },
        )

    def test_lookup_interface_requires_exact_name(self):
        site = Site.objects.create(name="site-1", slug="site-1")
        manufacturer = Manufacturer.objects.create(name="vendor-1", slug="vendor-1")
        role = DeviceRole.objects.create(name="role-1", slug="role-1", color="9e9e9e")
        device_type = DeviceType.objects.create(
            manufacturer=manufacturer,
            model="model-1",
            slug="model-1",
        )
        device = Device.objects.create(
            name="device-1",
            site=site,
            role=role,
            device_type=device_type,
            status="active",
        )
        interface = Interface.objects.create(
            device=device,
            name="Ethernet1/1",
            type="1000base-t",
        )
        runner = ForwardSyncRunner(
            sync=self.sync, ingestion=None, client=None, logger_=Mock()
        )

        self.assertEqual(runner._lookup_interface(device, "Ethernet1/1"), interface)
        self.assertIsNone(runner._lookup_interface(device, "ethernet1/1"))

    def test_ensure_device_type_reuses_existing_slug_match(self):
        manufacturer = Manufacturer.objects.create(name="Cisco", slug="cisco")
        existing = DeviceType.objects.create(
            manufacturer=manufacturer,
            model="legacy-c4507",
            slug="ws-c4507r-e",
            part_number="legacy-c4507",
        )
        runner = ForwardSyncRunner(
            sync=self.sync, ingestion=None, client=None, logger_=Mock()
        )

        device_type = runner._ensure_device_type(
            {
                "manufacturer": "Cisco",
                "manufacturer_slug": "cisco",
                "model": "WS-C4507R-E",
                "slug": "ws-c4507r-e",
                "part_number": "WS-C4507R-E",
            }
        )
        existing.refresh_from_db()

        self.assertEqual(device_type.pk, existing.pk)
        self.assertEqual(existing.model, "WS-C4507R-E")
        self.assertEqual(existing.slug, "ws-c4507r-e")
        self.assertEqual(existing.part_number, "WS-C4507R-E")

    def test_ensure_device_type_rejects_conflicting_model_and_slug_matches(self):
        manufacturer = Manufacturer.objects.create(name="Cisco", slug="cisco")
        DeviceType.objects.create(
            manufacturer=manufacturer,
            model="WS-C4507R-E",
            slug="ws-c4507r-e-legacy",
        )
        DeviceType.objects.create(
            manufacturer=manufacturer,
            model="legacy-c4507",
            slug="ws-c4507r-e",
        )
        runner = ForwardSyncRunner(
            sync=self.sync, ingestion=None, client=None, logger_=Mock()
        )

        with self.assertRaisesMessage(
            ForwardQueryError,
            "Conflicting NetBox device types already exist",
        ):
            runner._ensure_device_type(
                {
                    "manufacturer": "Cisco",
                    "manufacturer_slug": "cisco",
                    "model": "WS-C4507R-E",
                    "slug": "ws-c4507r-e",
                    "part_number": "WS-C4507R-E",
                }
            )

    def test_apply_device_uses_manufacturer_specific_device_type(self):
        Manufacturer.objects.create(name="Juniper", slug="juniper")
        DeviceType.objects.create(
            manufacturer=Manufacturer.objects.get(name="Juniper"),
            model="shared-model",
            slug="shared-model",
        )
        manufacturer = Manufacturer.objects.create(name="Cisco", slug="cisco")
        expected_device_type = DeviceType.objects.create(
            manufacturer=manufacturer,
            model="shared-model",
            slug="shared-model",
            part_number="shared-model",
        )
        runner = ForwardSyncRunner(
            sync=self.sync, ingestion=None, client=None, logger_=Mock()
        )

        runner._apply_dcim_device(
            {
                "name": "device-1",
                "manufacturer": "Cisco",
                "manufacturer_slug": "cisco",
                "device_type": "shared-model",
                "device_type_slug": "shared-model",
                "site": "site-1",
                "site_slug": "site-1",
                "role": "switch",
                "role_slug": "switch",
                "role_color": "9e9e9e",
                "status": "active",
            }
        )

        device = Device.objects.get(name="device-1")
        self.assertEqual(device.device_type.pk, expected_device_type.pk)
        expected_device_type.refresh_from_db()
        self.assertEqual(expected_device_type.part_number, "shared-model")

    def test_run_persists_latest_processed_snapshot_metadata(self):
        ingestion = ForwardIngestion.objects.create(sync=self.sync)
        client = Mock()
        client.get_latest_processed_snapshot.return_value = {
            "id": "1248264",
            "state": "PROCESSED",
            "createdAt": "2026-03-31T12:00:00Z",
            "processedAt": "2026-03-31T12:15:00Z",
        }
        client.get_snapshot_metrics.return_value = {
            "snapshotState": "PROCESSED",
            "numSuccessfulDevices": 122,
            "numSuccessfulEndpoints": 1213,
        }
        logger = Mock()
        runner = ForwardSyncRunner(
            sync=self.sync,
            ingestion=ingestion,
            client=client,
            logger_=logger,
        )

        self.sync.get_model_strings = lambda: []
        self.sync.resolve_snapshot_id = lambda client=None: "1248264"

        runner.run()
        ingestion.refresh_from_db()

        self.assertEqual(ingestion.snapshot_selector, LATEST_PROCESSED_SNAPSHOT)
        self.assertEqual(ingestion.snapshot_id, "1248264")
        self.assertEqual(
            ingestion.snapshot_info,
            {
                "id": "1248264",
                "state": "PROCESSED",
                "createdAt": "2026-03-31T12:00:00Z",
                "processedAt": "2026-03-31T12:15:00Z",
            },
        )
        self.assertEqual(
            ingestion.snapshot_metrics,
            {
                "snapshotState": "PROCESSED",
                "numSuccessfulDevices": 122,
                "numSuccessfulEndpoints": 1213,
            },
        )
        client.get_latest_processed_snapshot.assert_called_once_with("235937")
        client.get_snapshot_metrics.assert_called_once_with("1248264")

    def test_run_fetches_all_pages_for_sync_queries(self):
        ingestion = ForwardIngestion.objects.create(sync=self.sync)
        client = Mock()
        client.get_latest_processed_snapshot.return_value = {
            "id": "1248264",
            "processedAt": "2026-03-31T12:15:00Z",
        }
        client.get_snapshot_metrics.return_value = {}
        client.run_nqe_query.return_value = []
        runner = ForwardSyncRunner(
            sync=self.sync,
            ingestion=ingestion,
            client=client,
            logger_=Mock(),
        )
        runner._apply_model_rows = Mock()

        self.sync.get_model_strings = lambda: ["dcim.device"]
        self.sync.resolve_snapshot_id = lambda client=None: "1248264"

        with patch(
            "forward_netbox.utilities.sync.get_query_specs",
            return_value=[
                QuerySpec(
                    model_string="dcim.device",
                    query_name="Forward Devices",
                    query="foreach device select {name: device.name}",
                )
            ],
        ):
            runner.run()

        client.run_nqe_query.assert_called_once_with(
            query="foreach device select {name: device.name}",
            query_id=None,
            commit_id=None,
            snapshot_id="1248264",
            parameters={},
            fetch_all=True,
        )

    def test_runner_defines_adapter_for_all_supported_models(self):
        runner = ForwardSyncRunner(
            sync=self.sync, ingestion=None, client=None, logger_=Mock()
        )

        for model_string in FORWARD_SUPPORTED_MODELS:
            handler_name = f"_apply_{model_string.replace('.', '_')}"
            self.assertTrue(
                hasattr(runner, handler_name),
                msg=f"Missing adapter handler for {model_string}",
            )

    def test_apply_virtual_chassis_attaches_device_without_inventing_position(self):
        site = Site.objects.create(name="site-1", slug="site-1")
        manufacturer = Manufacturer.objects.create(name="vendor-1", slug="vendor-1")
        role = DeviceRole.objects.create(name="role-1", slug="role-1", color="9e9e9e")
        device_type = DeviceType.objects.create(
            manufacturer=manufacturer,
            model="model-1",
            slug="model-1",
        )
        device = Device.objects.create(
            name="device-1",
            site=site,
            role=role,
            device_type=device_type,
            status="active",
        )
        runner = ForwardSyncRunner(
            sync=self.sync, ingestion=None, client=None, logger_=Mock()
        )

        vc = runner._apply_dcim_virtualchassis(
            {
                "device": device.name,
                "vc_name": "site-1-mlag-device-1--device-2",
                "vc_domain": "device-1--device-2",
            }
        )
        device.refresh_from_db()

        self.assertEqual(vc.name, "site-1-mlag-device-1--device-2")
        self.assertEqual(vc.domain, "device-1--device-2")
        self.assertEqual(device.virtual_chassis, vc)
        self.assertIsNone(device.vc_position)

    def test_apply_virtual_chassis_uses_supplied_position(self):
        site = Site.objects.create(name="site-2", slug="site-2")
        manufacturer = Manufacturer.objects.create(name="vendor-2", slug="vendor-2")
        role = DeviceRole.objects.create(name="role-2", slug="role-2", color="9e9e9e")
        device_type = DeviceType.objects.create(
            manufacturer=manufacturer,
            model="model-2",
            slug="model-2",
        )
        device = Device.objects.create(
            name="device-2",
            site=site,
            role=role,
            device_type=device_type,
            status="active",
        )
        runner = ForwardSyncRunner(
            sync=self.sync, ingestion=None, client=None, logger_=Mock()
        )

        vc = runner._apply_dcim_virtualchassis(
            {
                "device": device.name,
                "vc_name": "site-2-vpc-100",
                "vc_domain": "100",
                "vc_position": 2,
            }
        )
        device.refresh_from_db()

        self.assertEqual(device.virtual_chassis, vc)
        self.assertEqual(device.vc_position, 2)
        self.assertEqual(VirtualChassis.objects.get(pk=vc.pk).domain, "100")
