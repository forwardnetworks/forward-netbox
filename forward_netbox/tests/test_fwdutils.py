from datetime import datetime, timezone

import httpx
from types import SimpleNamespace
from django.test import SimpleTestCase, TestCase

from forward_netbox.utilities.fwdutils import ForwardRESTClient
from forward_netbox.exceptions import ForwardAPIError
from forward_netbox.utilities.fwdutils import ForwardSyncRunner
from dcim.models import (
    Manufacturer,
    DeviceRole,
    DeviceType,
    Device,
    Interface,
    VirtualChassis,
    InventoryItem,
    Site,
)
from dcim.choices import InterfaceTypeChoices
from tenancy.models import Tenant
from ipam.models import VRF


class ForwardRESTClientSnapshotTest(SimpleTestCase):
    def _make_client(self, response_json, *, path_expected):
        expected_auth = "Basic test-token"

        def handler(request: httpx.Request) -> httpx.Response:
            assert request.method == "GET"
            assert request.url.path == path_expected
            assert request.headers.get("Authorization") == expected_auth
            return httpx.Response(200, json=response_json)

        transport = httpx.MockTransport(handler)
        session = httpx.Client(base_url="https://example.com", transport=transport)
        self.addCleanup(session.close)
        return ForwardRESTClient(
            base_url="https://example.com",
            token="test-token",
            verify=True,
            network_id="12345",
            session=session,
        )

    def test_list_snapshots_for_network(self):
        response = {
            "name": "Hybrid Cloud Demo",
            "snapshots": [
                {
                    "id": "753593",
                    "state": "PROCESSED",
                    "creationDateMillis": 1714153610277,
                    "processedAtMillis": 1758101256253,
                }
            ],
        }
        client = self._make_client(response, path_expected="/api/networks/12345/snapshots")

        snapshots = client.list_snapshots()

        self.assertEqual(len(snapshots), 1)
        snap = snapshots[0]
        self.assertEqual(snap["snapshot_id"], "753593")
        self.assertEqual(snap["status"], "loaded")
        self.assertEqual(snap["name"], "Hybrid Cloud Demo - 753593")
        self.assertEqual(snap["network_id"], "12345")
        self.assertEqual(
            snap["start"],
            datetime.fromtimestamp(1714153610277 / 1000, tz=timezone.utc).isoformat(),
        )
        self.assertEqual(
            snap["end"],
            datetime.fromtimestamp(1758101256253 / 1000, tz=timezone.utc).isoformat(),
        )

    def test_list_snapshots_without_network_raises(self):
        client = ForwardRESTClient(
            base_url="https://example.com",
            token=None,
            verify=True,
            network_id=None,
        )
        self.addCleanup(client.close)

        with self.assertRaises(ForwardAPIError):
            client.list_snapshots()


class _DummyLogger:
    def log_info(self, *args, **kwargs):
        pass

    def log_warning(self, *args, **kwargs):
        pass

    def log_failure(self, *args, **kwargs):
        pass

    def init_statistics(self, *args, **kwargs):
        pass

    def increment_statistics(self, *args, **kwargs):
        pass


class _DummySync:
    def __init__(self):
        self.logger = _DummyLogger()
        self.snapshot_data = SimpleNamespace(status="loaded")

    def get_nqe_map(self):
        return {}


class ForwardSyncRunnerHandlersTest(TestCase):
    def setUp(self):
        manufacturer = Manufacturer.objects.create(name="Acme", slug="acme")
        role = DeviceRole.objects.create(name="Core", slug="core")
        device_type = DeviceType.objects.create(
            manufacturer=manufacturer,
            model="Router",
            slug="router",
        )
        site = Site.objects.create(name="Site", slug="site")

        self.device_a = Device.objects.create(
            name="device-a",
            site=site,
            device_type=device_type,
            role=role,
        )
        self.device_b = Device.objects.create(
            name="device-b",
            site=site,
            device_type=device_type,
            role=role,
        )
        self.interface_a = Interface.objects.create(
            device=self.device_a,
            name="et0",
            type=InterfaceTypeChoices.TYPE_1000BASE_T,
        )
        self.interface_b = Interface.objects.create(
            device=self.device_b,
            name="et0",
            type=InterfaceTypeChoices.TYPE_1000BASE_T,
        )

        self.runner = ForwardSyncRunner(
            sync=_DummySync(),
            client=None,
            ingestion=None,
            settings={},
        )

    def test_sync_cable_creates_conn(self):
        item = {
            "a_device": self.device_a.name,
            "a_interface": self.interface_a.name,
            "b_device": self.device_b.name,
            "b_interface": self.interface_b.name,
            "label": "uplink",
        }

        cable = self.runner._sync_cable(item)

        self.assertIsNotNone(cable)
        self.assertEqual(cable.label, "uplink")
        self.assertIn(self.interface_a, cable.a_terminations)
        self.assertIn(self.interface_b, cable.b_terminations)

        # running the sync again should reuse the existing cable
        same_cable = self.runner._sync_cable(item)
        self.assertEqual(cable.pk, same_cable.pk)

    def test_sync_inventory_item_creates_component(self):
        manufacturer = Manufacturer.objects.create(name="WidgetCo", slug="widgetco")
        item = {
            "device": self.device_a.name,
            "name": "Line Card LC-1",
            "serial": "SER123456",
            "manufacturer": manufacturer.name,
            "part_id": "LC-1",
        }

        result = self.runner._sync_inventory_item(item)

        self.assertIsInstance(result, InventoryItem)
        self.assertEqual(result.device, self.device_a)
        self.assertEqual(result.part_id, "LC-1")

    def test_sync_virtualchassis_assigns_master(self):
        members = [self.device_a.name, self.device_b.name]
        item = {
            "name": "vc-test",
            "master": self.device_b.name,
            "members": members,
        }

        vc = self.runner._sync_virtualchassis(item)

        self.assertIsInstance(vc, VirtualChassis)
        self.assertEqual(vc.master, self.device_b)
        device_b = Device.objects.get(pk=self.device_b.pk)
        self.assertEqual(device_b.virtual_chassis, vc)
        self.assertEqual(device_b.vc_position, 1)

    def test_sync_vrf_sets_fields(self):
        tenant = Tenant.objects.create(name="Tenant", slug="tenant")
        item = {
            "name": "vrf-blue",
            "rd": "65000:1",
            "tenant": tenant.name,
            "enforce_unique": True,
        }

        vrf = self.runner._sync_vrf(item)

        self.assertIsInstance(vrf, VRF)
        self.assertEqual(vrf.name, "vrf-blue")
        self.assertEqual(vrf.rd, "65000:1")
        self.assertTrue(vrf.enforce_unique)
        self.assertEqual(vrf.tenant, tenant)
