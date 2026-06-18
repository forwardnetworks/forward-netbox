from unittest.mock import Mock

from dcim.models import Device
from dcim.models import DeviceRole
from dcim.models import DeviceType
from dcim.models import Interface
from dcim.models import Manufacturer
from dcim.models import Site
from django.db import connection
from django.db import transaction
from django.test import TestCase
from ipam.models import IPAddress

from forward_netbox.models import ForwardSource
from forward_netbox.models import ForwardSync
from forward_netbox.utilities.apply_engine_bulk import bulk_orm_apply_interface
from forward_netbox.utilities.apply_engine_bulk import bulk_orm_apply_ipaddress
from forward_netbox.utilities.sync import ForwardSyncRunner
from forward_netbox.utilities.sync_interface import apply_dcim_interface
from forward_netbox.utilities.sync_ipam import apply_ipam_ipaddress


class BulkAdapterParityTest(TestCase):
    """Prove the experimental bulk paths produce the same DB state as the adapter.

    Runs the adapter on a row set, snapshots the resulting state, rolls back to a
    savepoint (leaving only the pre-created fixtures), runs the bulk path on the
    same rows, and asserts the snapshots match.
    """

    def setUp(self):
        self.source = ForwardSource.objects.create(
            name="parity-source",
            type="saas",
            url="https://fwd.app",
            status="ready",
            parameters={
                "username": "u@example.com",
                "password": "p",
                "verify": True,
                "network_id": "net-1",
            },
        )
        self.sync = ForwardSync.objects.create(
            name="parity-sync",
            source=self.source,
            parameters={"snapshot_id": "latestProcessed"},
        )
        mfr = Manufacturer.objects.create(name="MfrP", slug="mfr-p")
        dt = DeviceType.objects.create(manufacturer=mfr, model="dt-p", slug="dt-p")
        role = DeviceRole.objects.create(name="RoleP", slug="role-p")
        site = Site.objects.create(name="SiteP", slug="site-p")
        self.device = Device.objects.create(
            name="dev-p", device_type=dt, role=role, site=site
        )
        self.interface = Interface.objects.create(
            device=self.device, name="Ethernet1", type="1000base-t"
        )

    def _runner(self):
        return ForwardSyncRunner(
            sync=self.sync, ingestion=None, client=None, logger_=Mock()
        )

    def _run_both_and_compare(self, *, seed, adapter_apply, bulk_apply, capture):
        # Savepoint is taken BEFORE seeding so rollback also undoes the seed,
        # leaving a clean slate for the bulk run.
        sid = connection.savepoint()
        seed()
        adapter_apply(self._runner())
        adapter_state = capture()
        connection.savepoint_rollback(sid)

        sid2 = connection.savepoint()
        seed()
        bulk_apply(self._runner())
        bulk_state = capture()
        connection.savepoint_rollback(sid2)

        self.assertEqual(adapter_state, bulk_state)
        return adapter_state

    def test_ipaddress_bulk_matches_adapter(self):
        rows = [
            {
                "device": "dev-p",
                "interface": "Ethernet1",
                "address": "10.1.1.1/24",
                "status": "active",
                "vrf": None,
            },
            {
                "device": "dev-p",
                "interface": "Ethernet1",
                "address": "10.1.1.2/24",
                "status": "active",
                "vrf": "blue",
            },
            {
                "device": "dev-p",
                "interface": "Ethernet1",
                "address": "10.1.1.3/24",
                "status": "active",
                "vrf": None,
            },
        ]

        def seed():
            IPAddress.objects.create(address="10.1.1.3/24", status="deprecated")

        def capture():
            return [
                (
                    str(ip.address),
                    ip.vrf.name if ip.vrf else None,
                    ip.status,
                    ip.assigned_object_id,
                    ip.assigned_object_type_id,
                )
                for ip in IPAddress.objects.order_by("address")
            ]

        def adapter_apply(runner):
            for row in rows:
                apply_ipam_ipaddress(runner, row)

        state = self._run_both_and_compare(
            seed=seed,
            adapter_apply=adapter_apply,
            bulk_apply=lambda runner: bulk_orm_apply_ipaddress(runner, rows),
            capture=capture,
        )
        # Sanity: all three addresses present, updated row flipped to active.
        self.assertEqual(len(state), 3)
        self.assertTrue(all(row[2] == "active" for row in state))

    def test_interface_bulk_matches_adapter(self):
        rows = [
            {
                "device": "dev-p",
                "name": "Ethernet2",
                "type": "1000base-t",
                "enabled": True,
                "mtu": 1500,
            },
            {
                "device": "dev-p",
                "name": "Ethernet1",
                "type": "1000base-t",
                "enabled": False,
                "description": "uplink",
            },
        ]

        def seed():
            pass

        def capture():
            return [
                (
                    iface.name,
                    iface.type,
                    iface.enabled,
                    iface.mtu,
                    iface.description,
                )
                for iface in Interface.objects.filter(device=self.device).order_by(
                    "name"
                )
            ]

        def adapter_apply(runner):
            for row in rows:
                apply_dcim_interface(runner, row)

        state = self._run_both_and_compare(
            seed=seed,
            adapter_apply=adapter_apply,
            bulk_apply=lambda runner: bulk_orm_apply_interface(runner, rows),
            capture=capture,
        )
        names = {row[0] for row in state}
        self.assertIn("Ethernet2", names)
