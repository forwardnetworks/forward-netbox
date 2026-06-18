from unittest.mock import Mock

from dcim.models import Device
from dcim.models import DeviceRole
from dcim.models import DeviceType
from dcim.models import Interface
from dcim.models import Manufacturer
from dcim.models import Site
from django.contrib.contenttypes.models import ContentType
from django.test import TestCase
from ipam.models import IPAddress

from forward_netbox.models import ForwardSource
from forward_netbox.models import ForwardSync
from forward_netbox.utilities.sync import ForwardSyncRunner
from forward_netbox.utilities.sync_ipam import apply_ipam_ipaddress


class IpAddressDuplicateGlobalTest(TestCase):
    """A reused /30 link range can leave several global (VRF-less) IPs with the
    same host. The adapter must resolve deterministically instead of raising an
    ambiguous-coalesce error that fails the row."""

    def setUp(self):
        self.source = ForwardSource.objects.create(
            name="dup-src",
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
            name="dup-sync",
            source=self.source,
            parameters={"snapshot_id": "latestProcessed"},
        )
        mfr = Manufacturer.objects.create(name="MfrD", slug="mfr-d")
        dt = DeviceType.objects.create(manufacturer=mfr, model="dt-d", slug="dt-d")
        role = DeviceRole.objects.create(name="RoleD", slug="role-d")
        site = Site.objects.create(name="SiteD", slug="site-d")
        self.device = Device.objects.create(
            name="dev-d", device_type=dt, role=role, site=site
        )
        self.interface = Interface.objects.create(
            device=self.device, name="Ethernet1", type="1000base-t"
        )

    def _runner(self):
        return ForwardSyncRunner(
            sync=self.sync, ingestion=None, client=None, logger_=Mock()
        )

    def test_duplicate_global_ip_resolves_without_error(self):
        ct = ContentType.objects.get_for_model(Interface)
        other = Interface.objects.create(
            device=self.device, name="Ethernet9", type="1000base-t"
        )
        # Two global copies of the same host address.
        IPAddress.objects.create(address="192.168.1.1/30")
        IPAddress.objects.create(
            address="192.168.1.1/30",
            assigned_object_type=ct,
            assigned_object_id=other.pk,
        )

        row = {
            "device": "dev-d",
            "interface": "Ethernet1",
            "address": "192.168.1.1/30",
            "status": "active",
            "vrf": None,
        }
        # Must not raise (previously ForwardSearchError: Ambiguous coalesce lookup).
        result = apply_ipam_ipaddress(self._runner(), row)
        self.assertTrue(result)
        # Exactly one global copy is now assigned to Ethernet1; the duplicates
        # remain (pre-existing data the plugin did not create).
        assigned = IPAddress.objects.filter(
            address__net_host="192.168.1.1",
            assigned_object_type=ct,
            assigned_object_id=self.interface.pk,
        )
        self.assertEqual(assigned.count(), 1)
        self.assertEqual(
            IPAddress.objects.filter(address__net_host="192.168.1.1").count(), 2
        )
