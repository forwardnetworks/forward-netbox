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
from forward_netbox.utilities.primary_ip_audit import audit_primary_ip_resolution


class _FakeClient:
    def get_device_mgmt_tags(
        self, network_id, snapshot_id, *, include_tags, exclude_tags, include_match
    ):
        return {
            "dev-a": ["Mgmt_Vl211"],  # interface present + IP -> resolvable
            "dev-b": ["Mgmt_Vl211"],  # interface present, no IP
            "dev-c": ["Mgmt_Vl211"],  # interface not matched
            "ghost-dev": ["Mgmt_Vl211"],  # not in NetBox
        }


class PrimaryIpAuditTest(TestCase):
    def setUp(self):
        self.source = ForwardSource.objects.create(
            name="pip-source",
            type="saas",
            url="https://fwd.app",
            status="ready",
            parameters={"username": "u@x", "password": "p", "network_id": "net-1"},
        )
        self.sync = ForwardSync.objects.create(
            name="pip-sync",
            source=self.source,
            parameters={"snapshot_id": "latestProcessed"},
        )
        mfr = Manufacturer.objects.create(name="Mfr", slug="mfr")
        dt = DeviceType.objects.create(manufacturer=mfr, model="dt", slug="dt")
        role = DeviceRole.objects.create(name="Role", slug="role")
        site = Site.objects.create(name="Site", slug="site")

        def device(name):
            return Device.objects.create(
                name=name, device_type=dt, role=role, site=site
            )

        iface_ct = ContentType.objects.get_for_model(Interface)
        dev_a = device("dev-a")
        iface_a = Interface.objects.create(device=dev_a, name="Vlan211", type="virtual")
        IPAddress.objects.create(
            address="10.0.0.1/24",
            assigned_object_type=iface_ct,
            assigned_object_id=iface_a.pk,
        )
        dev_b = device("dev-b")
        Interface.objects.create(device=dev_b, name="Vlan211", type="virtual")
        dev_c = device("dev-c")
        Interface.objects.create(
            device=dev_c, name="GigabitEthernet0/0", type="1000base-t"
        )

    def test_buckets_unresolved_by_reason(self):
        payload = audit_primary_ip_resolution(self.sync, _FakeClient(), snapshot_id="1")
        self.assertEqual(payload["mgmt_tagged_devices"], 4)
        self.assertEqual(payload["resolvable"], 1)
        self.assertEqual(payload["unresolved"], 3)
        self.assertEqual(payload["unresolved_device_not_in_netbox"], 1)
        self.assertEqual(payload["unresolved_interface_not_matched"], 1)
        self.assertEqual(payload["unresolved_interface_present_no_ip"], 1)
        self.assertEqual(payload["example_device_not_in_netbox"], ["ghost-dev"])
