# An interface that kept a VLAN from the site its device used to be at is
# refused by NetBox on EVERY later write, even one that only changes the MTU,
# because `full_clean` validates the whole object. A customer sync recorded 130
# such refusals on one run. The row supplied no valid VLAN for the device's site,
# so the old site's VLAN cannot be kept: the interface apply clears it.
from types import SimpleNamespace
from unittest.mock import Mock

from dcim.models import Device
from dcim.models import DeviceRole
from dcim.models import DeviceType
from dcim.models import Interface
from dcim.models import Manufacturer
from dcim.models import Site
from django.test import TestCase
from ipam.models import VLAN

from forward_netbox.models import ForwardSource
from forward_netbox.models import ForwardSync
from forward_netbox.utilities.apply_engine_bulk import bulk_orm_apply_interface
from forward_netbox.utilities.sync import ForwardSyncRunner
from forward_netbox.utilities.sync_interface import apply_dcim_interface
from forward_netbox.utilities.sync_interface import stale_cross_site_untagged_vlan


class StaleUntaggedVlanTest(TestCase):
    def setUp(self):
        self.site = Site.objects.create(name="site-a", slug="site-a")
        self.old_site = Site.objects.create(name="site-b", slug="site-b")
        manufacturer = Manufacturer.objects.create(name="Cisco", slug="cisco")
        device_type = DeviceType.objects.create(
            manufacturer=manufacturer, model="model-a", slug="model-a"
        )
        role = DeviceRole.objects.create(name="role-a", slug="role-a", color="9e9e9e")
        self.device = Device.objects.create(
            name="dev-1",
            site=self.site,
            role=role,
            device_type=device_type,
            status="active",
        )
        self.old_vlan = VLAN.objects.create(
            site=self.old_site, vid=10, name="old", status="active"
        )
        self.local_vlan = VLAN.objects.create(
            site=self.site, vid=20, name="local", status="active"
        )
        self.global_vlan = VLAN.objects.create(vid=30, name="global", status="active")
        source = ForwardSource.objects.create(
            name="src",
            type="saas",
            url="https://forward.example",
            status="ready",
            parameters={"network_id": "network-1"},
        )
        sync = ForwardSync.objects.create(name="sync", source=source)
        self.runner = ForwardSyncRunner(
            sync=sync, ingestion=None, client=None, logger_=Mock()
        )

    def _stale_interface(self, name="eth1", vlan=None):
        # Built by queryset update: `full_clean` refuses this pairing, which is
        # exactly the state a writer bypassing validation leaves behind.
        interface = Interface.objects.create(
            device=self.device, name=name, type="1000base-t", mode="access", mtu=1500
        )
        Interface.objects.filter(pk=interface.pk).update(
            untagged_vlan=vlan or self.old_vlan
        )
        return Interface.objects.get(pk=interface.pk)

    def _row(self, **extra):
        row = {
            "device": "dev-1",
            "name": "eth1",
            "type": "1000base-t",
            "enabled": True,
            "mtu": 9000,
        }
        row.update(extra)
        return row

    def test_only_a_vlan_from_another_site_is_stale(self):
        for vlan, expected in (
            (self.old_vlan, True),
            (self.local_vlan, False),
            (self.global_vlan, False),
        ):
            with self.subTest(vlan=vlan.name):
                interface = self._stale_interface(name=f"eth-{vlan.name}", vlan=vlan)
                self.assertIs(
                    stale_cross_site_untagged_vlan(
                        SimpleNamespace(), interface, self.device
                    ),
                    expected,
                )

    def test_an_interface_with_no_vlan_is_never_stale(self):
        interface = Interface.objects.create(
            device=self.device, name="eth9", type="1000base-t"
        )
        self.assertFalse(
            stale_cross_site_untagged_vlan(SimpleNamespace(), interface, self.device)
        )

    def test_the_bulk_path_clears_it_so_an_mtu_change_is_not_refused(self):
        interface = self._stale_interface()

        bulk_orm_apply_interface(self.runner, [self._row()])

        interface.refresh_from_db()
        self.assertEqual(interface.mtu, 9000)
        self.assertIsNone(interface.untagged_vlan)

    def test_the_row_path_clears_it_too(self):
        interface = self._stale_interface()

        apply_dcim_interface(self.runner, self._row())

        interface.refresh_from_db()
        self.assertEqual(interface.mtu, 9000)
        self.assertIsNone(interface.untagged_vlan)

    def test_a_vlan_the_row_supplies_for_the_device_site_wins(self):
        interface = self._stale_interface()

        bulk_orm_apply_interface(
            self.runner, [self._row(mode="access", untagged_vlan=20)]
        )

        interface.refresh_from_db()
        self.assertEqual(interface.untagged_vlan, self.local_vlan)

    def test_a_valid_vlan_is_left_alone_when_the_row_supplies_none(self):
        interface = self._stale_interface(vlan=self.local_vlan)

        bulk_orm_apply_interface(self.runner, [self._row()])

        interface.refresh_from_db()
        self.assertEqual(interface.untagged_vlan, self.local_vlan)
        self.assertEqual(interface.mtu, 9000)
