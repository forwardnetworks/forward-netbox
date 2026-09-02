# The mechanism behind a customer's post-2.7.2 interface refusals, closed at
# the point it is made rather than found afterwards.
#
# A device written with a new `site` keeps its interfaces' untagged VLANs from
# the old site. Neither `bulk_update` nor `save()` runs `Interface.clean()`,
# so nothing revalidates them, and the next interface sync is refused on every
# one - by NetBox, correctly. The audit command (2.7.x) found the rows on
# request; this revalidates them whenever either device apply path writes a
# device, and clears them only when this sync manages `dcim.interface`.
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

from forward_netbox.models import ForwardDeviceIdentity
from forward_netbox.models import ForwardIngestion
from forward_netbox.models import ForwardSource
from forward_netbox.models import ForwardSync
from forward_netbox.utilities.apply_engine_bulk import bulk_orm_apply_device
from forward_netbox.utilities.interface_vlan_audit import audit_interface_untagged_vlans
from forward_netbox.utilities.interface_vlan_audit import (
    clear_cross_site_untagged_vlans,
)
from forward_netbox.utilities.sync import ForwardSyncRunner
from forward_netbox.utilities.sync_device import apply_dcim_device


class _Fixture(TestCase):
    def setUp(self):
        self.site = Site.objects.create(name="site-a", slug="site-a")
        self.other_site = Site.objects.create(name="site-b", slug="site-b")
        manufacturer = Manufacturer.objects.create(name="Cisco", slug="cisco")
        self.device_type = DeviceType.objects.create(
            manufacturer=manufacturer, model="model-a", slug="model-a"
        )
        self.role = DeviceRole.objects.create(
            name="role-a", slug="role-a", color="9e9e9e"
        )
        self.old_site_vlan = VLAN.objects.create(
            site=self.other_site, vid=10, name="old-site-vlan", status="active"
        )
        self.same_site_vlan = VLAN.objects.create(
            site=self.site, vid=20, name="same-site-vlan", status="active"
        )
        self.global_vlan = VLAN.objects.create(
            vid=30, name="global-vlan", status="active"
        )

    def _device(self, name="dev-1", site=None):
        return Device.objects.create(
            name=name,
            site=site or self.site,
            role=self.role,
            device_type=self.device_type,
            status="active",
        )

    def _interface(self, device, name, vlan):
        # Built directly: `full_clean()` refuses the cross-site pairing, which
        # is exactly the state a writer bypassing validation leaves behind.
        interface = Interface.objects.create(
            device=device, name=name, type="1000base-t", mode="access"
        )
        Interface.objects.filter(pk=interface.pk).update(untagged_vlan=vlan)
        return interface

    def _moved_device(self):
        """A device at site-a whose interfaces still carry site-b's VLAN."""
        device = self._device()
        self._interface(device, "eth1", self.old_site_vlan)
        self._interface(device, "eth2", self.same_site_vlan)
        self._interface(device, "eth3", self.global_vlan)
        return device

    def _runner(self, *, manages_interfaces):
        sync = SimpleNamespace(
            is_model_enabled=lambda model: manages_interfaces
            and model == "dcim.interface"
        )
        return SimpleNamespace(sync=sync, _record_aggregated_skip_warning=Mock())

    def _vlan_of(self, device, name):
        return Interface.objects.get(device=device, name=name).untagged_vlan


class ClearCrossSiteUntaggedVlansTest(_Fixture):
    def test_only_the_other_sites_vlan_is_cleared(self):
        device = self._moved_device()
        runner = self._runner(manages_interfaces=True)

        result = clear_cross_site_untagged_vlans(runner, [device.pk])

        self.assertEqual(result["cleared"], 1)
        self.assertEqual(result["devices"], [device.pk])
        self.assertIsNone(self._vlan_of(device, "eth1"))
        self.assertEqual(self._vlan_of(device, "eth2"), self.same_site_vlan)
        self.assertEqual(self._vlan_of(device, "eth3"), self.global_vlan)
        warning = runner._record_aggregated_skip_warning.call_args.kwargs
        self.assertEqual(warning["reason"], "cross-site-untagged-vlan")
        self.assertIn("Cleared 1 untagged VLAN", warning["warning_message"])
        self.assertIn(f"pk {device.pk} (1)", warning["warning_message"])

    def test_a_sync_that_does_not_manage_interfaces_reports_and_leaves_them(self):
        # The interfaces are someone else's; clearing would be data loss.
        device = self._moved_device()
        runner = self._runner(manages_interfaces=False)

        result = clear_cross_site_untagged_vlans(runner, [device.pk])

        self.assertEqual(result["cleared"], 0)
        self.assertEqual(result["reported"], 1)
        self.assertEqual(self._vlan_of(device, "eth1"), self.old_site_vlan)
        warning = runner._record_aggregated_skip_warning.call_args.kwargs
        self.assertIn("does not manage dcim.interface", warning["warning_message"])
        self.assertIn("forward_interface_vlan_audit", warning["warning_message"])

    def test_a_clean_device_costs_no_warning(self):
        device = self._device()
        self._interface(device, "eth2", self.same_site_vlan)
        runner = self._runner(manages_interfaces=True)

        result = clear_cross_site_untagged_vlans(runner, [device.pk])

        self.assertEqual(result, {"cleared": 0, "reported": 0, "devices": []})
        runner._record_aggregated_skip_warning.assert_not_called()

    def test_the_message_carries_keys_not_names(self):
        device = self._moved_device()
        runner = self._runner(manages_interfaces=True)
        clear_cross_site_untagged_vlans(runner, [device.pk])
        message = runner._record_aggregated_skip_warning.call_args.kwargs[
            "warning_message"
        ]
        self.assertNotIn("dev-1", message)
        self.assertNotIn("old-site-vlan", message)


class DeviceApplyPathsRevalidateTest(_Fixture):
    """Both paths that write a device revalidate its interfaces."""

    def _sync(self, *, manages_interfaces):
        source = ForwardSource.objects.create(
            name="vlan-src",
            type="saas",
            url="https://fwd.app",
            status="ready",
            parameters={"network_id": "net-1"},
        )
        # `dcim.interface` is a core model, enabled unless the sync says
        # otherwise - so "not managed" is an explicit False, not an absence.
        parameters = {
            "snapshot_id": "latestProcessed",
            "dcim.interface": manages_interfaces,
        }
        return ForwardSync.objects.create(
            name="vlan-sync", source=source, parameters=parameters
        )

    def _row(self, name="dev-1", serial="NEW-SERIAL"):
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
            "serial": serial,
        }

    def test_the_bulk_path_clears_after_writing_the_device(self):
        device = self._moved_device()
        runner = ForwardSyncRunner(
            sync=self._sync(manages_interfaces=True),
            ingestion=None,
            client=None,
            logger_=Mock(),
        )

        self.assertTrue(bulk_orm_apply_device(runner, [self._row()]))

        device.refresh_from_db()
        self.assertEqual(device.serial, "NEW-SERIAL")
        self.assertIsNone(self._vlan_of(device, "eth1"))
        self.assertEqual(self._vlan_of(device, "eth2"), self.same_site_vlan)

    def test_the_bulk_path_leaves_an_unchanged_device_alone(self):
        # No write, no revalidation: the state is reported by the audit, and
        # touching interfaces of a device this run did not write would be a
        # side effect nothing asked for.
        device = self._moved_device()
        device.serial = "NEW-SERIAL"
        device.save()
        runner = ForwardSyncRunner(
            sync=self._sync(manages_interfaces=True),
            ingestion=None,
            client=None,
            logger_=Mock(),
        )

        bulk_orm_apply_device(runner, [self._row()])

        self.assertEqual(self._vlan_of(device, "eth1"), self.old_site_vlan)

    def test_the_row_path_clears_after_writing_the_device(self):
        device = self._moved_device()
        runner = ForwardSyncRunner(
            sync=self._sync(manages_interfaces=True),
            ingestion=None,
            client=None,
            logger_=Mock(),
        )

        apply_dcim_device(runner, self._row())

        device.refresh_from_db()
        self.assertEqual(device.serial, "NEW-SERIAL")
        self.assertIsNone(self._vlan_of(device, "eth1"))

    def test_the_row_path_does_not_clear_when_interfaces_are_not_managed(self):
        device = self._moved_device()
        runner = ForwardSyncRunner(
            sync=self._sync(manages_interfaces=False),
            ingestion=None,
            client=None,
            logger_=Mock(),
        )

        apply_dcim_device(runner, self._row())

        self.assertEqual(self._vlan_of(device, "eth1"), self.old_site_vlan)


class AuditOwnedOnlyTest(_Fixture):
    def test_owned_only_restricts_the_audit_to_forward_created_devices(self):
        owned = self._moved_device()
        someone_elses = self._device("dev-2")
        self._interface(someone_elses, "eth1", self.old_site_vlan)
        source = ForwardSource.objects.create(
            name="audit-src", type="saas", url="https://fwd.app", status="ready"
        )
        sync = ForwardSync.objects.create(name="audit-sync", source=source)
        ForwardDeviceIdentity.objects.create(
            sync=sync,
            source_device_key=owned.name,
            device=owned,
            ingestion=ForwardIngestion.objects.create(sync=sync, snapshot_id="s"),
        )

        everything = audit_interface_untagged_vlans()
        owned_only = audit_interface_untagged_vlans(owned_only=True)

        self.assertEqual(everything["cross_site_count"], 2)
        self.assertEqual(owned_only["cross_site_count"], 1)
        self.assertTrue(owned_only["owned_only"])
        self.assertEqual(owned_only["cross_site"][0]["device"], owned.name)
