# A customer is building a plugin that adds rack and geo information to devices
# the Forward sync covers, and asked whether the sync would remove it. It must not:
# the sync writes a short, fixed list of device fields and nothing else. This pins
# that contract on both apply paths, so a later change that starts writing (or
# blanking) another field fails here instead of in a customer's NetBox.
from unittest.mock import Mock

from core.models import ObjectType
from dcim.models import Device
from dcim.models import DeviceRole
from dcim.models import DeviceType
from dcim.models import Location
from dcim.models import Manufacturer
from dcim.models import Rack
from dcim.models import Site
from django.core.exceptions import ValidationError
from django.test import TestCase
from extras.models import CustomField
from extras.models import Tag
from tenancy.models import Tenant

from forward_netbox.models import ForwardSource
from forward_netbox.models import ForwardSync
from forward_netbox.utilities.apply_engine_bulk import bulk_orm_apply_device
from forward_netbox.utilities.sync import ForwardSyncRunner
from forward_netbox.utilities.sync_device import apply_dcim_device

# What the sync owns on a device it covers (see docs/01_User_Guide/operations.md,
# "What the sync writes on a device"). Anything not in this list is the operator's.
OWNED = ("name", "site", "role", "device_type", "platform", "serial", "status")


class DeviceFieldsTheSyncOwnsTest(TestCase):
    def setUp(self):
        self.site = Site.objects.create(name="site-a", slug="site-a")
        self.other_site = Site.objects.create(name="site-b", slug="site-b")
        maker = Manufacturer.objects.create(name="Cisco", slug="cisco")
        self.device_type = DeviceType.objects.create(
            manufacturer=maker, model="model-a", slug="model-a"
        )
        self.role = DeviceRole.objects.create(
            name="role-a", slug="role-a", color="9e9e9e"
        )
        source = ForwardSource.objects.create(
            name="own-src",
            type="saas",
            url="https://fwd.app",
            status="ready",
            parameters={"network_id": "net-1"},
        )
        self.sync = ForwardSync.objects.create(
            name="own-sync",
            source=source,
            parameters={"snapshot_id": "latestProcessed"},
        )
        self.location = Location.objects.create(
            name="hall-1", slug="hall-1", site=self.site
        )
        self.rack = Rack.objects.create(
            name="rack-1", site=self.site, location=self.location
        )
        self.tenant = Tenant.objects.create(name="tenant-a", slug="tenant-a")
        field = CustomField.objects.create(name="owner_note", type="text")
        field.object_types.set([ObjectType.objects.get_for_model(Device)])
        self.tag = Tag.objects.create(
            name="from-other-plugin", slug="from-other-plugin"
        )

        self.device = Device.objects.create(
            name="core-sw-01",
            site=self.site,
            role=self.role,
            device_type=self.device_type,
            status="active",
            serial="OLD-SERIAL",
            # Everything below is not Forward's to touch.
            rack=self.rack,
            location=self.location,
            position=10,
            face="front",
            latitude="41.878100",
            longitude="-87.629800",
            description="added by another plugin",
            comments="added by another plugin",
            tenant=self.tenant,
            asset_tag="ASSET-1",
            custom_field_data={"owner_note": "keep me"},
        )
        self.device.tags.add(self.tag)

    def _runner(self):
        return ForwardSyncRunner(
            sync=self.sync, ingestion=None, client=None, logger_=Mock()
        )

    def _row(self, **overrides):
        row = {
            "name": "core-sw-01",
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
            "status": "offline",
            "serial": "NEW-SERIAL",
        }
        row.update(overrides)
        return row

    def _assert_only_the_owned_fields_moved(self):
        fresh = Device.objects.get(pk=self.device.pk)
        # What Forward owns follows Forward.
        self.assertEqual(fresh.serial, "NEW-SERIAL")
        self.assertEqual(fresh.status, "offline")
        # What it does not own is exactly as the other plugin left it.
        self.assertEqual(fresh.rack_id, self.rack.pk)
        self.assertEqual(fresh.location_id, self.location.pk)
        self.assertEqual(fresh.position, 10)
        self.assertEqual(fresh.face, "front")
        self.assertEqual(str(fresh.latitude), "41.878100")
        self.assertEqual(str(fresh.longitude), "-87.629800")
        self.assertEqual(fresh.description, "added by another plugin")
        self.assertEqual(fresh.comments, "added by another plugin")
        self.assertEqual(fresh.tenant_id, self.tenant.pk)
        self.assertEqual(fresh.asset_tag, "ASSET-1")
        self.assertEqual(fresh.custom_field_data, {"owner_note": "keep me"})
        self.assertEqual(
            list(fresh.tags.values_list("slug", flat=True)), [self.tag.slug]
        )

    def test_the_per_row_path_leaves_rack_geo_and_custom_data_alone(self):
        apply_dcim_device(self._runner(), self._row())

        self._assert_only_the_owned_fields_moved()

    def test_the_bulk_path_leaves_rack_geo_and_custom_data_alone(self):
        bulk_orm_apply_device(self._runner(), [self._row()])

        self._assert_only_the_owned_fields_moved()

    def test_a_field_forward_has_no_value_for_is_not_blanked(self):
        # Forward reports no serial for this device. The sync does write the
        # serial it is given, so this is the owned-field behaviour; the point is
        # that nothing else blanks alongside it.
        for apply in (
            lambda: apply_dcim_device(self._runner(), self._row(serial="")),
            lambda: bulk_orm_apply_device(self._runner(), [self._row(serial="")]),
        ):
            Device.objects.filter(pk=self.device.pk).update(serial="OLD-SERIAL")
            apply()
            fresh = Device.objects.get(pk=self.device.pk)
            self.assertEqual(fresh.rack_id, self.rack.pk)
            self.assertEqual(fresh.comments, "added by another plugin")
            self.assertEqual(fresh.custom_field_data, {"owner_note": "keep me"})

    def test_a_site_change_that_would_strand_the_rack_is_never_applied_silently(self):
        # Forward moving the device to another site while another plugin has it
        # racked at the old one cannot be applied: NetBox requires a device's rack
        # to belong to its site. The device must be left as it was, not moved with
        # a rack in the wrong site and not stripped of its rack.
        row = self._row(site="site-b", site_slug="site-b")
        for apply in (
            lambda: apply_dcim_device(self._runner(), row),
            lambda: bulk_orm_apply_device(self._runner(), [row]),
        ):
            try:
                apply()
            except ValidationError:
                # The per-row path refuses with NetBox's own validation error;
                # the bulk path records it as an issue instead. Anything else
                # (a crash) must fail this test.
                pass
            fresh = Device.objects.get(pk=self.device.pk)
            self.assertEqual(fresh.site_id, self.site.pk)
            self.assertEqual(fresh.rack_id, self.rack.pk)
            self.assertEqual(fresh.location_id, self.location.pk)

    def test_the_documented_owned_list_matches_what_the_bulk_path_writes(self):
        # The bulk path names the fields it updates; the contract above is only
        # true while that list is the documented one (virtual-chassis membership
        # is written alongside, by its own rule).
        import inspect

        source = inspect.getsource(bulk_orm_apply_device)
        for field in OWNED:
            self.assertIn(f'"{field}"', source)
        for field in ("rack", "location", "latitude", "comments", "tenant"):
            self.assertNotIn(f'"{field}"', source)
