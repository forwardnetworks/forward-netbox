"""A device whose name changed only in case is the same device.

NetBox decides device identity with `dcim_device_unique_name_site`, which is
`UniqueConstraint(Lower("name"), "site")`. Both apply paths matched existing
devices by EXACT name, so a device Forward reports as `CORE-SW-01` against a
stored `core-sw-01` in the same site was classified as new. On the bulk path
the database then refused the whole `bulk_create`, and a customer's sync died
on that constraint with nothing wrong in the Forward data at all.
"""

from unittest.mock import Mock

from dcim.models import Device
from dcim.models import DeviceRole
from dcim.models import DeviceType
from dcim.models import Manufacturer
from dcim.models import Site
from django.test import TestCase

from forward_netbox.models import ForwardSource
from forward_netbox.models import ForwardSync
from forward_netbox.utilities.apply_engine_bulk import bulk_orm_apply_device
from forward_netbox.utilities.sync import ForwardSyncRunner
from forward_netbox.utilities.sync_device import apply_dcim_device


class DeviceNameCaseIdentityTest(TestCase):
    def setUp(self):
        self.site = Site.objects.create(name="site-a", slug="site-a")
        manufacturer = Manufacturer.objects.create(name="Cisco", slug="cisco")
        self.device_type = DeviceType.objects.create(
            manufacturer=manufacturer, model="model-a", slug="model-a"
        )
        self.role = DeviceRole.objects.create(
            name="role-a", slug="role-a", color="9e9e9e"
        )
        source = ForwardSource.objects.create(
            name="case-src",
            type="saas",
            url="https://fwd.app",
            status="ready",
            parameters={"network_id": "net-1"},
        )
        self.sync = ForwardSync.objects.create(
            name="case-sync",
            source=source,
            parameters={"snapshot_id": "latestProcessed"},
        )

    def _existing(self, name):
        return Device.objects.create(
            name=name,
            site=self.site,
            role=self.role,
            device_type=self.device_type,
            status="active",
        )

    def _runner(self):
        return ForwardSyncRunner(
            sync=self.sync, ingestion=None, client=None, logger_=Mock()
        )

    def _row(self, name):
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
            "serial": "",
        }

    def test_the_bulk_path_updates_the_existing_device_instead_of_colliding(self):
        existing = self._existing("core-sw-01")
        runner = self._runner()
        runner._record_issue = Mock()

        bulk_orm_apply_device(runner, [self._row("CORE-SW-01")])

        # The mechanism, asserted directly. Unfixed, the row was classified as
        # a CREATE and the database refused it on `dcim_device_unique_name_site`.
        # Outside a branch (this test) that refusal is isolated row by row and
        # recorded as an issue; inside one - every real sync - it is re-raised
        # and the whole sync fails, which is what a customer hit.
        runner._record_issue.assert_not_called()
        self.assertEqual(Device.objects.filter(site=self.site).count(), 1)
        existing.refresh_from_db()
        # Forward is the source of truth for the spelling.
        self.assertEqual(existing.name, "CORE-SW-01")

    def test_the_row_path_updates_the_existing_device_instead_of_colliding(self):
        existing = self._existing("core-sw-01")

        apply_dcim_device(self._runner(), self._row("CORE-SW-01"))

        self.assertEqual(Device.objects.filter(site=self.site).count(), 1)
        existing.refresh_from_db()
        self.assertEqual(existing.name, "CORE-SW-01")

    def test_an_exact_match_is_still_preferred_over_a_case_variant(self):
        # Two spellings can coexist when their tenants differ; the exact one
        # must win exactly as it did before, not become ambiguous.
        from tenancy.models import Tenant

        tenant = Tenant.objects.create(name="t", slug="t")
        other = self._existing("core-sw-01")
        other.tenant = tenant
        other.save()
        exact = self._existing("CORE-SW-01")

        bulk_orm_apply_device(self._runner(), [self._row("CORE-SW-01")])

        self.assertEqual(Device.objects.filter(site=self.site).count(), 2)
        other.refresh_from_db()
        exact.refresh_from_db()
        self.assertEqual(other.name, "core-sw-01")
        self.assertEqual(exact.name, "CORE-SW-01")

    def test_an_unrelated_new_device_is_still_created(self):
        self._existing("core-sw-01")

        bulk_orm_apply_device(self._runner(), [self._row("core-sw-02")])

        self.assertEqual(
            sorted(Device.objects.values_list("name", flat=True)),
            ["core-sw-01", "core-sw-02"],
        )
