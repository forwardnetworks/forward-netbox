# A device's platform is SET_NULL, not PROTECT, so the database let a sync
# delete a platform still in use and blanked the field on every device that had
# it. Seen live: an include tag that matched no device emptied the platform
# list, and one run deleted all 13 platforms and blanked the platform on 3,572
# devices. A shared catalogue row is deleted only when nothing uses it.
from unittest.mock import Mock

from dcim.models import Device
from dcim.models import DeviceRole
from dcim.models import DeviceType
from dcim.models import Manufacturer
from dcim.models import Platform
from dcim.models import Site
from django.test import TestCase

from forward_netbox.exceptions import ForwardDependencySkipError
from forward_netbox.models import ForwardSource
from forward_netbox.models import ForwardSync
from forward_netbox.utilities.sync import ForwardSyncRunner
from forward_netbox.utilities.sync_core_models import delete_dcim_platform


class CatalogDeleteKeepsUsedRowsTest(TestCase):
    def setUp(self):
        source = ForwardSource.objects.create(
            name="catalog-src",
            type="saas",
            url="https://fwd.app",
            status="ready",
            parameters={"network_id": "net-1"},
        )
        self.sync = ForwardSync.objects.create(
            name="catalog-sync",
            source=source,
            parameters={"snapshot_id": "latestProcessed"},
        )
        maker = Manufacturer.objects.create(name="Maker", slug="maker")
        self.used = Platform.objects.create(name="used-os", slug="used-os")
        self.unused = Platform.objects.create(name="unused-os", slug="unused-os")
        self.device = Device.objects.create(
            name="router-1",
            site=Site.objects.create(name="site-1", slug="site-1"),
            role=DeviceRole.objects.create(name="role-1", slug="role-1"),
            device_type=DeviceType.objects.create(
                manufacturer=maker, model="model-1", slug="model-1"
            ),
            platform=self.used,
        )

    def _runner(self):
        return ForwardSyncRunner(
            sync=self.sync, ingestion=None, client=None, logger_=Mock()
        )

    def test_a_platform_a_device_uses_is_kept_and_the_device_keeps_it(self):
        with self.assertRaises(ForwardDependencySkipError) as caught:
            delete_dcim_platform(self._runner(), {"slug": "used-os", "name": "used-os"})

        self.assertTrue(caught.exception.dependency_is_protecting)
        self.assertIn("dcim.device", str(caught.exception.dependency))
        self.assertTrue(Platform.objects.filter(pk=self.used.pk).exists())
        self.device.refresh_from_db()
        self.assertEqual(self.device.platform_id, self.used.pk)

    def test_a_platform_nothing_uses_is_deleted(self):
        self.assertTrue(
            delete_dcim_platform(
                self._runner(), {"slug": "unused-os", "name": "unused-os"}
            )
        )
        self.assertFalse(Platform.objects.filter(pk=self.unused.pk).exists())
