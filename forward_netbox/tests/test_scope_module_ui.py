from unittest.mock import Mock
from unittest.mock import patch

from core.models import Job
from dcim.models import Device
from dcim.models import DeviceRole
from dcim.models import DeviceType
from dcim.models import Manufacturer
from dcim.models import Site
from dcim.models.device_components import ModuleBay
from django.contrib.auth import get_user_model
from django.test import Client
from django.test import TestCase
from django.urls import reverse

from forward_netbox.jobs import create_forward_module_bays
from forward_netbox.jobs import prune_forward_orphans
from forward_netbox.models import ForwardSource
from forward_netbox.models import ForwardSync
from forward_netbox.utilities.module_readiness import compute_module_readiness_for_sync
from forward_netbox.utilities.module_readiness import create_missing_module_bays


class ScopeModuleUiTest(TestCase):
    def setUp(self):
        self.source = ForwardSource.objects.create(
            name="ui-src",
            type="saas",
            url="https://fwd.app",
            status="ready",
            parameters={
                "username": "u@example.com",
                "password": "p",
                "verify": True,
                "network_id": "net-1",
                "device_tag_include_tags": ["Prod_Core"],
                "device_tag_include_match": "any",
            },
        )
        self.sync = ForwardSync.objects.create(
            name="ui-sync",
            source=self.source,
            parameters={"snapshot_id": "latestProcessed"},
        )
        mfr = Manufacturer.objects.create(name="MfrU", slug="mfr-u")
        self.dt = DeviceType.objects.create(manufacturer=mfr, model="dt-u", slug="dt-u")
        self.role = DeviceRole.objects.create(name="RoleU", slug="role-u")
        self.site = Site.objects.create(name="SiteU", slug="site-u")

    def _device(self, name):
        return Device.objects.create(
            name=name, device_type=self.dt, role=self.role, site=self.site
        )

    # --- module readiness utilities -----------------------------------------

    @patch("forward_netbox.utilities.module_readiness.fetch_module_rows_for_sync")
    def test_module_readiness_and_create_is_idempotent(self, mock_fetch):
        dev = self._device("dev-m")
        mock_fetch.return_value = [
            {"device": "dev-m", "module_bay": "Slot 1"},
            {"device": "dev-m", "module_bay": "Slot 2"},
            {"device": "ghost", "module_bay": "Slot 1"},
        ]
        report = compute_module_readiness_for_sync(self.sync)
        self.assertEqual(report.unique_missing_bays, 2)
        self.assertEqual(report.missing_device_rows, 1)

        result = create_missing_module_bays(report)
        self.assertEqual(result["created"], 2)
        self.assertEqual(ModuleBay.objects.filter(device=dev).count(), 2)

        # Re-running creates nothing (bays already exist).
        result2 = create_missing_module_bays(report)
        self.assertEqual(result2["created"], 0)
        self.assertEqual(ModuleBay.objects.filter(device=dev).count(), 2)

    # --- view smoke tests ----------------------------------------------------

    def _superuser_client(self):
        user = get_user_model().objects.create_user(username="admin-ui", password="x")
        user.is_superuser = True
        user.is_staff = True
        user.save()
        client = Client()
        client.force_login(user)
        return client

    def test_scope_reconciliation_view_and_prune(self):
        self._device("dev-a")
        self._device("dev-stale")
        fwd_client = Mock()
        fwd_client.run_nqe_query = Mock(
            return_value=[{"name": "dev-a", "completed": True}]
        )
        client = self._superuser_client()
        with (
            patch.object(ForwardSource, "get_client", return_value=fwd_client),
            patch.object(ForwardSync, "resolve_snapshot_id", return_value="snap-1"),
        ):
            # GET preview renders.
            resp = client.get(
                reverse(
                    "plugins:forward_netbox:forwardsync_scope_reconciliation",
                    kwargs={"pk": self.sync.pk},
                )
            )
            self.assertEqual(resp.status_code, 200)
            # POST enqueues a background prune job (no synchronous delete).
            resp = client.post(
                reverse(
                    "plugins:forward_netbox:forwardsync_prune_orphans",
                    kwargs={"pk": self.sync.pk},
                )
            )
            self.assertEqual(resp.status_code, 302)
            job = Job.objects.filter(name__icontains="prune orphans").latest("pk")
            self.assertEqual(job.object_id, self.sync.pk)
            # Devices are still present until the job runs.
            self.assertTrue(Device.objects.filter(name="dev-stale").exists())
            # Run the job: the orphan is pruned, the in-scope device kept.
            prune_forward_orphans(job)
        self.assertTrue(Device.objects.filter(name="dev-a").exists())
        self.assertFalse(Device.objects.filter(name="dev-stale").exists())

    def test_module_readiness_view_and_create(self):
        self._device("dev-m")
        client = self._superuser_client()
        with patch(
            "forward_netbox.utilities.module_readiness.fetch_module_rows_for_sync",
            return_value=[{"device": "dev-m", "module_bay": "Slot 1"}],
        ):
            resp = client.get(
                reverse(
                    "plugins:forward_netbox:forwardsync_module_readiness",
                    kwargs={"pk": self.sync.pk},
                )
            )
            self.assertEqual(resp.status_code, 200)
            # POST enqueues a background create-bays job.
            resp = client.post(
                reverse(
                    "plugins:forward_netbox:forwardsync_create_module_bays",
                    kwargs={"pk": self.sync.pk},
                )
            )
            self.assertEqual(resp.status_code, 302)
            job = Job.objects.filter(name__icontains="create module bays").latest("pk")
            self.assertEqual(job.object_id, self.sync.pk)
            # Bays are created when the job runs.
            create_forward_module_bays(job)
        self.assertEqual(
            ModuleBay.objects.filter(device__name="dev-m", name="Slot 1").count(), 1
        )
