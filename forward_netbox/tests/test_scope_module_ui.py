from unittest.mock import Mock
from unittest.mock import patch

from core.models import Job
from dcim.models import Device
from dcim.models import DeviceRole
from dcim.models import DeviceType
from dcim.models import Manufacturer
from dcim.models import Rack
from dcim.models import Site
from dcim.models.device_components import ModuleBay
from django.contrib.auth import get_user_model
from django.test import Client
from django.test import TestCase
from django.urls import reverse

from forward_netbox.jobs import create_forward_module_bays
from forward_netbox.jobs import prune_forward_orphans
from forward_netbox.jobs import tag_forward_backfilled_devices
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
                "device_tag_include_tags": ["N.Patel"],
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

    def test_tag_backfilled_devices_adds_and_removes_tag(self):
        from forward_netbox.utilities.scope_reconciliation import (
            tag_backfilled_devices,
        )

        self._device("dev-collected")
        self._device("dev-backfilled")
        fwd_client = Mock()
        fwd_client.run_nqe_query = Mock(
            return_value=[
                {"name": "dev-collected", "completed": True},
                {"name": "dev-backfilled", "completed": False},
            ]
        )
        with (
            patch.object(ForwardSource, "get_client", return_value=fwd_client),
            patch.object(ForwardSync, "resolve_snapshot_id", return_value="snap-1"),
        ):
            result = tag_backfilled_devices(self.sync)
            self.assertEqual(result["tagged"], 1)
            self.assertEqual(
                set(
                    Device.objects.filter(tags__slug="forward-backfilled").values_list(
                        "name", flat=True
                    )
                ),
                {"dev-backfilled"},
            )
            # The device now collects fresh — the tag is removed on the next run.
            fwd_client.run_nqe_query.return_value = [
                {"name": "dev-collected", "completed": True},
                {"name": "dev-backfilled", "completed": True},
            ]
            result2 = tag_backfilled_devices(self.sync)
            self.assertEqual(result2["untagged"], 1)
            self.assertEqual(
                Device.objects.filter(tags__slug="forward-backfilled").count(), 0
            )

    def test_tag_backfilled_devices_also_tags_out_of_scope(self):
        from forward_netbox.utilities.scope_reconciliation import (
            tag_backfilled_devices,
        )

        self._device("dev-collected")
        self._device("dev-orphan")  # in NetBox, not returned by the Forward scope
        fwd_client = Mock()
        fwd_client.run_nqe_query = Mock(
            return_value=[{"name": "dev-collected", "completed": True}]
        )
        with (
            patch.object(ForwardSource, "get_client", return_value=fwd_client),
            patch.object(ForwardSync, "resolve_snapshot_id", return_value="snap-1"),
        ):
            result = tag_backfilled_devices(self.sync)
        self.assertEqual(result["total_out_of_scope"], 1)
        self.assertEqual(
            set(
                Device.objects.filter(tags__slug="forward-out-of-scope").values_list(
                    "name", flat=True
                )
            ),
            {"dev-orphan"},
        )

    def test_prune_also_removes_empty_orphan_sites(self):
        # site-u (self.site) has a rack — non-empty, must be kept even though
        # it has no devices and is not in the Forward location result.
        Rack.objects.create(name="rack-u", site=self.site)
        site_active = Site.objects.create(name="Site Active", slug="site-active")
        site_stale = Site.objects.create(name="Site Stale", slug="site-stale")
        Device.objects.create(
            name="dev-site-a",
            device_type=self.dt,
            role=self.role,
            site=site_active,
        )
        Device.objects.create(
            name="dev-site-stale",
            device_type=self.dt,
            role=self.role,
            site=site_stale,
        )
        fwd_client = Mock()
        # Forward only knows site-active; dev-site-stale (and site-stale) are orphans.
        fwd_client.run_nqe_query = Mock(
            return_value=[
                {"name": "dev-site-a", "completed": True, "location": "site active"}
            ]
        )
        client = self._superuser_client()
        with (
            patch.object(ForwardSource, "get_client", return_value=fwd_client),
            patch.object(ForwardSync, "resolve_snapshot_id", return_value="snap-1"),
        ):
            resp = client.post(
                reverse(
                    "plugins:forward_netbox:forwardsync_prune_orphans",
                    kwargs={"pk": self.sync.pk},
                )
            )
            self.assertEqual(resp.status_code, 302)
            job = Job.objects.filter(name__icontains="prune orphans").latest("pk")
            prune_forward_orphans(job)
        # In-scope device + its site kept.
        self.assertTrue(Device.objects.filter(name="dev-site-a").exists())
        self.assertTrue(Site.objects.filter(slug="site-active").exists())
        # Out-of-scope device deleted; its site is now empty + not in Forward → deleted.
        self.assertFalse(Device.objects.filter(name="dev-site-stale").exists())
        self.assertFalse(Site.objects.filter(slug="site-stale").exists())
        # site-u has a rack → kept despite not being in Forward locations.
        self.assertTrue(Site.objects.filter(slug="site-u").exists())

    def test_collection_gap_alert_command_breaches_and_tags(self):
        import json
        from io import StringIO

        from django.core.management import call_command

        self._device("dev-collected")
        self._device("dev-backfilled")
        fwd_client = Mock()
        fwd_client.run_nqe_query = Mock(
            return_value=[
                {"name": "dev-collected", "completed": True},
                {"name": "dev-backfilled", "completed": False},
            ]
        )
        out = StringIO()
        with (
            patch.object(ForwardSource, "get_client", return_value=fwd_client),
            patch.object(ForwardSync, "resolve_snapshot_id", return_value="snap-1"),
        ):
            with self.assertRaises(SystemExit):
                call_command(
                    "forward_collection_gap_alert",
                    sync_name="ui-sync",
                    threshold=0,
                    tag=True,
                    fail_on_breach=True,
                    stdout=out,
                )
        data = json.loads(out.getvalue())
        self.assertTrue(data["breached"])
        self.assertEqual(data["backfilled_count"], 1)
        self.assertEqual(data["alert"].count("Collection gap"), 1)
        self.assertTrue(Device.objects.filter(tags__slug="forward-backfilled").exists())

    def test_tag_backfilled_view_enqueues_job(self):
        self._device("dev-backfilled")
        fwd_client = Mock()
        fwd_client.run_nqe_query = Mock(
            return_value=[{"name": "dev-backfilled", "completed": False}]
        )
        client = self._superuser_client()
        with (
            patch.object(ForwardSource, "get_client", return_value=fwd_client),
            patch.object(ForwardSync, "resolve_snapshot_id", return_value="snap-1"),
        ):
            resp = client.post(
                reverse(
                    "plugins:forward_netbox:forwardsync_tag_backfilled",
                    kwargs={"pk": self.sync.pk},
                )
            )
            self.assertEqual(resp.status_code, 302)
            job = Job.objects.filter(name__icontains="tag backfilled").latest("pk")
            tag_forward_backfilled_devices(job)
        self.assertTrue(
            Device.objects.filter(
                name="dev-backfilled", tags__slug="forward-backfilled"
            ).exists()
        )

    def test_tag_delete_eligible_ipam_view_enqueues_and_runs_job(self):
        from forward_netbox.jobs import tag_forward_delete_eligible_ipam

        fwd_client = Mock()
        stub_result = {
            "tag_slug": "forward-delete-eligible",
            "models_tagged": ["ipam.vrf"],
            "skipped": [],
            "results": [],
            "total_eligible": 3,
        }
        client = self._superuser_client()
        with (
            patch.object(ForwardSource, "get_client", return_value=fwd_client),
            patch(
                "forward_netbox.utilities.scope_ipam_audit.tag_delete_eligible_ipam",
                return_value=stub_result,
            ) as mock_tag,
        ):
            resp = client.post(
                reverse(
                    "plugins:forward_netbox:forwardsync_tag_delete_eligible_ipam",
                    kwargs={"pk": self.sync.pk},
                )
            )
            self.assertEqual(resp.status_code, 302)
            job = Job.objects.filter(name__icontains="tag delete-eligible IPAM").latest(
                "pk"
            )
            self.assertEqual(job.object_id, self.sync.pk)
            tag_forward_delete_eligible_ipam(job)
            mock_tag.assert_called_once()
        job.refresh_from_db()
        self.assertEqual(job.data["total_eligible"], 3)

    def test_refresh_device_analysis_job(self):
        from forward_netbox.jobs import refresh_forward_device_analysis
        from forward_netbox.models import ForwardDeviceAnalysis

        self._device("dev-an")
        self._device("dev-down")
        fwd_client = Mock()
        fwd_client.run_nqe_query = Mock(
            return_value=[
                {
                    "name": "dev-an",
                    "reachable": True,
                    "collection_result": "DeviceSnapshotResult.completed",
                    "blast_radius": 7,
                    "cve_count": 2,
                    "up_interfaces": 5,
                    "detail": "DC1",
                },
                {
                    "name": "dev-down",
                    "reachable": False,
                    "collection_result": "DeviceSnapshotResult.collectionFailed"
                    "(DeviceCollectionError.AUTHENTICATION_FAILED)",
                    "blast_radius": 0,
                },
                # Not in NetBox -> skipped (analysis is a NetBox-side overlay).
                {"name": "ghost", "reachable": False, "blast_radius": 0},
            ]
        )
        client = self._superuser_client()
        with (
            patch.object(ForwardSource, "get_client", return_value=fwd_client),
            patch.object(ForwardSync, "resolve_snapshot_id", return_value="snap-1"),
        ):
            resp = client.post(
                reverse(
                    "plugins:forward_netbox:forwardsync_refresh_device_analysis",
                    kwargs={"pk": self.sync.pk},
                )
            )
            self.assertEqual(resp.status_code, 302)
            job = Job.objects.filter(name__icontains="refresh device analysis").latest(
                "pk"
            )
            refresh_forward_device_analysis(job)

        rows = ForwardDeviceAnalysis.objects.filter(sync=self.sync)
        self.assertEqual(rows.count(), 2)
        analysis = rows.get(device__name="dev-an")
        self.assertTrue(analysis.reachable)
        self.assertEqual(analysis.blast_radius, 7)
        self.assertEqual(analysis.cve_count, 2)
        self.assertEqual(analysis.up_interfaces, 5)
        self.assertEqual(analysis.collection_result, "completed")
        # The failed device surfaces the specific Forward collection error token.
        down = rows.get(device__name="dev-down")
        self.assertFalse(down.reachable)
        self.assertEqual(down.collection_result, "AUTHENTICATION_FAILED")

        # List view + device-detail panel render the stored analysis.
        list_resp = client.get(
            reverse("plugins:forward_netbox:forwarddeviceanalysis_list")
        )
        self.assertEqual(list_resp.status_code, 200)
        self.assertContains(list_resp, "dev-an")

        from forward_netbox.template_content import ForwardDeviceAnalysisPanel

        panel = ForwardDeviceAnalysisPanel(
            context={"object": analysis.device, "request": None}
        )
        rendered = panel.right_page()
        self.assertIn("Forward Analysis", rendered)
        # Deep-link pivot into the Forward app.
        self.assertIn("Open in Forward", rendered)
        self.assertIn("https://fwd.app", rendered)

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
