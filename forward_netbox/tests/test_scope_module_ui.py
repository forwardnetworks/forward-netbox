from datetime import timedelta
from unittest.mock import Mock
from unittest.mock import patch

from core.models import Job
from dcim.models import Device
from dcim.models import DeviceRole
from dcim.models import DeviceType
from dcim.models import Manufacturer
from dcim.models import Rack
from dcim.models import Site
from django.contrib.auth import get_user_model
from django.contrib.messages import get_messages
from django.test import Client
from django.test import TestCase
from django.urls import reverse
from django.utils import timezone

from forward_netbox.choices import ForwardSyncStatusChoices
from forward_netbox.jobs import DeviceScopeTagReconciliationJob
from forward_netbox.jobs import PruneOrphansJob
from forward_netbox.models import ForwardIngestion
from forward_netbox.models import ForwardSource
from forward_netbox.models import ForwardSync
from forward_netbox.utilities.module_readiness import compute_module_readiness_for_sync
from forward_netbox.utilities.ownership import reconcile_sync_scope_tag_claims


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
                # The absence quarantine is exercised by its own tests and by
                # the two cases at the end of this file. Everything else here
                # is about the view and the prune wiring, and would otherwise
                # assert against a prune held back before it deleted anything.
                "device_tag_prune_absence_runs": 0,
                "device_tag_prune_absence_hours": 0,
            },
        )
        self.sync = ForwardSync.objects.create(
            name="ui-sync",
            source=self.source,
            status=ForwardSyncStatusChoices.COMPLETED,
            parameters={"snapshot_id": "latestProcessed"},
        )
        self.ingestion = ForwardIngestion.objects.create(
            sync=self.sync,
            snapshot_id="snap-1",
            baseline_ready=True,
        )
        mfr = Manufacturer.objects.create(name="MfrU", slug="mfr-u")
        self.dt = DeviceType.objects.create(manufacturer=mfr, model="dt-u", slug="dt-u")
        self.role = DeviceRole.objects.create(name="RoleU", slug="role-u")
        self.site = Site.objects.create(name="SiteU", slug="site-u")

    def _device(self, name):
        return Device.objects.create(
            name=name, device_type=self.dt, role=self.role, site=self.site
        )

    def _claim_scope(self, *device_names):
        reconcile_sync_scope_tag_claims(
            self.sync,
            {name: ["Prod_Core"] for name in device_names},
            generation=self.ingestion.pk,
            snapshot_id=self.ingestion.snapshot_id,
        )

    # --- module readiness utilities -----------------------------------------

    @patch("forward_netbox.utilities.module_readiness.fetch_module_rows_for_sync")
    def test_module_readiness_reports_branch_creation_plan(self, mock_fetch):
        self._device("dev-m")
        mock_fetch.return_value = [
            {"device": "dev-m", "module_bay": "Slot 1"},
            {"device": "dev-m", "module_bay": "Slot 2"},
            {"device": "ghost", "module_bay": "Slot 1"},
        ]
        report = compute_module_readiness_for_sync(self.sync)
        self.assertEqual(report.unique_missing_bays, 2)
        self.assertEqual(report.missing_device_rows, 1)
        self.assertEqual(
            [row["name"] for row in report.module_bay_plan_rows],
            ["Slot 1", "Slot 2"],
        )

    # --- view smoke tests ----------------------------------------------------

    def _run_scope_reconciliation_job(self, fwd_client):
        """Compute and store the report the way production now does.

        The page no longer computes on GET: `compute_scope_reconciliation`
        issues two live NQE fetch-all queries and a customer hit a 504 on it, so
        it runs in a background job and the view renders the stored result.
        """
        from uuid import uuid4

        from core.choices import JobStatusChoices
        from django.contrib.contenttypes.models import ContentType

        from forward_netbox.jobs import _scope_reconciliation_work

        job = Job.objects.create(
            object_type=ContentType.objects.get_for_model(ForwardSync),
            object_id=self.sync.pk,
            name=f"{self.sync.name} - scope reconciliation",
            status=JobStatusChoices.STATUS_COMPLETED,
            job_id=uuid4(),
        )
        _scope_reconciliation_work(job)
        return job

    def _superuser_client(self):
        user = get_user_model().objects.create_user(username="admin-ui", password="x")
        user.is_superuser = True
        user.is_staff = True
        user.save()
        client = Client()
        client.force_login(user)
        return client

    def test_ingestion_evidence_offers_delete_but_not_edit(self):
        # These assertions previously required delete to be ABSENT. That pinned
        # a workaround, not a requirement: 2.6.3 fixed a NoReverseMatch crash by
        # removing the delete action rather than registering the view it pointed
        # at, and a customer reported on 2.6.5 that ingestions could no longer be
        # deleted at all. Delete is intended; edit is not, because an ingestion
        # records what a sync did and is not editable after the fact.
        from netbox.object_actions import BulkDelete
        from netbox.object_actions import BulkEdit

        from forward_netbox.tables import ForwardIngestionTable
        from forward_netbox.views import ForwardIngestionListView

        self.assertIn(BulkDelete, ForwardIngestionListView.actions)
        self.assertNotIn(BulkEdit, ForwardIngestionListView.actions)
        self.assertIn("delete", ForwardIngestionTable.base_columns["actions"].actions)
        self.assertNotIn("edit", ForwardIngestionTable.base_columns["actions"].actions)
        # The routes must exist; their absence is what the 2.6.3 workaround was
        # working around.
        reverse(
            "plugins:forward_netbox:forwardingestion_delete",
            kwargs={"pk": self.ingestion.pk},
        )
        reverse("plugins:forward_netbox:forwardingestion_bulk_delete")

    def test_ingestion_list_renders_for_superuser_with_delete_link(self):
        response = self._superuser_client().get(
            reverse("plugins:forward_netbox:forwardingestion_list")
        )

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, f"Ingestion {self.ingestion.pk}")
        self.assertContains(response, 'aria-label="Delete"')
        self.assertNotContains(response, 'aria-label="Edit"')
        self.assertNotContains(response, 'aria-label="Changelog"')

    def test_scope_reconciliation_view_and_prune(self):
        self._device("dev-a")
        self._device("dev-stale")
        self._claim_scope("dev-stale")
        opengear = Manufacturer.objects.create(name="Opengear", slug="opengear")
        DeviceType.objects.create(
            manufacturer=opengear,
            model="Opengear Console-Example, Linux 6.0 OpenGear Version 9.9",
            slug="opengear-console-example-legacy",
        )
        fwd_client = Mock()
        fwd_client.run_nqe_query = Mock(
            return_value=[{"name": "dev-a", "completed": True}]
        )
        client = self._superuser_client()
        with (
            patch.object(ForwardSource, "get_client", return_value=fwd_client),
            patch.object(ForwardSync, "resolve_snapshot_id", return_value="snap-1"),
        ):
            self._run_scope_reconciliation_job(fwd_client)
            # GET renders the stored report; it must not query Forward itself.
            resp = client.get(
                reverse(
                    "plugins:forward_netbox:forwardsync_scope_reconciliation",
                    kwargs={"pk": self.sync.pk},
                )
            )
            self.assertEqual(resp.status_code, 200)
            self.assertContains(resp, "Post-Upgrade Catalog Reconciliation")
            self.assertContains(resp, "Stale endpoint DeviceTypes")
            self.assertContains(resp, "Reconcile device scope tags")
            self.assertNotContains(
                resp,
                'class="btn btn-outline-warning" disabled',
                html=False,
            )
            self.assertEqual(
                resp.context["upgrade_reconciliation"]["stale_endpoint_device_types"][
                    "candidate_count"
                ],
                1,
            )
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
            PruneOrphansJob.handle(job)
        self.assertTrue(Device.objects.filter(name="dev-a").exists())
        self.assertFalse(Device.objects.filter(name="dev-stale").exists())

    def test_scope_reconciliation_view_reports_imported_endpoints(self):
        self.source.parameters = {**self.source.parameters, "sync_endpoints": True}
        self.source.save(update_fields=["parameters"])
        self._device("dev-a")
        self._device("endpoint-a")
        fwd_client = Mock()
        fwd_client.run_nqe_query.side_effect = [
            [{"name": "dev-a", "completed": True}],
            [{"name": "endpoint-a"}],
        ]
        client = self._superuser_client()
        with (
            patch.object(ForwardSource, "get_client", return_value=fwd_client),
            patch.object(ForwardSync, "resolve_snapshot_id", return_value="snap-1"),
        ):
            self._run_scope_reconciliation_job(fwd_client)
            response = client.get(
                reverse(
                    "plugins:forward_netbox:forwardsync_scope_reconciliation",
                    kwargs={"pk": self.sync.pk},
                )
            )

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "In scope (SNMP endpoints)")
        self.assertContains(response, ">1</td>", html=False)

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

    def test_scope_tag_overlay_skips_absent_backfilled_target(self):
        from forward_netbox.models import ForwardDeviceTagClaim
        from forward_netbox.utilities.scope_reconciliation import (
            tag_backfilled_devices,
        )

        self.source.parameters = {
            **self.source.parameters,
            "apply_device_scope_tags": True,
        }
        self.source.save(update_fields=["parameters"])
        present = self._device("dev-present")
        fwd_client = Mock()
        fwd_client.run_nqe_query.return_value = [
            {
                "name": present.name,
                "completed": True,
                "tagNames": ["Prod_Core"],
            },
            {
                "name": "dev-absent-backfilled",
                "completed": False,
                "tagNames": ["Prod_Core"],
            },
        ]

        with (
            patch.object(ForwardSource, "get_client", return_value=fwd_client),
            patch.object(ForwardSync, "resolve_snapshot_id", return_value="snap-1"),
        ):
            result = tag_backfilled_devices(self.sync)

        self.assertEqual(result["scope_claims_added"], 1)
        self.assertTrue(result["ownership_current"])
        self.assertTrue(present.tags.filter(name="Prod_Core").exists())
        self.assertEqual(
            set(
                ForwardDeviceTagClaim.objects.filter(
                    sync=self.sync,
                    claim_type="scope",
                ).values_list("device__name", flat=True)
            ),
            {present.name},
        )

    def test_tag_backfilled_devices_also_tags_out_of_scope(self):
        from forward_netbox.utilities.scope_reconciliation import (
            tag_backfilled_devices,
        )

        self._device("dev-collected")
        self._device("dev-orphan")  # in NetBox, not returned by the Forward scope
        self._claim_scope("dev-orphan")
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

    def test_out_of_scope_cleanup_removes_only_managed_include_tags(self):
        from django.utils.text import slugify
        from extras.models import Tag

        from forward_netbox.models import ForwardDeviceTagClaim
        from forward_netbox.utilities.ownership import ensure_device_tag_claim
        from forward_netbox.utilities.scope_reconciliation import (
            tag_backfilled_devices,
        )

        self.source.parameters["apply_device_scope_tags"] = True
        self.source.save(update_fields=["parameters"])
        orphan = self._device("dev-orphan")
        managed = Tag.objects.create(name="Prod_Core", slug=slugify("Prod_Core"))
        operator = Tag.objects.create(name="operator-owned", slug="operator-owned")
        orphan.tags.add(operator)
        ensure_device_tag_claim(
            self.sync,
            orphan,
            managed,
            ForwardDeviceTagClaim.ClaimType.SCOPE,
        )
        fwd_client = Mock()
        fwd_client.run_nqe_query = Mock(
            return_value=[{"name": "dev-collected", "completed": True}]
        )

        with (
            patch.object(ForwardSource, "get_client", return_value=fwd_client),
            patch.object(ForwardSync, "resolve_snapshot_id", return_value="snap-1"),
        ):
            result = tag_backfilled_devices(self.sync)

        self.assertEqual(result["out_of_scope_scope_tags_removed"], 1)
        self.assertEqual(
            set(orphan.tags.values_list("slug", flat=True)),
            {"forward-out-of-scope", "operator-owned"},
        )

    def test_out_of_scope_cleanup_preserves_include_tags_when_disabled(self):
        from django.utils.text import slugify
        from extras.models import Tag

        from forward_netbox.utilities.scope_reconciliation import (
            tag_backfilled_devices,
        )

        orphan = self._device("dev-orphan")
        managed = Tag.objects.create(name="Prod_Core", slug=slugify("Prod_Core"))
        orphan.tags.add(managed)
        fwd_client = Mock()
        fwd_client.run_nqe_query = Mock(
            return_value=[{"name": "dev-collected", "completed": True}]
        )

        with (
            patch.object(ForwardSource, "get_client", return_value=fwd_client),
            patch.object(ForwardSync, "resolve_snapshot_id", return_value="snap-1"),
        ):
            result = tag_backfilled_devices(self.sync)

        self.assertEqual(result["out_of_scope_scope_tags_removed"], 0)
        self.assertEqual(
            set(orphan.tags.values_list("slug", flat=True)), {managed.slug}
        )

    def test_scope_reconciliation_uses_pinned_snapshot(self):
        from forward_netbox.utilities.scope_reconciliation import (
            compute_scope_reconciliation,
        )

        self._device("dev-pinned")
        fwd_client = Mock()
        fwd_client.run_nqe_query.return_value = [
            {"name": "dev-pinned", "completed": True}
        ]

        with (
            patch.object(ForwardSource, "get_client", return_value=fwd_client),
            patch.object(ForwardSync, "resolve_snapshot_id") as resolve_snapshot,
        ):
            compute_scope_reconciliation(self.sync, snapshot_id="snapshot-pinned")

        resolve_snapshot.assert_not_called()
        self.assertEqual(
            fwd_client.run_nqe_query.call_args.kwargs["snapshot_id"],
            "snapshot-pinned",
        )

    def test_stale_snapshot_reconciliation_does_not_mutate_tags(self):
        from extras.models import Tag

        from forward_netbox.choices import ForwardSyncStatusChoices
        from forward_netbox.models import ForwardIngestion
        from forward_netbox.utilities.post_sync import StalePostSyncSnapshotError
        from forward_netbox.utilities.scope_reconciliation import (
            tag_backfilled_devices,
        )

        device = self._device("dev-stale-overlay")
        operator = Tag.objects.create(name="Operator stale", slug="operator-stale")
        device.tags.add(operator)
        self.sync.status = ForwardSyncStatusChoices.COMPLETED
        self.sync.save(update_fields=["status"])
        ForwardIngestion.objects.create(
            sync=self.sync,
            snapshot_id="snapshot-old",
            baseline_ready=True,
        )
        ForwardIngestion.objects.create(
            sync=self.sync,
            snapshot_id="snapshot-new",
            baseline_ready=True,
        )

        with self.assertRaises(StalePostSyncSnapshotError):
            tag_backfilled_devices(
                self.sync,
                snapshot_id="snapshot-old",
                report={
                    "_present_backfilled": {device.name},
                    "_out_of_scope": set(),
                    "_tagged_names": {device.name},
                },
            )

        self.assertEqual(
            set(device.tags.values_list("slug", flat=True)),
            {operator.slug},
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
        self._claim_scope("dev-site-stale")
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
            PruneOrphansJob.handle(job)
        # In-scope device + its site kept.
        self.assertTrue(Device.objects.filter(name="dev-site-a").exists())
        self.assertTrue(Site.objects.filter(slug="site-active").exists())
        # Out-of-scope device deleted; its site is now empty + not in Forward → deleted.
        self.assertFalse(Device.objects.filter(name="dev-site-stale").exists())
        self.assertFalse(Site.objects.filter(slug="site-stale").exists())
        # site-u has a rack → kept despite not being in Forward locations.
        self.assertTrue(Site.objects.filter(slug="site-u").exists())

    def test_prune_keeps_sites_with_non_device_objects(self):
        # Regression: a site empty of devices+racks but still holding a VLAN
        # (PROTECT) or a prefix (CASCADE) is NOT truly empty. Deleting it would
        # either raise ProtectedError (aborting the whole prune) or silently
        # cascade-delete the prefix. Such sites must be kept; only a site nothing
        # references is pruned.
        from ipam.models import Prefix
        from ipam.models import VLAN

        from forward_netbox.utilities.scope_reconciliation import prune_orphan_sites

        Rack.objects.create(name="rack-keep", site=self.site)  # keeps setUp's site-u
        site_vlan = Site.objects.create(name="Site Vlan", slug="site-vlan")
        site_prefix = Site.objects.create(name="Site Prefix", slug="site-prefix")
        Site.objects.create(name="Site Empty", slug="site-empty")
        VLAN.objects.create(vid=10, name="v10", site=site_vlan)  # PROTECT
        # NetBox 4.x scopes a prefix via the generic `scope` (mirrored to _site).
        Prefix.objects.create(prefix="10.0.0.0/24", scope=site_prefix)  # CASCADE

        report = {
            "_tagged_names": {"dev-x"},
            "_forward_site_slugs": {"site-active"},  # none of the above are in-scope
        }
        result = prune_orphan_sites(self.sync, report=report)

        # Only the truly-empty orphan is pruned; no ProtectedError raised.
        self.assertEqual(result["pruned_site_count"], 1)
        self.assertFalse(Site.objects.filter(slug="site-empty").exists())
        # VLAN-bearing and prefix-bearing sites are kept; the prefix survives.
        self.assertTrue(Site.objects.filter(slug="site-vlan").exists())
        self.assertTrue(Site.objects.filter(slug="site-prefix").exists())
        self.assertTrue(Prefix.objects.filter(prefix="10.0.0.0/24").exists())
        # site-u (rack) kept.
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
            job = Job.objects.filter(name__icontains="reconcile device scope").latest(
                "pk"
            )
            DeviceScopeTagReconciliationJob.handle(job)
        self.assertTrue(
            Device.objects.filter(
                name="dev-backfilled", tags__slug="forward-backfilled"
            ).exists()
        )

    def test_tag_delete_eligible_ipam_view_enqueues_and_runs_job(self):
        from forward_netbox.jobs import TagDeleteEligibleIpamJob

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
            TagDeleteEligibleIpamJob.handle(job)
            mock_tag.assert_called_once()
        job.refresh_from_db()
        self.assertEqual(job.data["total_eligible"], 3)

    def test_refresh_device_analysis_job(self):
        from forward_netbox.jobs import DeviceAnalysisRefreshJob
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
                    "cve_ids": ["CVE-2022-5678", "CVE-2021-1234"],
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
            DeviceAnalysisRefreshJob.handle(job)

        rows = ForwardDeviceAnalysis.objects.filter(sync=self.sync)
        self.assertEqual(rows.count(), 2)
        analysis = rows.get(device__name="dev-an")
        self.assertTrue(analysis.reachable)
        self.assertEqual(analysis.blast_radius, 7)
        self.assertEqual(analysis.cve_count, 2)
        # CVE IDs stored (deduped + sorted) so the panel can list them.
        self.assertEqual(analysis.cve_ids, ["CVE-2021-1234", "CVE-2022-5678"])
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
        # The actual CVE IDs behind the exposure count are listed + linked to NVD.
        self.assertIn("CVE-2021-1234", rendered)
        self.assertIn("nvd.nist.gov/vuln/detail/CVE-2022-5678", rendered)
        # Deep-link pivot into the Forward app.
        self.assertIn("Open in Forward", rendered)
        self.assertIn("https://fwd.app", rendered)

    def test_device_analysis_fetch_uses_pinned_snapshot(self):
        from forward_netbox.utilities.device_analysis import (
            fetch_device_analysis_rows,
        )

        fwd_client = Mock()
        fwd_client.run_nqe_query.return_value = []
        with (
            patch.object(ForwardSource, "get_client", return_value=fwd_client),
            patch.object(ForwardSync, "resolve_snapshot_id") as resolve_snapshot,
        ):
            rows, snapshot_id = fetch_device_analysis_rows(
                self.sync,
                snapshot_id="snapshot-pinned",
            )

        self.assertEqual(rows, [])
        self.assertEqual(snapshot_id, "snapshot-pinned")
        resolve_snapshot.assert_not_called()
        self.assertEqual(
            fwd_client.run_nqe_query.call_args.kwargs["snapshot_id"],
            "snapshot-pinned",
        )

    def test_module_readiness_view_reports_branch_plan(self):
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
            self.assertContains(resp, "Missing bays")
            self.assertContains(resp, "created inside the sync branch")

    def test_module_readiness_view_does_not_expose_exception_details(self):
        client = self._superuser_client()
        with (
            self.assertLogs("forward_netbox.views", level="WARNING") as logs,
            patch(
                "forward_netbox.utilities.module_readiness.compute_module_readiness_for_sync",
                side_effect=RuntimeError("sentinel-private-detail"),
            ),
        ):
            response = client.get(
                reverse(
                    "plugins:forward_netbox:forwardsync_module_readiness",
                    kwargs={"pk": self.sync.pk},
                )
            )

        rendered_messages = " ".join(
            str(message) for message in get_messages(response.wsgi_request)
        )
        self.assertEqual(response.status_code, 302)
        self.assertNotIn("sentinel-private-detail", rendered_messages)
        self.assertNotIn("sentinel-private-detail", " ".join(logs.output))
        self.assertIn("Review server logs", rendered_messages)

    # --- absence quarantine on the panel --------------------------------------

    def _quarantine_on(self):
        """Restore production defaults for the two cases that are about them."""
        self.source.parameters = {
            **self.source.parameters,
            "device_tag_prune_absence_runs": 3,
            "device_tag_prune_absence_hours": 72,
        }
        self.source.save()
        self.sync.refresh_from_db()

    def test_the_panel_names_what_the_quarantine_is_holding(self):
        """An operator who cannot see the hold reads it as the prune being broken."""
        self._quarantine_on()
        self._device("dev-a")
        self._device("dev-stale")
        self._claim_scope("dev-stale")
        fwd_client = Mock()
        fwd_client.run_nqe_query = Mock(
            return_value=[{"name": "dev-a", "completed": True}]
        )
        client = self._superuser_client()
        with (
            patch.object(ForwardSource, "get_client", return_value=fwd_client),
            patch.object(ForwardSync, "resolve_snapshot_id", return_value="snap-1"),
        ):
            self._run_scope_reconciliation_job(fwd_client)
            resp = client.get(
                reverse(
                    "plugins:forward_netbox:forwardsync_scope_reconciliation",
                    kwargs={"pk": self.sync.pk},
                )
            )

        self.assertEqual(resp.status_code, 200)
        self.assertContains(resp, "in quarantine")
        # The override has to be reachable, or the hold is a dead end.
        self.assertContains(resp, 'name="include_quarantined"')
        self.assertEqual(
            resp.context["payload"]["out_of_scope_quarantine"]["held"],
            1,
        )

    def test_a_quarantined_orphan_survives_the_prune(self):
        """Absent once, for no time at all - the prune must leave it alone."""
        self._quarantine_on()
        self._device("dev-a")
        self._device("dev-stale")
        self._claim_scope("dev-stale")
        fwd_client = Mock()
        fwd_client.run_nqe_query = Mock(
            return_value=[{"name": "dev-a", "completed": True}]
        )
        client = self._superuser_client()
        with (
            patch.object(ForwardSource, "get_client", return_value=fwd_client),
            patch.object(ForwardSync, "resolve_snapshot_id", return_value="snap-1"),
        ):
            self._run_scope_reconciliation_job(fwd_client)
            client.post(
                reverse(
                    "plugins:forward_netbox:forwardsync_prune_orphans",
                    kwargs={"pk": self.sync.pk},
                )
            )
            job = Job.objects.filter(name__icontains="prune orphans").latest("pk")
            PruneOrphansJob.handle(job)

        self.assertTrue(Device.objects.filter(name="dev-stale").exists())
        self.assertEqual(job.data["quarantine_held_device_count"], 1)

    def test_the_override_checkbox_reaches_the_job(self):
        """The box has to actually carry, or the hold is a dead end.

        Asserted at the enqueue boundary rather than by running the job: NetBox
        keeps job kwargs in RQ, not on the Job row, so a test that calls
        `handle(job)` cannot recover what the view supplied. The deletion half
        is covered directly in `test_absence_quarantine`.
        """
        client = self._superuser_client()
        prune_url = reverse(
            "plugins:forward_netbox:forwardsync_prune_orphans",
            kwargs={"pk": self.sync.pk},
        )
        with patch(
            "forward_netbox.utilities.sync_facade.enqueue_button_job",
            return_value=Mock(pk=1),
        ) as enqueue:
            client.post(prune_url, {"include_quarantined": "1"})
            ticked = enqueue.call_args.kwargs["job_kwargs"]["include_quarantined"]

            client.post(prune_url)
            unticked = enqueue.call_args.kwargs["job_kwargs"]["include_quarantined"]

        self.assertIs(ticked, True)
        self.assertIs(unticked, False)

    # --- the streak only advances on a run that finished ----------------------

    def _absent_fixture(self):
        """One in-scope device and one orphan, with a client that reports only
        the first. `dev-stale` is the absence every case below measures."""
        self._quarantine_on()
        self._device("dev-a")
        self._device("dev-stale")
        self._claim_scope("dev-stale")
        fwd_client = Mock()
        fwd_client.run_nqe_query = Mock(
            return_value=[{"name": "dev-a", "completed": True}]
        )
        return fwd_client

    def test_an_orphan_survives_the_claim_release_and_reaches_eligibility(self):
        """The whole quarantine, end to end, through nothing but real runs.

        This is the case that no direct `record_device_absence` test can reach.
        The scope claim is released on the FIRST run that sees the device
        missing, and `out_of_scope` is derived from live claims - so without the
        absence row counting as "previously managed" the device silently stops
        being an orphan on run 2. The streak sticks at 1, the panel's held count
        drops to 0, and the prune becomes a permanent no-op for exactly the
        devices it exists to delete. It fails safe and delivers nothing.
        """
        from forward_netbox.models import ForwardDeviceAbsence
        from forward_netbox.models import ForwardDeviceTagClaim
        from forward_netbox.utilities.scope_reconciliation import (
            compute_scope_reconciliation,
        )
        from forward_netbox.utilities.scope_reconciliation import (
            prune_orphan_devices,
        )
        from forward_netbox.utilities.scope_reconciliation import (
            tag_backfilled_devices,
        )

        fwd_client = self._absent_fixture()
        with (
            patch.object(ForwardSource, "get_client", return_value=fwd_client),
            patch.object(ForwardSync, "resolve_snapshot_id", return_value="snap-1"),
        ):
            for _ in range(3):
                tag_backfilled_devices(self.sync)

            # The claim really is gone - this test would pass for the wrong
            # reason if the release stopped happening.
            self.assertFalse(
                ForwardDeviceTagClaim.objects.filter(
                    sync=self.sync,
                    claim_type="scope",
                    device__name="dev-stale",
                ).exists()
            )
            row = ForwardDeviceAbsence.objects.get(sync=self.sync)
            self.assertEqual(row.device.name, "dev-stale")
            self.assertEqual(row.consecutive_absent_runs, 3)

            # Still an orphan on run 3, which is what the claim release used to
            # destroy.
            report = compute_scope_reconciliation(self.sync)
            self.assertEqual(report["_out_of_scope"], {"dev-stale"})

            # Runs are served; the clock is not, so it is still held.
            held = prune_orphan_devices(self.sync, report=report)
            self.assertEqual(held["pruned_device_count"], 0)
            self.assertEqual(held["quarantine_held_device_count"], 1)

            ForwardDeviceAbsence.objects.filter(pk=row.pk).update(
                first_absent_at=timezone.now() - timedelta(hours=73)
            )
            released = prune_orphan_devices(
                self.sync,
                report=compute_scope_reconciliation(self.sync),
            )

        self.assertEqual(released["pruned_device_count"], 1)
        self.assertFalse(Device.objects.filter(name="dev-stale").exists())

    def test_a_returning_device_clears_the_streak_through_the_real_job(self):
        """Coming back has to mean coming back in Forward.

        The mirror of the case above: now that an absence row keeps a device
        counted as previously managed, the row has to be released by the device
        reappearing in the Forward result - otherwise widening what counts as
        managed would make an absence permanent instead of merely durable.
        """
        from forward_netbox.models import ForwardDeviceAbsence
        from forward_netbox.utilities.scope_reconciliation import (
            tag_backfilled_devices,
        )

        fwd_client = self._absent_fixture()
        with (
            patch.object(ForwardSource, "get_client", return_value=fwd_client),
            patch.object(ForwardSync, "resolve_snapshot_id", return_value="snap-1"),
        ):
            tag_backfilled_devices(self.sync)
            tag_backfilled_devices(self.sync)
            self.assertEqual(
                ForwardDeviceAbsence.objects.get(
                    sync=self.sync
                ).consecutive_absent_runs,
                2,
            )

            fwd_client.run_nqe_query.return_value = [
                {"name": "dev-a", "completed": True},
                {"name": "dev-stale", "completed": True},
            ]
            returned = tag_backfilled_devices(self.sync)

        self.assertEqual(returned["absence_streaks_cleared"], 1)
        self.assertFalse(ForwardDeviceAbsence.objects.filter(sync=self.sync).exists())
        self.assertTrue(Device.objects.filter(name="dev-stale").exists())

    def test_a_completed_run_records_the_absence_streak(self):
        """The streak has to actually accumulate through the real job.

        Every other streak test calls `record_device_absence` directly, so
        nothing yet proves the post-sync job reaches it - and a quarantine whose
        streak never advances holds every orphan forever.
        """
        from forward_netbox.models import ForwardDeviceAbsence
        from forward_netbox.utilities.scope_reconciliation import (
            tag_backfilled_devices,
        )

        fwd_client = self._absent_fixture()
        with (
            patch.object(ForwardSource, "get_client", return_value=fwd_client),
            patch.object(ForwardSync, "resolve_snapshot_id", return_value="snap-1"),
        ):
            first = tag_backfilled_devices(self.sync)
            second = tag_backfilled_devices(self.sync)

        self.assertEqual(first["absence_streaks_started"], 1)
        self.assertEqual(second["absence_streaks_advanced"], 1)
        row = ForwardDeviceAbsence.objects.get(sync=self.sync)
        self.assertEqual(row.device.name, "dev-stale")
        self.assertEqual(row.consecutive_absent_runs, 2)

    def test_a_run_that_dies_before_recording_does_not_advance_the_streak(self):
        """An absence we could not confirm is not evidence of absence.

        The streak is what licenses a permanent delete, so a run that dies
        partway must leave the count exactly where it was rather than banking a
        confirmation it never earned.
        """
        from forward_netbox.models import ForwardDeviceAbsence
        from forward_netbox.utilities.scope_reconciliation import (
            tag_backfilled_devices,
        )

        fwd_client = self._absent_fixture()
        with (
            patch.object(ForwardSource, "get_client", return_value=fwd_client),
            patch.object(ForwardSync, "resolve_snapshot_id", return_value="snap-1"),
        ):
            tag_backfilled_devices(self.sync)
            with patch(
                "forward_netbox.utilities.ownership.finalize_device_tag_domain",
                side_effect=RuntimeError("collection died mid-run"),
            ):
                with self.assertRaises(RuntimeError):
                    tag_backfilled_devices(self.sync)

        row = ForwardDeviceAbsence.objects.get(sync=self.sync)
        self.assertEqual(row.consecutive_absent_runs, 1)

    def test_a_streak_rolls_back_with_the_run_that_recorded_it(self):
        """The row is written inside the tagging transaction, not beside it.

        Dying before the recording is the easy half - it simply never runs. This
        is the other half: the streak is written and THEN the run fails, and the
        row has to go with it rather than survive to claim an absence the tags
        were rolled back on.
        """
        from forward_netbox.models import ForwardDeviceAbsence
        from forward_netbox.utilities import scope_reconciliation
        from forward_netbox.utilities.scope_reconciliation import (
            tag_backfilled_devices,
        )

        fwd_client = self._absent_fixture()
        record_for_real = scope_reconciliation.record_device_absence

        def _record_then_die(*args, **kwargs):
            record_for_real(*args, **kwargs)
            raise RuntimeError("died after recording, before commit")

        with (
            patch.object(ForwardSource, "get_client", return_value=fwd_client),
            patch.object(ForwardSync, "resolve_snapshot_id", return_value="snap-1"),
            patch.object(
                scope_reconciliation,
                "record_device_absence",
                side_effect=_record_then_die,
            ),
        ):
            with self.assertRaises(RuntimeError):
                tag_backfilled_devices(self.sync)

        self.assertFalse(ForwardDeviceAbsence.objects.filter(sync=self.sync).exists())
